package com.zero.consumer;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Entry point for the payment consumer.
 *
 * Subscribes to Kafka orders.created, processes each record through the
 * pipeline, and writes results to PostgreSQL. Manual offset commit
 * (at-least-once delivery). Optionally serves the REST dashboard.
 *
 * Mirrors Python consumer/main.py run() + main().
 */
public class PaymentConsumerApplication {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumerApplication.class);

    public static void main(String[] args) throws Exception {
        boolean once = false;
        for (String arg : args) {
            if ("--once".equals(arg)) once = true;
        }

        Settings settings = Settings.load();
        log.info("Starting payment-consumer bootstrap={} topic={} group={}",
                settings.kafkaBootstrapServers, settings.kafkaOrdersTopic, settings.kafkaConsumerGroup);

        DatabaseClient db = new DatabaseClient(settings);
        DlqPublisher dlq = new DlqPublisher(settings);
        Metrics metrics = new Metrics();
        Pipeline pipeline = new Pipeline(db, dlq, settings, metrics);

        // Start dashboard in background thread
        Dashboard dashboard = new Dashboard(settings, metrics);
        Thread dashThread = new Thread(() -> {
            try {
                dashboard.start();
            } catch (Exception e) {
                log.error("Dashboard failed to start: {}", e.getMessage(), e);
            }
        }, "dashboard");
        dashThread.setDaemon(true);
        dashThread.start();

        // Build Kafka consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.kafkaBootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, settings.kafkaConsumerGroup);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, settings.kafkaClientId);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps);

        // Volatile flag for shutdown hook
        final boolean[] running = {true};
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — stopping consumer...");
            running[0] = false;
            consumer.wakeup();
        }, "shutdown-hook"));

        consumer.subscribe(Collections.singletonList(settings.kafkaOrdersTopic));

        int processed = 0;
        int emptyPolls = 0;
        try {
            while (running[0]) {
                ConsumerRecords<byte[], byte[]> records;
                try {
                    records = consumer.poll(Duration.ofMillis(500));
                } catch (org.apache.kafka.common.errors.WakeupException e) {
                    if (!running[0]) break;
                    throw e;
                }

                for (ConsumerRecord<byte[], byte[]> record : records) {
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    try {
                        pipeline.processRecord(record);
                    } catch (OffsetRejectException e) {
                        // Outcome NOT durably persisted — do NOT commit.
                        // Rewind to this record's offset and break so it will be redelivered.
                        log.error("Offset NOT committed for partition={} offset={} — rewinding for redelivery: {}",
                                record.partition(), record.offset(), e.getMessage());
                        consumer.seek(tp, record.offset());
                        break;
                    } catch (Exception e) {
                        // Unexpected failure — rewind and break to retry or fail deterministically.
                        log.error("Unexpected error at partition={} offset={}; rewinding for redelivery: {}",
                                record.partition(), record.offset(), e.getMessage(), e);
                        consumer.seek(tp, record.offset());
                        break;
                    }

                    // Commit explicitly ONLY up to this record's offset + 1
                    try {
                        consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(record.offset() + 1)));
                        processed++;
                    } catch (CommitFailedException e) {
                        log.warn("commitSync failed (rebalance or timeout) at partition={} offset={}: {}",
                                record.partition(), record.offset(), e.getMessage());
                        break;
                    }
                }

                if (!records.isEmpty()) {
                    if (once) break;
                } else if (once) {
                    emptyPolls++;
                    if (emptyPolls >= 6) { // ~3 seconds waiting for group rebalance/assignment
                        break;
                    }
                }
            }
        } finally {
            consumer.close();
            dlq.close();
            db.close();
            dashboard.stop();
        }

        log.info("Stopped after {} record(s)", processed);
    }

    /**
     * Process a batch of records partitioned across topics and partitions with safe offset commits.
     * Guarantees:
     * 1. Offsets are committed per partition for contiguous successful records only.
     * 2. If a record fails, no subsequent record on the same partition is committed.
     * 3. The partition is rewound via seek() to the failed record's offset so it and any
     *    unprocessed records in this batch are redelivered on the next poll().
     * 4. Failure on one partition does not halt processing on independent partitions.
     *
     * @param consumer the Kafka consumer instance
     * @param pipeline the business pipeline
     * @param records the polled batch
     * @return count of successfully processed records in this batch
     */
    public static int processBatch(
            Consumer<byte[], byte[]> consumer,
            Pipeline pipeline,
            ConsumerRecords<byte[], byte[]> records) {

        int processedInBatch = 0;

        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(tp);
            for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                try {
                    pipeline.processRecord(record);
                    // Durable outcome reached (applied, duplicate, parked, or DLQ'd).
                    // Commit explicit next offset for this partition:
                    long nextOffset = record.offset() + 1;
                    consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(nextOffset)));
                    processedInBatch++;
                } catch (OffsetRejectException e) {
                    // Outcome NOT durably persisted (e.g. DB or DLQ write failed).
                    // Rewind consumer position for this partition so this record and
                    // any remaining records on this partition are redelivered.
                    log.error("Offset NOT committed for topic={} partition={} offset={} ({}) — rewinding and stopping partition processing",
                            record.topic(), record.partition(), record.offset(), e.getMessage());
                    consumer.seek(tp, record.offset());
                    break; // Stop processing further records in THIS partition for this batch
                } catch (Exception e) {
                    // Unexpected error — rewind and stop partition processing
                    log.error("Unexpected error at topic={} partition={} offset={}; rewinding and stopping partition processing",
                            record.topic(), record.partition(), record.offset(), e);
                    consumer.seek(tp, record.offset());
                    break; // Stop processing further records in THIS partition for this batch
                }
            }
        }
        return processedInBatch;
    }
}
