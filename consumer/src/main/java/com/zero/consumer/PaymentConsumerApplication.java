package com.zero.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
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
                    try {
                        pipeline.processRecord(record);
                    } catch (OffsetRejectException e) {
                        // Outcome NOT durably persisted — do NOT commit.
                        // The record will be redelivered (at-least-once).
                        log.error("Offset NOT committed for partition={} offset={} — will be redelivered",
                                record.partition(), record.offset());
                        continue;
                    } catch (Exception e) {
                        // Unexpected failure — deterministic for poison records on redelivery.
                        log.error("Unexpected error at partition={} offset={}; not committing",
                                record.partition(), record.offset(), e);
                        continue;
                    }
                    // Commit only after a durable outcome
                    consumer.commitSync();
                    processed++;
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
            db.close();
            dashboard.stop();
        }

        log.info("Stopped after {} record(s)", processed);
    }
}
