package com.zero.consumer.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import com.zero.consumer.exception.OffsetRejectException;
import com.zero.consumer.service.RecordProcessor;

/**
 * Per-partition offset management with safe commits. Extracted verbatim from
 * the old PaymentConsumerApplication so it can be unit-tested with MockConsumer.
 *
 * Guarantees:
 * 1. Offsets are committed per partition for contiguous successful records only.
 * 2. If a record fails, no subsequent record on the same partition is committed.
 * 3. The partition is rewound via seek() to the failed record's offset so it and any
 * unprocessed records in this batch are redelivered on the next poll().
 * 4. Failure on one partition does not halt processing on independent partitions.
 */
public final class BatchProcessor {

    private static final Logger log = LoggerFactory.getLogger(BatchProcessor.class);

    private BatchProcessor() {
    }

    /**
     * @param consumer the Kafka consumer instance
     * @param pipeline the business pipeline
     * @param records  the polled batch
     * @return count of successfully processed records in this batch
     */
    public static int processBatch(
            Consumer<byte[], byte[]> consumer,
            RecordProcessor pipeline,
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
                    try {
                        consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(nextOffset)));
                        processedInBatch++;
                    } catch (CommitFailedException e) {
                        log.warn("commitSync failed (rebalance or timeout) at topic={} partition={} offset={}: {}",
                                record.topic(), record.partition(), record.offset(), e.getMessage());
                        break;
                    }
                } catch (OffsetRejectException e) {
                    // Outcome NOT durably persisted (e.g. DB or DLQ write failed).
                    // Rewind consumer position for this partition so this record and
                    // any remaining records on this partition are redelivered.
                    log.error(
                            "Offset NOT committed for topic={} partition={} offset={} ({}) — rewinding and stopping partition processing",
                            record.topic(), record.partition(), record.offset(), e.getMessage());
                    consumer.seek(tp, record.offset());
                    break; // Stop processing further records in THIS partition for this batch
                } catch (Exception e) {
                    // Unexpected error — rewind and stop partition processing
                    log.error(
                            "Unexpected error at topic={} partition={} offset={}; rewinding and stopping partition processing",
                            record.topic(), record.partition(), record.offset(), e);
                    consumer.seek(tp, record.offset());
                    break; // Stop processing further records in THIS partition for this batch
                }
            }
        }
        return processedInBatch;
    }
}
