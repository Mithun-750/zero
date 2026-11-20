package com.zero.consumer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Interface for per-record processing pipeline.
 */
public interface RecordProcessor {
    String processRecord(ConsumerRecord<byte[], byte[]> record);
}
