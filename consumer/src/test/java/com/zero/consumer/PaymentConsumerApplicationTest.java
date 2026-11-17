package com.zero.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Deterministic unit tests for Kafka consumer per-partition offset management and failure recovery.
 */
class PaymentConsumerApplicationTest {

    private static final String TOPIC = "orders.created";
    private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition P1 = new TopicPartition(TOPIC, 1);

    private MockConsumer<byte[], byte[]> mockConsumer;
    private RecordProcessor mockPipeline;

    @BeforeEach
    void setUp() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.assign(List.of(P0, P1));
        mockConsumer.updateBeginningOffsets(Map.of(P0, 0L, P1, 0L));
        mockPipeline = mock(RecordProcessor.class);
    }

    private ConsumerRecord<byte[], byte[]> createRecord(TopicPartition tp, long offset, String key, String jsonPayload) {
        return new ConsumerRecord<>(
                tp.topic(),
                tp.partition(),
                offset,
                key.getBytes(StandardCharsets.UTF_8),
                jsonPayload.getBytes(StandardCharsets.UTF_8)
        );
    }

    @Test
    @DisplayName("Scenario A: Multiple successful records in one partition commit contiguous next offset")
    void testMultipleSuccessfulRecordsInOnePartition() {
        ConsumerRecord<byte[], byte[]> r100 = createRecord(P0, 100L, "agg-1", "{\"event_id\":\"e1\"}");
        ConsumerRecord<byte[], byte[]> r101 = createRecord(P0, 101L, "agg-2", "{\"event_id\":\"e2\"}");
        ConsumerRecord<byte[], byte[]> r102 = createRecord(P0, 102L, "agg-3", "{\"event_id\":\"e3\"}");

        when(mockPipeline.processRecord(r100)).thenReturn("applied");
        when(mockPipeline.processRecord(r101)).thenReturn("applied");
        when(mockPipeline.processRecord(r102)).thenReturn("applied");

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> map = Map.of(P0, List.of(r100, r101, r102));
        ConsumerRecords<byte[], byte[]> batch = new ConsumerRecords<>(map);

        int processed = PaymentConsumerApplication.processBatch(mockConsumer, mockPipeline, batch);

        assertEquals(3, processed, "All 3 records must be processed");
        assertNotNull(mockConsumer.committed(P0), "Committed offset for P0 must not be null");
        assertEquals(103L, mockConsumer.committed(P0).offset(), "Committed offset must be 103 (last processed offset 102 + 1)");
    }

    @Test
    @DisplayName("Scenario B: Failure in a partition stops processing that partition and rewinds without skipping")
    void testFailureFollowedByAnotherRecordInSamePartition() {
        ConsumerRecord<byte[], byte[]> r100 = createRecord(P0, 100L, "agg-1", "{\"event_id\":\"e1\"}");
        ConsumerRecord<byte[], byte[]> r101 = createRecord(P0, 101L, "agg-2", "{\"event_id\":\"e2\"}");
        ConsumerRecord<byte[], byte[]> r102 = createRecord(P0, 102L, "agg-3", "{\"event_id\":\"e3\"}");

        when(mockPipeline.processRecord(r100)).thenReturn("applied");
        when(mockPipeline.processRecord(r101)).thenThrow(new OffsetRejectException("Simulated database write failure"));

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> map = Map.of(P0, List.of(r100, r101, r102));
        ConsumerRecords<byte[], byte[]> batch = new ConsumerRecords<>(map);

        // Pre-condition: poll would have advanced consumer position to end of batch (103)
        mockConsumer.seek(P0, 103L);

        int processed = PaymentConsumerApplication.processBatch(mockConsumer, mockPipeline, batch);

        assertEquals(1, processed, "Only the first record before failure must be processed");
        assertEquals(101L, mockConsumer.committed(P0).offset(), "Committed offset must stay at 101 (from successful offset 100)");

        // Verify r102 was NEVER processed
        verify(mockPipeline, never()).processRecord(r102);

        // Verify seek() was called to rewind position back to failed offset 101 for redelivery
        assertEquals(101L, mockConsumer.position(P0), "Position must be rewound to failed offset 101");
    }

    @Test
    @DisplayName("Scenario C: Successful processing across two independent partitions commits each independently")
    void testSuccessfulProcessingAcrossTwoPartitions() {
        ConsumerRecord<byte[], byte[]> p0_r100 = createRecord(P0, 100L, "agg-1", "{\"event_id\":\"e1\"}");
        ConsumerRecord<byte[], byte[]> p0_r101 = createRecord(P0, 101L, "agg-2", "{\"event_id\":\"e2\"}");

        ConsumerRecord<byte[], byte[]> p1_r200 = createRecord(P1, 200L, "agg-3", "{\"event_id\":\"e3\"}");
        ConsumerRecord<byte[], byte[]> p1_r201 = createRecord(P1, 201L, "agg-4", "{\"event_id\":\"e4\"}");

        when(mockPipeline.processRecord(p0_r100)).thenReturn("applied");
        when(mockPipeline.processRecord(p0_r101)).thenReturn("applied");
        when(mockPipeline.processRecord(p1_r200)).thenReturn("applied");
        when(mockPipeline.processRecord(p1_r201)).thenReturn("applied");

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> map = new LinkedHashMap<>();
        map.put(P0, List.of(p0_r100, p0_r101));
        map.put(P1, List.of(p1_r200, p1_r201));
        ConsumerRecords<byte[], byte[]> batch = new ConsumerRecords<>(map);

        int processed = PaymentConsumerApplication.processBatch(mockConsumer, mockPipeline, batch);

        assertEquals(4, processed, "All 4 records across both partitions must be processed");
        assertEquals(102L, mockConsumer.committed(P0).offset(), "P0 committed offset must be 102");
        assertEquals(202L, mockConsumer.committed(P1).offset(), "P1 committed offset must be 202");
    }

    @Test
    @DisplayName("Scenario D: Failure in one partition halts that partition while other partitions continue safely")
    void testFailureInOnePartitionWhileAnotherPartitionContinues() {
        // P0: 100 (success), 101 (failure), 102 (unprocessed)
        ConsumerRecord<byte[], byte[]> p0_r100 = createRecord(P0, 100L, "agg-1", "{\"event_id\":\"e1\"}");
        ConsumerRecord<byte[], byte[]> p0_r101 = createRecord(P0, 101L, "agg-2", "{\"event_id\":\"e2\"}");
        ConsumerRecord<byte[], byte[]> p0_r102 = createRecord(P0, 102L, "agg-3", "{\"event_id\":\"e3\"}");

        // P1: 200 (success), 201 (success)
        ConsumerRecord<byte[], byte[]> p1_r200 = createRecord(P1, 200L, "agg-4", "{\"event_id\":\"e4\"}");
        ConsumerRecord<byte[], byte[]> p1_r201 = createRecord(P1, 201L, "agg-5", "{\"event_id\":\"e5\"}");

        when(mockPipeline.processRecord(p0_r100)).thenReturn("applied");
        when(mockPipeline.processRecord(p0_r101)).thenThrow(new OffsetRejectException("Deadlock in DB"));
        when(mockPipeline.processRecord(p1_r200)).thenReturn("applied");
        when(mockPipeline.processRecord(p1_r201)).thenReturn("applied");

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> map = new LinkedHashMap<>();
        map.put(P0, List.of(p0_r100, p0_r101, p0_r102));
        map.put(P1, List.of(p1_r200, p1_r201));
        ConsumerRecords<byte[], byte[]> batch = new ConsumerRecords<>(map);

        // Simulate fetch advancing P0 to 103, P1 to 202
        mockConsumer.seek(P0, 103L);
        mockConsumer.seek(P1, 202L);

        int processed = PaymentConsumerApplication.processBatch(mockConsumer, mockPipeline, batch);

        assertEquals(3, processed, "1 from P0 + 2 from P1 must be processed");

        // P0 checks
        assertEquals(101L, mockConsumer.committed(P0).offset(), "P0 committed offset must be 101");
        assertEquals(101L, mockConsumer.position(P0), "P0 position must be rewound to 101");
        verify(mockPipeline, never()).processRecord(p0_r102);

        // P1 checks
        assertEquals(202L, mockConsumer.committed(P1).offset(), "P1 committed offset must be 202");
        assertEquals(202L, mockConsumer.position(P1), "P1 position must remain at 202");
    }

    @Test
    @DisplayName("Scenario E: Immediate failure leaves all records in partition eligible for redelivery")
    void testImmediateFailureLeavesAllRecordsEligibleForRedelivery() {
        ConsumerRecord<byte[], byte[]> r50 = createRecord(P0, 50L, "agg-1", "{\"event_id\":\"e1\"}");
        ConsumerRecord<byte[], byte[]> r51 = createRecord(P0, 51L, "agg-2", "{\"event_id\":\"e2\"}");

        when(mockPipeline.processRecord(r50)).thenThrow(new OffsetRejectException("DLQ publish timed out"));

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> map = Map.of(P0, List.of(r50, r51));
        ConsumerRecords<byte[], byte[]> batch = new ConsumerRecords<>(map);

        mockConsumer.seek(P0, 52L); // poll position

        int processed = PaymentConsumerApplication.processBatch(mockConsumer, mockPipeline, batch);

        assertEquals(0, processed, "Zero records should be marked processed");
        assertNull(mockConsumer.committed(P0), "No offset should have been committed for P0");
        assertEquals(50L, mockConsumer.position(P0), "P0 must be rewound back to 50 for redelivery");
        verify(mockPipeline, never()).processRecord(r51);
    }

    @Test
    @DisplayName("Scenario F: Duplicate event processing records durable outcome and commits safely")
    void testDuplicateEventProcessingRecordsDurableOutcomeAndCommits() {
        ConsumerRecord<byte[], byte[]> r10 = createRecord(P0, 10L, "agg-1", "{\"event_id\":\"e1\"}");
        ConsumerRecord<byte[], byte[]> r11 = createRecord(P0, 11L, "agg-2", "{\"event_id\":\"e2\"}");

        when(mockPipeline.processRecord(r10)).thenReturn("duplicate");
        when(mockPipeline.processRecord(r11)).thenReturn("applied");

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> map = Map.of(P0, List.of(r10, r11));
        ConsumerRecords<byte[], byte[]> batch = new ConsumerRecords<>(map);

        int processed = PaymentConsumerApplication.processBatch(mockConsumer, mockPipeline, batch);

        assertEquals(2, processed, "Both duplicate and applied events are durable outcomes and counted");
        assertEquals(12L, mockConsumer.committed(P0).offset(), "Committed offset must be 12 (last record 11 + 1)");
    }

    @Test
    @DisplayName("Scenario G: Unexpected runtime exception rewinds partition and stops safely")
    void testUnexpectedRuntimeExceptionRewindsAndStops() {
        ConsumerRecord<byte[], byte[]> r20 = createRecord(P0, 20L, "agg-1", "{\"event_id\":\"e1\"}");
        ConsumerRecord<byte[], byte[]> r21 = createRecord(P0, 21L, "agg-2", "{\"event_id\":\"e2\"}");

        when(mockPipeline.processRecord(r20)).thenThrow(new RuntimeException("Unexpected network timeout"));

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> map = Map.of(P0, List.of(r20, r21));
        ConsumerRecords<byte[], byte[]> batch = new ConsumerRecords<>(map);

        mockConsumer.seek(P0, 22L);

        int processed = PaymentConsumerApplication.processBatch(mockConsumer, mockPipeline, batch);

        assertEquals(0, processed, "Zero records committed");
        assertNull(mockConsumer.committed(P0), "No offset committed");
        assertEquals(20L, mockConsumer.position(P0), "Position must be rewound to 20");
        verify(mockPipeline, never()).processRecord(r21);
    }
}

