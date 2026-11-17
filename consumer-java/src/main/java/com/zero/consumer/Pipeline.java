package com.zero.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Per-record processing pipeline.
 *
 * Outcome per record (all durable → offset committed by caller):
 *   applied   — payment created (or idempotent no-op)
 *   duplicate — event_id already in processed_events
 *   parked    — aggregate blocked; follower event parked in parked_events
 *   failed    — poison event routed to DLQ + aggregate blocked + ledger row
 *
 * OffsetRejectException means "outcome NOT persisted" — callers must NOT commit offset.
 *
 * Mirrors Python consumer/pipeline.py process_record().
 */
public class Pipeline {

    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);
    private static final String DEFAULT_AGGREGATE_TYPE = "ORDER";

    private final ObjectMapper mapper = new ObjectMapper();
    private final DatabaseClient db;
    private final DlqPublisher dlq;
    private final Settings settings;
    private final Metrics metrics;

    public Pipeline(DatabaseClient db, DlqPublisher dlq, Settings settings, Metrics metrics) {
        this.db = db;
        this.dlq = dlq;
        this.settings = settings;
        this.metrics = metrics;
    }

    @SuppressWarnings("unchecked")
    public String processRecord(ConsumerRecord<byte[], byte[]> record) {
        byte[] raw = record.value() != null ? record.value() : new byte[0];

        // --- 1. Parse JSON. Unparseable records → DLQ immediately (no aggregate block) ---
        Map<String, Object> event;
        try {
            Object parsed = mapper.readValue(raw, Object.class);
            if (!(parsed instanceof Map)) {
                throw new Exception("root is not a JSON object");
            }
            event = (Map<String, Object>) parsed;
        } catch (Exception e) {
            log.error("Unparseable record topic={} partition={} offset={}: {}",
                    record.topic(), record.partition(), record.offset(), e.getMessage());
            return failEvent(raw, null, "VALIDATION",
                    "JSON parse error: " + e.getMessage(), 1, record);
        }

        // Check if event has identifiable aggregate fields
        boolean identifiable = event.get("event_id") != null && !event.get("event_id").toString().isBlank()
                && event.get("aggregate_id") != null && !event.get("aggregate_id").toString().isBlank();

        // --- 2. Block check BEFORE dedup and apply ---
        if (identifiable) {
            Object aggregateTypeObj = event.get("aggregate_type");
            String aggregateType = (aggregateTypeObj != null && !aggregateTypeObj.toString().isBlank())
                    ? aggregateTypeObj.toString() : DEFAULT_AGGREGATE_TYPE;
            String aggregateId = event.get("aggregate_id").toString();

            try {
                if (db.isBlocked(aggregateType, aggregateId)) {
                    try {
                        boolean parkedNew = db.parkEvent(aggregateType, aggregateId, event,
                                record.topic(), record.partition(), record.offset());
                        metrics.recordOutcome("parked");
                        if (parkedNew) {
                            log.info("Parked follower event={} aggregate={}",
                                    event.get("event_id"), aggregateId);
                        }
                        return "parked";
                    } catch (Exception ex) {
                        log.error("Could not park event at offset={}; not committing", record.offset(), ex);
                        throw new OffsetRejectException(ex.getMessage(), ex);
                    }
                }
            } catch (OffsetRejectException e) {
                throw e;
            } catch (Exception e) {
                log.error("Error checking block for offset={}", record.offset(), e);
                throw new OffsetRejectException("DB error during block check: " + e.getMessage(), e);
            }
        }

        // --- 3. Apply with retry logic for transient errors ---
        int attempts = 0;
        while (true) {
            try {
                String outcome = db.applyEvent(event);
                metrics.recordOutcome(outcome);
                log.info("Event {} outcome={} offset={}", event.get("event_id"), outcome, record.offset());
                return outcome;
            } catch (ValidationException | BusinessRuleException e) {
                // Permanent failure — DLQ immediately, no retry
                log.error("Permanent failure for offset={} class={}: {}", record.offset(), e.getClass().getSimpleName(), e.getMessage());
                String failureClass = (e instanceof BusinessRuleException be)
                        ? be.getFailureClass() : ((ValidationException) e).getFailureClass();
                return failEvent(raw, event, failureClass, e.getMessage(), attempts + 1, record);
            } catch (TransientException e) {
                attempts++;
                if (attempts >= settings.consumerMaxRetries) {
                    log.error("Transient failure exhausted for offset={}: {}", record.offset(), e.getMessage());
                    return failEvent(raw, event, TransientException.FAILURE_CLASS,
                            "transient error after " + attempts + " attempts: " + e.getMessage(),
                            attempts, record);
                }
                long backoffMs = (long)(settings.consumerRetryBackoffSeconds * Math.pow(2, attempts) * 1000)
                        + (long)(Math.random() * 200);
                log.warn("Transient error attempt={}/{} offset={}, retry in {}ms: {}",
                        attempts, settings.consumerMaxRetries, record.offset(), backoffMs, e.getMessage());
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new OffsetRejectException("Interrupted during retry backoff", ie);
                }
            } catch (Exception e) {
                // Unexpected SQL error — treat as transient
                attempts++;
                if (attempts >= settings.consumerMaxRetries) {
                    log.error("Unexpected error exhausted for offset={}: {}", record.offset(), e.getMessage());
                    return failEvent(raw, event, TransientException.FAILURE_CLASS,
                            "unexpected error after " + attempts + " attempts: " + e.getMessage(),
                            attempts, record);
                }
                long backoffMs = (long)(settings.consumerRetryBackoffSeconds * Math.pow(2, attempts) * 1000)
                        + (long)(Math.random() * 200);
                log.warn("Unexpected error attempt={}/{} offset={}, retry in {}ms: {}",
                        attempts, settings.consumerMaxRetries, record.offset(), backoffMs, e.getMessage());
                try { Thread.sleep(backoffMs); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new OffsetRejectException("Interrupted during retry backoff", ie);
                }
            }
        }
    }

    /**
     * DLQ + ledger + block aggregate.
     * Raises OffsetRejectException if the DLQ write fails.
     * Order: DLQ publish FIRST, then ledger + block atomically.
     */
    @SuppressWarnings("unchecked")
    private String failEvent(byte[] raw, Map<String, Object> event,
                              String failureClass, String failureReason,
                              int attemptCount, ConsumerRecord<byte[], byte[]> record) {
        DlqPublisher.DlqMeta dlqMeta;
        try {
            Long sourceTs = record.timestamp() >= 0 ? record.timestamp() : null;
            dlqMeta = dlq.publishToDlq(
                    raw, event, failureClass, failureReason, attemptCount,
                    record.topic(), record.partition(), record.offset(), sourceTs);
        } catch (Exception e) {
            log.error("DLQ publish failed for offset={} — refusing to commit: {}", record.offset(), e.getMessage());
            throw new OffsetRejectException(e.getMessage(), e);
        }

        Map<String, Object> ev = (event != null) ? event : new java.util.LinkedHashMap<>();
        String aggregateId = ev.get("aggregate_id") != null ? ev.get("aggregate_id").toString() : null;
        Object aggregateTypeObj = ev.get("aggregate_type");
        String aggregateType = (aggregateTypeObj != null && !aggregateTypeObj.toString().isBlank())
                ? aggregateTypeObj.toString() : DEFAULT_AGGREGATE_TYPE;
        String eventId = ev.get("event_id") != null ? ev.get("event_id").toString() : null;
        String eventType = ev.get("event_type") != null ? ev.get("event_type").toString() : null;
        String rawText = new String(raw, StandardCharsets.UTF_8);

        try {
            db.recordFailureAndBlock(
                    eventId, aggregateId, aggregateType, eventType,
                    failureClass, failureReason, rawText,
                    record.topic(), record.partition(), record.offset(),
                    attemptCount,
                    dlqMeta.dlqTopic(), dlqMeta.dlqPartition(), dlqMeta.dlqOffset());
        } catch (Exception e) {
            // The DLQ was written but the DB failed — log critically but we
            // still committed the DLQ offset, so we cannot safely discard.
            // Raise OffsetRejectException so caller doesn't commit source offset.
            log.error("DB write (record_failure) failed after DLQ publish offset={}: {}", record.offset(), e.getMessage());
            throw new OffsetRejectException("DB failure after DLQ publish: " + e.getMessage(), e);
        }

        if (aggregateId != null) {
            log.error("Aggregate blocked type={} id={} reason={}", aggregateType, aggregateId, failureReason);
        } else {
            log.error("Poison event has no identifiable aggregate — no block set");
        }

        metrics.recordOutcome("failed");
        metrics.recordFailureClass(failureClass);
        return "failed";
    }
}
