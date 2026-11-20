package com.zero.consumer.repository;

import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


import com.zero.consumer.exception.ValidationException;
import com.zero.consumer.exception.TransientException;
import com.zero.consumer.service.EventValidator;
/**
 * All database operations for the consumer, backed by Spring's {@link JdbcTemplate}
 * (HikariCP pool under the hood). Keeps the exact SQL of the original
 * hand-rolled client.
 *
 * Atomic multi-statement blocks are real Spring transactions:
 *   - {@link #applyEvent(Map)} = payments INSERT + processed_events INSERT in one tx
 *   - {@link #recordFailureAndBlock(...)} = event_failures INSERT + aggregate block in one tx
 *   - dashboard resolve/discard runs inside FailureResolutionService's @Transactional block
 */
@Repository
public class DatabaseClient {

    private static final Logger log = LoggerFactory.getLogger(DatabaseClient.class);
    public static final String CONSUMER_NAME = "payment-consumer";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final JdbcTemplate jdbc;

    public DatabaseClient(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // -------------------- aggregate blocks --------------------

    public boolean isBlocked(String aggregateType, String aggregateId) {
        Boolean blocked = jdbc.query(
                "SELECT is_blocked FROM aggregate_processing_state " +
                "WHERE aggregate_type = ? AND aggregate_id = ?",
                ps -> {
                    ps.setString(1, aggregateType);
                    ps.setString(2, aggregateId);
                },
                rs -> { return rs.next() && rs.getBoolean("is_blocked"); });
        return Boolean.TRUE.equals(blocked);
    }

    private static boolean isValidUuid(String s) {
        if (s == null || s.isBlank()) return false;
        try {
            java.util.UUID.fromString(s);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public void blockAggregate(String aggregateType, String aggregateId,
                               String eventId, String reason) {
        if (!isValidUuid(aggregateId)) {
            log.warn("Cannot block aggregate with invalid UUID: {}", aggregateId);
            return;
        }
        jdbc.update("""
                INSERT INTO aggregate_processing_state
                    (aggregate_type, aggregate_id, is_blocked, blocked_event_id,
                     blocked_reason, blocked_at, blocked_by_consumer, updated_at)
                VALUES (?, ?, TRUE, ?, ?, NOW(), ?, NOW())
                ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE SET
                    is_blocked = TRUE,
                    blocked_event_id = EXCLUDED.blocked_event_id,
                    blocked_reason = EXCLUDED.blocked_reason,
                    blocked_at = NOW(),
                    blocked_by_consumer = EXCLUDED.blocked_by_consumer,
                    updated_at = NOW()
                """,
                aggregateType,
                aggregateId,
                isValidUuid(eventId) ? eventId : null,
                truncate(reason, 2000),
                CONSUMER_NAME);
    }

    // -------------------- parked events --------------------

    /**
     * Park a follower event for a blocked aggregate.
     * Returns true if newly parked, false if already parked (idempotent).
     */
    public boolean parkEvent(String aggregateType, String aggregateId,
                             Map<String, Object> event, String sourceTopic,
                             int sourcePartition, long kafkaOffset) {
        Boolean inserted = jdbc.query("""
                INSERT INTO parked_events
                    (consumer_name, aggregate_type, aggregate_id, event_id,
                     event_type, event, source_topic, source_partition, kafka_offset)
                VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?)
                ON CONFLICT (consumer_name, event_id) DO NOTHING
                RETURNING id
                """,
                ps -> {
                    ps.setString(1, CONSUMER_NAME);
                    ps.setString(2, aggregateType);
                    ps.setString(3, aggregateId);
                    ps.setString(4, str(event.get("event_id")));
                    ps.setString(5, str(event.getOrDefault("event_type", "Unknown")));
                    ps.setString(6, toJson(event));
                    ps.setString(7, sourceTopic);
                    ps.setInt(8, sourcePartition);
                    ps.setLong(9, kafkaOffset);
                },
                rs -> { return rs.next(); });
        return Boolean.TRUE.equals(inserted);
    }

    // -------------------- apply event (atomic: payments + processed_events) --------------------

    /**
     * Apply one event in a single transaction. Returns "applied" or "duplicate".
     * Throws ValidationException (permanent) or TransientException (retryable).
     */
    @Transactional
    public String applyEvent(Map<String, Object> event) {
        EventValidator.NormalizedEvent norm = EventValidator.validate(event);
        String eventId = norm.eventId();

        try {
            // Dedup check inside transaction
            if (alreadyProcessed(eventId)) {
                return "duplicate";
            }

            jdbc.update("""
                    INSERT INTO payments (order_id, idempotency_key, amount, currency, status)
                    VALUES (?, ?, ?, ?, 'INITIATED')
                    """,
                    norm.orderId(),
                    eventId,
                    BigDecimal.valueOf(norm.amount()),
                    norm.currency());

            jdbc.update("""
                    INSERT INTO processed_events (event_id, consumer_name, aggregate_id)
                    VALUES (?, ?, ?)
                    """,
                    eventId, CONSUMER_NAME, norm.aggregateId());

            return "applied";
        } catch (DataIntegrityViolationException e) {
            // Integrity constraint (FK violation, duplicate key, etc.)
            // Not fixable by retrying — permanent
            String reason = truncate(mostSpecificMessage(e), 500);
            throw new ValidationException("integrity error applying event: " + reason, e);
        } catch (DataAccessException e) {
            // Connection / operational error — transient
            throw new TransientException(truncate(mostSpecificMessage(e), 500), e);
        }
    }

    private boolean alreadyProcessed(String eventId) {
        Boolean found = jdbc.query(
                "SELECT 1 FROM processed_events WHERE event_id = ?",
                ps -> ps.setString(1, eventId),
                ResultSet::next);
        return Boolean.TRUE.equals(found);
    }

    // -------------------- failure ledger --------------------

    /**
     * Record a DLQ failure (insert into event_failures).
     * Returns a map with the inserted row data for further use.
     */
    public Map<String, Object> recordFailure(
            String eventId, String aggregateId, String eventType,
            String failureClass, String failureReason, String rawPayload,
            String sourceTopic, int sourcePartition, long sourceOffset,
            int attemptCount, String dlqTopic, Integer dlqPartition, Long dlqOffset
    ) {
        List<Map<String, Object>> rows = queryList("""
                INSERT INTO event_failures
                    (consumer_name, event_id, aggregate_id, event_type,
                     failure_class, failure_reason, raw_payload,
                     source_topic, source_partition, source_offset, attempt_count,
                     dlq_topic, dlq_partition, dlq_offset)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING *
                """,
                CONSUMER_NAME,
                isValidUuid(eventId) ? eventId : null,
                isValidUuid(aggregateId) ? aggregateId : null,
                eventType,
                failureClass,
                truncate(failureReason, 2000),
                truncate(rawPayload, 10_000),
                sourceTopic,
                sourcePartition,
                sourceOffset,
                attemptCount,
                dlqTopic,
                dlqPartition,
                dlqOffset);
        return rows.isEmpty() ? new LinkedHashMap<>() : rows.get(0);
    }

    // -------------------- dashboard queries --------------------

    public List<Map<String, Object>> listFailures(String status, int limit) {
        String sql = status != null
                ? "SELECT * FROM event_failures WHERE status = ? ORDER BY created_at DESC LIMIT ?"
                : "SELECT * FROM event_failures ORDER BY created_at DESC LIMIT ?";
        return status != null
                ? queryList(sql, status, limit)
                : queryList(sql, limit);
    }

    public List<Map<String, Object>> listBlocked() {
        return queryList("""
                SELECT s.*,
                       (SELECT count(*) FROM parked_events p
                         WHERE p.aggregate_type = s.aggregate_type
                           AND p.aggregate_id = s.aggregate_id
                           AND p.status = 'PARKED') AS parked_count
                FROM aggregate_processing_state s
                WHERE s.is_blocked = TRUE
                ORDER BY s.blocked_at DESC NULLS LAST
                """);
    }

    public List<Map<String, Object>> listParkedRecent(int limit) {
        return queryList("""
                SELECT * FROM parked_events
                WHERE status = 'PARKED'
                ORDER BY parked_at DESC
                LIMIT ?
                """, limit);
    }

    public List<Map<String, Object>> listParked(String aggregateType, String aggregateId) {
        return queryList("""
                SELECT * FROM parked_events
                WHERE aggregate_type = ? AND aggregate_id = ? AND status = 'PARKED'
                ORDER BY kafka_offset, parked_at
                """, aggregateType, aggregateId);
    }

    public List<Map<String, Object>> listRecentPayments(int limit) {
        return queryList("""
                SELECT p.id, p.order_id, o.order_number, o.customer_name, o.customer_email,
                       p.amount, p.currency, p.status, p.idempotency_key,
                       p.created_at AS applied_at
                FROM payments p
                JOIN orders o ON o.id = p.order_id
                ORDER BY p.created_at DESC
                LIMIT ?
                """, limit);
    }

    public List<Map<String, Object>> listRecentProcessed(int limit) {
        return queryList("""
                SELECT event_id, aggregate_id, consumer_name, processed_at
                FROM processed_events
                ORDER BY processed_at DESC
                LIMIT ?
                """, limit);
    }

    public long blockedCount() {
        Long n = jdbc.queryForObject(
                "SELECT count(*) AS n FROM aggregate_processing_state WHERE is_blocked", Long.class);
        return n != null ? n : 0;
    }

    public long parkedCount() {
        Long n = jdbc.queryForObject(
                "SELECT count(*) AS n FROM parked_events WHERE status = 'PARKED'", Long.class);
        return n != null ? n : 0;
    }

    public long openFailureCount() {
        Long n = jdbc.queryForObject(
                "SELECT count(*) AS n FROM event_failures WHERE status = 'OPEN'", Long.class);
        return n != null ? n : 0;
    }

    public Map<String, Long> outboxStatusCounts() {
        return statusCounts("SELECT status, count(*) AS n FROM outbox GROUP BY status");
    }

    public Map<String, Long> paymentStatusCounts() {
        return statusCounts("SELECT status, count(*) AS n FROM payments GROUP BY status");
    }

    public void ping() {
        jdbc.queryForObject("SELECT 1", Integer.class);
    }

    // -------------------- resolve / discard (dashboard write ops) --------------------

    /**
     * Get a failure row by ID, optionally with FOR UPDATE.
     * Must be called inside a transaction when {@code forUpdate} is true.
     */
    public Map<String, Object> getFailure(String failureId, boolean forUpdate) {
        String sql = "SELECT * FROM event_failures WHERE id = ?" + (forUpdate ? " FOR UPDATE" : "");
        List<Map<String, Object>> rows = jdbc.query(
                sql,
                ps -> ps.setObject(1, java.util.UUID.fromString(failureId)),
                (rs, rowNum) -> rowToMap(rs));
        return rows.isEmpty() ? null : rows.get(0);
    }

    public void updateFailure(String failureId, String status, String resolvedBy, String note) {
        jdbc.update("""
                UPDATE event_failures
                SET status = ?, resolved_at = NOW(), resolved_by = ?, resolution_note = ?
                WHERE id = ?
                """,
                status,
                resolvedBy,
                note != null ? truncate(note, 2000) : "",
                java.util.UUID.fromString(failureId));
    }

    public void unblockAggregate(String aggregateType, String aggregateId,
                                 String action, String resolvedBy, String note) {
        jdbc.update("""
                UPDATE aggregate_processing_state
                SET is_blocked = FALSE,
                    resolved_at = NOW(),
                    resolved_by = ?,
                    resolution_note = ?,
                    resolution_action = ?,
                    updated_at = NOW()
                WHERE aggregate_type = ? AND aggregate_id = ?
                """,
                resolvedBy,
                note != null ? truncate(note, 2000) : "",
                action,
                aggregateType,
                aggregateId);
    }

    public void markParked(Object parkedId, String status) {
        jdbc.update("UPDATE parked_events SET status = ? WHERE id = ?", status, parkedId);
    }

    // -------------------- record failure + block aggregate atomically --------------------

    /**
     * DLQ ledger row + aggregate block in one transaction.
     * Called from Pipeline after a successful DLQ publish.
     * Throws DataAccessException on failure (caller maps it to OffsetRejectException).
     */
    @Transactional
    public void recordFailureAndBlock(
            String eventId, String aggregateId, String aggregateType, String eventType,
            String failureClass, String failureReason, String rawPayload,
            String sourceTopic, int sourcePartition, long sourceOffset,
            int attemptCount, String dlqTopic, Integer dlqPartition, Long dlqOffset
    ) {
        recordFailure(eventId, aggregateId, eventType, failureClass, failureReason,
                rawPayload, sourceTopic, sourcePartition, sourceOffset,
                attemptCount, dlqTopic, dlqPartition, dlqOffset);

        if (aggregateId != null) {
            blockAggregate(aggregateType, aggregateId, eventId, failureReason);
        }
    }

    // -------------------- helpers --------------------

    private List<Map<String, Object>> queryList(String sql, Object... args) {
        return jdbc.query(sql, (rs, rowNum) -> rowToMap(rs), args);
    }

    private Map<String, Long> statusCounts(String sql) {
        Map<String, Long> result = new LinkedHashMap<>();
        for (Map<String, Object> row : jdbc.queryForList(sql)) {
            result.put((String) row.get("status"), ((Number) row.get("n")).longValue());
        }
        return result;
    }

    private static String mostSpecificMessage(DataAccessException e) {
        Throwable cause = e.getMostSpecificCause();
        return cause != null && cause.getMessage() != null ? cause.getMessage() : e.getMessage();
    }

    private static String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max);
    }

    private static String str(Object o) {
        return o == null ? "" : o.toString();
    }

    private String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            return "{}";
        }
    }

    private static Map<String, Object> rowToMap(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            Object val = rs.getObject(i);
            // Convert Timestamps to ISO string for JSON serialization
            if (val instanceof java.sql.Timestamp ts) {
                row.put(meta.getColumnName(i), ts.toInstant().toString());
            } else if (val instanceof java.sql.Date d) {
                row.put(meta.getColumnName(i), d.toString());
            } else if (val instanceof org.postgresql.util.PGobject pgo) {
                if ("json".equalsIgnoreCase(pgo.getType()) || "jsonb".equalsIgnoreCase(pgo.getType())) {
                    try {
                        row.put(meta.getColumnName(i), MAPPER.readValue(pgo.getValue(), Object.class));
                    } catch (Exception e) {
                        row.put(meta.getColumnName(i), pgo.getValue());
                    }
                } else {
                    row.put(meta.getColumnName(i), pgo.getValue());
                }
            } else {
                row.put(meta.getColumnName(i), val);
            }
        }
        return row;
    }
}
