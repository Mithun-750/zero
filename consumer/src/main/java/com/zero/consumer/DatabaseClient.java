package com.zero.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * All JDBC database operations for the consumer.
 * Mirrors Python consumer/db.py, state.py, and applier.py.
 *
 * Connection is used with autoCommit=true for simple operations;
 * explicit transactions (setAutoCommit(false)) are used for atomic
 * multi-statement blocks (applyEvent = payments + processed_events).
 */
public class DatabaseClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DatabaseClient.class);
    public static final String CONSUMER_NAME = "payment-consumer";

    private final Connection conn;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final ObjectMapper mapper = MAPPER;

    public DatabaseClient(Settings settings) throws SQLException {
        String url = String.format("jdbc:postgresql://%s:%d/%s",
                settings.postgresHost, settings.postgresPort, settings.postgresDb);
        Properties props = new Properties();
        props.setProperty("user", settings.postgresUser);
        props.setProperty("password", settings.postgresPassword);
        props.setProperty("stringtype", "unspecified");
        this.conn = DriverManager.getConnection(url, props);
        this.conn.setAutoCommit(true);
    }

    // -------------------- aggregate blocks --------------------

    public boolean isBlocked(String aggregateType, String aggregateId) throws SQLException {
        String sql = "SELECT is_blocked FROM aggregate_processing_state " +
                     "WHERE aggregate_type = ? AND aggregate_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, aggregateType);
            ps.setString(2, aggregateId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getBoolean("is_blocked");
                }
                return false;
            }
        }
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
                               String eventId, String reason) throws SQLException {
        if (!isValidUuid(aggregateId)) {
            log.warn("Cannot block aggregate with invalid UUID: {}", aggregateId);
            return;
        }
        String sql = """
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
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, aggregateType);
            ps.setString(2, aggregateId);
            ps.setString(3, isValidUuid(eventId) ? eventId : null);
            ps.setString(4, truncate(reason, 2000));
            ps.setString(5, CONSUMER_NAME);
            ps.executeUpdate();
        }
    }

    // -------------------- parked events --------------------

    /**
     * Park a follower event for a blocked aggregate.
     * Returns true if newly parked, false if already parked (idempotent).
     */
    public boolean parkEvent(String aggregateType, String aggregateId,
                              Map<String, Object> event, String sourceTopic,
                              int sourcePartition, long kafkaOffset) throws SQLException {
        String sql = """
                INSERT INTO parked_events
                    (consumer_name, aggregate_type, aggregate_id, event_id,
                     event_type, event, source_topic, source_partition, kafka_offset)
                VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?)
                ON CONFLICT (consumer_name, event_id) DO NOTHING
                RETURNING id
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, CONSUMER_NAME);
            ps.setString(2, aggregateType);
            ps.setString(3, aggregateId);
            ps.setString(4, str(event.get("event_id")));
            ps.setString(5, str(event.getOrDefault("event_type", "Unknown")));
            ps.setString(6, toJson(event));
            ps.setString(7, sourceTopic);
            ps.setInt(8, sourcePartition);
            ps.setLong(9, kafkaOffset);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next(); // true = newly inserted, false = conflict (already parked)
            }
        }
    }

    // -------------------- apply event (atomic: payments + processed_events) --------------------

    /**
     * Apply one event. Returns "applied" or "duplicate".
     * Throws ValidationException (permanent) or TransientException (retryable).
     * This is the core atomic operation: payments INSERT + processed_events INSERT
     * in one transaction.
     */
    public String applyEvent(Map<String, Object> event) throws SQLException {
        EventValidator.NormalizedEvent norm = EventValidator.validate(event);
        String eventId = norm.eventId();

        conn.setAutoCommit(false);
        try {
            // Dedup check inside transaction
            if (alreadyProcessed(eventId)) {
                conn.commit();
                return "duplicate";
            }

            // Insert payment
            String paymentSql = """
                    INSERT INTO payments (order_id, idempotency_key, amount, currency, status)
                    VALUES (?, ?, ?, ?, 'INITIATED')
                    """;
            try (PreparedStatement ps = conn.prepareStatement(paymentSql)) {
                ps.setString(1, norm.orderId());
                ps.setString(2, eventId);
                ps.setBigDecimal(3, java.math.BigDecimal.valueOf(norm.amount()));
                ps.setString(4, norm.currency());
                ps.executeUpdate();
            }

            // Insert processed_events ledger
            String ledgerSql = """
                    INSERT INTO processed_events (event_id, consumer_name, aggregate_id)
                    VALUES (?, ?, ?)
                    """;
            try (PreparedStatement ps = conn.prepareStatement(ledgerSql)) {
                ps.setString(1, eventId);
                ps.setString(2, CONSUMER_NAME);
                ps.setString(3, norm.aggregateId());
                ps.executeUpdate();
            }

            conn.commit();
            return "applied";
        } catch (PSQLException e) {
            rollbackQuietly();
            String state = e.getSQLState();
            if (state != null && state.startsWith("23")) {
                // Integrity constraint (FK violation, duplicate key, etc.)
                // Not fixable by retrying — permanent
                String reason = truncate(e.getMessage(), 500);
                throw new ValidationException("integrity error applying event: " + reason, e);
            }
            // Connection / operational error — transient
            throw new TransientException(truncate(e.getMessage(), 500), e);
        } catch (SQLException e) {
            rollbackQuietly();
            throw new TransientException(truncate(e.getMessage(), 500), e);
        } finally {
            conn.setAutoCommit(true);
        }
    }

    private boolean alreadyProcessed(String eventId) throws SQLException {
        String sql = "SELECT 1 FROM processed_events WHERE event_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, eventId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
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
    ) throws SQLException {
        String sql = """
                INSERT INTO event_failures
                    (consumer_name, event_id, aggregate_id, event_type,
                     failure_class, failure_reason, raw_payload,
                     source_topic, source_partition, source_offset, attempt_count,
                     dlq_topic, dlq_partition, dlq_offset)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING *
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, CONSUMER_NAME);
            ps.setString(2, isValidUuid(eventId) ? eventId : null);
            ps.setString(3, isValidUuid(aggregateId) ? aggregateId : null);
            ps.setString(4, eventType);
            ps.setString(5, failureClass);
            ps.setString(6, truncate(failureReason, 2000));
            ps.setString(7, truncate(rawPayload, 10_000));
            ps.setString(8, sourceTopic);
            ps.setInt(9, sourcePartition);
            ps.setLong(10, sourceOffset);
            ps.setInt(11, attemptCount);
            ps.setString(12, dlqTopic);
            if (dlqPartition != null) ps.setInt(13, dlqPartition); else ps.setNull(13, Types.INTEGER);
            if (dlqOffset != null) ps.setLong(14, dlqOffset); else ps.setNull(14, Types.BIGINT);
            try (ResultSet rs = ps.executeQuery()) {
                return resultSetToMap(rs);
            }
        }
    }

    // -------------------- dashboard queries --------------------

    public List<Map<String, Object>> listFailures(String status, int limit) throws SQLException {
        String sql = status != null
                ? "SELECT * FROM event_failures WHERE status = ? ORDER BY created_at DESC LIMIT ?"
                : "SELECT * FROM event_failures ORDER BY created_at DESC LIMIT ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            if (status != null) {
                ps.setString(1, status);
                ps.setInt(2, limit);
            } else {
                ps.setInt(1, limit);
            }
            return resultSetToList(ps.executeQuery());
        }
    }

    public List<Map<String, Object>> listBlocked() throws SQLException {
        String sql = """
                SELECT s.*,
                       (SELECT count(*) FROM parked_events p
                         WHERE p.aggregate_type = s.aggregate_type
                           AND p.aggregate_id = s.aggregate_id
                           AND p.status = 'PARKED') AS parked_count
                FROM aggregate_processing_state s
                WHERE s.is_blocked = TRUE
                ORDER BY s.blocked_at DESC NULLS LAST
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            return resultSetToList(rs);
        }
    }

    public List<Map<String, Object>> listParkedRecent(int limit) throws SQLException {
        String sql = """
                SELECT * FROM parked_events
                WHERE status = 'PARKED'
                ORDER BY parked_at DESC
                LIMIT ?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, limit);
            return resultSetToList(ps.executeQuery());
        }
    }

    public List<Map<String, Object>> listParked(String aggregateType, String aggregateId) throws SQLException {
        String sql = """
                SELECT * FROM parked_events
                WHERE aggregate_type = ? AND aggregate_id = ? AND status = 'PARKED'
                ORDER BY kafka_offset, parked_at
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, aggregateType);
            ps.setString(2, aggregateId);
            return resultSetToList(ps.executeQuery());
        }
    }

    public List<Map<String, Object>> listRecentPayments(int limit) throws SQLException {
        String sql = """
                SELECT p.id, p.order_id, o.order_number, o.customer_name, o.customer_email,
                       p.amount, p.currency, p.status, p.idempotency_key,
                       p.created_at AS applied_at
                FROM payments p
                JOIN orders o ON o.id = p.order_id
                ORDER BY p.created_at DESC
                LIMIT ?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, limit);
            return resultSetToList(ps.executeQuery());
        }
    }

    public List<Map<String, Object>> listRecentProcessed(int limit) throws SQLException {
        String sql = """
                SELECT event_id, aggregate_id, consumer_name, processed_at
                FROM processed_events
                ORDER BY processed_at DESC
                LIMIT ?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, limit);
            return resultSetToList(ps.executeQuery());
        }
    }

    public long blockedCount() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT count(*) AS n FROM aggregate_processing_state WHERE is_blocked");
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong("n") : 0;
        }
    }

    public long parkedCount() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT count(*) AS n FROM parked_events WHERE status = 'PARKED'");
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong("n") : 0;
        }
    }

    public long openFailureCount() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT count(*) AS n FROM event_failures WHERE status = 'OPEN'");
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong("n") : 0;
        }
    }

    public Map<String, Long> outboxStatusCounts() throws SQLException {
        Map<String, Long> result = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT status, count(*) AS n FROM outbox GROUP BY status");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.put(rs.getString("status"), rs.getLong("n"));
            }
        }
        return result;
    }

    public Map<String, Long> paymentStatusCounts() throws SQLException {
        Map<String, Long> result = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT status, count(*) AS n FROM payments GROUP BY status");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.put(rs.getString("status"), rs.getLong("n"));
            }
        }
        return result;
    }

    public void ping() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT 1");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
        }
    }

    // -------------------- resolve / discard (dashboard write ops) --------------------

    /**
     * Get a failure row by ID, optionally with FOR UPDATE.
     */
    public Map<String, Object> getFailure(String failureId, boolean forUpdate) throws SQLException {
        String sql = "SELECT * FROM event_failures WHERE id = ?" + (forUpdate ? " FOR UPDATE" : "");
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, java.util.UUID.fromString(failureId));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rowToMap(rs);
                return null;
            }
        }
    }

    public void updateFailure(String failureId, String status, String resolvedBy, String note) throws SQLException {
        String sql = """
                UPDATE event_failures
                SET status = ?, resolved_at = NOW(), resolved_by = ?, resolution_note = ?
                WHERE id = ?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setString(2, resolvedBy);
            ps.setString(3, note != null ? truncate(note, 2000) : "");
            ps.setObject(4, java.util.UUID.fromString(failureId));
            ps.executeUpdate();
        }
    }

    public void unblockAggregate(String aggregateType, String aggregateId,
                                  String action, String resolvedBy, String note) throws SQLException {
        String sql = """
                UPDATE aggregate_processing_state
                SET is_blocked = FALSE,
                    resolved_at = NOW(),
                    resolved_by = ?,
                    resolution_note = ?,
                    resolution_action = ?,
                    updated_at = NOW()
                WHERE aggregate_type = ? AND aggregate_id = ?
                """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, resolvedBy);
            ps.setString(2, note != null ? truncate(note, 2000) : "");
            ps.setString(3, action);
            ps.setString(4, aggregateType);
            ps.setString(5, aggregateId);
            ps.executeUpdate();
        }
    }

    public void markParked(Object parkedId, String status) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE parked_events SET status = ? WHERE id = ?")) {
            ps.setString(1, status);
            ps.setObject(2, parkedId);
            ps.executeUpdate();
        }
    }

    // -------------------- transaction helpers (for dashboard resolve) --------------------

    public void beginTransaction() throws SQLException {
        conn.setAutoCommit(false);
    }

    public void commitTransaction() throws SQLException {
        conn.commit();
        conn.setAutoCommit(true);
    }

    public void rollbackTransaction() {
        try {
            conn.rollback();
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            log.warn("Rollback failed: {}", e.getMessage());
        }
    }

    // -------------------- record failure + block aggregate atomically --------------------

    /**
     * DLQ ledger row + aggregate block in one transaction.
     * Called from Pipeline after a successful DLQ publish.
     */
    public void recordFailureAndBlock(
            String eventId, String aggregateId, String aggregateType, String eventType,
            String failureClass, String failureReason, String rawPayload,
            String sourceTopic, int sourcePartition, long sourceOffset,
            int attemptCount, String dlqTopic, Integer dlqPartition, Long dlqOffset
    ) throws SQLException {
        conn.setAutoCommit(false);
        try {
            recordFailure(eventId, aggregateId, eventType, failureClass, failureReason,
                    rawPayload, sourceTopic, sourcePartition, sourceOffset,
                    attemptCount, dlqTopic, dlqPartition, dlqOffset);

            if (aggregateId != null) {
                blockAggregate(aggregateType, aggregateId, eventId, failureReason);
            }
            conn.commit();
        } catch (SQLException e) {
            rollbackQuietly();
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // -------------------- helpers --------------------

    private void rollbackQuietly() {
        try { conn.rollback(); } catch (Exception ignored) {}
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
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            return "{}";
        }
    }

    private static Map<String, Object> resultSetToMap(ResultSet rs) throws SQLException {
        if (rs.next()) return rowToMap(rs);
        return new LinkedHashMap<>();
    }

    private static List<Map<String, Object>> resultSetToList(ResultSet rs) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        while (rs.next()) {
            rows.add(rowToMap(rs));
        }
        return rows;
    }

    private static Map<String, Object> rowToMap(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            Object val = rs.getObject(i);
            // Convert Timestamps to ISO string for JSON serialization
            if (val instanceof Timestamp ts) {
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

    @Override
    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            log.warn("Error closing DB connection: {}", e.getMessage());
        }
    }
}
