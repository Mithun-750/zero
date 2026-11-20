package com.zero.producer.repository;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC access to the outbox table. Uses Spring's {@link JdbcTemplate}
 * (HikariCP connection pool under the hood).
 * Keeps the exact SQL of the original hand-rolled client.
 */
@Repository
public class DatabaseClient {

    private static final Logger log = LoggerFactory.getLogger(DatabaseClient.class);

    private static final String SQL_FETCH =
            "SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at " +
            "FROM outbox " +
            "WHERE status = 'PENDING' " +
            "ORDER BY created_at, id " +
            "LIMIT ?";

    private static final String SQL_MARK_PUBLISHED =
            "UPDATE outbox " +
            "SET status = 'PUBLISHED', " +
            "    published_at = CURRENT_TIMESTAMP, " +
            "    error_message = NULL " +
            "WHERE id = ?::uuid AND status = 'PENDING'";

    private static final String SQL_RECORD_FAILURE =
            "UPDATE outbox " +
            "SET retry_count = retry_count + 1, " +
            "    error_message = ? " +
            "WHERE id = ?::uuid AND status = 'PENDING' " +
            "RETURNING retry_count";

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public DatabaseClient(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    // ── public API ────────────────────────────────────────────────────────────

    /**
     * Fetch up to {@code batchSize} PENDING outbox rows ordered by created_at, id.
     * Each row is a {@code Map<String, Object>} with keys:
     * id, aggregate_type, aggregate_id, event_type, payload (as parsed Map), created_at (Timestamp).
     */
    public List<Map<String, Object>> fetchPendingEvents(int batchSize) {
        return jdbc.query(SQL_FETCH, ps -> ps.setInt(1, batchSize), this::mapRow);
    }

    /**
     * Mark a row as PUBLISHED. Throws if the row was not found / already published.
     */
    public void markPublished(String id) {
        int updateCount = jdbc.update(SQL_MARK_PUBLISHED, id);
        if (updateCount != 1) {
            throw new IllegalStateException(
                    "markPublished: expected 1 updated row for id=" + id +
                    " but got " + updateCount);
        }
    }

    /**
     * Increment retry_count and store error_message. Returns the new retry_count,
     * or -1 if the row was not found (already not PENDING).
     */
    public int recordPublishFailure(String id, String errorMessage) {
        // Truncate to 2000 chars as per spec
        String msg = errorMessage != null && errorMessage.length() > 2000
                ? errorMessage.substring(0, 2000)
                : errorMessage;

        Integer result = jdbc.query(SQL_RECORD_FAILURE, ps -> {
            ps.setString(1, msg);
            ps.setString(2, id);
        }, rs -> rs.next() ? rs.getInt("retry_count") : -1);

        return result == null ? -1 : result;
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
        String payloadJson = rs.getString("payload");
        Map<String, Object> payload;
        try {
            payload = objectMapper.readValue(
                    payloadJson, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            throw new SQLException("Failed to parse outbox payload as JSON: " + e.getMessage(), e);
        }

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id",             rs.getString("id"));
        row.put("aggregate_type", rs.getString("aggregate_type"));
        row.put("aggregate_id",   rs.getString("aggregate_id"));
        row.put("event_type",     rs.getString("event_type"));
        row.put("payload",        payload);
        row.put("created_at",     rs.getTimestamp("created_at"));
        return row;
    }
}
