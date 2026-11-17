package com.zero.producer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Plain-JDBC access to the outbox table.
 * Each method acquires and releases its own connection so the class itself is stateless
 * with respect to connections (autoCommit = true throughout).
 */
public final class DatabaseClient {

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

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final ObjectMapper objectMapper;

    public DatabaseClient(Settings settings, ObjectMapper objectMapper) {
        this.jdbcUrl      = settings.jdbcUrl();
        this.user         = settings.postgresUser;
        this.password     = settings.postgresPassword;
        this.objectMapper = objectMapper;
    }

    // ── public API ────────────────────────────────────────────────────────────

    /**
     * Fetch up to {@code batchSize} PENDING outbox rows ordered by created_at, id.
     * Each row is returned as a plain {@code Map<String, Object>} with keys:
     * id, aggregate_type, aggregate_id, event_type, payload (as parsed Map), created_at (Timestamp).
     */
    public List<Map<String, Object>> fetchPendingEvents(int batchSize) throws Exception {
        List<Map<String, Object>> rows = new ArrayList<>();
        try (Connection conn = openConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_FETCH)) {

            ps.setInt(1, batchSize);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String payloadJson = rs.getString("payload");
                    Map<String, Object> payload = objectMapper.readValue(
                        payloadJson, new TypeReference<Map<String, Object>>() {});

                    Map<String, Object> row = new java.util.LinkedHashMap<>();
                    row.put("id",             rs.getString("id"));
                    row.put("aggregate_type", rs.getString("aggregate_type"));
                    row.put("aggregate_id",   rs.getString("aggregate_id"));
                    row.put("event_type",     rs.getString("event_type"));
                    row.put("payload",        payload);
                    row.put("created_at",     rs.getTimestamp("created_at"));
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    /**
     * Mark a row as PUBLISHED.  Throws if the row was not found / already published.
     */
    public void markPublished(String id) throws Exception {
        try (Connection conn = openConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_MARK_PUBLISHED)) {

            ps.setString(1, id);
            int updateCount = ps.executeUpdate();
            if (updateCount != 1) {
                throw new RuntimeException(
                    "markPublished: expected 1 updated row for id=" + id +
                    " but got " + updateCount);
            }
        }
    }

    /**
     * Increment retry_count and store error_message.  Returns the new retry_count.
     */
    public int recordPublishFailure(String id, String errorMessage) throws Exception {
        // Truncate to 2000 chars as per spec
        String msg = errorMessage != null && errorMessage.length() > 2000
            ? errorMessage.substring(0, 2000)
            : errorMessage;

        try (Connection conn = openConnection();
             PreparedStatement ps = conn.prepareStatement(SQL_RECORD_FAILURE)) {

            ps.setString(1, msg);
            ps.setString(2, id);

            // RETURNING clause — use executeQuery()
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("retry_count");
                }
                // row not found (already not PENDING)
                return -1;
            }
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private Connection openConnection() throws SQLException {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("stringtype", "unspecified");
        Connection conn = DriverManager.getConnection(jdbcUrl, props);
        conn.setAutoCommit(true);
        return conn;
    }
}
