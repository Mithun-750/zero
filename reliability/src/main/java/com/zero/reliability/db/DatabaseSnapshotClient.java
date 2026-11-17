package com.zero.reliability.db;

import com.zero.reliability.config.ReliabilitySettings;
import com.zero.reliability.model.DatabaseStateSnapshot;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseSnapshotClient {

    private final ReliabilitySettings settings;

    public DatabaseSnapshotClient(ReliabilitySettings settings) {
        this.settings = settings;
    }

    public DatabaseStateSnapshot captureSnapshot() throws SQLException {
        try (Connection conn = DriverManager.getConnection(
                settings.jdbcUrl(), settings.postgresUser, settings.postgresPassword)) {

            long totalOrders = queryCount(conn, "SELECT count(*) FROM orders");

            long outboxTotal = queryCount(conn, "SELECT count(*) FROM outbox");
            long outboxPending = queryCount(conn, "SELECT count(*) FROM outbox WHERE status = 'PENDING'");
            long outboxProcessing = queryCount(conn, "SELECT count(*) FROM outbox WHERE status = 'PROCESSING'");
            long outboxPublished = queryCount(conn, "SELECT count(*) FROM outbox WHERE status = 'PUBLISHED'");
            long outboxFailed = queryCount(conn, "SELECT count(*) FROM outbox WHERE status = 'FAILED'");

            long processedEventsTotal = queryCount(conn, "SELECT count(*) FROM processed_events");
            Map<String, Long> processedEventsByConsumer = queryGroupCounts(
                    conn, "SELECT consumer_name, count(*) as c FROM processed_events GROUP BY consumer_name");

            long paymentsTotal = queryCount(conn, "SELECT count(*) FROM payments");
            Map<String, Long> paymentsByStatus = queryGroupCounts(
                    conn, "SELECT status, count(*) as c FROM payments GROUP BY status");

            long eventFailuresTotal = queryCount(conn, "SELECT count(*) FROM event_failures");
            Map<String, Long> eventFailuresByStatus = queryGroupCounts(
                    conn, "SELECT status, count(*) as c FROM event_failures GROUP BY status");

            long parkedEventsTotal = queryCount(conn, "SELECT count(*) FROM parked_events");
            Map<String, Long> parkedEventsByStatus = queryGroupCounts(
                    conn, "SELECT status, count(*) as c FROM parked_events GROUP BY status");

            return new DatabaseStateSnapshot(
                    Instant.now().toString(),
                    totalOrders,
                    outboxTotal,
                    outboxPending,
                    outboxProcessing,
                    outboxPublished,
                    outboxFailed,
                    processedEventsTotal,
                    processedEventsByConsumer,
                    paymentsTotal,
                    paymentsByStatus,
                    eventFailuresTotal,
                    eventFailuresByStatus,
                    parkedEventsTotal,
                    parkedEventsByStatus
            );
        }
    }

    private long queryCount(Connection conn, String sql) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        }
    }

    private Map<String, Long> queryGroupCounts(Connection conn, String sql) throws SQLException {
        Map<String, Long> map = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String key = rs.getString(1);
                long count = rs.getLong(2);
                map.put(key != null ? key : "NULL", count);
            }
        }
        return map;
    }
}
