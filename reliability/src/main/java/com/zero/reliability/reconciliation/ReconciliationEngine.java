package com.zero.reliability.reconciliation;

import com.zero.reliability.config.ReliabilitySettings;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class ReconciliationEngine {

    private final ReliabilitySettings settings;

    public ReconciliationEngine(ReliabilitySettings settings) {
        this.settings = settings;
    }

    public ReconciliationReport reconcile(
            String experimentId,
            List<String> expectedOrderIds,
            List<String> expectedOutboxEventIds) throws SQLException {

        try (Connection conn = DriverManager.getConnection(
                settings.jdbcUrl(), settings.postgresUser, settings.postgresPassword)) {

            UUID[] orderUuids = expectedOrderIds.stream().map(UUID::fromString).toArray(UUID[]::new);
            Array orderUuidArray = conn.createArrayOf("uuid", orderUuids);

            // 1. Check orders
            Set<String> foundOrderIds = new HashSet<>();
            Instant firstOrderCreatedAt = null;
            Instant lastOrderCreatedAt = null;

            String orderSql = "SELECT id, status, created_at FROM orders WHERE id = ANY(?)";
            try (PreparedStatement ps = conn.prepareStatement(orderSql)) {
                ps.setArray(1, orderUuidArray);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String id = rs.getString("id");
                        foundOrderIds.add(id);
                        Timestamp ts = rs.getTimestamp("created_at");
                        if (ts != null) {
                            Instant inst = ts.toInstant();
                            if (firstOrderCreatedAt == null || inst.isBefore(firstOrderCreatedAt)) {
                                firstOrderCreatedAt = inst;
                            }
                            if (lastOrderCreatedAt == null || inst.isAfter(lastOrderCreatedAt)) {
                                lastOrderCreatedAt = inst;
                            }
                        }
                    }
                }
            }

            List<String> missingOrderIds = new ArrayList<>();
            for (String expectedId : expectedOrderIds) {
                if (!foundOrderIds.contains(expectedId)) {
                    missingOrderIds.add(expectedId);
                }
            }

            // 2. Check outbox
            Set<String> foundOutboxIds = new HashSet<>();
            Map<String, Integer> outboxStatusBreakdown = new LinkedHashMap<>();
            List<String> unpublishedOutboxIds = new ArrayList<>();
            List<String> failedOutboxIds = new ArrayList<>();
            Instant firstPublishedAt = null;
            Instant lastPublishedAt = null;

            String outboxSql = "SELECT id, aggregate_id, status, created_at, published_at FROM outbox WHERE aggregate_id = ANY(?)";
            try (PreparedStatement ps = conn.prepareStatement(outboxSql)) {
                ps.setArray(1, orderUuidArray);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String outboxId = rs.getString("id");
                        String status = rs.getString("status");
                        foundOutboxIds.add(outboxId);
                        outboxStatusBreakdown.put(status, outboxStatusBreakdown.getOrDefault(status, 0) + 1);

                        if (!"PUBLISHED".equalsIgnoreCase(status)) {
                            unpublishedOutboxIds.add(outboxId);
                        }
                        if ("FAILED".equalsIgnoreCase(status)) {
                            failedOutboxIds.add(outboxId);
                        }

                        Timestamp pubTs = rs.getTimestamp("published_at");
                        if (pubTs != null) {
                            Instant inst = pubTs.toInstant();
                            if (firstPublishedAt == null || inst.isBefore(firstPublishedAt)) {
                                firstPublishedAt = inst;
                            }
                            if (lastPublishedAt == null || inst.isAfter(lastPublishedAt)) {
                                lastPublishedAt = inst;
                            }
                        }
                    }
                }
            }

            List<String> targetOutboxIds = (expectedOutboxEventIds != null && !expectedOutboxEventIds.isEmpty())
                    ? expectedOutboxEventIds
                    : new ArrayList<>(foundOutboxIds);

            List<String> missingOutboxIds = new ArrayList<>();
            for (String expectedEventId : targetOutboxIds) {
                if (!foundOutboxIds.contains(expectedEventId)) {
                    missingOutboxIds.add(expectedEventId);
                }
            }

            UUID[] outboxUuids = foundOutboxIds.stream().map(UUID::fromString).toArray(UUID[]::new);
            Array outboxUuidArray = conn.createArrayOf("uuid", outboxUuids);

            // 3. Check processed_events
            Set<String> foundProcessedEventIds = new HashSet<>();
            Instant firstProcessedAt = null;
            Instant lastProcessedAt = null;
            int duplicateProcessedCount = 0;

            if (outboxUuids.length > 0) {
                String processedSql = "SELECT event_id, processed_at FROM processed_events WHERE event_id = ANY(?)";
                try (PreparedStatement ps = conn.prepareStatement(processedSql)) {
                    ps.setArray(1, outboxUuidArray);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            String evId = rs.getString("event_id");
                            if (!foundProcessedEventIds.add(evId)) {
                                duplicateProcessedCount++;
                            }
                            Timestamp procTs = rs.getTimestamp("processed_at");
                            if (procTs != null) {
                                Instant inst = procTs.toInstant();
                                if (firstProcessedAt == null || inst.isBefore(firstProcessedAt)) {
                                    firstProcessedAt = inst;
                                }
                                if (lastProcessedAt == null || inst.isAfter(lastProcessedAt)) {
                                    lastProcessedAt = inst;
                                }
                            }
                        }
                    }
                }
            }

            List<String> missingProcessedEventIds = new ArrayList<>();
            for (String expectedEventId : targetOutboxIds) {
                if (!foundProcessedEventIds.contains(expectedEventId)) {
                    missingProcessedEventIds.add(expectedEventId);
                }
            }

            // 4. Check payments
            Set<String> foundPaymentOrderIds = new HashSet<>();
            Map<String, Integer> paymentStatusBreakdown = new LinkedHashMap<>();
            int paymentsCount = 0;

            String paymentSql = "SELECT id, order_id, status FROM payments WHERE order_id = ANY(?)";
            try (PreparedStatement ps = conn.prepareStatement(paymentSql)) {
                ps.setArray(1, orderUuidArray);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        paymentsCount++;
                        String oId = rs.getString("order_id");
                        foundPaymentOrderIds.add(oId);
                        String st = rs.getString("status");
                        paymentStatusBreakdown.put(st, paymentStatusBreakdown.getOrDefault(st, 0) + 1);
                    }
                }
            }

            List<String> missingPaymentOrderIds = new ArrayList<>();
            for (String oId : expectedOrderIds) {
                if (!foundPaymentOrderIds.contains(oId)) {
                    missingPaymentOrderIds.add(oId);
                }
            }

            // 5. Check event_failures (DLQ)
            List<String> eventFailureIds = new ArrayList<>();
            if (outboxUuids.length > 0) {
                String dlqSql = "SELECT event_id FROM event_failures WHERE event_id = ANY(?) OR aggregate_id = ANY(?)";
                try (PreparedStatement ps = conn.prepareStatement(dlqSql)) {
                    ps.setArray(1, outboxUuidArray);
                    ps.setArray(2, orderUuidArray);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            eventFailureIds.add(rs.getString("event_id"));
                        }
                    }
                }
            }

            // 6. Check parked_events
            List<String> parkedEventIds = new ArrayList<>();
            if (outboxUuids.length > 0) {
                String parkedSql = "SELECT event_id FROM parked_events WHERE event_id = ANY(?) OR aggregate_id = ANY(?)";
                try (PreparedStatement ps = conn.prepareStatement(parkedSql)) {
                    ps.setArray(1, outboxUuidArray);
                    ps.setArray(2, orderUuidArray);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            parkedEventIds.add(rs.getString("event_id"));
                        }
                    }
                }
            }

            boolean clean = missingOrderIds.isEmpty()
                    && missingOutboxIds.isEmpty()
                    && unpublishedOutboxIds.isEmpty()
                    && failedOutboxIds.isEmpty()
                    && missingProcessedEventIds.isEmpty()
                    && duplicateProcessedCount == 0
                    && missingPaymentOrderIds.isEmpty()
                    && eventFailureIds.isEmpty()
                    && parkedEventIds.isEmpty();

            StringBuilder summary = new StringBuilder();
            if (clean) {
                summary.append(String.format(
                        "RECONCILIATION CLEAN: Exactly %d orders matched across orders, outbox, processed_events, and payments with 0 discrepancies.",
                        expectedOrderIds.size()));
            } else {
                summary.append(String.format(
                        "RECONCILIATION DISCREPANCY: missingOrders=%d, missingOutbox=%d, unpublishedOutbox=%d, missingProcessed=%d, missingPayments=%d, dlq=%d, parked=%d",
                        missingOrderIds.size(), missingOutboxIds.size(), unpublishedOutboxIds.size(),
                        missingProcessedEventIds.size(), missingPaymentOrderIds.size(),
                        eventFailureIds.size(), parkedEventIds.size()));
            }

            return new ReconciliationReport(
                    experimentId,
                    Instant.now().toString(),
                    clean,
                    expectedOrderIds.size(),
                    foundOrderIds.size(),
                    missingOrderIds,
                    targetOutboxIds.size(),
                    foundOutboxIds.size(),
                    missingOutboxIds,
                    outboxStatusBreakdown,
                    unpublishedOutboxIds,
                    failedOutboxIds,
                    targetOutboxIds.size(),
                    foundProcessedEventIds.size(),
                    missingProcessedEventIds,
                    duplicateProcessedCount,
                    paymentsCount,
                    paymentStatusBreakdown,
                    missingPaymentOrderIds,
                    eventFailureIds.size(),
                    eventFailureIds,
                    parkedEventIds.size(),
                    parkedEventIds,
                    firstOrderCreatedAt != null ? firstOrderCreatedAt.toString() : null,
                    lastOrderCreatedAt != null ? lastOrderCreatedAt.toString() : null,
                    firstPublishedAt != null ? firstPublishedAt.toString() : null,
                    lastPublishedAt != null ? lastPublishedAt.toString() : null,
                    firstProcessedAt != null ? firstProcessedAt.toString() : null,
                    lastProcessedAt != null ? lastProcessedAt.toString() : null,
                    summary.toString()
            );
        }
    }
}
