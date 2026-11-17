package com.zero.reliability;

import com.zero.reliability.reconciliation.ReconciliationReport;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReconciliationReportTest {

    @Test
    void testCleanReport() {
        ReconciliationReport report = new ReconciliationReport(
                "TEST-EXP-001",
                Instant.now().toString(),
                true,
                100, 100, Collections.emptyList(),
                100, 100, Collections.emptyList(),
                Map.of("PUBLISHED", 100),
                Collections.emptyList(), Collections.emptyList(),
                100, 100, Collections.emptyList(),
                0,
                100, Map.of("INITIATED", 100),
                Collections.emptyList(),
                0, Collections.emptyList(),
                0, Collections.emptyList(),
                null, null, null, null, null, null,
                "RECONCILIATION CLEAN"
        );

        assertTrue(report.clean());
        assertTrue(report.missingOrderIds().isEmpty());
        assertTrue(report.missingOutboxIds().isEmpty());
        assertTrue(report.missingProcessedEventIds().isEmpty());
    }

    @Test
    void testDiscrepancyReport() {
        ReconciliationReport report = new ReconciliationReport(
                "TEST-EXP-002",
                Instant.now().toString(),
                false,
                100, 99, List.of("missing-order-uuid"),
                100, 99, List.of("missing-outbox-uuid"),
                Map.of("PUBLISHED", 98, "FAILED", 1),
                List.of("missing-outbox-uuid"), List.of("failed-outbox-uuid"),
                100, 98, List.of("missing-outbox-uuid", "failed-outbox-uuid"),
                0,
                98, Map.of("INITIATED", 98),
                List.of("missing-order-uuid", "missing-order-uuid-2"),
                1, List.of("failed-outbox-uuid"),
                0, Collections.emptyList(),
                null, null, null, null, null, null,
                "RECONCILIATION DISCREPANCY"
        );

        assertFalse(report.clean());
        assertFalse(report.missingOrderIds().isEmpty());
        assertFalse(report.missingOutboxIds().isEmpty());
        assertFalse(report.missingProcessedEventIds().isEmpty());
    }
}

