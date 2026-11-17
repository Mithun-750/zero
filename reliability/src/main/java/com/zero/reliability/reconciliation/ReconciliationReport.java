package com.zero.reliability.reconciliation;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public record ReconciliationReport(
        @JsonProperty("experimentId") String experimentId,
        @JsonProperty("timestamp") String timestamp,
        @JsonProperty("clean") boolean clean,
        @JsonProperty("totalExpectedOrders") int totalExpectedOrders,
        @JsonProperty("foundOrdersCount") int foundOrdersCount,
        @JsonProperty("missingOrderIds") List<String> missingOrderIds,
        @JsonProperty("totalExpectedOutbox") int totalExpectedOutbox,
        @JsonProperty("foundOutboxCount") int foundOutboxCount,
        @JsonProperty("missingOutboxIds") List<String> missingOutboxIds,
        @JsonProperty("outboxStatusBreakdown") Map<String, Integer> outboxStatusBreakdown,
        @JsonProperty("unpublishedOutboxIds") List<String> unpublishedOutboxIds,
        @JsonProperty("failedOutboxIds") List<String> failedOutboxIds,
        @JsonProperty("totalExpectedProcessedEvents") int totalExpectedProcessedEvents,
        @JsonProperty("foundProcessedEventsCount") int foundProcessedEventsCount,
        @JsonProperty("missingProcessedEventIds") List<String> missingProcessedEventIds,
        @JsonProperty("duplicateProcessedCount") int duplicateProcessedCount,
        @JsonProperty("paymentsCount") int paymentsCount,
        @JsonProperty("paymentStatusBreakdown") Map<String, Integer> paymentStatusBreakdown,
        @JsonProperty("missingPaymentOrderIds") List<String> missingPaymentOrderIds,
        @JsonProperty("eventFailuresCount") int eventFailuresCount,
        @JsonProperty("eventFailureIds") List<String> eventFailureIds,
        @JsonProperty("parkedEventsCount") int parkedEventsCount,
        @JsonProperty("parkedEventIds") List<String> parkedEventIds,
        @JsonProperty("firstOrderCreatedAt") String firstOrderCreatedAt,
        @JsonProperty("lastOrderCreatedAt") String lastOrderCreatedAt,
        @JsonProperty("firstOutboxPublishedAt") String firstOutboxPublishedAt,
        @JsonProperty("lastOutboxPublishedAt") String lastOutboxPublishedAt,
        @JsonProperty("firstProcessedAt") String firstProcessedAt,
        @JsonProperty("lastProcessedAt") String lastProcessedAt,
        @JsonProperty("summary") String summary
) {}
