package com.zero.reliability.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public record DatabaseStateSnapshot(
        @JsonProperty("timestamp") String timestamp,
        @JsonProperty("totalOrders") long totalOrders,
        @JsonProperty("outboxTotal") long outboxTotal,
        @JsonProperty("outboxPending") long outboxPending,
        @JsonProperty("outboxProcessing") long outboxProcessing,
        @JsonProperty("outboxPublished") long outboxPublished,
        @JsonProperty("outboxFailed") long outboxFailed,
        @JsonProperty("processedEventsTotal") long processedEventsTotal,
        @JsonProperty("processedEventsByConsumer") Map<String, Long> processedEventsByConsumer,
        @JsonProperty("paymentsTotal") long paymentsTotal,
        @JsonProperty("paymentsByStatus") Map<String, Long> paymentsByStatus,
        @JsonProperty("eventFailuresTotal") long eventFailuresTotal,
        @JsonProperty("eventFailuresByStatus") Map<String, Long> eventFailuresByStatus,
        @JsonProperty("parkedEventsTotal") long parkedEventsTotal,
        @JsonProperty("parkedEventsByStatus") Map<String, Long> parkedEventsByStatus
) {}
