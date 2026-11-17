package com.zero.reliability.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record ExperimentManifest(
        @JsonProperty("experimentId") String experimentId,
        @JsonProperty("createdAt") String createdAt,
        @JsonProperty("description") String description,
        @JsonProperty("targetOrderCount") int targetOrderCount,
        @JsonProperty("orderIds") List<String> orderIds,
        @JsonProperty("outboxEventIds") List<String> outboxEventIds,
        @JsonProperty("initialDatabaseSnapshot") DatabaseStateSnapshot initialDatabaseSnapshot,
        @JsonProperty("initialKafkaSnapshot") KafkaClusterSnapshot initialKafkaSnapshot
) {}
