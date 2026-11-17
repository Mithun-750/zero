package com.zero.reliability.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public record KafkaClusterSnapshot(
        @JsonProperty("timestamp") String timestamp,
        @JsonProperty("clusterId") String clusterId,
        @JsonProperty("controllerId") int controllerId,
        @JsonProperty("brokers") List<BrokerNodeInfo> brokers,
        @JsonProperty("topicPartitions") Map<String, List<PartitionInfo>> topicPartitions,
        @JsonProperty("consumerGroupOffsets") Map<String, Map<Integer, Long>> consumerGroupOffsets,
        @JsonProperty("consumerGroupLag") Map<String, Map<Integer, Long>> consumerGroupLag
) {
    public record BrokerNodeInfo(
            @JsonProperty("id") int id,
            @JsonProperty("host") String host,
            @JsonProperty("port") int port,
            @JsonProperty("rack") String rack
    ) {}

    public record PartitionInfo(
            @JsonProperty("partition") int partition,
            @JsonProperty("leader") int leader,
            @JsonProperty("replicas") List<Integer> replicas,
            @JsonProperty("isr") List<Integer> isr,
            @JsonProperty("beginningOffset") long beginningOffset,
            @JsonProperty("endOffset") long endOffset
    ) {}
}
