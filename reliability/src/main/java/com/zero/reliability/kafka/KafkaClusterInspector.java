package com.zero.reliability.kafka;

import com.zero.reliability.config.ReliabilitySettings;
import com.zero.reliability.model.KafkaClusterSnapshot;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaClusterInspector implements AutoCloseable {

    private final ReliabilitySettings settings;
    private final AdminClient adminClient;

    public KafkaClusterInspector(ReliabilitySettings settings) {
        this.settings = settings;
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, settings.kafkaBootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "8000");
        this.adminClient = AdminClient.create(props);
    }

    public KafkaClusterSnapshot inspect() throws Exception {
        DescribeClusterResult clusterResult = adminClient.describeCluster();
        String clusterId = clusterResult.clusterId().get(5, TimeUnit.SECONDS);
        Node controllerNode = clusterResult.controller().get(5, TimeUnit.SECONDS);
        int controllerId = controllerNode != null ? controllerNode.id() : -1;

        try {
            org.apache.kafka.clients.admin.QuorumInfo quorumInfo = adminClient.describeMetadataQuorum().quorumInfo().get(5, TimeUnit.SECONDS);
            if (quorumInfo != null && quorumInfo.leaderId() >= 0) {
                controllerId = quorumInfo.leaderId();
            }
        } catch (Exception ignored) {
            // fallback to describeCluster controller
        }

        List<KafkaClusterSnapshot.BrokerNodeInfo> brokerInfos = new ArrayList<>();
        for (Node node : clusterResult.nodes().get(5, TimeUnit.SECONDS)) {
            brokerInfos.add(new KafkaClusterSnapshot.BrokerNodeInfo(
                    node.id(), node.host(), node.port(), node.rack()
            ));
        }

        List<String> topicsToInspect = List.of(settings.kafkaOrdersTopic, settings.kafkaDlqTopic);
        Map<String, TopicDescription> descriptions = adminClient.describeTopics(topicsToInspect)
                .allTopicNames().get(5, TimeUnit.SECONDS);

        List<TopicPartition> allPartitions = new ArrayList<>();
        for (TopicDescription td : descriptions.values()) {
            for (TopicPartitionInfo tpi : td.partitions()) {
                allPartitions.add(new TopicPartition(td.name(), tpi.partition()));
            }
        }

        Map<TopicPartition, OffsetSpec> earliestSpecs = new HashMap<>();
        Map<TopicPartition, OffsetSpec> latestSpecs = new HashMap<>();
        for (TopicPartition tp : allPartitions) {
            earliestSpecs.put(tp, OffsetSpec.earliest());
            latestSpecs.put(tp, OffsetSpec.latest());
        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets =
                adminClient.listOffsets(earliestSpecs).all().get(5, TimeUnit.SECONDS);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                adminClient.listOffsets(latestSpecs).all().get(5, TimeUnit.SECONDS);

        Map<String, List<KafkaClusterSnapshot.PartitionInfo>> topicPartitions = new LinkedHashMap<>();
        for (String topic : topicsToInspect) {
            TopicDescription td = descriptions.get(topic);
            List<KafkaClusterSnapshot.PartitionInfo> partitionList = new ArrayList<>();
            if (td != null) {
                for (TopicPartitionInfo tpi : td.partitions()) {
                    TopicPartition tp = new TopicPartition(topic, tpi.partition());
                    long beginningOffset = earliestOffsets.containsKey(tp) ? earliestOffsets.get(tp).offset() : 0L;
                    long endOffset = latestOffsets.containsKey(tp) ? latestOffsets.get(tp).offset() : 0L;

                    List<Integer> replicas = tpi.replicas().stream().map(Node::id).toList();
                    List<Integer> isr = tpi.isr().stream().map(Node::id).toList();
                    int leaderId = tpi.leader() != null ? tpi.leader().id() : -1;

                    partitionList.add(new KafkaClusterSnapshot.PartitionInfo(
                            tpi.partition(),
                            leaderId,
                            replicas,
                            isr,
                            beginningOffset,
                            endOffset
                    ));
                }
            }
            topicPartitions.put(topic, partitionList);
        }

        Map<String, Map<Integer, Long>> consumerGroupOffsets = new LinkedHashMap<>();
        Map<String, Map<Integer, Long>> consumerGroupLag = new LinkedHashMap<>();

        try {
            ListConsumerGroupOffsetsResult groupOffsetsResult =
                    adminClient.listConsumerGroupOffsets(settings.kafkaConsumerGroup);
            Map<TopicPartition, OffsetAndMetadata> offsets =
                    groupOffsetsResult.partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);

            for (String topic : topicsToInspect) {
                Map<Integer, Long> topicOffsets = new LinkedHashMap<>();
                Map<Integer, Long> topicLag = new LinkedHashMap<>();

                TopicDescription td = descriptions.get(topic);
                if (td != null) {
                    for (TopicPartitionInfo tpi : td.partitions()) {
                        TopicPartition tp = new TopicPartition(topic, tpi.partition());
                        long endOffset = latestOffsets.containsKey(tp) ? latestOffsets.get(tp).offset() : 0L;
                        if (offsets.containsKey(tp) && offsets.get(tp) != null) {
                            long committed = offsets.get(tp).offset();
                            topicOffsets.put(tpi.partition(), committed);
                            long lag = Math.max(0L, endOffset - committed);
                            topicLag.put(tpi.partition(), lag);
                        } else {
                            topicOffsets.put(tpi.partition(), -1L);
                            topicLag.put(tpi.partition(), endOffset);
                        }
                    }
                }
                consumerGroupOffsets.put(topic, topicOffsets);
                consumerGroupLag.put(topic, topicLag);
            }
        } catch (Exception e) {
            Map<Integer, Long> fallbackOffsets = new LinkedHashMap<>();
            Map<Integer, Long> fallbackLag = new LinkedHashMap<>();
            consumerGroupOffsets.put(settings.kafkaOrdersTopic, fallbackOffsets);
            consumerGroupLag.put(settings.kafkaOrdersTopic, fallbackLag);
        }

        return new KafkaClusterSnapshot(
                Instant.now().toString(),
                clusterId,
                controllerId,
                brokerInfos,
                topicPartitions,
                consumerGroupOffsets,
                consumerGroupLag
        );
    }

    @Override
    public void close() {
        adminClient.close();
    }
}
