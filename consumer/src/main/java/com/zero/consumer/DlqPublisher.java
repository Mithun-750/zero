package com.zero.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Publishes poison events to the dead-letter topic (payments.dlq).
 * Manages an idempotent KafkaProducer (acks=all, idempotence=true).
 * Mirrors Python consumer/dlq.py.
 */
public class DlqPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);
    private static final int RAW_PAYLOAD_MAX = 10_000;
    private static final int REASON_MAX = 2_000;

    private final Settings settings;
    private final ObjectMapper mapper = new ObjectMapper();
    private final KafkaProducer<byte[], byte[]> producer;

    public DlqPublisher(Settings settings) {
        this.settings = settings;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.kafkaBootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, settings.kafkaClientId + "-dlq");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Result of a successful DLQ publish.
     */
    public record DlqMeta(String dlqTopic, int dlqPartition, long dlqOffset) {}

    /**
     * Build the DLQ envelope JSON structure (self-contained).
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> buildEnvelope(
            byte[] rawPayload,
            Map<String, Object> event,
            String failureClass,
            String failureReason,
            int attemptCount,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            Long sourceTimestamp
    ) {
        Map<String, Object> ev = (event != null) ? event : new LinkedHashMap<>();
        String rawText = rawPayload != null ? new String(rawPayload, StandardCharsets.UTF_8) : "";
        if (rawText.length() > RAW_PAYLOAD_MAX) rawText = rawText.substring(0, RAW_PAYLOAD_MAX);

        Map<String, Object> source = new LinkedHashMap<>();
        source.put("topic", sourceTopic);
        source.put("partition", sourcePartition);
        source.put("offset", sourceOffset);
        source.put("timestamp", sourceTimestamp);

        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("event_id", ev.get("event_id"));
        envelope.put("event_type", ev.get("event_type"));
        envelope.put("aggregate_type", ev.get("aggregate_type"));
        envelope.put("aggregate_id", ev.get("aggregate_id"));
        envelope.put("occurred_at", ev.get("occurred_at"));
        envelope.put("source", source);
        envelope.put("failure_class", failureClass != null ? failureClass : "UNKNOWN");
        envelope.put("failure_reason", truncate(failureReason, REASON_MAX));
        envelope.put("attempt_count", attemptCount);
        envelope.put("raw_payload", rawText);
        envelope.put("failed_at", Instant.now().toString());
        envelope.put("consumer_name", DatabaseClient.CONSUMER_NAME);
        return envelope;
    }

    /**
     * Publish to DLQ and return broker coordinates.
     * Throws DlqPublishException if the broker does not acknowledge.
     */
    @SuppressWarnings("unchecked")
    public DlqMeta publishToDlq(
            byte[] rawPayload,
            Map<String, Object> event,
            String failureClass,
            String failureReason,
            int attemptCount,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            Long sourceTimestamp
    ) {
        Map<String, Object> envelope = buildEnvelope(
                rawPayload, event, failureClass, failureReason,
                attemptCount, sourceTopic, sourcePartition, sourceOffset, sourceTimestamp);

        Object aggregateIdObj = envelope.get("aggregate_id");
        String key = (aggregateIdObj != null && !aggregateIdObj.toString().isBlank())
                ? aggregateIdObj.toString() : "unknown-aggregate";

        // Build Kafka headers
        List<org.apache.kafka.common.header.Header> headers = new ArrayList<>();
        String fClass = failureClass != null ? failureClass : "UNKNOWN";
        headers.add(new RecordHeader("failure_class", fClass.getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader("consumer_name", DatabaseClient.CONSUMER_NAME.getBytes(StandardCharsets.UTF_8)));
        Object eidObj = envelope.get("event_id");
        if (eidObj != null && !eidObj.toString().isBlank()) {
            headers.add(new RecordHeader("event_id", eidObj.toString().getBytes(StandardCharsets.UTF_8)));
        }
        if (aggregateIdObj != null && !aggregateIdObj.toString().isBlank()) {
            headers.add(new RecordHeader("aggregate_id", aggregateIdObj.toString().getBytes(StandardCharsets.UTF_8)));
        }

        try {
            byte[] value = mapper.writeValueAsBytes(envelope);
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                    settings.kafkaDlqTopic,
                    null, // partition — let Kafka decide
                    key.getBytes(StandardCharsets.UTF_8),
                    value,
                    headers
            );
            RecordMetadata meta = producer.send(record).get(30, TimeUnit.SECONDS);
            log.error("Event routed to DLQ topic={} partition={} offset={} class={} reason={}",
                    settings.kafkaDlqTopic, meta.partition(), meta.offset(), failureClass, failureReason);
            return new DlqMeta(settings.kafkaDlqTopic, meta.partition(), meta.offset());
        } catch (Exception e) {
            throw new DlqPublishException(
                    "could not publish to DLQ " + settings.kafkaDlqTopic + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            producer.flush();
            producer.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            log.warn("DLQ producer close failed: {}", e.getMessage());
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max);
    }
}
