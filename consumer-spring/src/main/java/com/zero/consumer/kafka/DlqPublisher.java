package com.zero.consumer.kafka;

import tools.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.zero.consumer.exception.DlqPublishException;
import com.zero.consumer.config.ConsumerProperties;
import com.zero.consumer.repository.DatabaseClient;

/**
 * Publishes poison events to the dead-letter topic (payments.dlq).
 * Uses the dedicated DLQ KafkaTemplate (acks=all, idempotence=true).
 */
@Component
public class DlqPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);
    private static final int RAW_PAYLOAD_MAX = 10_000;
    private static final int REASON_MAX = 2_000;

    private final ConsumerProperties props;
    private final ObjectMapper mapper = new ObjectMapper();
    private final KafkaTemplate<String, byte[]> dlqKafkaTemplate;

    public DlqPublisher(@Qualifier("dlqKafkaTemplate") KafkaTemplate<String, byte[]> dlqKafkaTemplate,
            ConsumerProperties props) {
        this.props = props;
        this.dlqKafkaTemplate = dlqKafkaTemplate;
    }

    /**
     * Result of a successful DLQ publish.
     */
    public record DlqMeta(String dlqTopic, int dlqPartition, long dlqOffset) {
    }

    /**
     * Build the DLQ envelope JSON structure (self-contained).
     */
    public Map<String, Object> buildEnvelope(
            byte[] rawPayload,
            Map<String, Object> event,
            String failureClass,
            String failureReason,
            int attemptCount,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            Long sourceTimestamp) {
        Map<String, Object> ev = (event != null) ? event : new LinkedHashMap<>();
        String rawText = rawPayload != null ? new String(rawPayload, StandardCharsets.UTF_8) : "";
        if (rawText.length() > RAW_PAYLOAD_MAX)
            rawText = rawText.substring(0, RAW_PAYLOAD_MAX);

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
    public DlqMeta publishToDlq(
            byte[] rawPayload,
            Map<String, Object> event,
            String failureClass,
            String failureReason,
            int attemptCount,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            Long sourceTimestamp) {
        Map<String, Object> envelope = buildEnvelope(
                rawPayload, event, failureClass, failureReason,
                attemptCount, sourceTopic, sourcePartition, sourceOffset, sourceTimestamp);

        Object aggregateIdObj = envelope.get("aggregate_id");
        String key = (aggregateIdObj != null && !aggregateIdObj.toString().isBlank())
                ? aggregateIdObj.toString()
                : "unknown-aggregate";

        // Build Kafka headers
        List<Header> headers = new ArrayList<>();
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
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                    props.dlqTopic(),
                    key,
                    value);
            // Copy custom headers onto the record
            headers.forEach(h -> record.headers().add(h.key(), h.value()));

            var result = dlqKafkaTemplate.send(record).get(30, TimeUnit.SECONDS);
            RecordMetadata meta = result.getRecordMetadata();
            log.error("Event routed to DLQ topic={} partition={} offset={} class={} reason={}",
                    props.dlqTopic(), meta.partition(), meta.offset(), failureClass, failureReason);
            return new DlqMeta(props.dlqTopic(), meta.partition(), meta.offset());
        } catch (Exception e) {
            throw new DlqPublishException(
                    "could not publish to DLQ " + props.dlqTopic() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        dlqKafkaTemplate.flush();
    }

    private static String truncate(String s, int max) {
        if (s == null)
            return null;
        return s.length() <= max ? s : s.substring(0, max);
    }
}
