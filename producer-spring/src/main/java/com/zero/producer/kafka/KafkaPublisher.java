package com.zero.producer.kafka;

import tools.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.zero.producer.config.OutboxProperties;
/**
 * Thin wrapper around the Spring-configured KafkaTemplate.
 * Producer settings (acks=all, idempotence, retries) live in application.yml
 * under {@code spring.kafka.producer.*}. Sends are synchronous.
 */
@Component
public class KafkaPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaPublisher.class);
    private static final long SEND_TIMEOUT_SECONDS = 120;

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final String topic;
    private final ObjectMapper objectMapper;

    public KafkaPublisher(KafkaTemplate<String, byte[]> kafkaTemplate,
                          OutboxProperties props,
                          ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic         = props.ordersTopic();
        this.objectMapper  = objectMapper;
        log.info("KafkaPublisher initialised — topic={}", topic);
    }

    /**
     * Serialise {@code event} to JSON, send to Kafka with {@code aggregateId} as the key,
     * and block until the broker acknowledges (up to 120 s).
     *
     * @throws Exception on serialisation or Kafka errors
     */
    public void publish(String aggregateId, Map<String, Object> event) throws Exception {
        byte[] valueBytes = objectMapper.writeValueAsBytes(event);

        // Synchronous send — blocks until broker ack or timeout
        var result = kafkaTemplate.send(new ProducerRecord<>(topic, aggregateId, valueBytes))
                .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        log.debug("Sent to {} partition={} offset={}", topic,
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
    }

    /** Flush and close handled by the Spring context; kept for API parity. */
    @Override
    public void close() {
        log.info("Closing KafkaPublisher…");
        kafkaTemplate.flush();
    }
}
