package com.zero.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Thin wrapper around {@link KafkaProducer} that serialises event Maps to JSON
 * and sends them synchronously (acks=all, idempotence=true).
 */
public final class KafkaPublisher implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KafkaPublisher.class);

    private final KafkaProducer<byte[], byte[]> producer;
    private final String topic;
    private final ObjectMapper objectMapper;

    public KafkaPublisher(Settings settings, ObjectMapper objectMapper) {
        this.topic        = settings.kafkaOrdersTopic;
        this.objectMapper = objectMapper;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  settings.kafkaBootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG,          settings.kafkaClientId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,               "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // Reasonable retries for idempotent producer
        props.put(ProducerConfig.RETRIES_CONFIG,            String.valueOf(Integer.MAX_VALUE));
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        this.producer = new KafkaProducer<>(props);
        log.info("KafkaPublisher initialised — bootstrap={} topic={}", settings.kafkaBootstrapServers, topic);
    }

    /**
     * Serialise {@code event} to JSON, send to Kafka with {@code aggregateId} as the key,
     * and block until the broker acknowledges (up to 120 s).
     *
     * @throws Exception on serialisation or Kafka errors
     */
    public void publish(String aggregateId, Map<String, Object> event) throws Exception {
        byte[] keyBytes   = aggregateId.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = objectMapper.writeValueAsBytes(event);

        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, keyBytes, valueBytes);

        // Synchronous send — blocks until broker ack or timeout
        producer.send(record).get(120, TimeUnit.SECONDS);
    }

    /** Flush any buffered records and close the underlying producer. */
    @Override
    public void close() {
        log.info("Closing KafkaPublisher…");
        producer.flush();
        producer.close();
    }
}
