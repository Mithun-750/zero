package com.zero.consumer.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

/**
 * Dedicated DLQ producer: 
 *      same broker and settings as the main producer
 *      (acks=all, idempotent — sourced from {@code spring.kafka.producer.*}),
 *      with {@code client.id} suffixed by {@code -dlq}.
 */
@Configuration
public class DlqKafkaConfig {

    @Bean
    public ProducerFactory<String, byte[]> dlqProducerFactory(
            KafkaProperties kafkaProperties) {

        Map<String, Object> props = kafkaProperties.buildProducerProperties();

        String baseClientId = kafkaProperties.getProducer().getClientId();

        props.put(ProducerConfig.CLIENT_ID_CONFIG,
                (baseClientId != null && !baseClientId.isBlank() 
                    ? baseClientId 
                    : "payment-consumer") + "-dlq"
        );

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, byte[]> dlqKafkaTemplate(
            ProducerFactory<String, byte[]> dlqProducerFactory) {

        return new KafkaTemplate<>(dlqProducerFactory);
    }
}
