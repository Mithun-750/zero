package com.zero.consumer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Consumer configuration, bound from {@code app.consumer.*}.
 * Kafka and datasource settings come from {@code spring.kafka.*} /
 * {@code spring.datasource.*}, mapped 1:1 from the same environment
 * variables the old Settings class used.
 */
@ConfigurationProperties(prefix = "app.consumer")
public record ConsumerProperties(
        @DefaultValue("false") boolean once,
        int maxRetries,
        double retryBackoffSeconds,
        String ordersTopic,
        String dlqTopic
) {}
