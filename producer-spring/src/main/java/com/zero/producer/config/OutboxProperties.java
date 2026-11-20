package com.zero.producer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Outbox relay configuration, bound from {@code app.outbox.*}.
 * Infrastructure settings (datasource, Kafka) are picked up from
 * {@code spring.datasource.*} / {@code spring.kafka.*} which are mapped
 * 1:1 from the same environment variables the old Settings class used.
 */
@ConfigurationProperties(prefix = "app.outbox")
public record OutboxProperties(
        @DefaultValue("false") boolean once,
        int batchSize,
        @DefaultValue("2s") Duration pollInterval,
        String ordersTopic
) {}
