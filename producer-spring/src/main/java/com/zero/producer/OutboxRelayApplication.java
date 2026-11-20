package com.zero.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Arrays;
import java.util.Map;

/**
 * Entry point for the Spring Boot outbox relay.
 *
 * <pre>
 * Usage:
 *   java -jar target/producer-spring-1.0.0.jar           # continuous mode (default)
 *   java -jar target/producer-spring-1.0.0.jar --once    # single batch then exit
 * </pre>
 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class OutboxRelayApplication {

    public static void main(String[] args) {
        boolean runOnce = Arrays.stream(args).anyMatch("--once"::equalsIgnoreCase);

        SpringApplication app = new SpringApplication(OutboxRelayApplication.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        if (runOnce) {
            app.setDefaultProperties(Map.of("app.outbox.once", "true"));
        }
        app.run(args);
    }
}
