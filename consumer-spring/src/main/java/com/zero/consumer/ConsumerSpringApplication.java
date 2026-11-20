package com.zero.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

import java.util.Arrays;
import java.util.Map;

/**
 * Entry point for the Spring Boot payment consumer.
 *
 * <pre>
 * Usage:
 *   java -jar target/consumer-spring-1.0.0.jar           # continuous mode (default)
 *   java -jar target/consumer-spring-1.0.0.jar --once    # process available messages and exit
 * </pre>
 *
 * Dashboard: http://${DASHBOARD_HOST:127.0.0.1}:${DASHBOARD_PORT:8090}
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class ConsumerSpringApplication {

    public static void main(String[] args) {
        boolean once = Arrays.stream(args).anyMatch("--once"::equalsIgnoreCase);

        SpringApplication app = new SpringApplication(ConsumerSpringApplication.class);
        if (once) {
            app.setDefaultProperties(Map.of("app.consumer.once", "true"));
        }
        app.run(args);
    }
}
