package com.zero.producer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * Single-batch mode ({@code --once}): process one batch, then shut down.
 */
@Component
@ConditionalOnProperty(name = "app.outbox.once", havingValue = "true")
public class OnceRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(OnceRunner.class);

    private final OutboxRelay relay;
    private final ApplicationContext context;

    public OnceRunner(OutboxRelay relay, ApplicationContext context) {
        this.relay = relay;
        this.context = context;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("Mode: single batch (--once)");
        relay.processBatch();
        int exitCode = SpringApplication.exit(context, () -> 0);
        System.exit(exitCode);
    }
}
