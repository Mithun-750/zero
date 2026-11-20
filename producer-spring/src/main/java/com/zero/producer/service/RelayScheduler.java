package com.zero.producer.service;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Continuous mode: poll the outbox on a fixed delay (default every 2 s).
 * Disabled when {@code app.outbox.once=true} (i.e. {@code --once} on the CLI).
 */
@Component
@ConditionalOnProperty(name = "app.outbox.once", havingValue = "false", matchIfMissing = true)
public class RelayScheduler {

    private final OutboxRelay relay;

    public RelayScheduler(OutboxRelay relay) {
        this.relay = relay;
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval}")
    public void tick() {
        relay.processBatch();
    }
}
