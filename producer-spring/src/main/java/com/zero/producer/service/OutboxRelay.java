package com.zero.producer.service;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

import com.zero.producer.config.OutboxProperties;
import com.zero.producer.kafka.KafkaPublisher;
import com.zero.producer.repository.DatabaseClient;
/**
 * Core outbox relay batch logic: fetch PENDING rows, publish to Kafka,
 * mark rows PUBLISHED only after broker ack. Shared by the scheduled
 * continuous runner and the {@code --once} runner.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final DatabaseClient db;
    private final KafkaPublisher kafka;
    private final OutboxProperties props;

    /** Set on context shutdown; checked inside the batch loop to stop promptly. */
    private volatile boolean stopRequested = false;

    public OutboxRelay(DatabaseClient db, KafkaPublisher kafka, OutboxProperties props) {
        this.db = db;
        this.kafka = kafka;
        this.props = props;
    }

    /** Invoked by the Spring context on graceful shutdown (SIGTERM, etc.). */
    @PreDestroy
    void onShutdown() {
        log.info("Shutdown signal received — stopping after current batch…");
        stopRequested = true;
    }

    public boolean isStopRequested() {
        return stopRequested;
    }

    /** Process a single batch. Exact port of the original processBatch(). */
    public void processBatch() {
        List<Map<String, Object>> rows;
        try {
            rows = db.fetchPendingEvents(props.batchSize());
        } catch (Exception e) {
            log.error("Failed to fetch pending events from DB: {}", e.getMessage(), e);
            return;
        }

        if (rows.isEmpty()) {
            log.debug("No pending events found.");
            return;
        }

        log.info("Processing batch of {} events", rows.size());

        int published = 0;
        int failed    = 0;

        for (Map<String, Object> row : rows) {
            if (stopRequested) {
                log.info("Stop requested — aborting mid-batch after {} published", published);
                break;
            }

            String id          = (String) row.get("id");
            String aggregateId = (String) row.get("aggregate_id");

            try {
                Map<String, Object> event = EventBuilder.buildEvent(row);
                kafka.publish(aggregateId, event);

                db.markPublished(id);
                log.info("Published event id={} aggregateId={}", id, aggregateId);
                published++;

            } catch (Exception e) {
                log.error("Failed to publish event id={}: {}", id, e.getMessage(), e);

                try {
                    int retryCount = db.recordPublishFailure(id, e.getMessage());
                    log.warn("Recorded failure for id={} — retry_count={}", id, retryCount);
                } catch (Exception dbEx) {
                    log.error("Failed to record publish failure for id={}: {}", id, dbEx.getMessage(), dbEx);
                }
                failed++;
            }
        }

        log.info("Batch complete — published={} failed={}", published, failed);
    }
}
