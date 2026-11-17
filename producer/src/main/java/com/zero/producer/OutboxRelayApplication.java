package com.zero.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Entry point for the outbox relay producer.
 *
 * <pre>
 * Usage:
 *   java -jar target/producer-1.0.0.jar           # continuous mode (default)
 *   java -jar target/producer-1.0.0.jar --once    # single batch then exit
 * </pre>
 */
public final class OutboxRelayApplication {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayApplication.class);

    /** Set to {@code true} by the JVM shutdown hook on SIGINT / SIGTERM. */
    private static volatile boolean stopRequested = false;

    public static void main(String[] args) throws Exception {
        boolean runOnce = hasFlag(args, "--once");

        // ── Load config ───────────────────────────────────────────────────────
        Settings settings = Settings.load();
        log.info("Starting outbox relay producer — {}", settings);
        if (runOnce) {
            log.info("Mode: single batch (--once)");
        } else {
            log.info("Mode: continuous (poll every {} ms)", settings.outboxPollIntervalMs);
        }

        // ── Graceful shutdown hook ─────────────────────────────────────────────
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received — stopping after current batch…");
            stopRequested = true;
        }, "shutdown-hook"));

        // ── Shared singletons ─────────────────────────────────────────────────
        ObjectMapper objectMapper = new ObjectMapper();
        DatabaseClient dbClient   = new DatabaseClient(settings, objectMapper);

        try (KafkaPublisher kafkaPublisher = new KafkaPublisher(settings, objectMapper)) {
            // ── Main loop ─────────────────────────────────────────────────────
            while (!stopRequested) {
                processBatch(dbClient, kafkaPublisher, settings.outboxBatchSize);

                if (runOnce) {
                    break;
                }

                // Poll sleep — interruptible so the shutdown hook can wake us
                try {
                    Thread.sleep(settings.outboxPollIntervalMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.info("Poll sleep interrupted — shutting down");
                    break;
                }
            }
        }

        log.info("Outbox relay producer stopped.");
    }

    // ── batch processing ──────────────────────────────────────────────────────

    private static void processBatch(
            DatabaseClient db,
            KafkaPublisher kafka,
            int batchSize) {

        List<Map<String, Object>> rows;
        try {
            rows = db.fetchPendingEvents(batchSize);
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

    // ── utilities ─────────────────────────────────────────────────────────────

    private static boolean hasFlag(String[] args, String flag) {
        for (String arg : args) {
            if (flag.equalsIgnoreCase(arg)) {
                return true;
            }
        }
        return false;
    }
}
