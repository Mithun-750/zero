package com.zero.consumer.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import com.zero.consumer.config.ConsumerProperties;
import com.zero.consumer.service.Pipeline;

import java.time.Duration;
import java.util.Collections;

/**
 * Continuous mode: subscribe to {@code orders.created} and pump records through
 * the pipeline on a background thread. Manual offset commits per partition via
 * {@link BatchProcessor}. Disabled when {@code app.consumer.once=true}.
 */
@Component
@ConditionalOnProperty(name = "app.consumer.once", havingValue = "false", matchIfMissing = true)
public class ConsumerWorker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWorker.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final Pipeline pipeline;
    private final ConsumerProperties props;
    private final KafkaProperties kafkaProperties;

    private volatile boolean running = false;
    private volatile boolean stopRequested = false;
    private Thread workerThread;
    private Consumer<byte[], byte[]> consumer;

    public ConsumerWorker(Pipeline pipeline, ConsumerProperties props, KafkaProperties kafkaProperties) {
        this.pipeline = pipeline;
        this.props = props;
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public void start() {
        log.info("Starting payment consumer — topic={}", props.ordersTopic());
        consumer = new KafkaConsumer<>(kafkaProperties.buildConsumerProperties());
        consumer.subscribe(Collections.singletonList(props.ordersTopic()));
        stopRequested = false;
        workerThread = new Thread(this::runLoop, "payment-consumer");
        workerThread.start();
        running = true;
    }

    private void runLoop() {
        int processed = 0;
        try {
            while (!stopRequested) {
                ConsumerRecords<byte[], byte[]> records;
                try {
                    records = consumer.poll(POLL_TIMEOUT);
                } catch (WakeupException e) {
                    if (stopRequested)
                        break;
                    throw e;
                }
                processed += BatchProcessor.processBatch(consumer, pipeline, records);
            }
        } catch (Exception e) {
            log.error("Consumer loop failed: {}", e.getMessage(), e);
        } finally {
            try {
                consumer.close();
            } catch (Exception e) {
                log.warn("Consumer close failed: {}", e.getMessage());
            }
            log.info("Payment consumer stopped after {} record(s)", processed);
        }
    }

    @Override
    public void stop() {
        log.info("Shutdown signal received — stopping consumer...");
        stopRequested = true;
        if (consumer != null) {
            consumer.wakeup();
        }
        if (workerThread != null) {
            try {
                workerThread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 0;
    }
}
