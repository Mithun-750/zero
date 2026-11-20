package com.zero.consumer.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.zero.consumer.config.ConsumerProperties;
import com.zero.consumer.service.Pipeline;

import java.time.Duration;
import java.util.Collections;

/**
 * Single-batch mode ({@code --once}): process all currently available messages
 * (or wait ~3 s for the group assignment to settle), then shut down.
 */
@Component
@ConditionalOnProperty(name = "app.consumer.once", havingValue = "true")
public class OnceConsumerRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(OnceConsumerRunner.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final Pipeline pipeline;
    private final ConsumerProperties props;
    private final KafkaProperties kafkaProperties;
    private final ApplicationContext context;

    public OnceConsumerRunner(Pipeline pipeline, ConsumerProperties props,
                              KafkaProperties kafkaProperties, ApplicationContext context) {
        this.pipeline = pipeline;
        this.props = props;
        this.kafkaProperties = kafkaProperties;
        this.context = context;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("Mode: single batch (--once) — topic={}", props.ordersTopic());

        int processed = 0;
        int emptyPolls = 0;
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(kafkaProperties.buildConsumerProperties())) {
            consumer.subscribe(Collections.singletonList(props.ordersTopic()));
            while (true) {
                ConsumerRecords<byte[], byte[]> records;
                try {
                    records = consumer.poll(POLL_TIMEOUT);
                } catch (WakeupException e) {
                    log.warn("Wakeup received — exiting");
                    break;
                }

                processed += BatchProcessor.processBatch(consumer, pipeline, records);

                if (!records.isEmpty()) {
                    break;
                } else {
                    emptyPolls++;
                    if (emptyPolls >= 6) { // ~3 seconds waiting for group rebalance/assignment
                        break;
                    }
                }
            }
        }

        log.info("Stopped after {} record(s)", processed);
        int exitCode = SpringApplication.exit(context, () -> 0);
        System.exit(exitCode);
    }
}
