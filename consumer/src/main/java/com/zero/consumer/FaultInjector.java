package com.zero.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Fault injector CLI: publish a corrupt OrderCreated event (+ valid followers)
 * straight to orders.created so you can watch the DLQ/block/park/resolve flow.
 *
 * Usage:
 *   java -cp consumer-java-1.0.0.jar com.zero.consumer.FaultInjector [--order-id UUID] [--followers N] [--corrupt-amount FLOAT] [--amount FLOAT]
 *
 * Mirrors Python consumer/inject.py.
 */
public class FaultInjector {

    private static final Logger log = LoggerFactory.getLogger(FaultInjector.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // Parse args
        String orderId = null;
        int followers = 2;
        double corruptAmount = -50.0;
        double amount = 99.0;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--order-id" -> orderId = args[++i];
                case "--followers" -> followers = Integer.parseInt(args[++i]);
                case "--corrupt-amount" -> corruptAmount = Double.parseDouble(args[++i]);
                case "--amount" -> amount = Double.parseDouble(args[++i]);
            }
        }

        Settings settings = Settings.load();

        if (orderId == null) {
            // Fetch the latest order id from DB
            String url = String.format("jdbc:postgresql://%s:%d/%s",
                    settings.postgresHost, settings.postgresPort, settings.postgresDb);
            Properties props = new Properties();
            props.setProperty("user", settings.postgresUser);
            props.setProperty("password", settings.postgresPassword);
            try (Connection conn = DriverManager.getConnection(url, props);
                 PreparedStatement ps = conn.prepareStatement(
                         "SELECT id FROM orders ORDER BY created_at DESC LIMIT 1");
                 ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    System.err.println("No orders found — run load-orders first, or pass --order-id");
                    System.exit(1);
                }
                orderId = rs.getString("id");
            }
        }

        log.info("Using aggregate (order) id={}", orderId);

        Map<String, Object> corrupt = buildEvent(orderId, corruptAmount);
        List<Map<String, Object>> followerEvents = new ArrayList<>();
        for (int i = 0; i < followers; i++) {
            followerEvents.add(buildEvent(orderId, amount));
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.kafkaBootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, settings.kafkaClientId + "-injector");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);
        try {
            // Publish corrupt event
            publish(producer, settings.kafkaOrdersTopic, orderId, corrupt, "corrupt");
            // Publish followers
            for (Map<String, Object> fe : followerEvents) {
                publish(producer, settings.kafkaOrdersTopic, orderId, fe, "follower");
            }
        } finally {
            producer.flush();
            producer.close();
        }

        log.info("Done. Expected: corrupt event -> DLQ + aggregate blocked; {} follower(s) -> parked. " +
                 "Watch http://{}:{}", followers, settings.dashboardHost, settings.dashboardPort);
    }

    private static Map<String, Object> buildEvent(String orderId, double amount) {
        Map<String, Object> order = new LinkedHashMap<>();
        order.put("order_id", orderId);
        order.put("customer_id", "CUST-INJECT");
        order.put("total_amount", amount);
        order.put("currency", "INR");
        order.put("status", "CREATED");

        Map<String, Object> trigger = new LinkedHashMap<>();
        trigger.put("action", "INITIATE_PAYMENT");
        trigger.put("amount", amount);
        trigger.put("currency", "INR");
        trigger.put("target_account", "CUST-INJECT");

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("order", order);
        data.put("payment_trigger", trigger);

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("event_id", UUID.randomUUID().toString());
        event.put("event_type", "OrderCreated");
        event.put("event_version", 1);
        event.put("occurred_at", Instant.now().toString());
        event.put("aggregate_type", "ORDER");
        event.put("aggregate_id", orderId);
        event.put("source", "fault-injector");
        event.put("data", data);
        return event;
    }

    private static void publish(KafkaProducer<byte[], byte[]> producer, String topic,
                                 String key, Map<String, Object> event, String label) throws Exception {
        byte[] value = MAPPER.writeValueAsBytes(event);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                topic, key.getBytes(StandardCharsets.UTF_8), value);
        RecordMetadata meta = producer.send(record).get(30, TimeUnit.SECONDS);
        log.info("Published {} event_id={} to {} partition={} offset={}",
                label, event.get("event_id"), topic, meta.partition(), meta.offset());
    }
}
