package com.zero.reliability;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.zero.reliability.config.ReliabilitySettings;
import com.zero.reliability.db.DatabaseSnapshotClient;
import com.zero.reliability.kafka.KafkaClusterInspector;
import com.zero.reliability.model.DatabaseStateSnapshot;
import com.zero.reliability.model.ExperimentManifest;
import com.zero.reliability.model.KafkaClusterSnapshot;
import com.zero.reliability.reconciliation.ReconciliationEngine;
import com.zero.reliability.reconciliation.ReconciliationReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ExperimentCLI {

    private static final Logger log = LoggerFactory.getLogger(ExperimentCLI.class);

    private static final ObjectMapper mapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        String command = args[0].toLowerCase();
        ReliabilitySettings settings = ReliabilitySettings.load();

        try {
            switch (command) {
                case "snapshot-db" -> handleSnapshotDb(settings, args);
                case "inspect-kafka" -> handleInspectKafka(settings, args);
                case "record-manifest" -> handleRecordManifest(settings, args);
                case "reconcile" -> handleReconcile(settings, args);
                case "capture-state" -> handleCaptureState(settings, args);
                default -> {
                    System.err.println("Unknown command: " + command);
                    printUsage();
                    System.exit(1);
                }
            }
        } catch (Exception e) {
            log.error("Command execution failed: {}", e.getMessage(), e);
            System.err.println("[ERROR] " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(2);
        }
    }

    private static void printUsage() {
        System.out.println("""
            Usage: java -jar target/reliability-1.0.0.jar <command> [options]

            Commands:
              snapshot-db [--output <file>]
                Capture initial PostgreSQL state counts.

              inspect-kafka [--output <file>]
                Inspect Kafka cluster KRaft topology, topics, partitions, leaders, ISR, and lag.

              record-manifest --experiment-id <id> --orders-file <file> --output <file>
                Read generated order UUIDs, query DB for created outbox event UUIDs,
                capture initial snapshots, and save experiment manifest.

              reconcile --manifest <file> [--output <file>]
                Reconcile experiment events across orders, outbox, processed_events, and payments.

              capture-state --experiment-id <id> --label <label> --output <file>
                Capture combined DB + Kafka state into a single JSON file.
            """);
    }

    private static void handleSnapshotDb(ReliabilitySettings settings, String[] args) throws Exception {
        String outputFile = getOption(args, "--output", null);
        DatabaseSnapshotClient client = new DatabaseSnapshotClient(settings);
        DatabaseStateSnapshot snapshot = client.captureSnapshot();
        String json = mapper.writeValueAsString(snapshot);
        System.out.println(json);

        if (outputFile != null) {
            saveToFile(outputFile, json);
        }
    }

    private static void handleInspectKafka(ReliabilitySettings settings, String[] args) throws Exception {
        String outputFile = getOption(args, "--output", null);
        try (KafkaClusterInspector inspector = new KafkaClusterInspector(settings)) {
            KafkaClusterSnapshot snapshot = inspector.inspect();
            String json = mapper.writeValueAsString(snapshot);
            System.out.println(json);

            if (outputFile != null) {
                saveToFile(outputFile, json);
            }
        }
    }

    private static void handleCaptureState(ReliabilitySettings settings, String[] args) throws Exception {
        String experimentId = getOption(args, "--experiment-id", "KAFKA-BROKER-FAILURE-2026-002");
        String label = getOption(args, "--label", "STATE_SNAPSHOT");
        String outputFile = getOption(args, "--output", null);

        DatabaseSnapshotClient dbClient = new DatabaseSnapshotClient(settings);
        DatabaseStateSnapshot dbSnapshot = dbClient.captureSnapshot();

        KafkaClusterSnapshot kafkaSnapshot;
        try (KafkaClusterInspector inspector = new KafkaClusterInspector(settings)) {
            kafkaSnapshot = inspector.inspect();
        }

        Map<String, Object> stateMap = new LinkedHashMap<>();
        stateMap.put("experimentId", experimentId);
        stateMap.put("timestamp", Instant.now().toString());
        stateMap.put("label", label);
        stateMap.put("database", dbSnapshot);
        stateMap.put("kafka", kafkaSnapshot);

        String json = mapper.writeValueAsString(stateMap);
        System.out.println(json);

        if (outputFile != null) {
            saveToFile(outputFile, json);
            System.out.printf("State snapshot [%s] saved to %s%n", label, outputFile);
        }
    }

    private static void handleRecordManifest(ReliabilitySettings settings, String[] args) throws Exception {
        String experimentId = getOption(args, "--experiment-id", "KAFKA-BROKER-FAILURE-2026-002");
        String ordersFile = getOption(args, "--orders-file", "reliability-testing/kafka-broker-failure/phase-d/orders-1000.json");
        String outputFile = getOption(args, "--output", "reliability-testing/kafka-broker-failure/phase-d/manifest.json");

        File f = new File(ordersFile);
        if (!f.exists()) {
            throw new IllegalArgumentException("Orders file not found: " + ordersFile);
        }

        JsonNode root = mapper.readTree(f);
        if (!root.isArray()) {
            throw new IllegalArgumentException("Orders JSON root must be an array");
        }

        List<String> orderIds = new ArrayList<>();
        for (JsonNode item : root) {
            String id = item.get("id").asText();
            orderIds.add(id);
        }

        System.out.printf("Loaded %d order IDs from %s%n", orderIds.size(), ordersFile);

        // Fetch outbox event IDs corresponding to these orders
        List<String> outboxEventIds = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(
                settings.jdbcUrl(), settings.postgresUser, settings.postgresPassword)) {
            UUID[] orderUuids = orderIds.stream().map(UUID::fromString).toArray(UUID[]::new);
            Array arr = conn.createArrayOf("uuid", orderUuids);

            String sql = "SELECT id FROM outbox WHERE aggregate_id = ANY(?) ORDER BY created_at ASC";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setArray(1, arr);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        outboxEventIds.add(rs.getString("id"));
                    }
                }
            }
        }

        System.out.printf("Found %d corresponding outbox events in database%n", outboxEventIds.size());

        // Baseline snapshots
        DatabaseSnapshotClient dbClient = new DatabaseSnapshotClient(settings);
        DatabaseStateSnapshot dbSnapshot = dbClient.captureSnapshot();

        KafkaClusterSnapshot kafkaSnapshot;
        try (KafkaClusterInspector inspector = new KafkaClusterInspector(settings)) {
            kafkaSnapshot = inspector.inspect();
        }

        ExperimentManifest manifest = new ExperimentManifest(
                experimentId,
                Instant.now().toString(),
                "Controlled Kafka broker failure and recovery experiment (DL-09 Phase D: Broker 2 failure)",
                orderIds.size(),
                orderIds,
                outboxEventIds,
                dbSnapshot,
                kafkaSnapshot
        );

        String json = mapper.writeValueAsString(manifest);
        saveToFile(outputFile, json);
        System.out.printf("Experiment manifest written to %s%n", outputFile);
    }

    private static void handleReconcile(ReliabilitySettings settings, String[] args) throws Exception {
        String manifestFile = getOption(args, "--manifest", "reliability-testing/kafka-broker-failure/phase-d/manifest.json");
        String outputFile = getOption(args, "--output", "reliability-testing/kafka-broker-failure/phase-d/reconciliation-results.json");

        File f = new File(manifestFile);
        if (!f.exists()) {
            throw new IllegalArgumentException("Manifest file not found: " + manifestFile);
        }

        ExperimentManifest manifest = mapper.readValue(f, ExperimentManifest.class);
        System.out.printf("Reconciling experiment %s (%d expected orders)...%n",
                manifest.experimentId(), manifest.orderIds().size());

        ReconciliationEngine engine = new ReconciliationEngine(settings);
        ReconciliationReport report = engine.reconcile(
                manifest.experimentId(),
                manifest.orderIds(),
                manifest.outboxEventIds()
        );

        String json = mapper.writeValueAsString(report);
        System.out.println(json);

        if (outputFile != null) {
            saveToFile(outputFile, json);
            System.out.printf("Reconciliation report saved to %s%n", outputFile);
        }
    }

    private static String getOption(String[] args, String flag, String defaultVal) {
        for (int i = 0; i < args.length - 1; i++) {
            if (flag.equalsIgnoreCase(args[i])) {
                return args[i + 1];
            }
        }
        return defaultVal;
    }

    private static void saveToFile(String filepath, String content) throws Exception {
        Path path = Paths.get(filepath);
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        Files.writeString(path, content);
    }
}
