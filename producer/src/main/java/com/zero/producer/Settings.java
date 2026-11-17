package com.zero.producer;

import io.github.cdimascio.dotenv.Dotenv;

/**
 * Immutable configuration loaded from environment variables (with .env fallback).
 * All fields are populated at construction time; callers treat this as a value object.
 */
public final class Settings {

    // ── PostgreSQL ────────────────────────────────────────────────────────────
    public final String postgresHost;
    public final int    postgresPort;
    public final String postgresDb;
    public final String postgresUser;
    public final String postgresPassword;

    // ── Kafka ─────────────────────────────────────────────────────────────────
    public final String kafkaBootstrapServers;
    public final String kafkaOrdersTopic;
    public final String kafkaClientId;

    // ── Outbox relay ─────────────────────────────────────────────────────────
    public final int  outboxBatchSize;
    public final long outboxPollIntervalMs;

    private Settings(Dotenv dotenv) {
        this.postgresHost     = get(dotenv, "POSTGRES_HOST",              "localhost");
        this.postgresPort     = Integer.parseInt(get(dotenv, "POSTGRES_PORT", "5434"));
        this.postgresDb       = get(dotenv, "POSTGRES_DB",               "order_db");
        this.postgresUser     = get(dotenv, "POSTGRES_USER",             "postgres");
        this.postgresPassword = get(dotenv, "POSTGRES_PASSWORD",         "postgres");

        this.kafkaBootstrapServers = get(dotenv, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9094,localhost:9096");
        this.kafkaOrdersTopic      = get(dotenv, "KAFKA_ORDERS_TOPIC",      "orders.created");
        this.kafkaClientId         = get(dotenv, "KAFKA_CLIENT_ID",         "order-outbox-relay");

        this.outboxBatchSize      = Integer.parseInt(get(dotenv, "OUTBOX_BATCH_SIZE",           "100"));
        this.outboxPollIntervalMs = Long.parseLong(get(dotenv, "OUTBOX_POLL_INTERVAL_SECONDS", "2")) * 1_000L;
    }

    /** Load settings — tries ../  relative to CWD for the .env file, falls back gracefully. */
    public static Settings load() {
        Dotenv dotenv;
        try {
            dotenv = Dotenv.configure().ignoreIfMissing().load();
            if (dotenv.get("POSTGRES_DB") == null) {
                dotenv = Dotenv.configure().directory("../").ignoreIfMissing().load();
            }
        } catch (Exception e) {
            dotenv = Dotenv.configure().ignoreIfMissing().load();
        }
        return new Settings(dotenv);
    }

    /** Resolve env var: dotenv first, then JVM system property, then default. */
    private static String get(Dotenv dotenv, String key, String defaultValue) {
        String val = dotenv.get(key);
        if (val == null || val.isBlank()) {
            val = System.getenv(key);
        }
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }

    /** JDBC connection URL derived from fields. */
    public String jdbcUrl() {
        return String.format("jdbc:postgresql://%s:%d/%s", postgresHost, postgresPort, postgresDb);
    }

    @Override
    public String toString() {
        return String.format(
            "Settings{pg=%s:%d/%s, kafka=%s, topic=%s, batchSize=%d, pollMs=%d}",
            postgresHost, postgresPort, postgresDb,
            kafkaBootstrapServers, kafkaOrdersTopic,
            outboxBatchSize, outboxPollIntervalMs);
    }
}
