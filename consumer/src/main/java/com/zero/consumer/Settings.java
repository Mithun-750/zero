package com.zero.consumer;

import io.github.cdimascio.dotenv.Dotenv;
import io.github.cdimascio.dotenv.DotenvBuilder;

/**
 * Immutable config loaded from environment variables (with .env fallback).
 * Mirrors Python consumer/config.py Settings dataclass.
 */
public final class Settings {

    public final String postgresHost;
    public final int postgresPort;
    public final String postgresDb;
    public final String postgresUser;
    public final String postgresPassword;

    public final String kafkaBootstrapServers;
    public final String kafkaOrdersTopic;
    public final String kafkaDlqTopic;
    public final String kafkaConsumerGroup;
    public final String kafkaClientId;

    public final int consumerMaxRetries;
    public final double consumerRetryBackoffSeconds;

    public final int dashboardPort;
    public final String dashboardHost;

    private Settings(Dotenv env) {
        this.postgresHost = get(env, "POSTGRES_HOST", "localhost");
        this.postgresPort = positiveInt(env, "POSTGRES_PORT", "5434");
        this.postgresDb = get(env, "POSTGRES_DB", "order_db");
        this.postgresUser = get(env, "POSTGRES_USER", "postgres");
        this.postgresPassword = get(env, "POSTGRES_PASSWORD", "postgres");

        this.kafkaBootstrapServers = get(env, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9094,localhost:9096");
        this.kafkaOrdersTopic = get(env, "KAFKA_ORDERS_TOPIC", "orders.created");
        this.kafkaDlqTopic = get(env, "KAFKA_DLQ_TOPIC", "payments.dlq");
        this.kafkaConsumerGroup = get(env, "KAFKA_CONSUMER_GROUP", "payment-consumer");
        this.kafkaClientId = get(env, "KAFKA_CONSUMER_CLIENT_ID", "payment-consumer");

        this.consumerMaxRetries = positiveInt(env, "CONSUMER_MAX_RETRIES", "3");
        this.consumerRetryBackoffSeconds = positiveDouble(env, "CONSUMER_RETRY_BACKOFF_SECONDS", "0.5");

        this.dashboardPort = positiveInt(env, "DASHBOARD_PORT", "8090");
        this.dashboardHost = get(env, "DASHBOARD_HOST", "127.0.0.1");
    }

    public static Settings load() {
        Dotenv env;
        try {
            env = Dotenv.configure().ignoreIfMissing().load();
            if (env.get("POSTGRES_DB") == null) {
                env = Dotenv.configure().directory("../").ignoreIfMissing().load();
            }
        } catch (Exception e) {
            env = Dotenv.configure().ignoreIfMissing().load();
        }
        return new Settings(env);
    }

    private static String get(Dotenv env, String key, String defaultVal) {
        String v = env.get(key);
        if (v == null || v.isBlank()) {
            v = System.getenv(key);
        }
        return (v != null && !v.isBlank()) ? v : defaultVal;
    }

    private static int positiveInt(Dotenv env, String key, String defaultVal) {
        String raw = get(env, key, defaultVal);
        int value = Integer.parseInt(raw);
        if (value <= 0) throw new IllegalArgumentException(key + " must be a positive integer");
        return value;
    }

    private static double positiveDouble(Dotenv env, String key, String defaultVal) {
        String raw = get(env, key, defaultVal);
        double value = Double.parseDouble(raw);
        if (value <= 0) throw new IllegalArgumentException(key + " must be a positive number");
        return value;
    }
}
