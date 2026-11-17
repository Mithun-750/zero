package com.zero.reliability.config;

import io.github.cdimascio.dotenv.Dotenv;

public final class ReliabilitySettings {

    public final String postgresHost;
    public final int    postgresPort;
    public final String postgresDb;
    public final String postgresUser;
    public final String postgresPassword;

    public final String kafkaBootstrapServers;
    public final String kafkaOrdersTopic;
    public final String kafkaDlqTopic;
    public final String kafkaConsumerGroup;

    private ReliabilitySettings(Dotenv dotenv) {
        this.postgresHost     = get(dotenv, "POSTGRES_HOST", "localhost");
        this.postgresPort     = Integer.parseInt(get(dotenv, "POSTGRES_PORT", "5434"));
        this.postgresDb       = get(dotenv, "POSTGRES_DB", "order_db");
        this.postgresUser     = get(dotenv, "POSTGRES_USER", "postgres");
        this.postgresPassword = get(dotenv, "POSTGRES_PASSWORD", "postgres");

        this.kafkaBootstrapServers = get(dotenv, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9094,localhost:9096");
        this.kafkaOrdersTopic      = get(dotenv, "KAFKA_ORDERS_TOPIC", "orders.created");
        this.kafkaDlqTopic         = get(dotenv, "KAFKA_DLQ_TOPIC", "payments.dlq");
        this.kafkaConsumerGroup    = get(dotenv, "KAFKA_CONSUMER_GROUP", "payment-consumer");
    }

    public static ReliabilitySettings load() {
        Dotenv dotenv;
        try {
            dotenv = Dotenv.configure().ignoreIfMissing().load();
            if (dotenv.get("POSTGRES_DB") == null) {
                dotenv = Dotenv.configure().directory("../").ignoreIfMissing().load();
            }
        } catch (Exception e) {
            dotenv = Dotenv.configure().ignoreIfMissing().load();
        }
        return new ReliabilitySettings(dotenv);
    }

    private static String get(Dotenv dotenv, String key, String defaultValue) {
        String val = dotenv.get(key);
        if (val == null || val.isBlank()) {
            val = System.getenv(key);
        }
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }

    public String jdbcUrl() {
        return String.format("jdbc:postgresql://%s:%d/%s", postgresHost, postgresPort, postgresDb);
    }

    @Override
    public String toString() {
        return String.format("ReliabilitySettings{pg=%s:%d/%s, kafka=%s, ordersTopic=%s, dlqTopic=%s, group=%s}",
                postgresHost, postgresPort, postgresDb, kafkaBootstrapServers, kafkaOrdersTopic, kafkaDlqTopic, kafkaConsumerGroup);
    }
}

