# producer-java

A Java 27 / Maven outbox-relay producer that faithfully replicates the behaviour of the Python `producer/` service.

## What it does

1. Polls the PostgreSQL `outbox` table for `status = 'PENDING'` rows in configurable batches.
2. Builds a structured event envelope from each row.
3. Publishes the envelope to the Kafka topic `orders.created` with `aggregate_id` as the message key (`acks=all`, idempotence enabled).
4. After a successful Kafka ACK, marks the row `PUBLISHED` in the DB.
5. On failure, increments `retry_count` and stores the error message, leaving the row `PENDING` for the next poll.

## Configuration

All settings are read from environment variables.  
Place a `.env` file in the **project root** (`zero/`) and the application will pick it up automatically via dotenv-java.

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5433` | PostgreSQL port |
| `POSTGRES_DB` | `order_db` | Database name |
| `POSTGRES_USER` | `postgres` | DB user |
| `POSTGRES_PASSWORD` | `postgres` | DB password |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap |
| `KAFKA_ORDERS_TOPIC` | `orders.created` | Target topic |
| `KAFKA_CLIENT_ID` | `order-outbox-relay` | Kafka client ID |
| `OUTBOX_BATCH_SIZE` | `100` | Rows per poll cycle |
| `OUTBOX_POLL_INTERVAL_SECONDS` | `2` | Seconds between polls |

## Build

Requires **Java 27** and **Maven 3.x** on `PATH`.

```bash
cd producer-java
mvn package
```

The shade plugin produces a self-contained fat jar at:

```
target/producer-java-1.0.0.jar
```

## Run

### Using Maven (Recommended)

From the project root:
```bash
# Continuous polling
mvn -pl producer-java exec:java

# Single batch mode
mvn -pl producer-java exec:java -Dexec.args="--once"
```

Or from within `producer-java/`:
```bash
cd producer-java
mvn exec:java
mvn exec:java -Dexec.args="--once"
```

### Using Executable JAR

**Continuous mode** (default — polls every `OUTBOX_POLL_INTERVAL_SECONDS` seconds):

```bash
java -jar target/producer-java-1.0.0.jar
```

**Single-batch mode** (process one batch then exit):

```bash
java -jar target/producer-java-1.0.0.jar --once
```

**Graceful shutdown**: send `SIGINT` (Ctrl-C) or `SIGTERM`; the process finishes the current batch before exiting.

## Project structure

```
producer-java/
├── pom.xml
├── README.md
└── src/main/
    ├── java/com/zero/producer/
    │   ├── OutboxRelayApplication.java   # main, CLI args, signal handling, run loop
    │   ├── Settings.java                 # immutable config record loaded from env
    │   ├── DatabaseClient.java           # fetch, markPublished, recordFailure — plain JDBC
    │   ├── KafkaPublisher.java           # wraps KafkaProducer, publish+flush+close
    │   └── EventBuilder.java             # build_event logic, payload normalisation
    └── resources/
        └── logback.xml                   # console appender, INFO level
```
