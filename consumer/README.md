# consumer

Java payment consumer. Subscribes to Kafka `orders.created`,
processes events through a validation + apply pipeline, and writes results to PostgreSQL.

## Prerequisites

- Java 27+
- Maven 3.10+
- Running PostgreSQL (see root `docker-compose.yml`)
- Running Kafka

## Build

```bash
cd consumer
mvn package -q
# produces: target/consumer-1.0.0.jar
```

## Run

### Using Maven (Recommended)

From the project root:
```bash
# Continuous consumer + dashboard
mvn -pl consumer exec:java

# Poll once and exit
mvn -pl consumer exec:java -Dexec.args="--once"

# Run fault injector CLI
mvn -pl consumer exec:java -Dexec.mainClass="com.zero.consumer.FaultInjector"
```

Or from within `consumer/`:
```bash
cd consumer
mvn exec:java
mvn exec:java -Dexec.args="--once"
mvn exec:java -Dexec.mainClass="com.zero.consumer.FaultInjector"
```

### Using Executable JAR

```bash
# Continuous mode (default)
java -jar target/consumer-1.0.0.jar

# Poll once and exit (dev/verification)
java -jar target/consumer-1.0.0.jar --once
```

## Configuration

Environment variables (with defaults). Copy `../.env` to override:

| Variable | Default |
|---|---|
| `POSTGRES_HOST` | `localhost` |
| `POSTGRES_PORT` | `5433` |
| `POSTGRES_DB` | `order_db` |
| `POSTGRES_USER` | `postgres` |
| `POSTGRES_PASSWORD` | `postgres` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `KAFKA_ORDERS_TOPIC` | `orders.created` |
| `KAFKA_DLQ_TOPIC` | `payments.dlq` |
| `KAFKA_CONSUMER_GROUP` | `payment-consumer` |
| `KAFKA_CONSUMER_CLIENT_ID` | `payment-consumer` |
| `CONSUMER_MAX_RETRIES` | `3` |
| `CONSUMER_RETRY_BACKOFF_SECONDS` | `0.5` |
| `DASHBOARD_PORT` | `8090` |
| `DASHBOARD_HOST` | `127.0.0.1` |

## Dashboard

REST dashboard runs at `http://127.0.0.1:8090` by default.

| Route | Description |
|---|---|
| `GET /` | Embedded HTML UI |
| `GET /api/metrics` | In-memory counters + DB gauges |
| `GET /api/failures` | DLQ event failures |
| `GET /api/blocked` | Blocked aggregates |
| `GET /api/parked` | Parked follower events |
| `GET /api/payments` | Recent applied payments |
| `GET /api/ledger` | Recent processed events |
| `POST /api/failures/{id}/resolve` | Resolve a DLQ failure (apply fixed event) |
| `POST /api/failures/{id}/discard` | Discard a DLQ failure |
| `GET /healthz` | Health check |
| `GET /readyz` | Readiness check (pings PostgreSQL) |

## Fault Injector

Inject a corrupt event + valid followers to test the DLQ/block/park/resolve flow:

```bash
java -cp target/consumer-1.0.0.jar com.zero.consumer.FaultInjector \
  [--order-id UUID] [--followers 2] [--corrupt-amount -50.0] [--amount 99.0]
```

## Processing Pipeline

1. **Parse** raw bytes as JSON → if unparseable → DLQ (VALIDATION failure)
2. **Block check** → if aggregate is blocked → park event in `parked_events` → commit offset
3. **Apply** → validate event + business rules → INSERT into `payments` + `processed_events` atomically
   - On permanent error (ValidationException/BusinessRuleException) → DLQ + block aggregate
   - On transient error → retry with exponential backoff; on exhaustion → DLQ
4. **Commit offset** only after a durable outcome (applied, duplicate, parked, or DLQ'd)

## Project Structure

```
consumer/
├── pom.xml
├── README.md
└── src/main/
    ├── java/com/zero/consumer/
    │   ├── PaymentConsumerApplication.java   # main, CLI args, Kafka poll loop, signal handling
    │   ├── Settings.java                     # immutable config from env
    │   ├── DatabaseClient.java               # all JDBC ops
    │   ├── DlqPublisher.java                 # one-off KafkaProducer per DLQ write
    │   ├── Pipeline.java                     # process_record logic
    │   ├── EventValidator.java               # validation + business rules
    │   ├── ConsumerException.java            # base exception
    │   ├── ValidationException.java
    │   ├── BusinessRuleException.java
    │   ├── TransientException.java
    │   ├── DlqPublishException.java
    │   ├── OffsetRejectException.java
    │   ├── Metrics.java                      # in-memory counters
    │   ├── Dashboard.java                    # Javalin REST server
    │   └── FaultInjector.java                # inject-fault CLI
    └── resources/
        └── logback.xml
```
