# Transactional Outbox and Kafka - Learning Project

This learning project demonstrates the **Transactional Outbox Pattern** with PostgreSQL and Apache Kafka: an order service commits order data and outbox events in a single database transaction, and a dedicated producer relay transports those events to Kafka for downstream consumers (such as a payment service).

---

## Architecture Overview

When an order is placed, instead of writing to the database and directly calling a message broker (which introduces distributed consistency and dual-write failure risks), the system writes:
1. The **Order** & **Order Items**
2. An **Outbox Event** (`OrderCreated` with payment trigger payload)

...inside a **single ACID database transaction** using a PostgreSQL trigger.

```
   +-------------------------------------------------------------+
   |                  Single Database Transaction                |
   |                                                             |
   |   1. INSERT INTO orders (order details, total_amount)       |
   |      +-- PostgreSQL Trigger (trg_order_created_outbox) --+  |
   |          |-> Auto-inserts into outbox (topic, payload) --+  |
   |   2. INSERT INTO order_items (item details, prices)         |
   +-------------------------------------------------------------+
                                  |
                                  v
                            [PostgreSQL]
                            |-- orders
                            |-- order_items
                            +-- outbox (status = 'PENDING')
                                  |
                     (Outbox relay publishes to Kafka)
                                  |
                                  v
                            [Kafka Topic]
                          (orders.created)
                                  |
                                  v
                       [Future Payment Service]
```

---

## Database Schema Highlights

The database schema (`schema.sql`) contains:
- **`orders`**: Stores core order records with `total_amount`, currency (`INR`), and status (`CREATED`).
- **`order_items`**: Line items linked to orders via foreign key.
- **`outbox`**: Transactional outbox table storing event metadata (`aggregate_id` UUID, `event_type`, `topic = 'orders.created'`, JSONB `payload`, `status = 'PENDING'`, and worker lease locks).
- **`payments`**: Records payment attempts (`INITIATED`, `PROCESSING`, `SUCCESS`, `FAILED`) with `idempotency_key` and gateway references.
- **`processed_events`**: Consumer idempotency ledger to track processed event IDs and prevent duplicate processing.
- **`aggregate_processing_state`**: State management table to pause/block aggregates upon permanent consumer failures / DLQ routing.

---

## Repository Structure

```
.
├── pom.xml                         # Root Maven Parent POM (builds producer & consumer)
├── docker-compose.yml              # PostgreSQL 17 and Apache Kafka 4.4.0 (KRaft)
├── .env.example                    # Template environment variables
├── .env                            # Local environment configuration
├── dev.sh                          # One-command startup script
├── flow.md                         # Detailed end-to-end architecture & order flow
├── schema.sql                      # Single database schema source of truth (mounted in Docker)
├── producer/                       # [JAVA] Outbox relay producer
│   ├── pom.xml                     # Maven build with shade fat jar & exec plugin
│   ├── README.md                   # Producer documentation & CLI usage
│   └── src/main/java/com/zero/producer/
│       ├── OutboxRelayApplication.java # Entry point, CLI args (--once), poll loop
│       ├── Settings.java           # Dotenv & env var configuration
│       ├── DatabaseClient.java     # JDBC outbox polling and status updates
│       ├── KafkaPublisher.java     # KafkaProducer (acks=all, idempotent)
│       └── EventBuilder.java       # Envelope construction & payload normalization
├── consumer/                       # [JAVA] Payment consumer & dashboard
│   ├── pom.xml                     # Maven build with shade fat jar & exec plugin
│   ├── README.md                   # Consumer documentation & dashboard usage
│   └── src/main/java/com/zero/consumer/
│       ├── PaymentConsumerApplication.java # Entry point & manual offset commit loop
│       ├── Pipeline.java           # 3-stage pipeline (Parse -> Block Check -> Apply/Retry)
│       ├── EventValidator.java     # Envelope validation and business rules
│       ├── DatabaseClient.java     # JDBC atomic payment application and state tracking
│       ├── DlqPublisher.java       # Dead-letter queue (DLQ) publisher with metadata headers
│       ├── Dashboard.java          # Embedded Javalin REST & live HTML monitoring UI
│       ├── FaultInjector.java      # Fault injector CLI for poison event simulation
│       └── Metrics.java            # Process metrics & failure classification counters
└── data-prep/                      # [PYTHON] Data preparation and synthetic order generation
    ├── pyproject.toml              # Python project with uv dependencies and CLI scripts
    ├── uv.lock                     # Locked data preparation dependencies
    └── src/data_prep/
        ├── generate_orders.py      # Synthetic order generation script
        ├── load_orders.py          # Database loader with atomic Outbox persistence
        └── test_load.py            # Test runner for quick outbox trigger verification
```

---

## Build Java Services

Build both the Java Producer and Consumer using Maven (Java 27 / Maven 3.10+):

```bash
mvn clean package -DskipTests
```

This compiles both modules and builds executable fat jars:
- `producer/target/producer-1.0.0.jar`
- `consumer/target/consumer-1.0.0.jar`

---

## Quickstart

### 1. Start PostgreSQL and Kafka

```bash
docker compose up -d
```

- **PostgreSQL (`zero-outbox-postgres`)**: Runs PostgreSQL 17 on port `5433` (`${POSTGRES_PORT:-5433}`) and automatically initializes `schema.sql`.
- **Kafka (`zero-outbox-kafka`)**: Single-node Apache Kafka 4.4.0 in KRaft mode on port `9092`.
- **`kafka-init`**: Automatically creates the `orders.created` and `payments.dlq` topics.

### 2. Generate and Load Orders (Python `data-prep`)

Generate mock orders and load them into the database using `data-prep`:

```bash
cd data-prep
uv sync
uv run test-load -c 20
```

This inserts 20 orders, triggering the PostgreSQL `trg_order_created_outbox` trigger to auto-populate the `outbox` table with `status = 'PENDING'`.

### 3. Run the Java Outbox Relay (Producer)

In a terminal, you can run directly with Maven:

```bash
# Run continuous polling with Maven (default every 2 seconds)
mvn -pl producer exec:java

# Or run a single batch and exit
mvn -pl producer exec:java -Dexec.args="--once"
```

Or using the built fat jar:
```bash
java -jar producer/target/producer-1.0.0.jar [--once]
```

- The relay polls `PENDING` outbox rows from PostgreSQL, publishes them to `orders.created` with `aggregate_id` as the Kafka key, and marks rows `PUBLISHED` only after Kafka acknowledges delivery (`acks=all`).
- On publish failure, the relay increments `retry_count`, logs the error, and leaves the row `PENDING` for retry.

### 4. Run the Java Payment Consumer

In another terminal, you can run directly with Maven:

```bash
# Run continuous consumer + embedded dashboard with Maven
mvn -pl consumer exec:java

# Or process available messages and exit
mvn -pl consumer exec:java -Dexec.args="--once"
```

Or using the built fat jar:
```bash
java -jar consumer/target/consumer-1.0.0.jar [--once]
```

- Subscribes to `orders.created` with manual offset commits (at-least-once).
- Checks whether the aggregate is blocked before processing. If blocked, followers are safely parked in `parked_events`.
- Validates the event envelope and domain rules, and records applied payments in `payments` and `processed_events` idempotently.
- On permanent failure, routes the poison event to `payments.dlq`, records the failure in `event_failures`, and pauses the aggregate in `aggregate_processing_state`.
- Launches the live web monitoring dashboard at **http://127.0.0.1:8090**.

### 5. Simulate Faults & DLQ Flow

Inject a poison event (e.g. negative amount) followed by valid follower events to test DLQ routing and aggregate blocking:

```bash
# Via Maven:
mvn -pl consumer exec:java -Dexec.mainClass="com.zero.consumer.FaultInjector"

# Or via JAR:
java -cp consumer/target/consumer-1.0.0.jar com.zero.consumer.FaultInjector
```

Then visit the dashboard at **http://127.0.0.1:8090** to view open failures, inspect parked events, and resolve/discard failures with one click.

### 7. Inspect Kafka Messages

To inspect messages published to the `orders.created` topic:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders.created --from-beginning
```

To inspect topic partition and replica details:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders.created
```

---

## Current Scope and Reliability Notes

- **Implemented**: PostgreSQL transactional outbox pattern via trigger, local Kafka topics (`orders.created`, `payments.dlq`), and the outbox-to-Kafka producer relay.
- **At-Least-Once Delivery**: The relay provides at-least-once delivery. If a crash occurs after broker acknowledgment but before database status update, duplicates can occur. Downstream consumers should deduplicate using `event_id` and the `processed_events` table.
- **Payment Consumer**: The consumer service and dead-letter queue (DLQ) processing represent future phases. The Kafka console consumer allows inspecting events in transit.

For producer-specific configuration, event envelope schema, and operations, see [producer/README.md](producer/README.md).
