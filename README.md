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
├── docker-compose.yml              # PostgreSQL and Apache Kafka services
├── .env.example                    # Template environment variables
├── .env                            # Local environment configuration
├── schema.sql                      # Single database schema source of truth (mounted in Docker)
├── producer/
│   ├── README.md                   # Producer setup, event contract, and operations
│   ├── pyproject.toml              # Producer dependencies and CLI entry point
│   ├── uv.lock                     # Locked producer dependencies
│   └── src/producer/               # Outbox polling and Kafka publishing
└── data-prep/
    ├── pyproject.toml              # Python project with uv dependencies and CLI scripts
    ├── uv.lock                     # Locked data preparation dependencies
    └── src/
        └── data_prep/
            ├── __init__.py
            ├── generate_orders.py  # Synthetic order generation script
            ├── load_orders.py      # Database loader with atomic Outbox persistence
            └── test_load.py        # Test runner for quick outbox trigger verification
```

---

## Quickstart

### 1. Start PostgreSQL and Kafka

```bash
docker compose up -d
```

- **PostgreSQL (`zero-outbox-postgres`)**: Runs PostgreSQL 17 on port `5433` (`${POSTGRES_PORT:-5433}`) and automatically initializes `schema.sql`.
- **Kafka (`zero-outbox-kafka`)**: Single-node Apache Kafka 4.4.0 in KRaft mode on port `9092`.
- **`kafka-init`**: Automatically creates the `orders.created` and `payments.dlq` topics.

### 2. Generate Orders

Generate mock orders and save them to `data-prep/data/orders.json`:

```bash
cd data-prep
uv sync
uv run generate-orders
```

Options:
- `-c`, `--count`: Number of orders to generate (default: 1000)
- `-o`, `--output`: Output file path (default: `data/orders.json`)

### 3. Load Orders into PostgreSQL

Load generated orders into the database:

```bash
cd data-prep
uv sync
uv run load-orders
```

Or generate and load directly on the fly:

```bash
uv run load-orders -g 1000
```

### 4. Quick Test Runner
### 4. Quick Test Runner

To test the database trigger with a test batch (default: 100 orders):
To test the database trigger with a test batch (default: 100 orders):

```bash
cd data-prep
uv sync

# Default: 100 test orders
# Default: 100 test orders
uv run test-load

# Custom count: e.g. 20 orders
uv run test-load -c 20
# Custom count: e.g. 20 orders
uv run test-load -c 20
```

### 5. Verify in PostgreSQL

Connect to the database:

```bash
docker exec -it zero-outbox-postgres psql -U postgres -d order_db
```

Check outbox events:

```sql
SELECT id, aggregate_id, event_type, topic, status, payload->'payment_trigger' AS payment_trigger 
FROM outbox 
LIMIT 5;
```

### 6. Run the Outbox Relay (Producer)

In another terminal:

```bash
cd producer
uv sync
uv run outbox-relay
```

- The relay polls pending outbox rows from PostgreSQL, publishes them to `orders.created` with `aggregate_id` as the Kafka key, and marks rows `PUBLISHED` only after Kafka acknowledges delivery.
- Run `uv run outbox-relay --once` to process a single batch and exit.
- On publish failure, the relay increments `retry_count`, logs the error message, and leaves the row `PENDING` for subsequent retry.

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
