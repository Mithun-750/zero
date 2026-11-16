# Transactional Outbox and Kafka - Learning Project

This learning project demonstrates the **Transactional Outbox Pattern** with PostgreSQL and Apache Kafka: an order service commits order data and outbox events in a single database transaction, a dedicated producer relay transports those events to Kafka, and an idempotent consumer processes events with **effectively-once database business processing** using PostgreSQL idempotency and manual Kafka offset management.

---

## Architecture Overview

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
                            [PostgreSQL] (Port 5434)
                            |-- orders
                            |-- order_items
                            +-- outbox (status = 'PENDING')
                                  |
                     (Outbox relay polls & publishes)
                                  | (acks=all, enable.idempotence=true, key=aggregate_id)
                                  v
   ┌─────────────────────────────────────────────────────────────┐
   │ Apache Kafka 4.4.0 (3-Broker KRaft Cluster)                 │
   │ Topic: orders.created (Partitions=3, RF=3, min.isr=2)       │
   │  ├── Broker 1: localhost:9092                               │
   │  ├── Broker 2: localhost:9094                               │
   │  └── Broker 3: localhost:9096                               │
   └─────────────────────────────────────────────────────────────┘
                                  |
                     (Order consumer reads event)
                                  | (enable_auto_commit=False)
                                  v
   ┌─────────────────────────────────────────────────────────────┐
   │ Order Consumer (producer.consumer)                          │
   │                                                             │
   │  1. Decode & validate JSON (event_id, aggregate_id)         │
   │  2. BEGIN PostgreSQL Transaction                            │
   │     ├── INSERT INTO processed_events (ON CONFLICT DO NOTHING│
   │     └── If inserted: execute test business action           │
   │     COMMIT PostgreSQL Transaction                           │
   │  3. If DB committed -> commit Kafka offset (offset + 1)     │
   └─────────────────────────────────────────────────────────────┘
```

---

## Database Schema Highlights

The database schema (`schema.sql`) contains:
- **`orders`**: Stores core order records with `total_amount`, currency (`INR`), and status (`CREATED`).
- **`order_items`**: Line items linked to orders via foreign key.
- **`outbox`**: Transactional outbox table storing event metadata (`aggregate_id` UUID, `event_type`, `topic = 'orders.created'`, JSONB `payload`, `status = 'PENDING'`, and worker lease locks).
- **`payments`**: Records payment attempts (`INITIATED`, `PROCESSING`, `SUCCESS`, `FAILED`) with `idempotency_key` and gateway references.
- **`processed_events`**: Consumer idempotency ledger to track processed event IDs (`event_id UUID PRIMARY KEY`) and prevent duplicate business processing.
- **`aggregate_processing_state`**: State management table to pause/block aggregates upon permanent consumer failures / DLQ routing.

---

## Repository Structure

```
.
├── docker-compose.yml              # PostgreSQL and 3-broker Apache Kafka services
├── .env.example                    # Template environment variables
├── .env                            # Local environment configuration
├── schema.sql                      # Database schema source of truth (mounted in Docker)
├── producer/
│   ├── README.md                   # Producer setup, event contract, and operations
│   ├── pyproject.toml              # Dependencies and CLI scripts (outbox-relay, order-consumer)
│   ├── uv.lock                     # Locked dependencies
│   ├── src/producer/
│   │   ├── config.py               # Typed settings loading
│   │   ├── database.py             # Outbox polling, marking published, atomic idempotency
│   │   ├── kafka_publisher.py      # Idempotent Kafka publisher (acks=all)
│   │   └── consumer.py             # Idempotent consumer with manual offset management
│   └── tests/
│       ├── test_outbox_relay.py    # Unit tests for envelope building
│       ├── test_idempotency.py     # Live PostgreSQL atomic idempotency & concurrency tests
│       ├── test_consumer.py        # Consumer unit & mocked offset commit tests
│       └── test_crash_recovery.py  # Live crash-window recovery integration test
└── data-prep/
    ├── pyproject.toml              # Data prep dependencies and CLI scripts
    ├── uv.lock                     # Locked dependencies
    └── src/data_prep/
        ├── generate_orders.py      # Synthetic order generation script
        ├── load_orders.py          # Database loader with atomic Outbox persistence
        └── test_load.py            # Test runner for quick outbox trigger verification
```

---

## Quickstart

### 1. Start Infrastructure

```bash
docker compose up -d
```

- **PostgreSQL (`zero-outbox-postgres`)**: Runs PostgreSQL 17 on port `5434` (`${POSTGRES_PORT:-5434}`) and automatically initializes `schema.sql`.
- **Kafka Cluster (`zero-outbox-kafka-1`, `2`, `3`)**: 3-broker KRaft cluster on ports `9092`, `9094`, and `9096`.
- **`kafka-init`**: Automatically creates `orders.created` and `payments.dlq` with 3 partitions, replication factor 3, and `min.insync.replicas=2`.

### 2. Generate and Load Test Orders

```bash
cd data-prep
uv run test-load -c 5
```

Inserts 5 orders into `orders` and `order_items`; the PostgreSQL trigger automatically populates 5 `PENDING` rows in `outbox`.

### 3. Run Outbox Relay (Producer)

In a terminal:

```bash
cd producer
uv run outbox-relay --once
```

Polls `PENDING` outbox records, publishes them to `orders.created` with `aggregate_id` as the message key, and updates status to `PUBLISHED` upon broker acknowledgment.

### 4. Run Order Consumer

In a terminal:

```bash
cd producer
uv run order-consumer --once
```

Options:
- `--once`: Process available messages and exit.
- `--max-messages N`: Process at most N messages and exit.
- Default: Continuous streaming listener.

### 5. Run Full Automated Test Suite

```bash
cd producer
uv run python -m unittest discover tests
```

Executes all 21 unit, database integration, concurrency, and live crash-recovery tests.

---

## Reliability & Idempotency Architecture

### Effectively-Once Database Processing
In distributed event systems, exactly-once network transmission across independent distributed systems (PostgreSQL and Kafka) is physically impossible. Instead, we achieve **effectively-once database business processing** via:

1. **Transactional Outbox Ingestion**: Business updates and outbox events commit in the same ACID database transaction.
2. **At-Least-Once Transport**: The Outbox Relay retries upon network or broker failure, guaranteeing no event is lost.
3. **Consumer-Side Atomic Idempotency**:
   ```sql
   INSERT INTO processed_events (event_id, consumer_name, aggregate_id)
   VALUES (%s, %s, %s)
   ON CONFLICT (event_id) DO NOTHING
   RETURNING event_id;
   ```
   If returned: executes business action and commits.
   If not returned: skips business action and commits.
4. **Manual Kafka Offset Commits**:
   `enable_auto_commit=False`. The consumer explicitly commits the offset (`record.offset + 1`) **only after** PostgreSQL commits.

### Crash-Window Recovery Behavior
If the consumer crashes after PostgreSQL commits but before the Kafka offset commit reaches the broker:
- Upon consumer restart, Kafka redelivers the message.
- The consumer runs the idempotency query.
- PostgreSQL detects the existing `event_id` and returns `DUPLICATE_SKIPPED`.
- The business callback is skipped, preventing duplicate business operations.
- The Kafka offset is safely committed.

---

## SQL Verification Queries

Connect to PostgreSQL:

```bash
docker compose exec postgres psql -U postgres -d order_db
```

Check outbox status:
```sql
SELECT status, COUNT(*) FROM outbox GROUP BY status;
```

Check processed events ledger:
```sql
SELECT event_id, consumer_name, aggregate_id, processed_at
FROM processed_events
ORDER BY processed_at DESC;
```

Verify zero duplicate business operations:
```sql
SELECT event_id, COUNT(*)
FROM processed_events
GROUP BY event_id
HAVING COUNT(*) > 1;
```

---

## Scenario DL-09: Kafka Partition Leader Failure

The system was subjected to chaos scenario **DL-09** using a 1,000-event workload ($E_1 \dots E_{1000}$):
- **Workload**: 1,000 distinct orders and outbox records generated (`DL09-2026-001`).
- **Failure Injection**: Broker 1 (`kafka-1`, leading Partition 0) was terminated mid-flight around event 500.
- **Automatic Leader Election**: KRaft controller detected leader unavailability and promoted Broker 2 as the new leader of Partition 0 in **6.108 seconds**.
- **Producer Resilience**: The producer client refreshed cluster metadata via surviving bootstrap nodes, redirected in-flight traffic to the new leader, and resumed publishing in **6.122 seconds**.
- **Consumer Continuity**: Order consumer drained all messages and committed offsets without acknowledging uncommitted events.
- **Replica Restoration**: Broker 1 container was restarted; all 3 replicas rejoined the ISR across all partitions in **5.914 seconds**.
- **Reconciliation**:
  - Accepted Outbox Events: `1000`
  - Published to Kafka: `1000`
  - Processed by Consumer: `1000`
  - Missing Acknowledged Events: `0`
  - Duplicate Business Operations: `0`
- **Observed RPO**: **0** (Zero loss of acknowledged business events)
- **Observed RTO**: **16.743 seconds** (Target: $\le 2$ hours)

To execute the DL-09 chaos experiment:
```powershell
uv run python -m chaos.dl09.run_dl09 --run-id DL09-2026-001 --broker kafka-1 --fail-at 500 --count 1000
```

---

## Scope & Status Notes

- **Phase 1 & 2**: KRaft 3-broker cluster, topic replication, and baseline verification complete.
- **Phase 3**: Idempotent Kafka consumer and manual offset management complete and verified.
- **Phase 4 (DL-09)**: 1,000-event test harness and Partition Leader Failure chaos validation complete (RPO=0, RTO=16.7s).
- **Upcoming Phase**: Scenario DL-10: Multiple Kafka Broker Failure and Quorum Degradation.
