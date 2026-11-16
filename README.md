# Transactional Outbox and Kafka - Learning Project

This learning project demonstrates how an order service can persist an order and an outbox event in PostgreSQL, then relay the event to Kafka. It currently includes order data preparation and the producer relay. A payment consumer is not implemented yet.

---

## Architecture Overview

When an order is placed, instead of writing to the database and directly calling a message broker (which introduces distributed consistency / dual-write failure risks), the system writes:
1. The **Order** & **Order Items**
2. An **Outbox Event** (`OrderCreated` with payment trigger payload)

...inside a **single ACID database transaction**.

```
   +-------------------------------------------------------------+
   |                  Single Database Transaction                |
   |                                                             |
   |   1. INSERT INTO orders (order details, total_amount)       |
   |      +-- PostgreSQL Trigger (trg_order_created_outbox) --+  |
   |          |-> Auto-inserts into outbox (payment_trigger) -+  |
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

## Repository Structure

```
.
├── docker-compose.yml              # PostgreSQL Docker Compose configuration
├── .env.example                    # Template environment variables
├── .env                            # Optional local environment file (ignored by Git)
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
            └── test_load.py        # Test runner with error injection
```

---

## Quickstart

### 1. Start PostgreSQL and Kafka

```powershell
docker compose up -d
```

The PostgreSQL container (`zero-outbox-postgres`) starts with a healthcheck and initializes the schema from `schema.sql` on first startup. Kafka runs locally as a single Apache Kafka 4.4.0 KRaft broker. The `kafka-init` service creates the `orders.created` and `payments.dlq` topics.

> **Note:** By default, Postgres is mapped to host port `5433` (`${POSTGRES_PORT:-5433}`) to prevent collisions if port `5432` is occupied on your machine. You can configure this in `.env`.

### 2. Generate Orders

Generate mock orders and save them to `data-prep/data/orders.json`:

```powershell
Set-Location data-prep
uv sync
uv run generate-orders
```

Options:
- `-c`, `--count`: Number of orders to generate (default: 1000)
- `-o`, `--output`: Output file path (default: `data/orders.json`)

### 3. Load Orders into PostgreSQL (with Outbox Events)

Load generated orders into the database:

```powershell
Set-Location data-prep
uv sync
uv run load-orders
```

Or generate and load directly on the fly:

```bash
uv run load-orders -g 1000
```

### 4. Quick Test Runner (Valid + Corrupted Records Verification)

To test the database trigger with a small test batch (default: 100 orders, 0 errors):

```powershell
Set-Location data-prep
uv sync

# Default: 100 valid orders + 0 corrupted orders
uv run test-load

# Custom counts: e.g. 100 valid and 3 corrupted orders
uv run test-load -c 100 -k 3
```

### 5. Verify in PostgreSQL

Connect to the database:

```powershell
docker exec -it zero-outbox-postgres psql -U postgres -d order_db
```

Check outbox events:

```sql
SELECT id, aggregate_id, event_type, status, payload->'order'->'payment_trigger' AS payment_trigger 
FROM outbox 
LIMIT 5;
```

### 6. Install and run the outbox relay

In another terminal:

```powershell
cd producer
uv sync
uv run outbox-relay
```

The relay polls pending outbox rows, sends them to `orders.created` with `aggregate_id` as the Kafka key, and marks them `PUBLISHED` only after Kafka acknowledges delivery. Use `uv run outbox-relay --once` to process one batch and exit. On a publish error, it records the error and retry count while leaving the row `PENDING`.

The outgoing event uses the outbox row's `id` as its stable `event_id`, including when the trigger payload contains a different generated ID. The relay provides at-least-once publication; consumers must deduplicate by `event_id`.

To inspect topic records:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders.created --from-beginning
```

To inspect topic status:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders.created
```

The payment consumer and DLQ publishing/retry behavior are not implemented in this phase. The Kafka console consumer is only a way to inspect messages; it does not process payments.

## Current scope and reliability notes

- Implemented: PostgreSQL transactional outbox, local Kafka topics, and the outbox-to-Kafka producer relay.
- Producer delivery is at least once. A crash after Kafka accepts an event but before PostgreSQL is updated can result in a duplicate; a future consumer should deduplicate using `event_id`.
- The current Kafka event includes order-header and payment-trigger data, not individual order items. The producer payload must be extended before a consumer can validate purchased products against a catalog.
- A single local Kafka broker cannot demonstrate broker high availability or establish an RPO 0 guarantee. Those targets require a replicated setup and recovery testing.

For producer-specific setup, event format, and operations, see [`producer/README.md`](</D:/Projects/DTCC/RPO-RTO Project/producer/README.md>).
