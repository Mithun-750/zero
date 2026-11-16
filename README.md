# Transactional Outbox Pattern

This project establishes the foundation of the **Transactional Outbox Pattern** for an e-commerce order management and payment workflow.

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
                     (Future Phase: Kafka Relay / CDC)
                                  |
                                  v
                            [Kafka Topic]
                           (orders.created)
                                  |
                                  v
                          [Payment Service]
```

---

## Repository Structure

```
.
├── docker-compose.yml              # PostgreSQL Docker Compose configuration
├── .env.example                    # Template environment variables
├── .env                            # Active environment variables (port 5433 by default)
├── schema.sql                      # Single database schema source of truth (mounted in Docker)
└── data-prep/
    ├── pyproject.toml              # Python project with uv dependencies and CLI scripts
    ├── data/
    │   └── orders.json             # Generated sample orders
    └── src/
        └── data_prep/
            ├── __init__.py
            ├── generate_orders.py  # Synthetic order generation script
            ├── load_orders.py      # Database loader with atomic Outbox persistence
            └── test_load.py        # Test runner with error injection
```

---

## Quickstart

### 1. Start the PostgreSQL Container

```bash
docker compose up -d
```

The database container (`zero-outbox-postgres`) starts with healthchecks and initializes the schema automatically from `schema.sql`.

> **Note:** By default, Postgres is mapped to host port `5433` (`${POSTGRES_PORT:-5433}`) to prevent collisions if port `5432` is occupied on your machine. You can configure this in `.env`.

### 2. Generate Orders

Generate mock orders and save them to `data-prep/data/orders.json`:

```bash
cd data-prep
uv run generate-orders
```

Options:
- `-c`, `--count`: Number of orders to generate (default: 1000)
- `-o`, `--output`: Output file path (default: `data/orders.json`)

### 3. Load Orders into PostgreSQL (with Outbox Events)

Load generated orders into the database:

```bash
cd data-prep
uv run load-orders
```

Or generate and load directly on the fly:

```bash
uv run load-orders -g 1000
```

### 4. Quick Test Runner

To test the database trigger with a test batch (default: 100 orders):

```bash
cd data-prep

# Default: 100 test orders
uv run test-load

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
SELECT id, aggregate_id, event_type, status, payload->'order'->'payment_trigger' AS payment_trigger 
FROM outbox 
LIMIT 5;
```
