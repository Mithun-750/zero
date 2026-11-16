# Data Prep - Order Generation & Outbox Loader

This module generates mock e-commerce orders and loads them into PostgreSQL, demonstrating the **Transactional Outbox Pattern**.

## Scripts

### 1. Generate Orders (`generate_orders.py`)

Generates synthetic orders with realistic items, prices, and customer info using Faker (default: 1000 orders).

```bash
uv run generate-orders
# or specify custom count:
uv run generate-orders -c 500 -o data/orders.json
```

### 2. Load Orders with Trigger-based Outbox Records (`load_orders.py`)

Reads orders from a JSON file (or generates them on the fly) and inserts them into PostgreSQL within a transaction:
- Inserts order record into `orders`
- Database trigger `trg_order_created_outbox` automatically fires on `orders` INSERT and writes the `OrderCreated` event into `outbox` with payment trigger payload
- Inserts item records into `order_items`

```bash
uv run load-orders -i data/orders.json
# or on the fly:
uv run load-orders -g 15
```

### 3. Quick Test Load (`test_load.py`)

Generates a small test batch (default: 100 orders), saves to a temporary test file, inserts them, and immediately queries & displays the newly auto-generated outbox events with their payment triggers:

```bash
# Default: 100 test orders
uv run test-load

# Custom: e.g. 20 orders
uv run test-load -c 20
```
