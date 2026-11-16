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

### 3. Quick Test Load with Error Injection (`test_load.py`)

Generates a test batch with configurable valid and corrupted records to test transaction rollbacks and verify that no orphan outbox records are created:

```bash
# Default: 100 valid orders + 0 corrupted orders
uv run test-load

# Custom: 100 valid orders + 5 corrupted orders
uv run test-load -c 100 -k 5
```

Injected corruption scenarios include:
- Negative order `total_amount` (`CHECK total_amount >= 0`)
- Missing / NULL `customer_id` (`NOT NULL` constraint)
- Negative item `quantity` (`CHECK quantity > 0`)
- Negative item `unit_price` (`CHECK unit_price >= 0`)
- Invalid UUID formatting
