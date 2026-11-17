# Kafka Reliability Test Workloads

This directory archives the synthetic workload payloads utilized during the Kafka reliability evaluations, along with the reproduction utilities.

---

## 1. Workload Datasets

| File | Associated Experiment | Order Count | Description |
| :--- | :--- | :--- | :--- |
| `baseline-orders.json` | Healthy Cluster Baseline | 1,000 | Baseline order dataset inserted prior to steady-state verification. |
| `broker-failure-orders.json` | Partition Leader Broker Failure | 1,000 | Workload injected to test broker crash, failover, and log recovery. |
| `controller-failure-orders.json` | KRaft Controller Failure | 1,000 | Workload injected to test controller election and dual-role failover. |

Each file contains an array of 1,000 structured e-commerce orders formatted according to the application domain schema.

---

## 2. Order Payload Schema

Each order record includes:
- `id` (UUID): Unique primary key.
- `order_number` (String): Business identifier (e.g. `ORD-YYYYMMDD-XXXXXX`).
- `customer` (Object): Customer demographic details (`customer_id`, `name`, `email`).
- `total_amount` (Decimal): Total order currency value.
- `currency` (String): ISO 4217 code (`USD`).
- `status` (String): Initial state (`CREATED`).
- `created_at` (ISO 8601 Timestamp).
- `items` (Array): Line item breakdown (`product_id`, `name`, `quantity`, `unit_price`, `subtotal`).

Database inserts trigger the PostgreSQL trigger `trg_order_created_outbox`, which automatically records a corresponding `PENDING` outbox entry in the transactional outbox table within the same ACID database transaction.

---

## 3. Workload Regeneration

To regenerate or insert a fresh batch of 1,000 unique orders into the PostgreSQL database without collision:

```powershell
$env:PYTHONPATH="data-prep/src"
py -3 reliability-testing/kafka-reliability/workloads/generate_workload.py
```

The script queries existing order numbers from PostgreSQL to guarantee zero unique key collisions, commits in batched transactions, and verifies database insertion before proceeding.

