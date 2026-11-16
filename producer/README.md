# Outbox Producer

The producer is a relay that reads committed `PENDING` events from PostgreSQL's `outbox` table and publishes them to Kafka. It is separate from `data-prep`: data-prep creates orders, while this project transports outbox events.

## Event flow

```text
PostgreSQL transaction
  ├─ orders
  ├─ order_items
  └─ trigger writes OrderCreated to outbox (PENDING)
                 │
                 ▼
          producer relay
                 │ publish and wait for broker acknowledgement
                 ▼
          orders.created
                 │
                 ▼
       future payment consumer
```

The relay does not publish directly from the order loader. If Kafka is unavailable, the event remains `PENDING`; the relay increments `retry_count`, records the latest error, and tries again on a later poll.

## Requirements

- Docker Desktop with Docker Compose, with this repository's PostgreSQL and Kafka services running.
- `uv`.

The project requires Python 3.15 or newer. `uv` can install a compatible Python version and creates a project-local `.venv`.

## Install and run

Run these commands from the repository root in a producer terminal:

```powershell
Set-Location producer
uv sync
uv run outbox-relay
```

The relay polls continuously. To process one batch and exit:

```powershell
uv run outbox-relay --once
```

The default batch size is 100 rows, and the default poll interval is two seconds. The relay processes the oldest pending rows first. It currently expects a single relay instance; it does not claim rows for coordination across multiple instances.

## Code layout

| File | Responsibility |
|---|---|
| `src/producer/config.py` | Loads `.env` and environment variables and validates numeric connection/poll settings. |
| `src/producer/database.py` | Opens the PostgreSQL connection, reads pending rows, marks acknowledged events as published, and records publish failures. |
| `src/producer/kafka_publisher.py` | Configures the Kafka client, serializes event JSON, uses the aggregate ID as the key, and waits for the send result. |
| `src/producer/outbox_relay.py` | Builds the event envelope, polls and publishes batches, logs outcomes, and handles shutdown signals. |
| `tests/test_outbox_relay.py` | Tests stable event IDs and rejects invalid event payloads. |

## Configuration

The relay loads environment variables from `.env` if present, otherwise uses these local defaults:

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5433` | PostgreSQL host port |
| `POSTGRES_DB` | `order_db` | Database name |
| `POSTGRES_USER` | `postgres` | Database user |
| `POSTGRES_PASSWORD` | `postgres` | Local-development password |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka address from the host |
| `KAFKA_ORDERS_TOPIC` | `orders.created` | Producer destination topic |
| `KAFKA_CLIENT_ID` | `order-outbox-relay` | Kafka client identifier |
| `OUTBOX_BATCH_SIZE` | `100` | Maximum pending events fetched per poll |
| `OUTBOX_POLL_INTERVAL_SECONDS` | `2` | Delay between polls |

These are local-learning defaults. Do not use the example database credentials or plaintext Kafka listener for a production deployment. `KAFKA_DLQ_TOPIC` is defined in the root environment template for the future consumer; the producer does not publish to the DLQ.

## Event contract

The producer publishes JSON to `orders.created` with this envelope:

```json
{
  "event_id": "outbox-row-uuid",
  "event_type": "OrderCreated",
  "event_version": 1,
  "occurred_at": "2026-11-16T15:26:08.086748+00:00",
  "aggregate_type": "ORDER",
  "aggregate_id": "order-uuid",
  "source": "order-service",
  "data": {
    "order": {
      "order_id": "order-uuid",
      "order_number": "ORD-20261116-123456",
      "customer_id": "CUST-1001",
      "customer_name": "Example Customer",
      "customer_email": "customer@example.com",
      "total_amount": 199.99,
      "currency": "USD",
      "status": "CREATED"
    },
    "payment_trigger": {
      "action": "INITIATE_PAYMENT",
      "amount": 199.99,
      "currency": "USD",
      "target_account": "CUST-1001"
    }
  }
}
```

The Kafka message key is `aggregate_id`, so events for an order route to the same partition. The current payload does not contain `order_items`; consumers cannot validate individual products until the event contract is extended.

The canonical `event_id` is the PostgreSQL outbox row's `id`. The payload's prior `event_id`, if any, is not used. Retries therefore keep the same ID, which lets a future consumer deduplicate deliveries.

## Delivery behavior

For each outbox row, the relay:

1. Reads a limited batch of `PENDING` rows.
2. Builds the event envelope and publishes to `orders.created` using `acks=all` and producer idempotence.
3. Waits for the Kafka send result.
4. On acknowledgement, updates the row to `PUBLISHED` and sets `processed_at`.
5. On publish failure, leaves the row `PENDING`, increments `retry_count`, and records the error.

PostgreSQL and Kafka do not share one transaction. If Kafka accepts a message and the relay exits before updating PostgreSQL, the event can be published again after restart. Delivery is therefore **at least once**, not exactly once end-to-end.

## Watch messages and status

Start a Kafka console reader in a separate terminal from the repository root:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders.created --from-beginning
```

This command displays records; it is not the payment consumer. Press `Ctrl+C` to stop it.

Check producer status from the repository root:

```powershell
docker compose exec -T postgres psql -U postgres -d order_db -c "SELECT status, count(*) FROM outbox GROUP BY status ORDER BY status;"
```

Run the focused event-contract tests from the producer directory:

```powershell
uv run python -m unittest discover -s tests -v
```

## Current boundaries

- The local Compose configuration runs one Kafka broker and creates `orders.created` and `payments.dlq` with replication factor 1. It is for functional development, not broker-failure demonstrations.
- The payment consumer, product catalog, payment state, consumer retries, and DLQ publishing are not part of this producer.
- The relay processes all pending outbox rows, including older ones. Starting it continuously will drain the backlog.
- Do not run multiple relay instances until safe row claiming or leasing is implemented.

## End-to-end local walkthrough

The relay runs on the host and connects to PostgreSQL and Kafka using the host ports. Kafka utilities run inside the Kafka container. Start each long-running command in its own PowerShell terminal.

### 1. Start the infrastructure

From the repository root:

```powershell
docker compose up -d
docker compose ps
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Wait until PostgreSQL and Kafka report healthy. The output should include `orders.created` and `payments.dlq`. `kafka-init` is a one-shot service; it exits after creating those topics successfully.

### 2. Create an order

In another terminal, generate and load one order:

```powershell
Set-Location data-prep
uv sync
uv run load-orders --generate 1
```

The loader creates the order and line items in PostgreSQL. The database trigger creates the outbox row in the same transaction. Confirm that there is a pending event:

```powershell
docker compose exec -T postgres psql -U postgres -d order_db -c "SELECT id, aggregate_id, event_type, status FROM outbox ORDER BY created_at DESC LIMIT 5;"
```

### 3. Start a Kafka console reader

In a third terminal, from the repository root:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders.created --from-beginning
```

This command waits for events and prints each event as JSON. With `--from-beginning`, it also prints events already stored in the topic, so output can be large if the topic has a backlog. Press `Ctrl+C` to stop the console reader.

### 4. Start the relay

In a fourth terminal:

```powershell
Set-Location producer
uv sync
uv run outbox-relay
```

The relay polls every two seconds by default, publishes pending rows in batches of 100, and continues running. Watch its logs and the console reader. The relay will also publish older pending rows before newer ones.

For a one-batch demonstration instead of a continuous process:

```powershell
uv run outbox-relay --once
```

To limit that run to one pending row in PowerShell:

```powershell
$env:OUTBOX_BATCH_SIZE = "1"
uv run outbox-relay --once
Remove-Item Env:\OUTBOX_BATCH_SIZE
```

The row is selected by oldest `created_at`, so this processes the oldest pending event, not necessarily the order just generated.

## Event envelope reference

| Field | Meaning |
|---|---|
| `event_id` | The outbox row UUID. It remains stable if the relay publishes the same row again. |
| `event_type` | Event name, currently `OrderCreated`. |
| `event_version` | Contract version, currently `1`. |
| `occurred_at` | Timestamp copied from the outbox payload, falling back to the row's creation time. |
| `aggregate_type` | Business entity type, currently `ORDER`. |
| `aggregate_id` | Order UUID. This is also the Kafka message key. |
| `source` | Producer identifier, currently `order-service`. |
| `data.order` | Order header fields copied from the database trigger payload. |
| `data.payment_trigger` | Payment action, amount, currency, and target account copied from the trigger payload. |

The producer deliberately overrides any `event_id` in the trigger payload with the outbox row ID. It does not create a new event ID for each retry. The event does not currently include `order_items`, so a consumer cannot yet validate purchased product IDs against `items.json` or another catalog.

## Outbox lifecycle and database queries

The intended producer-owned state transition is:

```text
PENDING ── Kafka acknowledgement received ──► PUBLISHED
   │
   └── publish error ──► remain PENDING, increment retry_count, save error_message
```

`processed_at` records when the relay marked an event `PUBLISHED`; it does not mean payment processing completed. The producer does not currently move events to `FAILED` or apply a retry limit. A malformed event or repeated broker failure remains pending and is retried on later polls.

View counts by status:

```powershell
docker compose exec -T postgres psql -U postgres -d order_db -c "SELECT status, count(*) FROM outbox GROUP BY status ORDER BY status;"
```

Inspect recent rows and any recorded errors:

```powershell
docker compose exec -T postgres psql -U postgres -d order_db -c "SELECT id, aggregate_id, status, retry_count, processed_at, error_message FROM outbox ORDER BY created_at DESC LIMIT 10;"
```

Describe the topic:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders.created
```

## Delivery guarantees and failure windows

The relay waits for the Kafka send result before updating PostgreSQL. This avoids marking an event published when Kafka has not acknowledged it. However, the database update and Kafka publish are not one distributed transaction:

1. Kafka may acknowledge the message.
2. The relay may stop before it updates the outbox row.
3. On restart, that row is still `PENDING` and can be published again.

This is expected **at-least-once** behavior. Kafka producer idempotence can handle certain producer retries within a producer session; it does not close the crash window between Kafka and PostgreSQL. A consumer must use `event_id` for durable idempotency.

If publication fails, the relay stores the latest error (up to 2,000 characters) and increments `retry_count`. It continues to the next row in the batch, then polls again. If PostgreSQL itself is unavailable while recording the error or changing status, the relay exits with an error rather than claiming success.

The code fetches rows without a database lease or `FOR UPDATE SKIP LOCKED` claim. Run one relay instance only. Multiple instances can fetch and publish the same row concurrently.

## Environment and dependency notes

- `producer/pyproject.toml` declares the producer dependencies and `outbox-relay` command.
- `producer/uv.lock` pins the resolved dependency versions for repeatable installs.
- `uv sync` creates the local `.venv` and installs the locked dependencies.
- `.env.example` is at the repository root. Copy it to `.env` if you need to change local settings; `.env` is ignored by Git.
- The default Kafka listener is unauthenticated plaintext on localhost. The example PostgreSQL username/password are also for local development only.

## Troubleshooting

| Symptom | Checks |
|---|---|
| Relay cannot connect to PostgreSQL | Run `docker compose ps`; confirm PostgreSQL is healthy and `POSTGRES_PORT` matches the mapped host port, normally `5433`. |
| Relay cannot connect to Kafka | Confirm Kafka is healthy, `KAFKA_BOOTSTRAP_SERVERS` is normally `localhost:9092`, and port `9092` is not occupied by another service. |
| No event appears in the console reader | Check for `PENDING` rows in PostgreSQL, confirm the console is subscribed to `orders.created`, and inspect relay logs. |
| Event stays `PENDING` | Inspect `retry_count` and `error_message`; verify Kafka is healthy and the payload contains an `order` object. |
| Topic is missing | Run `docker compose up -d` and inspect `docker compose ps -a` and `docker compose logs kafka-init`. |
| `uv` is not recognized | Restart the terminal after installing `uv`; `uv run` must be run from the `producer` directory for this project. |

## Tests

The focused unit tests cover event-envelope creation, stable event IDs, and invalid payload rejection:

```powershell
Set-Location producer
uv run python -m unittest discover -s tests -v
```

The tests do not require a live database or Kafka broker. The end-to-end walkthrough above verifies the integration with the local services.
