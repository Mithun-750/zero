# Payment Consumer

Consumes `orders.created` (produced by the outbox relay), applies payments,
and keeps the stream safe from poison events. It is the third piece of the
learning stack: `data-prep` (load orders) → `producer` (outbox relay → Kafka)
→ **`consumer` (Kafka → payments, plus the failure dashboard)**.

Delivery semantics: **at-least-once**. The consumer dedupes durably through
`processed_events` (one row per applied `event_id`) and
`payments.idempotency_key = event_id`, so re-delivered events are no-ops —
you will see `duplicate` counters, never double payments.

## What it does per record

```
raw bytes
   │
   ├─ JSON parse ──✗──► payments.dlq  + event_failures(OPEN) + block aggregate
   │                     (no aggregate identifiable → block skipped)
   ▼
block check: is aggregate blocked? ──yes──► parked_events (replay later), offset committed
   │ no
   ▼
validate (envelope + business rules)
   ├─ permanent ✗──► payments.dlq  + event_failures(OPEN) + block aggregate
   ▼
apply: insert payments (INITIATED) + processed_events  ── 1 transaction
   ├─ transient ✗──► retry ≤3 (backoff+jitter) ──exhausted──► TRANSIENT_EXHAUSTED → DLQ + block
   ▼
commit offset
```

Poison events **never retry forever** and never wedge the partition: other
aggregates on the same partition keep flowing, because blocking is per
`aggregate_id`, not per partition.

## Requirements

- Docker Compose services up (`postgres` + `kafka` on ports 5433/9092)
- `uv`, `python>=3.12` (runs in the repo-root shared `.venv`)
- Orders loaded (`data-prep`) and the relay running (`producer`) or the consumer idles

## Quickstart

```bash
docker compose up -d                     # postgres + kafka
cd data-prep && uv run load-orders -g 10  # create orders → outbox PENDING
cd ../producer && uv run outbox-relay     # outbox → orders.created
cd ../consumer && uv run --active payment-consumer   # consume + serve dashboard
```

Open **http://127.0.0.1:8090**

> `--active` matters: the root `.venv` is the shared environment; a bare
> `uv sync`/`uv run` would create `consumer/.venv` (this was deliberately
> avoided). Alternatively `export UV_PROJECT_ENVIRONMENT=/path/to/zero/.venv`.

## Dashboard (port 8090)

- **Overview cards** — consumed/applied/duplicate/parked/failed, success rate,
  blocked-aggregate count (red banner), open-failure count, per-partition lag,
  outbox backlog.
- **DLQ table** — every poison event with class/reason/offset and a status
  filter (OPEN / RESOLVED / DISCARDED / all). For OPEN rows:
  - **resolve…** — edit the event JSON (fix the bad field), **apply & unblock**;
  - **discard** — skip the event, optionally discard parked followers too.
- **Blocked aggregates** — parked followers per aggregate.
- **Applied payments / Event ledger** — what the consumer actually did.
- **APIs** — `/api/metrics`, `/api/failures`, `/api/blocked`, `/api/parked`,
  `/api/payments`, `/api/ledger`, `/healthz`, `/readyz`.

## DLQ + aggregate-block drill

```bash
uv run --active inject-fault              # corrupt event (amount=-50) + 2 valid followers
```

Watch: corrupt event → DLQ + `BUSINESS_RULE` failure row, aggregate blocked
(red banner), 2 followers parked, `payments` unchanged for the order. Then:

- **Resolve** — edit `payment_trigger.amount` to `50`, apply & unblock.
  The fixed event applies, followers replay in `kafka_offset` order, the
  aggregate unblocks. Failure row → RESOLVED.
- **Discard** — the bad event is skipped; with "also discard parked
  followers" the followers are dropped too; without it, followers replay.

Resolutions run in ONE transaction — a failure anywhere aborts everything
(nothing is applied or unblocked, failure stays OPEN).

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_CONSUMER_GROUP` | `payment-consumer` | Consumer group id |
| `KAFKA_CONSUMER_CLIENT_ID` | `payment-consumer` | Client id (distinct from the relay's `KAFKA_CLIENT_ID`) |
| `KAFKA_ORDERS_TOPIC` / `KAFKA_DLQ_TOPIC` | `orders.created` / `payments.dlq` | Topics |
| `CONSUMER_MAX_RETRIES` | `3` | Transient retry budget per record |
| `CONSUMER_RETRY_BACKOFF_SECONDS` | `0.5` | Backoff base (attempts grow 1×/2×/4× + jitter) |
| `DASHBOARD_PORT` / `DASHBOARD_HOST` | `8090` / `127.0.0.1` | Dashboard bind |

## Limitations (by design)

- **Single consumer instance** — same limitation as the relay; partition
  ownership is not coordinated beyond consumer-group assignment.
- **In-memory counters reset on restart** — durable truth is PostgreSQL
  (`payments`, `processed_events`, `event_failures`, `parked_events`,
  `aggregate_processing_state`); only the rate cards reset.
- **Raw event payload is not persisted** — the payments row + ledger are the
  durable record. The original bytes survive in `event_failures.raw_payload`
  only for failures.
- **Parked followers replay inside the resolve request** — a poison
  *follower* aborts the whole resolution with a 500; fix or discard both.

## Tests

```bash
uv run --active python -m unittest discover -s tests -v
```

29 unit tests, no broker/database required (module seams are mocked):
`test_classify.py` (validator), `test_dlq.py` (envelope),
`test_pipeline.py` (every decision path incl. DLQ-failure no-commit),
`test_resolve.py` (fix/drain/unblock ordering, discard semantics,
abort-on-invalid).
