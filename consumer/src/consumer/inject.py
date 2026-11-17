"""Fault injector: publish a corrupt OrderCreated event (+ valid followers)
straight to orders.created so you can watch the DLQ / block / park / resolve
flow in the dashboard.

Usage:
  uv run --active inject-fault                          # latest order, 2 followers
  uv run --active inject-fault --order-id <uuid> --followers 3
  uv run --active inject-fault --order-id <uuid> --corrupt-amount -50

With default corrupt value, every follower is a *valid* event and gets parked;
the corrupt event itself goes to the DLQ and blocks the aggregate.
"""
from __future__ import annotations

import argparse
import json
import logging
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from consumer import db
from consumer.config import load_settings

logger = logging.getLogger("consumer.inject")


def build_event(order_id: str, amount, event_id: str | None = None) -> dict:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": "OrderCreated",
        "event_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "aggregate_type": "ORDER",
        "aggregate_id": order_id,
        "source": "fault-injector",
        "data": {
            "order": {
                "order_id": order_id,
                "customer_id": "CUST-INJECT",
                "total_amount": amount,
                "currency": "INR",
                "status": "CREATED",
            },
            "payment_trigger": {
                "action": "INITIATE_PAYMENT",
                "amount": amount,
                "currency": "INR",
                "target_account": "CUST-INJECT",
            },
        },
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = load_settings()

    parser = argparse.ArgumentParser(description="Inject a corrupt event + valid followers")
    parser.add_argument("--order-id", help="Existing ORDER id (must exist in orders table)")
    parser.add_argument("--followers", type=int, default=2, help="Number of valid follower events to publish after the corrupt one")
    parser.add_argument("--corrupt-amount", type=float, default=-50.0, help="Amount used for the corrupt event (must be <= 0 to be rejected)")
    parser.add_argument("--amount", type=float, default=99.0, help="Amount for the valid follower events")
    args = parser.parse_args()

    order_id = args.order_id
    if not order_id:
        conn = db.connect(settings)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM orders ORDER BY created_at DESC LIMIT 1")
                row = cur.fetchone()
        finally:
            conn.close()
        if not row:
            raise SystemExit("No orders found — run load-orders first, or pass --order-id")
        order_id = str(row["id"])
    logger.info("Using aggregate (order) id=%s", order_id)

    # Fresh event_id per injection → never deduped. A corrupt-amount value
    # <= 0 is what the validator rejects (BUSINESS_RULE), sending the event
    # to the DLQ and blocking the aggregate; the followers are fully valid
    # and land in parked_events to be replayed on resolution.
    corrupt = build_event(order_id, args.corrupt_amount)
    followers = [build_event(order_id, args.amount) for _ in range(args.followers)]

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id=f"{settings.kafka_client_id}-injector",
        acks="all",
        enable_idempotence=True,
    )
    plan = [("corrupt", corrupt)] + [("follower", e) for e in followers]
    try:
        for label, event in plan:
            value = json.dumps(event, separators=(",", ":")).encode("utf-8")
            meta = producer.send(
                settings.kafka_orders_topic, key=order_id.encode("utf-8"), value=value
            ).get(timeout=30)
            logger.info(
                "Published %s event_id=%s to %s partition=%d offset=%d",
                label, event["event_id"], settings.kafka_orders_topic, meta.partition, meta.offset,
            )
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=10)

    logger.info(
        "Done. Expected: corrupt event -> DLQ + aggregate blocked; %d follower(s) -> parked. "
        "Watch http://%s:%d\nThen: %s is BLOCKED — open the failures table, hit 'resolve' "
        "(edit payload, e.g. set amount to 50) or 'discard' to unblock.",
        args.followers, settings.dashboard_host, settings.dashboard_port, order_id,
    )


if __name__ == "__main__":
    main()
