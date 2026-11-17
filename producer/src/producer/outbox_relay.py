"""Poll PostgreSQL's outbox and publish events to Kafka."""

from __future__ import annotations

import argparse
import logging
import signal
import threading
from datetime import datetime
from typing import Any

from producer.config import Settings, load_settings
from producer.database import connect, fetch_pending_events, mark_published, record_publish_failure
from producer.kafka_publisher import KafkaPublisher

logger = logging.getLogger("producer.outbox_relay")


def build_event(row: dict[str, Any]) -> dict[str, Any]:
    payload = row["payload"]
    if not isinstance(payload, dict):
        raise ValueError(f"Outbox event {row['id']} has no valid order payload")

    if isinstance(payload.get("order"), dict):
        order = payload["order"]
    elif "order_id" in payload or "order_number" in payload:
        order = payload
    else:
        raise ValueError(f"Outbox event {row['id']} has no valid order payload")

    payment_trigger = order.get("payment_trigger")
    order_data = {key: value for key, value in order.items() if key != "payment_trigger"}
    data: dict[str, Any] = {"order": order_data}
    if payment_trigger is not None:
        data["payment_trigger"] = payment_trigger

    occurred_at = payload.get("timestamp", row.get("created_at"))
    if isinstance(occurred_at, datetime):
        occurred_at = occurred_at.isoformat()

    return {
        "event_id": str(row["id"]),
        "event_type": row["event_type"],
        "event_version": 1,
        "occurred_at": occurred_at,
        "aggregate_type": row["aggregate_type"],
        "aggregate_id": str(row["aggregate_id"]),
        "source": "order-service",
        "data": data,
    }


def process_pending_batch(
    settings: Settings,
    conn: Any,
    publisher: KafkaPublisher,
) -> tuple[int, int]:
    rows = fetch_pending_events(conn, settings.outbox_batch_size)
    published = 0
    failed = 0

    for row in rows:
        try:
            event = build_event(row)
            publisher.publish(event, key=str(row["aggregate_id"]))
        except Exception as exc:
            retries = record_publish_failure(conn, row["id"], exc)
            logger.error(
                "Could not publish event_id=%s; leaving it PENDING (retry_count=%s): %s",
                row["id"],
                retries,
                exc,
            )
            failed += 1
            continue

        mark_published(conn, row["id"])
        logger.info("Published event_id=%s to topic=%s", row["id"], settings.kafka_orders_topic)
        published += 1

    return published, failed


def run(settings: Settings, once: bool = False) -> int:
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda _signum, _frame: stop_event.set())
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stop_event.set())

    conn = connect(settings)
    publisher = KafkaPublisher(settings)
    try:
        while not stop_event.is_set():
            published, failed = process_pending_batch(settings, conn, publisher)
            if published or failed:
                logger.info("Batch complete: published=%s failed=%s", published, failed)
            if once:
                return 1 if failed else 0
            stop_event.wait(settings.outbox_poll_interval_seconds)
    finally:
        try:
            publisher.close()
        finally:
            conn.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish PostgreSQL outbox events to Kafka.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process one batch and exit instead of polling continuously",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    raise SystemExit(run(load_settings(), once=args.once))


if __name__ == "__main__":
    main()
