"""Kafka OrderCreated consumer: apply payments, route poison events to DLQ,
block affected aggregates, park their followers. Serves the dashboard."""
from __future__ import annotations

import argparse
import logging
import signal
import threading

from kafka import KafkaConsumer

from consumer import db, pipeline
from consumer.config import Settings, load_settings
from consumer.dashboard import create_app
from consumer.metrics import Metrics

logger = logging.getLogger("consumer.main")


def run(settings: Settings, once: bool = False) -> int:
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda _s, _f: stop_event.set())
    signal.signal(signal.SIGTERM, lambda _s, _f: stop_event.set())

    conn = db.connect(settings)
    metrics = Metrics()

    consumer = KafkaConsumer(
        settings.kafka_orders_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        client_id=settings.kafka_client_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: b,  # raw bytes; JSON errors are handled downstream
    )

    if settings.dashboard_port:
        import uvicorn

        app = create_app(settings, metrics, consumer=consumer)
        server = uvicorn.Server(
            uvicorn.Config(app, host=settings.dashboard_host, port=settings.dashboard_port,
                           log_level="warning")
        )
        threading.Thread(target=server.run, name="dashboard", daemon=True).start()
        logger.info("Dashboard listening on http://%s:%d", settings.dashboard_host, settings.dashboard_port)

    processed = 0
    try:
        while not stop_event.is_set():
            records = consumer.poll(timeout_ms=500)
            for _tp, batch in records.items():
                for record in batch:
                    try:
                        pipeline.process_record(conn, record, settings, metrics)
                    except pipeline.OffsetRejectError:
                        # Outcome NOT durably persisted — do NOT commit. The
                        # record will be redelivered (at-least-once delivery,
                        # idempotent apply via processed_events).
                        logger.critical(
                            "Offset NOT committed for partition=%d offset=%s — will be redelivered",
                            record.partition, record.offset,
                        )
                        continue
                    except Exception:
                        # Unexpected failure is deterministic for poison
                        # records: on redelivery apply_event classifies it
                        # (permanent → DLQ), so not committing is safe.
                        logger.exception(
                            "Unexpected error at partition=%d offset=%s; not committing",
                            record.partition, record.offset,
                        )
                        continue
                    # Commit only after a durable outcome (applied,
                    # duplicate, parked, or DLQ'd).
                    consumer.commit()
                    processed += 1
            if once:
                break
    finally:
        consumer.close()
        conn.close()
    logger.info("Stopped after %d record(s)", processed)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume OrderCreated events")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Poll once and exit (dev/verification)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    raise SystemExit(run(load_settings(), once=args.once))


if __name__ == "__main__":
    main()
