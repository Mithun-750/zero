"""Lightweight Kafka consumer with atomic PostgreSQL idempotency and manual offset management."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import threading
import uuid
from typing import Any, Callable

from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

from producer.config import Settings, load_settings
from producer.database import connect, process_event_idempotently

logger = logging.getLogger("producer.consumer")


def default_business_action(cur: Any, aggregate_id: Any) -> None:
    """Minimal test business action representing downstream order verification.

    Executes within the same transaction as processed_events insertion.
    Does not modify real payment or order statuses.
    """
    cur.execute(
        "SELECT id, total_amount FROM orders WHERE id = %s",
        (aggregate_id,),
    )


class OrderConsumer:
    def __init__(
        self,
        settings: Settings,
        business_action_fn: Callable[[Any, Any], None] | None = None,
        hook_before_offset_commit: Callable[[Any], None] | None = None,
        group_id: str | None = None,
    ) -> None:
        self.settings = settings
        self.business_action_fn = business_action_fn or default_business_action
        self.hook_before_offset_commit = hook_before_offset_commit
        self.stop_event = threading.Event()
        self.group_id = group_id or settings.kafka_consumer_group

        # Split comma-separated bootstrap servers if provided as a single string
        bootstrap_servers = (
            settings.kafka_bootstrap_servers.split(",")
            if isinstance(settings.kafka_bootstrap_servers, str)
            else settings.kafka_bootstrap_servers
        )

        logger.info(
            "Initializing OrderConsumer: group=%s bootstrap=%s topic=%s",
            self.group_id,
            bootstrap_servers,
            settings.kafka_orders_topic,
        )

        self.consumer = KafkaConsumer(
            settings.kafka_orders_topic,
            bootstrap_servers=bootstrap_servers,
            group_id=self.group_id,
            client_id=f"{settings.kafka_consumer_name}-client",
            enable_auto_commit=False,
            auto_offset_reset=settings.kafka_consumer_auto_offset_reset,
            value_deserializer=None,  # Manual byte decoding for fail-stop error handling
            consumer_timeout_ms=1000,  # Return from poll to check stop_event
        )
        self.conn = connect(settings)

    def process_record(self, record: Any) -> tuple[bool, str]:
        """Process a single Kafka record sequentially with manual offset commit."""
        tp = TopicPartition(record.topic, record.partition)

        # 1. Decode bytes to UTF-8
        try:
            raw_payload = record.value.decode("utf-8")
        except Exception as exc:
            logger.error(
                "Invalid UTF-8 payload at topic=%s partition=%s offset=%s: %s",
                record.topic,
                record.partition,
                record.offset,
                exc,
            )
            raise ValueError(f"Malformed UTF-8 payload at offset {record.offset}") from exc

        # 2. Parse JSON
        try:
            event = json.loads(raw_payload)
        except Exception as exc:
            logger.error(
                "Invalid JSON at topic=%s partition=%s offset=%s: %s",
                record.topic,
                record.partition,
                record.offset,
                exc,
            )
            raise ValueError(f"Malformed JSON at offset {record.offset}") from exc

        if not isinstance(event, dict):
            logger.error(
                "Event payload is not a JSON object at topic=%s partition=%s offset=%s",
                record.topic,
                record.partition,
                record.offset,
            )
            raise ValueError(f"Event payload must be a dict at offset {record.offset}")

        # 3. Validate required fields
        if "event_id" not in event or not event["event_id"]:
            logger.error(
                "Missing event_id at topic=%s partition=%s offset=%s",
                record.topic,
                record.partition,
                record.offset,
            )
            raise ValueError(f"Missing event_id at offset {record.offset}")

        if "aggregate_id" not in event or not event["aggregate_id"]:
            logger.error(
                "Missing aggregate_id at topic=%s partition=%s offset=%s",
                record.topic,
                record.partition,
                record.offset,
            )
            raise ValueError(f"Missing aggregate_id at offset {record.offset}")

        # 4. Parse UUIDs
        try:
            event_id_obj = uuid.UUID(str(event["event_id"]))
            aggregate_id_obj = uuid.UUID(str(event["aggregate_id"]))
        except ValueError as exc:
            logger.error(
                "Invalid UUID format (event_id=%s, aggregate_id=%s) at offset=%s: %s",
                event.get("event_id"),
                event.get("aggregate_id"),
                record.offset,
                exc,
            )
            raise ValueError(f"Invalid UUID at offset {record.offset}") from exc

        logger.info(
            "Received event_id=%s aggregate_id=%s topic=%s partition=%s offset=%s",
            event_id_obj,
            aggregate_id_obj,
            record.topic,
            record.partition,
            record.offset,
        )

        # 5. Atomic PostgreSQL Idempotency & Business Execution
        try:
            is_new, status = process_event_idempotently(
                conn=self.conn,
                event_id=event_id_obj,
                consumer_name=self.settings.kafka_consumer_name,
                aggregate_id=aggregate_id_obj,
                business_action_fn=self.business_action_fn,
            )
        except Exception as exc:
            logger.error(
                "Database transaction failed for event_id=%s (offset=%s not committed): %s",
                event_id_obj,
                record.offset,
                exc,
            )
            raise

        if is_new:
            logger.info(
                "Event successfully processed event_id=%s: status=%s",
                event_id_obj,
                status,
            )
        else:
            logger.info(
                "Duplicate event detected event_id=%s: business operation skipped (%s)",
                event_id_obj,
                status,
            )

        # 6. Optional fault injection hook (simulates crash between DB commit and Kafka offset commit)
        if self.hook_before_offset_commit:
            self.hook_before_offset_commit(event_id_obj)

        # 7. Commit Kafka offset ONLY after DB commit
        next_offset = OffsetAndMetadata(record.offset + 1, "")
        try:
            self.consumer.commit(offsets={tp: next_offset})
            logger.info(
                "Kafka offset committed: topic=%s partition=%s committed_offset=%s",
                record.topic,
                record.partition,
                record.offset + 1,
            )
        except Exception as exc:
            logger.error(
                "Failed to commit Kafka offset=%s for event_id=%s: %s",
                record.offset + 1,
                event_id_obj,
                exc,
            )
            raise

        return is_new, status

    def run(self, max_messages: int | None = None, once: bool = False) -> int:
        """Run consumer loop until stopped, once drained, or max_messages reached."""
        processed_count = 0
        logger.info("OrderConsumer started listening on %s...", self.settings.kafka_orders_topic)

        try:
            while not self.stop_event.is_set():
                poll_result = self.consumer.poll(timeout_ms=1000)
                if not poll_result:
                    if once:
                        logger.info("No further messages in current poll; exiting due to --once flag.")
                        break
                    continue

                for tp, records in poll_result.items():
                    for record in records:
                        if self.stop_event.is_set():
                            break

                        self.process_record(record)
                        processed_count += 1

                        if max_messages and processed_count >= max_messages:
                            logger.info("Reached max_messages limit (%s); stopping.", max_messages)
                            return processed_count

                if once:
                    logger.info("Batch processed under --once flag; exiting.")
                    break
        finally:
            self.close()

        logger.info("OrderConsumer stopped cleanly. Total messages processed: %s", processed_count)
        return processed_count

    def close(self) -> None:
        """Safely close Kafka consumer and PostgreSQL connection."""
        try:
            self.consumer.close()
        except Exception as exc:
            logger.warning("Error closing KafkaConsumer: %s", exc)

        try:
            self.conn.close()
        except Exception as exc:
            logger.warning("Error closing PostgreSQL connection: %s", exc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume order events with PostgreSQL idempotency.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process current available messages and exit",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Maximum number of messages to process before exiting",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    settings = load_settings()
    consumer = OrderConsumer(settings)

    def handle_signal(_signum: int, _frame: Any) -> None:
        logger.info("Received termination signal; shutting down consumer...")
        consumer.stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        consumer.run(max_messages=args.max_messages, once=args.once)
    except Exception as exc:
        logger.error("Consumer terminated with error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()

