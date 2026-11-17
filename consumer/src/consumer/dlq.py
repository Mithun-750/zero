"""Publish poison events to the dead-letter topic (payments.dlq).

The DLQ message is self-contained: original bytes, source coordinates,
failure class/reason, attempt count. Kafka headers mirror the key
correlations for tooling.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer

from consumer.classify import DlqPublishError
from consumer.config import Settings
from consumer.db import CONSUMER_NAME

logger = logging.getLogger("consumer.dlq")

RAW_PAYLOAD_MAX = 10_000
REASON_MAX = 2_000


def build_dlq_envelope(
    *,
    raw_payload: bytes,
    event: dict[str, Any] | None,
    failure_class: str,
    failure_reason: str,
    attempt_count: int,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    source_timestamp: int | None,
) -> dict[str, Any]:
    # The DLQ message must be self-contained: an operator (or a replayer)
    # should be able to act on it without any other system. Raw bytes are
    # preserved verbatim (truncated) so unparseable records stay debuggable;
    # JSON consumers can still use the structured fields.
    event = event if isinstance(event, dict) else {}
    raw_text = raw_payload.decode("utf-8", errors="replace")[:RAW_PAYLOAD_MAX]
    return {
        "event_id": event.get("event_id"),
        "event_type": event.get("event_type"),
        "aggregate_type": event.get("aggregate_type"),
        "aggregate_id": event.get("aggregate_id"),
        "occurred_at": event.get("occurred_at"),
        "source": {
            "topic": source_topic,
            "partition": source_partition,
            "offset": source_offset,
            "timestamp": source_timestamp,
        },
        "failure_class": failure_class,
        "failure_reason": failure_reason[:REASON_MAX],
        "attempt_count": attempt_count,
        "raw_payload": raw_text,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "consumer_name": CONSUMER_NAME,
    }


def publish_to_dlq(
    settings: Settings,
    *,
    raw_payload: bytes,
    event: dict[str, Any] | None,
    failure_class: str,
    failure_reason: str,
    attempt_count: int,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    source_timestamp: int | None,
) -> dict[str, Any]:
    """Publish to the DLQ and return the broker coordinates.

    Raises DlqPublishError if the broker does not acknowledge — callers
    MUST NOT commit the source offset in that case (see pipeline._fail_event).
    A one-off producer is created per DLQ write: DLQ is a rare, emergency
    path, so we trade throughput for never wedging on a shared producer.
    """
    envelope = build_dlq_envelope(
        raw_payload=raw_payload,
        event=event,
        failure_class=failure_class,
        failure_reason=failure_reason,
        attempt_count=attempt_count,
        source_topic=source_topic,
        source_partition=source_partition,
        source_offset=source_offset,
        source_timestamp=source_timestamp,
    )
    # Keyed by aggregate_id so a burst of poison events for one aggregate
    # stays ordered on one DLQ partition; headers mirror the fields used by
    # tooling to grep/route without decoding the body.
    key = envelope.get("aggregate_id") or "unknown-aggregate"

    headers = [
        ("failure_class", failure_class.encode("utf-8")),
        ("consumer_name", CONSUMER_NAME.encode("utf-8")),
    ]
    if envelope.get("event_id"):
        headers.append(("event_id", str(envelope["event_id"]).encode("utf-8")))
    if envelope.get("aggregate_id"):
        headers.append(("aggregate_id", str(envelope["aggregate_id"]).encode("utf-8")))

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id=f"{settings.kafka_client_id}-dlq",
        acks="all",
        enable_idempotence=True,
    )
    try:
        value = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        meta = producer.send(
            settings.kafka_dlq_topic,
            key=str(key).encode("utf-8"),
            value=value,
            headers=headers,
        ).get(timeout=30)
    except Exception as exc:
        raise DlqPublishError(f"could not publish to DLQ {settings.kafka_dlq_topic}: {exc}") from exc
    finally:
        try:
            producer.flush(timeout=5)
            producer.close(timeout=5)
        except Exception:
            logger.warning("DLQ producer close failed", exc_info=True)

    logger.error(
        "Event routed to DLQ topic=%s partition=%s offset=%s class=%s reason=%s",
        settings.kafka_dlq_topic, meta.partition, meta.offset, failure_class, failure_reason,
    )
    return {"dlq_topic": settings.kafka_dlq_topic, "dlq_partition": meta.partition, "dlq_offset": meta.offset}
