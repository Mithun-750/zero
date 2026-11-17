"""Per-record processing pipeline.

Outcome per record (all durable → offset committed by the caller):

  applied    — payment created (or idempotent no-op)
  duplicate  — event_id already in processed_events
  parked     — aggregate blocked; follower event parked in parked_events
  failed     — poison event routed to DLQ + aggregate blocked + ledger row

OffsetRejectError means "the outcome was NOT persisted" — callers must
abstain from committing the offset (redelivery is the correct behavior).
"""
from __future__ import annotations

import json
import logging
import random
import time
from typing import Any

from consumer import applier, dlq, state
from consumer.classify import (
    TRANSIENT_EXHAUSTED,
    PermanentError,
    TransientError,
)
from consumer.config import Settings
from consumer.metrics import Metrics

logger = logging.getLogger("consumer.pipeline")

DEFAULT_AGGREGATE_TYPE = "ORDER"


class OffsetRejectError(Exception):
    """The outcome was not persisted durably. The source offset MUST NOT be
    committed; the record will be redelivered (at-least-once)."""


def _raw_bytes(raw: Any) -> bytes:
    if raw is None:
        return b""
    if isinstance(raw, (bytes, bytearray)):
        return bytes(raw)
    return str(raw).encode("utf-8", errors="replace")


def _raw_text(raw: Any) -> str:
    return _raw_bytes(raw).decode("utf-8", errors="replace")


def _fail_event(
    conn,
    settings: Settings,
    *,
    raw: Any,
    event: dict[str, Any] | None,
    failure_class: str,
    failure_reason: str,
    attempt_count: int,
    record: Any,
    metrics: Metrics,
) -> str:
    """DLQ + ledger + block. Raises OffsetRejectError if the DLQ write fails.

    Order matters: DLQ publish FIRST (broker-ack needed before we consider
    the record 'handled'), then the ledger row + aggregate block atomically.
    If the DLQ publish fails we cannot claim the record is handled — the
    caller must not commit the offset.
    """
    try:
        dlq_meta = dlq.publish_to_dlq(
            settings,
            raw_payload=_raw_bytes(raw),
            event=event,
            failure_class=failure_class,
            failure_reason=failure_reason,
            attempt_count=attempt_count,
            source_topic=record.topic,
            source_partition=record.partition,
            source_offset=record.offset,
            source_timestamp=getattr(record, "timestamp", None),
        )
    except Exception as exc:  # DlqPublishError or anything unexpected
        logger.critical(
            "DLQ publish failed for offset=%s — refusing to commit: %s", record.offset, exc
        )
        raise OffsetRejectError(str(exc)) from exc

    event = event if isinstance(event, dict) else {}
    aggregate_id = event.get("aggregate_id")
    aggregate_type = event.get("aggregate_type") or DEFAULT_AGGREGATE_TYPE
    event_id = event.get("event_id")

    # Ledger row + aggregate block must land atomically: an OPEN failure row
    # without the block would let followers slip through next redelivery.
    with conn.transaction():
        state.record_failure(
            conn,
            event_id=str(event_id) if event_id else None,
            aggregate_id=str(aggregate_id) if aggregate_id else None,
            event_type=event.get("event_type"),
            failure_class=failure_class,
            failure_reason=failure_reason,
            raw_payload=_raw_text(raw),
            source_topic=record.topic,
            source_partition=record.partition,
            source_offset=record.offset,
            attempt_count=attempt_count,
            **dlq_meta,
        )

        # Only records carrying event_id + aggregate_id belong to an
        # aggregate; unidentifiable poison records go to the DLQ but block
        # nothing (we cannot know which stream to pause).
        if aggregate_id:
            state.block_aggregate(
                conn,
                aggregate_type,
                str(aggregate_id),
                str(event_id) if event_id else None,
                failure_reason,
            )
            logger.error(
                "Aggregate blocked type=%s id=%s reason=%s",
                aggregate_type, aggregate_id, failure_reason,
            )
        else:
            logger.error("Poison event has no identifiable aggregate — no block set")

    metrics.record_outcome("failed")
    metrics.record_failure_class(failure_class)
    return "failed"


def process_record(conn, record: Any, settings: Settings, metrics: Metrics) -> str:
    raw = record.value

    # --- 1. Parse. Unparseable records cannot be attributed to an aggregate
    #        (no block) — straight to DLQ. ---
    try:
        event = json.loads(raw)
    except Exception as exc:
        logger.error(
            "Unparseable record topic=%s partition=%s offset=%s: %s",
            record.topic, record.partition, record.offset, exc,
        )
        return _fail_event(
            conn, settings,
            raw=raw, event=None,
            failure_class="VALIDATION",
            failure_reason=f"JSON parse error: {exc}",
            attempt_count=1, record=record, metrics=metrics,
        )

    identifiable = (
        isinstance(event, dict)
        and bool(event.get("event_id"))
        and bool(event.get("aggregate_id"))
    )

    # --- 2. Block check BEFORE dedup and apply. Followers of a blocked
    #        aggregate are parked, never applied — ordering is preserved
    #        because they replay in kafka_offset order on resolution. ---
    if identifiable:
        aggregate_type = event.get("aggregate_type") or DEFAULT_AGGREGATE_TYPE
        aggregate_id = str(event["aggregate_id"])
        if state.is_blocked(conn, aggregate_type, aggregate_id):
            try:
                parked = state.park_event(
                    conn,
                    aggregate_type=aggregate_type,
                    aggregate_id=aggregate_id,
                    event=event,
                    source_topic=record.topic,
                    source_partition=record.partition,
                    kafka_offset=record.offset,
                )
            except Exception as exc:
                logger.exception("could not park event at offset=%s; not committing", record.offset)
                raise OffsetRejectError(str(exc)) from exc
            metrics.record_outcome("parked")
            if parked:
                logger.info("Parked follower event=%s aggregate=%s", event.get("event_id"), aggregate_id)
            return "parked"

    # --- 3. Apply. Transient failures get a bounded retry budget; permanent
    #        failures (validation / business rules / integrity) never retry. ---
    attempts = 0
    while True:
        try:
            outcome = applier.apply_event(conn, event)
            break
        except PermanentError as exc:
            logger.error("Permanent failure for offset=%s class=%s: %s",
                         record.offset, exc.failure_class, exc)
            return _fail_event(
                conn, settings,
                raw=raw, event=event if isinstance(event, dict) else None,
                failure_class=exc.failure_class,
                failure_reason=str(exc),
                attempt_count=attempts + 1, record=record, metrics=metrics,
            )
        except TransientError as exc:
            attempts += 1
            if attempts >= settings.consumer_max_retries:
                logger.error("Transient failure exhausted for offset=%s: %s", record.offset, exc)
                return _fail_event(
                    conn, settings,
                    raw=raw, event=event if isinstance(event, dict) else None,
                    failure_class=TRANSIENT_EXHAUSTED,
                    failure_reason=f"transient error after {attempts} attempts: {exc}",
                    attempt_count=attempts, record=record, metrics=metrics,
                )
            # backoff: 1x, 2x, 4x … of retry_backoff_seconds + jitter
            backoff = settings.consumer_retry_backoff_seconds * (2**attempts) + random.uniform(0, 0.2)
            logger.warning(
                "Transient error attempt=%d/%d offset=%s, retry in %.2fs: %s",
                attempts, settings.consumer_max_retries, record.offset, backoff, exc,
            )
            time.sleep(backoff)

    metrics.record_outcome(outcome)
    logger.info("Event %s outcome=%s offset=%s", event.get("event_id"), outcome, record.offset)
    return outcome
