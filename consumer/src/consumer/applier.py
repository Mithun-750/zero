"""Shared apply path — used by BOTH the Kafka pipeline and the DLQ resolver.

One code path, one set of semantics. Deliberately a single transaction:
  1. validate (permanent errors raise before any write)
  2. dedupe via processed_events
  3. insert a payments row (INITIATED, idempotency_key = event_id)
  4. insert the processed_events ledger row

Steps 3 and 4 must commit or abort together; if the process dies between
them the idempotency key and the ledger PK make redelivery safe in both
directions.
"""
from __future__ import annotations

from typing import Any

import psycopg

from consumer.classify import (
    TransientError,
    ValidationError,
    validate_event,
)
from consumer.db import CONSUMER_NAME


def already_processed(conn: psycopg.Connection[dict[str, Any]], event_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM processed_events WHERE event_id = %s", (event_id,))
        return cur.fetchone() is not None


def apply_event(conn: psycopg.Connection[dict[str, Any]], event: Any) -> str:
    """Apply one event. Returns 'applied' | 'duplicate'.

    Raises ValidationError / BusinessRuleError (permanent → DLQ) or
    TransientError (retryable).
    """
    norm = validate_event(event)  # validates + normalizes for both call sites
    event_id = norm["event_id"]

    try:
        with conn.transaction():
            if already_processed(conn, event_id):
                return "duplicate"
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO payments
                        (order_id, idempotency_key, amount, currency, status)
                    VALUES (%s, %s, %s, %s, 'INITIATED')
                    """,
                    (norm["order_id"], event_id, norm["amount"], norm["currency"]),
                )
                cur.execute(
                    """
                    INSERT INTO processed_events
                        (event_id, consumer_name, aggregate_id)
                    VALUES (%s, %s, %s)
                    """,
                    (event_id, CONSUMER_NAME, norm["aggregate_id"]),
                )
    except psycopg.IntegrityError as exc:
        # FK violation (order_id not in orders) or an idempotency collision.
        # Neither is fixed by retrying, and the payment is never silently
        # half-applied (the whole transaction rolls back).
        reason = (exc.diag.message_primary or str(exc)).strip()[:500]
        raise ValidationError(f"integrity error applying event: {reason}") from exc
    except psycopg.OperationalError as exc:
        # Connection dropped / server closed — transient, retry-worthy.
        raise TransientError(str(exc)[:500]) from exc
    return "applied"
