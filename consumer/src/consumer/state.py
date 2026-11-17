"""PostgreSQL state: aggregate blocks, parked events, failure ledger.

This module is the only place that touches aggregate_processing_state,
parked_events and event_failures.
"""
from __future__ import annotations

import json
from typing import Any

import psycopg
from psycopg.rows import dict_row

from consumer.db import CONSUMER_NAME

# ---------------- aggregate blocks ----------------
# aggregate_processing_state is the single source of truth for "is this
# aggregate paused?" — checked on EVERY record before apply (cheap PK
# lookup, no caching, so unblocking is visible immediately).

def is_blocked(conn: psycopg.Connection[dict[str, Any]], aggregate_type: str, aggregate_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT is_blocked FROM aggregate_processing_state
            WHERE aggregate_type = %s AND aggregate_id = %s
            """,
            (aggregate_type, aggregate_id),
        )
        row = cur.fetchone()
        return bool(row and row["is_blocked"])


def block_aggregate(
    conn: psycopg.Connection[dict[str, Any]],
    aggregate_type: str,
    aggregate_id: str,
    event_id: str | None,
    reason: str,
) -> None:
    # UPSERT so a re-blocked aggregate keeps exactly one row but latest reason.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO aggregate_processing_state
                (aggregate_type, aggregate_id, is_blocked, blocked_event_id,
                 blocked_reason, blocked_at, blocked_by_consumer, updated_at)
            VALUES (%s, %s, TRUE, %s, %s, NOW(), %s, NOW())
            ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE SET
                is_blocked = TRUE,
                blocked_event_id = EXCLUDED.blocked_event_id,
                blocked_reason = EXCLUDED.blocked_reason,
                blocked_at = NOW(),
                blocked_by_consumer = EXCLUDED.blocked_by_consumer,
                updated_at = NOW()
            """,
            (aggregate_type, aggregate_id, event_id, reason[:2000], CONSUMER_NAME),
        )


def unblock_aggregate(
    conn: psycopg.Connection[dict[str, Any]],
    aggregate_type: str,
    aggregate_id: str,
    action: str,
    resolved_by: str,
    note: str | None,
) -> None:
    # is_blocked=FALSE but we keep blocked_event_id/reason and add resolution
    # metadata — the block history is preserved for audit/debugging.
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE aggregate_processing_state
            SET is_blocked = FALSE,
                resolved_at = NOW(),
                resolved_by = %s,
                resolution_note = %s,
                resolution_action = %s,
                updated_at = NOW()
            WHERE aggregate_type = %s AND aggregate_id = %s
            """,
            (resolved_by, (note or "")[:2000], action, aggregate_type, aggregate_id),
        )


def get_block(
    conn: psycopg.Connection[dict[str, Any]], aggregate_type: str, aggregate_id: str
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM aggregate_processing_state
            WHERE aggregate_type = %s AND aggregate_id = %s
            """,
            (aggregate_type, aggregate_id),
        )
        return cur.fetchone()


def list_blocked(conn: psycopg.Connection[dict[str, Any]]) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.*,
                   (SELECT count(*) FROM parked_events p
                     WHERE p.aggregate_type = s.aggregate_type
                       AND p.aggregate_id = s.aggregate_id
                       AND p.status = 'PARKED') AS parked_count
            FROM aggregate_processing_state s
            WHERE s.is_blocked = TRUE
            ORDER BY s.blocked_at DESC NULLS LAST
            """
        )
        return cur.fetchall()


def blocked_count(conn: psycopg.Connection[dict[str, Any]]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM aggregate_processing_state WHERE is_blocked")
        row = cur.fetchone()
        return int(row["n"] if row else 0)


# ---------------- parked events ----------------

def park_event(
    conn: psycopg.Connection[dict[str, Any]],
    *,
    aggregate_type: str,
    aggregate_id: str,
    event: dict[str, Any],
    source_topic: str,
    source_partition: int,
    kafka_offset: int,
) -> bool:
    """Park a follower event for a blocked aggregate. Returns True if parked,
    False if it was already parked (crash between commit and park insert).

    ON CONFLICT DO NOTHING against UNIQUE (consumer_name, event_id) makes
    re-parking on consumer redelivery idempotent — offset is only committed
    after this insert, so a redelivery must not double-park.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO parked_events
                (consumer_name, aggregate_type, aggregate_id, event_id,
                 event_type, event, source_topic, source_partition, kafka_offset)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
            ON CONFLICT (consumer_name, event_id) DO NOTHING
            RETURNING id
            """,
            (
                CONSUMER_NAME,
                aggregate_type,
                aggregate_id,
                str(event["event_id"]),
                str(event.get("event_type") or "Unknown"),
                json.dumps(event, separators=(",", ":"), ensure_ascii=False),
                source_topic,
                source_partition,
                kafka_offset,
            ),
        )
        return cur.fetchone() is not None


def list_parked(
    conn: psycopg.Connection[dict[str, Any]],
    aggregate_type: str,
    aggregate_id: str,
    status: str = "PARKED",
) -> list[dict[str, Any]]:
    # kafka_offset is the true per-partition order — replay MUST visit
    # followers in the order they arrived, not parked_at wall-clock order.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM parked_events
            WHERE aggregate_type = %s AND aggregate_id = %s AND status = %s
            ORDER BY kafka_offset, parked_at
            """,
            (aggregate_type, aggregate_id, status),
        )
        return cur.fetchall()


def list_parked_recent(conn: psycopg.Connection[dict[str, Any]], limit: int = 100) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM parked_events
            WHERE status = 'PARKED'
            ORDER BY parked_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def mark_parked(conn: psycopg.Connection[dict[str, Any]], parked_id: Any, status: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE parked_events SET status = %s WHERE id = %s",
            (status, parked_id),
        )


def parked_count(conn: psycopg.Connection[dict[str, Any]]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM parked_events WHERE status = 'PARKED'")
        row = cur.fetchone()
        return int(row["n"] if row else 0)


# ---------------- failure ledger ----------------
# event_failures is the dashboard's DLQ view (mirror of the payments.dlq
# topic): every poison event gets one row, keyed by source partition+offset.
# raw_payload is truncated to 10k chars to keep rows bounded.

def record_failure(
    conn: psycopg.Connection[dict[str, Any]],
    *,
    event_id: str | None,
    aggregate_id: str | None,
    event_type: str | None,
    failure_class: str,
    failure_reason: str,
    raw_payload: str,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    attempt_count: int,
    dlq_topic: str | None = None,
    dlq_partition: int | None = None,
    dlq_offset: int | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO event_failures
                (consumer_name, event_id, aggregate_id, event_type,
                 failure_class, failure_reason, raw_payload,
                 source_topic, source_partition, source_offset, attempt_count,
                 dlq_topic, dlq_partition, dlq_offset)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                CONSUMER_NAME,
                event_id,
                aggregate_id,
                event_type,
                failure_class,
                failure_reason[:2000],
                raw_payload[:10_000],
                source_topic,
                source_partition,
                source_offset,
                attempt_count,
                dlq_topic,
                dlq_partition,
                dlq_offset,
            ),
        )
        row = cur.fetchone()
        assert row is not None  # INSERT ... RETURNING always produces one row
        return row


def get_failure(
    conn: psycopg.Connection[dict[str, Any]], failure_id: str, for_update: bool = False
) -> dict[str, Any] | None:
    sql = "SELECT * FROM event_failures WHERE id = %s"
    if for_update:
        sql += " FOR UPDATE"
    with conn.cursor() as cur:
        cur.execute(sql, (failure_id,))
        return cur.fetchone()


def list_failures(
    conn: psycopg.Connection[dict[str, Any]], status: str | None = None, limit: int = 100
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM event_failures"
    params: list[Any] = []
    if status:
        sql += " WHERE status = %s"
        params.append(status)
    sql += " ORDER BY created_at DESC LIMIT %s"
    params.append(limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def update_failure(
    conn: psycopg.Connection[dict[str, Any]],
    failure_id: str,
    *,
    status: str,
    resolved_by: str,
    note: str | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE event_failures
            SET status = %s, resolved_at = NOW(), resolved_by = %s, resolution_note = %s
            WHERE id = %s
            """,
            (status, resolved_by, (note or "")[:2000], failure_id),
        )


def open_failure_count(conn: psycopg.Connection[dict[str, Any]]) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM event_failures WHERE status = 'OPEN'")
        row = cur.fetchone()
        return int(row["n"] if row else 0)


# ---------------- record views (dashboard) ----------------

def list_recent_payments(conn: psycopg.Connection[dict[str, Any]], limit: int = 20) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.id, p.order_id, o.order_number, o.customer_name, o.customer_email,
                   p.amount, p.currency, p.status, p.idempotency_key,
                   p.created_at AS applied_at
            FROM payments p
            JOIN orders o ON o.id = p.order_id
            ORDER BY p.created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def list_recent_processed(conn: psycopg.Connection[dict[str, Any]], limit: int = 20) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_id, aggregate_id, consumer_name, processed_at
            FROM processed_events
            ORDER BY processed_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


# ---------------- metrics gauges ----------------

def outbox_status_counts(conn: psycopg.Connection[dict[str, Any]]) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT status, count(*) AS n FROM outbox GROUP BY status")
        return {row["status"]: int(row["n"]) for row in cur.fetchall()}


def payment_status_counts(conn: psycopg.Connection[dict[str, Any]]) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT status, count(*) AS n FROM payments GROUP BY status")
        return {row["status"]: int(row["n"]) for row in cur.fetchall()}
