"""Database operations for reading and updating outbox rows."""

from __future__ import annotations

from typing import Any, Callable

import psycopg
from psycopg.rows import dict_row

from producer.config import Settings


def connect(settings: Settings) -> psycopg.Connection[dict[str, Any]]:
    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        row_factory=dict_row,
        autocommit=True,
    )


def fetch_pending_events(
    conn: psycopg.Connection[dict[str, Any]], batch_size: int
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at
            FROM outbox
            WHERE status = 'PENDING'
            ORDER BY created_at, id
            LIMIT %s
            """,
            (batch_size,),
        )
        return cur.fetchall()


def mark_published(conn: psycopg.Connection[dict[str, Any]], event_id: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE outbox
            SET status = 'PUBLISHED',
                published_at = CURRENT_TIMESTAMP,
                error_message = NULL
            WHERE id = %s AND status = 'PENDING'
            """,
            (event_id,),
        )
        if cur.rowcount != 1:
            raise RuntimeError(f"Outbox event {event_id} was not marked PUBLISHED")


def record_publish_failure(
    conn: psycopg.Connection[dict[str, Any]], event_id: Any, error: Exception
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE outbox
            SET retry_count = retry_count + 1,
                error_message = %s
            WHERE id = %s AND status = 'PENDING'
            RETURNING retry_count
            """,
            (str(error)[:2000], event_id),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"Could not record publish failure for outbox event {event_id}")
        return row["retry_count"]


def process_event_idempotently(
    conn: psycopg.Connection[dict[str, Any]],
    event_id: Any,
    consumer_name: str,
    aggregate_id: Any,
    business_action_fn: Callable[[psycopg.Cursor[dict[str, Any]], Any], None] | None = None,
) -> tuple[bool, str]:
    """Atomically record event processing and execute business logic within a single transaction.

    Returns:
        (True, "PROCESSED") if the event was processed for the first time.
        (False, "DUPLICATE_SKIPPED") if the event was already registered in processed_events.
    """
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO processed_events (
                    event_id,
                    consumer_name,
                    aggregate_id
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id;
                """,
                (event_id, consumer_name, aggregate_id),
            )
            row = cur.fetchone()
            if row is not None:
                if business_action_fn is not None:
                    business_action_fn(cur, aggregate_id)
                return True, "PROCESSED"
            return False, "DUPLICATE_SKIPPED"

