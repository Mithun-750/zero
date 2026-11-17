"""Database plumbing for the consumer (connections + shared constants).

All SQL lives in state.py (blocks/parked/failures) and applier.py
(payment apply path).

Connections run autocommit=True; explicit multi-statement atomicity is done
with `with conn.transaction():` blocks (psycopg3 gives us one real
transaction wrapping the nested statements even under autocommit).
"""
from __future__ import annotations

from typing import Any, cast

import psycopg
from psycopg.rows import dict_row

from consumer.config import Settings

CONSUMER_NAME = "payment-consumer"


def connect(settings: Settings) -> psycopg.Connection[dict[str, Any]]:
    # row_factory=dict_row means every row is a dict; psycopg can't infer
    # the generic from the kwarg, so it's annotated here once.
    return cast(
        psycopg.Connection[dict[str, Any]],
        psycopg.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
            row_factory=cast(Any, dict_row),
            autocommit=True,
        ),
    )
