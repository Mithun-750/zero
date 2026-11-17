"""In-memory counters + live DB gauges for the dashboard.

Counters are PROCESS-LOCAL: they reset on restart, which is fine for a
"what is my consumer doing right now" view. Anything about durable state
(blocked aggregates, parked events, outbox backlog, payment totals) is read
live from PostgreSQL on every snapshot instead.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any

import psycopg

from consumer import state


class Metrics:
    def __init__(self) -> None:
        self.started_at = time.time()
        self._lock = threading.Lock()
        self.counters = {
            "consumed": 0,
            "applied": 0,
            "duplicate": 0,
            "parked": 0,
            "failed": 0,
        }
        self.failures_by_class: dict[str, int] = {}

    def record_outcome(self, outcome: str) -> None:
        with self._lock:
            self.counters["consumed"] += 1
            self.counters[outcome] = self.counters.get(outcome, 0) + 1

    def record_failure_class(self, failure_class: str) -> None:
        with self._lock:
            self.failures_by_class[failure_class] = (
                self.failures_by_class.get(failure_class, 0) + 1
            )

    def snapshot(self, conn: psycopg.Connection[dict[str, Any]]) -> dict[str, Any]:
        with self._lock:
            counters = dict(self.counters)
            failures_by_class = dict(self.failures_by_class)

        applied = counters.get("applied", 0)
        consumed = counters.get("consumed", 0)
        success_rate = (applied / consumed) if consumed else 1.0

        return {
            "started_at": datetime.fromtimestamp(self.started_at, tz=timezone.utc).isoformat(),
            "uptime_seconds": int(time.time() - self.started_at),
            "counters": counters,
            "events_consumed_total": consumed,
            "failures_by_class": failures_by_class,
            "success_rate": round(success_rate, 4),
            "blocked_aggregates": state.blocked_count(conn),
            "parked_events": state.parked_count(conn),
            "open_failures": state.open_failure_count(conn),
            "outbox_by_status": state.outbox_status_counts(conn),
            "payments_by_status": state.payment_status_counts(conn),
        }
