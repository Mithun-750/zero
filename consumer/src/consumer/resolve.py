"""Resolve DLQ'd failures and unblock aggregates.

Two resolutions:

  Resolve (fix & replay):
    1. validate the operator-fixed event (before any writes)
    2. apply it through the SAME applier used by the pipeline
    3. drain parked followers for that aggregate in kafka_offset order
    4. mark failure RESOLVED + unblock the aggregate (REPLAYED)

  Discard:
    - optionally also discard the parked followers, or replay them
      (ordering preserved either way — only the bad event is skipped)
    - mark failure DISCARDED + unblock the aggregate (DISCARDED)

Steps run in ONE transaction: a failure anywhere leaves the aggregate
blocked and the failure OPEN.
"""
from __future__ import annotations

import logging
from typing import Any

import psycopg

from consumer import applier, state
from consumer.classify import validate_event
from consumer.db import CONSUMER_NAME  # noqa: F401  (re-exported for callers)

logger = logging.getLogger("consumer.resolve")

DEFAULT_AGGREGATE_TYPE = "ORDER"


class ResolutionError(Exception):
    """The failure cannot be resolved (unknown id, already resolved, …)."""


def resolve_failure(
    conn: psycopg.Connection[dict[str, Any]],
    failure_id: str,
    *,
    fixed_event: dict[str, Any] | None = None,
    discard: bool = False,
    include_followers: bool = False,
    resolved_by: str = "operator",
    note: str | None = None,
) -> dict[str, Any]:
    # Fail fast: validate the operator-supplied payload BEFORE opening the
    # transaction — a 400 here changes nothing.
    if not discard:
        if not isinstance(fixed_event, dict):
            raise ResolutionError("a fixed event payload is required for resolve")
        validate_event(fixed_event)  # raises ValidationError → caller 400s

    with conn.transaction():
        # FOR UPDATE: two dashboards resolving the same failure at once must
        # serialize; the second sees status != OPEN and gets a clean 400
        # instead of double-applying the fix.
        failure = state.get_failure(conn, failure_id, for_update=True)
        if failure is None:
            raise ResolutionError(f"failure {failure_id} not found")
        if failure["status"] != "OPEN":
            raise ResolutionError(f"failure {failure_id} is already {failure['status']}")

        aggregate_id = str(failure["aggregate_id"]) if failure["aggregate_id"] else None
        aggregate_type = DEFAULT_AGGREGATE_TYPE

        result = {
            "failure_id": failure_id,
            "resolution": "DISCARDED" if discard else "RESOLVED",
            "applied_fix": False,
            "parked_replayed": 0,
            "parked_discarded": 0,
            "unblocked": False,
        }

        if aggregate_id:
            if discard:
                # Discard = apply NOTHING (the bad event is unrecoverable).
                # Followers still get replayed unless the operator explicitly
                # discards them — replaying in offset order preserves the
                # order they arrived in, skipping only the poison record.
                logger.info("Discarding event %s for aggregate=%s", failure["event_id"], aggregate_id)
            else:
                outcome = applier.apply_event(conn, fixed_event)
                result["applied_fix"] = outcome == "applied"
                logger.info(
                    "Fixed event applied (outcome=%s) for aggregate=%s", outcome, aggregate_id
                )

            # Drain followers in the ORIGINAL arrival order (kafka_offset).
            # All-or-nothing by design: if a follower fails permanently the
            # whole transaction rolls back — nothing is applied, the failure
            # stays OPEN, the aggregate stays blocked. Silently skipping a
            # poison follower would hide exactly the class of bug we're
            # built to surface. The operator fixes (or discards) it too.
            for parked in state.list_parked(conn, aggregate_type, aggregate_id):
                if discard and include_followers:
                    state.mark_parked(conn, parked["id"], "DISCARDED")
                    result["parked_discarded"] += 1
                else:
                    # Permanent errors here abort the whole transaction → nothing unblocks.
                    applier.apply_event(conn, parked["event"])
                    state.mark_parked(conn, parked["id"], "REPLAYED")
                    result["parked_replayed"] += 1

            state.unblock_aggregate(
                conn,
                aggregate_type,
                aggregate_id,
                "DISCARDED" if discard else "REPLAYED",
                resolved_by,
                note,
            )
            result["unblocked"] = True
        else:
            logger.info("Resolving unidentifiable poison event %s (no aggregate to unblock)", failure_id)

        state.update_failure(
            conn,
            failure_id,
            status="DISCARDED" if discard else "RESOLVED",
            resolved_by=resolved_by,
            note=note or ("discarded" if discard else "replayed fixed event + followers"),
        )

    return result
