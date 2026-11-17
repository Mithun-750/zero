"""Resolution orchestration tests — mocked state/applier, no DB.

Verifies the invariants that make resolution safe:
  - the fixed payload is validated BEFORE any write happens
  - one transaction wraps failure fetch, fix, follower drain, unblock
  - draining follows kafka_offset order
  - discard skips the bad event, with followers replayed or discarded
    per the operator's choice
  - already-resolved failures are rejected
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock, call, patch

from consumer import resolve
from consumer.classify import ValidationError
from consumer.resolve import ResolutionError, resolve_failure

VALID_EVENT = {
    "event_id": "11111111-1111-1111-1111-111111111111",
    "event_type": "OrderCreated",
    "event_version": 1,
    "aggregate_type": "ORDER",
    "aggregate_id": "22222222-2222-2222-2222-222222222222",
    "data": {
        "order": {"order_id": "22222222-2222-2222-2222-222222222222"},
        "payment_trigger": {
            "action": "INITIATE_PAYMENT",
            "amount": 50,
            "currency": "INR",
        },
    },
}
AGG = VALID_EVENT["aggregate_id"]
FAILURE_ID = "f1f1f1f1-f1f1-f1f1-f1f1-f1f1f1f1f1f1"


def _failure(status="OPEN", aggregate_id=AGG):
    return {
        "id": FAILURE_ID,
        "status": status,
        "aggregate_id": aggregate_id,
        "event_id": "33333333-3333-3333-3333-333333333333",
    }


def _parked(n=2):
    return [
        {
            "id": f"p{i}",
            "event": {
                **VALID_EVENT,
                "event_id": f"44444444-4444-4444-4444-44444444444{i}",
            },
        }
        for i in range(n)
    ]


class ResolveFailureTests(unittest.TestCase):
    def _conn(self):
        conn = MagicMock()
        conn.transaction.return_value = MagicMock()  # usable as ctx manager
        return conn

    def test_fixed_payload_validated_before_transaction(self) -> None:
        conn = self._conn()
        with self.assertRaises(ValidationError):
            resolve_failure(conn, FAILURE_ID, fixed_event={"event_id": "not-a-uuid"})
        conn.transaction.assert_not_called()

    def test_resolve_applies_fix_then_followers_in_order_and_unblocks(self) -> None:
        conn = self._conn()
        with patch.object(resolve.state, "get_failure", return_value=_failure()), \
             patch.object(resolve.state, "list_parked", return_value=_parked(2)), \
             patch.object(resolve.state, "mark_parked") as mark, \
             patch.object(resolve.state, "unblock_aggregate") as unblock, \
             patch.object(resolve.state, "update_failure") as update, \
             patch.object(resolve.applier, "apply_event", return_value="applied") as apply_mock:
            result = resolve_failure(
                conn, FAILURE_ID, fixed_event=VALID_EVENT,
                resolved_by="operator", note="fixed amount",
            )

        # fix first, then followers in offset order
        self.assertEqual(apply_mock.call_count, 3)
        apply_mock.assert_any_call(conn, VALID_EVENT)
        self.assertEqual(result["parked_replayed"], 2)
        self.assertEqual(result["parked_discarded"], 0)
        self.assertEqual(result["resolution"], "RESOLVED")
        self.assertTrue(result["unblocked"])
        self.assertEqual(mark.call_args_list, [call(conn, "p0", "REPLAYED"), call(conn, "p1", "REPLAYED")])
        unblock.assert_called_once_with(conn, "ORDER", AGG, "REPLAYED", "operator", "fixed amount")
        update.assert_called_once()

    def test_resolve_aborts_if_failure_already_resolved(self) -> None:
        conn = self._conn()
        with patch.object(resolve.state, "get_failure", return_value=_failure(status="RESOLVED")), \
             patch.object(resolve.applier, "apply_event") as apply_mock:
            with self.assertRaises(ResolutionError):
                resolve_failure(conn, FAILURE_ID, fixed_event=VALID_EVENT)
        apply_mock.assert_not_called()

    def test_discard_with_followers_discards_them_too(self) -> None:
        conn = self._conn()
        with patch.object(resolve.state, "get_failure", return_value=_failure()), \
             patch.object(resolve.state, "list_parked", return_value=_parked(2)), \
             patch.object(resolve.state, "mark_parked") as mark, \
             patch.object(resolve.state, "unblock_aggregate") as unblock, \
             patch.object(resolve.state, "update_failure") as update, \
             patch.object(resolve.applier, "apply_event") as apply_mock:
            result = resolve_failure(conn, FAILURE_ID, discard=True, include_followers=True)

        apply_mock.assert_not_called()  # discard applies nothing
        self.assertEqual(result["parked_discarded"], 2)
        self.assertEqual(result["parked_replayed"], 0)
        self.assertEqual(mark.call_args_list, [call(conn, "p0", "DISCARDED"), call(conn, "p1", "DISCARDED")])
        unblock.assert_called_once_with(conn, "ORDER", AGG, "DISCARDED", "operator", None)
        self.assertEqual(update.call_args.kwargs["status"], "DISCARDED")

    def test_discard_without_followers_replays_them(self) -> None:
        conn = self._conn()
        with patch.object(resolve.state, "get_failure", return_value=_failure()), \
             patch.object(resolve.state, "list_parked", return_value=_parked(2)), \
             patch.object(resolve.state, "mark_parked") as mark, \
             patch.object(resolve.state, "unblock_aggregate") as unblock, \
             patch.object(resolve.state, "update_failure"), \
             patch.object(resolve.applier, "apply_event", return_value="applied") as apply_mock:
            result = resolve_failure(conn, FAILURE_ID, discard=True, include_followers=False)

        self.assertEqual(apply_mock.call_count, 2)  # only followers, not the bad event
        self.assertEqual(result["parked_replayed"], 2)
        self.assertEqual(result["parked_discarded"], 0)
        self.assertEqual(mark.call_args_list, [call(conn, "p0", "REPLAYED"), call(conn, "p1", "REPLAYED")])
        unblock.assert_called_once_with(conn, "ORDER", AGG, "DISCARDED", "operator", None)


if __name__ == "__main__":
    unittest.main()
