from __future__ import annotations

import unittest
from datetime import datetime, timezone

from producer.outbox_relay import build_event


class BuildEventTests(unittest.TestCase):
    def test_uses_outbox_id_as_stable_event_id(self) -> None:
        row = {
            "id": "outbox-event-id",
            "aggregate_type": "ORDER",
            "aggregate_id": "order-id",
            "event_type": "OrderCreated",
            "created_at": datetime(2026, 11, 16, tzinfo=timezone.utc),
            "payload": {
                "event_id": "different-payload-id",
                "timestamp": "2026-11-16T00:00:00+00:00",
                "order": {
                    "order_id": "order-id",
                    "total_amount": 12.5,
                    "payment_trigger": {"action": "INITIATE_PAYMENT"},
                },
            },
        }

        event = build_event(row)

        self.assertEqual(event["event_id"], "outbox-event-id")
        self.assertEqual(event["aggregate_id"], "order-id")
        self.assertEqual(event["data"]["payment_trigger"]["action"], "INITIATE_PAYMENT")
        self.assertNotIn("payment_trigger", event["data"]["order"])

    def test_rejects_payload_without_order(self) -> None:
        row = {
            "id": "outbox-event-id",
            "payload": {},
        }

        with self.assertRaises(ValueError):
            build_event(row)


if __name__ == "__main__":
    unittest.main()
