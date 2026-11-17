"""DLQ envelope tests — pure builder, no broker."""
from __future__ import annotations

import unittest
from typing import Any

from consumer.dlq import RAW_PAYLOAD_MAX, REASON_MAX, build_dlq_envelope


def _envelope(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = dict(
        raw_payload=b'{"broken":',
        event=None,
        failure_class="VALIDATION",
        failure_reason="JSON parse error: Expecting ',' delimiter",
        attempt_count=1,
        source_topic="orders.created",
        source_partition=2,
        source_offset=42,
        source_timestamp=None,
    )
    base.update(overrides)
    return build_dlq_envelope(**base)


class BuildDlqEnvelopeTests(unittest.TestCase):
    def test_unparseable_record_keeps_raw_bytes(self) -> None:
        env = _envelope()
        self.assertEqual(env["raw_payload"], '{"broken":')
        self.assertEqual(env["failure_class"], "VALIDATION")
        self.assertEqual(env["source"]["topic"], "orders.created")
        self.assertEqual(env["source"]["partition"], 2)
        self.assertEqual(env["source"]["offset"], 42)
        self.assertEqual(env["consumer_name"], "payment-consumer")

    def test_event_fields_are_echoed_when_present(self) -> None:
        event = {
            "event_id": "11111111-1111-1111-1111-111111111111",
            "event_type": "OrderCreated",
            "aggregate_id": "22222222-2222-2222-2222-222222222222",
        }
        env = _envelope(event=event)
        self.assertEqual(env["event_id"], event["event_id"])
        self.assertEqual(env["aggregate_id"], event["aggregate_id"])

    def test_raw_payload_truncated_to_limit(self) -> None:
        env = _envelope(raw_payload=b"x" * (RAW_PAYLOAD_MAX * 2))
        self.assertEqual(len(env["raw_payload"]), RAW_PAYLOAD_MAX)

    def test_failure_reason_truncated_to_limit(self) -> None:
        env = _envelope(failure_reason="x" * (REASON_MAX * 2))
        self.assertEqual(len(env["failure_reason"]), REASON_MAX)

    def test_non_dict_event_is_ignored(self) -> None:
        env = _envelope(event="not-a-dict")  # type: ignore[arg-type]
        self.assertIsNone(env["event_id"])


if __name__ == "__main__":
    unittest.main()
