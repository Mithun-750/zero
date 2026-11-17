"""Pipeline decision-path tests.

Mocks the module seams (state / applier / dlq) so no broker or database is
required — every branch asserts which side effects happened and what the
caller may commit.
"""
from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from consumer import pipeline
from consumer.classify import BusinessRuleError
from consumer.config import Settings
from consumer.metrics import Metrics

VALID = {
    "event_id": "11111111-1111-1111-1111-111111111111",
    "event_type": "OrderCreated",
    "event_version": 1,
    "aggregate_type": "ORDER",
    "aggregate_id": "22222222-2222-2222-2222-222222222222",
    "data": {
        "order": {"order_id": "22222222-2222-2222-2222-222222222222"},
        "payment_trigger": {
            "action": "INITIATE_PAYMENT",
            "amount": 12.5,
            "currency": "INR",
        },
    },
}
VALID_BYTES = json.dumps(VALID).encode("utf-8")


def make_settings() -> Settings:
    return Settings(
        postgres_host="localhost",
        postgres_port=5433,
        postgres_db="order_db",
        postgres_user="postgres",
        postgres_password="postgres",
        kafka_bootstrap_servers="localhost:9092",
        kafka_orders_topic="orders.created",
        kafka_dlq_topic="payments.dlq",
        kafka_consumer_group="payment-consumer",
        kafka_client_id="payment-consumer",
        consumer_max_retries=2,
        consumer_retry_backoff_seconds=0.01,
        dashboard_port=0,
        dashboard_host="127.0.0.1",
    )


def make_record(value, offset=7):
    return SimpleNamespace(
        topic="orders.created",
        partition=2,
        offset=offset,
        value=value,
        timestamp=1_700_000_000_000,
    )


class PipelineTests(unittest.TestCase):
    def test_parked_when_aggregate_blocked(self) -> None:
        conn = MagicMock()
        settings = make_settings()
        with patch.object(pipeline.state, "is_blocked", return_value=True), \
             patch.object(pipeline.state, "park_event", return_value=True) as park, \
             patch.object(pipeline.applier, "apply_event") as apply_mock:
            outcome = pipeline.process_record(conn, make_record(VALID_BYTES), settings, Metrics())
        self.assertEqual(outcome, "parked")
        park.assert_called_once()
        apply_mock.assert_not_called()  # blocked followers MUST NOT be applied

    def test_redelivery_of_parked_event_is_not_double_parked(self) -> None:
        conn = MagicMock()
        settings = make_settings()
        with patch.object(pipeline.state, "is_blocked", return_value=True), \
             patch.object(pipeline.state, "park_event", return_value=False), \
             patch.object(pipeline.applier, "apply_event") as apply_mock:
            outcome = pipeline.process_record(conn, make_record(VALID_BYTES), settings, Metrics())
        self.assertEqual(outcome, "parked")  # idempotent no-op, still commits
        apply_mock.assert_not_called()

    def test_applied(self) -> None:
        conn = MagicMock()
        settings = make_settings()
        with patch.object(pipeline.state, "is_blocked", return_value=False), \
             patch.object(pipeline.applier, "apply_event", return_value="applied"):
            outcome = pipeline.process_record(conn, make_record(VALID_BYTES), settings, Metrics())
        self.assertEqual(outcome, "applied")

    def test_duplicate(self) -> None:
        conn = MagicMock()
        settings = make_settings()
        with patch.object(pipeline.state, "is_blocked", return_value=False), \
             patch.object(pipeline.applier, "apply_event", return_value="duplicate"):
            outcome = pipeline.process_record(conn, make_record(VALID_BYTES), settings, Metrics())
        self.assertEqual(outcome, "duplicate")

    def test_parse_error_goes_to_dlq_without_blocking(self) -> None:
        conn = MagicMock()
        settings = make_settings()
        dlq_meta = {"dlq_topic": "payments.dlq", "dlq_partition": 0, "dlq_offset": 1}
        with patch.object(pipeline.dlq, "publish_to_dlq", return_value=dlq_meta) as pub, \
             patch.object(pipeline.state, "record_failure") as rec, \
             patch.object(pipeline.state, "block_aggregate") as block:
            outcome = pipeline.process_record(conn, make_record(b"\x7bbroken"), settings, Metrics())
        self.assertEqual(outcome, "failed")
        pub.assert_called_once()
        rec.assert_called_once()
        block.assert_not_called()  # no aggregate identifiable → nothing to block

    def test_permanent_error_dlqs_and_blocks_aggregate(self) -> None:
        conn = MagicMock()
        settings = make_settings()
        dlq_meta = {"dlq_topic": "payments.dlq", "dlq_partition": 0, "dlq_offset": 2}
        with patch.object(pipeline.state, "is_blocked", return_value=False), \
             patch.object(pipeline.applier, "apply_event",
                          side_effect=BusinessRuleError("amount must be positive, got -50")), \
             patch.object(pipeline.dlq, "publish_to_dlq", return_value=dlq_meta), \
             patch.object(pipeline.state, "record_failure") as rec, \
             patch.object(pipeline.state, "block_aggregate") as block:
            outcome = pipeline.process_record(conn, make_record(VALID_BYTES), settings, Metrics())
        self.assertEqual(outcome, "failed")
        rec.assert_called_once()
        block.assert_called_once()  # identified aggregate → block it

    def test_dlq_failure_means_offset_must_not_be_committed(self) -> None:
        conn = MagicMock()
        settings = make_settings()
        with patch.object(pipeline.state, "is_blocked", return_value=False), \
             patch.object(pipeline.applier, "apply_event",
                          side_effect=BusinessRuleError("bad")), \
             patch.object(pipeline.dlq, "publish_to_dlq", side_effect=Exception("broker down")), \
             patch.object(pipeline.state, "record_failure") as rec, \
             patch.object(pipeline.state, "block_aggregate") as block:
            with self.assertRaises(pipeline.OffsetRejectError):
                pipeline.process_record(conn, make_record(VALID_BYTES), settings, Metrics())
        rec.assert_not_called()   # nothing persisted → no commit → redeliver
        block.assert_not_called()

    def test_transient_exhaustion_goes_to_dlq(self) -> None:
        conn = MagicMock()
        settings = make_settings()
        dlq_meta = {"dlq_topic": "payments.dlq", "dlq_partition": 0, "dlq_offset": 3}
        from consumer.classify import TransientError

        with patch.object(pipeline.state, "is_blocked", return_value=False), \
             patch.object(pipeline.applier, "apply_event",
                          side_effect=TransientError("db closed")), \
             patch.object(pipeline.dlq, "publish_to_dlq", return_value=dlq_meta), \
             patch.object(pipeline.state, "record_failure") as rec, \
             patch.object(pipeline.state, "block_aggregate") as block, \
             patch("consumer.pipeline.time.sleep"):  # keep the test fast
            outcome = pipeline.process_record(conn, make_record(VALID_BYTES), settings, Metrics())
        self.assertEqual(outcome, "failed")
        rec.assert_called_once()
        self.assertEqual(rec.call_args.kwargs["failure_class"], "TRANSIENT_EXHAUSTED")
        self.assertEqual(rec.call_args.kwargs["attempt_count"], settings.consumer_max_retries)
        block.assert_called_once()


if __name__ == "__main__":
    unittest.main()
