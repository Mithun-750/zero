"""Unit and mocked tests for OrderConsumer and manual offset management."""

from __future__ import annotations

import json
import unittest
import uuid
from typing import Any
from unittest.mock import MagicMock, call, patch

from kafka import OffsetAndMetadata, TopicPartition

from producer.config import load_settings
from producer.consumer import OrderConsumer, default_business_action


class FakeRecord:
    def __init__(
        self,
        topic: str = "orders.created",
        partition: int = 0,
        offset: int = 42,
        value: bytes = b"",
    ) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.value = value


class OrderConsumerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = load_settings()

    @patch("producer.consumer.connect")
    @patch("producer.consumer.KafkaConsumer")
    def _create_consumer(
        self,
        mock_kafka_cls: MagicMock,
        mock_connect_cls: MagicMock,
        business_action_fn: Any = None,
        hook: Any = None,
    ) -> tuple[OrderConsumer, MagicMock, MagicMock]:
        mock_kafka = MagicMock()
        mock_kafka_cls.return_value = mock_kafka
        mock_conn = MagicMock()
        mock_connect_cls.return_value = mock_conn

        consumer = OrderConsumer(
            self.settings,
            business_action_fn=business_action_fn,
            hook_before_offset_commit=hook,
        )
        return consumer, mock_kafka, mock_conn

    def test_enable_auto_commit_is_disabled(self) -> None:
        """Verify KafkaConsumer is created with enable_auto_commit=False."""
        with patch("producer.consumer.connect"), patch("producer.consumer.KafkaConsumer") as mock_kafka_cls:
            OrderConsumer(self.settings)
            _, kwargs = mock_kafka_cls.call_args
            self.assertFalse(kwargs.get("enable_auto_commit"))

    @patch("producer.consumer.process_event_idempotently")
    def test_valid_new_message_commits_offset(self, mock_idempotency: MagicMock) -> None:
        """Valid new message is processed and its offset (offset + 1) is committed."""
        mock_idempotency.return_value = (True, "PROCESSED")
        consumer, mock_kafka, _ = self._create_consumer()

        event_id = str(uuid.uuid4())
        agg_id = str(uuid.uuid4())
        payload = json.dumps({"event_id": event_id, "aggregate_id": agg_id}).encode("utf-8")
        record = FakeRecord(topic="orders.created", partition=1, offset=100, value=payload)

        is_new, status = consumer.process_record(record)

        self.assertTrue(is_new)
        self.assertEqual(status, "PROCESSED")
        mock_idempotency.assert_called_once()

        # Check explicit offset commit
        expected_tp = TopicPartition("orders.created", 1)
        mock_kafka.commit.assert_called_once_with(
            offsets={expected_tp: OffsetAndMetadata(101, "")}
        )

    @patch("producer.consumer.process_event_idempotently")
    def test_duplicate_message_skips_and_commits_offset(self, mock_idempotency: MagicMock) -> None:
        """Duplicate message skips business action and safely commits the offset."""
        mock_idempotency.return_value = (False, "DUPLICATE_SKIPPED")
        consumer, mock_kafka, _ = self._create_consumer()

        event_id = str(uuid.uuid4())
        agg_id = str(uuid.uuid4())
        payload = json.dumps({"event_id": event_id, "aggregate_id": agg_id}).encode("utf-8")
        record = FakeRecord(topic="orders.created", partition=0, offset=50, value=payload)

        is_new, status = consumer.process_record(record)

        self.assertFalse(is_new)
        self.assertEqual(status, "DUPLICATE_SKIPPED")
        expected_tp = TopicPartition("orders.created", 0)
        mock_kafka.commit.assert_called_once_with(
            offsets={expected_tp: OffsetAndMetadata(51, "")}
        )

    @patch("producer.consumer.process_event_idempotently")
    def test_database_failure_prevents_offset_commit(self, mock_idempotency: MagicMock) -> None:
        """Database error raises and prevents Kafka offset commit."""
        mock_idempotency.side_effect = RuntimeError("Database connection lost")
        consumer, mock_kafka, _ = self._create_consumer()

        event_id = str(uuid.uuid4())
        agg_id = str(uuid.uuid4())
        payload = json.dumps({"event_id": event_id, "aggregate_id": agg_id}).encode("utf-8")
        record = FakeRecord(topic="orders.created", partition=2, offset=75, value=payload)

        with self.assertRaises(RuntimeError):
            consumer.process_record(record)

        mock_kafka.commit.assert_not_called()

    @patch("producer.consumer.process_event_idempotently")
    def test_business_action_failure_prevents_offset_commit(self, mock_idempotency: MagicMock) -> None:
        """Failure inside the business callback raises and prevents offset commit."""
        mock_idempotency.side_effect = ValueError("Business validation failed")
        consumer, mock_kafka, _ = self._create_consumer()

        event_id = str(uuid.uuid4())
        agg_id = str(uuid.uuid4())
        payload = json.dumps({"event_id": event_id, "aggregate_id": agg_id}).encode("utf-8")
        record = FakeRecord(topic="orders.created", partition=2, offset=80, value=payload)

        with self.assertRaises(ValueError):
            consumer.process_record(record)

        mock_kafka.commit.assert_not_called()

    @patch("producer.consumer.process_event_idempotently")
    def test_offset_commit_failure_raises(self, mock_idempotency: MagicMock) -> None:
        """Kafka commit failure raises so caller knows message may be redelivered."""
        mock_idempotency.return_value = (True, "PROCESSED")
        consumer, mock_kafka, _ = self._create_consumer()
        mock_kafka.commit.side_effect = RuntimeError("Kafka broker commit timeout")

        event_id = str(uuid.uuid4())
        agg_id = str(uuid.uuid4())
        payload = json.dumps({"event_id": event_id, "aggregate_id": agg_id}).encode("utf-8")
        record = FakeRecord(topic="orders.created", partition=0, offset=10, value=payload)

        with self.assertRaises(RuntimeError):
            consumer.process_record(record)

    def test_invalid_json_is_not_committed(self) -> None:
        """Malformed JSON raises ValueError and never commits offset."""
        consumer, mock_kafka, _ = self._create_consumer()
        record = FakeRecord(value=b"NOT_A_VALID_JSON{{{")

        with self.assertRaises(ValueError):
            consumer.process_record(record)

        mock_kafka.commit.assert_not_called()

    def test_missing_event_id_is_not_committed(self) -> None:
        """Payload without event_id is rejected and never committed."""
        consumer, mock_kafka, _ = self._create_consumer()
        payload = json.dumps({"aggregate_id": str(uuid.uuid4())}).encode("utf-8")
        record = FakeRecord(value=payload)

        with self.assertRaises(ValueError):
            consumer.process_record(record)

        mock_kafka.commit.assert_not_called()

    def test_missing_aggregate_id_is_not_committed(self) -> None:
        """Payload without aggregate_id is rejected and never committed."""
        consumer, mock_kafka, _ = self._create_consumer()
        payload = json.dumps({"event_id": str(uuid.uuid4())}).encode("utf-8")
        record = FakeRecord(value=payload)

        with self.assertRaises(ValueError):
            consumer.process_record(record)

        mock_kafka.commit.assert_not_called()

    def test_invalid_uuid_format_is_not_committed(self) -> None:
        """Non-UUID string is rejected and never committed."""
        consumer, mock_kafka, _ = self._create_consumer()
        payload = json.dumps({"event_id": "not-a-uuid", "aggregate_id": str(uuid.uuid4())}).encode("utf-8")
        record = FakeRecord(value=payload)

        with self.assertRaises(ValueError):
            consumer.process_record(record)

        mock_kafka.commit.assert_not_called()

    def test_clean_shutdown(self) -> None:
        """Closing consumer closes both Kafka and PostgreSQL connections."""
        consumer, mock_kafka, mock_conn = self._create_consumer()
        consumer.close()

        mock_kafka.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("producer.consumer.process_event_idempotently")
    def test_batch_sequential_commits_independently(self, mock_idempotency: MagicMock) -> None:
        """A batch of 3 records commits sequentially, advancing offset strictly one by one."""
        mock_idempotency.return_value = (True, "PROCESSED")
        consumer, mock_kafka, _ = self._create_consumer()

        tp = TopicPartition("orders.created", 0)
        records = [
            FakeRecord(partition=0, offset=10, value=json.dumps({"event_id": str(uuid.uuid4()), "aggregate_id": str(uuid.uuid4())}).encode()),
            FakeRecord(partition=0, offset=11, value=json.dumps({"event_id": str(uuid.uuid4()), "aggregate_id": str(uuid.uuid4())}).encode()),
            FakeRecord(partition=0, offset=12, value=json.dumps({"event_id": str(uuid.uuid4()), "aggregate_id": str(uuid.uuid4())}).encode()),
        ]

        mock_kafka.poll.return_value = {tp: records}

        # Run with max_messages=3
        processed = consumer.run(max_messages=3)
        self.assertEqual(processed, 3)

        expected_commits = [
            call(offsets={tp: OffsetAndMetadata(11, "")}),
            call(offsets={tp: OffsetAndMetadata(12, "")}),
            call(offsets={tp: OffsetAndMetadata(13, "")}),
        ]
        mock_kafka.commit.assert_has_calls(expected_commits, any_order=False)


if __name__ == "__main__":
    unittest.main()

