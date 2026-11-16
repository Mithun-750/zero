"""Live test demonstrating crash-window recovery between DB commit and Kafka offset commit."""

from __future__ import annotations

import unittest
import uuid
from typing import Any

from producer.config import load_settings
from producer.consumer import OrderConsumer
from producer.database import connect
from producer.kafka_publisher import KafkaPublisher


class CrashWindowRecoveryIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = load_settings()
        self.conn = connect(self.settings)
        self.test_event_id = str(uuid.uuid4())
        self.test_agg_id = str(uuid.uuid4())

    def tearDown(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM processed_events WHERE event_id = %s::uuid",
                (self.test_event_id,),
            )
        self.conn.close()

    def test_crash_after_db_commit_recovers_cleanly(self) -> None:
        # 1. Publish test event to Kafka
        publisher = KafkaPublisher(self.settings)
        event = {
            "event_id": self.test_event_id,
            "event_type": "OrderCreated",
            "event_version": 1,
            "occurred_at": "2026-11-16T18:40:00+00:00",
            "aggregate_type": "ORDER",
            "aggregate_id": self.test_agg_id,
            "source": "order-service",
            "data": {"order": {"id": self.test_agg_id}},
        }
        publisher.publish(event, key=self.test_agg_id)
        publisher.close()

        # Track business callback executions
        business_call_count = 0

        def business_action(cur: Any, agg_id: Any) -> None:
            nonlocal business_call_count
            business_call_count += 1

        # Hook to simulate crash AFTER DB commit but BEFORE Kafka offset commit
        def simulated_crash_hook(event_id: Any) -> None:
            raise RuntimeError("CRASH_INJECTION: Consumer process killed before offset commit!")

        # 2. Run consumer that crashes at the failure window
        consumer_crashing = OrderConsumer(
            self.settings,
            business_action_fn=business_action,
            hook_before_offset_commit=simulated_crash_hook,
        )

        with self.assertRaises(RuntimeError):
            consumer_crashing.run(max_messages=1)

        consumer_crashing.close()

        # 3. Assert PostgreSQL committed the event, and business callback executed once
        self.assertEqual(business_call_count, 1)
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) as count FROM processed_events WHERE event_id = %s::uuid",
                (self.test_event_id,),
            )
            self.assertEqual(cur.fetchone()["count"], 1)

        # 4. Restart consumer normally (simulating process restart)
        # Because the offset was never committed, Kafka will redeliver the event
        consumer_restarted = OrderConsumer(
            self.settings,
            business_action_fn=business_action,
        )

        processed = consumer_restarted.run(max_messages=1)
        consumer_restarted.close()

        # 5. Assert:
        # - The redelivered message was processed
        self.assertEqual(processed, 1)
        # - The business callback was NOT executed again (still 1)
        self.assertEqual(business_call_count, 1)
        # - Exactly 1 row exists in processed_events (no duplicate records)
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) as count FROM processed_events WHERE event_id = %s::uuid",
                (self.test_event_id,),
            )
            self.assertEqual(cur.fetchone()["count"], 1)


if __name__ == "__main__":
    unittest.main()

