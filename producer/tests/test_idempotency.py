"""Integration tests for atomic idempotency in PostgreSQL."""

from __future__ import annotations

import concurrent.futures
import unittest
import uuid
from typing import Any

from producer.config import load_settings
from producer.database import connect, process_event_idempotently


class ProcessEventIdempotentlyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = load_settings()
        self.conn = connect(self.settings)
        self.created_event_ids: list[str] = []

    def tearDown(self) -> None:
        if self.created_event_ids:
            with self.conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM processed_events WHERE event_id = ANY(%s::uuid[])",
                    (self.created_event_ids,),
                )
        self.conn.close()

    def test_first_time_event(self) -> None:
        event_id = str(uuid.uuid4())
        aggregate_id = str(uuid.uuid4())
        self.created_event_ids.append(event_id)

        business_calls = []

        def business_action(cur: Any, agg_id: Any) -> None:
            business_calls.append(agg_id)

        processed, status = process_event_idempotently(
            self.conn,
            event_id=event_id,
            consumer_name="test-consumer",
            aggregate_id=aggregate_id,
            business_action_fn=business_action,
        )

        self.assertTrue(processed)
        self.assertEqual(status, "PROCESSED")
        self.assertEqual(len(business_calls), 1)
        self.assertEqual(business_calls[0], aggregate_id)

        # Verify database record
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT event_id, consumer_name, aggregate_id FROM processed_events WHERE event_id = %s",
                (event_id,),
            )
            row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row["event_id"]), event_id)
            self.assertEqual(row["consumer_name"], "test-consumer")
            self.assertEqual(str(row["aggregate_id"]), aggregate_id)

    def test_duplicate_event_is_skipped(self) -> None:
        event_id = str(uuid.uuid4())
        aggregate_id = str(uuid.uuid4())
        self.created_event_ids.append(event_id)

        business_calls = []

        def business_action(cur: Any, agg_id: Any) -> None:
            business_calls.append(agg_id)

        # First delivery
        p1, s1 = process_event_idempotently(
            self.conn,
            event_id=event_id,
            consumer_name="test-consumer",
            aggregate_id=aggregate_id,
            business_action_fn=business_action,
        )
        self.assertTrue(p1)
        self.assertEqual(s1, "PROCESSED")
        self.assertEqual(len(business_calls), 1)

        # Duplicate delivery
        p2, s2 = process_event_idempotently(
            self.conn,
            event_id=event_id,
            consumer_name="test-consumer",
            aggregate_id=aggregate_id,
            business_action_fn=business_action,
        )
        self.assertFalse(p2)
        self.assertEqual(s2, "DUPLICATE_SKIPPED")
        self.assertEqual(len(business_calls), 1)  # Business callback must NOT execute again

        # Exactly 1 row in database
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) as count FROM processed_events WHERE event_id = %s",
                (event_id,),
            )
            self.assertEqual(cur.fetchone()["count"], 1)

    def test_different_event_ids_processed_independently(self) -> None:
        event_id_1 = str(uuid.uuid4())
        event_id_2 = str(uuid.uuid4())
        self.created_event_ids.extend([event_id_1, event_id_2])

        p1, s1 = process_event_idempotently(
            self.conn,
            event_id=event_id_1,
            consumer_name="test-consumer",
            aggregate_id=str(uuid.uuid4()),
        )
        p2, s2 = process_event_idempotently(
            self.conn,
            event_id=event_id_2,
            consumer_name="test-consumer",
            aggregate_id=str(uuid.uuid4()),
        )

        self.assertTrue(p1)
        self.assertEqual(s1, "PROCESSED")
        self.assertTrue(p2)
        self.assertEqual(s2, "PROCESSED")

    def test_business_action_failure_rolls_back_everything(self) -> None:
        event_id = str(uuid.uuid4())
        aggregate_id = str(uuid.uuid4())
        self.created_event_ids.append(event_id)

        def failing_business_action(cur: Any, agg_id: Any) -> None:
            raise RuntimeError("Simulated business database failure")

        # Must raise the exception
        with self.assertRaises(RuntimeError):
            process_event_idempotently(
                self.conn,
                event_id=event_id,
                consumer_name="test-consumer",
                aggregate_id=aggregate_id,
                business_action_fn=failing_business_action,
            )

        # Assert processed_events record was rolled back
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) as count FROM processed_events WHERE event_id = %s",
                (event_id,),
            )
            self.assertEqual(cur.fetchone()["count"], 0)

        # Retry after fixing business logic succeeds
        recovered_calls = []
        p, s = process_event_idempotently(
            self.conn,
            event_id=event_id,
            consumer_name="test-consumer",
            aggregate_id=aggregate_id,
            business_action_fn=lambda cur, agg: recovered_calls.append(agg),
        )
        self.assertTrue(p)
        self.assertEqual(s, "PROCESSED")
        self.assertEqual(len(recovered_calls), 1)

    def test_caller_outer_transaction_not_prematurely_committed(self) -> None:
        event_id = str(uuid.uuid4())
        aggregate_id = str(uuid.uuid4())
        self.created_event_ids.append(event_id)

        # Caller starts their own outer transaction block
        with self.conn.transaction():
            p, s = process_event_idempotently(
                self.conn,
                event_id=event_id,
                consumer_name="test-consumer",
                aggregate_id=aggregate_id,
            )
            self.assertTrue(p)
            self.assertEqual(s, "PROCESSED")

        # Confirmed committed after outer transaction finishes
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) as count FROM processed_events WHERE event_id = %s",
                (event_id,),
            )
            self.assertEqual(cur.fetchone()["count"], 1)

    def test_concurrent_duplicate_processing(self) -> None:
        event_id = str(uuid.uuid4())
        aggregate_id = str(uuid.uuid4())
        self.created_event_ids.append(event_id)

        results: list[tuple[bool, str]] = []
        business_call_count = 0
        lock = concurrent.futures.thread.threading.Lock()

        def concurrent_worker() -> tuple[bool, str]:
            nonlocal business_call_count
            worker_conn = connect(self.settings)
            try:
                def business_action(cur: Any, agg: Any) -> None:
                    nonlocal business_call_count
                    with lock:
                        business_call_count += 1

                return process_event_idempotently(
                    worker_conn,
                    event_id=event_id,
                    consumer_name="test-consumer",
                    aggregate_id=aggregate_id,
                    business_action_fn=business_action,
                )
            finally:
                worker_conn.close()

        # Run 5 concurrent threads attempting to process the exact same event
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(concurrent_worker) for _ in range(5)]
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())

        # Exactly 1 thread must be PROCESSED; remaining 4 must be DUPLICATE_SKIPPED
        processed_count = sum(1 for res in results if res == (True, "PROCESSED"))
        duplicate_count = sum(1 for res in results if res == (False, "DUPLICATE_SKIPPED"))

        self.assertEqual(processed_count, 1)
        self.assertEqual(duplicate_count, 4)
        self.assertEqual(business_call_count, 1)

        # Database must have exactly 1 record
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) as count FROM processed_events WHERE event_id = %s",
                (event_id,),
            )
            self.assertEqual(cur.fetchone()["count"], 1)


if __name__ == "__main__":
    unittest.main()

