"""Event-by-event reconciliation and RPO analysis for DL-09 test runs."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[2]
for src_dir in [ROOT_DIR / "producer" / "src", ROOT_DIR / "data-prep" / "src"]:
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

from data_prep.load_orders import get_db_connection

logger = logging.getLogger("chaos.dl09.reconcile")


def reconcile_manifest(manifest_path: Path | str) -> dict[str, Any]:
    """Reconcile 1,000 events against PostgreSQL outbox and processed_events."""
    path = Path(manifest_path)
    if not path.exists():
        raise FileNotFoundError(f"Manifest not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    run_id = manifest["run_id"]
    events = manifest["events"]
    total_events = len(events)
    event_ids = [e["outbox_event_id"] for e in events]
    order_ids = [e["order_id"] for e in events]

    logger.info("Reconciling %s events for run_id=%s...", total_events, run_id)

    conn = get_db_connection()
    try:
        # 1. Query Outbox table for all test events
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, aggregate_id, status, retry_count, published_at, error_message
                FROM outbox
                WHERE id = ANY(%s::uuid[])
                """,
                (event_ids,),
            )
            outbox_rows = cur.fetchall()

        outbox_by_id = {str(row["id"]): row for row in outbox_rows}
        published_outbox_ids = {
            str(row["id"]) for row in outbox_rows if row["status"] == "PUBLISHED"
        }
        pending_outbox_ids = {
            str(row["id"]) for row in outbox_rows if row["status"] in ("PENDING", "FAILED")
        }

        # 2. Query Processed Events table
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, consumer_name, aggregate_id, processed_at
                FROM processed_events
                WHERE event_id = ANY(%s::uuid[])
                """,
                (event_ids,),
            )
            processed_rows = cur.fetchall()

        processed_event_ids = {str(row["event_id"]) for row in processed_rows}

        # 3. Check for duplicates in processed_events
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, COUNT(*) as count
                FROM processed_events
                WHERE event_id = ANY(%s::uuid[])
                GROUP BY event_id
                HAVING COUNT(*) > 1
                """,
                (event_ids,),
            )
            duplicate_rows = cur.fetchall()

        duplicate_business_ops = sum(row["count"] - 1 for row in duplicate_rows)

        # 4. Identify missing / unexplained events
        # An event is missing if it was accepted in DB outbox, marked published, but never recorded in processed_events
        missing_acknowledged_events = published_outbox_ids - processed_event_ids
        unexplained_events = set(event_ids) - (published_outbox_ids | pending_outbox_ids)

        # Summary Metrics
        accepted_events = len(outbox_rows)
        published_events = len(published_outbox_ids)
        pending_events = len(pending_outbox_ids)
        processed_events = len(processed_event_ids)

        # Observed RPO: Data loss occurs if an event was acknowledged/published but lost from Kafka/downstream
        # RPO = 0 if missing_acknowledged_events == 0 and unexplained_events == 0
        observed_rpo = len(missing_acknowledged_events)

        report = {
            "run_id": run_id,
            "target_events": total_events,
            "accepted_events": accepted_events,
            "published_events": published_events,
            "processed_events": processed_events,
            "pending_outbox_events": pending_events,
            "missing_acknowledged_events": len(missing_acknowledged_events),
            "unexplained_events": len(unexplained_events),
            "duplicate_business_operations": duplicate_business_ops,
            "observed_rpo": observed_rpo,
            "rpo_zero_satisfied": (observed_rpo == 0 and len(unexplained_events) == 0),
        }

        print("\n" + "=" * 70)
        print(f"EVENT RECONCILIATION REPORT: {run_id}")
        print("=" * 70)
        print(f"Accepted Outbox Events         : {accepted_events}")
        print(f"Published to Kafka             : {published_events}")
        print(f"Processed by Consumer (DB)     : {processed_events}")
        print(f"Pending in Outbox              : {pending_events}")
        print(f"Missing Acknowledged Events    : {len(missing_acknowledged_events)}")
        print(f"Unexplained Missing Events     : {len(unexplained_events)}")
        print(f"Duplicate Business Operations  : {duplicate_business_ops}")
        print(f"Observed RPO                   : {observed_rpo} (Target: 0)")
        print(f"RPO = 0 Status                 : {'PASS' if report['rpo_zero_satisfied'] else 'FAIL'}")
        print("=" * 70 + "\n")

        return report

    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile DL-09 test events.")
    parser.add_argument(
        "--manifest",
        type=str,
        default="chaos/dl09/manifest_DL09-2026-001.json",
        help="Path to manifest JSON file",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    reconcile_manifest(args.manifest)


if __name__ == "__main__":
    main()
