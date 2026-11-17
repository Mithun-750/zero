"""Event generator and database loader for 1,000-event resilience testing."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Ensure project packages are on path
ROOT_DIR = Path(__file__).resolve().parents[2]
for src_dir in [ROOT_DIR / "producer" / "src", ROOT_DIR / "data-prep" / "src"]:
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

from data_prep.generate_orders import generate_orders
from data_prep.load_orders import get_db_connection, insert_order

logger = logging.getLogger("chaos.dl09.generate_events")


def generate_and_load_manifest(
    run_id: str,
    count: int = 1000,
    output_dir: Path | str = "chaos/dl09",
) -> Path:
    """Generate `count` orders with sequential logical IDs and persist atomically to PostgreSQL.

    Saves a manifest mapping logical IDs (E1..E1000) to order UUIDs and Outbox event UUIDs.
    """
    output_path = Path(output_dir) / f"manifest_{run_id}.json"
    if output_path.exists():
        raise FileExistsError(f"Manifest already exists for run_id={run_id} at {output_path}")

    conn = get_db_connection()
    try:
        # Pre-flight check: ensure no orders with this run_id exist
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) as count FROM orders WHERE order_number LIKE %s",
                (f"ORD-{run_id}-%",),
            )
            existing = cur.fetchone()["count"]
            if existing > 0:
                raise RuntimeError(f"Database already contains {existing} orders for run_id={run_id}")

        logger.info("Generating %s mock orders for run_id=%s...", count, run_id)
        raw_orders = generate_orders(count)

        manifest_events: list[dict[str, Any]] = []

        logger.info("Inserting %s orders into PostgreSQL (trigger will populate outbox)...", count)
        for idx, order in enumerate(raw_orders, start=1):
            logical_id = f"E{idx}"
            order["order_number"] = f"ORD-{run_id}-{logical_id}"

            # Insert order + items (fires trg_order_created_outbox atomically)
            insert_order(conn, order)

            # Retrieve the created outbox row to capture the actual event UUID
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, aggregate_id, event_type, status, created_at
                    FROM outbox
                    WHERE aggregate_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (order["id"],),
                )
                outbox_row = cur.fetchone()
                if not outbox_row:
                    raise RuntimeError(f"Failed to find outbox record for order {order['id']}")

                manifest_events.append(
                    {
                        "logical_id": logical_id,
                        "order_id": str(order["id"]),
                        "order_number": order["order_number"],
                        "outbox_event_id": str(outbox_row["id"]),
                        "aggregate_id": str(outbox_row["aggregate_id"]),
                        "status": outbox_row["status"],
                        "created_at": outbox_row["created_at"].isoformat(),
                    }
                )

            if idx % 100 == 0 or idx == count:
                logger.info("  Progress: %s/%s orders loaded into database", idx, count)

        manifest_data = {
            "run_id": run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "target_count": count,
            "actual_count": len(manifest_events),
            "events": manifest_events,
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(manifest_data, f, indent=2)

        logger.info("Saved test run manifest with %s events to: %s", len(manifest_events), output_path)
        return output_path

    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate and load 1,000 events for DL-09.")
    parser.add_argument(
        "--run-id",
        type=str,
        default="DL09-2026-001",
        help="Unique test run identifier (default: DL09-2026-001)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1000,
        help="Number of events to generate (default: 1000)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    generate_and_load_manifest(run_id=args.run_id, count=args.count)


if __name__ == "__main__":
    main()
