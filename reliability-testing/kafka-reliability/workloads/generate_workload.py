import os
import sys
import json
import uuid
import random
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(usecwd=True))
sys.path.insert(0, os.path.abspath("data-prep/src"))

from data_prep.load_orders import get_db_connection, insert_order
from data_prep.generate_orders import generate_orders

OUTPUT_DIR = Path(os.getenv("WORKLOAD_OUTPUT_DIR", "reliability-testing/kafka-reliability/workloads"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ORDERS_FILE = Path(os.getenv("WORKLOAD_OUTPUT_FILE", OUTPUT_DIR / "generated-orders.json"))

def main():
    conn = get_db_connection()
    cur = conn.cursor()

    # Query existing order numbers to ensure absolute uniqueness
    cur.execute("SELECT order_number FROM orders")
    existing_numbers = set(row["order_number"] for row in cur.fetchall())
    conn.commit()
    print(f"Found {len(existing_numbers)} existing order numbers in database.")

    # Generate 1000 orders
    orders = []
    today_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    
    # Custom loop to guarantee zero order_number collisions
    used_numbers = set(existing_numbers)
    generated = generate_orders(count=1000)
    
    seq = 500000
    for order in generated:
        # Guarantee uniqueness
        while order["order_number"] in used_numbers:
            seq += 1
            order["order_number"] = f"ORD-{today_str}-E{seq:05d}"
        used_numbers.add(order["order_number"])
        orders.append(order)

    print(f"Generated {len(orders)} unique orders.")

    # Save to JSON
    with open(ORDERS_FILE, "w", encoding="utf-8") as f:
        json.dump(orders, f, indent=2)
    print(f"Saved 1,000 orders to {ORDERS_FILE}")

    # Insert into PostgreSQL
    conn.autocommit = False
    inserted_count = 0
    try:
        for idx, order in enumerate(orders):
            insert_order(conn, order)
            inserted_count += 1
            if (idx + 1) % 200 == 0:
                conn.commit()
                print(f"Committed {idx + 1}/1000 orders...")
        conn.commit()
        print("All 1,000 orders committed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error during insert: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()

    # Verify state via a fresh connection
    conn2 = get_db_connection()
    cur2 = conn2.cursor()
    order_ids = [order["id"] for order in orders]
    cur2.execute("SELECT COUNT(*) AS cnt FROM orders WHERE id = ANY(%s)", (order_ids,))
    orders_in_db = cur2.fetchone()["cnt"]

    cur2.execute("SELECT COUNT(*) AS cnt FROM outbox WHERE aggregate_id = ANY(%s) AND status = 'PENDING'", (order_ids,))
    pending_outbox = cur2.fetchone()["cnt"]
    conn2.close()

    print(f"Verification: Orders in DB: {orders_in_db}/1000, Pending Outbox in DB: {pending_outbox}/1000")
    if orders_in_db != 1000 or pending_outbox != 1000:
        print("[FAIL] Mismatch in generated orders or pending outbox records!", file=sys.stderr)
        sys.exit(1)
    print("[SUCCESS] Workload generated and verified.")

if __name__ == "__main__":
    main()
