"""Database loader module for orders.

Inserts orders and order items into PostgreSQL within an ACID transaction.
The outbox event is automatically created by the PostgreSQL trigger ('trg_order_created_outbox').
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import find_dotenv, load_dotenv
import psycopg
from psycopg.rows import dict_row

from data_prep.generate_orders import generate_orders, save_orders_to_file

# Search for .env in current dir, parent dir, or project root
load_dotenv(find_dotenv(usecwd=True))


def get_db_connection() -> psycopg.Connection:
    """Establish and return a connection to the PostgreSQL database."""
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5434"))
    dbname = os.getenv("POSTGRES_DB", "order_db")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")

    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    try:
        conn = psycopg.connect(conn_str, row_factory=dict_row, autocommit=True)
        return conn
    except Exception as exc:
        print(f"[Error] Failed to connect to PostgreSQL at {host}:{port}/{dbname}: {exc}", file=sys.stderr)
        print("Please check that the Postgres Docker container is running and port is correct.", file=sys.stderr)
        raise


def insert_order(conn: psycopg.Connection, order: dict[str, Any]) -> None:
    """
    Insert order and line items within a transaction.
    The database trigger 'trg_order_created_outbox' automatically populates
    the outbox table with an 'OrderCreated' payment trigger event.
    """
    order_id = order["id"]
    order_number = order["order_number"]
    customer = order["customer"]
    total_amount = order["total_amount"]
    currency = order["currency"]
    order_status = order.get("status", "CREATED")
    created_at = order.get("created_at", datetime.now(timezone.utc).isoformat())
    items = order.get("items", [])

    with conn.transaction():
        with conn.cursor() as cur:
            # 1. Insert into orders (fires database trigger trg_order_created_outbox)
            cur.execute(
                """
                INSERT INTO orders (
                    id, order_number, customer_id, customer_name, customer_email,
                    total_amount, currency, status, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    order_id,
                    order_number,
                    customer["customer_id"],
                    customer["customer_name"],
                    customer["customer_email"],
                    total_amount,
                    currency,
                    order_status,
                    created_at,
                    created_at,
                ),
            )

            # 2. Insert into order_items
            for item in items:
                cur.execute(
                    """
                    INSERT INTO order_items (
                        id, order_id, product_id, product_name, quantity, unit_price, subtotal, created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        item.get("item_id", str(uuid.uuid4())),
                        order_id,
                        item["product_id"],
                        item["product_name"],
                        item["quantity"],
                        item["unit_price"],
                        item["subtotal"],
                        created_at,
                    ),
                )


def load_orders(orders: list[dict[str, Any]]) -> tuple[int, int]:
    """Load a list of orders into Postgres."""
    conn = get_db_connection()
    success_count = 0
    fail_count = 0

    print(f"Loading {len(orders)} order(s) into PostgreSQL...")

    try:
        for idx, order in enumerate(orders, start=1):
            try:
                insert_order(conn, order)
                success_count += 1
                if idx % 10 == 0 or idx == len(orders):
                    print(f"  Processed {idx}/{len(orders)} orders...")
            except Exception as e:
                fail_count += 1
                print(f"  [Error] Failed to process order {order.get('id')}: {e}", file=sys.stderr)

        # Query summary counts to verify trigger generation
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) as count FROM orders")
            total_orders = cur.fetchone()["count"]

            cur.execute("SELECT count(*) as count FROM order_items")
            total_items = cur.fetchone()["count"]

            cur.execute("SELECT count(*) as count FROM outbox WHERE status = 'PENDING'")
            pending_outbox = cur.fetchone()["count"]

        print("\n--- Load Verification Summary ---")
        print(f"Successfully inserted : {success_count} orders")
        if fail_count:
            print(f"Failed to insert      : {fail_count} orders")
        print(f"Total in 'orders'     : {total_orders}")
        print(f"Total in 'order_items': {total_items}")
        print(f"Total in 'outbox'     : {pending_outbox} (auto-populated by DB trigger)")
        print("---------------------------------\n")

    finally:
        conn.close()

    return success_count, fail_count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load orders into PostgreSQL; Outbox table is auto-populated via PostgreSQL trigger."
    )
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        default="data/orders.json",
        help="Path to JSON file containing orders (default: data/orders.json)",
    )
    parser.add_argument(
        "-g",
        "--generate",
        type=int,
        default=0,
        help="If set, generates this many orders on the fly instead of reading from file",
    )
    args = parser.parse_args()

    if args.generate > 0:
        print(f"Generating {args.generate} fresh orders on the fly...")
        orders = generate_orders(args.generate)
        save_orders_to_file(orders, args.input)
    else:
        input_path = Path(args.input)
        if not input_path.exists():
            print(f"Input file '{args.input}' not found. Generating 1000 new orders first...")
            orders = generate_orders(1000)
            save_orders_to_file(orders, args.input)
        else:
            with input_path.open("r", encoding="utf-8") as f:
                orders = json.load(f)

    load_orders(orders)


if __name__ == "__main__":
    main()
