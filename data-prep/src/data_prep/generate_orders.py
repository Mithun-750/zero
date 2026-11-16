"""Order generation module for Transactional Outbox pattern demo.

Generates synthetic e-commerce orders with line items ready for database loading.
"""

from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from faker import Faker

fake = Faker()

SAMPLE_PRODUCTS = [
    {"product_id": "PROD-1001", "name": "Wireless Noise-Canceling Headphones", "price": 199.99},
    {"product_id": "PROD-1002", "name": "Mechanical Keyboard RGB", "price": 119.50},
    {"product_id": "PROD-1003", "name": "Ergonomic Office Chair", "price": 349.00},
    {"product_id": "PROD-1004", "name": "Ultra-Wide Gaming Monitor 34-inch", "price": 499.99},
    {"product_id": "PROD-1005", "name": "USB-C Fast Charging Station", "price": 39.99},
    {"product_id": "PROD-1006", "name": "Bluetooth Portable Speaker", "price": 79.95},
    {"product_id": "PROD-1007", "name": "Smart Fitness Watch", "price": 149.00},
    {"product_id": "PROD-1008", "name": "Leather Laptop Backpack", "price": 89.90},
    {"product_id": "PROD-1009", "name": "4K Webcam with Microphone", "price": 69.99},
    {"product_id": "PROD-1010", "name": "Aluminum Laptop Stand", "price": 29.99},
]


def generate_single_order() -> dict[str, Any]:
    """Generate a single order with random line items."""
    order_id = str(uuid.uuid4())
    order_timestamp = datetime.now(timezone.utc).isoformat()
    order_number = f"ORD-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{random.randint(100000, 999999)}"

    # Pick 1 to 4 distinct products
    chosen_products = random.sample(SAMPLE_PRODUCTS, k=random.randint(1, 4))
    items: list[dict[str, Any]] = []
    total_amount = 0.0

    for prod in chosen_products:
        quantity = random.randint(1, 3)
        unit_price = prod["price"]
        subtotal = round(quantity * unit_price, 2)
        total_amount += subtotal

        items.append(
            {
                "item_id": str(uuid.uuid4()),
                "product_id": prod["product_id"],
                "product_name": prod["name"],
                "quantity": quantity,
                "unit_price": unit_price,
                "subtotal": subtotal,
            }
        )

    total_amount = round(total_amount, 2)

    return {
        "id": order_id,
        "order_number": order_number,
        "customer": {
            "customer_id": f"CUST-{random.randint(1000, 9999)}",
            "customer_name": fake.name(),
            "customer_email": fake.email(),
        },
        "total_amount": total_amount,
        "currency": "USD",
        "status": "CREATED",
        "created_at": order_timestamp,
        "items": items,
    }


def generate_orders(count: int = 10) -> list[dict[str, Any]]:
    """Generate multiple synthetic orders."""
    return [generate_single_order() for _ in range(count)]


def save_orders_to_file(orders: list[dict[str, Any]], filepath: str | Path) -> Path:
    """Save generated orders to a JSON file."""
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(orders, f, indent=2)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic orders for Outbox pattern demonstration.")
    parser.add_argument(
        "-c", "--count", type=int, default=10, help="Number of orders to generate (default: 10)"
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default="data/orders.json",
        help="Path to output JSON file (default: data/orders.json)",
    )
    args = parser.parse_args()

    print(f"Generating {args.count} orders...")
    orders = generate_orders(args.count)
    saved_path = save_orders_to_file(orders, args.output)
    print(f"Successfully generated {len(orders)} orders and saved to: {saved_path}")


if __name__ == "__main__":
    main()

