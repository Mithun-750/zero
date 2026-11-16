"""Data prep package for Order Outbox service."""

from data_prep.generate_orders import generate_orders
from data_prep.load_orders import load_orders


def main() -> None:
    print("Transactional Outbox Data Prep Toolkit")
    print("Available commands:")
    print("  uv run generate-orders [-c COUNT] [-o PATH]")
    print("  uv run load-orders [-i PATH] [-g COUNT]")
    print("  uv run test-load [-c COUNT] [-t PATH]")


__all__ = ["generate_orders", "load_orders"]
