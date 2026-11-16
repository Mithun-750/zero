"""Configuration for the outbox relay."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import find_dotenv, load_dotenv


def _positive_int(name: str, default: str) -> int:
    value = int(os.getenv(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _positive_float(name: str, default: str) -> float:
    value = float(os.getenv(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be a positive number")
    return value


@dataclass(frozen=True)
class Settings:
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    kafka_bootstrap_servers: str
    kafka_orders_topic: str
    kafka_client_id: str
    outbox_batch_size: int
    outbox_poll_interval_seconds: float


def load_settings() -> Settings:
    load_dotenv(find_dotenv(usecwd=True))
    return Settings(
        postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
        postgres_port=_positive_int("POSTGRES_PORT", "5433"),
        postgres_db=os.getenv("POSTGRES_DB", "order_db"),
        postgres_user=os.getenv("POSTGRES_USER", "postgres"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_orders_topic=os.getenv("KAFKA_ORDERS_TOPIC", "orders.created"),
        kafka_client_id=os.getenv("KAFKA_CLIENT_ID", "order-outbox-relay"),
        outbox_batch_size=_positive_int("OUTBOX_BATCH_SIZE", "100"),
        outbox_poll_interval_seconds=_positive_float("OUTBOX_POLL_INTERVAL_SECONDS", "2"),
    )
