"""Configuration for the payment consumer."""
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
    kafka_dlq_topic: str
    kafka_consumer_group: str
    kafka_client_id: str
    consumer_max_retries: int
    consumer_retry_backoff_seconds: float
    dashboard_port: int
    dashboard_host: str


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
        kafka_dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", "payments.dlq"),
        kafka_consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "payment-consumer"),
        # The repo shares ONE root .env between relay and consumer, which
        # sets KAFKA_CLIENT_ID=order-outbox-relay. The consumer has its own
        # identity variable so Kafka logs show it as the consumer, not the
        # relay. (Producer keeps KAFKA_CLIENT_ID=order-outbox-relay.)
        kafka_client_id=os.getenv("KAFKA_CONSUMER_CLIENT_ID", "payment-consumer"),
        consumer_max_retries=_positive_int("CONSUMER_MAX_RETRIES", "3"),
        consumer_retry_backoff_seconds=_positive_float("CONSUMER_RETRY_BACKOFF_SECONDS", "0.5"),
        dashboard_port=_positive_int("DASHBOARD_PORT", "8090"),
        dashboard_host=os.getenv("DASHBOARD_HOST", "127.0.0.1"),
    )
