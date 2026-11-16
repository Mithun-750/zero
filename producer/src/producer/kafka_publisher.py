"""Kafka publishing with broker-acknowledgement confirmation."""

from __future__ import annotations

import json
from typing import Any

from kafka import KafkaProducer

from producer.config import Settings


class KafkaPublisher:
    def __init__(self, settings: Settings) -> None:
        self._topic = settings.kafka_orders_topic
        self._producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=settings.kafka_client_id,
            acks="all",
            enable_idempotence=True,
        )

    def publish(self, event: dict[str, Any], key: str) -> None:
        value = json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        self._producer.send(self._topic, key=key.encode("utf-8"), value=value).get(timeout=120)

    def close(self) -> None:
        self._producer.flush(timeout=10)
        self._producer.close(timeout=10)
