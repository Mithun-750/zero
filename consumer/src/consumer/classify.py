"""Failure classification for consumer-side errors.

Every record ends in exactly one durable outcome, and the offset is committed
only after that outcome is durable:

  applied    — payment created (or idempotent no-op)
  duplicate  — event_id already in processed_events
  parked     — aggregate blocked; follower event parked in parked_events
  failed     — poison event routed to DLQ + aggregate blocked + ledger row

This module decides which failure bucket an error falls into. The taxonomy
is deliberately closed — the pipeline never invents a fifth outcome.
"""
from __future__ import annotations

import re
import uuid
from typing import Any

# Classes stored in event_failures.failure_class and shown in the dashboard.
VALIDATION = "VALIDATION"          # unparseable / envelope-invalid / contract drift
BUSINESS_RULE = "BUSINESS_RULE"    # parseable but nonsensical (amount <= 0, bad currency)
TRANSIENT_EXHAUSTED = "TRANSIENT_EXHAUSTED"  # retries exhausted (DB down, etc.)


class ConsumerError(Exception):
    """Base for consumer-side failures."""


class PermanentError(ConsumerError):
    """The event can never be applied as-is. Goes straight to DLQ — retrying
    a permanent failure just blocks the aggregate and the partition."""

    failure_class = VALIDATION


class ValidationError(PermanentError):
    # Unparseable bytes, missing/invalid envelope fields, unsupported
    # event_version, unparseable UUID.
    failure_class = VALIDATION


class BusinessRuleError(PermanentError):
    # Parsable but invalid per domain rules (amount, currency).
    failure_class = BUSINESS_RULE


class TransientError(ConsumerError):
    """Retryable infra failure (DB/broker hiccup). The pipeline retries
    consumer_max_retries times with backoff; on exhaustion the record is
    routed to the DLQ as TRANSIENT_EXHAUSTED."""

    failure_class = TRANSIENT_EXHAUSTED


class DlqPublishError(ConsumerError):
    """The DLQ write failed. Do NOT commit the source offset — the record
    must be redelivered and re-classified rather than silently lost."""


# Fields that must be UUID-shaped; anything else is permanent (not fixable
# by retrying).
_UUID_FIELDS = ("event_id", "aggregate_id")


def _require_uuid(obj: dict[str, Any], field: str) -> str:
    value = obj.get(field)
    if not isinstance(value, str):
        raise ValidationError(f"{field} is missing or not a string")
    try:
        uuid.UUID(value)
    except ValueError:
        raise ValidationError(f"{field} is not a valid UUID: {value!r}") from None
    return value


def validate_event(event: Any) -> dict[str, Any]:
    """Validate the envelope + business rules.

    Returns a normalized dict used downstream (applier, resolver). Raises
    ValidationError / BusinessRuleError — both permanent, both → DLQ.
    """
    if not isinstance(event, dict):
        raise ValidationError("event is not a JSON object")

    event_id = _require_uuid(event, "event_id")
    aggregate_id = _require_uuid(event, "aggregate_id")

    event_type = event.get("event_type")
    if not isinstance(event_type, str) or not event_type.strip():
        raise ValidationError("event_type is missing or not a string")

    # Default 1 = legacy envelopes without the field; any other version means
    # contract drift and must not be processed blindly.
    version = event.get("event_version", 1)
    if version != 1:
        raise ValidationError(f"unsupported event_version: {version!r}")

    data = event.get("data")
    if not isinstance(data, dict):
        raise ValidationError("data is missing or not an object")

    order = data.get("order")
    if not isinstance(order, dict):
        raise ValidationError("data.order is missing or not an object")

    trigger = data.get("payment_trigger")
    if not isinstance(trigger, dict):
        raise ValidationError("data.payment_trigger is missing or not an object")

    action = trigger.get("action")
    if action != "INITIATE_PAYMENT":
        raise ValidationError(f"unsupported payment_trigger.action: {action!r}")

    amount = trigger.get("amount")
    # JSON `true` decodes to Python bool, which isinstance(int) would accept —
    # exclude it explicitly before the numeric check.
    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        raise BusinessRuleError(f"payment_trigger.amount must be a number, got {amount!r}")
    if amount <= 0:
        raise BusinessRuleError(f"payment_trigger.amount must be positive, got {amount}")

    currency = trigger.get("currency") or order.get("currency") or "INR"
    if not isinstance(currency, str) or not re.fullmatch(r"[A-Z]{3}", currency):
        raise BusinessRuleError(f"currency must be a 3-letter uppercase code, got {currency!r}")

    order_id = order.get("order_id") or aggregate_id

    return {
        "event_id": str(event_id),
        "aggregate_id": str(aggregate_id),
        "aggregate_type": str(event.get("aggregate_type") or "ORDER"),
        "event_type": event_type.strip(),
        "occurred_at": event.get("occurred_at"),
        "amount": amount,
        "currency": currency,
        "order_id": str(order_id),
    }
