"""Validator unit tests — pure functions, no DB or broker."""
from __future__ import annotations

import unittest

from consumer.classify import (
    BusinessRuleError,
    ValidationError,
    validate_event,
)

VALID = {
    "event_id": "11111111-1111-1111-1111-111111111111",
    "event_type": "OrderCreated",
    "event_version": 1,
    "aggregate_type": "ORDER",
    "aggregate_id": "22222222-2222-2222-2222-222222222222",
    "occurred_at": "2026-11-16T00:00:00+00:00",
    "data": {
        "order": {"order_id": "22222222-2222-2222-2222-222222222222", "total_amount": 12.5},
        "payment_trigger": {
            "action": "INITIATE_PAYMENT",
            "amount": 12.5,
            "currency": "INR",
            "target_account": "CUST-1",
        },
    },
}


def _variant(mutator):
    import copy

    event = copy.deepcopy(VALID)
    mutator(event)
    return event


class ValidateEventTests(unittest.TestCase):
    def test_valid_event_normalizes(self) -> None:
        norm = validate_event(VALID)
        self.assertEqual(norm["event_id"], VALID["event_id"])
        self.assertEqual(norm["aggregate_id"], VALID["aggregate_id"])
        self.assertEqual(norm["amount"], 12.5)
        self.assertEqual(norm["currency"], "INR")
        self.assertEqual(norm["order_id"], VALID["aggregate_id"])

    def test_currency_falls_back_to_order(self) -> None:
        event = _variant(lambda e: e["data"]["payment_trigger"].pop("currency"))
        event["data"]["order"]["currency"] = "INR"
        self.assertEqual(validate_event(event)["currency"], "INR")

    def test_rejects_non_uuid_event_id(self) -> None:
        event = _variant(lambda e: e.update(event_id="not-a-uuid"))
        with self.assertRaises(ValidationError):
            validate_event(event)

    def test_rejects_missing_payment_trigger(self) -> None:
        event = _variant(lambda e: e["data"].pop("payment_trigger"))
        with self.assertRaises(ValidationError):
            validate_event(event)

    def test_rejects_missing_order(self) -> None:
        event = _variant(lambda e: e["data"].pop("order"))
        with self.assertRaises(ValidationError):
            validate_event(event)

    def test_rejects_non_positive_amount(self) -> None:
        event = _variant(lambda e: e["data"]["payment_trigger"].update(amount=0))
        with self.assertRaises(BusinessRuleError):
            validate_event(event)

    def test_rejects_bool_amount(self) -> None:
        event = _variant(lambda e: e["data"]["payment_trigger"].update(amount=True))
        with self.assertRaises(BusinessRuleError):
            validate_event(event)

    def test_rejects_unknown_event_version(self) -> None:
        event = _variant(lambda e: e.update(event_version=99))
        with self.assertRaises(ValidationError):
            validate_event(event)

    def test_rejects_lowercase_currency(self) -> None:
        event = _variant(lambda e: e["data"]["payment_trigger"].update(currency="inr"))
        with self.assertRaises(BusinessRuleError):
            validate_event(event)

    def test_rejects_wrong_trigger_action(self) -> None:
        event = _variant(lambda e: e["data"]["payment_trigger"].update(action="REFUND"))
        with self.assertRaises(ValidationError):
            validate_event(event)

    def test_failure_classes_match_dashboard(self) -> None:
        self.assertEqual(ValidationError.failure_class, "VALIDATION")
        self.assertEqual(BusinessRuleError.failure_class, "BUSINESS_RULE")


if __name__ == "__main__":
    unittest.main()
