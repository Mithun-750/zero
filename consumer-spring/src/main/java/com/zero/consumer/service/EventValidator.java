package com.zero.consumer.service;

import java.util.Map;
import java.util.regex.Pattern;

import com.zero.consumer.exception.BusinessRuleException;
import com.zero.consumer.exception.ValidationException;
/**
 * Validates the event envelope + business rules.
 * Mirrors Python consumer/classify.py validate_event().
 * Returns a NormalizedEvent record on success.
 * Throws ValidationException or BusinessRuleException (both permanent → DLQ).
 */
public class EventValidator {

    private static final Pattern CURRENCY_PATTERN = Pattern.compile("[A-Z]{3}");
    // Simple UUID regex for string validation
    private static final Pattern UUID_PATTERN = Pattern.compile(
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    /**
     * Normalized event returned from validation.
     */
    public record NormalizedEvent(
            String eventId,
            String aggregateId,
            String aggregateType,
            String eventType,
            String occurredAt,
            double amount,
            String currency,
            String orderId
    ) {}

    /**
     * Validate the envelope + business rules.
     * Throws ValidationException or BusinessRuleException on failure.
     */
    @SuppressWarnings("unchecked")
    public static NormalizedEvent validate(Object event) {
        if (!(event instanceof Map)) {
            throw new ValidationException("event is not a JSON object");
        }
        Map<String, Object> ev = (Map<String, Object>) event;

        String eventId = requireUuid(ev, "event_id");
        String aggregateId = requireUuid(ev, "aggregate_id");

        Object eventTypeObj = ev.get("event_type");
        if (!(eventTypeObj instanceof String) || ((String) eventTypeObj).isBlank()) {
            throw new ValidationException("event_type is missing or not a string");
        }
        String eventType = ((String) eventTypeObj).strip();

        // Default version = 1 (legacy envelopes without the field)
        Object versionObj = ev.getOrDefault("event_version", 1);
        int version;
        if (versionObj instanceof Integer i) {
            version = i;
        } else if (versionObj instanceof Long l) {
            version = l.intValue();
        } else if (versionObj instanceof Number n) {
            version = n.intValue();
        } else {
            throw new ValidationException("unsupported event_version: " + versionObj);
        }
        if (version != 1) {
            throw new ValidationException("unsupported event_version: " + versionObj);
        }

        Object dataObj = ev.get("data");
        if (!(dataObj instanceof Map)) {
            throw new ValidationException("data is missing or not an object");
        }
        Map<String, Object> data = (Map<String, Object>) dataObj;

        Object orderObj = data.get("order");
        if (!(orderObj instanceof Map)) {
            throw new ValidationException("data.order is missing or not an object");
        }
        Map<String, Object> order = (Map<String, Object>) orderObj;

        Object triggerObj = data.get("payment_trigger");
        if (!(triggerObj instanceof Map)) {
            throw new ValidationException("data.payment_trigger is missing or not an object");
        }
        Map<String, Object> trigger = (Map<String, Object>) triggerObj;

        Object actionObj = trigger.get("action");
        if (!"INITIATE_PAYMENT".equals(actionObj)) {
            throw new ValidationException("unsupported payment_trigger.action: " + actionObj);
        }

        Object amountObj = trigger.get("amount");
        // JSON `true` / `false` should be rejected; check for Boolean explicitly
        if (amountObj instanceof Boolean) {
            throw new BusinessRuleException("payment_trigger.amount must be a number, got " + amountObj);
        }
        if (!(amountObj instanceof Number)) {
            throw new BusinessRuleException("payment_trigger.amount must be a number, got " + amountObj);
        }
        double amount = ((Number) amountObj).doubleValue();
        if (amount <= 0) {
            throw new BusinessRuleException("payment_trigger.amount must be positive, got " + amount);
        }

        // Currency: payment_trigger.currency or order.currency or "INR"
        String currency = coalesce(
                trigger.get("currency"),
                order.get("currency"),
                "INR"
        );
        if (!CURRENCY_PATTERN.matcher(currency).matches()) {
            throw new BusinessRuleException("currency must be a 3-letter uppercase code, got " + currency);
        }

        // order_id: order.order_id or fallback to aggregate_id
        String orderId;
        Object orderIdObj = order.get("order_id");
        if (orderIdObj != null && !orderIdObj.toString().isBlank()) {
            orderId = orderIdObj.toString();
        } else {
            orderId = aggregateId;
        }

        Object aggregateTypeObj = ev.get("aggregate_type");
        String aggregateType = (aggregateTypeObj != null && !aggregateTypeObj.toString().isBlank())
                ? aggregateTypeObj.toString() : "ORDER";

        Object occurredAtObj = ev.get("occurred_at");
        String occurredAt = occurredAtObj != null ? occurredAtObj.toString() : null;

        return new NormalizedEvent(eventId, aggregateId, aggregateType, eventType,
                occurredAt, amount, currency, orderId);
    }

    private static String requireUuid(Map<String, Object> obj, String field) {
        Object value = obj.get(field);
        if (!(value instanceof String s)) {
            throw new ValidationException(field + " is missing or not a string");
        }
        if (!UUID_PATTERN.matcher(s).matches()) {
            throw new ValidationException(field + " is not a valid UUID: " + s);
        }
        return s;
    }

    private static String coalesce(Object... candidates) {
        for (Object c : candidates) {
            if (c instanceof String s && !s.isBlank()) {
                return s;
            }
        }
        return "INR"; // fallback (last candidate should always be "INR")
    }
}
