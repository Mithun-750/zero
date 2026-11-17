package com.zero.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds the outgoing event envelope from a raw outbox row.
 *
 * <p>Supports two payload shapes written by the DB trigger:
 * <ul>
 *   <li><b>Flat</b>: {@code { "order_id": ..., "payment_trigger": {...}, ... }}</li>
 *   <li><b>Nested</b>: {@code { "order": { "order_id": ..., ... }, "payment_trigger": {...} }}</li>
 * </ul>
 *
 * <p>The resulting {@code data} block is always:
 * <pre>
 * {
 *   "order":           { all order fields except payment_trigger },
 *   "payment_trigger": { ... }   // omitted when absent
 * }
 * </pre>
 */
public final class EventBuilder {

    private static final Logger log = LoggerFactory.getLogger(EventBuilder.class);

    private EventBuilder() {}

    /**
     * Build the event envelope Map from a raw outbox row.
     *
     * @param row Map produced by {@link DatabaseClient#fetchPendingEvents}
     * @return event envelope ready for JSON serialisation and Kafka publishing
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> buildEvent(Map<String, Object> row) {
        String id            = (String) row.get("id");
        String aggregateType = (String) row.get("aggregate_type");
        String aggregateId   = (String) row.get("aggregate_id");
        String eventType     = (String) row.get("event_type");
        Timestamp createdAt  = (Timestamp) row.get("created_at");
        Map<String, Object> payload = (Map<String, Object>) row.get("payload");

        // ── Normalise payload ─────────────────────────────────────────────────
        Map<String, Object> orderFields;
        Map<String, Object> paymentTrigger = null;

        if (payload.containsKey("order")) {
            // Nested shape: { "order": {...}, "payment_trigger": {...} }
            orderFields    = new LinkedHashMap<>((Map<String, Object>) payload.get("order"));
            paymentTrigger = (Map<String, Object>) payload.get("payment_trigger");
            if (paymentTrigger == null && orderFields.containsKey("payment_trigger")) {
                paymentTrigger = (Map<String, Object>) orderFields.remove("payment_trigger");
            }
        } else {
            // Flat shape: all fields at the top level
            paymentTrigger = (Map<String, Object>) payload.get("payment_trigger");

            // Copy all fields except payment_trigger into the order map
            orderFields = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : payload.entrySet()) {
                if (!"payment_trigger".equals(entry.getKey())) {
                    orderFields.put(entry.getKey(), entry.getValue());
                }
            }
        }

        // ── Build data block ──────────────────────────────────────────────────
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("order", orderFields);
        if (paymentTrigger != null) {
            data.put("payment_trigger", paymentTrigger);
        }

        Object occurredAtObj = payload.get("timestamp");
        String occurredAt = (occurredAtObj != null) ? occurredAtObj.toString() :
                (createdAt != null ? createdAt.toInstant().toString() : java.time.Instant.now().toString());

        // ── Assemble envelope ─────────────────────────────────────────────────
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("event_id",      id);
        envelope.put("event_type",    eventType);
        envelope.put("event_version", 1);
        envelope.put("occurred_at",   occurredAt);
        envelope.put("aggregate_type", aggregateType != null ? aggregateType : "ORDER");
        envelope.put("aggregate_id",  aggregateId);
        envelope.put("source",        "order-service");
        envelope.put("data",          data);

        return envelope;
    }
}
