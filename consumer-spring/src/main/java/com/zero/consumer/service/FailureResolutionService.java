package com.zero.consumer.service;

import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.zero.consumer.exception.BusinessRuleException;
import com.zero.consumer.exception.ValidationException;
import com.zero.consumer.repository.DatabaseClient;
/**
 * Dashboard write operations: resolve (apply fixed event) or discard a DLQ'd
 * failure, then drain parked followers (replay or discard), unblock the
 * aggregate and mark the failure resolved — all in ONE transaction.
 * Rolls back automatically on any RuntimeException.
 */
@Service
public class FailureResolutionService {

    private static final Logger log = LoggerFactory.getLogger(FailureResolutionService.class);
    private static final String AGGREGATE_TYPE = "ORDER";

    private final DatabaseClient db;
    private final ObjectMapper mapper = new ObjectMapper();

    public FailureResolutionService(DatabaseClient db) {
        this.db = db;
    }

    /** Resolution failures that map to HTTP 400. */
    public static class ResolutionException extends RuntimeException {
        public ResolutionException(String msg) { super(msg); }
    }

    /**
     * @param fixedEvent       required when discard=false (validated); ignored for discard
     * @param discard           true to discard the poison event instead of applying the fix
     * @param includeFollowers true (discard only) to also mark parked followers DISCARDED
     */
    @Transactional
    @SuppressWarnings("unchecked")
    public Map<String, Object> resolveFailure(
            String failureId,
            Map<String, Object> fixedEvent,
            boolean discard,
            boolean includeFollowers,
            String resolvedBy,
            String note
    ) {
        Map<String, Object> failure = db.getFailure(failureId, true);
        if (failure == null) throw new ResolutionException("failure " + failureId + " not found");
        if (!"OPEN".equals(failure.get("status"))) {
            throw new ResolutionException("failure " + failureId + " is already " + failure.get("status"));
        }

        Object aggIdObj = failure.get("aggregate_id");
        String aggregateId = aggIdObj != null ? aggIdObj.toString() : null;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("failure_id", failureId);
        result.put("resolution", discard ? "DISCARDED" : "RESOLVED");
        result.put("applied_fix", false);
        result.put("parked_replayed", 0);
        result.put("parked_discarded", 0);
        result.put("unblocked", false);

        if (aggregateId != null) {
            if (!discard) {
                String outcome = db.applyEvent(fixedEvent);
                result.put("applied_fix", "applied".equals(outcome));
                log.info("Fixed event applied (outcome={}) for aggregate={}", outcome, aggregateId);
            } else {
                log.info("Discarding event {} for aggregate={}", failure.get("event_id"), aggregateId);
            }

            // Drain parked followers in original arrival order
            List<Map<String, Object>> parkedList = db.listParked(AGGREGATE_TYPE, aggregateId);
            int replayed = 0, discarded = 0;
            for (Map<String, Object> parked : parkedList) {
                if (discard && includeFollowers) {
                    db.markParked(parked.get("id"), "DISCARDED");
                    discarded++;
                } else {
                    // parked.event is JSONB — it comes back as a Map or PGobject
                    Object eventObj = parked.get("event");
                    Map<String, Object> parkedEvent;
                    if (eventObj instanceof Map) {
                        parkedEvent = (Map<String, Object>) eventObj;
                    } else if (eventObj instanceof String s) {
                        parkedEvent = mapper.readValue(s, Map.class);
                    } else if (eventObj instanceof org.postgresql.util.PGobject pgo) {
                        parkedEvent = mapper.readValue(pgo.getValue(), Map.class);
                    } else {
                        throw new IllegalStateException("Cannot deserialize parked event: " + eventObj);
                    }
                    try {
                        db.applyEvent(parkedEvent);
                        db.markParked(parked.get("id"), "REPLAYED");
                        replayed++;
                    } catch (ValidationException | BusinessRuleException ex) {
                        throw new ResolutionException("Follower event " + parked.get("event_id")
                                + " failed replay: " + ex.getMessage()
                                + ". Discard with followers or address corrupt follower first.");
                    }
                }
            }
            result.put("parked_replayed", replayed);
            result.put("parked_discarded", discarded);

            db.unblockAggregate(AGGREGATE_TYPE, aggregateId,
                    discard ? "DISCARDED" : "REPLAYED", resolvedBy, note);
            result.put("unblocked", true);
        } else {
            log.info("Resolving unidentifiable poison event {} (no aggregate to unblock)", failureId);
        }

        db.updateFailure(failureId,
                discard ? "DISCARDED" : "RESOLVED",
                resolvedBy,
                note != null ? note : (discard ? "discarded" : "replayed fixed event + followers"));

        return result;
    }
}
