package com.zero.consumer.controller;

import com.zero.consumer.exception.BusinessRuleException;
import com.zero.consumer.exception.ValidationException;
import com.zero.consumer.repository.DatabaseClient;
import com.zero.consumer.service.EventValidator;
import com.zero.consumer.service.FailureResolutionService;
import com.zero.consumer.service.Metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Spring MVC dashboard: metrics overview + DLQ/blocked-aggregate operations.
 * Routes (same paths and JSON shapes as the old Javalin dashboard):
 * GET / — static UI (src/main/resources/static/index.html)
 * GET /api/metrics
 * GET /api/failures
 * GET /api/blocked
 * GET /api/parked
 * GET /api/payments
 * GET /api/ledger
 * POST /api/failures/{id}/resolve
 * POST /api/failures/{id}/discard
 * GET /healthz
 * GET /readyz
 */
@RestController
public class DashboardController {

    private static final Logger log = LoggerFactory.getLogger(DashboardController.class);
    private static final String AGGREGATE_TYPE = "ORDER";

    private final Metrics metrics;
    private final DatabaseClient db;
    private final FailureResolutionService resolutionService;

    public DashboardController(
            Metrics metrics, DatabaseClient db, FailureResolutionService resolutionService) {
        this.metrics = metrics;
        this.db = db;
        this.resolutionService = resolutionService;
    }

    // --- Read APIs ---
    @GetMapping("/api/metrics")
    public ResponseEntity<?> metrics() {
        try {
            long consumed = metrics.getConsumed();
            long appliedCount = metrics.getApplied();
            double successRate = consumed == 0 ? 1.0 : (double) appliedCount / consumed;

            Map<String, Object> counters = new LinkedHashMap<>();
            counters.put("consumed", consumed);
            counters.put("applied", appliedCount);
            counters.put("duplicate", metrics.getDuplicate());
            counters.put("parked", metrics.getParked());
            counters.put("failed", metrics.getFailed());

            Map<String, Object> snapshot = new LinkedHashMap<>();
            snapshot.put("started_at", Instant.ofEpochMilli(
                    metrics.getStartedAtMs()).toString());
            snapshot.put("uptime_seconds", metrics.getUptimeSeconds());
            snapshot.put("counters", counters);
            snapshot.put("events_consumed_total", consumed);
            snapshot.put("failures_by_class", metrics.getFailuresByClass());
            snapshot.put("success_rate", Math.round(successRate * 10000.0) / 10000.0);
            snapshot.put("blocked_aggregates", db.blockedCount());
            snapshot.put("parked_events", db.parkedCount());
            snapshot.put("open_failures", db.openFailureCount());
            snapshot.put("outbox_by_status", db.outboxStatusCounts());
            snapshot.put("payments_by_status", db.paymentStatusCounts());
            snapshot.put("consumer_lag", Map.of("note", "live consumer not attached to dashboard"));
            return ResponseEntity.ok(snapshot);
        } catch (Exception e) {
            log.error("metrics failed", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/api/failures")
    public ResponseEntity<?> failures(
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "100") int limit) {
        if ("all".equalsIgnoreCase(status))
            status = null;
        try {
            return ResponseEntity.ok(Map.of("failures", db.listFailures(status, limit)));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/api/blocked")
    public ResponseEntity<?> blocked() {
        try {
            return ResponseEntity.ok(Map.of("blocked", db.listBlocked()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/api/parked")
    public ResponseEntity<?> parked(
            @RequestParam(required = false) String aggregate_id,
            @RequestParam(defaultValue = "100") int limit) {

        try {
            if (aggregate_id != null && !aggregate_id.isBlank()) {
                return ResponseEntity.ok(
                        Map.of(
                                "parked",
                                db.listParked(
                                        AGGREGATE_TYPE,
                                        aggregate_id)));
            }
            return ResponseEntity.ok(
                    Map.of(
                            "parked",
                            db.listParkedRecent(limit)));
        } catch (Exception e) {
            return ResponseEntity.status(
                    HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(
                            Map.of(
                                    "error",
                                    e.getMessage()));
        }
    }

    @GetMapping("/api/payments")
    public ResponseEntity<?> payments(@RequestParam(defaultValue = "20") int limit) {

        try {

            return ResponseEntity.ok(
                    Map.of(
                            "payments",
                            db.listRecentPayments(limit)));
        } catch (Exception e) {

            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(
                            Map.of(
                                    "error",
                                    e.getMessage()));
        }
    }

    @GetMapping("/api/ledger")
    public ResponseEntity<?> ledger(@RequestParam(defaultValue = "20") int limit) {
        try {
            return ResponseEntity.ok(Map.of("ledger", db.listRecentProcessed(limit)));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    // --- Write APIs ---
    @PostMapping("/api/failures/{failureId}/resolve")
    public ResponseEntity<?> resolve(
            @PathVariable("failureId") String failureId,
            @RequestBody(required = false) Map<String, Object> body) {
        try {
            Map<String, Object> payload = body != null ? body : Map.of();
            Object fixedEventObj = payload.get("event");
            String resolvedBy = payload.getOrDefault("resolved_by", "operator").toString();
            String note = payload.get("note") != null ? payload.get("note").toString() : null;

            if (!(fixedEventObj instanceof Map)) {
                return ResponseEntity.badRequest()
                        .body(Map.of("detail", "a fixed event payload is required for resolve"));
            }
            Map<String, Object> fixedEvent = (Map<String, Object>) fixedEventObj;

            // Validate the fixed event first
            try {
                EventValidator.validate(fixedEvent);
            } catch (ValidationException | BusinessRuleException e) {
                return ResponseEntity.badRequest()
                        .body(Map.of("detail", "fixed event is invalid: " + e.getMessage()));
            }

            Map<String, Object> result = resolutionService.resolveFailure(
                    failureId, fixedEvent, false, false, resolvedBy, note);
            return ResponseEntity.ok(result);
        } catch (FailureResolutionService.ResolutionException e) {
            return ResponseEntity.badRequest().body(Map.of("detail", e.getMessage()));
        } catch (Exception e) {
            log.error("resolve failed for {}", failureId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(
                            Map.of(
                                    "detail",
                                    "resolution aborted, nothing changed: " + e.getMessage()));
        }
    }

    @PostMapping("/api/failures/{failureId}/discard")
    public ResponseEntity<?> discard(
            @PathVariable("failureId") String failureId,
            @RequestBody(required = false) Map<String, Object> body) {
        try {
            Map<String, Object> payload = body != null ? body : Map.of();
            boolean includeFollowers = Boolean.TRUE.equals(payload.get("include_followers"));
            String resolvedBy = payload.getOrDefault("resolved_by", "operator").toString();
            String note = payload.get("note") != null ? payload.get("note").toString() : null;

            Map<String, Object> result = resolutionService.resolveFailure(
                    failureId, null, true, includeFollowers, resolvedBy, note);
            return ResponseEntity.ok(result);
        } catch (FailureResolutionService.ResolutionException e) {
            return ResponseEntity.badRequest().body(Map.of("detail", e.getMessage()));
        } catch (Exception e) {
            log.error("discard failed for {}", failureId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(
                            Map.of(
                                    "detail",
                                    "resolution aborted, nothing changed: " + e.getMessage()));
        }
    }

    // --- Health ---
    @GetMapping("/healthz")
    public Map<String, Object> healthz() {
        return Map.of("status", "ok");
    }

    @GetMapping("/readyz")
    public ResponseEntity<?> readyz() {
        try {
            db.ping();
            return ResponseEntity.ok(Map.of("status", "ready"));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("detail", "postgres unreachable: " + e.getMessage()));
        }
    }
}
