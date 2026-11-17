package com.zero.consumer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory counters + live DB gauges for the dashboard.
 * Counters are process-local — they reset on restart.
 * Mirrors Python consumer/metrics.py Metrics class.
 */
public class Metrics {

    private final long startedAt = System.currentTimeMillis();

    // Process-local counters
    private final AtomicLong consumed = new AtomicLong(0);
    private final AtomicLong applied = new AtomicLong(0);
    private final AtomicLong duplicate = new AtomicLong(0);
    private final AtomicLong parked = new AtomicLong(0);
    private final AtomicLong failed = new AtomicLong(0);

    // Failure class breakdown
    private final ConcurrentHashMap<String, AtomicLong> failuresByClass = new ConcurrentHashMap<>();

    public void recordOutcome(String outcome) {
        consumed.incrementAndGet();
        switch (outcome) {
            case "applied" -> applied.incrementAndGet();
            case "duplicate" -> duplicate.incrementAndGet();
            case "parked" -> parked.incrementAndGet();
            case "failed" -> failed.incrementAndGet();
        }
    }

    public void recordFailureClass(String failureClass) {
        failuresByClass.computeIfAbsent(failureClass, k -> new AtomicLong(0)).incrementAndGet();
    }

    public long getConsumed() { return consumed.get(); }
    public long getApplied() { return applied.get(); }
    public long getDuplicate() { return duplicate.get(); }
    public long getParked() { return parked.get(); }
    public long getFailed() { return failed.get(); }
    public long getStartedAtMs() { return startedAt; }

    public java.util.Map<String, Long> getFailuresByClass() {
        java.util.Map<String, Long> result = new java.util.LinkedHashMap<>();
        failuresByClass.forEach((k, v) -> result.put(k, v.get()));
        return result;
    }

    public double getSuccessRate() {
        long c = consumed.get();
        return c == 0 ? 1.0 : (double) applied.get() / c;
    }

    public long getUptimeSeconds() {
        return (System.currentTimeMillis() - startedAt) / 1000;
    }
}
