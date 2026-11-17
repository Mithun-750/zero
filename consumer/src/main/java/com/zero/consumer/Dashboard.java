package com.zero.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

/**
 * Javalin REST dashboard: metrics overview + DLQ/blocked-aggregate operations.
 * Mirrors Python consumer/dashboard.py create_app().
 *
 * Routes:
 *   GET /            — embedded HTML UI
 *   GET /api/metrics
 *   GET /api/failures
 *   GET /api/blocked
 *   GET /api/parked
 *   GET /api/payments
 *   GET /api/ledger
 *   POST /api/failures/{id}/resolve
 *   POST /api/failures/{id}/discard
 *   GET /healthz
 *   GET /readyz
 */
public class Dashboard {

    private static final Logger log = LoggerFactory.getLogger(Dashboard.class);
    private static final String AGGREGATE_TYPE = "ORDER";

    private final Settings settings;
    private final Metrics metrics;
    private final ObjectMapper mapper = new ObjectMapper();
    private Javalin app;

    public Dashboard(Settings settings, Metrics metrics) {
        this.settings = settings;
        this.metrics = metrics;
    }

    public void start() {
        app = Javalin.create(config -> {
            config.showJavalinBanner = false;
        });

        app.get("/", ctx -> ctx.html(PAGE));

        app.get("/api/metrics", ctx -> {
            try (DatabaseClient db = new DatabaseClient(settings)) {
                Map<String, Object> snapshot = buildSnapshot(db);
                ctx.json(snapshot);
            } catch (Exception e) {
                ctx.status(500).json(Map.of("error", e.getMessage()));
            }
        });

        app.get("/api/failures", ctx -> {
            String status = ctx.queryParam("status");
            String limitStr = ctx.queryParam("limit");
            int limit = limitStr != null ? Integer.parseInt(limitStr) : 100;
            if ("all".equalsIgnoreCase(status)) status = null;
            try (DatabaseClient db = new DatabaseClient(settings)) {
                ctx.json(Map.of("failures", db.listFailures(status, limit)));
            } catch (Exception e) {
                ctx.status(500).json(Map.of("error", e.getMessage()));
            }
        });

        app.get("/api/blocked", ctx -> {
            try (DatabaseClient db = new DatabaseClient(settings)) {
                ctx.json(Map.of("blocked", db.listBlocked()));
            } catch (Exception e) {
                ctx.status(500).json(Map.of("error", e.getMessage()));
            }
        });

        app.get("/api/parked", ctx -> {
            String aggregateId = ctx.queryParam("aggregate_id");
            String limitStr = ctx.queryParam("limit");
            int limit = limitStr != null ? Integer.parseInt(limitStr) : 100;
            try (DatabaseClient db = new DatabaseClient(settings)) {
                if (aggregateId != null && !aggregateId.isBlank()) {
                    ctx.json(Map.of("parked", db.listParked(AGGREGATE_TYPE, aggregateId)));
                } else {
                    ctx.json(Map.of("parked", db.listParkedRecent(limit)));
                }
            } catch (Exception e) {
                ctx.status(500).json(Map.of("error", e.getMessage()));
            }
        });

        app.get("/api/payments", ctx -> {
            String limitStr = ctx.queryParam("limit");
            int limit = limitStr != null ? Integer.parseInt(limitStr) : 20;
            try (DatabaseClient db = new DatabaseClient(settings)) {
                ctx.json(Map.of("payments", db.listRecentPayments(limit)));
            } catch (Exception e) {
                ctx.status(500).json(Map.of("error", e.getMessage()));
            }
        });

        app.get("/api/ledger", ctx -> {
            String limitStr = ctx.queryParam("limit");
            int limit = limitStr != null ? Integer.parseInt(limitStr) : 20;
            try (DatabaseClient db = new DatabaseClient(settings)) {
                ctx.json(Map.of("ledger", db.listRecentProcessed(limit)));
            } catch (Exception e) {
                ctx.status(500).json(Map.of("error", e.getMessage()));
            }
        });

        // --- Write APIs ---

        app.post("/api/failures/{failure_id}/resolve", ctx -> {
            String failureId = ctx.pathParam("failure_id");
            try {
                Map<String, Object> body = mapper.readValue(ctx.body(), Map.class);
                Object fixedEvent = body.get("event");
                String resolvedBy = body.getOrDefault("resolved_by", "operator").toString();
                String note = body.get("note") != null ? body.get("note").toString() : null;

                if (!(fixedEvent instanceof Map)) {
                    ctx.status(400).json(Map.of("detail", "a fixed event payload is required for resolve"));
                    return;
                }
                // Validate the fixed event first
                try {
                    EventValidator.validate(fixedEvent);
                } catch (ValidationException | BusinessRuleException e) {
                    ctx.status(400).json(Map.of("detail", "fixed event is invalid: " + e.getMessage()));
                    return;
                }

                Map<String, Object> result = resolveFailure(failureId, (Map<String, Object>) fixedEvent,
                        false, false, resolvedBy, note);
                ctx.json(result);
            } catch (ResolutionException e) {
                ctx.status(400).json(Map.of("detail", e.getMessage()));
            } catch (Exception e) {
                log.error("resolve failed for {}", failureId, e);
                ctx.status(500).json(Map.of("detail", "resolution aborted, nothing changed: " + e.getMessage()));
            }
        });

        app.post("/api/failures/{failure_id}/discard", ctx -> {
            String failureId = ctx.pathParam("failure_id");
            try {
                Map<String, Object> body = mapper.readValue(ctx.body(), Map.class);
                boolean includeFollowers = Boolean.TRUE.equals(body.get("include_followers"));
                String resolvedBy = body.getOrDefault("resolved_by", "operator").toString();
                String note = body.get("note") != null ? body.get("note").toString() : null;

                Map<String, Object> result = resolveFailure(failureId, null, true, includeFollowers, resolvedBy, note);
                ctx.json(result);
            } catch (ResolutionException e) {
                ctx.status(400).json(Map.of("detail", e.getMessage()));
            } catch (Exception e) {
                log.error("discard failed for {}", failureId, e);
                ctx.status(500).json(Map.of("detail", "resolution aborted, nothing changed: " + e.getMessage()));
            }
        });

        // --- Health ---

        app.get("/healthz", ctx -> ctx.json(Map.of("status", "ok")));

        app.get("/readyz", ctx -> {
            try (DatabaseClient db = new DatabaseClient(settings)) {
                db.ping();
                ctx.json(Map.of("status", "ready"));
            } catch (Exception e) {
                ctx.status(503).json(Map.of("detail", "postgres unreachable: " + e.getMessage()));
            }
        });

        app.start(settings.dashboardHost, settings.dashboardPort);
        log.info("Dashboard listening on http://{}:{}", settings.dashboardHost, settings.dashboardPort);
    }

    public void stop() {
        if (app != null) app.stop();
    }

    private Map<String, Object> buildSnapshot(DatabaseClient db) throws Exception {
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
        snapshot.put("started_at", Instant.ofEpochMilli(metrics.getStartedAtMs()).toString());
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
        return snapshot;
    }

    // -------------------- resolve / discard --------------------

    private static class ResolutionException extends RuntimeException {
        ResolutionException(String msg) { super(msg); }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> resolveFailure(
            String failureId,
            Map<String, Object> fixedEvent,
            boolean discard,
            boolean includeFollowers,
            String resolvedBy,
            String note
    ) throws Exception {
        try (DatabaseClient db = new DatabaseClient(settings)) {
            db.beginTransaction();
            try {
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
                            // parked.event is JSONB — it comes back as a Map
                            Object eventObj = parked.get("event");
                            Map<String, Object> parkedEvent;
                            if (eventObj instanceof Map) {
                                parkedEvent = (Map<String, Object>) eventObj;
                            } else if (eventObj instanceof String s) {
                                parkedEvent = mapper.readValue(s, Map.class);
                            } else {
                                throw new RuntimeException("Cannot deserialize parked event: " + eventObj);
                            }
                            db.applyEvent(parkedEvent);
                            db.markParked(parked.get("id"), "REPLAYED");
                            replayed++;
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

                db.commitTransaction();
                return result;
            } catch (ResolutionException e) {
                db.rollbackTransaction();
                throw e;
            } catch (Exception e) {
                db.rollbackTransaction();
                throw e;
            }
        }
    }

    // -------------------- embedded HTML page (from Python dashboard.py _PAGE) --------------------

    private static final String PAGE = """
            <!DOCTYPE html>
            <html lang="en">
            <head>
            <meta charset="utf-8">
            <title>zero · payment consumer</title>
            <style>
              :root { color-scheme: dark; }
              * { box-sizing: border-box; }
              body { margin: 0; font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
                     background: #0d1117; color: #c9d1d9; font-size: 13px; }
              header { display: flex; align-items: baseline; gap: 16px;
                       padding: 14px 20px; border-bottom: 1px solid #30363d; }
              header h1 { font-size: 15px; margin: 0; color: #58a6ff; }
              header span { color: #8b949e; font-size: 12px; }
              main { padding: 16px 20px; display: grid; gap: 14px; }
              .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 10px; }
              .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 10px 12px; }
              .card .label { color: #8b949e; font-size: 11px; text-transform: uppercase; }
              .card .value { font-size: 22px; color: #e6edf3; margin-top: 2px; }
              .card.bad .value, .card.bad { border-color: #f85149; color: #f85149; }
              .banner { display: none; background: #3d1214; border: 1px solid #f85149; color: #ffa3a1;
                        border-radius: 8px; padding: 10px 14px; }
              section { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px; }
              section h2 { font-size: 12px; text-transform: uppercase; color: #8b949e; margin: 0 0 8px;
                           display: flex; align-items: center; gap: 10px; }
              table { width: 100%; border-collapse: collapse; }
              th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid #21262d;
                       vertical-align: top; }
              th { color: #8b949e; font-size: 11px; text-transform: uppercase; }
              tr:last-child td { border-bottom: none; }
              .reason { max-width: 420px; overflow-wrap: anywhere; }
              button { background: #21262d; color: #c9d1d9; border: 1px solid #30363d;
                       border-radius: 6px; padding: 5px 10px; cursor: pointer; font: inherit; }
              button:hover { border-color: #58a6ff; color: #58a6ff; }
              button.danger:hover { border-color: #f85149; color: #f85149; }
              select { background: #21262d; color: #c9d1d9; border: 1px solid #30363d;
                       border-radius: 6px; padding: 4px 8px; font: inherit; }
              .modal { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.6);
                       align-items: center; justify-content: center; }
              .modal .box { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
                            padding: 16px; width: 640px; max-width: 92vw; }
              .modal textarea { width: 100%; height: 260px; background: #0d1117; color: #c9d1d9;
                                border: 1px solid #30363d; border-radius: 6px; padding: 8px;
                                font: inherit; }
              .modal .row { display: flex; gap: 8px; margin-top: 10px; justify-content: flex-end; }
              .modal label { color: #8b949e; font-size: 12px; }
              pre { background: #0d1117; border: 1px solid #21262d; border-radius: 6px;
                    padding: 8px; overflow: auto; max-height: 220px; }
              .muted { color: #8b949e; }
              .status { font-weight: 700; }
              .status.OPEN { color: #f85149; }
              .status.RESOLVED { color: #3fb950; }
              .status.DISCARDED { color: #d29922; }
            </style>
            </head>
            <body>
            <header>
              <h1>zero · payment-consumer</h1>
              <span id="uptime">uptime: —</span>
              <span id="lag">lag: —</span>
              <span id="refreshed"></span>
            </header>
            <main>
              <div class="banner" id="banner"></div>

              <div class="cards" id="cards"></div>

              <section>
                <h2>DLQ — failures <span class="muted" id="failureCount"></span>
                  <select id="failureFilter">
                    <option value="OPEN" selected>OPEN</option>
                    <option value="RESOLVED">RESOLVED</option>
                    <option value="DISCARDED">DISCARDED</option>
                    <option value="all">all</option>
                  </select>
                </h2>
                <table>
                  <thead><tr><th>failed_at</th><th>aggregate</th><th>class</th><th>reason</th><th>offset</th><th>status</th><th>actions / note</th></tr></thead>
                  <tbody id="failures"></tbody>
                </table>
              </section>

              <section>
                <h2>Blocked aggregates</h2>
                <table>
                  <thead><tr><th>aggregate_id</th><th>blocked_at</th><th>reason</th><th>blocked_event</th><th>parked</th></tr></thead>
                  <tbody id="blocked"></tbody>
                </table>
              </section>

              <section>
                <h2>Parked followers</h2>
                <table>
                  <thead><tr><th>parked_at</th><th>aggregate_id</th><th>event_id</th><th>kafka_offset</th></tr></thead>
                  <tbody id="parked"></tbody>
                </table>
              </section>

              <section>
                <h2>Applied payments (latest 20)</h2>
                <table>
                  <thead><tr><th>applied_at</th><th>order</th><th>customer</th><th>amount</th><th>status</th></tr></thead>
                  <tbody id="payments"></tbody>
                </table>
              </section>

              <section>
                <h2>Event ledger — processed_events (latest 20)</h2>
                <table>
                  <thead><tr><th>processed_at</th><th>event_id</th><th>aggregate_id</th><th>consumer</th></tr></thead>
                  <tbody id="ledger"></tbody>
                </table>
              </section>

              <section>
                <h2>Live snapshot</h2>
                <pre id="raw">loading…</pre>
              </section>
            </main>

            <div class="modal" id="resolveModal">
              <div class="box">
                <h3>Resolve failure — edit the event, then apply</h3>
                <div class="muted" id="resolveMeta"></div>
                <textarea id="resolvePayload" spellcheck="false"></textarea>
                <div class="row">
                  <button onclick="closeModal()">cancel</button>
                  <button onclick="submitResolve()">apply &amp; unblock</button>
                </div>
              </div>
            </div>

            <div class="modal" id="discardModal">
              <div class="box">
                <h3>Discard failure</h3>
                <p class="muted">The event will not be applied. Blocked aggregate will be unblocked.</p>
                <label><input type="checkbox" id="discardFollowers"> also discard parked followers (<span id="discardParkedCount">0</span>)</label>
                <div class="row">
                  <button onclick="closeModal()">cancel</button>
                  <button class="danger" onclick="submitDiscard()">discard &amp; unblock</button>
                </div>
              </div>
            </div>

            <script>
            let currentFailureId = null;

            async function jget(u) { const r = await fetch(u); if (!r.ok) throw new Error(u + ' -> ' + r.status); return r.json(); }

            function esc(s) { return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
            function ts(s) { return esc(String(s ?? '').replace('T', ' ').slice(0, 19)); }
            const money = (a, c) => `${esc(a)} ${esc(c)}`;

            async function refresh() {
              const filter = document.getElementById('failureFilter').value;
              const [m, f, b, p, pay, led] = await Promise.all([
                jget('/api/metrics'), jget('/api/failures?status=' + encodeURIComponent(filter) + '&limit=200'),
                jget('/api/blocked'), jget('/api/parked'),
                jget('/api/payments?limit=20'), jget('/api/ledger?limit=20'),
              ]);

              document.getElementById('uptime').textContent = 'uptime: ' + Math.floor(m.uptime_seconds / 60) + 'm';
              const lag = m.consumer_lag || {};
              document.getElementById('lag').textContent = 'lag: ' + (lag.total ?? (lag.note || lag.error || '—'));
              document.getElementById('refreshed').textContent = 'synced ' + new Date().toLocaleTimeString();

              const c = m.counters || {};
              const cards = [
                ['consumed', c.consumed ?? 0], ['applied', c.applied ?? 0], ['duplicate', c.duplicate ?? 0],
                ['parked', c.parked ?? 0], ['failed (DLQ)', c.failed ?? 0],
                ['success rate', (100 * (m.success_rate ?? 1)).toFixed(1) + '%'],
                ['blocked', m.blocked_aggregates], ['open failures', m.open_failures],
              ];
              document.getElementById('cards').innerHTML = cards.map(([l, v]) => {
                const bad = (l === 'blocked' || l === 'open failures') && v > 0;
                return `<div class="card ${bad ? 'bad' : ''}"><div class="label">${l}</div><div class="value">${v}</div></div>`;
              }).join('');

              const banner = document.getElementById('banner');
              if (m.blocked_aggregates > 0) {
                banner.style.display = 'block';
                banner.textContent = `⚠ ${m.blocked_aggregates} aggregate(s) blocked — follower events are parked until the DLQ failure is resolved.`;
              } else {
                banner.style.display = 'none';
              }

              const failures = f.failures || [];
              document.getElementById('failureCount').textContent = `(${failures.length})`;
              document.getElementById('failures').innerHTML = failures.map(x => `
                <tr>
                  <td>${ts(x.created_at)}</td>
                  <td>${esc(x.aggregate_id || '—')}</td>
                  <td>${esc(x.failure_class)}</td>
                  <td class="reason">${esc(x.failure_reason)}</td>
                  <td>${x.source_partition}:${x.source_offset}</td>
                  <td class="status ${esc(x.status)}">${esc(x.status)}</td>
                  <td>${x.status === 'OPEN'
                    ? `<button onclick='openResolve(${JSON.stringify(x.id)}, ${JSON.stringify(x.raw_payload)})'>resolve…</button>
                       <button class="danger" onclick='openDiscard(${JSON.stringify(x.id)})'>discard</button>`
                    : `<span class="muted">${esc(x.resolution_note || '')} — by ${esc(x.resolved_by || '?')}</span>`}</td>
                </tr>`).join('') || '<tr><td colspan="7" class="muted">none</td></tr>';

              document.getElementById('blocked').innerHTML = (b.blocked || []).map(x => `
                <tr>
                  <td>${esc(x.aggregate_id)}</td>
                  <td>${ts(x.blocked_at)}</td>
                  <td class="reason">${esc(x.blocked_reason || '')}</td>
                  <td>${esc(x.blocked_event_id || '—')}</td>
                  <td>${x.parked_count}</td>
                </tr>`).join('') || '<tr><td colspan="5" class="muted">no blocked aggregates</td></tr>';

              document.getElementById('parked').innerHTML = (p.parked || []).map(x => `
                <tr>
                  <td>${ts(x.parked_at)}</td>
                  <td>${esc(x.aggregate_id)}</td>
                  <td>${esc(x.event_id)}</td>
                  <td>${x.source_partition}:${x.kafka_offset}</td>
                </tr>`).join('') || '<tr><td colspan="4" class="muted">nothing parked</td></tr>';

              document.getElementById('payments').innerHTML = (pay.payments || []).map(x => `
                <tr>
                  <td>${ts(x.applied_at)}</td>
                  <td>${esc(x.order_number)}</td>
                  <td>${esc(x.customer_name)} <span class="muted">${esc(x.customer_email)}</span></td>
                  <td>${money(x.amount, x.currency)}</td>
                  <td>${esc(x.status)}</td>
                </tr>`).join('') || '<tr><td colspan="5" class="muted">no payments yet</td></tr>';

              document.getElementById('ledger').innerHTML = (led.ledger || []).map(x => `
                <tr>
                  <td>${ts(x.processed_at)}</td>
                  <td>${esc(x.event_id)}</td>
                  <td>${esc(x.aggregate_id)}</td>
                  <td>${esc(x.consumer_name)}</td>
                </tr>`).join('') || '<tr><td colspan="4" class="muted">no events processed yet</td></tr>';

              document.getElementById('raw').textContent = JSON.stringify(m, null, 2);
            }

            document.getElementById('failureFilter').addEventListener('change', refresh);

            function openResolve(id, rawPayload) {
              currentFailureId = id;
              document.getElementById('resolveMeta').textContent = 'failure id: ' + id;
              let pretty = rawPayload;
              try { pretty = JSON.stringify(JSON.parse(rawPayload), null, 2); } catch (_) {}
              document.getElementById('resolvePayload').value = pretty;
              document.getElementById('resolveModal').style.display = 'flex';
            }

            async function openDiscard(id) {
              currentFailureId = id;
              const r = await jget('/api/parked');
              const failure = (await jget('/api/failures?status=OPEN&limit=500')).failures.find(x => x.id === id);
              const aggId = failure && failure.aggregate_id;
              const n = (r.parked || []).filter(p => p.aggregate_id === aggId).length;
              document.getElementById('discardParkedCount').textContent = n;
              document.getElementById('discardFollowers').checked = true;
              document.getElementById('discardModal').style.display = 'flex';
            }

            function closeModal() {
              document.getElementById('resolveModal').style.display = 'none';
              document.getElementById('discardModal').style.display = 'none';
              currentFailureId = null;
            }

            async function submitResolve() {
              let event;
              try { event = JSON.parse(document.getElementById('resolvePayload').value); }
              catch (e) { alert('Payload is not valid JSON: ' + e.message); return; }
              const r = await fetch('/api/failures/' + currentFailureId + '/resolve', {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({event, resolved_by: 'operator'}),
              });
              if (!r.ok) { alert((await r.json()).detail || 'resolve failed'); return; }
              closeModal(); refresh();
            }

            async function submitDiscard() {
              const r = await fetch('/api/failures/' + currentFailureId + '/discard', {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({include_followers: document.getElementById('discardFollowers').checked, resolved_by: 'operator'}),
              });
              if (!r.ok) { alert((await r.json()).detail || 'discard failed'); return; }
              closeModal(); refresh();
            }

            refresh();
            setInterval(refresh, 5000);
            </script>
            </body>
            </html>
            """;
}
