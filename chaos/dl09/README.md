# Scenario DL-09: Kafka Partition Leader Failure

## Objective
Demonstrate that when a Kafka broker acting as a partition leader crashes while events are being actively published:
1. Kafka elects an eligible in-sync replica (ISR) as the new partition leader.
2. The producer detects the transient failure, refreshes cluster metadata from surviving brokers, and retries.
3. Publishing resumes automatically after leader election.
4. The consumer continues consuming without losing any successfully acknowledged event.
5. Consumer idempotency prevents duplicate business operations.
6. The failed broker is restored and rejoins the ISR.
7. Full reconciliation demonstrates **RPO = 0** and **RTO <= 2 hours**.

---

## Test Execution Details

- **Test Run ID**: `DL09-2026-001`
- **Total Workload**: 1,000 distinct business events ($E_1 \dots E_{1000}$)
- **Targeted Broker**: `kafka-1` (Broker ID 1, port 9092)
- **Targeted Partition**: Partition 0 (initial leader: Broker 1)
- **Failure Injection Point**: Mid-flight at event 500
- **Kafka Topic**: `orders.created` (Partitions: 3, RF: 3, `min.insync.replicas: 2`)
- **Producer Configuration**: `acks=all`, `enable.idempotence=true`, `retries=inf`

---

## Chronological Failure Timeline & Metrics

| Timestamp (UTC) | Milestone | Description |
| :--- | :--- | :--- |
| `2026-11-16T18:41:13Z` | Workload Ingestion | 1,000 orders inserted into PostgreSQL; trigger populated 1,000 Outbox events. |
| `2026-11-16T18:41:18Z` | Pipeline Start | Order consumer and Outbox relay launched. |
| `2026-11-16T18:41:24.221Z` | **Failure Injected** | Partition leader container `zero-outbox-kafka-1` stopped mid-flight (~500 events). |
| `2026-11-16T18:41:30.329Z` | **Leader Election** | KRaft elected Broker 2 as the new leader of Partition 0 in **6.108 seconds**. |
| `2026-11-16T18:41:30.343Z` | **Producer Recovery** | Producer refreshed metadata via surviving brokers and resumed publishing in **6.122 seconds**. |
| `2026-11-16T18:41:35.050Z` | Pipeline Drained | All 1,000 events published to Kafka and consumed into `processed_events`. |
| `2026-11-16T18:41:35.508Z` | Broker Start | Container `zero-outbox-kafka-1` restarted. |
| `2026-11-16T18:41:40.964Z` | **ISR Restoration** | Broker 1 resynchronized; all 3 partitions returned to full ISR `[1, 2, 3]` in **5.914 seconds**. |
| `2026-11-16T18:41:41.083Z` | Reconciliation Complete | Full event-ID reconciliation completed. Total Recovery Time: **16.743 seconds**. |

---

## Event-by-Event Reconciliation Summary

```text
======================================================================
EVENT RECONCILIATION REPORT: DL09-2026-001
======================================================================
Accepted Outbox Events         : 1000
Published to Kafka             : 1000
Processed by Consumer (DB)     : 1000
Pending in Outbox              : 0
Missing Acknowledged Events    : 0
Unexplained Missing Events     : 0
Duplicate Business Operations  : 0
Observed RPO                   : 0 (Target: 0)
RPO = 0 Status                 : PASS
======================================================================
```

---

## Recovery Metrics

- **Observed RPO**: **0** (Zero loss of acknowledged events)
- **Observed RTO**: **16.743 seconds** (Target: $\le 2$ hours)
  - Leader Election Latency: **6.108s**
  - Producer Recovery Latency: **6.122s**
  - Full Replica Synchronization: **5.914s**
- **Test Verdict**: **PASS**

