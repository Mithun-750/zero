# Kafka Reliability & Fault Tolerance Test Suite

This directory documents the reliability, failover behavior, and durability characteristics of the 3-broker Apache Kafka cluster and Java Transactional Outbox pipeline under steady-state and injected fault conditions.

---

## 1. Reliability Objectives

The reliability test suite was designed to empirically validate:
1. **At-Least-Once Delivery with Exactly-Once Processing**: Ensuring that transactional events generated in PostgreSQL are safely published, redelivered under failure without gaps, and processed idempotently without duplicate business side-effects.
2. **Partition Leader Failover**: Quantifying the Recovery Time Objective (RTO) when a broker leading an active data partition experiences an ungraceful shutdown.
3. **KRaft Consensus Resilience**: Verifying that active KRaft controller failure triggers an autonomous Raft election among surviving voters without metadata corruption or operator intervention.
4. **Data Durability (RPO = 0)**: Confirming that with `acks=all`, `min.insync.replicas=2`, and `enable.idempotence=true`, zero acknowledged events are lost across any broker crash.
5. **Replication Catch-Up**: Demonstrating that revived nodes cleanly rejoin the cluster, catch up with high-watermark offsets, and return to the In-Sync Replicas (`ISR`) set.

---

## 2. System Architecture & Fault-Tolerant Topology

```
+-----------------------------------------------------------------------------------------+
|                                    PostgreSQL 17                                        |
|  - orders (ACID commit)                                                                 |
|  - outbox (PostgreSQL trigger: trg_order_created_outbox)                                |
|  - processed_events (Unique constraint on event_id for idempotency)                     |
|  - payments (Business state: INITIATED)                                                 |
+-----------------------------------------------------------------------------------------+
                                    ▲                       │
                        Polls PENDING                       │ Idempotent Write
                                    │                       ▼
+-----------------------+           │           +-----------------------+
|  Outbox Relay Producer| ──────────┼─────────> |   Payment Consumer    |
|  - Java 21, acks=all  |           │           |  - Java 21, Lag = 0   |
|  - Idempotence: true  |           │           |  - Explicit per-record|
|  - Retries: MAX_VALUE |           │           |    offset commit      |
+-----------------------+           │           +-----------------------+
            │                       │                       ▲
            │ Produces              │                       │ Consumes
            ▼                       │                       │
+-----------------------------------------------------------------------------------------+
|                           Apache Kafka 4.4.0 (KRaft Quorum)                             |
|  Topic: orders.created (3 Partitions, Replication Factor 3, min.insync.replicas=2)       |
|                                                                                         |
|  +--------------------+     +--------------------+     +--------------------+           |
|  |      kafka-1       |     |      kafka-2       |     |      kafka-3       |           |
|  | Node 1 | Port 9092 | <-> | Node 2 | Port 9094 | <-> | Node 3 | Port 9096 |           |
|  | Controller / Broker|     | Controller / Broker|     | Controller / Broker|           |
|  +--------------------+     +--------------------+     +--------------------+           |
+-----------------------------------------------------------------------------------------+
```

### Durability & Consistency Contract
- **Replication**: RF = 3, `min.insync.replicas = 2`. Any write acknowledged by Kafka has been written to the write-ahead log of at least two nodes.
- **Producer Configuration**: `acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`, `retries=Integer.MAX_VALUE`.
- **Consumer Offset Strategy**: Manual, synchronous offset commit after atomic database commit (`commitSync`). If an error occurs on a partition, processing halts for that partition, seeks back to the failed offset, and preserves offset progress on independent partitions.
- **Database Idempotency**: Atomic `INSERT INTO processed_events (event_id, ...) VALUES (...) ON CONFLICT (event_id) DO NOTHING` paired with business operations in a single database transaction.

---

## 3. Experiment Summaries & Measured Results

### A. Healthy Cluster Baseline (`baseline/`)
- **Workload**: 1,000 synthetic orders generated and committed to PostgreSQL.
- **Outbox Publishing**: 1,000 / 1,000 events published with `acks=all` (Duration: 30.6s, Throughput: 32.7 msgs/sec).
- **Consumer Processing**: 1,000 / 1,000 events consumed and persisted (Duration: 11.7s, Throughput: 85.5 msgs/sec).
- **Reconciliation**: **CLEAN**. 0 missing orders, 0 missing outbox, 0 duplicate payments, final lag: 0.

### B. Partition Leader Broker Failure (`broker-failure/`)
- **Failure Target**: `kafka-3` (Node ID: 3, leader of Partition 2).
- **Cluster State**: Broker 2 was active KRaft controller; Broker 1 and Broker 2 remained active voters.
- **Partition Failover**: KRaft detected Broker 3 disconnection and elected **Broker 1** as the new leader for Partition 2 in **22.986 seconds**. ISR contracted to `[1, 2]`.
- **Producer Performance**: All 1,000 outbox records published during the outage with `acks=all` with **0 failures** (Duration: 49.4s, Throughput: 20.2 msgs/sec).
- **Consumer Performance**: All 1,000 events consumed and committed during the outage (Duration: 30.2s, Throughput: 33.1 msgs/sec, final lag: 0).
- **Replication Catch-Up**: Broker 3 restarted, caught up with logs, and rejoined ISR `[1, 2, 3]` across all partitions in **38.152 seconds**.
- **Metrics**: **Failover RTO: 22.986s**, **Full Recovery RTO: 38.152s**, **RPO: 0 msgs (0.000s)**, **Reconciliation: CLEAN**.

### C. KRaft Controller Failure (`controller-failure/`)
- **Failure Target**: `kafka-1` (Node ID: 1, active KRaft controller at `LeaderEpoch: 5`, also partition leader for Partition 0).
- **Controller Election**: Surviving voters 2 and 3 elected **Broker 3** as new KRaft controller in **16.498 seconds** (`LeaderEpoch: 5 -> 6`, Quorum: 2/3 maintained).
- **Dual-Role Failover**: Newly elected controller immediately reassigned Partition 0 leadership to **Broker 2**. ISR contracted to `[2, 3]`.
- **Producer Performance**: Published 1,000 / 1,000 records during controller outage with `acks=all` (Duration: 28.168s, Throughput: 35.5 msgs/sec, 0 failures).
- **Consumer Performance**: Consumed and committed 1,000 / 1,000 records during controller outage (Duration: 18.87s, Throughput: 53.0 msgs/sec, final lag: 0).
- **Replication Catch-Up**: Broker 1 restarted, caught up metadata and partition logs (`MaxFollowerLag: 0`), and rejoined ISR `[1, 2, 3]` in **16.548 seconds**. Broker 3 stably retained controller role.
- **Metrics**: **Controller Election RTO: 16.498s**, **Full Recovery RTO: 16.548s**, **RPO: 0 msgs (0.000s)**, **Reconciliation: CLEAN**.

---

## 4. Evidence Navigation

All raw JSON evidence, manifests, and metrics are preserved:

- **Baseline Evidence**: [`baseline/`](baseline/)
  - [`manifest.json`](baseline/manifest.json): 1,000 baseline order and event UUIDs.
  - [`baseline-results.json`](baseline/baseline-results.json): Steady-state reconciliation report.
  - [`relevant-snapshots.json`](baseline/relevant-snapshots.json): Pre-flight database and Kafka topology snapshot.
- **Broker Failure Evidence**: [`broker-failure/`](broker-failure/)
  - [`manifest.json`](broker-failure/manifest.json): Workload IDs and pre-failure cluster parameters.
  - [`healthy-baseline.json`](broker-failure/healthy-baseline.json): Snapshot before broker failure.
  - [`failure-snapshot.json`](broker-failure/failure-snapshot.json): Snapshot showing Partition 2 reassigned to Broker 1 and ISR `[1, 2]`.
  - [`recovery-snapshot.json`](broker-failure/recovery-snapshot.json): Snapshot showing Broker 3 in ISR `[1, 2, 3]`.
  - [`reconciliation-results.json`](broker-failure/reconciliation-results.json): Zero discrepancies verified.
  - [`experiment-results.json`](broker-failure/experiment-results.json): Measured timelines, RTO, and throughput.
- **Controller Failure Evidence**: [`controller-failure/`](controller-failure/)
  - [`manifest.json`](controller-failure/manifest.json): Workload IDs and pre-failure parameters.
  - [`healthy-baseline.json`](controller-failure/healthy-baseline.json): Snapshot before controller failure (`LeaderId: 1, Epoch: 5`).
  - [`failure-snapshot.json`](controller-failure/failure-snapshot.json): Snapshot during failure (`LeaderId: 3, Epoch: 6`).
  - [`election-results.json`](controller-failure/election-results.json): Controller quorum vote records and failover timing.
  - [`recovery-snapshot.json`](controller-failure/recovery-snapshot.json): Post-restart metadata and ISR `[1, 2, 3]`.
  - [`reconciliation-results.json`](controller-failure/reconciliation-results.json): 1,000/1,000 clean match.
  - [`experiment-results.json`](controller-failure/experiment-results.json): Summary metrics and recovery latency.
- **Synthetic Workloads**: [`workloads/`](workloads/)
  - Raw JSON datasets and insertion scripts.
- **Comprehensive Technical Report**:
  - [`reports/Kafka-Reliability-Engineering-Report.md`](reports/Kafka-Reliability-Engineering-Report.md)

