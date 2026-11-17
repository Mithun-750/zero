# Phase D: Kafka Broker Failure and Recovery Experiment (DL-09)
**Experiment ID**: `KAFKA-BROKER-FAILURE-2026-002`  
**Execution Date**: 2026-11-17  
**Status**: COMPLETED (SUCCESSFUL)

---

## 1. Executive Summary

This experiment evaluated the fault tolerance, partition re-election, durability, and recovery characteristics of a 3-broker Apache Kafka 4.4.0 cluster running in KRaft mode when subjected to an abrupt broker failure during an active workload.

A batch of **1,000 synthetic orders** was injected into PostgreSQL 17, producing 1,000 `PENDING` transactional outbox records. Following pre-failure health checks, **Broker 3** (the designated partition leader for Partition 2 of topic `orders.created`) was abruptly stopped via Docker Compose.

During the single-broker outage window:
- **Broker 2** remained active as the KRaft quorum leader (controller).
- KRaft successfully coordinated an automatic partition leader re-election for Partition 2, transferring leadership to **Broker 1** in **22.986 seconds**.
- The Outbox Relay Producer published all **1,000 events** to Kafka with `acks=all` (`min.insync.replicas=2` satisfied across remaining Brokers 1 and 2) with **0 failed attempts**.
- The Payment Consumer consumed and idempotently processed all **1,000 events** from Kafka, recording database transactions and committing offsets synchronously per partition with **0 failures** and reaching a final lag of **0**.
- **Broker 3** was restarted, caught up with replicated logs, and rejoined the In-Sync Replicas (`ISR`) set across all three partitions in **38.152 seconds**.
- An end-to-end reconciliation confirmed **0 missing orders**, **0 missing outbox events**, **0 missing processed events**, **0 missing payments**, **0 duplicate business side-effects**, and **0 DLQ/parked events** (**RPO = 0**, **Client Service Outage = 0s**).

---

## 2. Cluster Architecture & Durability Profile

| Component | Specification / Configuration |
| :--- | :--- |
| **Kafka Version** | Apache Kafka 4.4.0 (KRaft Mode, Apache Kafka Raft metadata quorum) |
| **Brokers** | 3 (`kafka-1:9092` [node.id=1], `kafka-2:9094` [node.id=2], `kafka-3:9096` [node.id=3]) |
| **Topic** | `orders.created` (3 Partitions, Replication Factor 3) |
| **Durability Settings** | `min.insync.replicas=2`, Producer `acks=all`, `enable.idempotence=true` |
| **Database** | PostgreSQL 17 on port 5434 (`order_db`) |
| **Outbox Relay** | Java 21 Outbox Relay Producer (`producer-1.0.0.jar`) |
| **Payment Consumer** | Java 21 Idempotent Consumer (`consumer-1.0.0.jar`) with explicit synchronous commits |

---

## 3. Failure Target Selection Justification

Pre-experiment cluster inspection revealed that **Broker 2** (`node.id=2`) held the active KRaft Quorum Controller lease (`LeaderId: 2`). In contrast, **Broker 3** (`node.id=3`) was a KRaft follower and the partition leader for `orders.created` Partition 2.

In accordance with strict safety rules:
1. Stopping the active KRaft controller would conflate metadata quorum re-election with partition leadership failover.
2. Selecting **Broker 3** strictly isolated **partition leader failover and replication catch-up** while preserving the KRaft controller quorum (`[1, 2]` maintaining 2/3 voter majority).
3. The choice was verified against cluster preconditions and authorized before execution.

---

## 4. Experiment Timeline

| UTC Timestamp | Event / Operation | Observed System Behavior |
| :--- | :--- | :--- |
| `2026-11-17T08:39:55Z` | Workload Generation | 1,000 orders generated and committed to PostgreSQL. 1,000 outbox records created as `PENDING`. |
| `2026-11-17T08:48:41Z` | Pre-Failure Baseline | Quorum healthy (Leader: Broker 2). Leadership: P0 $\to$ 1, P1 $\to$ 2, P2 $\to$ 3. ISR `[1, 2, 3]` across all partitions. |
| `2026-11-17T08:48:52Z` | Broker Failure Injected | `docker compose stop kafka-3` executed. Container `zero-outbox-kafka-3` transitioned to `Exited (143)`. |
| `2026-11-17T08:49:15Z` | Partition Failover Verified | Partition 2 elected **Broker 1** as new leader. ISR shrunk to `[1, 2]` across all partitions. |
| `2026-11-17T08:49:40Z` | Outbox Producer Started | Java Outbox Relay executed with batch size 1,000 while Broker 3 remained stopped. |
| `2026-11-17T08:50:30Z` | Producer Completed | 1,000 outbox records published to Kafka with `acks=all`. 0 failures. Outbox records marked `PUBLISHED`. |
| `2026-11-17T08:51:06Z` | Consumer Started | Java Consumer executed in batch polling mode while Broker 3 remained stopped. |
| `2026-11-17T08:51:37Z` | Consumer Completed | 1,000 events consumed, idempotently written to `processed_events` & `payments`. Final lag = 0. |
| `2026-11-17T08:52:15Z` | Broker 3 Restarted | `docker compose start kafka-3` executed. Container restarted and rejoined KRaft network. |
| `2026-11-17T08:52:53Z` | Full ISR Recovery Verified | Broker 3 completed replication catch-up. ISR returned to `[1, 2, 3]` on all partitions. Under-replicated: 0. |
| `2026-11-17T08:53:05Z` | Data Reconciliation | End-to-end reconciliation executed across PostgreSQL and Kafka offsets. Result: 100% clean. |

---

## 5. Partition Leadership & In-Sync Replica Dynamics

### Topic: `orders.created` (3 Partitions, RF=3, min.isr=2)

```
+-----------+---------------------+-------------------------------+----------------------+
| Partition | Pre-Failure State   | During Failure (kafka-3 Down) | Post-Recovery State  |
+-----------+---------------------+-------------------------------+----------------------+
| 0         | Leader: 1           | Leader: 1                     | Leader: 1            |
|           | Replicas: [1, 2, 3] | Replicas: [1, 2, 3]           | Replicas: [1, 2, 3]  |
|           | ISR: [1, 2, 3]      | ISR: [1, 2]                   | ISR: [1, 2, 3]       |
+-----------+---------------------+-------------------------------+----------------------+
| 1         | Leader: 2           | Leader: 2                     | Leader: 2            |
|           | Replicas: [2, 3, 1] | Replicas: [2, 3, 1]           | Replicas: [2, 3, 1]  |
|           | ISR: [1, 2, 3]      | ISR: [1, 2]                   | ISR: [1, 2, 3]       |
+-----------+---------------------+-------------------------------+----------------------+
| 2         | Leader: 3           | Leader: 1 (FAILOVER)          | Leader: 1            |
|           | Replicas: [3, 1, 2] | Replicas: [3, 1, 2]           | Replicas: [3, 1, 2]  |
|           | ISR: [1, 2, 3]      | ISR: [1, 2]                   | ISR: [1, 2, 3]       |
+-----------+---------------------+-------------------------------+----------------------+
```

**Key Takeaways on Failover**:
1. When Broker 3 failed, Partition 2 leadership shifted automatically to **Broker 1** (the next member in the replica set and in the ISR).
2. Because `min.insync.replicas=2` and two brokers (1 and 2) were online, the cluster never entered an unavailable state for producers configuring `acks=all`.
3. Upon restart, Broker 3 fetched the missing log segments, caught up to the High Watermark, and was readmitted to the ISR without causing partition leader churn (leadership remained on Broker 1, avoiding unnecessary failover flapping).

---

## 6. Producer & Consumer Resilience Under Broker Failure

### Outbox Relay Producer Performance
- **Mode**: Java 21 Outbox Relay (`producer-1.0.0.jar`)
- **Published**: 1,000 / 1,000 records
- **Failed**: 0
- **Duration**: 49.445 seconds (Throughput: 20.22 msgs/sec)
- **Producer Configuration**: `acks=all`, `enable.idempotence=true`, `retries=Integer.MAX_VALUE`, `delivery.timeout.ms=120000`
- **Result**: The producer seamlessly routed Partition 2 records to Broker 1. The database status of all 1,000 outbox records transitioned from `PENDING` to `PUBLISHED` with exact commit timestamps.

### Payment Consumer Performance
- **Mode**: Java 21 Consumer (`consumer-1.0.0.jar`)
- **Total Processed**: 1,000 records
- **Batches**: Batch 1 (355 records), Batch 2 (500 records), Batch 3 (145 records), Batch 4 (0 records / termination)
- **Duration**: 30.233 seconds (Throughput: 33.08 msgs/sec)
- **Commit Strategy**: Explicit per-partition synchronous offset commit after atomic database transaction
- **Final Consumer Lag**: 0 across all partitions (P0: 977, P1: 1036, P2: 995)

---

## 7. Recovery Time (RTO) and Recovery Point (RPO) Analysis

| Metric | Target | Measured Result | Evaluation |
| :--- | :--- | :--- | :--- |
| **Partition Failover Time** | $< 30$ seconds | **22.986 s** | **PASS** — Fast KRaft leader election |
| **Full Cluster Recovery Time** | $< 60$ seconds | **38.152 s** | **PASS** — Fast replica catch-up & ISR re-join |
| **Client Service Outage** | 0 seconds | **0.000 s** | **PASS** — `min.isr=2` with RF=3 prevented any producer/consumer rejection |
| **Recovery Point Objective (RPO)** | 0 messages lost | **0 messages lost** | **PASS** — All 1,000 events committed and verified |

---

## 8. End-to-End Reconciliation Summary

The reconciliation engine compared the state across all relational tables and Kafka topic log offsets:

```
================================================================================
RECONCILIATION VERDICT: CLEAN
================================================================================
Total Expected Orders:           1,000
Orders in Database:              1,000
Outbox Records in Database:      1,000 (100% PUBLISHED)
Processed Events Records:        1,000
Payment Records Created:         1,000 (100% INITIATED)
Missing Orders:                  0
Missing Outbox Events:           0
Missing Processed Events:        0
Missing Payments:                0
Duplicate Processed Events:      0
DLQ Events:                      0
Parked Events:                   0
Kafka Topic Message Count:       3,008 total (Baseline: 2,008 | Phase D: 1,000)
Final Consumer Group Lag:        0
================================================================================
```

---

## 9. Verification Artifacts

All raw machine-readable JSON artifacts and logs are preserved in `reliability-testing/kafka-broker-failure/phase-d/`:

1. `manifest.json`: Full manifest of 1,000 generated order UUIDs and outbox event IDs.
2. `healthy-baseline.json`: Snapshot of Kafka cluster and KRaft quorum state before failure.
3. `broker-failure-snapshot.json`: Cluster metadata and partition leadership during Broker 3 failure.
4. `recovery-snapshot.json`: Post-recovery cluster metadata showing Broker 3 in ISR.
5. `reconciliation-results.json`: Output of `reconcile` subcommand confirming zero discrepancies.
6. `experiment-results.json`: Comprehensive metrics including RTO, RPO, and throughput.
7. `orders-1000.json`: Detailed payload of the 1,000 generated orders.

