# Kafka Broker Failure & Recovery Experiment: Baseline Workload (Phase C)

## Experiment Metadata
- **Experiment ID**: `KAFKA-BROKER-FAILURE-2026-001`
- **Execution Timestamp**: `2026-11-17T07:54:45Z`
- **Application Runtime**: Java 21 (OpenJDK 64-Bit Server VM, build 21.0.6+7-LTS)
- **Build Tool**: Apache Maven 3.9.11
- **Message Broker**: Apache Kafka 4.4.0 (KRaft mode, 3 brokers)
- **Database**: PostgreSQL 17.4 (Docker container `zero-outbox-postgres`, port `5434`, database `order_db`)
- **Status**: **BASELINE COMPLETED & FULLY RECONCILED (CLEAN)**

---

## Infrastructure Baseline

### 1. Kafka Cluster Topology
- **Cluster ID**: `4L6g3nShT-eMCtK--X86sw`
- **Active Controller**: Broker 1 (Node ID: 1)
- **Cluster Nodes**:
  - Broker 1: `localhost:9092`
  - Broker 2: `localhost:9094`
  - Broker 3: `localhost:9096`

### 2. Topic Configuration (`orders.created`)
- **Partitions**: 3
- **Replication Factor**: 3
- **Min In-Sync Replicas (`min.insync.replicas`)**: 2
- **Partition Allocation & Health**:
  - **Partition 0**: Leader: `1` | Replicas: `[1, 2, 3]` | ISR: `[1, 2, 3]` | Start Offset: `319` | End Offset: `647` (+328)
  - **Partition 1**: Leader: `2` | Replicas: `[2, 3, 1]` | ISR: `[1, 2, 3]` | Start Offset: `337` | End Offset: `681` (+344)
  - **Partition 2**: Leader: `3` | Replicas: `[3, 1, 2]` | ISR: `[1, 2, 3]` | Start Offset: `352` | End Offset: `680` (+328)
- **Final Consumer Group Offsets (`payment-consumer`)**:
  - Partition 0: Committed `647` (Lag: `0`)
  - Partition 1: Committed `681` (Lag: `0`)
  - Partition 2: Committed `680` (Lag: `0`)
  - **Total Group Lag**: **`0`**

---

## Workload Execution & Metrics

| Metric | Target | Actual | Discrepancy |
| :--- | :---: | :---: | :---: |
| **Orders Generated** | 1,000 | 1,000 | 0 |
| **Outbox Events Created** | 1,000 | 1,000 | 0 |
| **Outbox Events Published** | 1,000 | 1,000 | 0 |
| **Kafka Messages Emitted** | 1,000 | 1,000 | 0 |
| **Consumer Events Processed** | 1,000 | 1,000 | 0 |
| **Duplicate Processing** | 0 | 0 | 0 |
| **Payments Created** | 1,000 | 1,000 | 0 |
| **Pending Outbox Events** | 0 | 0 | 0 |
| **Failed Outbox Events** | 0 | 0 | 0 |
| **DLQ Routed Events** | 0 | 0 | 0 |
| **Parked Events** | 0 | 0 | 0 |

---

## Database Baseline vs. Final State

| Table / Counter | Pre-Experiment Baseline | Post-Experiment Final | Experiment Net Change |
| :--- | :---: | :---: | :---: |
| `orders` | 1,004 | 2,004 | **+1,000** |
| `outbox` (Total) | 1,004 | 2,004 | **+1,000** |
| `outbox` (PENDING) | 0 | 0 | 0 |
| `outbox` (PUBLISHED) | 1,004 | 2,004 | **+1,000** |
| `outbox` (FAILED) | 0 | 0 | 0 |
| `processed_events` (Total) | 1,007 | 2,007 | **+1,000** |
| `processed_events` (`payment-consumer`) | 0 | 1,000 | **+1,000** |
| `processed_events` (`order-processing-consumer`) | 1,007 | 1,007 | 0 (historical preserved) |
| `payments` (`INITIATED`) | 0 | 1,000 | **+1,000** |
| `event_failures` (DLQ) | 1 | 1 | 0 (historical preserved) |
| `parked_events` | 0 | 0 | 0 |

---

## Exact UUID Set Reconciliation Report

The reconciliation engine compared the exact sets of 1,000 order UUIDs and 1,000 outbox event UUIDs across all data stores:

- **Order Existence Check**: 1,000 / 1,000 found (Missing: 0)
- **Outbox Linkage Check (`outbox.aggregate_id = orders.id`)**: 1,000 / 1,000 found (Missing: 0)
- **Outbox Status Check**: 1,000 `PUBLISHED` (Unpublished: 0, Failed: 0)
- **Consumer Processing Check (`processed_events.event_id = outbox.id`)**: 1,000 / 1,000 found (Missing: 0)
- **Idempotency Deduplication Check**: 0 duplicates detected
- **Payment Creation Check (`payments.idempotency_key = outbox.id`)**: 1,000 / 1,000 found (Missing: 0)
- **DLQ & Parked Isolation**: 0 experiment events encountered poison payload or aggregate blocking
- **Reconciliation Verdict**: **`CLEAN (100% Data Integrity across all 5 layers)`**

---

## Timestamps & Processing Latency
- **Order Generation**: `2026-11-17T07:52:29.504Z` - `2026-11-17T07:52:29.729Z` (~225 ms)
- **Outbox Publishing**: `2026-11-17T07:53:10.701Z` - `2026-11-17T07:53:29.770Z` (~19.07 s, ~52.4 msgs/sec with `acks=all`)
- **Consumer Processing**: `2026-11-17T07:54:11.688Z` - `2026-11-17T07:54:23.402Z` (~11.71 s, ~85.4 msgs/sec with explicit sync commit and DB writes)
- **Total Pipeline Execution**: ~1 minute 54 seconds end-to-end

---

## Artifact Files
- `orders-1000.json`: 1,000 synthetic order payloads.
- `manifest.json`: Experiment specification containing all 1,000 order IDs, 1,000 outbox event IDs, and pre-run environment snapshots.
- `baseline-results.json`: JSON output of the Java reconciliation engine verifying 0 discrepancies.

