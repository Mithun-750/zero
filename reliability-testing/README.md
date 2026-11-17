# Reliability Testing

This directory serves as the centralized repository for distributed systems fault-injection experiments, baseline measurements, and verification artifacts for the Transactional Outbox and Apache Kafka architecture.

---

## 1. Project Overview

The system under test implements an event-driven e-commerce transaction processing pipeline using the **Transactional Outbox Pattern**:
1. Business entities (orders and line items) are committed to a relational database in ACID transactions alongside outbox event records.
2. An Outbox Relay Producer tails pending outbox events and publishes them to an Apache Kafka topic with strict durability guarantees (`acks=all`, `enable.idempotence=true`).
3. An Idempotent Payment Consumer subscribes to the Kafka topic, processes payments through an atomic idempotency table, and commits partition offsets explicitly.

---

## 2. Technology Stack

- **Application Platform**: Java 21 LTS (OpenJDK) built with Apache Maven
- **Database Engine**: PostgreSQL 17
- **Message Broker**: Apache Kafka 4.4.0 (KRaft Consensus Mode, RF=3, `min.insync.replicas=2`)
- **Reliability Framework**: Custom Java AdminClient inspector, JDBC snapshot client, and reconciliation engine
- **Workload Automation**: Python 3.11 with `psycopg 3` and `faker`

---

## 3. Directory Layout

The completed test suites, evidence snapshots, and comprehensive engineering reports are organized under [`kafka-reliability/`](kafka-reliability/):

```text
reliability-testing/
│
├── README.md                                 # Root reliability testing overview (this file)
│
└── kafka-reliability/                        # Kafka distributed reliability test suite
    ├── README.md                             # Architectural overview & experiment summaries
    │
    ├── baseline/                             # Steady-state healthy cluster baseline (1,000 orders)
    │   ├── manifest.json
    │   ├── baseline-results.json
    │   └── relevant-snapshots.json
    │
    ├── broker-failure/                       # Partition leader broker crash & recovery (1,000 orders)
    │   ├── manifest.json
    │   ├── healthy-baseline.json
    │   ├── failure-snapshot.json
    │   ├── recovery-snapshot.json
    │   ├── reconciliation-results.json
    │   └── experiment-results.json
    │
    ├── controller-failure/                   # Active KRaft controller failover & recovery (1,000 orders)
    │   ├── manifest.json
    │   ├── healthy-baseline.json
    │   ├── failure-snapshot.json
    │   ├── election-results.json
    │   ├── recovery-snapshot.json
    │   ├── reconciliation-results.json
    │   └── experiment-results.json
    │
    ├── workloads/                            # Synthetic order datasets and generation utilities
    │   ├── README.md
    │   ├── baseline-orders.json
    │   ├── broker-failure-orders.json
    │   ├── controller-failure-orders.json
    │   └── generate_workload.py
    │
    └── reports/                              # Consolidated technical reports
        └── Kafka-Reliability-Engineering-Report.md
```

For detailed methodology, failure timelines, measured Recovery Time Objectives (RTO), Recovery Point Objectives (RPO), and reconciliation verdicts, refer to the [Kafka Reliability Technical Report](kafka-reliability/reports/Kafka-Reliability-Engineering-Report.md).

