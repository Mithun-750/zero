# End-to-End Order Processing Flow & Architecture

This document describes the complete lifecycle of an order across PostgreSQL, Kafka, the Python data generation pipeline, and the Java producer and consumer microservices.

---

## 1. System Architecture Diagram

```mermaid
sequenceDiagram
    autonumber
    actor Client as Data Prep (Python)
    participant PG as PostgreSQL (order_db)
    participant Relay as Producer Relay (Java)
    participant Kafka as Apache Kafka (KRaft)
    participant Consumer as Payment Consumer (Java)
    participant DLQ as Kafka (payments.dlq)
    actor Ops as Operator Dashboard

    Note over Client,PG: 1. Ingestion & Transactional Outbox
    Client->>PG: BEGIN Transaction
    Client->>PG: INSERT INTO orders
    Note right of PG: Trigger: trigger_order_created_outbox()<br/>Inserts into outbox (status='PENDING')
    Client->>PG: INSERT INTO order_items
    Client->>PG: COMMIT Transaction

    Note over PG,Kafka: 2. Outbox Polling & Kafka Relay
    loop Every 2s Poll Cycle
        Relay->>PG: SELECT * FROM outbox WHERE status='PENDING'
        Relay->>Relay: Build Event Envelope (EventBuilder)
        Relay->>Kafka: send(topic="orders.created", key=aggregate_id)
        Kafka-->>Relay: Broker ACK (acks=all)
        Relay->>PG: UPDATE outbox SET status='PUBLISHED'
    end

    Note over Kafka,Consumer: 3. Consumption & Business Application
    Consumer->>Kafka: poll() batch from "orders.created"
    Consumer->>PG: Check is_blocked(aggregate_id)?
    alt Aggregate is Blocked
        Consumer->>PG: INSERT INTO parked_events (order preserved)
        Consumer->>Kafka: commitSync()
    else Aggregate is Healthy
        Consumer->>Consumer: Validate Envelope & Business Rules
        alt Valid Order
            Consumer->>PG: BEGIN Transaction
            Consumer->>PG: INSERT INTO payments (status='INITIATED')
            Consumer->>PG: INSERT INTO processed_events (event_id)
            Consumer->>PG: COMMIT Transaction
            Consumer->>Kafka: commitSync()
        else Poison / Corrupt Record (Amount <= 0 / Invalid)
            Consumer->>DLQ: publishToDlq("payments.dlq", headers, raw)
            DLQ-->>Consumer: Broker ACK
            Consumer->>PG: BEGIN Transaction
            Consumer->>PG: INSERT INTO event_failures (status='OPEN')
            Consumer->>PG: block_aggregate(aggregate_id)
            Consumer->>PG: COMMIT Transaction
            Consumer->>Kafka: commitSync()
        end
    end

    Note over Consumer,Ops: 4. Operator Resolution via UI
    Ops->>Consumer: POST /api/failures/{id}/resolve (fixed JSON)
    Consumer->>PG: BEGIN Transaction
    Consumer->>PG: applyEvent(fixedEvent)
    Consumer->>PG: Replay parked_events in kafka_offset order
    Consumer->>PG: unblock_aggregate(aggregate_id)
    Consumer->>PG: UPDATE event_failures SET status='RESOLVED'
    Consumer->>PG: COMMIT Transaction
```

---

## 2. Infrastructure Initialization

The infrastructure runs locally via Docker Compose defined in [docker-compose.yml](docker-compose.yml#L1-L143).

### 2.1 PostgreSQL 17
* **Container**: `zero-outbox-postgres`
* **Port**: `5434` mapped to internal `5432` (`${POSTGRES_PORT:-5434}:5432`) ([docker-compose.yml](docker-compose.yml#L10-L11))
* **Schema Mount**: Mounts [schema.sql](schema.sql#L1-L222) to `/docker-entrypoint-initdb.d/01_schema.sql` on startup.
* **Healthcheck**: Uses `pg_isready -U postgres -d order_db` ([docker-compose.yml](docker-compose.yml#L15-L19)).

### 2.2 Apache Kafka 4.4.0 (3-Broker KRaft Cluster)
* **Brokers**:
  * `zero-outbox-kafka-1` (Node ID: 1, Host Port: `9092`, Controller: `29093`) ([docker-compose.yml](docker-compose.yml#L21-L53))
  * `zero-outbox-kafka-2` (Node ID: 2, Host Port: `9094`, Controller: `29093`) ([docker-compose.yml](docker-compose.yml#L54-L86))
  * `zero-outbox-kafka-3` (Node ID: 3, Host Port: `9096`, Controller: `29093`) ([docker-compose.yml](docker-compose.yml#L87-L119))
* **Mode**: KRaft without Zookeeper (`KAFKA_PROCESS_ROLES: "broker,controller"`), controller quorum voters across all 3 nodes.
* **Replication & Fault Tolerance**:
  * `default.replication.factor: 3`, `min.insync.replicas: 2`, `offsets.topic.replication.factor: 3`.
  * Guarantees high availability: survives loss of any single broker without downtime or data loss.

### 2.3 Automated Topic Initialization
The `kafka-init` service depends on `kafka-1`, `kafka-2`, and `kafka-3` being healthy, then creates topics with 3 partitions, replication factor 3, and `min.insync.replicas=2` ([docker-compose.yml](docker-compose.yml#L120-L138)):
```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --create --if-not-exists --topic orders.created --partitions 3 --replication-factor 3 --config min.insync.replicas=2
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --create --if-not-exists --topic payments.dlq --partitions 3 --replication-factor 3 --config min.insync.replicas=2
```

---

## 3. Order Ingestion & Transactional Outbox (Python Data-Prep)

Orders originate from the synthetic order generator and loader in Python.

### 3.1 Order Generation
In [data-prep/src/data_prep/generate_orders.py](data-prep/src/data_prep/generate_orders.py#L34-L77), `generate_single_order()` creates an order record:
* Generates a unique `order_id` (UUID), `order_number` (`ORD-YYYYMMDD-XXXXXX`), and customer details via Faker.
* Selects 1 to 4 random catalog products, calculating line item subtotals and `total_amount` in `INR`.

### 3.2 Atomic Insertion & DB Trigger
In [data-prep/src/data_prep/load_orders.py](data-prep/src/data_prep/load_orders.py#L46-L106), `insert_order()` executes within a single ACID transaction (`with conn.transaction():`):

```python
# 1. Insert into orders table (Fires the PostgreSQL Trigger)
cur.execute("""
    INSERT INTO orders (id, order_number, customer_id, customer_name, customer_email,
                        total_amount, currency, status, created_at, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", (...))

# 2. Insert line items
for item in items:
    cur.execute("""
        INSERT INTO order_items (id, order_id, product_id, product_name, quantity, unit_price, subtotal)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (...))
```

### 3.3 The Outbox Trigger (`schema.sql`)
The PostgreSQL trigger [trg_order_created_outbox](schema.sql#L147-L151) fires `AFTER INSERT ON orders FOR EACH ROW` and executes [trigger_order_created_outbox()](schema.sql#L107-L144):
```sql
INSERT INTO outbox (
    aggregate_type,
    aggregate_id,
    event_type,
    topic,
    payload,
    status,
    created_at
) VALUES (
    'ORDER',
    NEW.id,
    'OrderCreated',
    'orders.created',
    jsonb_build_object(
        'order_id', NEW.id,
        'order_number', NEW.order_number,
        'customer_id', NEW.customer_id,
        'customer_name', NEW.customer_name,
        'customer_email', NEW.customer_email,
        'total_amount', NEW.total_amount,
        'currency', NEW.currency,
        'status', NEW.status,
        'payment_trigger', jsonb_build_object(
            'action', 'INITIATE_PAYMENT',
            'amount', NEW.total_amount,
            'currency', NEW.currency,
            'target_account', NEW.customer_id
        )
    ),
    'PENDING',
    NEW.created_at
);
```
> [!IMPORTANT]
> The order and its outbox event are committed atomically in the same database transaction. A broker outage or application crash cannot cause a dual-write inconsistency.

---

## 4. Outbox Relay Producer (Java)

The Java producer ([producer/](producer)) runs continuously to transport events from PostgreSQL to Kafka.

### 4.1 Polling Loop
[producer/src/main/java/com/zero/producer/OutboxRelayApplication.java](producer/src/main/java/com/zero/producer/OutboxRelayApplication.java#L49-L66) runs the poll cycle:
1. `fetchPendingEvents(batchSize)` queries the database:
   ```sql
   SELECT id, aggregate_type, aggregate_id, event_type, payload, created_at
   FROM outbox
   WHERE status = 'PENDING'
   ORDER BY created_at, id
   LIMIT ?
   ```
   Implemented in [producer/src/main/java/com/zero/producer/DatabaseClient.java](producer/src/main/java/com/zero/producer/DatabaseClient.java#L62-L86).

### 4.2 Event Envelope Construction
[producer/src/main/java/com/zero/producer/EventBuilder.java](producer/src/main/java/com/zero/producer/EventBuilder.java#L40-L88) builds the standardized CloudEvents-like envelope:
* Separates `payment_trigger` from order attributes.
* Formats timestamps to ISO-8601 strings.
* Sets `source: "order-service"`, `event_version: 1`, `aggregate_type: "ORDER"`, and `aggregate_id`.

```json
{
  "event_id": "e621fcb3-e517-4f48-b2f7-58b66bd801aa",
  "event_type": "OrderCreated",
  "event_version": 1,
  "occurred_at": "2026-11-17T06:09:55.123Z",
  "aggregate_type": "ORDER",
  "aggregate_id": "dcae324d-b66f-480a-b57d-f5fb876738e2",
  "source": "order-service",
  "data": {
    "order": {
      "order_id": "dcae324d-b66f-480a-b57d-f5fb876738e2",
      "order_number": "ORD-20261117-877222",
      "customer_id": "CUST-6224",
      "customer_name": "Kayla Rocha",
      "customer_email": "rocha@example.com",
      "total_amount": 447.00,
      "currency": "INR",
      "status": "CREATED"
    },
    "payment_trigger": {
      "action": "INITIATE_PAYMENT",
      "amount": 447.00,
      "currency": "INR",
      "target_account": "CUST-6224"
    }
  }
}
```

### 4.3 Synchronous Kafka Publishing
In [producer/src/main/java/com/zero/producer/KafkaPublisher.java](producer/src/main/java/com/zero/producer/KafkaPublisher.java#L54-L62):
* The message is keyed with `aggregate_id.getBytes(StandardCharsets.UTF_8)` ensuring all events for the same order route to the same Kafka partition in strict order.
* Producer is configured with `acks="all"` and `enable.idempotence=true` ([producer/src/main/java/com/zero/producer/KafkaPublisher.java](producer/src/main/java/com/zero/producer/KafkaPublisher.java#L38-L39)).
* Blocks on `producer.send(...).get(120, TimeUnit.SECONDS)` to guarantee broker persistence before proceeding.

### 4.4 Status Transition
* **On Success**: Calls [producer/src/main/java/com/zero/producer/DatabaseClient.java](producer/src/main/java/com/zero/producer/DatabaseClient.java#L91-L103) (`markPublished()`) executing:
  ```sql
  UPDATE outbox
  SET status = 'PUBLISHED', published_at = CURRENT_TIMESTAMP, error_message = NULL
  WHERE id = ? AND status = 'PENDING'
  ```
* **On Publish Failure**: Calls [producer/src/main/java/com/zero/producer/DatabaseClient.java](producer/src/main/java/com/zero/producer/DatabaseClient.java#L108-L129) (`recordPublishFailure()`), incrementing `retry_count` and saving the exception message while leaving the row `PENDING` for redelivery.

---

## 5. Payment Consumer Pipeline (Java)

The Java consumer ([consumer/](consumer)) processes records with manual offset commits to guarantee at-least-once processing.

### 5.1 Consumption Loop & Per-Partition Offset Management
[consumer/src/main/java/com/zero/consumer/PaymentConsumerApplication.java](consumer/src/main/java/com/zero/consumer/PaymentConsumerApplication.java#L80-L165) polls the `orders.created` topic with `enable.auto.commit = false`.
* **Per-Partition Batch Processing (`processBatch`)**:
  Instead of a flat loop that commits arbitrary offsets, `processBatch` groups records by `TopicPartition`:
  ```java
  for (TopicPartition tp : records.partitions()) {
      for (ConsumerRecord<byte[], byte[]> record : records.records(tp)) {
          ...
      }
  }
  ```
* **Contiguous Per-Record Offset Commits**:
  Only after a durable outcome is achieved (applied, duplicate, parked, or DLQ'd) does the consumer commit the exact next offset for that partition:
  ```java
  consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(record.offset() + 1)));
  ```
* **Partition Rewind (`seek`) on Failure**:
  If [consumer/src/main/java/com/zero/consumer/OffsetRejectException.java](consumer/src/main/java/com/zero/consumer/OffsetRejectException.java#L1-L10) is caught (e.g., DB failure or DLQ write failure):
  1. The consumer executes `consumer.seek(tp, record.offset())` to rewind Kafka's pointer back to the failed record.
  2. Stops processing further records on that specific partition for this batch (`break`).
  3. **Fault Isolation**: Other independent partitions in the same batch continue processing without interruption.
  4. Guarantees strict at-least-once delivery with zero skipped messages.

### 5.2 Three-Stage Processing Pipeline ([consumer/src/main/java/com/zero/consumer/Pipeline.java](consumer/src/main/java/com/zero/consumer/Pipeline.java#L43-L147))

```
Incoming Kafka Record
        |
        v
[1. Parse JSON] -------- (Malformed JSON) ---------> [Route to DLQ (No Aggregate Block)]
        |
        +---- (Valid JSON)
        |
        v
[2. Block Check] ------- (Aggregate Blocked) -------> [Park in parked_events (Offset Committed)]
        |
        +---- (Aggregate Not Blocked)
        |
        v
[3. Validate & Apply] -- (Permanent Error: <=0 INR) -> [Route to DLQ + Block Aggregate + Ledger]
        |
        +---- (Transient DB Error) -----------------> [Retry with Exponential Backoff + Jitter]
        |
        +---- (Success / Duplicate) ----------------> [Write payments & processed_events]
        |
        v
Commit Kafka Offset (commitSync)
```

#### Stage 1: Parse
* If the raw payload cannot be parsed as a JSON object, it is immediately routed to the DLQ as a `VALIDATION` failure via `failEvent()` without blocking any aggregate (since no valid `aggregate_id` can be identified).

#### Stage 2: Aggregate Block Check & Follower Parking
* If the order has an `aggregate_id`, the consumer checks whether the aggregate is blocked:
  ```sql
  SELECT is_blocked FROM aggregate_processing_state
  WHERE aggregate_type = ? AND aggregate_id = ?
  ```
  ([consumer/src/main/java/com/zero/consumer/DatabaseClient.java](consumer/src/main/java/com/zero/consumer/DatabaseClient.java#L40-L53)).
* If blocked, the event is a **follower** of an unresolved poison event. To preserve strict ordering, it is parked via `db.parkEvent()` in [schema.sql](schema.sql#L160-L176) (`parked_events`) with its `kafka_offset`.
* The consumer commits the offset and proceeds. The follower will be replayed later in exact `kafka_offset` order when the operator resolves the poison failure.

#### Stage 3: Event Validation & Atomic Apply
[consumer/src/main/java/com/zero/consumer/EventValidator.java](consumer/src/main/java/com/zero/consumer/EventValidator.java#L38-L133) validates domain constraints:
* `event_id` and `aggregate_id` must be valid UUIDs.
* `payment_trigger.action` must equal `"INITIATE_PAYMENT"`.
* `payment_trigger.amount` must be a positive number (`> 0`). Non-positive amounts raise [consumer/src/main/java/com/zero/consumer/BusinessRuleException.java](consumer/src/main/java/com/zero/consumer/BusinessRuleException.java#L1-L17) (permanent error).
* `currency` must be a 3-letter uppercase ISO code (e.g., `INR`).

[consumer/src/main/java/com/zero/consumer/DatabaseClient.java](consumer/src/main/java/com/zero/consumer/DatabaseClient.java#L121-L176) (`applyEvent()`) applies the payment within an explicit JDBC transaction (`conn.setAutoCommit(false)`):
1. **Deduplication**: Checks if `event_id` exists in `processed_events`. If yes, commits and returns `"duplicate"`.
2. **Payment Creation**: Inserts into `payments` table with `status = 'INITIATED'` and `idempotency_key = event_id`.
3. **Ledger Record**: Inserts into `processed_events` table (`event_id`, `consumer_name`, `aggregate_id`).
4. **Commit**: Both rows commit atomically.

---

## 6. Dead-Letter Queue (DLQ) & Fault Tolerance

When an unrecoverable failure occurs (such as a negative order amount or malformed schema):

### 6.1 DLQ Publisher ([consumer/src/main/java/com/zero/consumer/DlqPublisher.java](consumer/src/main/java/com/zero/consumer/DlqPublisher.java#L87-L152))
1. Publishes a self-contained diagnostic message to topic `payments.dlq`.
2. Sets Kafka headers: `failure_class`, `consumer_name`, `event_id`, `aggregate_id`.
3. The message body contains:
   * Original coordinates: `source_topic`, `source_partition`, `source_offset`.
   * Truncated raw payload (`raw_payload` up to 10,000 characters).
   * Exact failure reason and class (`BUSINESS_RULE` or `VALIDATION`).
4. Requires broker acknowledgement (`acks=all`) before the consumer proceeds.

### 6.2 Atomic Failure Record & Aggregate Blocking
[consumer/src/main/java/com/zero/consumer/DatabaseClient.java](consumer/src/main/java/com/zero/consumer/DatabaseClient.java#L462-L484) (`recordFailureAndBlock()`) executes inside an ACID transaction:
1. **Insert into `event_failures`**: Stores the failure record with `status = 'OPEN'`.
2. **Upsert `aggregate_processing_state`**: Sets `is_blocked = TRUE`, records `blocked_reason`, and links `blocked_event_id`.

```sql
INSERT INTO aggregate_processing_state
    (aggregate_type, aggregate_id, is_blocked, blocked_event_id, blocked_reason, blocked_at, blocked_by_consumer, updated_at)
VALUES (?, ?, TRUE, ?, ?, NOW(), ?, NOW())
ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE SET
    is_blocked = TRUE,
    blocked_event_id = EXCLUDED.blocked_event_id,
    blocked_reason = EXCLUDED.blocked_reason,
    blocked_at = NOW(),
    updated_at = NOW()
```

---

## 7. Operational Dashboard & Resolution Flow

The consumer embeds a Javalin HTTP server on port `8090` ([consumer/src/main/java/com/zero/consumer/Dashboard.java](consumer/src/main/java/com/zero/consumer/Dashboard.java#L45-L182)):

### 7.1 Real-Time Dashboard UI
* Served at `GET http://127.0.0.1:8090`.
* Displays real-time metrics (`/api/metrics`), failure lists (`/api/failures`), blocked aggregates (`/api/blocked`), parked followers (`/api/parked`), and recent payments (`/api/payments`).

### 7.2 Fault Injection CLI
Run [consumer/src/main/java/com/zero/consumer/FaultInjector.java](consumer/src/main/java/com/zero/consumer/FaultInjector.java#L32-L100):
```bash
mvn -pl consumer exec:java -Dexec.mainClass="com.zero.consumer.FaultInjector"
```
* Publishes a corrupt event with `amount = -50.0` followed by 2 valid follower events (`amount = 99.0`) for an existing order ID.
* The consumer sends the corrupt event to the DLQ, blocks the aggregate, and parks both followers.

### 7.3 Resolving a Failure in One Transaction
In [consumer/src/main/java/com/zero/consumer/Dashboard.java](consumer/src/main/java/com/zero/consumer/Dashboard.java#L223-L300) (`resolveFailure()`):
1. **Operator Edit**: The operator edits the payload in the UI (e.g., changes amount to `50.0`) and clicks **Apply & Unblock**.
2. **Validation**: The fixed event is validated against business rules before opening the transaction.
3. **Locking**: Selects the failure `FOR UPDATE` to prevent concurrent resolutions.
4. **Apply Fix**: Applies the fixed event via `applyEvent(fixedEvent)`.
5. **Replay Parked Followers**: Queries `parked_events` for the aggregate and applies them in strict `kafka_offset` order. Marks rows `REPLAYED`.
6. **Unblock Aggregate**: Calls `unblockAggregate()`, flipping `is_blocked = FALSE` and setting resolution metadata.
7. **Mark Resolved**: Updates `event_failures` to `status = 'RESOLVED'`.
8. **Commit**: All steps commit together. If any follower fails, the transaction rolls back, keeping the aggregate paused.

---

## 8. Reliability Testing & Cluster Reconciliation (`reliability`)

The `reliability` module provides an automated verification and audit suite ([reliability/](reliability)) to test cluster partition tolerance, broker failovers, and reconcile end-to-end ledger consistency.

### 8.1 Key Reliability Operations ([ExperimentCLI.java](reliability/src/main/java/com/zero/reliability/ExperimentCLI.java))
* **Database Snapshot (`snapshot-db`)**:
  Captures a complete point-in-time snapshot of the database state including orders, pending outbox, published outbox, payments, and parked events ([DatabaseSnapshotClient.java](reliability/src/main/java/com/zero/reliability/db/DatabaseSnapshotClient.java)).
* **Kafka Cluster Inspection (`inspect-kafka`)**:
  Connects to all 3 brokers, inspecting partition leader distribution, In-Sync Replicas (ISR), log end offsets, and consumer group commit lag across `orders.created` and `payments.dlq` ([KafkaClusterInspector.java](reliability/src/main/java/com/zero/reliability/kafka/KafkaClusterInspector.java)).
* **Reconciliation Engine (`reconcile`)**:
  Cross-checks database orders and outbox rows against Kafka partition log offsets and payment records. Verifies:
  1. **Zero Message Loss**: Every `PUBLISHED` outbox record exists in Kafka and is represented in `payments` or `event_failures`.
  2. **Deduplication Correctness**: No duplicate payments created despite network retries.
  3. **Strict Partition Ordering**: Outbox events per aggregate follow monotonic sequence.
  Generates a detailed structured audit report ([ReconciliationEngine.java](reliability/src/main/java/com/zero/reliability/reconciliation/ReconciliationEngine.java)).

```bash
# Capture full cluster & DB snapshot before/after experiment
mvn -pl reliability exec:java -Dexec.args="capture-state target/experiment-pre"

# Run ledger reconciliation
mvn -pl reliability exec:java -Dexec.args="reconcile"
```

---

## 9. Summary Table of Microservice Commands

| Task | Command |
|---|---|
| **Build all Java modules** | `mvn clean package -DskipTests` |
| **Run all unit & partition tests** | `mvn test` |
| **Start full stack (infra + services)** | `./dev.sh` |
| **Generate & load orders (Python)** | `cd data-prep && uv run test-load -c 20` |
| **Run Producer Relay (Continuous)** | `mvn -pl producer exec:java` |
| **Run Producer Relay (Single batch)** | `mvn -pl producer exec:java -Dexec.args="--once"` |
| **Run Consumer + Dashboard (Continuous)** | `mvn -pl consumer exec:java` |
| **Run Consumer (Poll once & exit)** | `mvn -pl consumer exec:java -Dexec.args="--once"` |
| **Simulate Poison Event (Fault Injector)**| `mvn -pl consumer exec:java -Dexec.mainClass="com.zero.consumer.FaultInjector"` |
| **Run Reliability Cluster Reconciliation**| `mvn -pl reliability exec:java -Dexec.args="reconcile"` |
| **Open Web Dashboard** | `http://localhost:8090` |

