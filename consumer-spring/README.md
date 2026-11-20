# consumer-spring — Spring Boot Payment Consumer

Spring Boot port of the `consumer/` payment service. Subscribes to `orders.created`
(manual offset commits, seek-on-failure — exact same semantics as the original
`PaymentConsumerApplication.processBatch`, covered by the same unit tests),
applies events idempotently (`payments` + `processed_events` in one tx), routes
poison events to `payments.dlq`, blocks the affected aggregate, and serves the
monitoring dashboard (REST + static HTML UI) on port 8090.

## Build & test

```bash
mvn verify
```

`BatchProcessorTest` is the ported offset-commit/rewind suite (MockConsumer, 7 tests).

## Run

Requires PostgreSQL + Kafka (see root `docker-compose.yml`) and the same `.env`
environment variables as before:

```bash
set -a && source ../.env && set +a

# Continuous consumer + dashboard
java -jar target/consumer-spring-1.0.0.jar

# Process available messages (or wait ~3s for assignment), then exit
java -jar target/consumer-spring-1.0.0.jar --once
```

Dashboard: http://${DASHBOARD_HOST:127.0.0.1}:${DASHBOARD_PORT:8090}
(same routes/JSON shapes as the old Javalin dashboard; HTML moved to `src/main/resources/static/index.html`)

## Fault injection

```bash
mvn spring-boot:run -Dspring-boot.run.main-class=com.zero.consumer.cli.FaultInjector
# custom: mvn spring-boot:run -Dspring-boot.run.main-class=com.zero.consumer.cli.FaultInjector -Dspring-boot.run.arguments="--followers 3"
```

## Configuration

Same env var names as before; `application.yml` maps them with placeholders
(defaults shown):

| Env var | Default | Used for |
|---|---|---|
| `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | `localhost` / `5434` / `order_db` / `postgres` / `postgres` | JDBC URL + HikariCP |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9094,localhost:9096` | Kafka |
| `KAFKA_ORDERS_TOPIC` | `orders.created` | Consumed topic |
| `KAFKA_DLQ_TOPIC` | `payments.dlq` | DLQ topic |
| `KAFKA_CONSUMER_GROUP` / `KAFKA_CONSUMER_CLIENT_ID` | `payment-consumer` / `payment-consumer` | Consumer group / client id (DLQ producer uses `<client-id>-dlq`) |
| `CONSUMER_MAX_RETRIES` / `CONSUMER_RETRY_BACKOFF_SECONDS` | `3` / `0.5` | Transient retry policy |
| `DASHBOARD_PORT` / `DASHBOARD_HOST` | `8090` / `127.0.0.1` | Dashboard bind |

## Project layout

```
src/main/java/com/zero/consumer/
├── ConsumerSpringApplication.java   # entry point
├── config/                          # ConsumerProperties, DlqKafkaConfig
├── controller/                      # DashboardController (REST)
├── kafka/                           # ConsumerWorker (SmartLifecycle poll loop), OnceConsumerRunner,
│                                    #   BatchProcessor (per-partition offset commit/seek), DlqPublisher
├── service/                         # Pipeline, FailureResolutionService, EventValidator, Metrics, RecordProcessor
├── repository/                      # DatabaseClient (JdbcTemplate — SQL unchanged)
├── cli/                             # FaultInjector (standalone main, not a Spring bean)
└── exception/                       # ConsumerException hierarchy (DLQ / offset / validation)
```

Static UI lives in `src/main/resources/static/index.html`.

## What changed vs `consumer/`

- Manual `KafkaConsumer` poll loop lives on in `ConsumerWorker` (SmartLifecycle) — the
  per-partition commit/seek semantics from `processBatch` are kept bit-for-bit
  (`BatchProcessor`, unit-tested). Continuous shutdown via `stop()` + `wakeup()`.
- Hand-rolled JDBC → `JdbcTemplate`; manual `setAutoCommit(false)` blocks → real
  `@Transactional` boundaries (`applyEvent`, `recordFailureAndBlock`,
  `FailureResolutionService`) — so the dashboard resolve/discard path (previously a
  dedicated connection + explicit BEGIN/COMMIT) is now a proper Spring transaction with
  automatic rollback.
- Javalin → `@RestController` (`DashboardController`), embedded `PAGE` string →
  `static/index.html`.
- `PaymentConsumerApplication`/`Settings` (dotenv) → `ConsumerSpringApplication` +
  `ConsumerProperties` + `application.yml` placeholders.
- Shared single `Connection` (not thread-safe) is gone — everything goes through HikariCP.
