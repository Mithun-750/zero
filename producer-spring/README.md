# producer-spring — Spring Boot Outbox Relay

Spring Boot port of the `producer/` outbox relay. Polls the PostgreSQL
`outbox` table for `PENDING` events, publishes them to Kafka (`orders.created`,
`acks=all`, idempotent producer), and marks rows `PUBLISHED` only after broker ack.

## Build

```bash
mvn package
```

Produces `target/producer-spring-1.0.0.jar` (executable Spring Boot jar, via
`spring-boot-maven-plugin` — no shaded uber-jar).

## Run

Requires PostgreSQL + Kafka (see root `docker-compose.yml`) and the same `.env`
environment variables as before (Spring does not read `.env` by itself):

```bash
set -a && source ../.env && set +a

# Continuous mode (polls every OUTBOX_POLL_INTERVAL_SECONDS, default 2s)
java -jar target/producer-spring-1.0.0.jar

# Single batch, then exit
java -jar target/producer-spring-1.0.0.jar --once
```

## Configuration

Same env var names as before; `application.yml` maps them with placeholders
(defaults shown):

| Env var | Default | Used for |
|---|---|---|
| `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | `localhost` / `5434` / `order_db` / `postgres` / `postgres` | JDBC URL + HikariCP |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9094,localhost:9096` | Kafka |
| `KAFKA_CLIENT_ID` | `order-outbox-relay` | Producer client id |
| `KAFKA_ORDERS_TOPIC` | `orders.created` | Target topic |
| `OUTBOX_BATCH_SIZE` | `100` | Batch size per poll |
| `OUTBOX_POLL_INTERVAL_SECONDS` | `2` | Poll interval (fixed delay) |

Datasource Hikari property `stringtype=unspecified` is set (as the old client
did) so UUID columns compare correctly against string binds.

## Project layout

```
src/main/java/com/zero/producer/
├── OutboxRelayApplication.java      # entry point (WebApplicationType.NONE)
├── config/                          # OutboxProperties (@ConfigurationProperties)
├── service/                         # OutboxRelay (batch logic), RelayScheduler, OnceRunner, EventBuilder
├── repository/                      # DatabaseClient (JdbcTemplate — SQL unchanged)
└── kafka/                           # KafkaPublisher (KafkaTemplate)
```

## What changed vs `producer/`

- Plain poll-loop + shutdown hook → `@Scheduled` fixed-delay relay (`RelayScheduler`),
  graceful stop via `@PreDestroy` on `OutboxRelay`. `--once` selects `OnceRunner`.
- Hand-rolled JDBC (`DriverManager`) → `JdbcTemplate` (SQL unchanged).
- Raw `KafkaProducer` → `KafkaTemplate` with producer settings from `application.yml`
  (synchronous `.get(120s)` preserved).
- `Settings` (dotenv) → `OutboxProperties` + `application.yml` placeholders.
- `dotenv-java` dependency removed; Spring binds the same env vars.
