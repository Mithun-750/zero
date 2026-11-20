#!/bin/bash
# Bring up the whole stack: infrastructure + outbox relay + payment consumer.
# Ctrl+C stops the relay and consumer (infra stays up).
#
# Stack selection:
#   STACK=spring  (default) — Spring Boot services (producer-spring/, consumer-spring/)
#   STACK=legacy            — old plain-Java services (producer/, consumer/)
set -euo pipefail
cd "$(dirname "$0")"

STACK="${STACK:-spring}"
if [ "$STACK" = "spring" ]; then
    echo "▸ stack: spring (STACK=legacy for the old services)"
else
    echo "▸ stack: legacy"
fi

# Spring apps don't read .env themselves — export it for both modes
# (legacy services load it via dotenv; harmless for them).
if [ -f .env ]; then
    set -a
    # shellcheck source=/dev/null
    source .env
    set +a
fi

echo "▸ docker compose up -d (postgres + 3-broker kafka cluster)"
docker compose up -d
echo "▸ waiting for infrastructure to become healthy…"
for i in $(seq 1 60); do
    pg=$(docker inspect -f '{{.State.Health.Status}}' zero-outbox-postgres 2>/dev/null || echo starting)
    k1=$(docker inspect -f '{{.State.Health.Status}}' zero-outbox-kafka-1 2>/dev/null || echo starting)
    k2=$(docker inspect -f '{{.State.Health.Status}}' zero-outbox-kafka-2 2>/dev/null || echo starting)
    k3=$(docker inspect -f '{{.State.Health.Status}}' zero-outbox-kafka-3 2>/dev/null || echo starting)
    if [ "$pg" = healthy ] && [ "$k1" = healthy ] && [ "$k2" = healthy ] && [ "$k3" = healthy ]; then
        echo "▸ postgres + 3 kafka brokers are healthy"
        break
    fi
    sleep 2
done

echo "▸ waiting for topic initialization (kafka-init)…"
docker compose wait kafka-init 2>/dev/null || sleep 3

if [ "$STACK" = "spring" ]; then
    echo "▸ building producer-spring and consumer-spring…"
    mvn -q -f producer-spring/pom.xml package -DskipTests
    mvn -q -f consumer-spring/pom.xml package -DskipTests

    echo "▸ starting outbox relay (producer-spring)…"
    java -jar producer-spring/target/producer-spring-1.0.0.jar &
    RELAY_PID=$!

    echo "▸ starting payment consumer (dashboard on http://127.0.0.1:${DASHBOARD_PORT:-8090})…"
    java -jar consumer-spring/target/consumer-spring-1.0.0.jar &
    CONSUMER_PID=$!
else
    echo "▸ starting outbox relay (producer)…"
    mvn -pl producer exec:java -q &
    RELAY_PID=$!

    echo "▸ starting payment consumer (dashboard on http://127.0.0.1:${DASHBOARD_PORT:-8090})…"
    mvn -pl consumer exec:java -q &
    CONSUMER_PID=$!
fi

cleanup() {
    echo "▸ stopping relay ($RELAY_PID) and consumer ($CONSUMER_PID)…"
    kill $RELAY_PID $CONSUMER_PID 2>/dev/null || true
    wait $RELAY_PID $CONSUMER_PID 2>/dev/null || true
    exit 0
}
trap cleanup INT TERM

wait
