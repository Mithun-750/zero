#!/bin/bash
# Bring up the whole stack: infrastructure + outbox relay + payment consumer.
# Ctrl+C stops the relay and consumer (infra stays up).
set -euo pipefail
cd "$(dirname "$0")"

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

echo "▸ starting outbox relay (producer)…"
mvn -pl producer exec:java -q &
RELAY_PID=$!

echo "▸ starting payment consumer (dashboard on http://127.0.0.1:8090)…"
mvn -pl consumer exec:java -q &
CONSUMER_PID=$!

cleanup() {
  echo "▸ stopping relay ($RELAY_PID) and consumer ($CONSUMER_PID)…"
  kill $RELAY_PID $CONSUMER_PID 2>/dev/null || true
  wait $RELAY_PID $CONSUMER_PID 2>/dev/null || true
  exit 0
}
trap cleanup INT TERM

wait
