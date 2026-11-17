#!/bin/bash
# Bring up the whole stack: infrastructure + outbox relay + payment consumer.
# Ctrl+C stops the relay and consumer (infra stays up).
set -euo pipefail
cd "$(dirname "$0")"

echo "▸ docker compose up -d (postgres + kafka)"
docker compose up -d
echo "▸ waiting for health…"
for i in $(seq 1 60); do
  pg=$(docker inspect -f '{{.State.Health.Status}}' zero-outbox-postgres 2>/dev/null || echo starting)
  kf=$(docker inspect -f '{{.State.Health.Status}}' zero-outbox-kafka 2>/dev/null || echo starting)
  if [ "$pg" = healthy ] && [ "$kf" = healthy ]; then echo "▸ infra healthy"; break; fi
  sleep 2
done

echo "▸ starting outbox relay (producer)…"
(cd producer && uv run outbox-relay) &
RELAY_PID=$!

echo "▸ starting payment consumer (dashboard on http://127.0.0.1:8090)…"
(cd consumer && uv run --active payment-consumer) &
CONSUMER_PID=$!

cleanup() {
  echo "▸ stopping relay ($RELAY_PID) and consumer ($CONSUMER_PID)…"
  kill $RELAY_PID $CONSUMER_PID 2>/dev/null || true
  wait $RELAY_PID $CONSUMER_PID 2>/dev/null || true
  exit 0
}
trap cleanup INT TERM

wait
