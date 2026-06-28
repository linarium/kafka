#!/usr/bin/env bash
# Чтение через SSL (kafka-console-consumer в Docker).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOPIC="${1:-topic-1}"
TIMEOUT_MS="${2:-10000}"
BOOTSTRAP="${BOOTSTRAP_SERVERS:-host.docker.internal:9092,host.docker.internal:9093,host.docker.internal:9094}"
GROUP="${SSL_CONSUMER_GROUP:-ssl-consumer-group}"

echo "Чтение из ${TOPIC} (group=${GROUP}, timeout=${TIMEOUT_MS}ms)..."
docker run --rm \
  -v "${ROOT}/ssl:/ssl" \
  confluentinc/cp-kafka:7.6.1 \
  kafka-console-consumer \
  --bootstrap-server "${BOOTSTRAP}" \
  --consumer.config /ssl/consumer-client.properties \
  --topic "${TOPIC}" \
  --group "${GROUP}" \
  --from-beginning \
  --timeout-ms "${TIMEOUT_MS}"
