#!/usr/bin/env bash
# Отправка через SSL (kafka-console-producer в Docker).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOPIC="${1:-topic-1}"
MESSAGE="${2:-hello-ssl-$(date +%s)}"
BOOTSTRAP="${BOOTSTRAP_SERVERS:-host.docker.internal:9092,host.docker.internal:9093,host.docker.internal:9094}"

echo "Отправка в ${TOPIC}: ${MESSAGE}"
docker run --rm \
  -v "${ROOT}/ssl:/ssl" \
  confluentinc/cp-kafka:7.6.1 \
  bash -c "echo '${MESSAGE}' | kafka-console-producer \
    --bootstrap-server '${BOOTSTRAP}' \
    --producer.config /ssl/producer-client.properties \
    --topic '${TOPIC}'"

echo "OK"
