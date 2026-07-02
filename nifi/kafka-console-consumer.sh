#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROPS="${ROOT}/client.properties"
TOPIC="${1:-orders-events}"

if [[ ! -f "$PROPS" ]]; then
  echo "Сначала: ./setup-client.sh" >&2
  exit 1
fi

docker run --rm -i \
  -v "${ROOT}/client.properties:/config/client.properties:ro" \
  -v "${ROOT}/../yandex/certs:/certs:ro" \
  confluentinc/cp-kafka:7.7.1 \
  kafka-console-consumer \
    --bootstrap-server "$(grep '^bootstrap.servers=' "$PROPS" | cut -d= -f2-)" \
    --topic "$TOPIC" \
    --from-beginning \
    --consumer.config /config/client.properties \
    --timeout-ms 15000
