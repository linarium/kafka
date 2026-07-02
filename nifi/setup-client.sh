#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a
source "$ROOT/.env"
set +a

if [[ -z "${KAFKA_USERNAME:-}" || -z "${KAFKA_PASSWORD:-}" ]]; then
  echo "Заполните KAFKA_USERNAME и KAFKA_PASSWORD в .env" >&2
  exit 1
fi

JAAS=$(printf 'org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";' \
  "$KAFKA_USERNAME" "$KAFKA_PASSWORD")

cat > "$ROOT/client.properties" <<EOF
bootstrap.servers=${KAFKA_BOOTSTRAP_SERVERS}
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=${JAAS}
ssl.truststore.location=/certs/YandexInternalRootCA.crt
ssl.truststore.type=PEM
ssl.endpoint.identification.algorithm=https
EOF

echo "client.properties создан"
