#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a
source "$ROOT/.env"
set +a

export SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG=$(
  printf 'org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";' \
    "$KAFKA_USERNAME" "$KAFKA_PASSWORD"
)

cd "$ROOT"
docker compose -f docker-compose.schema-registry.yaml up -d
