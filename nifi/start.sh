#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

CA="${ROOT}/../yandex/certs/YandexInternalRootCA.crt"
JKS="${ROOT}/yandex-truststore.jks"
PASS="${NIFI_TRUSTSTORE_PASSWORD:-changeit}"

if [[ ! -f "$CA" ]]; then
  echo "Не найден CA: $CA" >&2
  exit 1
fi

if [[ ! -f "$JKS" ]]; then
  if command -v keytool >/dev/null 2>&1; then
    keytool -importcert -noprompt -alias yandexca \
      -file "$CA" -keystore "$JKS" -storepass "$PASS"
  else
    docker run --rm \
      -v "${CA}:/tmp/ca.crt:ro" \
      -v "${ROOT}:/out" \
      eclipse-temurin:11-jre \
      keytool -importcert -noprompt -alias yandexca \
        -file /tmp/ca.crt \
        -keystore /out/yandex-truststore.jks \
        -storepass "$PASS"
  fi
fi

docker compose up -d
echo "NiFi: http://localhost:8888/nifi"
echo "Логин: admin / adminadminadmin1"
