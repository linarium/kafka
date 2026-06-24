#!/bin/sh
set -e

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
CONNECTOR_NAME="postgres-users-orders-connector"

echo "Ожидание Kafka Connect: ${CONNECT_URL}..."
until curl -sf "${CONNECT_URL}/" >/dev/null; do
  sleep 2
done

if curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}" >/dev/null; then
  echo "Коннектор ${CONNECTOR_NAME} уже зарегистрирован."
  exit 0
fi

echo "Регистрация Debezium-коннектора..."
curl -sf -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d @/config/debezium-postgres-connector.json

echo
echo "Коннектор зарегистрирован."
