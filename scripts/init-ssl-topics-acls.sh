#!/usr/bin/env bash
# Создание topic-1, topic-2 и ACL для SSL-кластера.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka1:29092}"
CONSUMER_GROUP="${SSL_CONSUMER_GROUP:-ssl-consumer-group}"

echo "Ожидание Kafka (${BOOTSTRAP})..."
for i in $(seq 1 90); do
  if kafka-broker-api-versions --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; then
    echo "Kafka доступна."
    break
  fi
  if [ "$i" -eq 90 ]; then
    echo "Kafka не ответила за 90 попыток." >&2
    exit 1
  fi
  sleep 2
done

create_topic() {
  local topic="$1"
  local partitions="${2:-3}"
  local rf="${3:-2}"
  echo "Создание топика ${topic}..."
  kafka-topics \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${rf}" \
    --bootstrap-server "${BOOTSTRAP}"
}

create_topic "topic-1"
create_topic "topic-2"

echo "Настройка ACL..."

# topic-1: продюсер и консьюмер
kafka-acls \
  --bootstrap-server "${BOOTSTRAP}" \
  --add \
  --allow-principal User:kafka-producer \
  --operation Write \
  --operation Describe \
  --topic topic-1

kafka-acls \
  --bootstrap-server "${BOOTSTRAP}" \
  --add \
  --allow-principal User:kafka-consumer \
  --operation Read \
  --operation Describe \
  --topic topic-1

kafka-acls \
  --bootstrap-server "${BOOTSTRAP}" \
  --add \
  --allow-principal User:kafka-consumer \
  --operation Read \
  --group "${CONSUMER_GROUP}"

# topic-2: только запись для продюсера
kafka-acls \
  --bootstrap-server "${BOOTSTRAP}" \
  --add \
  --allow-principal User:kafka-producer \
  --operation Write \
  --operation Describe \
  --topic topic-2

echo "Список топиков:"
kafka-topics --list --bootstrap-server "${BOOTSTRAP}"

echo "Список ACL:"
kafka-acls --bootstrap-server "${BOOTSTRAP}" --list

echo "Готово"
