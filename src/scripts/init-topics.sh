#!/bin/bash
# Создаёт топики messages, filtered_messages, blocked_users.

set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka1:29092}"
TOPICS=(
  "messages:3:2"
  "filtered_messages:3:2"
  "blocked_users:3:2"
)

echo "Ожидание Kafka (${BOOTSTRAP})..."
for i in $(seq 1 60); do
  if kafka-broker-api-versions --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; then
    echo "Kafka доступна."
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "Kafka не ответила за 60 попыток." >&2
    exit 1
  fi
  sleep 2
done

for spec in "${TOPICS[@]}"; do
  IFS=":" read -r topic partitions rf <<< "${spec}"
  echo "Создание топика ${topic} (partitions=${partitions}, rf=${rf})..."
  kafka-topics \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${rf}" \
    --bootstrap-server "${BOOTSTRAP}"
done

echo "Список топиков:"
kafka-topics --list --bootstrap-server "${BOOTSTRAP}"
