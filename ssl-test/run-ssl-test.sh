#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

docker compose up -d zookeeper kafka1 kafka2 kafka3
sleep 15
docker compose run --rm kafka-ssl-init

cd src && pip install -q -r requirements.txt && cd ..

python3 ssl-test/ssl_producer.py --topic topic-1 --message "test-topic-1"
python3 ssl-test/ssl_consumer.py --topic topic-1 --timeout 5

python3 ssl-test/ssl_producer.py --topic topic-2 --message "test-topic-2"
set +e
python3 ssl-test/ssl_consumer.py --topic topic-2 --timeout 5 --group-id ssl-consumer-group
rc=$?
set -e

if [ "$rc" -eq 0 ]; then
  echo "Ошибка: консьюмер прочитал topic-2" >&2
  exit 1
fi

echo "OK"
