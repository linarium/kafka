# Kafka в Yandex Cloud

Кластер `kafka737`, топик `orders-events`, Schema Registry, producer/consumer.

## Краткое описание выполненных шагов

1. **Kafka.** Создан кластер `kafka737` (3 брокера, Kafka 3.9.2, зоны a/b/d). Включён публичный доступ, добавлен пользователь Kafka, скачан CA (`certs/YandexInternalRootCA.crt`), собран `client.properties.example`.

2. **Топик.** Создан `orders-events`: 3 партиции, RF=3, `cleanup.policy=delete`, `segment.bytes=1073741824`, `min.insync.replicas=2`. Проверка: `yc kafka topic get`.

3. **Schema Registry.** Docker на `:8081`, хранение схем в `_schemas` облачного кластера. Зарегистрирован subject `orders-events-value` (Avro, `schemas/order-event.avsc`).

4. **Producer / Consumer.** Скрипты `producer.py`, `consumer.py` (SASL_SSL, SCRAM-SHA-512). Тестовые сообщения отправлены и прочитаны.

## Кластер

| Параметр    | Значение               |
|-------------|------------------------|
| Имя         | `kafka737`             |
| ID          | `c9qstpg586766gg3o8rl` |
| Kafka       | 3.9.2                  |
| Брокеры     | 3 (ru-central1-a/b/d)  |
| Подключение | SASL_SSL, порт 9091    |

Bootstrap:

```text
rc1a-jgsgh146qedrm60e.mdb.yandexcloud.net:9091,
rc1b-g2ihcv06eui2slel.mdb.yandexcloud.net:9091,
rc1d-nid72s2343jq9vsd.mdb.yandexcloud.net:9091
```

### Ресурсы брокера

| vCPU   | RAM   | Диск               |
|--------|-------|--------------------|
| 2      | 8 GB  | network-ssd 100 GB |

## Шаг 1. Развёртывание

```bash
mkdir -p yandex/certs
curl -o yandex/certs/YandexInternalRootCA.crt \
  https://storage.yandexcloud.net/cloud-certs/CA.pem
```

Конфиг клиента: [`client.properties.example`](client.properties.example).

## Шаг 2. Топик orders-events

| Параметр            | Значение   |
|---------------------|------------|
| Партиции            | 3          |
| RF                  | 3          |
| cleanup.policy      | delete     |
| segment.bytes       | 1073741824 |
| min.insync.replicas | 2          |

```bash
yc kafka topic get orders-events --cluster-name kafka737
```

Вывод `yc kafka topic get`:

```text
name: orders-events
cluster_id: c9qstpg586766gg3o8rl
partitions: "3"
replication_factor: "3"
topic_config_3:
  cleanup_policy: CLEANUP_POLICY_DELETE
  min_insync_replicas: "2"
  segment_bytes: "1073741824"
```

## Шаг 3. Schema Registry

```bash
cd yandex
cp .env.example .env
./start-schema-registry.sh
./register-schema.sh
```

Схема: [`schemas/order-event.avsc`](schemas/order-event.avsc), subject `orders-events-value`.

```bash
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/orders-events-value/versions/1
```

## Шаг 4. Producer / Consumer

```bash
pip install confluent-kafka
set -a && source .env && set +a
python3 producer.py --topic orders-events
python3 consumer.py --topic orders-events
```
