# Apache NiFi + Yandex Kafka

## Выполненные шаги

1. NiFi в Docker, UI на порту 8888.
2. Поток `GenerateFlowFile` → `PublishKafka_2_6`.
3. Проверка: `kafka-console-consumer.sh`, `producer.py`, `consumer.py`.

## Запуск

```bash
cd nifi
cp ../yandex/.env .env
./setup-client.sh
./start.sh
```

UI: http://localhost:8888/nifi - `admin` / `adminadminadmin1`

## Настройка flow

Controller Settings → Controller Services → добавить `StandardRestrictedSSLContextService`:

| Свойство            | Значение                         |
|---------------------|----------------------------------|
| Truststore Filename | `/opt/nifi/certs/truststore.jks` |
| Truststore Type     | `JKS`                            |
| Truststore Password | `changeit`                       |

Включить сервис (Enable).

PublishKafka_2_6:

| Свойство              | Значение                                                                                                                                       |
|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| Kafka Brokers         | `rc1a-jgsgh146qedrm60e.mdb.yandexcloud.net:9091,rc1b-g2ihcv06eui2slel.mdb.yandexcloud.net:9091,rc1d-nid72s2343jq9vsd.mdb.yandexcloud.net:9091` |
| Topic Name            | `orders-events`                                                                                                                                |
| Security Protocol     | `SASL_SSL`                                                                                                                                     |
| SASL Mechanism        | `SCRAM-SHA-512`                                                                                                                                |
| Username / Password   | из `.env`                                                                                                                                      |
| SSL Context Service   | StandardRestrictedSSLContextService                                                                                                            |
| Kerberos Service Name | `kafka`                                                                                                                                        |
| Use Transactions      | `false`                                                                                                                                        |
| Delivery Guarantee    | `1`                                                                                                                                            |

Запустить оба процессора.

## Проверка

```bash
docker compose ps
./kafka-console-consumer.sh orders-events
set -a && source .env && set +a
python3 producer.py
python3 consumer.py
```
