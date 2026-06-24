# Потоковая обработка сообщений (Kafka + Faust)

Фильтрация по блокировкам пользователей и цензура запрещённых слов. Обработчик - `stream-processor` (Faust).

## Логика

| Компонент                 | Назначение                                                                                                                         |
|---------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| `apply_block_events`      | Читает `blocked_users`, обновляет таблицу `user_blocks`                                                                            |
| `process_messages`        | Читает `messages`, отбрасывает сообщения от заблокированных отправителей, маскирует запрещённые слова, пишет в `filtered_messages` |
| `apply_word_updates`      | Обновляет таблицу `banned_words` (начальный набор - `BANNED_WORDS`, далее HTTP `:6066`)                                            |
| `censor.apply_censorship` | Маскировка слов символом `*`                                                                                                       |

Таблицы Faust (персистентное состояние):

| Таблица        | Ключ         | Значение                           |
|----------------|--------------|------------------------------------|
| `user_blocks`  | `blocker_id` | список заблокированных `sender_id` |
| `banned_words` | слово        | `true`, если слово запрещено       |

Топики: `messages`, `filtered_messages`, `blocked_users` (по 3 партиции, RF=2). Топик `banned_word_updates` создаётся Faust для обновления цензуры.

Модели (`models.py`): `ChatMessage`, `BlockEvent`, `WordEvent`.

## Запуск

```bash
docker compose up -d zookeeper kafka1 kafka2 kafka3 kafka-ui kafka-init stream-processor
docker compose logs -f stream-processor
```

При первом старте подождите ~30–60 с, пока поднимутся брокеры.

## Тестирование

```bash
cd src
pip install -r requirements.txt
export BOOTSTRAP_SERVERS=localhost:9092,localhost:9093,localhost:9094

python scripts/send_test_data.py
python scripts/consume_filtered.py
```

### Тестовые данные

Блокировка (`blocked_users`):

```json
{"blocker_id": "alice", "blocked_id": "bob", "action": "block"}
```

Сообщения (`messages`):

```json
{"id": "msg-1", "sender_id": "bob", "recipient_id": "alice", "text": "Привет, alice!", "ts": 1710000000.0}
{"id": "msg-2", "sender_id": "charlie", "recipient_id": "alice", "text": "Это сообщение содержит badword в тексте", "ts": 1710000001.0}
{"id": "msg-3", "sender_id": "charlie", "recipient_id": "dave", "text": "Чистое сообщение без запрещённых слов", "ts": 1710000002.0}
{"id": "msg-4", "sender_id": "dave", "recipient_id": "alice", "text": "Buy spam products now!", "ts": 1710000003.0}
```

Скрипт `send_test_data.py` отправляет эти данные автоматически (перед сообщениями - блокировку bob у alice, пауза 5 с).

### Ожидаемый результат

| id    | Результат                                            |
|-------|------------------------------------------------------|
| msg-1 | не попадает в `filtered_messages` (bob заблокирован) |
| msg-2 | `badword` → `*******`                                |
| msg-3 | без изменений                                        |
| msg-4 | `spam` → `****`                                      |

Начальный список запрещённых слов: `spam,badword,offensive` (переменная `BANNED_WORDS` в `docker-compose.yaml`).

Добавить слово без перезапуска: `curl http://localhost:6066/banned-words/add/слово`


## Kafka + Debezium + мониторинг

Локальный стенд: PostgreSQL → Debezium (Kafka Connect) → Apache Kafka. 
Метрики Connect собирает Prometheus, графики - в Grafana.

### Компоненты

| Компонент        | Назначение                        | Порт                   |
|------------------|-----------------------------------|------------------------|
| Zookeeper        | координация Kafka                 | `2181`                 |
| Kafka (kafka1–3) | брокеры, топики CDC               | `9092`, `9093`, `9094` |
| PostgreSQL       | таблицы `users`, `orders`         | внутри Docker-сети     |
| Kafka Connect    | Debezium Connector, REST API      | `8083`, метрики `9404` |
| connect-init     | регистрация коннектора при старте | -                      |
| Prometheus       | сбор метрик Connect               | `9090`                 |
| Grafana          | дашборды                          | `3000`                 |
| Kafka UI         | просмотр топиков                  | `8080`                 |

### Запуск

```bash
docker compose up -d --build
```

Проверка:

```bash
docker compose ps
```

Остановка:

```bash
docker compose down
```

Сброс данных PostgreSQL:

```bash
docker compose down -v
```

### Debezium Connector

Файл конфигурации: [`config/debezium-postgres-connector.json`](config/debezium-postgres-connector.json)

| Параметр                      | Значение                     | Описание                               |
|-------------------------------|------------------------------|----------------------------------------|
| `connector.class`             | `PostgresConnector`          | коннектор Debezium для PostgreSQL      |
| `database.hostname`           | `postgres`                   | хост БД в Docker-сети                  |
| `database.dbname`             | `shopdb`                     | имя базы                               |
| `database.user` / `password`  | `debezium` / `dbz`           | пользователь REPLICATION + SELECT      |
| `topic.prefix`                | `shop`                       | префикс топиков                        |
| `table.include.list`          | `public.users,public.orders` | только эти таблицы                     |
| `plugin.name`                 | `pgoutput`                   | logical decoding PostgreSQL            |
| `publication.name`            | `dbz_publication`            | создаётся в `postgres/init.sql`        |
| `publication.autocreate.mode` | `disabled`                   | publication не создаётся автоматически |
| `slot.name`                   | `debezium_slot`              | replication slot                       |

### Тестовые данные

При первом запуске [`postgres/init.sql`](postgres/init.sql) создаёт схему и загружает 4 пользователей (John, Jane, Alice, Bob) и 5 заказов (Product A–E).

Повторная загрузка:

```bash
docker exec -i postgres psql -U postgres -d shopdb < postgres/seed-test-data.sql
```

Новое CDC-событие:

```bash
docker exec postgres psql -U postgres -d shopdb -c \
  "UPDATE users SET email = 'john.updated@example.com' WHERE id = 1;"
```

### Проверка

**Контейнеры:**

```bash
docker ps --filter name=kafka --filter name=postgres --filter name=connect
```

**Статус коннектора:**

```bash
curl -s http://localhost:8083/connectors/postgres-users-orders-connector/status | python3 -m json.tool
```

Ожидается `"state": "RUNNING"`.

**Топики:**

```bash
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 \
  --list | grep shop
```

**Consumer (Python):**

```bash
cd src
pip install -r requirements.txt
python3 debezium_consumer.py
```

Bootstrap: `localhost:9092,localhost:9093,localhost:9094`. Все три брокера должны работать.

Чтобы прочитать топики заново:

```bash
GROUP_ID=debezium-consumer-2 python3 debezium_consumer.py
```