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
