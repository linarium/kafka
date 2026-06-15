#!/usr/bin/env python3
"""Отправляет тестовые данные в топики messages и blocked_users."""

from __future__ import annotations

import json
import os
import sys
import time
import uuid

from confluent_kafka import Producer


def _delivery(err, msg) -> None:
    if err:
        print(f"Ошибка доставки: {err}", file=sys.stderr)
    else:
        print(f"-> {msg.topic()} [{msg.partition()}] {msg.value().decode()}")


def main() -> None:
    bootstrap = os.getenv(
        "BOOTSTRAP_SERVERS",
        "localhost:9092,localhost:9093,localhost:9094",
    )
    producer = Producer({"bootstrap.servers": bootstrap})

    def send(topic: str, payload: dict) -> None:
        producer.produce(
            topic,
            key=payload.get("id") or payload.get("blocker_id") or str(uuid.uuid4()),
            value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            callback=_delivery,
        )

    print("=== 1. Блокировка: alice блокирует bob ===")
    send(
        "blocked_users",
        {"blocker_id": "alice", "blocked_id": "bob", "action": "block"},
    )
    producer.flush()

    time.sleep(5)

    print("\n=== 2. Сообщения в messages ===")
    cases = [
        {
            "id": "msg-1",
            "sender_id": "bob",
            "recipient_id": "alice",
            "text": "Привет, alice!",
            "ts": time.time(),
        },
        {
            "id": "msg-2",
            "sender_id": "charlie",
            "recipient_id": "alice",
            "text": "Это сообщение содержит badword в тексте",
            "ts": time.time(),
        },
        {
            "id": "msg-3",
            "sender_id": "charlie",
            "recipient_id": "dave",
            "text": "Чистое сообщение без запрещённых слов",
            "ts": time.time(),
        },
        {
            "id": "msg-4",
            "sender_id": "dave",
            "recipient_id": "alice",
            "text": "Buy spam products now!",
            "ts": time.time(),
        },
    ]

    for case in cases:
        send("messages", case)

    producer.flush()
    print("\nГотово. Проверьте топик filtered_messages (см. README).")


if __name__ == "__main__":
    main()
