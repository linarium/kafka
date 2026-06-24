#!/usr/bin/env python3
"""Читает CDC-события Debezium из топиков users и orders и выводит в stdout."""

from __future__ import annotations

import json
import os
import signal
import sys
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

BOOTSTRAP_SERVERS = os.getenv(
    "BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9093,localhost:9094",
)
TOPICS = [
    "shop.public.users",
    "shop.public.orders",
]
GROUP_ID = os.getenv("GROUP_ID", "debezium-consumer")
POLL_TIMEOUT_SEC = 1.0

_running = True


def _stop(*_args: object) -> None:
    global _running
    _running = False


def _format_event(payload: dict[str, Any]) -> str:
    op = payload.get("op", "?")
    op_labels = {
        "c": "создание",
        "u": "обновление",
        "d": "удаление",
        "r": "чтение",
    }
    op_label = op_labels.get(op, op)
    source = payload.get("source", {})
    table = source.get("table", "unknown")
    before = payload.get("before")
    after = payload.get("after")
    return (
        f"таблица={table} операция={op_label} "
        f"до={json.dumps(before, ensure_ascii=False)} "
        f"после={json.dumps(after, ensure_ascii=False)}"
    )


def main() -> int:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe(TOPICS)
    print(f"Подписка на {TOPICS}, bootstrap={BOOTSTRAP_SERVERS}", flush=True)

    try:
        while _running:
            try:
                msg = consumer.poll(timeout=POLL_TIMEOUT_SEC)
            except KafkaException as exc:
                print(f"ошибка poll: {exc}", file=sys.stderr)
                continue

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"ошибка сообщения: {msg.error()}", file=sys.stderr)
                continue

            try:
                raw = json.loads(msg.value().decode("utf-8"))
                payload = raw.get("payload", raw)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                print(f"ошибка декодирования: {exc}", file=sys.stderr)
                continue

            print(
                f"[{msg.topic()} p={msg.partition()} o={msg.offset()}] "
                f"{_format_event(payload)}",
                flush=True,
            )
    finally:
        consumer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
