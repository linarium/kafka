#!/usr/bin/env python3
"""Читает filtered_messages и печатает результат обработки."""

from __future__ import annotations

import json
import os
import sys
import time

from confluent_kafka import Consumer, KafkaException


def main() -> None:
    bootstrap = os.getenv(
        "BOOTSTRAP_SERVERS",
        "localhost:9092,localhost:9093,localhost:9094",
    )
    timeout = float(os.getenv("CONSUME_TIMEOUT_SEC", "15"))

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"filtered-check-{os.getpid()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(["filtered_messages"])

    print(f"Чтение filtered_messages, таймаут {timeout} с...")
    deadline = time.time() + timeout
    count = 0

    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            data = json.loads(msg.value().decode("utf-8"))
            count += 1
            print(json.dumps(data, ensure_ascii=False, indent=2))
    finally:
        consumer.close()

    if count == 0:
        print("Сообщений нет. Запущен ли stream-processor?", file=sys.stderr)
        sys.exit(1)

    print(f"\nПолучено сообщений: {count}")


if __name__ == "__main__":
    main()
