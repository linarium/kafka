#!/usr/bin/env python3
"""Консьюмер orders-events (Yandex Kafka, SASL_SSL)."""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

REPO_ROOT = Path(__file__).resolve().parents[1]
CA_CERT = REPO_ROOT / "yandex" / "certs" / "YandexInternalRootCA.crt"
DEFAULT_BOOTSTRAP = (
    "rc1a-jgsgh146qedrm60e.mdb.yandexcloud.net:9091,"
    "rc1b-g2ihcv06eui2slel.mdb.yandexcloud.net:9091,"
    "rc1d-nid72s2343jq9vsd.mdb.yandexcloud.net:9091"
)


def build_config(group_id: str) -> dict[str, str]:
    return {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP),
        "group.id": group_id,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": os.getenv("KAFKA_USERNAME", ""),
        "sasl.password": os.getenv("KAFKA_PASSWORD", ""),
        "ssl.ca.location": str(CA_CERT),
        "ssl.endpoint.identification.algorithm": "https",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Консьюмер Yandex Kafka")
    parser.add_argument("--topic", default="orders-events")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--group-id", default=os.getenv("KAFKA_CONSUMER_GROUP", "orders-events-consumer"))
    args = parser.parse_args()

    consumer = Consumer(build_config(args.group_id))
    consumer.subscribe([args.topic])
    print(f"Чтение из {args.topic} (group={args.group_id})...")

    deadline = time.monotonic() + args.timeout
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Ошибка: {msg.error()}", file=sys.stderr)
                return 1
            value = msg.value().decode("utf-8") if msg.value() else ""
            key = msg.key().decode("utf-8") if msg.key() else None
            print(
                f"Получено: topic={msg.topic()} partition={msg.partition()} "
                f"offset={msg.offset()} key={key} value={value}"
            )
            return 0
    except KeyboardInterrupt:
        print("\nОстановлено")
    finally:
        consumer.close()

    print("Сообщений не получено.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
