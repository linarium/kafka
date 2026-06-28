#!/usr/bin/env python3
"""SSL-консьюмер (сертификат CN=kafka-consumer)."""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

ROOT = Path(__file__).resolve().parents[1]
SSL_DIR = ROOT / "ssl"


def build_config(group_id: str) -> dict[str, str]:
    password = os.getenv("SSL_PASSWORD", "mypass123")
    bootstrap = os.getenv(
        "BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"
    )
    client_dir = SSL_DIR / "clients" / "consumer"

    return {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "security.protocol": "SSL",
        "ssl.ca.location": str(SSL_DIR / "ca-cert.pem"),
        "ssl.certificate.location": str(client_dir / "consumer-cert.pem"),
        "ssl.key.location": str(client_dir / "consumer-key.pem"),
        "ssl.key.password": password,
        "ssl.endpoint.identification.algorithm": "none",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="SSL-консьюмер Kafka")
    parser.add_argument("--topic", default="topic-1")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--group-id", default=os.getenv("SSL_CONSUMER_GROUP", "ssl-consumer-group"))
    args = parser.parse_args()

    consumer = Consumer(build_config(args.group_id))
    consumer.subscribe([args.topic])
    print(f"Чтение из {args.topic} (group={args.group_id})...")

    deadline = time.monotonic() + args.timeout
    received = 0
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Ошибка: {msg.error()}", file=sys.stderr)
                if msg.error().code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
                    return 2
                return 1
            value = msg.value().decode("utf-8")
            print(f"[{msg.topic()} p{msg.partition()} @{msg.offset()}] {value}")
            received += 1
    except Exception as exc:
        print(f"Ошибка консьюмера: {exc}", file=sys.stderr)
        return 1
    finally:
        consumer.close()

    if received == 0:
        print("Сообщений не получено.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
