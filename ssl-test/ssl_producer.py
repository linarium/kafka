#!/usr/bin/env python3
"""SSL-продюсер для topic-1 / topic-2 (сертификат CN=kafka-producer)."""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from confluent_kafka import Producer

ROOT = Path(__file__).resolve().parents[1]
SSL_DIR = ROOT / "ssl"


def build_config() -> dict[str, str]:
    password = os.getenv("SSL_PASSWORD", "mypass123")
    bootstrap = os.getenv(
        "BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"
    )
    client_dir = SSL_DIR / "clients" / "producer"

    return {
        "bootstrap.servers": bootstrap,
        "security.protocol": "SSL",
        "ssl.ca.location": str(SSL_DIR / "ca-cert.pem"),
        "ssl.certificate.location": str(client_dir / "producer-cert.pem"),
        "ssl.key.location": str(client_dir / "producer-key.pem"),
        "ssl.key.password": password,
        "ssl.endpoint.identification.algorithm": "none",
        "acks": "all",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="SSL-продюсер Kafka")
    parser.add_argument("--topic", default="topic-1")
    parser.add_argument("--message", default=f"msg-{int(time.time())}")
    args = parser.parse_args()

    producer = Producer(build_config())

    def on_delivery(err, msg) -> None:
        if err is not None:
            print(f"Ошибка доставки: {err}", file=sys.stderr)
            return
        print(
            f"Доставлено: topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )

    print(f"Отправка в {args.topic}: {args.message}")
    producer.produce(args.topic, value=args.message.encode("utf-8"), callback=on_delivery)
    producer.flush(10)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
