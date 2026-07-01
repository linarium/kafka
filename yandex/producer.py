#!/usr/bin/env python3
"""Продюсер orders-events (Yandex Kafka, SASL_SSL)."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from confluent_kafka import Producer

YANDEX_DIR = Path(__file__).resolve().parent
DEFAULT_BOOTSTRAP = (
    "rc1a-jgsgh146qedrm60e.mdb.yandexcloud.net:9091,"
    "rc1b-g2ihcv06eui2slel.mdb.yandexcloud.net:9091,"
    "rc1d-nid72s2343jq9vsd.mdb.yandexcloud.net:9091"
)


def build_config() -> dict[str, str]:
    return {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": os.getenv("KAFKA_USERNAME", ""),
        "sasl.password": os.getenv("KAFKA_PASSWORD", ""),
        "ssl.ca.location": str(YANDEX_DIR / "certs" / "YandexInternalRootCA.crt"),
        "ssl.endpoint.identification.algorithm": "https",
        "acks": "all",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Продюсер Yandex Kafka")
    parser.add_argument("--topic", default="orders-events")
    parser.add_argument("--message", default="")
    parser.add_argument("--key", default="")
    args = parser.parse_args()

    payload = args.message or json.dumps(
        {"order_id": f"ord-{int(time.time())}", "status": "created", "ts": time.time()},
        ensure_ascii=False,
    )

    producer = Producer(build_config())

    def on_delivery(err, msg) -> None:
        if err is not None:
            print(f"Ошибка доставки: {err}", file=sys.stderr)
            return
        print(
            f"Доставлено: topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )

    key = args.key.encode("utf-8") if args.key else None
    print(f"Отправка в {args.topic}: {payload}")
    producer.produce(args.topic, key=key, value=payload.encode("utf-8"), callback=on_delivery)
    producer.flush(10)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
