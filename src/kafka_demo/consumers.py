from __future__ import annotations

import logging
import threading
import time
from typing import Final

from confluent_kafka import Consumer, KafkaError, KafkaException

from kafka_demo.serializers import deserialize_event

logger: Final = logging.getLogger(__name__)

# Уникальные группы по типу консьюмера (разные группы = одинаковые сообщения для обоих типов).
GROUP_SINGLE: Final = "single-message-group"
GROUP_BATCH: Final = "batch-message-group"


def _hostname_suffix() -> str:
    import os

    return os.getenv("HOSTNAME", "local").replace(".", "-")


def run_single_message_consumer(
    bootstrap_servers: str,
    topic: str,
    stop_event: threading.Event,
) -> None:
    """Одна запись за итерацию: poll → обработка → авто-коммит смещения.

    enable.auto.commit=True: фиксируем offset после успешной обработки согласно auto.commit.interval.ms.
    """
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": GROUP_SINGLE,
        "client.id": f"single-consumer-{_hostname_suffix()}",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 3000,
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    logger.info("SingleMessageConsumer подписан на %s group=%s", topic, GROUP_SINGLE)

    while not stop_event.is_set():
        try:
            msg = consumer.poll(timeout=1.0)
        except KafkaException as exc:
            logger.error("poll single: %s", exc)
            continue
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error("Ошибка сообщения: %s", msg.error())
            continue
        try:
            ev = deserialize_event(msg.value())
        except Exception:
            continue
        print(f"[SingleMessageConsumer] {ev} partition={msg.partition()} offset={msg.offset()}")
        time.sleep(0.01)

    consumer.close()
    logger.info("SingleMessageConsumer остановлен")


def run_batch_message_consumer(
    bootstrap_servers: str,
    topic: str,
    stop_event: threading.Event,
    batch_min: int = 10,
    batch_deadline_sec: float = 3.0,
) -> None:
    """Набираем минимум batch_min сообщений или выходим по таймауту, обрабатываем пачку, затем один коммит.

    fetch.min.bytes - сколько данных должно быть в пачке, чтобы брокер отдал ответ.
    fetch.wait.max.ms - ожидание перед ответом.
    max.poll.interval.ms - сколько ждать между poll при долгой обработке.
    """
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": GROUP_BATCH,
        "client.id": f"batch-consumer-{_hostname_suffix()}",
        "enable.auto.commit": False,
        "enable.auto.offset.store": True,
        "auto.offset.reset": "earliest",
        "fetch.min.bytes": 1,
        "fetch.wait.max.ms": 500,
        "max.partition.fetch.bytes": 2_097_152,
        "max.poll.interval.ms": 300_000,
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    logger.info("BatchMessageConsumer подписан на %s group=%s", topic, GROUP_BATCH)

    while not stop_event.is_set():
        batch: list = []
        deadline = time.monotonic() + batch_deadline_sec
        while len(batch) < batch_min and time.monotonic() < deadline and not stop_event.is_set():
            try:
                msg = consumer.poll(timeout=0.5)
            except KafkaException as exc:
                logger.error("poll batch: %s", exc)
                break
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Ошибка сообщения: %s", msg.error())
                continue
            batch.append(msg)

        if not batch:
            continue

        processed = 0
        for m in batch:
            try:
                ev = deserialize_event(m.value())
            except Exception:
                continue
            processed += 1
            print(f"[BatchMessageConsumer] {ev} partition={m.partition()} offset={m.offset()}")

        if processed == 0:
            continue

        # Один вызов commit: фиксирует все сохранённые смещения
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as exc:
            logger.error("commit batch: %s", exc)

    consumer.close()
    logger.info("BatchMessageConsumer остановлен")
