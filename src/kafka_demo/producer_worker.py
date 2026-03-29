from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Final

from confluent_kafka import KafkaError, Producer

from kafka_demo.message import EventMessage
from kafka_demo.serializers import serialize_event

logger: Final = logging.getLogger(__name__)


def _delivery_report(err: KafkaError | None, msg: object | None) -> None:
    """Callback после попытки доставки."""
    if err is not None:
        logger.error("Ошибка доставки: %s", err)
        return
    if msg is None:
        return
    logger.debug("Доставлено: topic=%s partition=%s offset=%s", msg.topic(), msg.partition(), msg.offset())


def run_producer_loop(
    bootstrap_servers: str,
    topic: str,
    stop_event: threading.Event,
    send_interval_sec: float = 1.5,
) -> None:
    """
    acks='all' - ждём подтверждения от всех in-sync реплик.
    retries - повтор при временных сбоях.
    linger.ms - небольшая батчинговая задержка на стороне клиента.
    """
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",
        "retries": 2_147_483_647,
        "enable.idempotence": False,
        "linger.ms": 5,
        "compression.type": "snappy",
    }
    producer = Producer(conf)

    logger.info("Продюсер запущен, topic=%s", topic)

    while not stop_event.is_set():
        ev = EventMessage(
            id=str(uuid.uuid4()),
            text=f"ping-{time.time():.3f}",
            ts=time.time(),
        )
        try:
            raw = serialize_event(ev)
        except Exception:
            time.sleep(send_interval_sec)
            continue

        print(f"Отправка: {ev} bytes={len(raw)}")
        try:
            producer.produce(
                topic,
                key=ev.id.encode("utf-8"),
                value=raw,
                on_delivery=_delivery_report,
            )
        except BufferError:
            logger.warning("Очередь продюсера полна, poll()")
            producer.poll(0.5)
            continue
        except Exception:
            logger.exception("produce()")
            time.sleep(send_interval_sec)
            continue

        # Обязательно дергать poll, чтобы отработали callbacks и сбрасывалась очередь.
        producer.poll(0)

        if stop_event.wait(send_interval_sec):
            break

    producer.flush(timeout=10)
    logger.info("Продюсер остановлен")
