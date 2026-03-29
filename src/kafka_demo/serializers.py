from __future__ import annotations

import json
import logging
from typing import Final

from kafka_demo.message import EventMessage

logger: Final = logging.getLogger(__name__)


def serialize_event(msg: EventMessage) -> bytes:
    """Преобразует EventMessage в байты для значений продюсера."""
    try:
        payload = {"id": msg.id, "text": msg.text, "ts": msg.ts}
        return json.dumps(payload, ensure_ascii=False).encode("utf-8")
    except Exception as e:
        text = f"Не удалось сериализовать сообщение: {e}"
        logger.exception(text)
        print(text)
        raise


def deserialize_event(raw: bytes | None) -> EventMessage:
    """Восстанавливает EventMessage из payload консьюмера."""
    try:
        if raw is None:
            raise ValueError("value is None")
        data = json.loads(raw.decode("utf-8"))
        return EventMessage(
            id=str(data["id"]),
            text=str(data["text"]),
            ts=float(data["ts"]),
        )
    except Exception as e:
        text = f"не удалось разобрать сообщение: {e}"
        logger.exception(text)
        print(text)
        raise
