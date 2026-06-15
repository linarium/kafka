from __future__ import annotations

import faust


class ChatMessage(faust.Record, serializer="json"):
    """Входящее или отфильтрованное сообщение чата."""

    id: str
    sender_id: str
    recipient_id: str
    text: str
    ts: float


class BlockEvent(faust.Record, serializer="json"):
    """Событие блокировки или разблокировки пользователя."""

    blocker_id: str
    blocked_id: str
    action: str  # "block" | "unblock"


class WordEvent(faust.Record, serializer="json"):
    """Событие добавления или удаления запрещённого слова."""

    word: str
    action: str  # "add" | "remove"
