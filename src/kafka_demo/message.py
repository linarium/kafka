from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EventMessage:
    """Прикладное сообщение."""

    id: str
    text: str
    ts: float
