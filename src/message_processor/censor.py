from __future__ import annotations

import re
from typing import Iterable


def apply_censorship(text: str, banned_words: Iterable[str], mask_char: str = "*") -> str:
    """Маскирует запрещённые слова в тексте (без учёта регистра, по границам слова)."""
    result = text
    for word in banned_words:
        if not word:
            continue
        pattern = re.compile(rf"\b{re.escape(word)}\b", re.IGNORECASE)
        result = pattern.sub(mask_char * len(word), result)
    return result
