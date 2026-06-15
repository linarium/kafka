from __future__ import annotations

import logging
import os
import re

import faust

from message_processor.censor import apply_censorship
from message_processor.models import BlockEvent, ChatMessage, WordEvent

logger = logging.getLogger(__name__)

BROKER = os.getenv(
    "FAUST_BROKER",
    "kafka://kafka1:29092;kafka2:29092;kafka3:29092",
)
WEB_PORT = int(os.getenv("FAUST_WEB_PORT", "6066"))

INITIAL_BANNED_WORDS = frozenset(
    token.strip().lower()
    for token in os.getenv("BANNED_WORDS", "spam,badword,offensive").split(",")
    if token.strip()
)

app = faust.App(
    "message-processor",
    broker=BROKER,
    store="rocksdb://",
    version=1,
    topic_partitions=3,
    web_port=WEB_PORT,
)

messages_topic = app.topic("messages", value_type=ChatMessage, partitions=3)
filtered_topic = app.topic("filtered_messages", value_type=ChatMessage, partitions=3)
blocked_topic = app.topic("blocked_users", value_type=BlockEvent, partitions=3)
word_updates_topic = app.topic("banned_word_updates", value_type=WordEvent, partitions=1)

user_blocks: faust.GlobalTable[str, list[str]] = app.GlobalTable(
    "user_blocks",
    default=list,
    partitions=3,
)

banned_words: faust.Table[str, bool] = app.Table(
    "banned_words",
    default=bool,
    partitions=1,
)

_runtime_banned: set[str] = set()
_runtime_blocks: dict[str, list[str]] = {}


@app.task
async def seed_banned_words() -> None:
    for word in INITIAL_BANNED_WORDS:
        await word_updates_topic.send(value=WordEvent(word=word, action="add"))
        logger.info("Начальное слово в очереди: %s", word)


def _banned_words_for_text(text: str) -> set[str]:
    words = set(INITIAL_BANNED_WORDS)
    for token in re.findall(r"\b\w+\b", text, re.IGNORECASE):
        key = token.lower()
        if key in banned_words and banned_words[key]:
            words.add(key)
    return words


@app.agent(word_updates_topic)
async def apply_word_updates(events: faust.StreamT[WordEvent]) -> None:
    async for event in events:
        try:
            word = event.word.strip().lower()
            if not word:
                continue
            if event.action == "add":
                banned_words[word] = True
                _runtime_banned.add(word)
                logger.info("Добавлено запрещённое слово: %s", word)
            elif event.action == "remove":
                if word in banned_words:
                    del banned_words[word]
                _runtime_banned.discard(word)
                logger.info("Удалено запрещённое слово: %s", word)
            else:
                logger.warning("Неизвестное действие для слова: %s", event.action)
        except Exception:
            logger.exception("Ошибка обработки события слова: %s", event.asdict())


@app.agent(blocked_topic)
async def apply_block_events(events: faust.StreamT[BlockEvent]) -> None:
    async for event in events:
        try:
            blocker = event.blocker_id
            blocked = event.blocked_id
            current = _runtime_blocks.get(blocker)
            if current is None:
                current = list(user_blocks[blocker])

            if event.action == "block":
                if blocked not in current:
                    current.append(blocked)
                    logger.info("Пользователь %s заблокировал %s", blocker, blocked)
            elif event.action == "unblock":
                if blocked in current:
                    current.remove(blocked)
                    logger.info("Пользователь %s разблокировал %s", blocker, blocked)
            else:
                logger.warning("Неизвестное действие блокировки: %s", event.action)
                continue

            _runtime_blocks[blocker] = current
            user_blocks[blocker] = current
        except Exception:
            logger.exception("Ошибка обработки блокировки: %s", event.asdict())


@app.agent(messages_topic)
async def process_messages(messages: faust.StreamT[ChatMessage]) -> None:
    async for message in messages:
        try:
            blocked_list = _runtime_blocks.get(message.recipient_id)
            if blocked_list is None:
                blocked_list = list(user_blocks[message.recipient_id])
                _runtime_blocks[message.recipient_id] = blocked_list
            if message.sender_id in blocked_list:
                logger.info(
                    "Сообщение %s отброшено: %s заблокирован у %s",
                    message.id,
                    message.sender_id,
                    message.recipient_id,
                )
                continue

            banned = _banned_words_for_text(message.text)
            censored_text = apply_censorship(message.text, banned)
            out = ChatMessage(
                id=message.id,
                sender_id=message.sender_id,
                recipient_id=message.recipient_id,
                text=censored_text,
                ts=message.ts,
            )
            await filtered_topic.send(value=out)
            if censored_text != message.text:
                logger.info(
                    "Сообщение %s отправлено %s (текст отцензурен)",
                    message.id,
                    message.recipient_id,
                )
            else:
                logger.info(
                    "Сообщение %s отправлено %s",
                    message.id,
                    message.recipient_id,
                )
        except Exception:
            logger.exception("Ошибка обработки сообщения: %s", message.asdict())


@app.page("/banned-words/")
async def list_banned_words(web: faust.Web):
    return web.json({"words": sorted(_runtime_banned)})


@app.page("/banned-words/add/{word}")
async def add_banned_word(web: faust.Web, word: str):
    normalized = word.strip().lower()
    await word_updates_topic.send(value=WordEvent(word=normalized, action="add"))
    return web.json({"status": "ok", "word": normalized})


@app.page("/banned-words/remove/{word}")
async def remove_banned_word(web: faust.Web, word: str):
    normalized = word.strip().lower()
    await word_updates_topic.send(value=WordEvent(word=normalized, action="remove"))
    return web.json({"status": "ok", "word": normalized})
