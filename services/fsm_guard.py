"""Track active FSM sessions and queue notifications for busy users."""
from __future__ import annotations

from typing import Any

_busy: set[int] = set()
_queue: dict[int, list[tuple[str, Any]]] = {}  # chat_id -> [(html_text, reply_markup), ...]


def set_busy(chat_id: int) -> None:
    _busy.add(chat_id)


def set_free(chat_id: int) -> None:
    _busy.discard(chat_id)


def is_busy(chat_id: int) -> bool:
    return chat_id in _busy


def enqueue(chat_id: int, text: str, reply_markup: Any = None) -> None:
    _queue.setdefault(chat_id, []).append((text, reply_markup))


def drain(chat_id: int) -> list[tuple[str, Any]]:
    """Return and clear queued notifications for chat_id.
    Each item is (html_text, reply_markup_or_None)."""
    return _queue.pop(chat_id, [])
