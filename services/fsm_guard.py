"""Track active FSM sessions and queue notifications for busy users."""
from __future__ import annotations

_busy: set[int] = set()
_queue: dict[int, list[str]] = {}  # chat_id -> [html_text, ...]


def set_busy(chat_id: int) -> None:
    _busy.add(chat_id)


def set_free(chat_id: int) -> None:
    _busy.discard(chat_id)


def is_busy(chat_id: int) -> bool:
    return chat_id in _busy


def enqueue(chat_id: int, text: str) -> None:
    _queue.setdefault(chat_id, []).append(text)


def drain(chat_id: int) -> list[str]:
    """Return and clear queued notifications for chat_id."""
    return _queue.pop(chat_id, [])
