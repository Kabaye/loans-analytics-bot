from __future__ import annotations

from typing import Any

from bot.integrations.fsm_guard import drain as _drain_notifications
from bot.integrations.fsm_guard import set_busy as _set_busy
from bot.integrations.fsm_guard import set_free as _set_free


def mark_chat_busy(chat_id: int) -> None:
    _set_busy(chat_id)


def release_chat(chat_id: int) -> None:
    _set_free(chat_id)


def drain_queued_notifications(chat_id: int) -> list[tuple[str, Any]]:
    return _drain_notifications(chat_id)


__all__ = ["drain_queued_notifications", "mark_chat_busy", "release_chat"]
