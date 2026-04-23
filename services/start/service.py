from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from bot import config
from bot.repositories.users import ensure_user, get_user_seen_version, set_user_seen_version


async def ensure_chat_user(
    chat_id: int,
    *,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
) -> None:
    await ensure_user(
        chat_id,
        username=username,
        first_name=first_name,
        last_name=last_name,
    )


@lru_cache(maxsize=1)
def _load_patch_notes() -> str:
    path = Path(config.PATCH_NOTES_PATH)
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return f"Обновление {config.APP_VERSION}"


async def get_pending_patch_notes(chat_id: int) -> str | None:
    seen_version = await get_user_seen_version(chat_id)
    if seen_version == config.APP_VERSION:
        return None
    return _load_patch_notes()


async def mark_patch_notes_seen(chat_id: int) -> None:
    await set_user_seen_version(chat_id, config.APP_VERSION)


__all__ = ["ensure_chat_user", "get_pending_patch_notes", "mark_patch_notes_seen"]
