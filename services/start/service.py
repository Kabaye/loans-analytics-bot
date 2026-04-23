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


def _version_key(value: str) -> tuple[int, ...]:
    parts = []
    for part in value.split("."):
        try:
            parts.append(int(part))
        except ValueError:
            parts.append(0)
    return tuple(parts)


@lru_cache(maxsize=1)
def _load_patch_notes_history() -> list[tuple[str, str]]:
    patch_dir = Path(config.PATCH_NOTES_DIR)
    if not patch_dir.exists():
        return [(config.APP_VERSION, f"Обновление {config.APP_VERSION}")]

    items: list[tuple[str, str]] = []
    for path in patch_dir.glob("*.md"):
        version = path.stem
        try:
            text = path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if text:
            items.append((version, text))

    if not items:
        return [(config.APP_VERSION, f"Обновление {config.APP_VERSION}")]

    items.sort(key=lambda item: _version_key(item[0]))
    return items


async def get_pending_patch_notes(chat_id: int) -> list[str]:
    seen_version = await get_user_seen_version(chat_id)
    current_key = _version_key(config.APP_VERSION)
    seen_key = _version_key(seen_version) if seen_version else None

    pending: list[str] = []
    for version, text in _load_patch_notes_history():
        version_key = _version_key(version)
        if version_key > current_key:
            continue
        if seen_key is not None and version_key <= seen_key:
            continue
        pending.append(text)
    return pending


async def mark_patch_notes_seen(chat_id: int) -> None:
    await set_user_seen_version(chat_id, config.APP_VERSION)


__all__ = ["ensure_chat_user", "get_pending_patch_notes", "mark_patch_notes_seen"]
