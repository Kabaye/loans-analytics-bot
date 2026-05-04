from __future__ import annotations

from bot.domain.borrowers import BorrowEntry
from bot.repositories.seen_entries import (
    bootstrap_seen_entries,
    is_seen_service_initialized,
    load_seen_entry_keys,
    sync_seen_entries,
)

_seen_keys: dict[str, set[tuple[str, str]]] = {}
_seen_keys_loaded: dict[str, bool] = {}


async def compute_fresh(entries: list[BorrowEntry], service: str) -> list[BorrowEntry]:
    current_keys = {(str(entry.request_type or "borrow"), str(entry.id)) for entry in entries}

    if not _seen_keys_loaded.get(service):
        _seen_keys[service] = await load_seen_entry_keys(service)
        _seen_keys_loaded[service] = True
        if not await is_seen_service_initialized(service):
            await bootstrap_seen_entries(service, entries)
            _seen_keys[service] = current_keys
            return []

    previous_keys = _seen_keys[service]
    fresh = [entry for entry in entries if (str(entry.request_type or "borrow"), str(entry.id)) not in previous_keys]
    await sync_seen_entries(service, entries)
    _seen_keys[service] = current_keys
    return fresh


__all__ = ["compute_fresh"]
