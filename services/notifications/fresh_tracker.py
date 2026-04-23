from __future__ import annotations

from bot.domain.borrowers import BorrowEntry
from bot.repositories.seen_entries import (
    load_seen_entry_ids,
    seed_seen_entry_ids,
    sync_seen_entry_ids,
)

_seen_ids: dict[str, set[str]] = {}
_seen_ids_loaded: dict[str, bool] = {}


async def compute_fresh(entries: list[BorrowEntry], service: str) -> list[BorrowEntry]:
    current_ids = {entry.id for entry in entries}

    if not _seen_ids_loaded.get(service):
        _seen_ids[service] = await load_seen_entry_ids(service)
        _seen_ids_loaded[service] = True
        if not _seen_ids[service]:
            await seed_seen_entry_ids(service, current_ids)
            _seen_ids[service] = current_ids
            return []

    previous_ids = _seen_ids[service]
    fresh = [entry for entry in entries if entry.id not in previous_ids]
    await sync_seen_entry_ids(service, previous_ids, current_ids)
    _seen_ids[service] = current_ids
    return fresh


__all__ = ["compute_fresh"]
