from __future__ import annotations

from bot.domain.borrowers import BorrowEntry
from bot.repositories.seen_entries import (
    bootstrap_seen_entries,
    is_seen_service_initialized,
    load_seen_entry_state,
    sync_seen_entries,
)

_seen_state: dict[str, dict[tuple[str, str], str | None]] = {}
_seen_keys_loaded: dict[str, bool] = {}


async def compute_fresh(entries: list[BorrowEntry], service: str) -> list[BorrowEntry]:
    current_state = {
        (str(entry.request_type or "borrow"), str(entry.id)): entry.freshness_fingerprint()
        for entry in entries
    }

    if not _seen_keys_loaded.get(service):
        _seen_state[service] = await load_seen_entry_state(service)
        _seen_keys_loaded[service] = True
        if not await is_seen_service_initialized(service):
            await bootstrap_seen_entries(service, entries)
            _seen_state[service] = current_state
            return []

    previous_state = _seen_state[service]
    fresh = [
        entry
        for entry in entries
        if previous_state.get((str(entry.request_type or "borrow"), str(entry.id))) != entry.freshness_fingerprint()
    ]
    await sync_seen_entries(service, entries)
    _seen_state[service] = current_state
    return fresh


__all__ = ["compute_fresh"]
