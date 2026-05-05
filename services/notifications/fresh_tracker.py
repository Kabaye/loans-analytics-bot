from __future__ import annotations

from datetime import datetime, timezone

from bot.domain.borrowers import BorrowEntry
from bot.repositories.notification_watermarks import (
    load_notification_watermark,
    save_notification_watermark,
)
from bot.repositories.seen_entries import (
    bootstrap_seen_entries,
    is_seen_service_initialized,
    load_seen_entry_state,
    sync_seen_entries,
)

_seen_state: dict[str, dict[tuple[str, str], str | None]] = {}
_seen_keys_loaded: dict[str, bool] = {}
_watermark_state: dict[str, tuple[datetime | None, set[str]]] = {}
_watermark_loaded: dict[str, bool] = {}
_TIMESTAMP_WATERMARK_SERVICES = {"finkit", "zaimis"}


def _parse_timestamp(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _timestamp_marker(entry: BorrowEntry) -> str:
    return f"{str(entry.request_type or 'borrow')}:{str(entry.id)}"


def _entry_watermark_timestamp(entry: BorrowEntry, service: str) -> datetime | None:
    if service == "finkit":
        return (
            _parse_timestamp(entry.updated_at)
            or _parse_timestamp((entry.raw_data or {}).get("modified"))
            or _parse_timestamp(entry.created_at)
            or _parse_timestamp((entry.raw_data or {}).get("created"))
        )
    if service == "zaimis":
        return (
            _parse_timestamp(entry.updated_at)
            or _parse_timestamp((entry.raw_data or {}).get("updatedAt"))
            or _parse_timestamp(entry.created_at)
            or _parse_timestamp((entry.raw_data or {}).get("createdAt"))
        )
    return _parse_timestamp(entry.updated_at) or _parse_timestamp(entry.created_at)


async def _compute_fresh_with_watermark(entries: list[BorrowEntry], service: str) -> list[BorrowEntry]:
    if not _watermark_loaded.get(service):
        row = await load_notification_watermark(service)
        _watermark_state[service] = (
            _parse_timestamp((row or {}).get("last_ts")),
            {str(item) for item in ((row or {}).get("ids_at_last_ts") or [])},
        )
        _watermark_loaded[service] = True

    last_ts, ids_at_last_ts = _watermark_state.get(service, (None, set()))
    timestamped_entries: list[tuple[BorrowEntry, datetime, str]] = []
    for entry in entries:
        entry_ts = _entry_watermark_timestamp(entry, service)
        if entry_ts is None:
            continue
        timestamped_entries.append((entry, entry_ts, _timestamp_marker(entry)))

    if last_ts is None:
        if timestamped_entries:
            max_ts = max(entry_ts for _entry, entry_ts, _marker in timestamped_entries)
            ids_at_max_ts = sorted(marker for _entry, entry_ts, marker in timestamped_entries if entry_ts == max_ts)
            await save_notification_watermark(service, max_ts.isoformat(), ids_at_max_ts)
            _watermark_state[service] = (max_ts, set(ids_at_max_ts))
        return []

    fresh: list[BorrowEntry] = []
    for entry, entry_ts, marker in timestamped_entries:
        if entry_ts > last_ts or (entry_ts == last_ts and marker not in ids_at_last_ts):
            fresh.append(entry)

    if timestamped_entries:
        max_ts = max(entry_ts for _entry, entry_ts, _marker in timestamped_entries)
        ids_at_max_ts = sorted(marker for _entry, entry_ts, marker in timestamped_entries if entry_ts == max_ts)
        await save_notification_watermark(service, max_ts.isoformat(), ids_at_max_ts)
        _watermark_state[service] = (max_ts, set(ids_at_max_ts))

    return fresh


async def compute_fresh(entries: list[BorrowEntry], service: str) -> list[BorrowEntry]:
    if service in _TIMESTAMP_WATERMARK_SERVICES:
        return await _compute_fresh_with_watermark(entries, service)

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
