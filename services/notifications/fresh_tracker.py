from __future__ import annotations

from datetime import datetime, timezone

from bot.domain.borrowers import BorrowEntry
from bot.repositories.notification_watermarks import (
    load_notification_watermark,
    save_notification_watermark,
)
_watermark_state: dict[str, datetime | None] = {}
_watermark_loaded: dict[str, bool] = {}
_TIMESTAMP_WATERMARK_SERVICES = {"finkit", "zaimis", "kapusta"}


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
    if service == "kapusta":
        return (
            _parse_timestamp(entry.created_at)
            or _parse_timestamp((entry.raw_data or {}).get("created"))
            or _parse_timestamp((entry.raw_data or {}).get("createdAt"))
            or _parse_timestamp((entry.raw_data or {}).get("created_at"))
        )
    return _parse_timestamp(entry.updated_at) or _parse_timestamp(entry.created_at)


async def _compute_fresh_with_watermark(entries: list[BorrowEntry], service: str) -> list[BorrowEntry]:
    if not _watermark_loaded.get(service):
        row = await load_notification_watermark(service)
        _watermark_state[service] = _parse_timestamp((row or {}).get("last_ts"))
        _watermark_loaded[service] = True

    last_ts = _watermark_state.get(service)
    timestamped_entries: list[tuple[BorrowEntry, datetime]] = []
    for entry in entries:
        entry_ts = _entry_watermark_timestamp(entry, service)
        if entry_ts is None:
            continue
        timestamped_entries.append((entry, entry_ts))

    if last_ts is None:
        if timestamped_entries:
            max_ts = max(entry_ts for _entry, entry_ts in timestamped_entries)
            await save_notification_watermark(service, max_ts.isoformat())
            _watermark_state[service] = max_ts
        return []

    fresh: list[BorrowEntry] = []
    for entry, entry_ts in timestamped_entries:
        if entry_ts > last_ts:
            fresh.append(entry)

    if timestamped_entries:
        max_ts = max(entry_ts for _entry, entry_ts in timestamped_entries)
        await save_notification_watermark(service, max_ts.isoformat())
        _watermark_state[service] = max_ts

    return fresh


async def compute_fresh(entries: list[BorrowEntry], service: str) -> list[BorrowEntry]:
    if service not in _TIMESTAMP_WATERMARK_SERVICES:
        return []
    return await _compute_fresh_with_watermark(entries, service)


__all__ = ["compute_fresh"]
