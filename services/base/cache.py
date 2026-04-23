from __future__ import annotations

from datetime import datetime, timezone

VALID_SERVICES = ("kapusta", "finkit", "zaimis")

_cached_loans: dict[str, list[dict]] = {service: [] for service in VALID_SERVICES}
_cached_at: dict[str, str | None] = {service: None for service in VALID_SERVICES}


def set_cached_entries(service: str, entries: list[dict]) -> None:
    _cached_loans[service] = entries
    _cached_at[service] = datetime.now(timezone.utc).isoformat()


def get_cached_entries(service: str) -> list[dict]:
    return list(_cached_loans.get(service, []))


def get_cached_at(service: str) -> str | None:
    return _cached_at.get(service)


def get_cached_snapshot(service: str) -> dict:
    entries = _cached_loans.get(service, [])
    return {
        "entries": list(entries),
        "count": len(entries),
        "cached_at": _cached_at.get(service),
    }


__all__ = [
    "VALID_SERVICES",
    "get_cached_at",
    "get_cached_entries",
    "get_cached_snapshot",
    "set_cached_entries",
]
