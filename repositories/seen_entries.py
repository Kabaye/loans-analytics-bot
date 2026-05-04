from __future__ import annotations

from bot.domain.borrowers import BorrowEntry
from bot.repositories.db import get_db


def _entry_key(entry: BorrowEntry) -> tuple[str, str]:
    return (str(entry.request_type or "borrow"), str(entry.id))


async def load_seen_entry_keys(service: str) -> set[tuple[str, str]]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT request_type, entry_id FROM seen_entries WHERE service = ?",
            (service,),
        )
        return {
            (str(row["request_type"] or "borrow"), str(row["entry_id"]))
            for row in rows
            if str(row["entry_id"] or "").strip()
        }
    finally:
        await db.close()


async def is_seen_service_initialized(service: str) -> bool:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT service FROM seen_entry_services WHERE service = ? LIMIT 1",
            (service,),
        )
        return bool(rows)
    finally:
        await db.close()


async def bootstrap_seen_entries(service: str, entries: list[BorrowEntry]) -> None:
    db = await get_db()
    try:
        for entry in entries:
            request_type, entry_id = _entry_key(entry)
            await db.execute(
                """
                INSERT INTO seen_entries (
                    service,
                    request_type,
                    entry_id,
                    fingerprint,
                    detected_at,
                    first_seen_at,
                    last_seen_at,
                    last_detected_at,
                    is_active,
                    deactivated_at
                )
                VALUES (?, ?, ?, ?, datetime('now'), datetime('now'), datetime('now'), datetime('now'), 1, NULL)
                ON CONFLICT(service, request_type, entry_id) DO UPDATE SET
                    fingerprint = excluded.fingerprint,
                    last_seen_at = datetime('now'),
                    last_detected_at = datetime('now'),
                    is_active = 1,
                    deactivated_at = NULL
                """,
                (service, request_type, entry_id, entry.freshness_fingerprint()),
            )
        await db.execute(
            """
            INSERT INTO seen_entry_services (service, initialized_at, last_scan_at)
            VALUES (?, datetime('now'), datetime('now'))
            ON CONFLICT(service) DO UPDATE SET
                last_scan_at = datetime('now')
            """,
            (service,),
        )
        await db.commit()
    finally:
        await db.close()


async def sync_seen_entries(service: str, entries: list[BorrowEntry]) -> None:
    current_keys = {_entry_key(entry): entry for entry in entries}
    db = await get_db()
    try:
        active_rows = await db.execute_fetchall(
            "SELECT request_type, entry_id FROM seen_entries WHERE service = ? AND is_active = 1",
            (service,),
        )
        active_keys = {
            (str(row["request_type"] or "borrow"), str(row["entry_id"]))
            for row in active_rows
            if str(row["entry_id"] or "").strip()
        }
        for key, entry in current_keys.items():
            request_type, entry_id = key
            await db.execute(
                """
                INSERT INTO seen_entries (
                    service,
                    request_type,
                    entry_id,
                    fingerprint,
                    detected_at,
                    first_seen_at,
                    last_seen_at,
                    last_detected_at,
                    is_active,
                    deactivated_at
                )
                VALUES (?, ?, ?, ?, datetime('now'), datetime('now'), datetime('now'), datetime('now'), 1, NULL)
                ON CONFLICT(service, request_type, entry_id) DO UPDATE SET
                    fingerprint = excluded.fingerprint,
                    last_seen_at = datetime('now'),
                    last_detected_at = datetime('now'),
                    is_active = 1,
                    deactivated_at = NULL
                """,
                (service, request_type, entry_id, entry.freshness_fingerprint()),
            )
        missing_keys = active_keys - set(current_keys)
        if missing_keys:
            await db.executemany(
                """
                UPDATE seen_entries
                SET is_active = 0,
                    deactivated_at = COALESCE(deactivated_at, datetime('now'))
                WHERE service = ? AND request_type = ? AND entry_id = ?
                """,
                [(service, request_type, entry_id) for request_type, entry_id in missing_keys],
            )
        await db.execute(
            """
            INSERT INTO seen_entry_services (service, initialized_at, last_scan_at)
            VALUES (?, datetime('now'), datetime('now'))
            ON CONFLICT(service) DO UPDATE SET
                last_scan_at = datetime('now')
            """,
            (service,),
        )
        await db.execute(
            """
            DELETE FROM seen_entries
            WHERE service = ?
              AND is_active = 0
              AND COALESCE(deactivated_at, last_seen_at, first_seen_at) < datetime('now', '-90 days')
            """,
            (service,),
        )
        await db.commit()
    finally:
        await db.close()


__all__ = [
    "bootstrap_seen_entries",
    "is_seen_service_initialized",
    "load_seen_entry_keys",
    "sync_seen_entries",
]
