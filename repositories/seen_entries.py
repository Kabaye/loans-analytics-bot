from bot.repositories.db import get_db


async def load_seen_entry_ids(service: str) -> set[str]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT entry_id FROM seen_entries WHERE service = ?",
            (service,),
        )
        return {row["entry_id"] for row in rows}
    finally:
        await db.close()


async def seed_seen_entry_ids(service: str, entry_ids: set[str]) -> None:
    if not entry_ids:
        return
    db = await get_db()
    try:
        await db.executemany(
            "INSERT OR IGNORE INTO seen_entries (service, entry_id) VALUES (?, ?)",
            [(service, entry_id) for entry_id in entry_ids],
        )
        await db.commit()
    finally:
        await db.close()


async def sync_seen_entry_ids(service: str, previous_ids: set[str], current_ids: set[str]) -> None:
    new_ids = current_ids - previous_ids
    gone_ids = previous_ids - current_ids
    if not new_ids and not gone_ids:
        return
    db = await get_db()
    try:
        if new_ids:
            await db.executemany(
                "INSERT OR IGNORE INTO seen_entries (service, entry_id) VALUES (?, ?)",
                [(service, entry_id) for entry_id in new_ids],
            )
        if gone_ids:
            await db.executemany(
                "DELETE FROM seen_entries WHERE service = ? AND entry_id = ?",
                [(service, entry_id) for entry_id in gone_ids],
            )
        await db.execute(
            "DELETE FROM seen_entries WHERE service = ? AND first_seen < datetime('now', '-7 days')",
            (service,),
        )
        await db.commit()
    finally:
        await db.close()


__all__ = [
    "load_seen_entry_ids",
    "seed_seen_entry_ids",
    "sync_seen_entry_ids",
]
