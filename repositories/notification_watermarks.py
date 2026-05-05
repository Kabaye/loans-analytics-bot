from __future__ import annotations

from bot.repositories.db import get_db


async def load_notification_watermark(service: str) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT service, last_ts, updated_at
            FROM notification_watermarks
            WHERE service = ?
            LIMIT 1
            """,
            (service,),
        )
        if not rows:
            return None
        return dict(rows[0])
    finally:
        await db.close()


async def save_notification_watermark(service: str, last_ts: str | None) -> None:
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO notification_watermarks (
                service, last_ts, updated_at
            ) VALUES (?, ?, datetime('now'))
            ON CONFLICT(service) DO UPDATE SET
                last_ts = excluded.last_ts,
                updated_at = datetime('now')
            """,
            (service, last_ts),
        )
        await db.commit()
    finally:
        await db.close()


__all__ = [
    "load_notification_watermark",
    "save_notification_watermark",
]
