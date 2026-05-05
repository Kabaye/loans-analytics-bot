from __future__ import annotations

import json

from bot.repositories.db import get_db


async def load_notification_watermark(service: str) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT service, last_ts, ids_at_last_ts_json, initialized_at, updated_at
            FROM notification_watermarks
            WHERE service = ?
            LIMIT 1
            """,
            (service,),
        )
        if not rows:
            return None
        row = dict(rows[0])
        try:
            row["ids_at_last_ts"] = json.loads(row.get("ids_at_last_ts_json") or "[]")
        except Exception:
            row["ids_at_last_ts"] = []
        return row
    finally:
        await db.close()


async def save_notification_watermark(service: str, last_ts: str | None, ids_at_last_ts: list[str]) -> None:
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO notification_watermarks (
                service, last_ts, ids_at_last_ts_json, initialized_at, updated_at
            ) VALUES (?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(service) DO UPDATE SET
                last_ts = excluded.last_ts,
                ids_at_last_ts_json = excluded.ids_at_last_ts_json,
                updated_at = datetime('now')
            """,
            (service, last_ts, json.dumps(ids_at_last_ts, ensure_ascii=False)),
        )
        await db.commit()
    finally:
        await db.close()


__all__ = [
    "load_notification_watermark",
    "save_notification_watermark",
]
