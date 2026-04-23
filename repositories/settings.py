from __future__ import annotations

import json

from bot.repositories.db import get_db


async def get_site_settings(service: str) -> dict:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM site_settings WHERE service=?",
            (service,),
        )
        if rows:
            return dict(rows[0])
        return {
            "service": service,
            "polling_enabled": 1,
            "poll_interval": 60,
            "active_hour_start": 0,
            "active_hour_end": 24,
        }
    finally:
        await db.close()


async def get_json_schema_state(service: str) -> dict[str, list[str]] | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT schema_json FROM json_schema_state WHERE service = ?",
            (service,),
        )
        if not rows:
            return None
        return json.loads(rows[0]["schema_json"])
    finally:
        await db.close()


async def save_json_schema_state(service: str, schema: dict[str, list[str]]) -> None:
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO json_schema_state (service, schema_json, updated_at)
            VALUES (?, ?, datetime('now'))
            ON CONFLICT(service) DO UPDATE SET
                schema_json = excluded.schema_json,
                updated_at = datetime('now')
            """,
            (service, json.dumps(schema, ensure_ascii=False, sort_keys=True)),
        )
        await db.commit()
    finally:
        await db.close()


async def save_api_change_alert(
    service: str,
    title: str,
    details: str | None = None,
    sample_json: str | None = None,
) -> int:
    db = await get_db()
    try:
        cursor = await db.execute(
            """
            INSERT INTO api_change_alerts (service, title, details, sample_json)
            VALUES (?, ?, ?, ?)
            """,
            (service, title, details, sample_json),
        )
        await db.commit()
        return int(cursor.lastrowid)
    finally:
        await db.close()


async def list_api_change_alerts(limit: int = 50) -> list[dict]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT *
            FROM api_change_alerts
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in rows]
    finally:
        await db.close()


async def get_api_change_alert(alert_id: int) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM api_change_alerts WHERE id = ?",
            (alert_id,),
        )
        return dict(rows[0]) if rows else None
    finally:
        await db.close()


async def delete_api_change_alert(alert_id: int) -> None:
    db = await get_db()
    try:
        await db.execute("DELETE FROM api_change_alerts WHERE id = ?", (alert_id,))
        await db.commit()
    finally:
        await db.close()


async def clear_api_change_alerts() -> None:
    db = await get_db()
    try:
        await db.execute("DELETE FROM api_change_alerts")
        await db.commit()
    finally:
        await db.close()


async def get_all_site_settings() -> list[dict]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT * FROM site_settings
            WHERE service IN ('kapusta', 'finkit', 'zaimis')
            ORDER BY service
            """
        )
        return [dict(row) for row in rows]
    finally:
        await db.close()


async def update_site_setting(service: str, **kwargs) -> None:
    db = await get_db()
    try:
        sets = []
        values = []
        for key, value in kwargs.items():
            if key in ("polling_enabled", "poll_interval", "active_hour_start", "active_hour_end"):
                sets.append(f"{key}=?")
                values.append(value)
        if not sets:
            return
        values.append(service)
        await db.execute(
            f"UPDATE site_settings SET {', '.join(sets)} WHERE service=?",
            values,
        )
        await db.commit()
    finally:
        await db.close()

__all__ = [
    "clear_api_change_alerts",
    "delete_api_change_alert",
    "get_all_site_settings",
    "get_api_change_alert",
    "get_json_schema_state",
    "get_site_settings",
    "list_api_change_alerts",
    "save_api_change_alert",
    "save_json_schema_state",
    "update_site_setting",
]
