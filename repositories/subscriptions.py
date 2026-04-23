from __future__ import annotations

from datetime import datetime, timezone

from bot.domain.models import Subscription
from bot.repositories.db import get_db


def _coerce_utc_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _row_value(row, key: str):
    return row[key] if key in row.keys() else None


def _to_subscription(row) -> Subscription:
    return Subscription(
        id=_row_value(row, "id"),
        chat_id=_row_value(row, "chat_id"),
        service=_row_value(row, "service"),
        label=_row_value(row, "label"),
        sum_min=_row_value(row, "sum_min"),
        sum_max=_row_value(row, "sum_max"),
        rating_min=_row_value(row, "rating_min"),
        rating_max=_row_value(row, "rating_max"),
        period_min=_row_value(row, "period_min"),
        period_max=_row_value(row, "period_max"),
        interest_min=_row_value(row, "interest_min"),
        interest_max=_row_value(row, "interest_max"),
        require_employed=bool(_row_value(row, "require_employed")) if _row_value(row, "require_employed") is not None else None,
        require_income_confirmed=bool(_row_value(row, "require_income_confirmed")) if _row_value(row, "require_income_confirmed") is not None else None,
        is_active=bool(_row_value(row, "is_active")) if _row_value(row, "is_active") is not None else True,
        night_paused=bool(_row_value(row, "night_paused")) if _row_value(row, "night_paused") is not None else False,
        min_settled_loans=_row_value(row, "min_settled_loans") if _row_value(row, "min_settled_loans") else None,
        created_at=_coerce_utc_datetime(_row_value(row, "created_at")),
    )


async def list_subscriptions(chat_id: int) -> list[dict]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM subscriptions WHERE chat_id=? ORDER BY service, id",
            (chat_id,),
        )
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def list_subscription_briefs(chat_id: int) -> list[dict]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT id, service, label, is_active, night_paused FROM subscriptions WHERE chat_id=? ORDER BY service, id",
            (chat_id,),
        )
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def get_subscription(subscription_id: int, chat_id: int) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM subscriptions WHERE id=? AND chat_id=?",
            (subscription_id, chat_id),
        )
        return dict(rows[0]) if rows else None
    finally:
        await db.close()


async def create_subscription(chat_id: int, payload: dict) -> None:
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO subscriptions
            (chat_id, service, label, sum_min, sum_max, rating_min,
             period_min, period_max, interest_min,
             require_employed, require_income_confirmed, min_settled_loans)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                chat_id,
                payload["service"],
                payload.get("label"),
                payload.get("sum_min"),
                payload.get("sum_max"),
                payload.get("rating_min"),
                payload.get("period_min"),
                payload.get("period_max"),
                payload.get("interest_min"),
                1 if payload.get("require_employed") else None,
                1 if payload.get("require_income_confirmed") else None,
                payload.get("min_settled_loans"),
            ),
        )
        await db.commit()
    finally:
        await db.close()


async def update_subscription_field(subscription_id: int, chat_id: int, field: str, value) -> None:
    db = await get_db()
    try:
        await db.execute(
            f"UPDATE subscriptions SET {field}=? WHERE id=? AND chat_id=?",
            (value, subscription_id, chat_id),
        )
        await db.commit()
    finally:
        await db.close()


async def toggle_subscription_flag(subscription_id: int, field: str) -> bool:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            f"SELECT {field} FROM subscriptions WHERE id=?",
            (subscription_id,),
        )
        current = rows[0][field] if rows else None
        new_value = None if current else 1
        await db.execute(
            f"UPDATE subscriptions SET {field}=? WHERE id=?",
            (new_value, subscription_id),
        )
        await db.commit()
        return bool(new_value)
    finally:
        await db.close()


async def pause_active_subscriptions_for_night(chat_id: int) -> None:
    db = await get_db()
    try:
        await db.execute(
            "UPDATE subscriptions SET is_active=0, night_paused=1 WHERE chat_id=? AND is_active=1",
            (chat_id,),
        )
        await db.commit()
    finally:
        await db.close()


async def resume_night_paused_subscriptions(chat_id: int) -> None:
    db = await get_db()
    try:
        await db.execute(
            "UPDATE subscriptions SET is_active=1, night_paused=0 WHERE chat_id=? AND night_paused=1",
            (chat_id,),
        )
        await db.commit()
    finally:
        await db.close()


async def delete_subscription(subscription_id: int, chat_id: int) -> None:
    db = await get_db()
    try:
        await db.execute(
            "DELETE FROM subscriptions WHERE id=? AND chat_id=?",
            (subscription_id, chat_id),
        )
        await db.commit()
    finally:
        await db.close()


async def toggle_subscription_active(subscription_id: int, chat_id: int) -> None:
    db = await get_db()
    try:
        await db.execute(
            """
            UPDATE subscriptions
            SET is_active = CASE WHEN is_active=1 THEN 0 ELSE 1 END,
                night_paused = 0
            WHERE id=? AND chat_id=?
            """,
            (subscription_id, chat_id),
        )
        await db.commit()
    finally:
        await db.close()


async def deactivate_all_subscriptions(chat_id: int) -> None:
    db = await get_db()
    try:
        await db.execute(
            "UPDATE subscriptions SET is_active = 0 WHERE chat_id = ?",
            (chat_id,),
        )
        await db.commit()
    finally:
        await db.close()


async def count_active_subscriptions_by_service(chat_id: int) -> list[dict]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT service, COUNT(*) as cnt
            FROM subscriptions
            WHERE chat_id=? AND is_active=1
            GROUP BY service
            ORDER BY service
            """,
            (chat_id,),
        )
        return [dict(row) for row in rows]
    finally:
        await db.close()


async def list_active_subscriptions_for_service(service: str) -> list[tuple[int, Subscription]]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT s.*, u.chat_id as user_chat_id
            FROM subscriptions s
            JOIN users u ON s.chat_id = u.chat_id
            WHERE s.service = ? AND s.is_active = 1 AND u.is_allowed = 1
            ORDER BY s.created_at, s.id
            """,
            (service,),
        )
        return [(row["chat_id"], _to_subscription(row)) for row in rows]
    finally:
        await db.close()


async def has_active_subscriptions_for_service(service: str) -> bool:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT 1
            FROM subscriptions s
            JOIN users u ON s.chat_id = u.chat_id
            WHERE s.service = ? AND s.is_active = 1 AND u.is_allowed = 1
            LIMIT 1
            """,
            (service,),
        )
        return len(rows) > 0
    finally:
        await db.close()


__all__ = [
    "count_active_subscriptions_by_service",
    "create_subscription",
    "deactivate_all_subscriptions",
    "delete_subscription",
    "get_subscription",
    "has_active_subscriptions_for_service",
    "list_active_subscriptions_for_service",
    "list_subscription_briefs",
    "list_subscriptions",
    "pause_active_subscriptions_for_night",
    "resume_night_paused_subscriptions",
    "toggle_subscription_active",
    "toggle_subscription_flag",
    "update_subscription_field",
]
