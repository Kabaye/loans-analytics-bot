from __future__ import annotations

from datetime import datetime, timezone

from bot.repositories.borrower_info_sql import _BORROWER_INFO_FULL_NAME_SQL
from bot.repositories.db import get_db


async def get_opi_cache(document_id: str) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT opi_has_debt, opi_debt_amount, opi_checked_at, opi_full_name
            FROM borrower_info
            WHERE document_id = ? AND opi_checked_at IS NOT NULL
            """,
            (document_id,),
        )
        return dict(rows[0]) if rows else None
    finally:
        await db.close()


async def save_opi_result(
    document_id: str,
    has_debt: bool,
    debt_amount: float | None = None,
    full_name: str | None = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    normalized_full_name = (full_name or "").strip() or None
    db = await get_db()
    try:
        existing_rows = await db.execute_fetchall(
            "SELECT document_id FROM borrower_info WHERE document_id = ? LIMIT 1",
            (document_id,),
        )
        if not existing_rows and not normalized_full_name:
            return
        await db.execute(
            f"""
            INSERT INTO borrower_info (document_id, full_name, source, updated_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(document_id) DO UPDATE SET
                full_name = {_BORROWER_INFO_FULL_NAME_SQL},
                updated_at = datetime('now')
            """,
            (document_id, normalized_full_name, "search"),
        )
        await db.execute(
            """
            UPDATE borrower_info
            SET opi_has_debt = ?, opi_debt_amount = ?,
                opi_checked_at = ?, opi_full_name = ?,
                updated_at = datetime('now')
            WHERE document_id = ?
            """,
            (int(has_debt), debt_amount, now, normalized_full_name, document_id),
        )
        await db.commit()
    finally:
        await db.close()


async def get_stale_opi_documents(max_age_days: int = 3) -> list[dict]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT document_id, full_name
            FROM borrower_info
            WHERE document_id IS NOT NULL AND LENGTH(document_id) = 14
              AND (opi_checked_at IS NULL OR opi_checked_at < datetime('now', ?))
            ORDER BY opi_checked_at ASC NULLS FIRST
            """,
            (f"-{max_age_days} days",),
        )
        return [dict(row) for row in rows]
    finally:
        await db.close()


async def get_missing_opi_candidates(min_age_days: int = 10, limit: int = 200) -> list[dict]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT
                b.document_id,
                MAX(bi.full_name) AS full_name,
                GROUP_CONCAT(DISTINCT b.service) AS services,
                MIN(b.first_seen) AS first_seen,
                MAX(b.last_seen) AS last_seen,
                MAX(bi.loan_count) AS total_loans,
                MAX(bi.loan_status) AS loan_status,
                MAX(bi.source) AS source
            FROM borrowers b
            LEFT JOIN borrower_info bi ON bi.document_id = b.document_id
            WHERE b.document_id IS NOT NULL
              AND LENGTH(b.document_id) = 14
              AND (bi.opi_checked_at IS NULL OR bi.opi_checked_at = '')
              AND b.first_seen <= datetime('now', ?)
            GROUP BY b.document_id
            ORDER BY MIN(b.first_seen) ASC, b.document_id ASC
            LIMIT ?
            """,
            (f"-{min_age_days} days", limit),
        )
        return [dict(row) for row in rows]
    finally:
        await db.close()

__all__ = [
    "get_missing_opi_candidates",
    "get_opi_cache",
    "get_stale_opi_documents",
    "save_opi_result",
]
