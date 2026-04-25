from bot.repositories.borrower_info_sql import (
    _BORROWER_INFO_FULL_NAME_SQL,
    _BORROWER_INFO_SOURCE_SQL,
)
from bot.repositories.db import get_db


async def upsert_borrower(
    service: str,
    borrower_user_id: str,
    full_name: str | None = None,
    document_id: str | None = None,
    source: str | None = None,
) -> None:
    if full_name:
        full_name = full_name.upper()
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO borrowers (service, borrower_user_id, full_name, document_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(service, borrower_user_id) DO UPDATE SET
                full_name = COALESCE(excluded.full_name, borrowers.full_name),
                document_id = COALESCE(excluded.document_id, borrowers.document_id),
                last_seen = datetime('now')
            """,
            (service, borrower_user_id, full_name, document_id),
        )
        if document_id and len(document_id) == 14:
            await db.execute(
                f"""
                INSERT INTO borrower_info (document_id, full_name, source)
                VALUES (?, ?, ?)
                ON CONFLICT(document_id) DO UPDATE SET
                    full_name = {_BORROWER_INFO_FULL_NAME_SQL},
                    source = {_BORROWER_INFO_SOURCE_SQL},
                    updated_at = datetime('now')
                """,
                (document_id, full_name, source or f"{service}_borrow"),
            )
        await db.commit()
    finally:
        await db.close()


async def lookup_borrower(service: str, borrower_user_id: str) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT b.service, b.borrower_user_id, b.document_id, b.full_name,
                   b.total_loans, b.settled_loans, b.overdue_loans,
                   b.avg_rating, b.total_invested,
                   bi.loan_status, bi.sum_category, bi.rating AS bi_rating,
                   bi.notes, bi.last_loan_date, bi.loan_count, bi.source AS info_source,
                   bi.opi_has_debt, bi.opi_debt_amount, bi.opi_checked_at, bi.opi_full_name
            FROM borrowers b
            LEFT JOIN borrower_info bi ON b.document_id = bi.document_id
            WHERE b.service = ? AND b.borrower_user_id = ?
            """,
            (service, borrower_user_id),
        )
        return dict(rows[0]) if rows else None
    finally:
        await db.close()


async def lookup_borrower_info(document_id: str) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM borrower_info WHERE document_id = ?",
            (document_id,),
        )
        return dict(rows[0]) if rows else None
    finally:
        await db.close()


async def search_borrower_info(query: str, limit: int = 10) -> list[dict]:
    db = await get_db()
    try:
        query_value = query.strip()
        query_upper = query_value.upper().replace("Ё", "Е")
        has_digits = any(char.isdigit() for char in query_value)
        if has_digits and len(query_value) >= 7 and query_value.replace(" ", "").isalnum():
            rows = await db.execute_fetchall(
                "SELECT * FROM borrower_info WHERE document_id LIKE ? LIMIT ?",
                (f"%{query_upper}%", limit),
            )
        else:
            rows = await db.execute_fetchall(
                "SELECT * FROM borrower_info WHERE REPLACE(full_name, 'Ё', 'Е') LIKE ? LIMIT ?",
                (f"%{query_upper}%", limit),
            )
            found_docs = {row["document_id"] for row in rows if row["document_id"]}
            extra = await db.execute_fetchall(
                """
                SELECT DISTINCT full_name, document_id, service,
                       total_loans, settled_loans, overdue_loans
                FROM borrowers
                WHERE REPLACE(full_name, 'Ё', 'Е') LIKE ?
                LIMIT ?
                """,
                (f"%{query_upper}%", limit),
            )
            for row in extra:
                document_id = row["document_id"]
                if not document_id or document_id in found_docs:
                    continue
                rows.append(
                    {
                        "document_id": document_id,
                        "full_name": row["full_name"],
                        "loan_status": None,
                        "sum_category": None,
                        "rating": None,
                        "notes": f"из {row['service']}" if row["service"] else None,
                        "last_loan_date": None,
                        "loan_count": row["total_loans"],
                        "source": row["service"],
                        "opi_has_debt": None,
                        "opi_debt_amount": None,
                        "opi_checked_at": None,
                        "opi_full_name": None,
                        "total_invested": None,
                    }
                )
                found_docs.add(document_id)
        return [dict(row) for row in rows[:limit]]
    finally:
        await db.close()


async def upsert_borrower_info(
    document_id: str,
    full_name: str | None = None,
    loan_status: str | None = None,
    sum_category: str | None = None,
    rating: float | None = None,
    notes: str | None = None,
    last_loan_date: str | None = None,
    loan_count: int | None = None,
    total_invested: float | None = None,
    source: str = "added",
) -> None:
    if full_name:
        full_name = full_name.upper()
    db = await get_db()
    try:
        await db.execute(
            f"""
            INSERT INTO borrower_info
                (document_id, full_name, loan_status, sum_category, rating,
                 notes, last_loan_date, loan_count, total_invested, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(document_id) DO UPDATE SET
                full_name = {_BORROWER_INFO_FULL_NAME_SQL},
                loan_status = COALESCE(excluded.loan_status, borrower_info.loan_status),
                sum_category = COALESCE(excluded.sum_category, borrower_info.sum_category),
                rating = COALESCE(excluded.rating, borrower_info.rating),
                notes = COALESCE(excluded.notes, borrower_info.notes),
                last_loan_date = COALESCE(excluded.last_loan_date, borrower_info.last_loan_date),
                loan_count = CASE WHEN excluded.loan_count IS NOT NULL
                                  THEN excluded.loan_count
                                  ELSE borrower_info.loan_count END,
                total_invested = COALESCE(excluded.total_invested, borrower_info.total_invested),
                source = {_BORROWER_INFO_SOURCE_SQL},
                updated_at = datetime('now')
            """,
            (
                document_id,
                full_name,
                loan_status,
                sum_category,
                rating,
                notes,
                last_loan_date,
                loan_count,
                total_invested,
                source,
            ),
        )
        await db.commit()
    finally:
        await db.close()


async def upsert_borrower_from_investment(
    service: str,
    borrower_user_id: str,
    full_name: str | None = None,
    total_loans: int = 0,
    settled_loans: int = 0,
    overdue_loans: int = 0,
    avg_rating: float | None = None,
    total_invested: float | None = None,
) -> None:
    if full_name:
        full_name = full_name.upper()
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO borrowers
                   (service, borrower_user_id, full_name, total_loans, settled_loans,
                    overdue_loans, avg_rating, total_invested)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(service, borrower_user_id) DO UPDATE SET
                   full_name = COALESCE(excluded.full_name, borrowers.full_name),
                   total_loans = excluded.total_loans,
                   settled_loans = excluded.settled_loans,
                   overdue_loans = excluded.overdue_loans,
                   avg_rating = excluded.avg_rating,
                   total_invested = excluded.total_invested,
                   last_seen = datetime('now')
            """,
            (
                service,
                borrower_user_id,
                full_name,
                total_loans,
                settled_loans,
                overdue_loans,
                avg_rating,
                total_invested,
            ),
        )
        await db.commit()
    finally:
        await db.close()


async def get_borrowers_count() -> int:
    db = await get_db()
    try:
        rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM borrower_info")
        return rows[0]["cnt"] if rows else 0
    finally:
        await db.close()


async def get_borrowers_stats() -> dict:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN opi_has_debt = 1 THEN 1 ELSE 0 END) as with_debt,
                   SUM(CASE WHEN opi_has_debt = 0 THEN 1 ELSE 0 END) as no_debt,
                   SUM(CASE WHEN opi_checked_at IS NOT NULL THEN 1 ELSE 0 END) as opi_checked,
                   SUM(CASE WHEN total_invested > 0 THEN 1 ELSE 0 END) as with_investments
            FROM borrower_info
            """
        )
        result = dict(rows[0]) if rows else {}
        rows2 = await db.execute_fetchall(
            """
            SELECT COUNT(*) as mappings,
                   SUM(CASE WHEN document_id IS NOT NULL THEN 1 ELSE 0 END) as with_document
            FROM borrowers
            """
        )
        if rows2:
            result["mappings"] = rows2[0]["mappings"]
            result["with_document"] = rows2[0]["with_document"]
        return result
    finally:
        await db.close()


async def list_borrower_name_map(service: str) -> dict[str, str]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT borrower_user_id, full_name
            FROM borrowers
            WHERE service = ? AND full_name IS NOT NULL AND full_name != ''
            """,
            (service,),
        )
        return {row["full_name"]: row["borrower_user_id"] for row in rows if row["full_name"]}
    finally:
        await db.close()

__all__ = [
    "get_borrowers_count",
    "get_borrowers_stats",
    "list_borrower_name_map",
    "lookup_borrower",
    "lookup_borrower_info",
    "search_borrower_info",
    "upsert_borrower",
    "upsert_borrower_from_investment",
    "upsert_borrower_info",
]
