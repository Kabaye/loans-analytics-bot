from bot.repositories.db import get_db
from bot.repositories.legacy_database import (
    get_borrowers_count,
    get_borrowers_stats,
    lookup_borrower,
    lookup_borrower_info,
    search_borrower_info,
    upsert_borrower,
    upsert_borrower_from_investment,
    upsert_borrower_info,
)


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
