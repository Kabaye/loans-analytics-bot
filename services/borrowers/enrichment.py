from __future__ import annotations

from bot.domain.borrowers import BorrowEntry
from bot.repositories.borrowers import lookup_borrower, upsert_borrower


def _apply_cached_borrower(entry: BorrowEntry, cached: dict) -> None:
    if not entry.full_name and cached.get("full_name"):
        entry.full_name = cached["full_name"]
    if not entry.document_id and cached.get("document_id"):
        entry.document_id = cached["document_id"]

    if cached.get("opi_checked_at") and not entry.opi_checked:
        entry.opi_checked = True
        entry.opi_has_debt = bool(cached.get("opi_has_debt"))
        entry.opi_debt_amount = cached.get("opi_debt_amount")
        entry.opi_full_name = cached.get("opi_full_name")
        entry.opi_checked_at = cached.get("opi_checked_at")

    if cached.get("total_loans") and cached["total_loans"] > 0:
        entry.kb_known = True
        entry.kb_total_loans = cached.get("total_loans")
        entry.kb_settled = cached.get("settled_loans")
        entry.kb_overdue = cached.get("overdue_loans")
        entry.kb_avg_rating = cached.get("avg_rating")
        entry.kb_total_invested = cached.get("total_invested")

    if cached.get("loan_status"):
        entry.bi_loan_status = cached["loan_status"]
    if cached.get("sum_category"):
        entry.bi_sum_category = cached["sum_category"]
    if cached.get("bi_rating") is not None:
        entry.bi_rating = cached["bi_rating"]


async def enrich_entry_from_borrowers(entry: BorrowEntry) -> bool:
    if not entry.borrower_user_id:
        return False
    cached = await lookup_borrower(entry.service, entry.borrower_user_id)
    if not cached:
        return False
    _apply_cached_borrower(entry, cached)
    return True


async def enrich_entries_from_borrowers(entries: list[BorrowEntry]) -> None:
    for entry in entries:
        await enrich_entry_from_borrowers(entry)


async def list_borrower_ids_with_documents(service: str, borrower_user_ids: list[str]) -> set[str]:
    result: set[str] = set()
    for borrower_user_id in borrower_user_ids:
        cached = await lookup_borrower(service, borrower_user_id)
        if cached and cached.get("document_id"):
            result.add(borrower_user_id)
    return result


async def enrich_from_borrower_cache(
    entries: list[BorrowEntry],
    *,
    require_document_id: bool = False,
) -> list[BorrowEntry]:
    uncached: list[BorrowEntry] = []
    for entry in entries:
        if not await enrich_entry_from_borrowers(entry):
            uncached.append(entry)
            continue
        if require_document_id and not entry.document_id:
            uncached.append(entry)
    return uncached


def _persistable_name(entry: BorrowEntry) -> str | None:
    return (entry.full_name or entry.display_name or "").strip() or None


async def persist_borrower_entries(entries: list[BorrowEntry], *, source: str) -> None:
    for entry in entries:
        if not entry.borrower_user_id:
            continue
        full_name = _persistable_name(entry)
        if not full_name and not entry.document_id:
            continue
        await upsert_borrower(
            service=entry.service,
            borrower_user_id=entry.borrower_user_id,
            full_name=full_name,
            document_id=entry.document_id,
            source=source,
        )


__all__ = [
    "enrich_entries_from_borrowers",
    "enrich_entry_from_borrowers",
    "enrich_from_borrower_cache",
    "list_borrower_ids_with_documents",
    "persist_borrower_entries",
]
