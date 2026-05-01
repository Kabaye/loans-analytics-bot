from __future__ import annotations

from bot.domain.borrowers import BorrowEntry
from bot.repositories.borrowers import lookup_borrower, upsert_borrower


def _apply_cached_borrower(entry: BorrowEntry, cached: dict) -> None:
    if not entry.full_name and cached.get("full_name"):
        entry.full_name = cached["full_name"]
    if not entry.display_name and cached.get("current_display_name"):
        entry.display_name = cached["current_display_name"]
    if cached.get("display_names"):
        entry.display_names = list(cached["display_names"])
    if not entry.document_id and cached.get("document_id"):
        entry.document_id = cached["document_id"]

    if cached.get("opi_checked_at") and not entry.opi_checked:
        entry.opi_checked = True
        entry.opi_has_debt = bool(cached.get("opi_has_debt"))
        entry.opi_debt_amount = cached.get("opi_debt_amount")
        entry.opi_full_name = cached.get("opi_full_name")
        entry.opi_checked_at = cached.get("opi_checked_at")

    known_loans = cached.get("loan_count")
    if known_loans and known_loans > 0:
        entry.kb_known = True
        entry.kb_total_loans = known_loans
        entry.kb_avg_rating = cached.get("bi_rating")

    if entry.loans_count is None and known_loans is not None:
        entry.loans_count = known_loans
    if entry.has_active_loan is None and cached.get("has_active_loan") is not None:
        entry.has_active_loan = bool(cached.get("has_active_loan"))
    if entry.has_overdue is None and cached.get("has_overdue") is not None:
        entry.has_overdue = bool(cached.get("has_overdue"))

    if cached.get("loan_status"):
        entry.bi_loan_status = cached["loan_status"]
    if cached.get("loan_status_details_json"):
        entry.bi_loan_status_details_json = cached["loan_status_details_json"]
    if cached.get("sum_category"):
        entry.bi_sum_category = cached["sum_category"]
    if cached.get("bi_rating") is not None:
        entry.bi_rating = cached["bi_rating"]
    if cached.get("info_source") or cached.get("source"):
        entry.enrichment_source = cached.get("info_source") or cached.get("source")
    if cached.get("source_account_tag"):
        entry.source_account_tag = cached["source_account_tag"]


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
    return (entry.full_name or "").strip() or None


async def persist_borrower_entries(entries: list[BorrowEntry], *, source: str) -> None:
    for entry in entries:
        if not entry.borrower_user_id:
            continue
        full_name = _persistable_name(entry)
        display_name = (entry.display_name or "").strip() or None
        if not full_name and not display_name and not entry.document_id:
            continue
        await upsert_borrower(
            service=entry.service,
            borrower_user_id=entry.borrower_user_id,
            full_name=full_name,
            document_id=entry.document_id,
            source=source,
            display_name=display_name,
            display_names=entry.display_names,
        )


__all__ = [
    "enrich_entries_from_borrowers",
    "enrich_entry_from_borrowers",
    "enrich_from_borrower_cache",
    "list_borrower_ids_with_documents",
    "persist_borrower_entries",
]
