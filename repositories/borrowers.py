from __future__ import annotations

import json
import re
from typing import Iterable

from bot.repositories.db import get_db

_NAME_MASK_CHAR = "*"
_LEGAL_FULL_NAME_RE = re.compile(r"^[A-ZА-ЯЁ]+(?:[-'][A-ZА-ЯЁ]+)?(?:\s+[A-ZА-ЯЁ]+(?:[-'][A-ZА-ЯЁ]+)?){2,}$")

_SOURCE_PRIORITY = {
    "search": 10,
    "manual": 15,
    "sheets": 20,
    "finkit_borrow": 30,
    "zaimis_borrow": 30,
    "kapusta_borrow": 30,
    "finkit_investment_detail": 40,
    "zaimis_investment_detail": 40,
}

_CONTACT_SOURCE_PRIORITY = {
    "finkit_investment_detail": 10,
    "zaimis_investment_detail": 10,
    "manual": 20,
}


def _normalize_upper(value: str | None) -> str | None:
    text = (value or "").strip()
    return text.upper() if text else None


def _normalize_compare(value: str | None) -> str:
    return (value or "").strip().upper().replace("Ё", "Е")


def _is_probable_legal_full_name(value: str | None) -> bool:
    normalized = _normalize_upper(value)
    if not normalized or any(char.isdigit() for char in normalized):
        return False
    return bool(_LEGAL_FULL_NAME_RE.fullmatch(normalized))


def _merge_full_name(existing: str | None, incoming: str | None) -> str | None:
    current = _normalize_upper(existing)
    new_value = _normalize_upper(incoming)
    if not new_value:
        return current
    if not current:
        return new_value
    if _NAME_MASK_CHAR in new_value and _NAME_MASK_CHAR not in current:
        return current
    return new_value


def _parse_display_names(value: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        raw_items = list(value)
    else:
        text = str(value).strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            raw_items = [text]
        else:
            raw_items = parsed if isinstance(parsed, list) else [text]
    result: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        text = str(item or "").strip()
        key = _normalize_compare(text)
        if not text or not key or key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


def _merge_display_names(
    existing: str | list[str] | tuple[str, ...] | None,
    *incoming_values: str | list[str] | tuple[str, ...] | None,
) -> list[str]:
    merged = _parse_display_names(existing)
    seen = {_normalize_compare(item) for item in merged}
    for incoming in incoming_values:
        for item in _parse_display_names(incoming):
            key = _normalize_compare(item)
            if key in seen:
                merged = [current for current in merged if _normalize_compare(current) != key]
            else:
                seen.add(key)
            merged.append(item)
    return merged


def _current_display_name(display_names: str | list[str] | tuple[str, ...] | None) -> str | None:
    items = _parse_display_names(display_names)
    return items[-1] if items else None


def _serialize_display_names(display_names: list[str]) -> str | None:
    return json.dumps(display_names, ensure_ascii=False) if display_names else None


def _extract_account_tag(value: str, prefix: str) -> str | None:
    if not value.startswith(prefix):
        return None
    suffix = value[len(prefix):].strip("_")
    return suffix or None


def _normalize_source(source: str | None) -> tuple[str | None, str | None]:
    value = str(source or "").strip()
    if not value:
        return None, None
    if value in {"added", "opi"}:
        return "search", None
    if value == "manual":
        return "manual", None
    if value == "sheets":
        return "sheets", None
    if value in {"finkit", "zaimis", "kapusta"}:
        return value, None
    if value.startswith("finkit_investment_detail"):
        return "finkit_investment_detail", _extract_account_tag(value, "finkit_investment_detail_")
    if value.startswith("zaimis_investment_detail"):
        return "zaimis_investment_detail", _extract_account_tag(value, "zaimis_investment_detail_")
    if value.startswith("finkit_claim_pdf"):
        return "finkit_investment_detail", _extract_account_tag(value, "finkit_claim_pdf_")
    if value.startswith("finkit_archive_"):
        return "finkit_borrow", _extract_account_tag(value, "finkit_archive_")
    if value.startswith("zaimis_archive_"):
        return "zaimis_borrow", _extract_account_tag(value, "zaimis_archive_")
    if value.startswith("kapusta_archive_"):
        return "kapusta_borrow", _extract_account_tag(value, "kapusta_archive_")
    if value.startswith("finkit_overdue_pdf_"):
        return "finkit_investment_detail", _extract_account_tag(value, "finkit_overdue_pdf_")
    if value == "finkit_name_match":
        return "finkit_borrow", None
    if value.endswith("_borrow"):
        return value, None
    if value.startswith("finkit_"):
        return "finkit_borrow", None
    if value.startswith("zaimis_"):
        return "zaimis_borrow", None
    if value.startswith("kapusta_"):
        return "kapusta_borrow", None
    return value, None


def _normalize_contact_source(source: str | None) -> tuple[str | None, str | None]:
    value = str(source or "").strip()
    if not value:
        return None, None
    if value == "manual":
        return "manual", None
    if value.startswith("finkit_investment_detail"):
        return "finkit_investment_detail", _extract_account_tag(value, "finkit_investment_detail_")
    if value.startswith("finkit_claim_pdf"):
        return "finkit_investment_detail", _extract_account_tag(value, "finkit_claim_pdf_")
    if value.startswith("finkit_overdue_pdf_"):
        return "finkit_investment_detail", _extract_account_tag(value, "finkit_overdue_pdf_")
    if value.startswith("zaimis_investment_detail"):
        return "zaimis_investment_detail", _extract_account_tag(value, "zaimis_investment_detail_")
    return value, None


def _pick_source(existing: str | None, incoming: str | None, priorities: dict[str, int]) -> str | None:
    current = str(existing or "").strip() or None
    new_value = str(incoming or "").strip() or None
    if not new_value:
        return current
    if not current:
        return new_value
    if priorities.get(new_value, 0) >= priorities.get(current, 0):
        return new_value
    return current


def _merge_account_tag(
    existing_tag: str | None,
    chosen_source: str | None,
    incoming_source: str | None,
    incoming_tag: str | None,
) -> str | None:
    if chosen_source and chosen_source == incoming_source and incoming_tag:
        return incoming_tag
    return (existing_tag or "").strip() or None


def _serialize_status_details(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return json.dumps(value, ensure_ascii=False)


async def _fetch_borrower_row(db, service: str, borrower_user_id: str) -> dict | None:
    rows = await db.execute_fetchall(
        """
        SELECT *
        FROM borrowers
        WHERE service = ? AND borrower_user_id = ?
        LIMIT 1
        """,
        (service, borrower_user_id),
    )
    return dict(rows[0]) if rows else None


async def _fetch_borrower_info_row(db, document_id: str) -> dict | None:
    rows = await db.execute_fetchall(
        """
        SELECT *
        FROM borrower_info
        WHERE document_id = ?
        LIMIT 1
        """,
        (document_id,),
    )
    return dict(rows[0]) if rows else None


async def _collect_display_names_by_document(db, document_id: str) -> list[str]:
    rows = await db.execute_fetchall(
        """
        SELECT display_names
        FROM borrowers
        WHERE document_id = ?
        ORDER BY first_seen ASC, last_seen ASC, id ASC
        """,
        (document_id,),
    )
    merged: list[str] = []
    for row in rows:
        merged = _merge_display_names(merged, row["display_names"])
    return merged


def _status_from_overdue_days(days: int | None) -> str:
    if days is None:
        return "просрочка"
    if days <= 5:
        return "просрочка до 5 дней"
    if days <= 30:
        return "просрочка 6-30 дней"
    return "просрочка > 30 дней"


async def _derive_status_payload(db, document_id: str) -> tuple[str | None, str | None]:
    overdue_rows = await db.execute_fetchall(
        """
        SELECT COALESCE(days_overdue, 0) AS days_overdue
        FROM overdue_cases
        WHERE document_id = ?
          AND is_active = 1
        ORDER BY COALESCE(days_overdue, 0) DESC, updated_at DESC, id DESC
        """,
        (document_id,),
    )
    stats_rows = await db.execute_fetchall(
        """
        SELECT
            COALESCE(SUM(total_loans), 0) AS total_loans,
            COALESCE(SUM(settled_loans), 0) AS settled_loans,
            COALESCE(SUM(overdue_loans), 0) AS overdue_loans
        FROM borrowers
        WHERE document_id = ?
        """,
        (document_id,),
    )
    stats = dict(stats_rows[0]) if stats_rows else {"total_loans": 0, "settled_loans": 0, "overdue_loans": 0}
    total_loans = int(stats.get("total_loans") or 0)
    settled_loans = int(stats.get("settled_loans") or 0)
    overdue_loans = int(stats.get("overdue_loans") or 0)
    active_loans = max(total_loans - settled_loans - overdue_loans, 0)
    details: list[str] = []

    if overdue_rows:
        max_days = int(overdue_rows[0]["days_overdue"] or 0)
        details.append(f"активная просрочка {max_days} д.")
        if overdue_loans > 0:
            details.append("были просрочки в истории")
        if active_loans > 0:
            details.append("есть текущий займ")
        return _status_from_overdue_days(max_days), _serialize_status_details(details)

    if overdue_loans > 0:
        details.append("были просрочки в истории")
        if active_loans > 0:
            details.append("есть текущий займ")
            return "текущий", _serialize_status_details(details)
        return "закрыт, были просрочки", _serialize_status_details(details)

    if active_loans > 0:
        details.append("есть текущий займ")
        return "текущий", _serialize_status_details(details)

    if total_loans > 0:
        details.append("закрыт без просрочек")
        return "в срок", _serialize_status_details(details)

    return None, None


async def _refresh_borrower_status(db, document_id: str) -> None:
    if not document_id:
        return
    loan_status, details_json = await _derive_status_payload(db, document_id)
    if loan_status is None and details_json is None:
        return
    await db.execute(
        """
        INSERT INTO borrower_info (document_id, loan_status, loan_status_details_json, updated_at)
        VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(document_id) DO UPDATE SET
            loan_status = COALESCE(excluded.loan_status, borrower_info.loan_status),
            loan_status_details_json = COALESCE(excluded.loan_status_details_json, borrower_info.loan_status_details_json),
            updated_at = datetime('now')
        """,
        (document_id, loan_status, details_json),
    )


async def upsert_borrower(
    service: str,
    borrower_user_id: str,
    full_name: str | None = None,
    document_id: str | None = None,
    source: str | None = None,
    *,
    display_name: str | None = None,
    display_names: list[str] | tuple[str, ...] | None = None,
    source_account_tag: str | None = None,
) -> None:
    normalized_full_name = _normalize_upper(full_name)
    current_display = (display_name or "").strip() or None
    db = await get_db()
    try:
        existing = await _fetch_borrower_row(db, service, borrower_user_id)
        merged_display_names = _merge_display_names(
            (existing or {}).get("display_names"),
            display_names,
            current_display,
        )
        if service == "zaimis" and normalized_full_name and not _is_probable_legal_full_name(normalized_full_name):
            merged_display_names = _merge_display_names(merged_display_names, normalized_full_name)
            normalized_full_name = None

        merged_full_name = _merge_full_name((existing or {}).get("full_name"), normalized_full_name)
        merged_document_id = document_id or (existing or {}).get("document_id")

        await db.execute(
            """
            INSERT INTO borrowers (service, borrower_user_id, full_name, display_names, document_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(service, borrower_user_id) DO UPDATE SET
                full_name = ?,
                display_names = ?,
                document_id = COALESCE(excluded.document_id, borrowers.document_id),
                last_seen = datetime('now')
            """,
            (
                service,
                borrower_user_id,
                merged_full_name,
                _serialize_display_names(merged_display_names),
                merged_document_id,
                merged_full_name,
                _serialize_display_names(merged_display_names),
            ),
        )

        if merged_document_id and len(merged_document_id) == 14:
            existing_info = await _fetch_borrower_info_row(db, merged_document_id)
            normalized_source, normalized_tag = _normalize_source(source)
            chosen_source = _pick_source((existing_info or {}).get("source"), normalized_source, _SOURCE_PRIORITY)
            chosen_tag = _merge_account_tag(
                (existing_info or {}).get("source_account_tag"),
                chosen_source,
                normalized_source,
                source_account_tag or normalized_tag,
            )
            info_full_name = _merge_full_name((existing_info or {}).get("full_name"), merged_full_name)
            await db.execute(
                """
                INSERT INTO borrower_info (document_id, full_name, source, source_account_tag, updated_at)
                VALUES (?, ?, ?, ?, datetime('now'))
                ON CONFLICT(document_id) DO UPDATE SET
                    full_name = ?,
                    source = ?,
                    source_account_tag = ?,
                    updated_at = datetime('now')
                """,
                (
                    merged_document_id,
                    info_full_name,
                    chosen_source,
                    chosen_tag,
                    info_full_name,
                    chosen_source,
                    chosen_tag,
                ),
            )
            await _refresh_borrower_status(db, merged_document_id)
        await db.commit()
    finally:
        await db.close()


async def lookup_borrower(service: str, borrower_user_id: str) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT b.service, b.borrower_user_id, b.document_id, b.full_name, b.display_names,
                   b.total_loans, b.settled_loans, b.overdue_loans,
                   b.avg_rating, b.total_invested,
                   bi.loan_status, bi.loan_status_details_json,
                   bi.sum_category, bi.rating AS bi_rating,
                   bi.notes, bi.last_loan_date, bi.loan_count, bi.source AS info_source,
                   bi.contact_source, bi.source_account_tag,
                   bi.borrower_address, bi.borrower_zip,
                   bi.opi_has_debt, bi.opi_debt_amount, bi.opi_checked_at, bi.opi_full_name
            FROM borrowers b
            LEFT JOIN borrower_info bi ON b.document_id = bi.document_id
            WHERE b.service = ? AND b.borrower_user_id = ?
            """,
            (service, borrower_user_id),
        )
        if not rows:
            return None
        row = dict(rows[0])
        row["display_names"] = _parse_display_names(row.get("display_names"))
        row["current_display_name"] = _current_display_name(row.get("display_names"))
        row["source"] = row.get("info_source")
        return row
    finally:
        await db.close()


async def lookup_borrower_info(document_id: str) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM borrower_info WHERE document_id = ?",
            (document_id,),
        )
        if not rows:
            return None
        row = dict(rows[0])
        display_names = await _collect_display_names_by_document(db, document_id)
        row["display_names"] = display_names
        row["current_display_name"] = _current_display_name(display_names)
        return row
    finally:
        await db.close()


async def lookup_unique_document_id_by_full_name(full_name: str) -> str | None:
    normalized = _normalize_compare(full_name)
    if not normalized:
        return None
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT DISTINCT document_id
            FROM borrower_info
            WHERE REPLACE(full_name, 'Ё', 'Е') = ?
              AND NULLIF(TRIM(COALESCE(document_id, '')), '') IS NOT NULL
            LIMIT 2
            """,
            (normalized,),
        )
        if len(rows) != 1:
            return None
        return rows[0]["document_id"]
    finally:
        await db.close()


async def lookup_borrower_contacts(document_id: str) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT document_id, full_name, borrower_phone, borrower_email,
                   borrower_address, borrower_zip, contact_source, source, source_account_tag
            FROM borrower_info
            WHERE document_id = ?
              AND (
                    NULLIF(TRIM(COALESCE(borrower_phone, '')), '') IS NOT NULL
                 OR NULLIF(TRIM(COALESCE(borrower_email, '')), '') IS NOT NULL
                 OR NULLIF(TRIM(COALESCE(borrower_address, '')), '') IS NOT NULL
              )
            LIMIT 1
            """,
            (document_id,),
        )
        if not rows:
            return None
        row = dict(rows[0])
        display_names = await _collect_display_names_by_document(db, document_id)
        return {
            "document_id": row.get("document_id"),
            "full_name": row.get("full_name"),
            "display_names": display_names,
            "current_display_name": _current_display_name(display_names),
            "phone": row.get("borrower_phone"),
            "email": row.get("borrower_email"),
            "address": row.get("borrower_address"),
            "zip": row.get("borrower_zip"),
            "source": row.get("contact_source") or row.get("source"),
            "source_account_tag": row.get("source_account_tag"),
        }
    finally:
        await db.close()


async def search_borrower_info(query: str, limit: int = 10) -> list[dict]:
    db = await get_db()
    try:
        query_value = query.strip()
        query_upper = query_value.upper().replace("Ё", "Е")
        rows_list: list[dict] = []
        seen_docs: set[str] = set()
        if any(char.isdigit() for char in query_value) and len(query_value) >= 7 and query_value.replace(" ", "").isalnum():
            rows = await db.execute_fetchall(
                "SELECT * FROM borrower_info WHERE document_id LIKE ? LIMIT ?",
                (f"%{query_upper}%", limit),
            )
            rows_list = [dict(row) for row in rows]
        else:
            rows = await db.execute_fetchall(
                """
                SELECT *
                FROM borrower_info
                WHERE REPLACE(COALESCE(full_name, ''), 'Ё', 'Е') LIKE ?
                   OR COALESCE(full_name, '') LIKE ?
                LIMIT ?
                """,
                (f"%{query_upper}%", f"%{query_value}%", limit),
            )
            rows_list = [dict(row) for row in rows]
            seen_docs = {row["document_id"] for row in rows_list if row.get("document_id")}
            extra = await db.execute_fetchall(
                """
                SELECT DISTINCT full_name, document_id, service,
                       total_loans, settled_loans, overdue_loans, display_names
                FROM borrowers
                WHERE REPLACE(COALESCE(full_name, ''), 'Ё', 'Е') LIKE ?
                   OR COALESCE(full_name, '') LIKE ?
                   OR REPLACE(COALESCE(display_names, ''), 'Ё', 'Е') LIKE ?
                   OR COALESCE(display_names, '') LIKE ?
                LIMIT ?
                """,
                (f"%{query_upper}%", f"%{query_value}%", f"%{query_upper}%", f"%{query_value}%", limit),
            )
            for row in extra:
                document_id = row["document_id"]
                if not document_id or document_id in seen_docs:
                    continue
                display_names = _parse_display_names(row["display_names"])
                rows_list.append(
                    {
                        "document_id": document_id,
                        "full_name": row["full_name"],
                        "current_display_name": _current_display_name(display_names),
                        "display_names": display_names,
                        "loan_status": None,
                        "loan_status_details_json": None,
                        "sum_category": None,
                        "rating": None,
                        "notes": f"из {row['service']}" if row["service"] else None,
                        "last_loan_date": None,
                        "loan_count": row["total_loans"],
                        "source": row["service"],
                        "source_account_tag": None,
                        "opi_has_debt": None,
                        "opi_debt_amount": None,
                        "opi_checked_at": None,
                        "opi_full_name": None,
                        "total_invested": None,
                    }
                )
                seen_docs.add(document_id)

        document_ids = [row["document_id"] for row in rows_list if row.get("document_id")]
        display_names_map = {doc_id: await _collect_display_names_by_document(db, doc_id) for doc_id in document_ids}
        for row in rows_list:
            doc_id = row.get("document_id")
            display_names = display_names_map.get(doc_id, [])
            row["display_names"] = display_names
            row["current_display_name"] = row.get("current_display_name") or _current_display_name(display_names)
        return rows_list[:limit]
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
    source: str = "search",
    *,
    loan_status_details_json=None,
    source_account_tag: str | None = None,
) -> None:
    normalized_full_name = _normalize_upper(full_name)
    db = await get_db()
    try:
        existing = await _fetch_borrower_info_row(db, document_id)
        normalized_source, normalized_tag = _normalize_source(source)
        chosen_source = _pick_source((existing or {}).get("source"), normalized_source, _SOURCE_PRIORITY)
        chosen_tag = _merge_account_tag(
            (existing or {}).get("source_account_tag"),
            chosen_source,
            normalized_source,
            source_account_tag or normalized_tag,
        )
        merged_full_name = _merge_full_name((existing or {}).get("full_name"), normalized_full_name)
        merged_details = _serialize_status_details(loan_status_details_json) or (existing or {}).get("loan_status_details_json")
        await db.execute(
            """
            INSERT INTO borrower_info
                (document_id, full_name, loan_status, loan_status_details_json, sum_category, rating,
                 notes, last_loan_date, loan_count, total_invested, source, source_account_tag, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(document_id) DO UPDATE SET
                full_name = ?,
                loan_status = COALESCE(excluded.loan_status, borrower_info.loan_status),
                loan_status_details_json = COALESCE(excluded.loan_status_details_json, borrower_info.loan_status_details_json),
                sum_category = COALESCE(excluded.sum_category, borrower_info.sum_category),
                rating = COALESCE(excluded.rating, borrower_info.rating),
                notes = COALESCE(excluded.notes, borrower_info.notes),
                last_loan_date = COALESCE(excluded.last_loan_date, borrower_info.last_loan_date),
                loan_count = CASE WHEN excluded.loan_count IS NOT NULL
                                  THEN excluded.loan_count
                                  ELSE borrower_info.loan_count END,
                total_invested = COALESCE(excluded.total_invested, borrower_info.total_invested),
                source = COALESCE(excluded.source, borrower_info.source),
                source_account_tag = COALESCE(excluded.source_account_tag, borrower_info.source_account_tag),
                updated_at = datetime('now')
            """,
            (
                document_id,
                merged_full_name,
                loan_status,
                merged_details,
                sum_category,
                rating,
                notes,
                last_loan_date,
                loan_count,
                total_invested,
                chosen_source,
                chosen_tag,
                merged_full_name,
                chosen_source,
                chosen_tag,
            ),
        )
        await _refresh_borrower_status(db, document_id)
        await db.commit()
    finally:
        await db.close()


async def upsert_borrower_contacts(
    document_id: str,
    *,
    full_name: str | None = None,
    borrower_phone: str | None = None,
    borrower_email: str | None = None,
    borrower_address: str | None = None,
    borrower_zip: str | None = None,
    source: str = "manual",
    source_account_tag: str | None = None,
) -> None:
    normalized_full_name = _normalize_upper(full_name)
    borrower_phone = (borrower_phone or "").strip() or None
    borrower_email = (borrower_email or "").strip() or None
    borrower_address = (borrower_address or "").strip() or None
    borrower_zip = (borrower_zip or "").strip() or None
    db = await get_db()
    try:
        existing = await _fetch_borrower_info_row(db, document_id)
        normalized_source, normalized_tag = _normalize_source(source)
        normalized_contact_source, contact_tag = _normalize_contact_source(source)
        chosen_source = _pick_source((existing or {}).get("source"), normalized_source, _SOURCE_PRIORITY)
        chosen_contact_source = _pick_source((existing or {}).get("contact_source"), normalized_contact_source, _CONTACT_SOURCE_PRIORITY)
        chosen_tag = _merge_account_tag(
            (existing or {}).get("source_account_tag"),
            chosen_source if chosen_source == normalized_source else chosen_contact_source,
            normalized_source or normalized_contact_source,
            source_account_tag or contact_tag or normalized_tag,
        )
        merged_full_name = _merge_full_name((existing or {}).get("full_name"), normalized_full_name)
        await db.execute(
            """
            INSERT INTO borrower_info
                (document_id, full_name, borrower_phone, borrower_email, borrower_address, borrower_zip,
                 contact_source, source, source_account_tag, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(document_id) DO UPDATE SET
                full_name = ?,
                borrower_phone = COALESCE(excluded.borrower_phone, borrower_info.borrower_phone),
                borrower_email = COALESCE(excluded.borrower_email, borrower_info.borrower_email),
                borrower_address = COALESCE(excluded.borrower_address, borrower_info.borrower_address),
                borrower_zip = COALESCE(excluded.borrower_zip, borrower_info.borrower_zip),
                contact_source = COALESCE(excluded.contact_source, borrower_info.contact_source),
                source = COALESCE(excluded.source, borrower_info.source),
                source_account_tag = COALESCE(excluded.source_account_tag, borrower_info.source_account_tag),
                updated_at = datetime('now')
            """,
            (
                document_id,
                merged_full_name,
                borrower_phone,
                borrower_email,
                borrower_address,
                borrower_zip,
                chosen_contact_source,
                chosen_source,
                chosen_tag,
                merged_full_name,
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
    *,
    display_name: str | None = None,
    display_names: list[str] | tuple[str, ...] | None = None,
) -> None:
    normalized_full_name = _normalize_upper(full_name)
    current_display = (display_name or "").strip() or None
    db = await get_db()
    try:
        existing = await _fetch_borrower_row(db, service, borrower_user_id)
        merged_display_names = _merge_display_names(
            (existing or {}).get("display_names"),
            display_names,
            current_display,
        )
        if service == "zaimis" and normalized_full_name and not _is_probable_legal_full_name(normalized_full_name):
            merged_display_names = _merge_display_names(merged_display_names, normalized_full_name)
            normalized_full_name = None
        merged_full_name = _merge_full_name((existing or {}).get("full_name"), normalized_full_name)
        await db.execute(
            """
            INSERT INTO borrowers
                   (service, borrower_user_id, full_name, display_names, total_loans, settled_loans,
                    overdue_loans, avg_rating, total_invested)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(service, borrower_user_id) DO UPDATE SET
                   full_name = ?,
                   display_names = ?,
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
                merged_full_name,
                _serialize_display_names(merged_display_names),
                total_loans,
                settled_loans,
                overdue_loans,
                avg_rating,
                total_invested,
                merged_full_name,
                _serialize_display_names(merged_display_names),
            ),
        )
        effective_document_id = (existing or {}).get("document_id")
        if effective_document_id:
            await _refresh_borrower_status(db, effective_document_id)
        await db.commit()
    finally:
        await db.close()


async def list_borrower_mappings_by_document_ids(document_ids: list[str], service: str | None = None) -> list[dict]:
    if not document_ids:
        return []
    db = await get_db()
    try:
        placeholders = ",".join("?" for _ in document_ids)
        where = f"b.document_id IN ({placeholders})"
        params: list[object] = [*document_ids]
        if service:
            where += " AND b.service = ?"
            params.append(service)
        rows = await db.execute_fetchall(
            f"""
            SELECT b.service, b.borrower_user_id, b.document_id,
                   COALESCE(b.full_name, bi.full_name) AS full_name,
                   b.display_names
            FROM borrowers b
            LEFT JOIN borrower_info bi ON bi.document_id = b.document_id
            WHERE {where}
            ORDER BY b.last_seen DESC
            """,
            params,
        )
        result: list[dict] = []
        for row in rows:
            payload = dict(row)
            payload["display_names"] = _parse_display_names(payload.get("display_names"))
            payload["current_display_name"] = _current_display_name(payload.get("display_names"))
            result.append(payload)
        return result
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


async def refresh_borrower_statuses(document_ids: Iterable[str]) -> None:
    ids = [str(document_id).strip() for document_id in document_ids if str(document_id).strip()]
    if not ids:
        return
    db = await get_db()
    try:
        seen: set[str] = set()
        for document_id in ids:
            if document_id in seen:
                continue
            seen.add(document_id)
            await _refresh_borrower_status(db, document_id)
        await db.commit()
    finally:
        await db.close()


__all__ = [
    "get_borrowers_count",
    "get_borrowers_stats",
    "list_borrower_mappings_by_document_ids",
    "list_borrower_name_map",
    "lookup_borrower",
    "lookup_borrower_contacts",
    "lookup_borrower_info",
    "lookup_unique_document_id_by_full_name",
    "refresh_borrower_statuses",
    "search_borrower_info",
    "upsert_borrower",
    "upsert_borrower_contacts",
    "upsert_borrower_from_investment",
    "upsert_borrower_info",
]
