from __future__ import annotations

from collections import defaultdict

from bot.utils.borrower_addresses import normalize_borrower_addresses


def _normalize_address_line(value: str | None) -> str | None:
    text = str(value or "").strip()
    return text or None


async def load_borrower_addresses(db, document_id: str | None, *, full_name: str | None = None) -> list[dict[str, str]]:
    if not document_id:
        return []
    rows = await db.execute_fetchall(
        """
        SELECT address_line, zip, is_primary, sort_order
        FROM borrower_addresses
        WHERE document_id = ?
        ORDER BY is_primary DESC, sort_order ASC, id ASC
        """,
        (document_id,),
    )
    items = [
        {"address": row["address_line"], "zip": row["zip"] or None}
        for row in rows
        if _normalize_address_line(row["address_line"])
    ]
    return normalize_borrower_addresses(items, full_name=full_name)


async def load_borrower_addresses_map(db, document_ids: list[str], *, full_name_map: dict[str, str | None] | None = None) -> dict[str, list[dict[str, str]]]:
    normalized_ids = [str(document_id).strip() for document_id in document_ids if str(document_id or "").strip()]
    if not normalized_ids:
        return {}
    placeholders = ",".join("?" for _ in normalized_ids)
    rows = await db.execute_fetchall(
        f"""
        SELECT document_id, address_line, zip, is_primary, sort_order
        FROM borrower_addresses
        WHERE document_id IN ({placeholders})
        ORDER BY document_id, is_primary DESC, sort_order ASC, id ASC
        """,
        normalized_ids,
    )
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        address_line = _normalize_address_line(row["address_line"])
        if not address_line:
            continue
        grouped[row["document_id"]].append({"address": address_line, "zip": row["zip"] or None})
    result: dict[str, list[dict[str, str]]] = {}
    for document_id in normalized_ids:
        full_name = (full_name_map or {}).get(document_id)
        result[document_id] = normalize_borrower_addresses(grouped.get(document_id, []), full_name=full_name)
    return result


async def replace_borrower_addresses(
    db,
    document_id: str | None,
    addresses: list[dict[str, str]] | list[str] | tuple[str, ...] | None,
    *,
    full_name: str | None = None,
    source: str | None = None,
    source_account_tag: str | None = None,
) -> list[dict[str, str]]:
    normalized_document_id = str(document_id or "").strip()
    if not normalized_document_id:
        return []
    normalized_addresses = normalize_borrower_addresses(addresses, full_name=full_name)
    await db.execute("DELETE FROM borrower_addresses WHERE document_id = ?", (normalized_document_id,))
    for index, item in enumerate(normalized_addresses):
        await db.execute(
            """
            INSERT INTO borrower_addresses (
                document_id,
                address_line,
                zip,
                is_primary,
                sort_order,
                source,
                source_account_tag,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            """,
            (
                normalized_document_id,
                item["address"],
                item.get("zip"),
                1 if index == 0 else 0,
                index,
                source,
                source_account_tag,
            ),
        )
    return normalized_addresses


__all__ = ["load_borrower_addresses", "load_borrower_addresses_map", "replace_borrower_addresses"]
