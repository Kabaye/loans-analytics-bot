from __future__ import annotations

import json
import re
from typing import Any

from bot.utils.borrower_address import sanitize_borrower_address

_ZIP_RE = re.compile(r"\b(\d{6})\b")


def extract_zip_from_address(address: str | None) -> str | None:
    text = str(address or "").strip()
    if not text:
        return None
    match = _ZIP_RE.search(text)
    return match.group(1) if match else None


def _address_key(address: str | None) -> str:
    return str(address or "").strip().upper().replace("Ё", "Е")


def _normalize_address_entry(value: Any, *, full_name: str | None = None) -> dict[str, str] | None:
    if isinstance(value, dict):
        label = str(value.get("label") or "").strip() or None
        address = sanitize_borrower_address(
            value.get("address") or value.get("borrower_address"),
            full_name,
        )
        zip_code = str(value.get("zip") or value.get("borrower_zip") or "").strip() or None
    else:
        label = None
        address = sanitize_borrower_address(value, full_name)
        zip_code = None
    if not address:
        return None
    zip_code = zip_code or extract_zip_from_address(address)
    result = {"address": address}
    if zip_code:
        result["zip"] = zip_code
    if label:
        result["label"] = label
    return result


def normalize_borrower_addresses(
    value: Any,
    *,
    full_name: str | None = None,
) -> list[dict[str, str]]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            raw_items: list[Any] = [text]
        else:
            if isinstance(parsed, list):
                raw_items = parsed
            else:
                raw_items = [parsed]
    elif isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw_items = [value]

    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in raw_items:
        normalized = _normalize_address_entry(item, full_name=full_name)
        if not normalized:
            continue
        key = _address_key(normalized.get("address"))
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(normalized)
    return result


def merge_primary_borrower_address(
    primary_address: str | None,
    primary_zip: str | None,
    addresses: Any = None,
    *,
    full_name: str | None = None,
) -> list[dict[str, str]]:
    merged = normalize_borrower_addresses(addresses, full_name=full_name)
    primary = _normalize_address_entry(
        {"address": primary_address, "zip": primary_zip},
        full_name=full_name,
    )
    if not primary:
        return merged
    primary_key = _address_key(primary.get("address"))
    if primary_key:
        merged = [item for item in merged if _address_key(item.get("address")) != primary_key]
    return [primary, *merged]


def primary_borrower_address(
    addresses: Any,
    *,
    full_name: str | None = None,
) -> tuple[str | None, str | None]:
    normalized = normalize_borrower_addresses(addresses, full_name=full_name)
    if not normalized:
        return None, None
    first = normalized[0]
    return first.get("address"), first.get("zip")


def serialize_borrower_addresses(
    value: Any,
    *,
    full_name: str | None = None,
) -> str | None:
    normalized = normalize_borrower_addresses(value, full_name=full_name)
    return json.dumps(normalized, ensure_ascii=False) if normalized else None


__all__ = [
    "extract_zip_from_address",
    "merge_primary_borrower_address",
    "normalize_borrower_addresses",
    "primary_borrower_address",
    "serialize_borrower_addresses",
]
