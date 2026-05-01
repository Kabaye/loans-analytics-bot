from __future__ import annotations

import re

_ADDRESS_HINTS = (
    "ОБЛАСТ",
    "РАЙОН",
    "ГОРОД",
    "Г.",
    "ДЕРЕВН",
    "АГРОГОРОД",
    "ПОСЕЛ",
    "СЕЛО",
    "УЛ.",
    "УЛИЦ",
    "ПР.",
    "ПРОСП",
    "ПЕР.",
    "ПЕРЕУЛ",
    "Б-Р",
    "БУЛЬВАР",
    "ДОМ",
    "Д.",
    "КВ.",
    "КВАРТИР",
)


def _normalize(value: str | None) -> str:
    return (value or "").strip().upper().replace("Ё", "Е")


def _looks_like_address(value: str | None) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    normalized = _normalize(text)
    if re.match(r"^\d{6}\b", normalized):
        return True
    if "," in text:
        return True
    return any(hint in normalized for hint in _ADDRESS_HINTS)


def sanitize_borrower_address(address: str | None, full_name: str | None = None) -> str | None:
    cleaned = " ".join(str(address or "").replace("\n", " ").split()).strip(" ,")
    if not cleaned:
        return None

    full_name_tokens = [token for token in _normalize(full_name).split() if token]
    normalized_cleaned = _normalize(cleaned)

    for length in range(len(full_name_tokens), 0, -1):
        suffix = " ".join(full_name_tokens[-length:])
        if not suffix or not normalized_cleaned.startswith(suffix + " "):
            continue
        candidate = cleaned[len(suffix):].lstrip(" ,")
        if _looks_like_address(candidate):
            return candidate

    return cleaned


__all__ = ["sanitize_borrower_address"]
