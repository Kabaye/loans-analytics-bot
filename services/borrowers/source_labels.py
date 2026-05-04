from __future__ import annotations


def _extract_account_tag(value: str, prefix: str) -> str | None:
    if not value.startswith(prefix):
        return None
    suffix = value[len(prefix):].strip("_")
    return suffix or None


_SOURCE_LABELS = {
    "search": "поиск / проверка ОПИ",
    "manual": "введено вручную",
    "sheets": "таблица / импорт",
    "finkit": "текущий кейс FinKit",
    "zaimis": "текущий кейс ЗАЙМись",
    "kapusta": "текущий кейс Kapusta",
    "finkit_borrow": "архив FinKit",
    "zaimis_borrow": "архив ЗАЙМись",
    "kapusta_borrow": "архив Kapusta",
    "finkit_investment_detail": "детали займа FinKit",
    "zaimis_investment_detail": "детали займа ЗАЙМись",
}

_SOURCE_EXACT_ALIASES = {
    "added": "search",
    "opi": "search",
    "search": "search",
    "manual": "manual",
    "sheets": "sheets",
    "finkit": "finkit",
    "zaimis": "zaimis",
    "kapusta": "kapusta",
    "finkit_borrow": "finkit_borrow",
    "zaimis_borrow": "zaimis_borrow",
    "kapusta_borrow": "kapusta_borrow",
    "finkit_investment_detail": "finkit_investment_detail",
    "zaimis_investment_detail": "zaimis_investment_detail",
    "finkit_name_match": "finkit_borrow",
    "finkit_contract_pdf": "finkit_investment_detail",
    "zaimis_contract_pdf": "zaimis_investment_detail",
}

_SOURCE_PREFIX_ALIASES = [
    ("finkit_investment_detail_", "finkit_investment_detail"),
    ("zaimis_investment_detail_", "zaimis_investment_detail"),
    ("finkit_claim_pdf_", "finkit_investment_detail"),
    ("finkit_overdue_pdf_", "finkit_investment_detail"),
    ("finkit_archive_", "finkit_borrow"),
    ("zaimis_archive_", "zaimis_borrow"),
    ("kapusta_archive_", "kapusta_borrow"),
]

_CONTACT_SOURCE_EXACT_ALIASES = {
    "manual": "manual",
    "finkit_investment_detail": "finkit_investment_detail",
    "zaimis_investment_detail": "zaimis_investment_detail",
    "finkit_contract_pdf": "finkit_investment_detail",
    "zaimis_contract_pdf": "zaimis_investment_detail",
}

_CONTACT_SOURCE_PREFIX_ALIASES = [
    ("finkit_investment_detail_", "finkit_investment_detail"),
    ("zaimis_investment_detail_", "zaimis_investment_detail"),
    ("finkit_claim_pdf_", "finkit_investment_detail"),
    ("finkit_overdue_pdf_", "finkit_investment_detail"),
]


def _split_source(
    source: str | None,
    *,
    exact_aliases: dict[str, str],
    prefix_aliases: list[tuple[str, str]],
) -> tuple[str | None, str | None]:
    value = str(source or "").strip()
    if not value:
        return None, None
    canonical = exact_aliases.get(value)
    if canonical:
        return canonical, None
    for prefix, mapped in prefix_aliases:
        if value.startswith(prefix):
            return mapped, _extract_account_tag(value, prefix)
    return value, None


def split_borrower_source(source: str | None) -> tuple[str | None, str | None]:
    return _split_source(
        source,
        exact_aliases=_SOURCE_EXACT_ALIASES,
        prefix_aliases=_SOURCE_PREFIX_ALIASES,
    )


def split_contact_source(source: str | None) -> tuple[str | None, str | None]:
    return _split_source(
        source,
        exact_aliases=_CONTACT_SOURCE_EXACT_ALIASES,
        prefix_aliases=_CONTACT_SOURCE_PREFIX_ALIASES,
    )


def normalize_borrower_source(source: str | None) -> str | None:
    canonical, _ = split_borrower_source(source)
    return canonical


def normalize_contact_source(source: str | None) -> str | None:
    canonical, _ = split_contact_source(source)
    return canonical


def humanize_borrower_source(source: str | None) -> str | None:
    if not source:
        return None
    value = str(source).strip()
    if not value:
        return None
    canonical = normalize_borrower_source(value)
    return _SOURCE_LABELS.get(canonical or "") or value


__all__ = [
    "humanize_borrower_source",
    "normalize_borrower_source",
    "normalize_contact_source",
    "split_borrower_source",
    "split_contact_source",
]
