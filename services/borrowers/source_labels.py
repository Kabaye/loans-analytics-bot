from __future__ import annotations


def humanize_borrower_source(source: str | None) -> str | None:
    if not source:
        return None
    value = str(source).strip()
    if not value:
        return None

    direct = {
        "added": "добавлено вручную",
        "manual": "введено вручную",
        "opi": "ОПИ",
        "finkit_borrow": "FinKit",
        "zaimis_borrow": "ЗАЙМись",
        "kapusta_borrow": "Kapusta",
        "finkit_claim_pdf": "претензия FinKit",
        "finkit_contract_pdf": "договор FinKit",
        "finkit_investment_detail": "детали займа FinKit",
        "zaimis_contract_pdf": "договор ЗАЙМись",
    }
    if value in direct:
        return direct[value]

    if value.startswith("finkit_archive_"):
        return "архив FinKit"
    if value.startswith("zaimis_archive_"):
        return "архив ЗАЙМись"
    if value.startswith("kapusta_archive_"):
        return "архив Kapusta"
    if value.startswith("finkit_overdue_pdf_"):
        return "документы FinKit"
    if value.startswith("finkit_"):
        return "FinKit"
    if value.startswith("zaimis_"):
        return "ЗАЙМись"
    if value.startswith("kapusta_"):
        return "Kapusta"

    return value


__all__ = ["humanize_borrower_source"]
