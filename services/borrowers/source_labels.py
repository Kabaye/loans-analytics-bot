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
        "sheets": "таблица / импорт",
        "opi": "ОПИ",
        "finkit": "текущий кейс FinKit",
        "zaimis": "текущий кейс ЗАЙМись",
        "kapusta": "текущий кейс Kapusta",
        "finkit_borrow": "архив FinKit",
        "zaimis_borrow": "архив ЗАЙМись",
        "kapusta_borrow": "архив Kapusta",
        "finkit_claim_pdf": "претензия FinKit",
        "finkit_contract_pdf": "договор FinKit",
        "finkit_investment_detail": "детали займа FinKit",
        "finkit_name_match": "точное ФИО из базы + архив FinKit",
        "zaimis_contract_pdf": "договор ЗАЙМись",
    }
    if value in direct:
        return direct[value]

    if value.startswith("finkit_investment_detail"):
        return "детали займа FinKit"
    if value.startswith("finkit_name_match"):
        return "точное ФИО из базы + архив FinKit"
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
