from __future__ import annotations

import re

from bot.integrations.opi_client import OPIChecker
from bot.repositories.borrowers import lookup_borrower_info, upsert_borrower_info


def extract_document_ids(text: str) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for doc_id in re.findall(r"\b[0-9A-Z]{14}\b", text.upper()):
        if doc_id in seen:
            continue
        seen.add(doc_id)
        ids.append(doc_id)
    return ids


def format_borrower_card(info: dict) -> str:
    lines = ["<b>📋 Карточка заёмщика</b>"]
    lines.append(f"\n<b>ИН:</b> <code>{info['document_id']}</code>")
    if info.get("full_name"):
        lines.append(f"<b>ФИО:</b> {info['full_name']}")

    if info.get("loan_status"):
        status_icon = {
            "в срок": "✅",
            "просрочка до 20 дней": "⚠️",
            "просрочка > 20 дней": "🔶",
            "все плохо": "🔴",
        }.get(info["loan_status"], "📋")
        lines.append(f"<b>Статус:</b> {status_icon} {info['loan_status']}")

    if info.get("sum_category"):
        lines.append(f"<b>Категория сумм:</b> {info['sum_category']}")
    if info.get("rating") is not None:
        lines.append(f"<b>Рейтинг:</b> {info['rating']:.0f}")
    if info.get("loan_count"):
        lines.append(f"<b>Займов:</b> {info['loan_count']}")
    if info.get("last_loan_date"):
        lines.append(f"<b>Последний займ:</b> {info['last_loan_date']}")

    if info.get("opi_checked_at"):
        if info.get("opi_has_debt"):
            lines.append(f"\n❌ <b>ОПИ:</b> должен <b>{info.get('opi_debt_amount', 0):.2f}</b> BYN")
            if info.get("opi_full_name"):
                lines.append(f"  Имя в ОПИ: {info['opi_full_name']}")
        else:
            lines.append("\n✅ <b>ОПИ:</b> нет задолженности")
        checked = info["opi_checked_at"][:19].replace("T", " ") if info["opi_checked_at"] else "—"
        lines.append(f"  Проверено: {checked}")
    else:
        lines.append("\n⏳ ОПИ: не проверялось")

    if info.get("notes"):
        lines.append(f"\n📝 {info['notes']}")

    if info.get("source"):
        lines.append(f"\n<i>Источник: {info['source']}</i>")

    return "\n".join(lines)


async def run_opi_batch(doc_ids: list[str]) -> str:
    checker = OPIChecker()
    lines = ["🆔 <b>Проверка по ИН</b>", ""]
    try:
        for idx, doc_id in enumerate(doc_ids, start=1):
            info = await lookup_borrower_info(doc_id)
            result = await checker.check(doc_id)

            lines.append(f"{idx}. <code>{doc_id}</code>")
            full_name = (info or {}).get("full_name") or result.full_name
            if full_name:
                lines.append(full_name)
            if info and info.get("loan_status"):
                lines.append(f"Статус: {info['loan_status']}")
            if result.error:
                lines.append("⚠️ ОПИ: ошибка проверки")
            elif result.has_debt:
                lines.append(f"❌ ОПИ: долг {result.debt_amount:.2f} BYN")
            else:
                lines.append("✅ ОПИ: нет задолженности")
            lines.append("")
    finally:
        await checker.close()
    return "\n".join(lines).strip()


async def force_refresh_opi_card(doc_id: str) -> tuple[dict | None, str | None]:
    checker = OPIChecker()
    try:
        result = await checker.check(doc_id, use_cache=False)
    finally:
        await checker.close()
    info = await lookup_borrower_info(doc_id)
    return info, result.error


async def add_borrower_and_refresh_opi(
    document_id: str,
    full_name: str,
    loan_status: str | None,
    sum_category: str | None,
) -> dict | None:
    await upsert_borrower_info(
        document_id=document_id,
        full_name=full_name,
        loan_status=loan_status,
        sum_category=sum_category,
        source="added",
    )
    checker = OPIChecker()
    try:
        await checker.check(document_id, use_cache=False)
    finally:
        await checker.close()
    return await lookup_borrower_info(document_id)


__all__ = [
    "add_borrower_and_refresh_opi",
    "extract_document_ids",
    "force_refresh_opi_card",
    "format_borrower_card",
    "lookup_borrower_info",
    "run_opi_batch",
    "search_borrower_info",
    "upsert_borrower_info",
]
