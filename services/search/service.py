from __future__ import annotations

import re

from bot.integrations.opi_client import OPIChecker
from bot.repositories.borrowers import (
    list_borrower_mappings_by_document_ids,
    lookup_borrower_contacts,
    lookup_borrower_info,
    search_borrower_info,
    upsert_borrower_contacts,
    upsert_borrower_info,
)
from bot.repositories.overdue import lookup_latest_borrower_contacts
from bot.services.base.providers import ensure_finkit_parser, list_service_credentials
from bot.services.borrowers.source_labels import humanize_borrower_source

SERVICE_NAMES = {
    "kapusta": "Kapusta",
    "finkit": "FinKit",
    "zaimis": "ЗАЙМись",
}


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

    source_label = humanize_borrower_source(info.get("source"))
    if source_label:
        lines.append(f"\n<i>Источник: {source_label}</i>")

    return "\n".join(lines)


def format_contact_card(document_id: str, payload: dict) -> str:
    lines = ["📇 <b>Доп. инфа по ИН</b>", ""]
    lines.append(f"<b>ИН:</b> <code>{document_id}</code>")
    if payload.get("full_name"):
        lines.append(payload["full_name"])
    if payload.get("phone"):
        lines.append(f"<b>Телефон:</b> <code>{payload['phone']}</code>")
    if payload.get("email"):
        lines.append(f"<b>Email:</b> <code>{payload['email']}</code>")
    if payload.get("service"):
        lines.append(f"<b>Сервис:</b> {SERVICE_NAMES.get(str(payload['service']), str(payload['service']))}")
    source_label = humanize_borrower_source(payload.get("source"))
    if source_label:
        lines.append(f"<i>Источник: {source_label}</i>")
    return "\n".join(lines)


async def lookup_borrower_contact_info(document_id: str) -> dict | None:
    payload = await lookup_borrower_contacts(document_id)
    overdue_payload = await lookup_latest_borrower_contacts(document_id)
    if payload and overdue_payload:
        return {
            "document_id": document_id,
            "full_name": payload.get("full_name") or overdue_payload.get("full_name"),
            "service": overdue_payload.get("service"),
            "phone": payload.get("phone") or overdue_payload.get("phone"),
            "email": payload.get("email") or overdue_payload.get("email"),
            "source": payload.get("source") or overdue_payload.get("source"),
        }
    return payload or overdue_payload


def _normalize_name(value: str | None) -> str:
    return (value or "").strip().upper().replace("Ё", "Е")


async def _backfill_finkit_contacts(document_ids: list[str]) -> None:
    mappings = await list_borrower_mappings_by_document_ids(document_ids, service="finkit")
    if not mappings:
        return
    docs_by_name: dict[str, set[str]] = {}
    for row in mappings:
        name_key = _normalize_name(row.get("full_name"))
        if not name_key:
            continue
        docs_by_name.setdefault(name_key, set()).add(str(row["document_id"]))
    if not docs_by_name:
        return

    creds = await list_service_credentials("finkit")
    remaining = set(document_ids)
    for cred in creds:
        parser = await ensure_finkit_parser(cred)
        if parser is None:
            continue
        try:
            items = await parser.fetch_investments()
            if parser.needs_reauth:
                parser = await ensure_finkit_parser(cred, force_login=True)
                if parser is None:
                    continue
                items = await parser.fetch_investments()

            for item in items:
                name_key = _normalize_name(item.get("borrower_full_name"))
                if name_key not in docs_by_name:
                    continue
                investment_id = str(item.get("id") or "").strip()
                if not investment_id:
                    continue
                detail = await parser.fetch_investment_detail(investment_id) or {}
                phone = (detail.get("borrower_phone_number") or "").strip()
                email = (detail.get("borrower_email") or "").strip()
                if not phone and not email:
                    continue

                matched_doc_ids: set[str] = set()
                contract_url = detail.get("latest_contract_url")
                if contract_url:
                    contract_pdf = await parser.fetch_contract_pdf(str(contract_url))
                    if contract_pdf:
                        _, parsed_document_id = parser.parse_borrower_from_contract_pdf(contract_pdf)
                        if parsed_document_id and parsed_document_id in docs_by_name.get(name_key, set()):
                            matched_doc_ids.add(parsed_document_id)

                if not matched_doc_ids and len(docs_by_name.get(name_key, set())) == 1:
                    matched_doc_ids.update(docs_by_name[name_key])

                for document_id in matched_doc_ids:
                    await upsert_borrower_contacts(
                        document_id,
                        full_name=detail.get("borrower_full_name") or item.get("borrower_full_name"),
                        borrower_phone=phone or None,
                        borrower_email=email or None,
                        source="finkit_investment_detail",
                    )
                    remaining.discard(document_id)

                if not remaining:
                    return
        finally:
            await parser.close()


async def ensure_borrower_contact_info(document_ids: list[str]) -> None:
    missing: list[str] = []
    for document_id in document_ids:
        if not await lookup_borrower_contact_info(document_id):
            missing.append(document_id)
    if missing:
        await _backfill_finkit_contacts(missing)


async def run_opi_batch(doc_ids: list[str]) -> str:
    checker = OPIChecker()
    lines = ["🆔 <b>Проверка по ИН</b>", ""]
    try:
        await ensure_borrower_contact_info(doc_ids)
        for idx, doc_id in enumerate(doc_ids, start=1):
            info = await lookup_borrower_info(doc_id)
            contacts = await lookup_borrower_contact_info(doc_id)
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
            info_source_label = humanize_borrower_source((info or {}).get("source"))
            if info_source_label:
                lines.append(f"ℹ️ Данные: {info_source_label}")
            if contacts:
                if contacts.get("phone"):
                    lines.append(f"📞 Телефон: <code>{contacts['phone']}</code>")
                if contacts.get("email"):
                    lines.append(f"✉️ Email: <code>{contacts['email']}</code>")
                source_label = humanize_borrower_source(contacts.get("source"))
                if source_label and source_label != info_source_label:
                    lines.append(f"ℹ️ Контакты: {source_label}")
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
    "ensure_borrower_contact_info",
    "extract_document_ids",
    "force_refresh_opi_card",
    "format_contact_card",
    "format_borrower_card",
    "lookup_borrower_contact_info",
    "lookup_borrower_info",
    "run_opi_batch",
    "search_borrower_info",
    "upsert_borrower_info",
]
