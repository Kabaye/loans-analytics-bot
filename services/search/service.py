from __future__ import annotations

import json
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


def extract_document_id_batch(text: str) -> list[str]:
    doc_ids = extract_document_ids(text)
    if not doc_ids:
        return []
    stripped = re.sub(r"\b[0-9A-Z]{14}\b", "", text.upper())
    if re.sub(r"[\s,;]+", "", stripped):
        return []
    return doc_ids


def format_borrower_card(info: dict) -> str:
    lines = ["<b>📋 Карточка заёмщика</b>"]
    lines.append(f"\n<b>ИН:</b> <code>{info['document_id']}</code>")
    if info.get("full_name"):
        lines.append(f"<b>ФИО:</b> {info['full_name']}")
    elif info.get("current_display_name"):
        lines.append(f"<b>Текущий ник:</b> {info['current_display_name']}")
    if info.get("display_names"):
        names = [str(item) for item in info["display_names"] if str(item).strip()]
        if names:
            highlighted = ", ".join(names[:-1] + [f"<b>{names[-1]}</b>"])
            lines.append(f"<b>Ники:</b> {highlighted}")

    if info.get("loan_status"):
        status_icon = {
            "в срок": "✅",
            "текущий": "ℹ️",
            "просрочка до 5 дней": "⚠️",
            "просрочка 6-30 дней": "🔶",
            "просрочка > 30 дней": "🔴",
            "закрыт, были просрочки": "⚠️",
        }.get(info["loan_status"], "📋")
        lines.append(f"<b>Статус:</b> {status_icon} {info['loan_status']}")
    if info.get("loan_status_details_json"):
        try:
            details = info["loan_status_details_json"]
            if isinstance(details, str):
                details = json.loads(details)
        except Exception:
            details = None
        if isinstance(details, list) and details:
            lines.append("<b>Детали:</b> " + " → ".join(str(item) for item in details if str(item).strip()))

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
    elif payload.get("current_display_name"):
        lines.append(payload["current_display_name"])
    if payload.get("display_names"):
        names = [str(item) for item in payload["display_names"] if str(item).strip()]
        if names:
            lines.append("<b>Ники:</b> " + ", ".join(names[:-1] + [f"<b>{names[-1]}</b>"]))
    if payload.get("phone"):
        lines.append(f"<b>Телефон:</b> <code>{payload['phone']}</code>")
    if payload.get("email"):
        lines.append(f"<b>Email:</b> <code>{payload['email']}</code>")
    if payload.get("address"):
        lines.append(f"<b>Адрес:</b> {payload['address']}")
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
            "display_names": payload.get("display_names") or overdue_payload.get("display_names") or [],
            "current_display_name": payload.get("current_display_name") or overdue_payload.get("current_display_name"),
            "service": overdue_payload.get("service"),
            "phone": payload.get("phone") or overdue_payload.get("phone"),
            "email": payload.get("email") or overdue_payload.get("email"),
            "address": payload.get("address") or overdue_payload.get("address"),
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
        payload = await lookup_borrower_contact_info(document_id)
        if not payload or (not payload.get("phone") and not payload.get("email")):
            missing.append(document_id)
    if missing:
        await _backfill_finkit_contacts(missing)


def _needs_search_backfill(info: dict | None) -> bool:
    if not info:
        return True
    return not any(
        (
            (info.get("source") or "").strip(),
            (info.get("full_name") or "").strip(),
            (info.get("current_display_name") or "").strip(),
            info.get("loan_status"),
            info.get("notes"),
            info.get("rating") is not None,
            info.get("last_loan_date"),
            info.get("loan_count"),
            info.get("opi_checked_at"),
        )
    )


def _append_opi_summary(lines: list[str], info: dict | None, error: str | None) -> None:
    if info and info.get("opi_checked_at"):
        if info.get("opi_has_debt"):
            lines.append(f"❌ ОПИ: долг {float(info.get('opi_debt_amount') or 0):.2f} BYN")
        else:
            lines.append("✅ ОПИ: нет задолженности")
        return
    if error:
        lines.append(f"⚠️ ОПИ: {error}")
        return
    lines.append("⏳ ОПИ: не проверялось")


async def run_document_lookup_batch(doc_ids: list[str]) -> str:
    lines = ["🆔 <b>Поиск по ИН</b>", ""]
    await ensure_borrower_contact_info(doc_ids)

    initial_info_map = {doc_id: await lookup_borrower_info(doc_id) for doc_id in doc_ids}
    missing_doc_ids = [doc_id for doc_id, info in initial_info_map.items() if _needs_search_backfill(info)]
    opi_errors: dict[str, str | None] = {}

    if missing_doc_ids:
        checker = OPIChecker()
        try:
            for doc_id in missing_doc_ids:
                existing = initial_info_map.get(doc_id)
                result = None
                if not existing or not existing.get("opi_checked_at"):
                    result = await checker.check(doc_id)
                    opi_errors[doc_id] = result.error
                await upsert_borrower_info(
                    document_id=doc_id,
                    full_name=(
                        (result.full_name if result else None)
                        or (existing or {}).get("full_name")
                        or (existing or {}).get("opi_full_name")
                    ),
                    source="search",
                )
        finally:
            await checker.close()

    for idx, doc_id in enumerate(doc_ids, start=1):
        info = await lookup_borrower_info(doc_id)
        contacts = await lookup_borrower_contact_info(doc_id)

        lines.append(f"{idx}. <code>{doc_id}</code>")
        full_name = (info or {}).get("full_name") or (info or {}).get("current_display_name")
        if full_name:
            lines.append(full_name)
        if info and info.get("loan_status"):
            lines.append(f"Статус: {info['loan_status']}")

        _append_opi_summary(lines, info, opi_errors.get(doc_id))

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

    return "\n".join(lines).strip()


async def run_opi_batch(doc_ids: list[str]) -> str:
    return await run_document_lookup_batch(doc_ids)


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
        source="search",
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
    "extract_document_id_batch",
    "force_refresh_opi_card",
    "format_contact_card",
    "format_borrower_card",
    "lookup_borrower_contact_info",
    "lookup_borrower_info",
    "run_document_lookup_batch",
    "run_opi_batch",
    "search_borrower_info",
    "upsert_borrower_info",
]
