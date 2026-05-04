from __future__ import annotations

import asyncio
import io
import logging
import re
from datetime import datetime, timedelta, timezone
from html import escape

import pdfplumber

from bot import config
from bot.integrations.parsers.finkit import finkit_has_overdue_history, finkit_is_settled_on_time
from bot.integrations.telegram_admin import send_admin_html_message
from bot.repositories.borrowers import (
    get_borrowers_count,
    list_borrower_name_map,
    lookup_borrower,
    lookup_borrower_contacts,
    lookup_unique_document_id_by_full_name,
    upsert_borrower,
    upsert_borrower_contacts,
    upsert_borrower_from_investment,
)
from bot.services.base.providers import (
    ensure_finkit_parser,
    list_service_credentials,
    telegram_user_tag,
)
from bot.services.zaimis_sync import sync_zaimis_account

log = logging.getLogger(__name__)

_NAME_ID_RE = re.compile(
    r"Я,\s+([А-ЯЁA-Z]+(?:\s+[А-ЯЁA-Z]+){2}),\s+идентификационный\s+номер\s*([0-9A-Z]+)",
    re.IGNORECASE | re.UNICODE | re.MULTILINE | re.DOTALL,
)
_last_investment_refresh: datetime | None = None
INVESTMENT_REFRESH_INTERVAL_DAYS = 3


def _normalize_name(value: str | None) -> str:
    return (value or "").strip().upper().replace("Ё", "Е")


def _cookie_headers(parser) -> dict[str, str]:
    cookie_str = "; ".join(f"{key}={value}" for key, value in parser._session_cookies.items())
    return {"Accept": "application/json", "Referer": "https://finkit.by/", "Cookie": cookie_str}


def _extract_document_id_from_pdf(pdf_bytes: bytes) -> str | None:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            match = _NAME_ID_RE.search(text)
            if match:
                document_id = match.group(2).strip()
                if len(document_id) == 14:
                    return document_id
    return None


async def _lookup_finkit_enrichment_state(
    borrower_name: str,
    borrower_user_id: str | None,
) -> tuple[str | None, bool]:
    if not borrower_user_id:
        return None, False
    existing = await lookup_borrower("finkit", borrower_user_id)
    if not existing:
        return None, False
    if _normalize_name(existing.get("full_name")) and _normalize_name(existing.get("full_name")) != _normalize_name(borrower_name):
        return None, False
    document_id = existing.get("document_id")
    if not document_id:
        return None, False
    return document_id, await lookup_borrower_contacts(document_id) is not None


async def _enrich_finkit_borrower_from_detail(
    session,
    headers: dict[str, str],
    *,
    user_tag: str,
    borrower_name: str,
    investment_id: str,
    borrower_user_id: str | None = None,
) -> tuple[str | None, bool, bool]:
    detail_url = f"https://api-p2p.finkit.by/user/investments/{investment_id}/"
    async with session.get(detail_url, headers=headers) as resp:
        if resp.status != 200:
            return borrower_user_id, False, False
        detail = await resp.json()

    resolved_borrower_user_id = str(detail.get("loan") or borrower_user_id or investment_id)
    existing = await lookup_borrower("finkit", resolved_borrower_user_id)
    document_id = existing.get("document_id") if existing else None
    document_enriched = False

    if not document_id:
        pdf_url = detail.get("latest_contract_url")
        if pdf_url:
            async with session.get(pdf_url) as pdf_resp:
                if pdf_resp.status == 200:
                    document_id = _extract_document_id_from_pdf(await pdf_resp.read())
        if document_id:
            await upsert_borrower(
                service="finkit",
                borrower_user_id=resolved_borrower_user_id,
                full_name=borrower_name,
                document_id=document_id,
                source=f"finkit_archive_{user_tag}",
            )
            log.info("Finkit midnight PDF: %s → ИН %s", borrower_name, document_id)
            document_enriched = True

    if not document_id:
        document_id = await lookup_unique_document_id_by_full_name(borrower_name)
        if document_id:
            await upsert_borrower(
                service="finkit",
                borrower_user_id=resolved_borrower_user_id,
                full_name=borrower_name,
                document_id=document_id,
                source="finkit_name_match",
            )
            log.info("Finkit midnight name-match: %s → ИН %s", borrower_name, document_id)
            document_enriched = True

    contacts_updated = False
    if document_id and (detail.get("borrower_phone_number") or detail.get("borrower_email")):
        await upsert_borrower_contacts(
            document_id,
            full_name=borrower_name,
            borrower_phone=detail.get("borrower_phone_number"),
            borrower_email=detail.get("borrower_email"),
            source="finkit_investment_detail",
        )
        contacts_updated = True

    return resolved_borrower_user_id, document_enriched, contacts_updated


async def refresh_investments(bot) -> None:
    global _last_investment_refresh

    now = datetime.now(timezone.utc)
    if _last_investment_refresh is not None:
        elapsed = now - _last_investment_refresh
        if elapsed < timedelta(days=INVESTMENT_REFRESH_INTERVAL_DAYS):
            log.info(
                "🌙 Investment refresh skipped (last run %s ago, interval %d days)",
                elapsed,
                INVESTMENT_REFRESH_INTERVAL_DAYS,
            )
            return

    _last_investment_refresh = now
    log.info("🌙 Midnight job: refreshing investments → borrowers...")
    total_finkit = 0
    total_zaimis = 0
    errors: list[str] = []
    finkit_by_user: dict[str, int] = {}
    zaimis_by_user: dict[str, int] = {}

    try:
        creds = await list_service_credentials("finkit")
        for cred in creds:
            parser = None
            try:
                user_tag = telegram_user_tag(cred)
                parser = await ensure_finkit_parser(cred)
                if parser is None:
                    errors.append(f"Finkit login failed: {cred.login}")
                    continue

                session = await parser._get_session()
                headers = _cookie_headers(parser)
                all_investments: list[dict] = []
                page = 1
                relogged = False
                while True:
                    url = f"https://api-p2p.finkit.by/user/investments/?page={page}"
                    async with session.get(url, headers=headers) as resp:
                        if resp.status in (401, 403) and not relogged:
                            new_parser = await ensure_finkit_parser(cred, force_login=True)
                            if new_parser is None:
                                errors.append(f"Finkit re-login failed: {cred.login}")
                                break
                            try:
                                await parser.close()
                            except Exception:
                                pass
                            parser = new_parser
                            session = await parser._get_session()
                            headers = _cookie_headers(parser)
                            relogged = True
                            continue
                        if resp.status != 200:
                            break
                        data = await resp.json()
                    all_investments.extend(data.get("results", []))
                    if not data.get("next"):
                        break
                    page += 1

                log.info("Finkit %s: fetched %d investments from list", cred.login, len(all_investments))

                name_stats: dict[str, dict] = {}
                name_to_inv_ids: dict[str, list[str]] = {}
                for investment in all_investments:
                    borrower_name = (investment.get("borrower_full_name") or "").strip()
                    if not borrower_name:
                        continue
                    if borrower_name not in name_stats:
                        name_stats[borrower_name] = {
                            "total": 0,
                            "settled": 0,
                            "overdue": 0,
                            "ratings": [],
                            "invested": 0.0,
                        }
                        name_to_inv_ids[borrower_name] = []
                    stats = name_stats[borrower_name]
                    stats["total"] += 1
                    if finkit_is_settled_on_time(investment):
                        stats["settled"] += 1
                    if finkit_has_overdue_history(investment):
                        stats["overdue"] += 1
                    try:
                        stats["invested"] += float(investment.get("amount", 0))
                    except (ValueError, TypeError):
                        pass
                    try:
                        rating = float(investment.get("borrower_score", 0))
                        if rating > 0:
                            stats["ratings"].append(rating)
                    except (ValueError, TypeError):
                        pass
                    total_finkit += 1
                    finkit_by_user[user_tag] = finkit_by_user.get(user_tag, 0) + 1
                    investment_id = investment.get("id")
                    if investment_id:
                        name_to_inv_ids[borrower_name].append(investment_id)

                name_to_borrower_id = await list_borrower_name_map("finkit")
                pdf_enriched = 0
                contacts_enriched = 0
                for borrower_name, stats in name_stats.items():
                    borrower_user_id = name_to_borrower_id.get(borrower_name)
                    investment_ids = name_to_inv_ids.get(borrower_name) or []
                    if investment_ids:
                        document_id, has_contacts = await _lookup_finkit_enrichment_state(
                            borrower_name,
                            borrower_user_id,
                        )
                        enrichment_needed = not document_id or not has_contacts
                        try:
                            if enrichment_needed:
                                borrower_pdf_enriched = False
                                borrower_contacts_enriched = False
                                for investment_id in investment_ids:
                                    previous_user_id = borrower_user_id
                                    borrower_user_id, document_enriched, contacts_updated = await _enrich_finkit_borrower_from_detail(
                                        session,
                                        headers,
                                        user_tag=user_tag,
                                        borrower_name=borrower_name,
                                        investment_id=investment_id,
                                        borrower_user_id=borrower_user_id,
                                    )
                                    if borrower_user_id and borrower_user_id != previous_user_id:
                                        name_to_borrower_id[borrower_name] = borrower_user_id
                                    borrower_pdf_enriched = borrower_pdf_enriched or document_enriched
                                    borrower_contacts_enriched = borrower_contacts_enriched or contacts_updated
                                    document_id, has_contacts = await _lookup_finkit_enrichment_state(
                                        borrower_name,
                                        borrower_user_id,
                                    )
                                    await asyncio.sleep(0.2)
                                    if document_id and has_contacts:
                                        break
                                if borrower_pdf_enriched:
                                    pdf_enriched += 1
                                if borrower_contacts_enriched:
                                    contacts_enriched += 1
                        except Exception as exc:
                            log.warning("Finkit detail fetch error for %s: %s", borrower_name, exc)

                    if not borrower_user_id:
                        continue

                    avg_rating = sum(stats["ratings"]) / len(stats["ratings"]) if stats["ratings"] else None
                    await upsert_borrower_from_investment(
                        service="finkit",
                        borrower_user_id=borrower_user_id,
                        full_name=borrower_name or None,
                        total_loans=stats["total"],
                        settled_loans=stats["settled"],
                        overdue_loans=stats["overdue"],
                        avg_rating=avg_rating,
                    )

                if pdf_enriched:
                    log.info("Finkit midnight: enriched %d new borrowers with ИН from PDF", pdf_enriched)
                if contacts_enriched:
                    log.info("Finkit midnight: enriched %d borrowers with contacts from detail", contacts_enriched)
            except Exception as exc:
                errors.append(f"Finkit {cred.login}: {exc}")
            finally:
                if parser is not None:
                    try:
                        await parser.close()
                    except Exception:
                        pass
    except Exception as exc:
        errors.append(f"Finkit global: {exc}")

    try:
        creds = await list_service_credentials("zaimis")
        for cred in creds:
            try:
                user_tag = telegram_user_tag(cred)
                result = await sync_zaimis_account(
                    cred,
                    include_pdf=True,
                    sync_overdue_cases=False,
                )
                total_zaimis += result.total_orders
                zaimis_by_user[user_tag] = zaimis_by_user.get(user_tag, 0) + result.total_orders
                if result.pdf_enriched:
                    log.info("Zaimis PDF: saved %d borrowers with ИН", result.pdf_enriched)
            except Exception as exc:
                errors.append(f"Zaimis {cred.login}: {exc}")
    except Exception as exc:
        errors.append(f"Zaimis global: {exc}")

    log.info("🌙 Investments refresh done: finkit=%d items, zaimis=%d items", total_finkit, total_zaimis)

    try:
        borrowers_count = await get_borrowers_count()
        lines = [
            "🌙 <b>Ночное обновление инвестиций</b>",
            f"  Finkit: {total_finkit} записей",
            f"  Zaimis: {total_zaimis} записей",
            f"  Borrowers в БД: {borrowers_count}",
        ]
        if finkit_by_user:
            lines.append("")
            lines.append("<b>FinKit по Telegram-пользователям:</b>")
            for user_tag, count in sorted(finkit_by_user.items(), key=lambda item: (-item[1], item[0])):
                label = f"@{user_tag}" if not user_tag.startswith("chat_") else user_tag
                lines.append(f"  {escape(label)}: {count}")
        if zaimis_by_user:
            lines.append("")
            lines.append("<b>ЗАЙМись по Telegram-пользователям:</b>")
            for user_tag, count in sorted(zaimis_by_user.items(), key=lambda item: (-item[1], item[0])):
                label = f"@{user_tag}" if not user_tag.startswith("chat_") else user_tag
                lines.append(f"  {escape(label)}: {count}")
        if errors:
            lines.append(f"\n⚠️ Ошибки ({len(errors)}):")
            for error in errors[:5]:
                lines.append(f"  • {str(error)[:200]}")
        await send_admin_html_message(bot, "\n".join(lines))
    except Exception:
        pass


__all__ = ["INVESTMENT_REFRESH_INTERVAL_DAYS", "refresh_investments"]
