from __future__ import annotations

import asyncio
import io
import logging
import re
from datetime import datetime, timedelta, timezone
from html import escape

import pdfplumber

from bot import config
from bot.integrations.telegram_admin import send_admin_html_message
from bot.repositories.borrowers import (
    get_borrowers_count,
    list_borrower_name_map,
    upsert_borrower,
    upsert_borrower_from_investment,
)
from bot.services.borrowers.enrichment import list_borrower_ids_with_documents
from bot.services.base.providers import (
    ensure_finkit_parser,
    ensure_zaimis_parser,
    list_service_credentials,
    telegram_user_tag,
)

log = logging.getLogger(__name__)

_NAME_ID_RE = re.compile(
    r"Я,\s+([А-ЯЁA-Z]+(?:\s+[А-ЯЁA-Z]+){2}),\s+идентификационный\s+номер\s*([0-9A-Z]+)",
    re.IGNORECASE | re.UNICODE | re.MULTILINE | re.DOTALL,
)
_last_investment_refresh: datetime | None = None
INVESTMENT_REFRESH_INTERVAL_DAYS = 3


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
                            parser = await ensure_finkit_parser(cred, force_login=True)
                            if parser is None:
                                errors.append(f"Finkit re-login failed: {cred.login}")
                                break
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
                    if investment.get("status") == "settled" and investment.get("closed", False):
                        stats["settled"] += 1
                    if investment.get("is_overdue"):
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
                for borrower_name, stats in name_stats.items():
                    borrower_user_id = name_to_borrower_id.get(borrower_name)
                    if not borrower_user_id and name_to_inv_ids.get(borrower_name):
                        investment_id = name_to_inv_ids[borrower_name][0]
                        detail_url = f"https://api-p2p.finkit.by/user/investments/{investment_id}/"
                        try:
                            async with session.get(detail_url, headers=headers) as resp:
                                if resp.status == 200:
                                    detail = await resp.json()
                                    borrower_user_id = str(detail.get("loan", investment_id))
                                    pdf_url = detail.get("latest_contract_url")
                                    if pdf_url:
                                        try:
                                            async with session.get(pdf_url) as pdf_resp:
                                                if pdf_resp.status == 200:
                                                    document_id = _extract_document_id_from_pdf(await pdf_resp.read())
                                                    if document_id:
                                                        await upsert_borrower(
                                                            service="finkit",
                                                            borrower_user_id=borrower_user_id,
                                                            full_name=borrower_name,
                                                            document_id=document_id,
                                                            source=f"finkit_archive_{user_tag}",
                                                        )
                                                        pdf_enriched += 1
                                                        log.info("Finkit midnight PDF: %s → ИН %s", borrower_name, document_id)
                                        except Exception as exc:
                                            log.debug("Finkit midnight PDF error for %s: %s", borrower_name, exc)
                            await asyncio.sleep(0.2)
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
                        total_invested=stats["invested"],
                    )

                if pdf_enriched:
                    log.info("Finkit midnight: enriched %d new borrowers with ИН from PDF", pdf_enriched)
            except Exception as exc:
                errors.append(f"Finkit {cred.login}: {exc}")
    except Exception as exc:
        errors.append(f"Finkit global: {exc}")

    try:
        creds = await list_service_credentials("zaimis")
        for cred in creds:
            try:
                user_tag = telegram_user_tag(cred)
                parser = await ensure_zaimis_parser(cred)
                if parser is None:
                    errors.append(f"Zaimis login failed: {cred.login}")
                    continue

                orders = await parser.fetch_investments()
                if parser.needs_reauth:
                    parser = await ensure_zaimis_parser(cred, force_login=True)
                    if parser is None:
                        errors.append(f"Zaimis re-login failed: {cred.login}")
                        continue
                    orders = await parser.fetch_investments()

                zaimis_stats: dict[str, dict] = {}
                for order in orders:
                    counterparty = order.get("counterparty", {}) or {}
                    borrower_user_id = str(counterparty.get("id", ""))
                    if not borrower_user_id:
                        continue
                    offer = order.get("offer", {}) or {}
                    if borrower_user_id not in zaimis_stats:
                        zaimis_stats[borrower_user_id] = {
                            "full_name": counterparty.get("displayName", ""),
                            "total": 0,
                            "settled": 0,
                            "overdue": 0,
                            "ratings": [],
                            "invested": 0.0,
                        }
                    stats = zaimis_stats[borrower_user_id]
                    stats["total"] += 1
                    state = order.get("state")
                    if state == 3:
                        stats["settled"] += 1
                    if state == 4:
                        stats["overdue"] += 1
                    try:
                        stats["invested"] += float(order.get("amount", 0))
                    except (ValueError, TypeError):
                        pass
                    score = offer.get("score")
                    if score is not None:
                        try:
                            stats["ratings"].append(float(score))
                        except (ValueError, TypeError):
                            pass
                    total_zaimis += 1
                    zaimis_by_user[user_tag] = zaimis_by_user.get(user_tag, 0) + 1

                for borrower_user_id, stats in zaimis_stats.items():
                    avg_rating = sum(stats["ratings"]) / len(stats["ratings"]) if stats["ratings"] else None
                    await upsert_borrower_from_investment(
                        service="zaimis",
                        borrower_user_id=borrower_user_id,
                        full_name=stats["full_name"] or None,
                        total_loans=stats["total"],
                        settled_loans=stats["settled"],
                        overdue_loans=stats["overdue"],
                        avg_rating=avg_rating,
                        total_invested=stats["invested"],
                    )

                try:
                    skip_counterparty_ids = await list_borrower_ids_with_documents(
                        "zaimis",
                        list(zaimis_stats.keys()),
                    )
                    pdf_results = await parser.enrich_borrowers_from_orders(
                        orders,
                        skip_counterparty_ids=skip_counterparty_ids,
                    )
                    for borrower_user_id, (full_name, document_id) in pdf_results.items():
                        await upsert_borrower(
                            service="zaimis",
                            borrower_user_id=borrower_user_id,
                            full_name=full_name,
                            document_id=document_id,
                            source=f"zaimis_archive_{user_tag}",
                        )
                    if pdf_results:
                        log.info("Zaimis PDF: saved %d borrowers with ИН", len(pdf_results))
                except Exception as exc:
                    errors.append(f"Zaimis PDF enrichment: {exc}")
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
