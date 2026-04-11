"""Notifier — matches new borrows against subscriptions and sends TG messages."""
from __future__ import annotations

import asyncio
import logging
import math
from typing import Optional

from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter

from bot.database import get_db, lookup_borrower
from bot.models import BorrowEntry, Subscription

log = logging.getLogger(__name__)

SERVICE_ICONS = {
    "kapusta": "🥔",
    "finkit": "🏦",
    "mongo": "🦊",
    "zaimis": "💎",
}

SERVICE_NAMES = {
    "kapusta": "Капуста",
    "finkit": "Финкит",
    "mongo": "Монго",
    "zaimis": "Займись",
}

SERVICE_URLS = {
    "kapusta": "https://kapusta.by/borrow-requests",
    "finkit": "https://finkit.by/invest",
    "mongo": "https://mongo.by/p2p",
    "zaimis": "https://zaimis.by/app/all-loans?tab=giveLoan",
}

# Commission rates by service (fraction of invested amount)
COMMISSION_RATES = {
    "finkit": 0.05,   # 2% + 3%
    "kapusta": 0.045,  # 4.5%
    "mongo": 0.05,     # 5%
    "zaimis": 0.05,    # 5%
}

TAX_RATE = 0.13  # 13% income tax on gross profit


def _calc_commission(amount_return: float, service: str) -> float:
    """Calculate platform commission on the return amount."""
    if service == "finkit":
        p1 = math.floor(amount_return * 0.02 * 100 + 0.5) / 100
        p2 = math.floor(amount_return * 0.03 * 100 + 0.5) / 100
        return p1 + p2
    elif service == "kapusta":
        return round(amount_return * 0.045, 2)
    else:
        return round(amount_return * 0.05, 2)


def _calc_gross_profit(amount: float, daily_rate: float, days: int, service: str) -> float:
    """Calculate gross profit (total interest earned)."""
    rate_frac = daily_rate / 100
    if service == "kapusta":
        day_profit = round(amount * rate_frac, 2)
        return day_profit * days
    return amount * rate_frac * days


def calc_profits(entry: BorrowEntry) -> dict:
    """Calculate all profit metrics for an entry."""
    gross = _calc_gross_profit(entry.amount, entry.interest_day, entry.period_days, entry.service)
    amount_return = entry.amount + gross
    commission = _calc_commission(amount_return, entry.service)
    net = gross - commission
    tax = round(gross * TAX_RATE, 2)
    after_tax = net - tax

    return {
        "gross": round(gross, 2),
        "net": round(net, 2),
        "commission": round(commission, 2),
        "tax": tax,
        "after_tax": round(after_tax, 2),
        "amount_return": round(amount_return, 2),
    }


def format_notification(entry: BorrowEntry, sub: Subscription) -> str:
    icon = SERVICE_ICONS.get(entry.service, "📋")
    svc = SERVICE_NAMES.get(entry.service, entry.service)
    label = sub.label or ""

    # Recalculate profits using correct formulas
    p = calc_profits(entry)

    # Header
    lines = [f"{icon} <b>{svc}</b>  —  {label}", ""]

    # Core loan data (compact)
    lines.append(f"<b>{entry.amount:.0f}</b> сумма")
    lines.append(f"<b>{entry.period_days}</b> д. срок")
    lines.append(f"<b>{entry.interest_day:.1f}</b> ставка ({entry.interest_year:.1f}%)")
    lines.append(f"<b>{entry.credit_score:.0f}</b> рейтинг")
    lines.append(f"<b>{p['amount_return']:.2f}</b> возврат")

    # Penalty only for Zaimis
    if entry.service == "zaimis" and entry.penalty_interest:
        lines.append(f"<b>{entry.penalty_interest:.2f}</b>%/д пеня")

    # Borrower info section
    borrower_lines = []
    if entry.full_name:
        borrower_lines.append(f"\n<b>{entry.full_name}</b>")
    if entry.document_id:
        borrower_lines.append(f"🆔 ИН: <code>{entry.document_id}</code>")

    # Employment + work type
    if entry.is_employed is not None:
        emp_text = "трудоустроен" if entry.is_employed else "безработный"
        emp_icon = "✅" if entry.is_employed else "⚠️"
        if entry.display_name:
            emp_text += f", {entry.display_name}"
        borrower_lines.append(f"{emp_icon}  {emp_text}")

    # Previous nicknames (Zaimis)
    if entry.kb_display_names and len(entry.kb_display_names) > 1:
        prev = entry.kb_display_names[:-1]
        borrower_lines.append(f"📛 ранее: {', '.join(prev)}")

    # Total loans count (combined line)
    settled = entry.loans_count_settled if entry.loans_count_settled is not None else entry.kb_settled
    overdue_count = entry.loans_count_overdue if entry.loans_count_overdue is not None else entry.kb_overdue
    total_loans = entry.loans_count or entry.kb_total_loans
    if total_loans is not None or settled is not None:
        parts = []
        if total_loans is not None:
            parts.append(f"всего: {total_loans}")
        if settled is not None:
            parts.append(f"в срок: {settled}")
        if overdue_count is not None and overdue_count > 0:
            parts.append(f"просроч: {overdue_count}")
        borrower_lines.append(f"📊 займов: {', '.join(parts)}")
    elif overdue_count is not None and overdue_count > 0:
        borrower_lines.append(f"⚠️  просрочки:  {overdue_count}")

    if entry.has_overdue is not None and overdue_count is None:
        borrower_lines.append(f"{'⚠️' if entry.has_overdue else '✅'}  просрочки:  {'были' if entry.has_overdue else 'нет'}")

    if entry.has_active_loan is not None:
        borrower_lines.append(f"{'⚠️' if entry.has_active_loan else '✅'}  активный займ:  {'Да' if entry.has_active_loan else 'Нет'}")

    # Loan/repayment history (Finkit)
    if entry.has_loan_history is not None:
        borrower_lines.append(f"{'✅' if entry.has_loan_history else '❌'}  история займов:  {'да' if entry.has_loan_history else 'нет'}")
    if entry.has_repayment_history is not None:
        borrower_lines.append(f"{'✅' if entry.has_repayment_history else '❌'}  история погашений:  {'да' if entry.has_repayment_history else 'нет'}")

    # Income confirmed (Zaimis)
    if entry.is_income_confirmed is not None:
        borrower_lines.append(f"{'✅' if entry.is_income_confirmed else '❌'}  доход подтв.:  {'да' if entry.is_income_confirmed else 'нет'}")

    # OPI
    if entry.opi_checked:
        opi_date_str = ""
        if entry.opi_checked_at:
            try:
                from datetime import datetime as _dt
                _d = _dt.fromisoformat(entry.opi_checked_at)
                opi_date_str = f" (пров. {_d.strftime('%d.%m')})"
            except Exception:
                opi_date_str = ""
        if entry.opi_has_debt:
            borrower_lines.append(f"❌ ОПИ: должен <b>{entry.opi_debt_amount:.2f}</b>{opi_date_str}")
        else:
            borrower_lines.append(f"✅ ОПИ: нет задолженности{opi_date_str}")

    # Borrower info card (from Google Sheets / manual entry)
    if entry.bi_loan_status:
        status_icon = {"в срок": "✅", "просрочка": "⚠️", "все плохо": "🔴"}.get(
            entry.bi_loan_status.split()[0] if entry.bi_loan_status else "", "📋"
        )
        bi_parts = [f"{status_icon} Карточка: {entry.bi_loan_status}"]
        if entry.bi_sum_category:
            bi_parts.append(f"суммы: {entry.bi_sum_category}")
        if entry.bi_rating is not None:
            bi_parts.append(f"рейтинг: {entry.bi_rating:.0f}")
        borrower_lines.append(", ".join(bi_parts))

    # KB history
    if entry.kb_known and entry.kb_total_loans:
        kb_parts = [f"наших: {entry.kb_total_loans}"]
        if entry.kb_settled:
            kb_parts.append(f"погашено: {entry.kb_settled}")
        if entry.kb_overdue and entry.kb_overdue > 0:
            kb_parts.append(f"просрочено: {entry.kb_overdue}")
        if entry.kb_total_invested:
            kb_parts.append(f"инвест.: {entry.kb_total_invested:.0f}")
        borrower_lines.append(f"📋 {', '.join(kb_parts)}")

    if borrower_lines:
        lines.extend(borrower_lines)

    # Profit section
    lines.append("")
    lines.append(f"<b>{p['gross']:.2f}</b>  прибыль (грязная)")
    lines.append(f"<b>{p['net']:.2f}</b>  прибыль (чистая)")
    lines.append(f"<b>{p['after_tax']:.2f}</b>  после налога")
    lines.append(f"{p['commission']:.2f} комисс. / {p['tax']:.2f} налог")

    # Note/purpose
    if entry.note:
        lines.append(f"\n📝 {entry.note}")

    # Link — always use general service page
    svc_url = SERVICE_URLS.get(entry.service)
    if svc_url:
        lines.append(f"\n<a href=\"{svc_url}\">Открыть</a>")

    return "\n".join(lines)


async def get_active_subscriptions(service: str) -> list[tuple[int, Subscription]]:
    """Get all active subscriptions for a service. Returns (chat_id, Subscription) pairs."""
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT s.*, u.chat_id as user_chat_id
            FROM subscriptions s
            JOIN users u ON s.chat_id = u.chat_id
            WHERE s.service = ? AND s.is_active = 1 AND u.is_allowed = 1
            """,
            (service,),
        )
        result = []
        for row in rows:
            sub = Subscription(
                id=row["id"],
                chat_id=row["chat_id"],
                service=row["service"],
                label=row["label"],
                sum_min=row["sum_min"],
                sum_max=row["sum_max"],
                rating_min=row["rating_min"],
                period_min=row["period_min"],
                period_max=row["period_max"],
                interest_min=row["interest_min"],
                require_employed=bool(row["require_employed"]) if row["require_employed"] is not None else None,
                require_income_confirmed=bool(row["require_income_confirmed"]) if row["require_income_confirmed"] is not None else None,
                min_settled_loans=row["min_settled_loans"] if row["min_settled_loans"] else None,
            )
            result.append((row["chat_id"], sub))
        return result
    finally:
        await db.close()


async def has_active_subscriptions(service: str) -> bool:
    """Check if ANY user has active subscriptions for a service."""
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT 1 FROM subscriptions s
            JOIN users u ON s.chat_id = u.chat_id
            WHERE s.service = ? AND s.is_active = 1 AND u.is_allowed = 1
            LIMIT 1
            """,
            (service,),
        )
        return len(rows) > 0
    finally:
        await db.close()


async def enrich_entry_from_borrowers(entry: BorrowEntry) -> None:
    """Enrich a BorrowEntry with data from borrowers + borrower_info tables."""
    if not entry.borrower_user_id:
        return
    cached = await lookup_borrower(entry.service, entry.borrower_user_id)
    if not cached:
        return
    if not entry.full_name and cached.get("full_name"):
        entry.full_name = cached["full_name"]
    if not entry.document_id and cached.get("document_id"):
        entry.document_id = cached["document_id"]
    # OPI data from borrower_info (via JOIN)
    if cached.get("opi_checked_at") and not entry.opi_checked:
        entry.opi_checked = True
        entry.opi_has_debt = bool(cached.get("opi_has_debt"))
        entry.opi_debt_amount = cached.get("opi_debt_amount")
        entry.opi_full_name = cached.get("opi_full_name")
        entry.opi_checked_at = cached.get("opi_checked_at")
    # Display names history (Zaimis nicknames)
    if cached.get("display_names"):
        try:
            import json as _json
            entry.kb_display_names = _json.loads(cached["display_names"])
        except Exception:
            pass

    # Investment stats from borrowers table
    if cached.get("total_loans") and cached["total_loans"] > 0:
        entry.kb_known = True
        entry.kb_total_loans = cached.get("total_loans")
        entry.kb_settled = cached.get("settled_loans")
        entry.kb_overdue = cached.get("overdue_loans")
        entry.kb_avg_rating = cached.get("avg_rating")
        entry.kb_total_invested = cached.get("total_invested")
    # Borrower_info card data (loan history from Google Sheets)
    if cached.get("loan_status"):
        entry.bi_loan_status = cached["loan_status"]
    if cached.get("sum_category"):
        entry.bi_sum_category = cached["sum_category"]
    if cached.get("bi_rating") is not None:
        entry.bi_rating = cached["bi_rating"]


async def notify_users(
    bot: Bot,
    entries: list[BorrowEntry],
    service: str,
) -> int:
    """Match entries against subscriptions and send notifications. Returns count sent."""
    subs = await get_active_subscriptions(service)
    if not subs:
        return 0

    sent = 0
    sent_pairs: set[tuple[int, str]] = set()  # (chat_id, entry_id) dedup

    for entry in entries:
        # Enrich from borrowers table (Zaimis, Kapusta, Mongo — no PDF enrichment)
        if service != "finkit":
            await enrich_entry_from_borrowers(entry)

        for chat_id, sub in subs:
            if (chat_id, entry.id) in sent_pairs:
                continue
            if not sub.matches(entry):
                continue

            sent_pairs.add((chat_id, entry.id))

            text = format_notification(entry, sub)
            try:
                await bot.send_message(chat_id, text, parse_mode="HTML",
                                       disable_web_page_preview=True)
                sent += 1
                await asyncio.sleep(0.1)
            except TelegramRetryAfter as e:
                log.warning("Flood control for %s: retry after %ds", chat_id, e.retry_after)
                await asyncio.sleep(min(e.retry_after, 30))
                try:
                    await bot.send_message(chat_id, text, parse_mode="HTML",
                                           disable_web_page_preview=True)
                    sent += 1
                except Exception:
                    pass
            except Exception as e:
                log.warning("Failed to send notification to %s: %s", chat_id, e)

    log.info("Sent %d notifications for %s", sent, service)
    return sent
