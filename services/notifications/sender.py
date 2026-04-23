"""Notifier — matches new borrows against subscriptions and sends TG messages."""
from __future__ import annotations

import asyncio
import json
import logging
import math
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Optional

from aiogram import Bot, Router, F
from aiogram.exceptions import TelegramRetryAfter
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)

from bot.domain.models import BorrowEntry, Subscription
from bot.repositories.borrowers import lookup_borrower
from bot.repositories.db import get_db

log = logging.getLogger(__name__)

router = Router(name="notifier")

# In-memory cache for raw_data (limited size, FIFO eviction)
_RAW_DATA_CACHE: OrderedDict[str, dict] = OrderedDict()
_RAW_DATA_CACHE_MAX = 500


def _store_raw_data(raw_data: dict | None) -> str | None:
    """Store raw_data and return a short key for the inline button callback."""
    if not raw_data:
        return None
    key = uuid.uuid4().hex[:12]
    _RAW_DATA_CACHE[key] = raw_data
    # Evict oldest if over limit
    while len(_RAW_DATA_CACHE) > _RAW_DATA_CACHE_MAX:
        _RAW_DATA_CACHE.popitem(last=False)
    return key


@router.callback_query(F.data.startswith("raw_"))
async def cb_show_raw_data(callback: CallbackQuery):
    """Show raw JSON data for a notification entry."""
    key = callback.data.replace("raw_", "")
    raw = _RAW_DATA_CACHE.get(key)
    if not raw:
        await callback.answer("⏳ Данные устарели (перезапуск бота)", show_alert=True)
        return

    import html as html_mod
    text = json.dumps(raw, ensure_ascii=False, indent=2, default=str)
    # Telegram message limit is 4096 chars
    if len(text) > 3900:
        text = text[:3900] + "\n... (обрезано)"
    escaped = html_mod.escape(text)
    await callback.message.reply(f"<pre>{escaped}</pre>", parse_mode="HTML")
    await callback.answer()

SERVICE_ICONS = {
    "kapusta": "🥬",
    "finkit": "🔵",
    "zaimis": "🟪",
}

SERVICE_NAMES = {
    "kapusta": "Kapusta",
    "finkit": "FinKit",
    "zaimis": "ЗАЙМись",
}

SERVICE_URLS = {
    "kapusta": "https://kapusta.by/borrow-requests",
    "finkit": "https://finkit.by/app/invest-manually",
    "zaimis": "https://zaimis.by/app/all-loans?tab=giveLoan",
}

# Commission rates by service (fraction of invested amount)
COMMISSION_RATES = {
    "finkit": 0.05,   # 2% + 3%
    "kapusta": 0.045,  # 4.5%
    "zaimis": 0.05,    # 5%
}

TAX_RATE = 0.13  # 13% income tax on gross profit


def _coerce_utc_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


def _entry_available_at(entry: BorrowEntry) -> datetime | None:
    return _coerce_utc_datetime(entry.created_at) or _coerce_utc_datetime(entry.updated_at)


def _subscription_is_active_for_entry(sub: Subscription, entry: BorrowEntry) -> bool:
    entry_dt = _entry_available_at(entry)
    sub_dt = _coerce_utc_datetime(sub.created_at)
    if entry_dt and sub_dt and entry_dt < sub_dt:
        return False
    return True


def _subscription_label(sub: Subscription) -> str:
    label = (sub.label or "").strip()
    if not label or any(marker in label for marker in ("�", "Ð", "Ñ", "Ã", "Â")):
        return f"подписка #{sub.id:02d}"
    return label


def _render_subscription_caption(subs: list[Subscription]) -> str:
    labels: list[str] = []
    for sub in subs:
        label = _subscription_label(sub)
        if label not in labels:
            labels.append(label)
    return " / ".join(labels) if labels else "подписка"


def _rating_marker(entry: BorrowEntry) -> str:
    score = entry.credit_score or 0
    if entry.service == "finkit":
        if score < 30:
            return "❗❗️"
        if 31 <= score <= 40:
            return "❗️"
    if entry.service == "zaimis":
        if score < 40:
            return "❗❗️"
        warn_threshold = 50 if entry.is_employed else 65
        if score < warn_threshold:
            return "❗️"
    return ""


def _format_rating_line(entry: BorrowEntry) -> str:
    marker = _rating_marker(entry)
    if marker:
        return f"{marker}<b>{entry.credit_score:.0f}</b> рейтинг"
    return f"<b>{entry.credit_score:.0f}</b> рейтинг"


def _format_opi_date(entry: BorrowEntry) -> str | None:
    dt = _coerce_utc_datetime(entry.opi_checked_at)
    return dt.strftime("%d.%m") if dt else None


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


def _build_finkit_url(entry: BorrowEntry) -> str:
    """Build a direct FinKit invest-manually URL with filters matching this loan."""
    params = []
    params.append(f"ordering=-borrower_score_value")
    amt = int(entry.amount)
    params.append(f"amount_min={amt}")
    params.append(f"amount_max={amt}")
    score = int(entry.credit_score)
    params.append(f"borrower_score_min={score}")
    params.append(f"borrower_score_max={score}")
    return "https://finkit.by/app/invest-manually?" + "&".join(params)


def _format_opi_line(entry: BorrowEntry) -> str | None:
    """Format OPI line with date and appropriate icon."""
    if entry.opi_error:
        date_str = _format_opi_date(entry)
        suffix = f" ({date_str})" if date_str else ""
        return f"⚠️ ОПИ: ошибка проверки{suffix}"
    if not entry.opi_checked and not entry.opi_checked_at:
        return None
    date_str = _format_opi_date(entry)
    suffix = f" ({date_str})" if date_str else ""
    if entry.opi_has_debt is True:
        debt = entry.opi_debt_amount or 0
        icon = "❌" if debt >= 1000 else "⚠️"
        return f"{icon} ОПИ: долг {debt:.0f}{suffix}"
    if entry.opi_has_debt is False:
        return f"✅ ОПИ: нет задолженности{suffix}"
    return f"⚠️ ОПИ: ответ не получен{suffix}"


def _format_finkit_borrower(entry: BorrowEntry) -> list[str]:
    """Format borrower section for FinKit notifications."""
    lines = []
    if entry.full_name:
        lines.append(f"\n<b>{entry.full_name}</b>")
    if entry.document_id:
        lines.append(f"🆔 ИН: {entry.document_id}")

    # Work type with status icon
    if entry.is_employed is not None:
        work_name = entry.display_name or ("Рабочий / служащий" if entry.is_employed else "Безработный")
        icon = "✅" if entry.is_employed else "⚠️"
        lines.append(f"{icon} {work_name}")

    # Settled loans from API
    settled = entry.loans_count_settled
    if settled is not None:
        icon = "✅" if settled > 0 else "⚠️"
        lines.append(f"{icon} Возвраты в срок: {settled}")

    # Overdue from API
    overdue = entry.loans_count_overdue
    if overdue is not None and overdue > 0:
        lines.append(f"⚠️ Возвраты с просрочкой: {overdue}")

    # OPI
    opi_line = _format_opi_line(entry)
    if opi_line:
        lines.append(opi_line)

    return lines


def _format_zaimis_borrower(entry: BorrowEntry) -> list[str]:
    """Format borrower section for Zaimis notifications."""
    lines = []
    if entry.display_name:
        lines.append(f"\n<b>{entry.display_name}</b>")

    # Employment
    if entry.is_employed is not None:
        icon = "✅" if entry.is_employed else "⚠️"
        text = "трудоустроен" if entry.is_employed else "безработный"
        lines.append(f"{icon}  {text}")

    # Income confirmed
    if entry.is_income_confirmed is not None:
        icon = "✅" if entry.is_income_confirmed else "⚠️"
        text = "доход подтвержден" if entry.is_income_confirmed else "доход не подтвержден"
        lines.append(f"{icon}  {text}")

    # Note / purpose
    if entry.note:
        lines.append(f"📝 {entry.note}")

    return lines


def _format_kapusta_borrower(entry: BorrowEntry) -> list[str]:
    """Format borrower section for Kapusta notifications."""
    lines = []
    if entry.display_name:
        lines.append(f"\n<b>{entry.display_name}</b>")
    return lines


def _format_enrichment_section(entry: BorrowEntry) -> list[str]:
    """Format the enrichment section with data from our DB (not from API).
    This section is shown separately to distinguish DB-sourced data."""
    lines = []
    has_enrichment = False

    # Full name from DB (only if not already shown from API)
    enriched_name = None
    enriched_doc_id = None
    enriched_opi = None
    enriched_history = False

    # For Zaimis: full_name and document_id come from our DB enrichment
    if entry.service == "zaimis":
        if entry.full_name:
            enriched_name = entry.full_name
            has_enrichment = True
        if entry.document_id:
            enriched_doc_id = entry.document_id
            has_enrichment = True

    # OPI data for non-finkit (comes from enrichment)
    if entry.service != "finkit" and entry.opi_checked:
        enriched_opi = _format_opi_line(entry)
        if enriched_opi:
            has_enrichment = True

    # KB history from our borrowers table
    if entry.kb_known and entry.kb_total_loans and entry.kb_total_loans > 0:
        enriched_history = True
        has_enrichment = True

    if not has_enrichment:
        return []

    lines.append("\n<i>Инфа из займов:</i>")
    if enriched_name:
        lines.append(f"<b>{enriched_name}</b>")
    if enriched_doc_id:
        lines.append(f"🆔 ИН: {enriched_doc_id}")
    if enriched_opi:
        lines.append(enriched_opi)

    if enriched_history:
        lines.append("История займов:")
        total = entry.kb_total_loans or 0
        settled = entry.kb_settled or 0
        overdue = entry.kb_overdue or 0
        not_returned = total - settled - overdue
        if not_returned < 0:
            not_returned = 0
        lines.append(f"ℹ️ Брала {total}")
        if settled > 0:
            lines.append(f"✅ Вернула {settled}")
        if not_returned > 0:
            lines.append(f"⚠️ Текущие {not_returned}")
        if overdue > 0:
            lines.append(f"❌ Просрочка {overdue}")

    return lines


def format_notification(entry: BorrowEntry, sub: Subscription | list[Subscription]) -> str:
    subs = sub if isinstance(sub, list) else [sub]
    icon = SERVICE_ICONS.get(entry.service, "📋")
    svc = SERVICE_NAMES.get(entry.service, entry.service)
    label = _render_subscription_caption(subs)

    p = calc_profits(entry)

    # === Block 1: Header ===
    lines = [f"{icon} <b>{svc}</b>  —  {label}", ""]

    # === Block 2: Loan info ===
    lines.append(f"<b>{entry.amount:.0f}</b> сумма")
    lines.append(f"<b>{entry.period_days}</b> д. срок")
    lines.append(f"<b>{entry.interest_day:.2f}</b> ставка ({entry.interest_year:.1f}%)")
    lines.append(_format_rating_line(entry))
    lines.append(f"<b>{p['amount_return']:.2f}</b> возврат")

    if entry.service == "zaimis" and entry.penalty_interest:
        lines.append(f"<b>{entry.penalty_interest:.2f}</b>%/д пеня")

    # === Block 3: Borrower info from API ===
    if entry.service == "finkit":
        borrower_lines = _format_finkit_borrower(entry)
    elif entry.service == "zaimis":
        borrower_lines = _format_zaimis_borrower(entry)
    else:
        borrower_lines = _format_kapusta_borrower(entry)

    if borrower_lines:
        lines.extend(borrower_lines)

    # === Block 4: Enrichment from our DB ===
    enrichment_lines = _format_enrichment_section(entry)
    if enrichment_lines:
        lines.extend(enrichment_lines)

    # === Block 5: Profit summary ===
    lines.append("")
    lines.append(f"<b>{p['gross']:.2f}</b>  прибыль (грязная)")
    lines.append(f"<b>{p['net']:.2f}</b>  прибыль (чистая)")
    lines.append(f"<b>{p['after_tax']:.2f}</b>  после налога")
    lines.append(f"{p['commission']:.2f} комисс. / {p['tax']:.2f} налог")

    # === Block 6: Link ===
    if entry.service == "finkit":
        # Direct link with loan ID + date as text
        url = _build_finkit_url(entry)
        link_text = entry.id or "Открыть"
        if entry.created_at:
            dt_str = entry.created_at.strftime("%d.%m %H:%M:%S")
            link_text = f"{entry.id}  {dt_str}"
        lines.append(f"\n<a href=\"{url}\">{link_text}</a>")
    else:
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
            ORDER BY s.created_at, s.id
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
                created_at=_coerce_utc_datetime(row["created_at"]),
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
    *,
    skip_enrichment: bool = False,
) -> list[tuple[str, int, int, list[Subscription]]]:
    """Match entries against subscriptions and send notifications.

    Returns list of (entry_id, chat_id, message_id, subs) for sent messages
    that can later be edited via update_sent_notifications().

    If skip_enrichment=True, skip the enrich_entry_from_borrowers step
    (caller handles enrichment separately).
    """
    from bot.services.fsm_guard import is_busy, enqueue

    subs = await get_active_subscriptions(service)
    if not subs:
        return []

    sent_refs: list[tuple[str, int, int, list[Subscription]]] = []
    for entry in entries:
        # Enrich from borrowers table (unless caller handles it)
        if not skip_enrichment and service != "finkit":
            await enrich_entry_from_borrowers(entry)

        # Prepare raw_data button
        raw_key = _store_raw_data(entry.raw_data)
        kb = None
        if raw_key:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📋 Исходные данные", callback_data=f"raw_{raw_key}")]
            ])

        matched_by_chat: dict[int, list[Subscription]] = {}
        for chat_id, sub in subs:
            if not _subscription_is_active_for_entry(sub, entry):
                continue
            if not sub.matches(entry):
                continue
            matched_by_chat.setdefault(chat_id, []).append(sub)

        for chat_id, matched_subs in matched_by_chat.items():
            text = format_notification(entry, matched_subs)

            # If user is busy creating a subscription, queue the notification
            if is_busy(chat_id):
                enqueue(chat_id, text, kb)
                continue

            try:
                msg = await bot.send_message(chat_id, text, parse_mode="HTML",
                                             disable_web_page_preview=True,
                                             reply_markup=kb)
                sent_refs.append((entry.id, chat_id, msg.message_id, matched_subs))
                await asyncio.sleep(0.1)
            except TelegramRetryAfter as e:
                log.warning("Flood control for %s: retry after %ds", chat_id, e.retry_after)
                await asyncio.sleep(min(e.retry_after, 30))
                try:
                    msg = await bot.send_message(chat_id, text, parse_mode="HTML",
                                                 disable_web_page_preview=True,
                                                 reply_markup=kb)
                    sent_refs.append((entry.id, chat_id, msg.message_id, matched_subs))
                except Exception:
                    pass
            except Exception as e:
                log.warning("Failed to send notification to %s: %s", chat_id, e)

    log.info("Sent %d notifications for %s", len(sent_refs), service)
    return sent_refs


async def update_sent_notifications(
    bot: Bot,
    entries: list[BorrowEntry],
    sent_refs: list[tuple[str, int, int, list[Subscription]]],
    service: str,
) -> int:
    """Edit previously sent notifications with enriched data.

    Compares new text with original; only edits if content actually changed.
    Returns count of messages edited.
    """
    if not sent_refs:
        return 0

    entry_map = {e.id: e for e in entries}
    edited = 0
    for entry_id, chat_id, message_id, matched_subs in sent_refs:
        entry = entry_map.get(entry_id)
        if not entry:
            continue

        new_text = format_notification(entry, matched_subs)

        # Rebuild raw_data button
        raw_key = _store_raw_data(entry.raw_data)
        kb = None
        if raw_key:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📋 Исходные данные", callback_data=f"raw_{raw_key}")]
            ])

        try:
            await bot.edit_message_text(
                new_text,
                chat_id=chat_id,
                message_id=message_id,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=kb,
            )
            edited += 1
            await asyncio.sleep(0.1)
        except TelegramRetryAfter as e:
            await asyncio.sleep(min(e.retry_after, 30))
            try:
                await bot.edit_message_text(
                    new_text,
                    chat_id=chat_id,
                    message_id=message_id,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                    reply_markup=kb,
                )
                edited += 1
            except Exception:
                pass
        except Exception as e:
            if "message is not modified" not in str(e).lower():
                log.warning("Failed to edit notification %s/%s: %s", chat_id, message_id, e)

    if edited:
        log.info("Edited %d/%d notifications for %s", edited, len(sent_refs), service)
    return edited
