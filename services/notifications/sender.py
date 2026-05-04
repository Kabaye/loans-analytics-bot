"""Notifier — matches new borrows against subscriptions and sends TG messages."""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone

from bot.domain.borrowers import BorrowEntry
from bot.domain.borrower_views import NotificationEntryView
from bot.domain.raw_payloads import extract_raw_payload
from bot.domain.subscriptions import Subscription
from bot.repositories.subscriptions import (
    has_active_subscriptions_for_service,
    list_active_subscriptions_for_service,
)
from bot.services.borrowers.enrichment import enrich_entry_from_borrowers
from bot.services.borrowers.source_labels import humanize_borrower_source

log = logging.getLogger(__name__)

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


@dataclass(slots=True)
class PreparedNotification:
    entry_id: str
    chat_id: int
    text: str
    matched_subscriptions: list[Subscription]
    raw_payload: dict | None = None


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


def _entry_available_at(entry: NotificationEntryView) -> datetime | None:
    return _coerce_utc_datetime(entry.created_at) or _coerce_utc_datetime(entry.updated_at)


def _subscription_is_active_for_entry(sub: Subscription, entry: NotificationEntryView) -> bool:
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


def _rating_marker(entry: NotificationEntryView) -> str:
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


def _format_rating_line(entry: NotificationEntryView) -> str:
    marker = _rating_marker(entry)
    if marker:
        return f"{marker}<b>{entry.credit_score:.0f}</b> рейтинг"
    return f"<b>{entry.credit_score:.0f}</b> рейтинг"


def _format_opi_date(entry: NotificationEntryView) -> str | None:
    dt = _coerce_utc_datetime(entry.opi_checked_at)
    return dt.strftime("%d.%m") if dt else None


def _format_scoring_date(entry: NotificationEntryView) -> str | None:
    dt = _coerce_utc_datetime(entry.scoring_assessed_at)
    return dt.strftime("%d.%m %H:%M") if dt else None


def _format_debt_load(entry: NotificationEntryView) -> str | None:
    if entry.debt_load_score is None:
        return None
    return f"{entry.debt_load_score:.2f}"


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


def calc_profits(entry: NotificationEntryView) -> dict:
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


def _build_finkit_url(entry: NotificationEntryView) -> str:
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


def _format_opi_line(entry: NotificationEntryView) -> str | None:
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


def _format_finkit_borrower(entry: NotificationEntryView) -> list[str]:
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

    if entry.is_income_confirmed is not None:
        icon = "✅" if entry.is_income_confirmed else "⚠️"
        text = "Доход заемщика подтвержден" if entry.is_income_confirmed else "Доход заемщика не подтвержден"
        lines.append(f"{icon} {text}")
    scoring_date = _format_scoring_date(entry)
    if scoring_date:
        lines.append(f"🕒 Оценка выполнена: {scoring_date}")
    debt_load = _format_debt_load(entry)
    if debt_load is not None:
        lines.append(f"📉 Долговая нагрузка: {debt_load}")

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


def _format_zaimis_borrower(entry: NotificationEntryView) -> list[str]:
    """Format borrower section for Zaimis notifications."""
    lines = []
    current_display_name = entry.current_display_name
    if current_display_name:
        lines.append(f"\n<b>{current_display_name}</b>")

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


def _format_kapusta_borrower(entry: NotificationEntryView) -> list[str]:
    """Format borrower section for Kapusta notifications."""
    lines = []
    if entry.display_name:
        lines.append(f"\n<b>{entry.display_name}</b>")
    return lines


def _format_enrichment_section(entry: NotificationEntryView) -> list[str]:
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
        if entry.display_names:
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

    source_label = humanize_borrower_source(entry.enrichment_source) or SERVICE_NAMES.get(entry.service, entry.service)
    lines.append(f"\n<i>Инфа из займов ({source_label}):</i>")
    if enriched_name:
        lines.append(f"<b>{enriched_name}</b>")
    if enriched_doc_id:
        lines.append(f"🆔 ИН: {enriched_doc_id}")
    if entry.service == "zaimis" and entry.display_names:
        labels = [str(item) for item in entry.display_names if str(item).strip()]
        if labels:
            lines.append("Ники: " + ", ".join(labels[:-1] + [f"<b>{labels[-1]}</b>"]))
    if enriched_opi:
        lines.append(enriched_opi)

    if enriched_history:
        total = entry.kb_total_loans or 0
        lines.append(f"ℹ️ Брала {total}")

    return lines


def format_notification(entry: NotificationEntryView, sub: Subscription | list[Subscription]) -> str:
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
        created_at = _coerce_utc_datetime(entry.created_at)
        if created_at:
            dt_str = created_at.strftime("%d.%m %H:%M:%S")
            link_text = f"{entry.id}  {dt_str}"
        lines.append(f"\n<a href=\"{url}\">{link_text}</a>")
    else:
        svc_url = SERVICE_URLS.get(entry.service)
        if svc_url:
            lines.append(f"\n<a href=\"{svc_url}\">Открыть</a>")

    return "\n".join(lines)


async def get_active_subscriptions(service: str) -> list[tuple[int, Subscription]]:
    return await list_active_subscriptions_for_service(service)


async def has_active_subscriptions(service: str) -> bool:
    return await has_active_subscriptions_for_service(service)


async def prepare_notifications(
    entries: list[BorrowEntry],
    service: str,
    *,
    skip_enrichment: bool = False,
) -> list[PreparedNotification]:
    subs = await get_active_subscriptions(service)
    if not subs:
        return []

    plans: list[PreparedNotification] = []
    for entry in entries:
        if not skip_enrichment and service != "finkit":
            await enrich_entry_from_borrowers(entry)
        entry_view = NotificationEntryView.from_entry(entry)

        matched_by_chat: dict[int, list[Subscription]] = {}
        for chat_id, sub in subs:
            if not _subscription_is_active_for_entry(sub, entry_view):
                continue
            if not sub.matches(entry_view):
                continue
            matched_by_chat.setdefault(chat_id, []).append(sub)

        for chat_id, matched_subs in matched_by_chat.items():
            text = format_notification(entry_view, matched_subs)
            plans.append(
                PreparedNotification(
                    entry_id=entry_view.id,
                    chat_id=chat_id,
                    text=text,
                    matched_subscriptions=matched_subs,
                    raw_payload=extract_raw_payload(entry),
                )
            )

    return plans


__all__ = [
    "PreparedNotification",
    "enrich_entry_from_borrowers",
    "format_notification",
    "get_active_subscriptions",
    "has_active_subscriptions",
    "prepare_notifications",
]
