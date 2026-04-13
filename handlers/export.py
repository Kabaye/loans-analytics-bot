"""Export handler — download loans data as CSV/JSON/Excel files."""
from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
from datetime import datetime, timezone, timedelta

from aiogram import Router, F
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BufferedInputFile,
)

from bot.handlers.start import is_allowed
from bot.models import BorrowEntry
from bot.services.scheduler import get_parser
from bot.services.opi_checker import OPIChecker
from bot.database import lookup_borrower

log = logging.getLogger(__name__)
router = Router(name="export")

SERVICES = {
    "kapusta": "🥬 Kapusta",
    "finkit": "🔵 FinKit",
    "zaimis": "🟪 ЗАЙМись",
    "mongo": "🦊 Mongo",
}

# Rate limiting: last export timestamps per key
_last_export: dict[str, datetime] = {}
RATE_LIMIT_ALL = timedelta(minutes=30)
RATE_LIMIT_SERVICE = timedelta(minutes=10)

# CSV columns for export
CSV_COLUMNS = [
    "id", "service", "request_type", "amount", "period_days",
    "interest_day", "interest_year", "penalty_interest", "credit_score",
    "created_at", "profit_gross", "profit_net", "amount_return",
    "platform_fee_open", "platform_fee_close", "full_name", "document_id",
    "display_name", "is_income_confirmed", "is_employed", "has_active_loan",
    "has_overdue", "note", "status", "loans_count",
    "opi_checked", "opi_has_debt", "opi_debt_amount", "opi_full_name", "opi_error",
    "kb_known", "kb_total_loans", "kb_settled", "kb_overdue", "kb_cancelled",
    "kb_has_claims", "kb_avg_rating", "kb_last_rating", "kb_total_invested",
]


@router.callback_query(F.data == "export_menu")
async def cb_export_menu(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return

    lines = ["<b>📥 Скачать данные</b>\n"]
    lines.append("<i>Данные загружаются свежие с каждого сервиса.\n"
                 "FinKit: обогащение ПДФ + ОПИ + история заёмщиков.</i>")

    buttons = [
        [
            InlineKeyboardButton(text="🥬 Kapusta", callback_data="exp_fmt_kapusta"),
            InlineKeyboardButton(text="🔵 FinKit", callback_data="exp_fmt_finkit"),
        ],
        [
            InlineKeyboardButton(text="🟪 ЗАЙМись", callback_data="exp_fmt_zaimis"),
            InlineKeyboardButton(text="🦊 Mongo", callback_data="exp_fmt_mongo"),
        ],
        [InlineKeyboardButton(text="📁 Все — CSV", callback_data="exp_do_all_csv")],
        [InlineKeyboardButton(text="📋 Все — JSON", callback_data="exp_do_all_json")],
        [InlineKeyboardButton(text="📊 Все — Excel", callback_data="exp_do_all_xlsx")],
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
    ]
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="HTML")


# ---- Per-service format selection ----

@router.callback_query(F.data.startswith("exp_fmt_"))
async def cb_export_format_select(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return

    svc = callback.data.replace("exp_fmt_", "")
    label = SERVICES.get(svc, svc)

    buttons = [
        [InlineKeyboardButton(text="📁 CSV", callback_data=f"exp_do_{svc}_csv")],
        [InlineKeyboardButton(text="📋 JSON", callback_data=f"exp_do_{svc}_json")],
        [InlineKeyboardButton(text="📊 Excel", callback_data=f"exp_do_{svc}_xlsx")],
        [InlineKeyboardButton(text="↩ Назад", callback_data="export_menu")],
    ]
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.edit_text(
        f"<b>📥 Формат выгрузки для {label}</b>",
        reply_markup=kb,
        parse_mode="HTML",
    )


async def _collect_entries(services: list[str], chat_id: int) -> list[dict]:
    """Fetch fresh borrow + lend entries from parsers, enrich with PDF, OPI, and known borrowers."""
    all_entries: list[BorrowEntry] = []

    for svc in services:
        parser = get_parser(svc, chat_id)
        if not parser:
            log.info("Export: no parser for %s (chat_id=%s)", svc, chat_id)
            continue

        try:
            borrows = await parser.fetch_borrows()
            if borrows:
                # PDF enrichment for Finkit
                if svc == "finkit" and hasattr(parser, "enrich_with_pdf"):
                    await parser.enrich_with_pdf(borrows)

                all_entries.extend(borrows)
        except Exception as e:
            log.warning("Export: failed to fetch borrows for %s: %s", svc, e)

        try:
            lend_entries = await parser.fetch_lends()
            if lend_entries:
                all_entries.extend(lend_entries)
        except Exception as e:
            log.warning("Export: failed to fetch lends for %s: %s", svc, e)

    # OPI enrichment for entries with document_id
    entries_with_id = [e for e in all_entries if e.document_id]
    if entries_with_id:
        opi = OPIChecker()
        try:
            for entry in entries_with_id:
                try:
                    result = await asyncio.wait_for(
                        opi.check(entry.document_id), timeout=30,
                    )
                    entry.opi_checked = True
                    if result.error:
                        entry.opi_error = result.error
                        entry.opi_has_debt = None
                    else:
                        entry.opi_has_debt = result.has_debt
                        entry.opi_debt_amount = result.debt_amount
                        entry.opi_full_name = result.full_name
                except asyncio.TimeoutError:
                    entry.opi_checked = True
                    entry.opi_error = "таймаут (30с)"
                except Exception as ex:
                    entry.opi_checked = True
                    entry.opi_error = str(ex)
        finally:
            await opi.close()

    # Known borrower enrichment from borrowers table
    for entry in all_entries:
        buid = getattr(entry, "borrower_user_id", None)
        service = entry.service if hasattr(entry, "service") else None
        if not buid or not service:
            continue
        try:
            kb = await lookup_borrower(service=service, borrower_user_id=buid)
            if kb:
                entry.kb_known = True
                entry.kb_total_loans = kb.get("total_loans")
                entry.kb_settled = kb.get("settled_loans")
                entry.kb_overdue = kb.get("overdue_loans")
                entry.kb_avg_rating = kb.get("avg_rating")
                entry.kb_total_invested = kb.get("total_invested")
        except Exception as e:
            log.warning("KB enrichment error for %s: %s", entry.id, e)

    return [e.to_dict() for e in all_entries]


def _entries_to_csv(entries: list[dict]) -> bytes:
    """Convert entries to CSV bytes (UTF-8 with BOM for Excel compatibility)."""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for entry in entries:
        row = {k: entry.get(k, "") for k in CSV_COLUMNS}
        writer.writerow(row)
    return ("\ufeff" + output.getvalue()).encode("utf-8")


def _entries_to_json(entries: list[dict]) -> bytes:
    """Convert entries to pretty JSON bytes."""
    return json.dumps(entries, ensure_ascii=False, indent=2, default=str).encode("utf-8")


def _entries_to_xlsx(entries: list[dict]) -> bytes:
    """Convert entries to Excel bytes."""
    try:
        import openpyxl
        from openpyxl.utils import get_column_letter
    except ImportError:
        return _entries_to_csv(entries)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Заявки"

    for col_idx, col_name in enumerate(CSV_COLUMNS, 1):
        ws.cell(row=1, column=col_idx, value=col_name)

    for row_idx, entry in enumerate(entries, 2):
        for col_idx, col_name in enumerate(CSV_COLUMNS, 1):
            val = entry.get(col_name, "")
            if isinstance(val, (dict, list)):
                val = json.dumps(val, ensure_ascii=False)
            ws.cell(row=row_idx, column=col_idx, value=val)

    for col_idx in range(1, len(CSV_COLUMNS) + 1):
        ws.column_dimensions[get_column_letter(col_idx)].width = 15

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


FORMAT_HANDLERS = {
    "csv": (_entries_to_csv, "csv", "📁"),
    "json": (_entries_to_json, "json", "📋"),
    "xlsx": (_entries_to_xlsx, "xlsx", "📊"),
}


# ---- Unified export handler ----

@router.callback_query(F.data.startswith("exp_do_"))
async def cb_export_do(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return

    # Parse: exp_do_{service}_{format}  or  exp_do_all_{format}
    parts = callback.data.replace("exp_do_", "").rsplit("_", 1)
    if len(parts) != 2:
        await callback.answer("❌ Неверный формат")
        return

    target, fmt = parts
    services = list(SERVICES.keys()) if target == "all" else [target]
    label = "все_сервисы" if target == "all" else target

    if fmt not in FORMAT_HANDLERS:
        await callback.answer("❌ Неизвестный формат")
        return

    # Rate limiting
    now = datetime.now(timezone.utc)
    rate_key = target  # "all" or service name
    limit = RATE_LIMIT_ALL if target == "all" else RATE_LIMIT_SERVICE
    last = _last_export.get(rate_key)
    if last and (now - last) < limit:
        remaining = limit - (now - last)
        mins = int(remaining.total_seconds() // 60) + 1
        await callback.answer(f"⏳ Подождите ≈{mins} мин", show_alert=True)
        return

    converter, ext, icon = FORMAT_HANDLERS[fmt]

    await callback.message.edit_text(f"⏳ Загрузка данных ({label}, {ext})...")

    try:
        entries = await _collect_entries(services, callback.message.chat.id)
    except Exception as e:
        await callback.message.edit_text(
            f"❌ Ошибка загрузки: {e}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="↩ Назад", callback_data="export_menu")],
            ]),
        )
        return

    if not entries:
        await callback.message.edit_text(
            f"📭 Нет данных для {label}.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="↩ Назад", callback_data="export_menu")],
            ]),
        )
        return

    _last_export[rate_key] = now  # record successful export time

    file_bytes = converter(entries)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    filename = f"loans_{label}_{ts}.{ext}"

    # Count enriched stats
    kb_count = sum(1 for e in entries if e.get("kb_known"))
    opi_count = sum(1 for e in entries if e.get("opi_checked"))
    opi_err = sum(1 for e in entries if e.get("opi_error"))

    caption = f"{icon} {len(entries)} заявок ({label})"
    if kb_count:
        caption += f"\n👤 Известных заёмщиков: {kb_count}"
    if opi_count:
        caption += f"\n🔍 ОПИ проверено: {opi_count}"
    if opi_err:
        caption += f" (⚠️ ошибок: {opi_err})"

    await callback.message.answer_document(
        BufferedInputFile(file_bytes, filename=filename),
        caption=caption,
    )
    await callback.answer()
