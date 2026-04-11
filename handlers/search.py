"""Search borrower handler — search by ФИО or ИН + quick ИН check + cross-search."""
from __future__ import annotations

import logging
import re

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from bot.handlers.start import is_allowed
from bot.database import (
    search_borrower_info, lookup_borrower_info, upsert_borrower_info,
    search_borrowers,
)
from bot.services.opi_checker import OPIChecker

log = logging.getLogger(__name__)
router = Router(name="search")


class SearchStates(StatesGroup):
    waiting_query = State()


class AddBorrowerStates(StatesGroup):
    waiting_document_id = State()
    waiting_full_name = State()
    waiting_status = State()
    waiting_sum_category = State()


STATUS_OPTIONS = ["в срок", "просрочка до 20 дней", "просрочка > 20 дней", "все плохо"]
SUM_OPTIONS = ["до 300", "301-799", "больше 800"]

BACK_KB = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")]
])

# 14 alphanumeric chars = likely ИН (identification number)
IN_RE = re.compile(r"^[0-9A-Za-zА-Яа-я]{14}$")

SVC_ICONS = {"kapusta": "🥔", "finkit": "🏦", "mongo": "🦊", "zaimis": "💎"}


def _format_card(info: dict) -> str:
    """Format a borrower_info record as a readable card."""
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

    # OPI
    if info.get("opi_checked_at"):
        checked = info["opi_checked_at"][:10] if info["opi_checked_at"] else "—"
        if info.get("opi_has_debt"):
            lines.append(f"\n❌ <b>ОПИ:</b> должен <b>{info.get('opi_debt_amount', 0):.2f}</b> BYN")
            if info.get("opi_full_name"):
                lines.append(f"  Имя в ОПИ: {info['opi_full_name']}")
        else:
            lines.append("\n✅ <b>ОПИ:</b> нет задолженности")
        lines.append(f"  Проверено: {checked}")
    else:
        lines.append("\n⏳ ОПИ: не проверялось")

    if info.get("notes"):
        lines.append(f"\n📝 {info['notes']}")

    if info.get("source"):
        lines.append(f"\n<i>Источник: {info['source']}</i>")

    return "\n".join(lines)


def _format_borrower_row(b: dict, idx: int) -> str:
    """Format a borrowers table row as a compact line."""
    icon = SVC_ICONS.get(b["service"], "📋")
    name = b.get("full_name") or "—"
    doc = b.get("document_id") or "—"
    total = b.get("total_loans") or 0
    settled = b.get("settled_loans") or 0
    invested = b.get("total_invested") or 0
    return (
        f"{idx}. {icon} {name}\n"
        f"   ИН: <code>{doc}</code>\n"
        f"   Займов: {total}, погашено: {settled}, инвест.: {invested:.0f}"
    )


def _format_opi_result(result, document_id: str) -> str:
    """Format an OPI check result."""
    lines = ["<b>🔍 Проверка ОПИ</b>\n", f"<b>ИН:</b> <code>{document_id}</code>\n"]
    if result.error:
        lines.append(f"⚠️ Ошибка: {result.error}")
    elif result.has_debt:
        lines.append(f"❌ <b>Должен: {result.debt_amount:.2f} BYN</b>")
        if result.full_name:
            lines.append(f"ФИО: {result.full_name}")
    else:
        lines.append("✅ Нет задолженности")
        if result.full_name:
            lines.append(f"ФИО: {result.full_name}")
    return "\n".join(lines)


# --- Search ---

@router.callback_query(F.data == "search_borrower")
async def cb_search_start(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    await state.set_state(SearchStates.waiting_query)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить заёмщика", callback_data="add_borrower")],
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
    ])
    await callback.message.edit_text(
        "🔍 <b>Поиск заёмщика</b>\n\n"
        "Введите ФИО или ИН (14 символов → мгновенная проверка ОПИ):",
        reply_markup=kb,
        parse_mode="HTML",
    )


@router.message(SearchStates.waiting_query)
async def msg_search_query(message: Message, state: FSMContext):
    if not await is_allowed(message.chat.id):
        return

    query = message.text.strip() if message.text else ""
    if not query or len(query) < 2:
        await message.answer("❌ Слишком короткий запрос. Минимум 2 символа.")
        return

    # Quick ИН check: exactly 14 alphanumeric chars
    if IN_RE.match(query):
        await _handle_in_check(message, state, query.upper())
        return

    # Cross-search: borrower_info + borrowers tables
    bi_results = await search_borrower_info(query, limit=10)
    b_results = await search_borrowers(query, limit=10)

    if not bi_results and not b_results:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_borrower")],
            [InlineKeyboardButton(text="➕ Добавить заёмщика", callback_data="add_borrower")],
            [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
        ])
        await message.answer(
            f"🔍 По запросу «{query}» ничего не найдено.",
            reply_markup=kb,
            parse_mode="HTML",
        )
        return

    # If only one borrower_info result and no unique borrowers
    bi_doc_ids = {r["document_id"] for r in bi_results}
    unique_b = [b for b in b_results if b.get("document_id") not in bi_doc_ids]

    if len(bi_results) == 1 and not unique_b:
        await state.clear()
        info = bi_results[0]
        text = _format_card(info)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="🔄 Проверить ОПИ",
                callback_data=f"opi_check:{info['document_id']}",
            )],
            [InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_borrower")],
            [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
        ])
        await message.answer(text, reply_markup=kb, parse_mode="HTML")
        return

    # Show combined results
    await state.clear()
    lines = []
    buttons = []

    if bi_results:
        lines.append(f"<b>📋 Карточки ({len(bi_results)}):</b>\n")
        for i, r in enumerate(bi_results):
            name = r.get("full_name") or "—"
            doc = r["document_id"]
            status = r.get("loan_status") or ""
            lines.append(f"{i+1}. {name} / <code>{doc}</code> {status}")
            buttons.append([InlineKeyboardButton(
                text=f"📋 {i+1}. {name[:30]}",
                callback_data=f"bi_view:{doc}",
            )])

    if unique_b:
        lines.append(f"\n<b>👥 Заёмщики из сервисов ({len(unique_b)}):</b>\n")
        for j, b in enumerate(unique_b):
            lines.append(_format_borrower_row(b, len(bi_results) + j + 1))
            lines.append("")

    buttons.append([InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_borrower")])
    buttons.append([InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.answer("\n".join(lines), reply_markup=kb, parse_mode="HTML")


async def _handle_in_check(message: Message, state: FSMContext, document_id: str):
    """Handle quick ИН check: lookup DB, then instant OPI check."""
    await state.clear()

    # First check if already in borrower_info
    existing = await lookup_borrower_info(document_id)
    if existing:
        # Show existing card — user can re-check OPI from there
        text = _format_card(existing)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="🔄 Проверить ОПИ",
                callback_data=f"opi_check:{document_id}",
            )],
            [InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_borrower")],
            [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
        ])
        await message.answer(text, reply_markup=kb, parse_mode="HTML")
        return

    # Not in DB — instant OPI check
    status_msg = await message.answer("⏳ Проверяю ОПИ по ЕРИП...")

    checker = OPIChecker()
    try:
        result = await checker.check(document_id, use_cache=False)
    except Exception as ex:
        log.warning("Quick ИН check failed for %s: %s", document_id, ex)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_borrower")],
            [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
        ])
        await status_msg.edit_text(f"⚠️ Ошибка при проверке: {ex}", reply_markup=kb)
        return
    finally:
        await checker.close()

    text = _format_opi_result(result, document_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="➕ Добавить в базу",
            callback_data=f"quick_add:{document_id}",
        )],
        [InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_borrower")],
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
    ])
    await status_msg.edit_text(text, reply_markup=kb, parse_mode="HTML")


# --- Quick add after ИН check ---

@router.callback_query(F.data.startswith("quick_add:"))
async def cb_quick_add(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    doc_id = callback.data.split(":", 1)[1]

    # Check if already exists
    existing = await lookup_borrower_info(doc_id)
    if existing:
        await callback.answer("Уже есть в базе", show_alert=True)
        return

    await state.update_data(document_id=doc_id)
    await state.set_state(AddBorrowerStates.waiting_full_name)
    await callback.message.edit_text(
        f"➕ <b>Добавить заёмщика</b>\n\n"
        f"ИН: <code>{doc_id}</code>\n\n"
        "Введите ФИО заёмщика:",
        reply_markup=BACK_KB,
        parse_mode="HTML",
    )


# --- View card ---

@router.callback_query(F.data.startswith("bi_view:"))
async def cb_view_card(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    doc_id = callback.data.split(":", 1)[1]
    info = await lookup_borrower_info(doc_id)
    if not info:
        await callback.answer("Карточка не найдена", show_alert=True)
        return

    text = _format_card(info)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🔄 Проверить ОПИ",
            callback_data=f"opi_check:{doc_id}",
        )],
        [InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_borrower")],
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


# --- OPI check ---

@router.callback_query(F.data.startswith("opi_check:"))
async def cb_opi_check(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    doc_id = callback.data.split(":", 1)[1]
    await callback.answer("⏳ Проверяю ОПИ...", show_alert=False)

    checker = OPIChecker()
    try:
        result = await checker.check(doc_id, use_cache=False)
    finally:
        await checker.close()

    # Reload card after OPI check
    info = await lookup_borrower_info(doc_id)
    if not info:
        await callback.answer("Ошибка: карточка не найдена", show_alert=True)
        return

    text = _format_card(info)
    if result.error:
        text += f"\n\n⚠️ Ошибка OPI: {result.error}"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🔄 Проверить ОПИ",
            callback_data=f"opi_check:{doc_id}",
        )],
        [InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_borrower")],
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


# --- Add borrower ---

@router.callback_query(F.data == "add_borrower")
async def cb_add_start(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    await state.set_state(AddBorrowerStates.waiting_document_id)
    await callback.message.edit_text(
        "➕ <b>Добавить заёмщика</b>\n\n"
        "Введите ИН (идентификационный номер, 14 символов):",
        reply_markup=BACK_KB,
        parse_mode="HTML",
    )


@router.message(AddBorrowerStates.waiting_document_id)
async def msg_add_document_id(message: Message, state: FSMContext):
    if not await is_allowed(message.chat.id):
        return
    doc_id = message.text.strip().upper() if message.text else ""
    if len(doc_id) != 14:
        await message.answer(
            "❌ ИН должен быть ровно 14 символов.\nПопробуйте ещё раз:",
            reply_markup=BACK_KB,
        )
        return

    # Check if already exists
    existing = await lookup_borrower_info(doc_id)
    if existing:
        text = _format_card(existing)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_borrower")],
            [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
        ])
        await message.answer(
            f"ℹ️ Заёмщик с таким ИН уже есть в базе:\n\n{text}",
            reply_markup=kb,
            parse_mode="HTML",
        )
        await state.clear()
        return

    await state.update_data(document_id=doc_id)
    await state.set_state(AddBorrowerStates.waiting_full_name)
    await message.answer(
        f"ИН: <code>{doc_id}</code>\n\nВведите ФИО заёмщика:",
        reply_markup=BACK_KB,
        parse_mode="HTML",
    )


@router.message(AddBorrowerStates.waiting_full_name)
async def msg_add_full_name(message: Message, state: FSMContext):
    if not await is_allowed(message.chat.id):
        return
    name = message.text.strip() if message.text else ""
    if not name:
        await message.answer("❌ Введите ФИО:")
        return

    await state.update_data(full_name=name)
    await state.set_state(AddBorrowerStates.waiting_status)

    buttons = [[InlineKeyboardButton(text=s, callback_data=f"add_status:{s}")] for s in STATUS_OPTIONS]
    buttons.append([InlineKeyboardButton(text="⏩ Пропустить", callback_data="add_status:skip")])
    buttons.append([InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)

    await message.answer(
        f"ФИО: <b>{name}</b>\n\nВыберите статус по займам:",
        reply_markup=kb,
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("add_status:"), AddBorrowerStates.waiting_status)
async def cb_add_status(callback: CallbackQuery, state: FSMContext):
    status = callback.data.split(":", 1)[1]
    if status == "skip":
        status = None
    await state.update_data(loan_status=status)
    await state.set_state(AddBorrowerStates.waiting_sum_category)

    buttons = [[InlineKeyboardButton(text=s, callback_data=f"add_sum:{s}")] for s in SUM_OPTIONS]
    buttons.append([InlineKeyboardButton(text="⏩ Пропустить", callback_data="add_sum:skip")])
    buttons.append([InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)

    await callback.message.edit_text(
        "Выберите категорию сумм:",
        reply_markup=kb,
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("add_sum:"), AddBorrowerStates.waiting_sum_category)
async def cb_add_sum(callback: CallbackQuery, state: FSMContext):
    sum_cat = callback.data.split(":", 1)[1]
    if sum_cat == "skip":
        sum_cat = None

    data = await state.get_data()
    doc_id = data["document_id"]
    full_name = data["full_name"]
    loan_status = data.get("loan_status")

    # Save to DB
    await upsert_borrower_info(
        document_id=doc_id,
        full_name=full_name,
        loan_status=loan_status,
        sum_category=sum_cat,
        source="manual",
    )

    # Auto-check OPI
    await callback.message.edit_text("⏳ Сохраняю и проверяю ОПИ...", parse_mode="HTML")

    checker = OPIChecker()
    try:
        await checker.check(doc_id, use_cache=False)
    except Exception as ex:
        log.warning("OPI check on add failed for %s: %s", doc_id, ex)
    finally:
        await checker.close()

    # Show result card
    info = await lookup_borrower_info(doc_id)
    text = _format_card(info) if info else f"✅ Заёмщик {doc_id} сохранён"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск", callback_data="search_borrower")],
        [InlineKeyboardButton(text="➕ Ещё добавить", callback_data="add_borrower")],
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
    ])
    await callback.message.edit_text(
        f"✅ Заёмщик добавлен!\n\n{text}",
        reply_markup=kb,
        parse_mode="HTML",
    )
    await state.clear()
