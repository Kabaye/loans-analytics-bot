"""Unified borrower search handler — DB search, INN batch lookup, and manual add."""
from __future__ import annotations

import logging

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from bot.services.search.service import (
    add_borrower_and_refresh_opi,
    build_borrower_card_payload,
    extract_document_id_batch,
    force_refresh_opi_card,
    format_borrower_card,
    lookup_borrower_info,
    run_document_lookup_batch,
    search_borrower_info,
)
from bot.services.base.access import is_allowed

log = logging.getLogger(__name__)
router = Router(name="search")


class SearchStates(StatesGroup):
    waiting_query = State()
    waiting_batch_ids = State()


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


def _search_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить заёмщика", callback_data="add_borrower")],
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
    ])


def _search_nav_kb(include_add: bool = True) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_borrower")],
    ]
    if include_add:
        buttons.append([InlineKeyboardButton(text="➕ Добавить заёмщика", callback_data="add_borrower")])
    buttons.append([InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _result_card_kb(doc_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Проверить ОПИ", callback_data=f"opi_check:{doc_id}")],
        [InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_borrower")],
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
    ])


def _batch_result_kb(doc_ids: list[str]) -> InlineKeyboardMarkup:
    del doc_ids
    return _search_nav_kb(include_add=False)


SEARCH_PROMPT_TEXT = (
    "🔍 <b>Поиск заёмщика</b>\n\n"
    "Отправьте ФИО, ИН или пачку из 1-10 ИН.\n"
    "Если пришли только ИН — бот сначала ищет их в локальной базе, а для отсутствующих карточек "
    "делает проверку ОПИ и сохраняет результат.\n"
    "Если пришел обычный текст — бот ищет по базе как раньше.\n\n"
    "После результата можно сразу отправлять следующий запрос."
)


async def _open_search_prompt(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(SearchStates.waiting_query)
    await callback.message.edit_text(
        SEARCH_PROMPT_TEXT,
        reply_markup=_search_menu_kb(),
        parse_mode="HTML",
    )


# --- Search ---

@router.callback_query(F.data == "search_borrower")
async def cb_search_start(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    await state.clear()
    await _open_search_prompt(callback, state)


@router.callback_query(F.data == "search_db")
async def cb_search_db(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    await _open_search_prompt(callback, state)


@router.callback_query(F.data == "search_opi_batch")
async def cb_search_opi_batch(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    await _open_search_prompt(callback, state)


@router.message(SearchStates.waiting_query)
@router.message(SearchStates.waiting_batch_ids)
async def msg_search_query(message: Message, state: FSMContext):
    if not await is_allowed(message.chat.id):
        return

    query = message.text.strip() if message.text else ""
    if not query or len(query) < 2:
        await message.answer("❌ Слишком короткий запрос. Минимум 2 символа.")
        return

    batch_doc_ids = extract_document_id_batch(query)
    if batch_doc_ids:
        if len(batch_doc_ids) > 10:
            await message.answer(
                "❌ Слишком много ИН за раз. Отправьте не больше 10.",
                reply_markup=_search_nav_kb(include_add=False),
                parse_mode="HTML",
            )
            return
        await state.set_state(SearchStates.waiting_query)
        await message.answer(
            await run_document_lookup_batch(batch_doc_ids),
            reply_markup=_batch_result_kb(batch_doc_ids),
            parse_mode="HTML",
        )
        return

    results = await search_borrower_info(query, limit=10)

    if not results:
        await message.answer(
            f"🔍 По запросу «{query}» ничего не найдено.",
            reply_markup=_search_nav_kb(),
            parse_mode="HTML",
        )
        return

    if len(results) == 1:
        info = await build_borrower_card_payload(results[0]["document_id"]) or results[0]
        await message.answer(
            format_borrower_card(info),
            reply_markup=_result_card_kb(info["document_id"]),
            parse_mode="HTML",
        )
        return

    lines = [f"🔍 Найдено <b>{len(results)}</b> результатов:\n"]
    buttons = []
    for i, row in enumerate(results):
        name = row.get("full_name") or row.get("current_display_name") or "—"
        doc = row["document_id"]
        status = row.get("loan_status") or ""
        lines.append(f"{i+1}. {name} / <code>{doc}</code> {status}")
        buttons.append([
            InlineKeyboardButton(
                text=f"{i+1}. {name[:30]}",
                callback_data=f"bi_view:{doc}",
            )
        ])

    buttons.append([InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_borrower")])
    buttons.append([InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")])
    await message.answer(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("bi_view:"))
async def cb_view_card(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    doc_id = callback.data.split(":", 1)[1]
    info = await build_borrower_card_payload(doc_id)
    if not info:
        await callback.answer("Карточка не найдена", show_alert=True)
        return

    await callback.message.edit_text(
        format_borrower_card(info),
        reply_markup=_result_card_kb(doc_id),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("opi_check:"))
async def cb_opi_check(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    doc_id = callback.data.split(":", 1)[1]
    await callback.answer("⏳ Проверяю ОПИ...", show_alert=False)

    info, error = await force_refresh_opi_card(doc_id)
    if not info:
        await callback.answer("Ошибка: карточка не найдена", show_alert=True)
        return
    info = await build_borrower_card_payload(doc_id) or info

    text = format_borrower_card(info)
    if error:
        text += f"\n\n⚠️ Ошибка OPI: {error}"

    await callback.message.edit_text(
        text,
        reply_markup=_result_card_kb(doc_id),
        parse_mode="HTML",
    )


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

    existing = await lookup_borrower_info(doc_id)
    if existing:
        await message.answer(
            f"ℹ️ Заёмщик с таким ИН уже есть в базе:\n\n{format_borrower_card(existing)}",
            reply_markup=_search_nav_kb(include_add=False),
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

    await message.answer(
        f"ФИО: <b>{name}</b>\n\nВыберите статус по займам:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
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

    await callback.message.edit_text(
        "Выберите категорию сумм:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
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

    await callback.message.edit_text("⏳ Сохраняю и проверяю ОПИ...", parse_mode="HTML")
    try:
        info = await add_borrower_and_refresh_opi(doc_id, full_name, loan_status, sum_cat)
    except Exception as ex:
        log.warning("OPI check on add failed for %s: %s", doc_id, ex)
        info = await lookup_borrower_info(doc_id)
    if info:
        info = await build_borrower_card_payload(doc_id) or info
    text = format_borrower_card(info) if info else f"✅ Заёмщик {doc_id} сохранён"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_borrower")],
        [InlineKeyboardButton(text="➕ Ещё добавить", callback_data="add_borrower")],
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
    ])
    await callback.message.edit_text(
        f"✅ Заёмщик добавлен!\n\n{text}",
        reply_markup=kb,
        parse_mode="HTML",
    )
    await state.clear()
