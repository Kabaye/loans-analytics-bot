"""Search borrower handler — search by base or by document ID batch + manual add."""
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
    extract_document_ids,
    format_contact_card,
    force_refresh_opi_card,
    format_borrower_card,
    lookup_borrower_contact_info,
    lookup_borrower_info,
    run_opi_batch,
    search_borrower_info,
    upsert_borrower_info,
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
        [InlineKeyboardButton(text="📚 Поиск по базе", callback_data="search_db")],
        [InlineKeyboardButton(text="🆔 Поиск по ИН", callback_data="search_opi_batch")],
        [InlineKeyboardButton(text="➕ Добавить заёмщика", callback_data="add_borrower")],
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
    ])


def _search_nav_kb(include_add: bool = True) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="📚 Поиск по базе", callback_data="search_db")],
        [InlineKeyboardButton(text="🆔 Поиск по ИН", callback_data="search_opi_batch")],
    ]
    if include_add:
        buttons.append([InlineKeyboardButton(text="➕ Добавить заёмщика", callback_data="add_borrower")])
    buttons.append([InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _result_card_kb(doc_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Проверить ОПИ", callback_data=f"opi_check:{doc_id}")],
        [InlineKeyboardButton(text="📇 Телефон / email", callback_data=f"bi_contacts:{doc_id}")],
        [InlineKeyboardButton(text="📚 Поиск по базе", callback_data="search_db")],
        [InlineKeyboardButton(text="🆔 Поиск по ИН", callback_data="search_opi_batch")],
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
    ])


async def _batch_result_kb(doc_ids: list[str]) -> InlineKeyboardMarkup:
    del doc_ids
    return _search_nav_kb(include_add=False)


# --- Search ---

@router.callback_query(F.data == "search_borrower")
async def cb_search_start(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    await state.clear()
    await callback.message.edit_text(
        "🔍 <b>Поиск заёмщика</b>\n\n"
        "Выберите режим поиска:",
        reply_markup=_search_menu_kb(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "search_db")
async def cb_search_db(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    await state.set_state(SearchStates.waiting_query)
    await callback.message.edit_text(
        "📚 <b>Поиск по базе</b>\n\n"
        "Введите ФИО или ИН.\n"
        "После результата можно сразу вводить следующий запрос — кнопку нажимать не нужно.",
        reply_markup=_search_nav_kb(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "search_opi_batch")
async def cb_search_opi_batch(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    await state.set_state(SearchStates.waiting_batch_ids)
    await callback.message.edit_text(
        "🆔 <b>Поиск по ИН</b>\n\n"
        "Отправьте от 1 до 10 ИН одним сообщением.\n"
        "Можно в столбик, через пробел или через запятую.\n"
        "После результата можно сразу отправлять следующую пачку.",
        reply_markup=_search_nav_kb(include_add=False),
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

    results = await search_borrower_info(query, limit=10)

    if not results:
        await message.answer(
            f"🔍 По запросу «{query}» ничего не найдено.",
            reply_markup=_search_nav_kb(),
            parse_mode="HTML",
        )
        return

    if len(results) == 1:
        info = results[0]
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

    buttons.append([InlineKeyboardButton(text="📚 Поиск по базе", callback_data="search_db")])
    buttons.append([InlineKeyboardButton(text="🆔 Поиск по ИН", callback_data="search_opi_batch")])
    buttons.append([InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")])
    await message.answer(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )


@router.message(SearchStates.waiting_batch_ids)
async def msg_search_batch_ids(message: Message, state: FSMContext):
    if not await is_allowed(message.chat.id):
        return

    doc_ids = extract_document_ids(message.text or "")
    if not doc_ids:
        await message.answer(
            "❌ Не нашёл ни одного ИН. Отправьте от 1 до 10 ИН по 14 символов.",
            reply_markup=_search_nav_kb(include_add=False),
            parse_mode="HTML",
        )
        return
    if len(doc_ids) > 10:
        await message.answer(
            "❌ Слишком много ИН за раз. Отправьте не больше 10.",
            reply_markup=_search_nav_kb(include_add=False),
            parse_mode="HTML",
        )
        return

    await message.answer(
        await run_opi_batch(doc_ids),
        reply_markup=await _batch_result_kb(doc_ids),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("bi_view:"))
async def cb_view_card(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    doc_id = callback.data.split(":", 1)[1]
    info = await lookup_borrower_info(doc_id)
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

    text = format_borrower_card(info)
    if error:
        text += f"\n\n⚠️ Ошибка OPI: {error}"

    await callback.message.edit_text(
        text,
        reply_markup=_result_card_kb(doc_id),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("bi_contacts:"))
async def cb_view_contacts(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    doc_id = callback.data.split(":", 1)[1]
    info = await lookup_borrower_contact_info(doc_id)
    if not info:
        await callback.answer("Контакты не найдены", show_alert=True)
        return
    await callback.message.answer(
        format_contact_card(doc_id, info),
        parse_mode="HTML",
    )
    await callback.answer()


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
    text = format_borrower_card(info) if info else f"✅ Заёмщик {doc_id} сохранён"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📚 Поиск по базе", callback_data="search_db")],
        [InlineKeyboardButton(text="🆔 Поиск по ИН", callback_data="search_opi_batch")],
        [InlineKeyboardButton(text="➕ Ещё добавить", callback_data="add_borrower")],
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
    ])
    await callback.message.edit_text(
        f"✅ Заёмщик добавлен!\n\n{text}",
        reply_markup=kb,
        parse_mode="HTML",
    )
    await state.clear()
