"""Start / help handler + inline button main menu + whitelist middleware."""
from __future__ import annotations

import asyncio
import logging

from aiogram import Router, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from bot.integrations.fsm_guard import set_free, drain
from bot.services.base.access import is_admin as _is_admin_service, is_allowed as _is_allowed_service
from bot.services.start.service import ensure_chat_user, get_status_snapshot

log = logging.getLogger(__name__)
router = Router(name="start")

MAIN_MENU_TEXT = "🏠 <b>Главное меню</b>\nВыберите действие:"


async def is_allowed(chat_id: int) -> bool:
    return await _is_allowed_service(chat_id)


async def is_admin(chat_id: int) -> bool:
    return await _is_admin_service(chat_id)


def get_main_menu_kb(admin: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="📥 Скачать данные", callback_data="export_menu")],
        [
            InlineKeyboardButton(text="🔔 Подписки", callback_data="subs_menu"),
            InlineKeyboardButton(text="🔑 Учёт. данные", callback_data="creds_menu"),
        ],
        [
            InlineKeyboardButton(text="🔍 Поиск заёмщика", callback_data="search_borrower"),
            InlineKeyboardButton(text="⚖️ Просрочки", callback_data="overdue_menu"),
        ],
        [
            InlineKeyboardButton(text="ℹ️ Статус", callback_data="show_status"),
            InlineKeyboardButton(text="❓ Помощь", callback_data="show_help"),
        ],
    ]
    if admin:
        buttons.append([InlineKeyboardButton(text="👑 Администрирование", callback_data="admin_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    user = message.from_user
    await ensure_chat_user(
        message.chat.id,
        username=user.username if user else None,
        first_name=user.first_name if user else None,
        last_name=user.last_name if user else None,
    )

    if not await is_allowed(message.chat.id):
        await message.answer(
            "⛔ У вас нет доступа к боту.\n"
            f"Ваш chat_id: <code>{message.chat.id}</code>\n"
            "Отправьте его администратору для получения доступа.",
            parse_mode="HTML",
        )
        return

    await state.clear()
    admin = await is_admin(message.chat.id)
    kb = get_main_menu_kb(admin)
    await message.answer(
        "👋 Привет! Я бот для мониторинга P2P займов.\n\n" + MAIN_MENU_TEXT,
        reply_markup=kb,
        parse_mode="HTML",
    )


@router.callback_query(F.data == "main_menu")
async def cb_main_menu(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    await state.clear()
    chat_id = callback.message.chat.id
    set_free(chat_id)
    queued = drain(chat_id)
    for ntf_text, ntf_kb in queued:
        try:
            await callback.bot.send_message(chat_id, ntf_text, parse_mode="HTML",
                                            disable_web_page_preview=True,
                                            reply_markup=ntf_kb)
            await asyncio.sleep(0.1)
        except Exception as e:
            log.warning("Failed to send queued notification to %s: %s", chat_id, e)
    admin = await is_admin(callback.message.chat.id)
    kb = get_main_menu_kb(admin)
    await callback.message.edit_text(MAIN_MENU_TEXT, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "show_help")
async def cb_help(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")]
    ])
    await callback.message.edit_text(
        "<b>📖 Помощь</b>\n\n"
        "Бот периодически проверяет сайты P2P займов и отправляет уведомления "
        "о новых заявках заёмщиков, которые соответствуют вашим фильтрам.\n\n"
        "<b>Поддерживаемые сайты:</b>\n"
        "🥬 Kapusta (kapusta.by) — без регистрации\n"
        "🔵 FinKit (finkit.by) — нужен логин/пароль\n"
        "🟪 ЗАЙМись (zaimis.by) — нужен логин/пароль\n\n"
        "<b>Как начать:</b>\n"
        "1. 🔑 Учёт. данные — введите логин/пароль для FinKit/ЗАЙМись\n"
        "2. 🔔 Подписки — создайте подписки с фильтрами\n"
        "3. ⚖️ Просрочки — просматривайте просроченные кейсы и формируйте документы\n"
        "4. Готово! Уведомления будут приходить автоматически.\n\n"
        "<b>ОПИ проверка:</b>\n"
        "Для заявок на FinKit автоматически проверяется наличие исполнительных "
        "производств через ЕРИП (доступно благодаря PDF контрактам).",
        reply_markup=kb,
        parse_mode="HTML",
    )


@router.callback_query(F.data == "show_status")
async def cb_status(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    snapshot = await get_status_snapshot(callback.message.chat.id)
    subs = snapshot["subscriptions"]
    cred_services = snapshot["credential_services"]

    sub_lines = []
    for row in subs:
        sub_lines.append(f"  {row['service']}: {row['cnt']} подписок")

    # Site settings info
    settings = snapshot["site_settings"]
    site_lines = []
    for s in settings:
        status = "✅" if s.get("polling_enabled") else "⏸"
        interval = s.get("poll_interval", 60)
        site_lines.append(f"  {status} {s['service']}: интервал {interval}с")

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")]
    ])
    text = (
        "<b>📊 Ваш статус</b>\n\n"
        f"<b>Подписки:</b>\n"
        + ("\n".join(sub_lines) if sub_lines else "  Нет активных подписок")
        + "\n\n<b>Учётные данные:</b>\n"
        + (", ".join(cred_services) if cred_services else "  Не настроены")
        + "\n\n<b>Парсеры:</b>\n"
        + "\n".join(site_lines)
    )
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@router.message(Command("help"))
async def cmd_help(message: Message):
    if not await is_allowed(message.chat.id):
        return
    admin = await is_admin(message.chat.id)
    kb = get_main_menu_kb(admin)
    await message.answer(MAIN_MENU_TEXT, reply_markup=kb, parse_mode="HTML")


@router.message(Command("status"))
async def cmd_status(message: Message):
    """Redirect /status to the callback handler."""
    if not await is_allowed(message.chat.id):
        return
    admin = await is_admin(message.chat.id)
    kb = get_main_menu_kb(admin)
    await message.answer(MAIN_MENU_TEXT, reply_markup=kb, parse_mode="HTML")
