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

from bot.database import get_db
from bot.services.fsm_guard import set_free, drain

log = logging.getLogger(__name__)
router = Router(name="start")

MAIN_MENU_TEXT = "🏠 <b>Главное меню</b>\nВыберите действие:"


async def is_allowed(chat_id: int) -> bool:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT 1 FROM users WHERE chat_id=? AND is_allowed=1", (chat_id,)
        )
        return len(rows) > 0
    finally:
        await db.close()


async def is_admin(chat_id: int) -> bool:
    from bot.config import ADMIN_CHAT_ID
    if chat_id == ADMIN_CHAT_ID:
        return True
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT 1 FROM users WHERE chat_id=? AND is_admin=1", (chat_id,)
        )
        return len(rows) > 0
    finally:
        await db.close()


async def ensure_user(chat_id: int, username: str | None = None,
                      first_name: str | None = None, last_name: str | None = None) -> None:
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR IGNORE INTO users (chat_id, username, first_name, last_name) VALUES (?, ?, ?, ?)",
            (chat_id, username, first_name, last_name),
        )
        await db.execute(
            "UPDATE users SET username=COALESCE(?, username), first_name=COALESCE(?, first_name), last_name=COALESCE(?, last_name) WHERE chat_id=?",
            (username, first_name, last_name, chat_id),
        )
        await db.commit()
    finally:
        await db.close()


def get_main_menu_kb(admin: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="📥 Скачать данные", callback_data="export_menu")],
        [
            InlineKeyboardButton(text="🔔 Подписки", callback_data="subs_menu"),
            InlineKeyboardButton(text="🔑 Учёт. данные", callback_data="creds_menu"),
        ],
        [
            InlineKeyboardButton(text="🔍 Поиск заёмщика", callback_data="search_borrower"),
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
    await ensure_user(
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
        "3. Готово! Уведомления будут приходить автоматически.\n\n"
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

    from bot.database import get_all_site_settings

    db = await get_db()
    try:
        subs = await db.execute_fetchall(
            "SELECT service, COUNT(*) as cnt FROM subscriptions WHERE chat_id=? AND is_active=1 GROUP BY service",
            (callback.message.chat.id,),
        )
        creds = await db.execute_fetchall(
            "SELECT service FROM credentials WHERE chat_id=?",
            (callback.message.chat.id,),
        )
    finally:
        await db.close()

    sub_lines = []
    for row in subs:
        sub_lines.append(f"  {row['service']}: {row['cnt']} подписок")

    cred_services = [r["service"] for r in creds]

    # Site settings info
    settings = await get_all_site_settings()
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
