"""Credentials handler — users input their Finkit/Zaimis login+password.
Supports multiple accounts per service."""
from __future__ import annotations

import asyncio
import logging

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from bot.repositories.credentials import (
    delete_credential,
    list_credentials_for_delete,
    list_credentials_rows,
    upsert_credential,
)
from bot.services.base.access import is_allowed
from bot.services.credentials.archive_loader import load_investments_archive

log = logging.getLogger(__name__)
router = Router(name="credentials")

AUTH_SERVICES = {
    "finkit": "🔵 FinKit",
    "zaimis": "🟪 ЗАЙМись",
}


class CredForm(StatesGroup):
    service = State()
    login = State()
    password = State()


async def _show_credentials(target, chat_id: int, edit: bool = False):
    """Show credentials list. target is Message or CallbackQuery.message."""
    rows = await list_credentials_rows(chat_id)

    lines = ["<b>🔑 Ваши учётные данные:</b>\n"]
    for svc, name in AUTH_SERVICES.items():
        svc_rows = [r for r in rows if r["service"] == svc]
        if svc_rows:
            for r in svc_rows:
                lbl = f" ({r['label']})" if r["label"] else ""
                lines.append(f"✅ {name}: {r['login']}{lbl}")
        else:
            lines.append(f"❌ {name}: не настроено")

    buttons = [
        [InlineKeyboardButton(text=f"➕ {name}",
                              callback_data=f"cred_set_{svc}")]
        for svc, name in AUTH_SERVICES.items()
    ]
    if rows:
        buttons.append([
            InlineKeyboardButton(text="🗑 Удалить учётные данные", callback_data="cred_delete_choose")
        ])
    buttons.append([InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    text = "\n".join(lines)
    if edit:
        await target.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await target.answer(text, reply_markup=kb, parse_mode="HTML")


@router.message(Command("credentials"))
async def cmd_credentials(message: Message):
    if not await is_allowed(message.chat.id):
        return
    await _show_credentials(message, message.chat.id, edit=False)


@router.callback_query(F.data == "creds_menu")
async def cb_creds_menu(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    await state.clear()
    await _show_credentials(callback.message, callback.message.chat.id, edit=True)


@router.callback_query(F.data.startswith("cred_set_"))
async def cred_set_start(callback: CallbackQuery, state: FSMContext):
    service = callback.data.replace("cred_set_", "")
    await state.update_data(service=service)
    name = AUTH_SERVICES.get(service, service)
    await callback.message.edit_text(
        f"🔑 Настройка учётных данных для <b>{name}</b>\n\n"
        "Введите логин (email для Финкит, login для Займись):",
        parse_mode="HTML",
    )
    await state.set_state(CredForm.login)


@router.message(CredForm.login)
async def cred_set_login(message: Message, state: FSMContext):
    await state.update_data(login=message.text.strip())
    await message.answer("Введите пароль:")
    await state.set_state(CredForm.password)
    try:
        await message.delete()
    except Exception:
        pass


@router.message(CredForm.password)
async def cred_set_password(message: Message, state: FSMContext):
    data = await state.get_data()
    data["password"] = message.text.strip()

    try:
        await message.delete()
    except Exception:
        pass

    credential_id = await upsert_credential(
        message.chat.id,
        data["service"],
        data["login"],
        data["password"],
    )

    await state.clear()
    name = AUTH_SERVICES.get(data["service"], data["service"])
    msg = await message.answer(
        f"✅ Учётные данные для {name} сохранены!\n"
        "⏳ Загружаю архив инвестиций...",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔑 Учёт. данные", callback_data="creds_menu")],
            [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
        ]),
    )

    # Auto-load investment archive in background
    asyncio.create_task(_autoload_investments(
        credential_id, data["service"], data["login"], data["password"], msg
    ))


async def _autoload_investments(credential_id: int, service: str, login: str, password: str, status_msg: Message):
    """Load investment archive after credential save."""
    try:
        log.info("Auto-load investments starting for %s", service)
        count = await load_investments_archive(credential_id, service, login, password)

        log.info("Auto-load investments done for %s: %d entries", service, count)
        name = AUTH_SERVICES.get(service, service)
        try:
            await status_msg.edit_text(
                f"✅ Учётные данные для {name} сохранены!\n"
                f"📦 Загружено {count} инвестиций в архив.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔑 Учёт. данные", callback_data="creds_menu")],
                    [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
                ]),
            )
        except Exception:
            pass
    except Exception as e:
        log.warning("Auto-load investments failed for %s: %s", service, e)
        try:
            name = AUTH_SERVICES.get(service, service)
            await status_msg.edit_text(
                f"✅ Учётные данные для {name} сохранены!\n"
                f"⚠️ Не удалось загрузить архив: {str(e)[:100]}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔑 Учёт. данные", callback_data="creds_menu")],
                    [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
                ]),
            )
        except Exception:
            pass


# ---- Delete credentials ----

@router.callback_query(F.data == "cred_delete_choose")
async def cred_delete_choose(callback: CallbackQuery):
    rows = await list_credentials_for_delete(callback.message.chat.id)

    buttons = []
    for row in rows:
        name = AUTH_SERVICES.get(row["service"], row["service"])
        buttons.append([
            InlineKeyboardButton(
                text=f"🗑 {name} ({row['login']})",
                callback_data=f"cred_del_{row['id']}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="↩ Учёт. данные", callback_data="creds_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.edit_text("Выберите для удаления:", reply_markup=kb)


@router.callback_query(F.data.startswith("cred_del_"))
async def cred_delete(callback: CallbackQuery):
    cred_id = int(callback.data.replace("cred_del_", ""))
    deleted = await delete_credential(cred_id, callback.message.chat.id)
    if not deleted:
        await callback.answer("❌ Не найдено")
        return
    svc = deleted["service"]
    login = deleted["login"]

    name = AUTH_SERVICES.get(svc, svc)
    await callback.message.edit_text(
        f"✅ Учётные данные {name} ({login}) удалены.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔑 Учёт. данные", callback_data="creds_menu")],
            [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
        ]),
    )


@router.callback_query(F.data == "cred_back")
async def cred_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await _show_credentials(callback.message, callback.message.chat.id, edit=True)
