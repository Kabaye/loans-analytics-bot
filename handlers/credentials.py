"""Credentials handler — users input their Finkit/Zaimis login+password.
Supports multiple accounts per service."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from html import escape

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

from bot import config
from bot.services.credentials.service import (
    clone_credential_creditor_profile,
    clone_credential_signature_asset,
    delete_credential,
    get_credential_by_id,
    get_credential_creditor_profile,
    get_credential_signature_asset,
    list_credentials_for_delete,
    list_credentials_rows,
    load_investments_archive,
    save_credential_signature_asset,
    upsert_credential,
    upsert_credential_creditor_profile,
)
from bot.services.base.access import is_allowed

log = logging.getLogger(__name__)
router = Router(name="credentials")
SIGNATURES_DIR = Path(config.BASE_DIR) / "data" / "signatures"

AUTH_SERVICES = {
    "finkit": "🔵 FinKit",
    "zaimis": "🟪 ЗАЙМись",
}


class CredForm(StatesGroup):
    service = State()
    login = State()
    password = State()
    creditor_field = State()
    signature = State()


def _credential_label(row: dict) -> str:
    name = AUTH_SERVICES.get(row["service"], row["service"])
    label = f" ({row['label']})" if row.get("label") else ""
    return f"{name}: {row['login']}{label}"


def _display(value: object | None) -> str:
    if value is None:
        return "—"
    text = str(value).strip()
    return text if text else "—"


def _display_html(value: object | None) -> str:
    return escape(_display(value))


def _profile_status_icon(profile: dict | None) -> str:
    return "✅" if profile and profile.get("full_name") and profile.get("address") else "⚠️"


def _signature_status_icon(signature: dict | None) -> str:
    if not signature or not signature.get("file_path"):
        return "⚠️"
    return "♻️" if signature.get("source") == "legacy" else "✅"


async def _show_creditor_profiles(target, chat_id: int, edit: bool = False):
    rows = await list_credentials_rows(chat_id)
    if not rows:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔑 Учёт. данные", callback_data="creds_menu")],
            [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
        ])
        text = "🏦 <b>Данные займодавца и подпись</b>\n\nСначала добавьте учётные данные."
        if edit:
            await target.edit_text(text, reply_markup=kb, parse_mode="HTML")
        else:
            await target.answer(text, reply_markup=kb, parse_mode="HTML")
        return

    lines = ["🏦 <b>Данные займодавца и подпись</b>", ""]
    buttons: list[list[InlineKeyboardButton]] = []
    for row in rows:
        profile = await get_credential_creditor_profile(chat_id, int(row["id"]))
        signature = await get_credential_signature_asset(chat_id, int(row["id"]))
        status = f"{_profile_status_icon(profile)}👤 {_signature_status_icon(signature)}✍️"
        lines.append(f"{status} {_credential_label(row)}")
        buttons.append([
            InlineKeyboardButton(
                text=f"{status} {_credential_label(row)}",
                callback_data=f"cred_creditor_{row['id']}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="↩ Учёт. данные", callback_data="creds_menu")])
    text = "\n".join(lines)
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    if edit:
        await target.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await target.answer(text, reply_markup=kb, parse_mode="HTML")


async def _show_credential_creditor_detail(callback: CallbackQuery, credential_id: int):
    credential = await get_credential_by_id(callback.message.chat.id, credential_id)
    if not credential:
        await callback.answer("Логин не найден", show_alert=True)
        return
    profile = await get_credential_creditor_profile(callback.message.chat.id, credential_id)
    signature = await get_credential_signature_asset(callback.message.chat.id, credential_id)
    lines = [
        "🏦 <b>Данные займодавца</b>",
        "",
        f"<b>Логин:</b> {_display_html(_credential_label(credential))}",
        f"<b>ФИО:</b> {_display_html(profile.get('full_name') if profile else None)}",
        f"<b>Адрес:</b> {_display_html(profile.get('address') if profile else None)}",
        f"<b>Телефон:</b> {_display_html(profile.get('phone') if profile else None)}",
        f"<b>Email:</b> {_display_html(profile.get('email') if profile else None)}",
        f"<b>Подпись:</b> {'закреплена за логином' if signature and signature.get('file_path') and signature.get('source') != 'legacy' else 'используется общая' if signature and signature.get('file_path') else 'не загружена'}",
    ]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ ФИО", callback_data=f"cred_creditor_field_{credential_id}_full_name")],
        [InlineKeyboardButton(text="✏️ Адрес", callback_data=f"cred_creditor_field_{credential_id}_address")],
        [InlineKeyboardButton(text="✏️ Телефон", callback_data=f"cred_creditor_field_{credential_id}_phone")],
        [InlineKeyboardButton(text="✏️ Email", callback_data=f"cred_creditor_field_{credential_id}_email")],
        [InlineKeyboardButton(text="♻️ Скопировать профиль", callback_data=f"cred_creditor_copy_{credential_id}")],
        [InlineKeyboardButton(text="✍️ Подпись", callback_data=f"cred_signature_{credential_id}")],
        [InlineKeyboardButton(text="↩ К логинам", callback_data="cred_creditor_menu")],
    ])
    await callback.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="HTML")


async def _show_signature_detail(callback: CallbackQuery, credential_id: int, state: FSMContext | None = None):
    credential = await get_credential_by_id(callback.message.chat.id, credential_id)
    if not credential:
        await callback.answer("Логин не найден", show_alert=True)
        return
    signature = await get_credential_signature_asset(callback.message.chat.id, credential_id)
    if state is not None:
        await state.clear()
    text = (
        "✍️ <b>Подпись займодавца</b>\n\n"
        f"<b>Логин:</b> {_display_html(_credential_label(credential))}\n"
        f"<b>Статус:</b> {'закреплена за логином' if signature and signature.get('file_path') and signature.get('source') != 'legacy' else 'используется общая' if signature and signature.get('file_path') else 'не загружена'}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Загрузить подпись", callback_data=f"cred_signature_upload_{credential_id}")],
        [InlineKeyboardButton(text="♻️ Скопировать подпись", callback_data=f"cred_signature_copy_{credential_id}")],
        [InlineKeyboardButton(text="↩ К логину", callback_data=f"cred_creditor_{credential_id}")],
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


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
        buttons.append([
            InlineKeyboardButton(text="🏦 Данные займодавца и подпись", callback_data="cred_creditor_menu")
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


@router.callback_query(F.data == "cred_creditor_menu")
async def cb_cred_creditor_menu(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    await state.clear()
    await _show_creditor_profiles(callback.message, callback.message.chat.id, edit=True)


@router.callback_query(
    lambda callback: bool(callback.data)
    and callback.data.startswith("cred_creditor_")
    and not callback.data.startswith("cred_creditor_field_")
    and not callback.data.startswith("cred_creditor_copy_")
)
async def cb_cred_creditor_detail(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    credential_id = int(callback.data.replace("cred_creditor_", ""))
    await _show_credential_creditor_detail(callback, credential_id)


@router.callback_query(F.data.startswith("cred_creditor_field_"))
async def cb_cred_creditor_field(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    payload = callback.data.replace("cred_creditor_field_", "")
    credential_id_raw, field_name = payload.split("_", 1)
    credential_id = int(credential_id_raw)
    credential = await get_credential_by_id(callback.message.chat.id, credential_id)
    if not credential:
        await callback.answer("Логин не найден", show_alert=True)
        return
    profile = await get_credential_creditor_profile(callback.message.chat.id, credential_id) or {}
    field_labels = {
        "full_name": "ФИО займодавца",
        "address": "адрес займодавца",
        "phone": "телефон займодавца",
        "email": "email займодавца",
    }
    current_value = _display(profile.get(field_name))
    await state.set_state(CredForm.creditor_field)
    await state.update_data(credential_id=credential_id, creditor_field=field_name)
    await callback.message.edit_text(
        f"✏️ <b>{field_labels.get(field_name, field_name)}</b>\n\n"
        f"<b>Логин:</b> {_display_html(_credential_label(credential))}\n"
        f"<b>Текущее значение:</b> {_display_html(current_value)}\n\n"
        "Отправьте новое значение.\n"
        "Для телефона и email можно отправить <code>-</code>, чтобы очистить поле.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩ К логину", callback_data=f"cred_creditor_{credential_id}")],
        ]),
        parse_mode="HTML",
    )


@router.message(CredForm.creditor_field)
async def msg_cred_creditor_field(message: Message, state: FSMContext):
    if not await is_allowed(message.chat.id):
        return
    data = await state.get_data()
    credential_id = int(data.get("credential_id") or 0)
    field_name = str(data.get("creditor_field") or "")
    credential = await get_credential_by_id(message.chat.id, credential_id)
    if not credential or not field_name:
        await state.clear()
        await message.answer("Не удалось определить логин или поле.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏦 Данные займодавца", callback_data="cred_creditor_menu")],
        ]))
        return
    existing = await get_credential_creditor_profile(message.chat.id, credential_id) or {}
    raw_value = (message.text or "").strip()
    if field_name in {"phone", "email"} and raw_value == "-":
        value = None
    else:
        value = raw_value
    payload = {
        "full_name": existing.get("full_name"),
        "address": existing.get("address"),
        "phone": existing.get("phone"),
        "email": existing.get("email"),
    }
    payload[field_name] = value
    await upsert_credential_creditor_profile(message.chat.id, credential_id, **payload)
    await state.clear()
    await message.answer(
        "✅ Данные займодавца обновлены.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩ К логину", callback_data=f"cred_creditor_{credential_id}")],
            [InlineKeyboardButton(text="🏦 Все логины", callback_data="cred_creditor_menu")],
        ]),
    )


@router.callback_query(
    lambda callback: bool(callback.data)
    and callback.data.startswith("cred_creditor_copy_")
    and not callback.data.startswith("cred_creditor_copy_from_")
)
async def cb_cred_creditor_copy(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    target_credential_id = int(callback.data.replace("cred_creditor_copy_", ""))
    rows = await list_credentials_rows(callback.message.chat.id)
    buttons: list[list[InlineKeyboardButton]] = []
    for row in rows:
        source_id = int(row["id"])
        if source_id == target_credential_id:
            continue
        profile = await get_credential_creditor_profile(callback.message.chat.id, source_id)
        if not profile:
            continue
        buttons.append([
            InlineKeyboardButton(
                text=f"♻️ {_credential_label(row)}",
                callback_data=f"cred_creditor_copy_from_{target_credential_id}_{source_id}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="↩ К логину", callback_data=f"cred_creditor_{target_credential_id}")])
    await callback.message.edit_text(
        "♻️ <b>Скопировать профиль займодавца</b>\n\nВыберите логин-источник.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("cred_creditor_copy_from_"))
async def cb_cred_creditor_copy_from(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    payload = callback.data.replace("cred_creditor_copy_from_", "")
    target_raw, source_raw = payload.split("_", 1)
    target_credential_id = int(target_raw)
    source_credential_id = int(source_raw)
    copied = await clone_credential_creditor_profile(callback.message.chat.id, source_credential_id, target_credential_id)
    if not copied:
        await callback.answer("Источник профиля не найден", show_alert=True)
        return
    await _show_credential_creditor_detail(callback, target_credential_id)


@router.callback_query(
    lambda callback: bool(callback.data)
    and callback.data.startswith("cred_signature_")
    and not callback.data.startswith("cred_signature_upload_")
    and not callback.data.startswith("cred_signature_copy_")
)
async def cb_cred_signature(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    credential_id = int(callback.data.replace("cred_signature_", ""))
    await _show_signature_detail(callback, credential_id, state)


@router.callback_query(F.data.startswith("cred_signature_upload_"))
async def cb_cred_signature_upload(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    credential_id = int(callback.data.replace("cred_signature_upload_", ""))
    credential = await get_credential_by_id(callback.message.chat.id, credential_id)
    if not credential:
        await callback.answer("Логин не найден", show_alert=True)
        return
    await state.set_state(CredForm.signature)
    await state.update_data(credential_id=credential_id)
    await callback.message.edit_text(
        "✍️ <b>Загрузка подписи</b>\n\n"
        f"<b>Логин:</b> {_display_html(_credential_label(credential))}\n\n"
        "Пришлите изображение подписи как фото или PNG/JPG файл.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩ К подписи", callback_data=f"cred_signature_{credential_id}")],
        ]),
        parse_mode="HTML",
    )


@router.callback_query(
    lambda callback: bool(callback.data)
    and callback.data.startswith("cred_signature_copy_")
    and not callback.data.startswith("cred_signature_copy_from_")
)
async def cb_cred_signature_copy(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    target_credential_id = int(callback.data.replace("cred_signature_copy_", ""))
    rows = await list_credentials_rows(callback.message.chat.id)
    buttons: list[list[InlineKeyboardButton]] = []
    for row in rows:
        source_id = int(row["id"])
        if source_id == target_credential_id:
            continue
        signature = await get_credential_signature_asset(callback.message.chat.id, source_id)
        if not signature or not signature.get("file_path"):
            continue
        buttons.append([
            InlineKeyboardButton(
                text=f"♻️ {_credential_label(row)}",
                callback_data=f"cred_signature_copy_from_{target_credential_id}_{source_id}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="↩ К подписи", callback_data=f"cred_signature_{target_credential_id}")])
    await callback.message.edit_text(
        "♻️ <b>Скопировать подпись</b>\n\nВыберите логин-источник.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("cred_signature_copy_from_"))
async def cb_cred_signature_copy_from(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    payload = callback.data.replace("cred_signature_copy_from_", "")
    target_raw, source_raw = payload.split("_", 1)
    target_credential_id = int(target_raw)
    source_credential_id = int(source_raw)
    copied = await clone_credential_signature_asset(callback.message.chat.id, source_credential_id, target_credential_id)
    if not copied:
        await callback.answer("Источник подписи не найден", show_alert=True)
        return
    await _show_signature_detail(callback, target_credential_id)


async def _save_signature_message(message: Message, credential_id: int) -> str | None:
    SIGNATURES_DIR.mkdir(parents=True, exist_ok=True)
    bot = message.bot
    if message.photo:
        tg_file = await bot.get_file(message.photo[-1].file_id)
        ext = ".jpg"
        file_id = message.photo[-1].file_id
        unique_id = message.photo[-1].file_unique_id
        mime_type = "image/jpeg"
    elif message.document and (message.document.mime_type or "").startswith("image/"):
        tg_file = await bot.get_file(message.document.file_id)
        ext = Path(message.document.file_name or "").suffix or ".png"
        file_id = message.document.file_id
        unique_id = message.document.file_unique_id
        mime_type = message.document.mime_type
    else:
        return None
    out_path = SIGNATURES_DIR / f"{message.chat.id}_{credential_id}{ext}"
    await bot.download_file(tg_file.file_path, destination=str(out_path))
    await save_credential_signature_asset(
        message.chat.id,
        credential_id,
        file_path=str(out_path),
        mime_type=mime_type,
        telegram_file_id=file_id,
        telegram_unique_id=unique_id,
    )
    return str(out_path)


@router.message(CredForm.signature, F.photo | F.document)
async def msg_cred_signature(message: Message, state: FSMContext):
    if not await is_allowed(message.chat.id):
        return
    data = await state.get_data()
    credential_id = int(data.get("credential_id") or 0)
    credential = await get_credential_by_id(message.chat.id, credential_id)
    if not credential:
        await state.clear()
        await message.answer("Не удалось определить логин.")
        return
    saved = await _save_signature_message(message, credential_id)
    if not saved:
        await message.answer("Не удалось сохранить подпись. Пришлите фото или PNG/JPG файл.")
        return
    await state.clear()
    await message.answer(
        f"✅ Подпись сохранена для {_credential_label(credential)}.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩ К логину", callback_data=f"cred_creditor_{credential_id}")],
            [InlineKeyboardButton(text="🏦 Все логины", callback_data="cred_creditor_menu")],
        ]),
    )


@router.message(CredForm.signature)
async def msg_cred_signature_invalid(message: Message):
    if not await is_allowed(message.chat.id):
        return
    await message.answer("Пришлите подпись как фото или PNG/JPG файл.")


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
