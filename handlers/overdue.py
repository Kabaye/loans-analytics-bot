from __future__ import annotations

import json
import logging
from pathlib import Path
from html import escape

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from bot import config
from bot.services.overdue.service import (
    get_credential_by_id,
    get_credential_creditor_profile,
    get_overdue_case,
    get_user_signature_asset,
    list_overdue_cases,
    list_user_credentials,
    save_generated_document,
    save_user_signature_asset,
    update_overdue_case_contacts,
    upsert_credential_creditor_profile,
    upsert_overdue_case,
)
from bot.services.base.access import is_allowed
from bot.services.overdue.cases import enrich_finkit_case_from_claims, resolve_belarus_zip
from bot.services.overdue.documents import (
    build_sms_text,
    collect_claim_missing_fields,
    collect_sms_missing_fields,
    render_claim_docx,
    serialize_case_payload,
)

log = logging.getLogger(__name__)
router = Router(name="overdue")

SIGNATURES_DIR = Path(config.BASE_DIR) / "data" / "signatures"
BELPOST_INDEX_URL = "https://www.belpost.by/services/post-index.html"


class OverdueStates(StatesGroup):
    waiting_creditor_profile = State()
    waiting_signature = State()
    waiting_case_contacts = State()


def _back_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")]
    ])


def _menu_kb(has_cases: bool = True) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🗂 Список просрочек", callback_data="overdue_cases")],
        [InlineKeyboardButton(text="👤 Мой профиль", callback_data="overdue_profile")],
        [InlineKeyboardButton(text="✍️ Подпись", callback_data="overdue_signature")],
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _case_list_kb(cases: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for case in cases[:20]:
        rows.append([
            InlineKeyboardButton(
                text=_case_button_label(case),
                callback_data=f"overdue_case_{case['id']}",
            )
        ])
    rows.extend([
        [InlineKeyboardButton(text="👤 Мой профиль", callback_data="overdue_profile")],
        [InlineKeyboardButton(text="✍️ Подпись", callback_data="overdue_signature")],
        [InlineKeyboardButton(text="↩ Просрочки", callback_data="overdue_menu")],
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _case_actions_kb(case_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✉️ Сформировать SMS", callback_data=f"overdue_sms_{case_id}")],
        [InlineKeyboardButton(text="📄 Сформировать претензию", callback_data=f"overdue_claim_{case_id}")],
        [InlineKeyboardButton(text="🧾 Данные API", callback_data=f"overdue_raw_{case_id}")],
        [InlineKeyboardButton(text="📝 Данные должника", callback_data=f"overdue_edit_{case_id}")],
        [InlineKeyboardButton(text="👤 Профиль займодавца", callback_data=f"overdue_profile_case_{case_id}")],
        [InlineKeyboardButton(text="✍️ Подпись", callback_data="overdue_signature")],
        [InlineKeyboardButton(text="↩ К списку просрочек", callback_data="overdue_cases")],
    ])


def _display(value: object | None) -> str:
    if value is None:
        return "—"
    text = str(value).strip()
    return text if text else "—"


def _display_html(value: object | None) -> str:
    return escape(_display(value))


def _money(value: object | None) -> str:
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "—"


def _short_date(value: object | None) -> str:
    text = _display(value)
    if text == "—":
        return text
    return text[8:10] + "." + text[5:7] if len(text) >= 10 and text[4] == "-" else text


def _borrower_short_label(case: dict) -> str:
    full_name = (case.get("full_name") or case.get("display_name") or "").strip()
    if not full_name:
        return case.get("loan_number") or case.get("external_id") or f"case#{case['id']}"
    parts = [part for part in full_name.split() if part]
    if len(parts) == 1:
        return parts[0]
    surname = parts[0].title()
    initials = "".join(f"{part[0]}." for part in parts[1:] if part)
    return f"{surname} {initials}".strip()


def _case_button_label(case: dict) -> str:
    icon = {"finkit": "🔵", "zaimis": "🟪", "kapusta": "🥬"}.get(case.get("service"), "📄")
    due_date = _short_date(case.get("due_at"))
    amount = _money(case.get("amount"))
    total_due = _money(case.get("total_due"))
    return f"{icon} {_borrower_short_label(case)} • {due_date} • {amount} → {total_due}"


def _parse_raw(case: dict) -> dict:
    raw = case.get("raw_data")
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _case_notes(case: dict) -> list[str]:
    payload = _parse_raw(case)
    detail = payload.get("detail") or {}
    order = payload.get("order") or {}
    notes: list[str] = []

    actual_payments = detail.get("actual_payments") or order.get("actual_payments") or []
    paid_total = 0.0
    for payment in actual_payments:
        try:
            paid_total += float(payment.get("principal") or 0)
            paid_total += float(payment.get("percent") or 0)
            paid_total += float(payment.get("fine") or 0)
        except (TypeError, ValueError):
            continue
    amount = case.get("amount")
    principal = case.get("principal_outstanding")
    if paid_total > 0 or (
        isinstance(amount, (int, float)) and isinstance(principal, (int, float)) and principal < amount
    ):
        partial_value = _money(paid_total) if paid_total > 0 else "да"
        notes.append(f"<b>Частичное погашение:</b> {_display_html(partial_value)}")

    claim_sent_at = (
        detail.get("latest_claim_sent_at")
        or next((claim.get("sent_at") for claim in detail.get("claims") or [] if claim.get("sent_at")), None)
    )
    if claim_sent_at:
        notes.append(f"<b>Претензия уже отправлялась:</b> {_display_html(claim_sent_at)}")

    return notes


def _service_icon(service: str | None) -> str:
    return {"finkit": "🔵", "zaimis": "🟪", "kapusta": "🥬"}.get(service or "", "📄")


def _credential_label(credential: dict) -> str:
    service = _service_icon(credential.get("service"))
    label = credential.get("label")
    login = credential.get("login") or "—"
    return f"{service} {login}" + (f" ({label})" if label else "")


async def _get_case_creditor_profile(case: dict) -> dict | None:
    credential_id = case.get("credential_id")
    if not credential_id:
        return None
    return await get_credential_creditor_profile(case["chat_id"], int(credential_id))


async def _show_credential_profile_message(callback: CallbackQuery, credential_id: int) -> None:
    credential = await get_credential_by_id(credential_id, callback.message.chat.id)
    if not credential:
        await callback.answer("Логин не найден", show_alert=True)
        return
    profile = await get_credential_creditor_profile(callback.message.chat.id, credential_id)
    lines = [f"👤 <b>Профиль займодавца</b>", "", f"<b>Логин:</b> {_display_html(_credential_label(credential))}", ""]
    if not profile:
        lines.append("Пока не заполнен.")
    else:
        lines.extend([
            f"<b>ФИО / название:</b> {_display_html(profile.get('full_name'))}",
            f"<b>Адрес:</b> {_display_html(profile.get('address'))}",
            f"<b>Телефон:</b> {_display_html(profile.get('phone'))}",
            f"<b>Email:</b> {_display_html(profile.get('email'))}",
        ])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Заполнить / обновить", callback_data=f"overdue_profile_edit_{credential_id}")],
        [InlineKeyboardButton(text="↩ К профилям", callback_data="overdue_profile")],
    ])
    await callback.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="HTML")


async def _enrich_finkit_case_from_claims(case: dict) -> dict:
    return await enrich_finkit_case_from_claims(case)


def _format_case_text(case: dict) -> str:
    voluntary_days = case.get("voluntary_term_days")
    lines = [
        "⚖️ <b>Просроченный кейс</b>",
        "",
        f"<b>Сервис:</b> {_display_html(case.get('service'))}",
        f"<b>Аккаунт:</b> {_display_html(case.get('account_label') or case.get('credential_label') or case.get('credential_login'))}",
        f"<b>Займ / договор:</b> {_display_html(case.get('loan_number') or case.get('external_id'))}",
        f"<b>Дней просрочки:</b> {_display_html(case.get('days_overdue'))}",
        f"<b>Дата выдачи:</b> {_display_html(case.get('issued_at'))}",
        f"<b>Срок возврата:</b> {_display_html(case.get('due_at'))}",
        "",
        f"<b>Заемщик:</b> {_display_html(case.get('full_name'))}",
        f"<b>ИН:</b> {_display_html(case.get('document_id'))}",
        f"<b>Адрес:</b> {_display_html(case.get('borrower_address'))}",
        f"<b>ZIP:</b> {_display_html(case.get('borrower_zip'))}",
        f"<b>Телефон:</b> {_display_html(case.get('borrower_phone'))}",
        f"<b>Email:</b> {_display_html(case.get('borrower_email'))}",
        "",
        f"<b>Сумма займа:</b> {_money(case.get('amount'))}",
        f"<b>Основной долг:</b> {_money(case.get('principal_outstanding'))}",
        f"<b>Проценты:</b> {_money(case.get('accrued_percent'))}",
        f"<b>Пеня:</b> {_money(case.get('fine_outstanding'))}",
        f"<b>Итого:</b> {_money(case.get('total_due'))}",
        f"<b>Срок добровольного погашения:</b> {voluntary_days} дн." if voluntary_days else "<b>Срок добровольного погашения:</b> —",
    ]
    notes = _case_notes(case)
    if notes:
        lines.extend(["", *notes])
    return "\n".join(lines)


async def _show_case(callback: CallbackQuery, case_id: int) -> None:
    case = await get_overdue_case(case_id, callback.message.chat.id)
    if not case:
        await callback.answer("Кейс не найден", show_alert=True)
        return
    await callback.message.edit_text(
        _format_case_text(case),
        reply_markup=_case_actions_kb(case_id),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


@router.callback_query(F.data.startswith("overdue_raw_"))
async def cb_overdue_raw(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    case_id = int(callback.data.replace("overdue_raw_", ""))
    case = await get_overdue_case(case_id, callback.message.chat.id)
    if not case:
        await callback.answer("Кейс не найден", show_alert=True)
        return
    raw = case.get("raw_data") or "{}"
    try:
        payload = json.loads(raw)
        pretty = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    except Exception:
        pretty = str(raw)
    pretty = pretty[:3500]
    await callback.message.edit_text(
        f"🧾 <b>Данные API</b>\n\n<pre>{escape(pretty)}</pre>",
        reply_markup=_case_actions_kb(case_id),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "overdue_menu")
async def cb_overdue_menu(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    await state.clear()
    cases = await list_overdue_cases(callback.message.chat.id)
    text = (
        "⚖️ <b>Просрочки</b>\n\n"
        f"Активных кейсов: <b>{len(cases)}</b>\n"
        "Здесь можно открыть список просроченных займов, заполнить профиль займодавца по логину, "
        "загрузить подпись и вручную сформировать SMS или претензию."
    )
    await callback.message.edit_text(text, reply_markup=_menu_kb(), parse_mode="HTML")


@router.callback_query(F.data == "overdue_cases")
async def cb_overdue_cases(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    cases = await list_overdue_cases(callback.message.chat.id)
    if not cases:
        await callback.message.edit_text(
            "⚖️ <b>Просрочки</b>\n\nПока нет активных просроченных кейсов.",
            reply_markup=_menu_kb(has_cases=False),
            parse_mode="HTML",
        )
        return
    await callback.message.edit_text(
        "🗂 <b>Список просроченных кейсов</b>\n\nВыберите кейс:",
        reply_markup=_case_list_kb(cases),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("overdue_case_"))
async def cb_overdue_case(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    case_id = int(callback.data.replace("overdue_case_", ""))
    await _show_case(callback, case_id)


@router.callback_query(F.data == "overdue_profile")
async def cb_overdue_profile(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    credentials = await list_user_credentials(callback.message.chat.id, services=("finkit", "zaimis"))
    lines = ["👤 <b>Профили займодавца по логинам</b>", ""]
    if not credentials:
        lines.append("Сначала добавьте учётные данные сервиса.")
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔑 Учётные данные", callback_data="creds_menu")],
            [InlineKeyboardButton(text="↩ Просрочки", callback_data="overdue_menu")],
        ])
        await callback.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="HTML")
        return

    buttons: list[list[InlineKeyboardButton]] = []
    for credential in credentials:
        profile = await get_credential_creditor_profile(callback.message.chat.id, int(credential["id"]))
        status = "✅" if profile and profile.get("full_name") and profile.get("address") else "⚠️"
        lines.append(f"{status} {_credential_label(credential)}")
        buttons.append([
            InlineKeyboardButton(
                text=f"{status} {_credential_label(credential)}",
                callback_data=f"overdue_profile_cred_{credential['id']}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="↩ Просрочки", callback_data="overdue_menu")])
    await callback.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("overdue_profile_case_"))
async def cb_overdue_profile_case(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    case_id = int(callback.data.replace("overdue_profile_case_", ""))
    case = await get_overdue_case(case_id, callback.message.chat.id)
    if not case or not case.get("credential_id"):
        await callback.answer("Для кейса не найден логин", show_alert=True)
        return
    await _show_credential_profile_message(callback, int(case["credential_id"]))


@router.callback_query(F.data.startswith("overdue_profile_cred_"))
async def cb_overdue_profile_credential(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    credential_id = int(callback.data.replace("overdue_profile_cred_", ""))
    await _show_credential_profile_message(callback, credential_id)


@router.callback_query(F.data.startswith("overdue_profile_edit_"))
async def cb_overdue_profile_edit(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    credential_id = int(callback.data.replace("overdue_profile_edit_", ""))
    credential = await get_credential_by_id(credential_id, callback.message.chat.id)
    if not credential:
        await callback.answer("Логин не найден", show_alert=True)
        return
    await state.update_data(credential_id=credential_id)
    await state.set_state(OverdueStates.waiting_creditor_profile)
    await callback.message.edit_text(
        "👤 <b>Профиль займодавца</b>\n\n"
        f"Логин: <b>{escape(_credential_label(credential))}</b>\n\n"
        "Отправьте 4 строки:\n"
        "1. ФИО / название займодавца\n"
        "2. Адрес займодавца\n"
        "3. Телефон (или '-')\n"
        "4. Email (или '-')\n\n"
        "Пример:\n"
        "Иванов Иван Иванович\n"
        "г. Минск, ул. Лесная, д. 10, кв. 5\n"
        "+375291112233\n"
        "ivanov@example.com",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩ К профилю", callback_data=f"overdue_profile_cred_{credential_id}")],
        ]),
        parse_mode="HTML",
    )


@router.message(OverdueStates.waiting_creditor_profile)
async def msg_overdue_profile(message: Message, state: FSMContext):
    if not await is_allowed(message.chat.id):
        return
    data = await state.get_data()
    credential_id = data.get("credential_id")
    if not credential_id:
        await state.clear()
        await message.answer("Не удалось определить логин для профиля.")
        return
    parts = [line.strip() for line in (message.text or "").splitlines() if line.strip()]
    if len(parts) < 4:
        await message.answer("Нужно 4 строки: ФИО/название, адрес, телефон, email.")
        return
    full_name, address, phone, email = parts[:4]
    await upsert_credential_creditor_profile(
        message.chat.id,
        int(credential_id),
        full_name=full_name,
        address=address,
        phone=None if phone == "-" else phone,
        email=None if email == "-" else email,
    )
    await state.clear()
    await message.answer("✅ Профиль займодавца сохранён.", reply_markup=_menu_kb())


@router.callback_query(F.data == "overdue_signature")
async def cb_overdue_signature(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    signature = await get_user_signature_asset(callback.message.chat.id)
    text = (
        "✍️ <b>Подпись</b>\n\n"
        + ("Подпись уже загружена. Можно прислать новую, чтобы заменить текущую." if signature else "Подпись еще не загружена.")
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Загрузить подпись", callback_data="overdue_signature_upload")],
        [InlineKeyboardButton(text="↩ Просрочки", callback_data="overdue_menu")],
    ])
    await state.clear()
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "overdue_signature_upload")
async def cb_overdue_signature_upload(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    await state.set_state(OverdueStates.waiting_signature)
    await callback.message.edit_text(
        "✍️ <b>Загрузка подписи</b>\n\n"
        "Пришлите изображение подписи как фото или файл PNG/JPG.\n"
        "Мы сохраним его и будем автоматически вставлять в документ претензии.",
        reply_markup=_back_main_kb(),
        parse_mode="HTML",
    )


async def _save_signature_message(message: Message) -> str | None:
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

    out_path = SIGNATURES_DIR / f"{message.chat.id}{ext}"
    await bot.download_file(tg_file.file_path, destination=str(out_path))
    await save_user_signature_asset(
        message.chat.id,
        file_path=str(out_path),
        mime_type=mime_type,
        telegram_file_id=file_id,
        telegram_unique_id=unique_id,
    )
    return str(out_path)


@router.message(OverdueStates.waiting_signature, F.photo | F.document)
async def msg_overdue_signature(message: Message, state: FSMContext):
    if not await is_allowed(message.chat.id):
        return
    saved = await _save_signature_message(message)
    if not saved:
        await message.answer("Не удалось сохранить подпись. Пришлите фото или PNG/JPG файл.")
        return
    await state.clear()
    await message.answer("✅ Подпись сохранена.", reply_markup=_menu_kb())


@router.message(OverdueStates.waiting_signature)
async def msg_overdue_signature_invalid(message: Message):
    if not await is_allowed(message.chat.id):
        return
    await message.answer("Пришлите подпись как фото или изображение PNG/JPG.")


@router.callback_query(F.data.startswith("overdue_edit_"))
async def cb_overdue_edit(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    case_id = int(callback.data.replace("overdue_edit_", ""))
    case = await get_overdue_case(case_id, callback.message.chat.id)
    if not case:
        await callback.answer("Кейс не найден", show_alert=True)
        return
    await state.set_state(OverdueStates.waiting_case_contacts)
    await state.update_data(case_id=case_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔎 Поиск ZIP на Белпочте", url=BELPOST_INDEX_URL)],
        [InlineKeyboardButton(text="↩ К кейсу", callback_data=f"overdue_case_{case_id}")],
    ])
    await callback.message.edit_text(
        "📝 <b>Данные должника</b>\n\n"
        "Отправьте 5 строк именно по должнику:\n"
        "1. Адрес должника (не ваш)\n"
        "2. ZIP-код (или '-' для автопоиска)\n"
        "3. Телефон (или '-')\n"
        "4. Email (или '-')\n"
        "5. Срок добровольного погашения в днях\n\n"
        "Если ZIP не знаете, я попробую найти его автоматически. Кнопка Белпочты оставлена как запасной вариант.",
        reply_markup=kb,
        parse_mode="HTML",
    )


@router.message(OverdueStates.waiting_case_contacts)
async def msg_overdue_contacts(message: Message, state: FSMContext):
    if not await is_allowed(message.chat.id):
        return
    data = await state.get_data()
    case_id = data.get("case_id")
    parts = [line.strip() for line in (message.text or "").splitlines() if line.strip()]
    if len(parts) < 5:
        await message.answer("Нужно 5 строк: адрес, ZIP, телефон, email, срок в днях.")
        return
    address, zip_code_raw, phone, email, voluntary_days_raw = parts[:5]
    try:
        voluntary_days = int(voluntary_days_raw)
    except ValueError:
        await message.answer("Срок добровольного погашения должен быть целым числом дней.")
        return
    if voluntary_days <= 0:
        await message.answer("Срок добровольного погашения должен быть больше нуля.")
        return
    zip_code = None if zip_code_raw == "-" else zip_code_raw
    if not zip_code:
        zip_code = await resolve_belarus_zip(address)
    await update_overdue_case_contacts(
        case_id,
        message.chat.id,
        borrower_address=address,
        borrower_zip=zip_code,
        borrower_phone=None if phone == "-" else phone,
        borrower_email=None if email == "-" else email,
        voluntary_term_days=voluntary_days,
    )
    await state.clear()
    suffix = f" ZIP: {zip_code}." if zip_code else " ZIP не найден автоматически."
    await message.answer(f"✅ Данные должника сохранены.{suffix}", reply_markup=_menu_kb())


@router.callback_query(F.data.startswith("overdue_sms_"))
async def cb_overdue_sms(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    case_id = int(callback.data.replace("overdue_sms_", ""))
    case = await get_overdue_case(case_id, callback.message.chat.id)
    if not case:
        await callback.answer("Кейс не найден", show_alert=True)
        return
    missing = collect_sms_missing_fields(case, None)
    if missing:
        await callback.message.edit_text(
            "⚠️ <b>Нельзя сформировать SMS</b>\n\n"
            + "\n".join(f"• {escape(item)}" for item in missing),
            reply_markup=_case_actions_kb(case_id),
            parse_mode="HTML",
        )
        return
    sms_text = build_sms_text(case, {})
    await save_generated_document(
        case_id,
        callback.message.chat.id,
        doc_type="sms",
        text_content=sms_text,
        payload=serialize_case_payload(case, None),
    )
    await callback.message.edit_text(
        f"✉️ <b>SMS</b>\n\n<pre>{escape(sms_text)}</pre>",
        reply_markup=_case_actions_kb(case_id),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("overdue_claim_"))
async def cb_overdue_claim(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    case_id = int(callback.data.replace("overdue_claim_", ""))
    case = await get_overdue_case(case_id, callback.message.chat.id)
    signature = await get_user_signature_asset(callback.message.chat.id)
    if not case:
        await callback.answer("Кейс не найден", show_alert=True)
        return
    case = await _enrich_finkit_case_from_claims(case)
    creditor = await _get_case_creditor_profile(case)
    missing = collect_claim_missing_fields(case, creditor, signature)
    if missing:
        await save_generated_document(
            case_id,
            callback.message.chat.id,
            doc_type="claim_missing",
            payload=serialize_case_payload(case, creditor),
            missing_fields=missing,
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 Данные должника", callback_data=f"overdue_edit_{case_id}")],
            [InlineKeyboardButton(text="👤 Профиль займодавца", callback_data=f"overdue_profile_case_{case_id}")],
            [InlineKeyboardButton(text="✍️ Подпись", callback_data="overdue_signature")],
            [InlineKeyboardButton(text="↩ К кейсу", callback_data=f"overdue_case_{case_id}")],
        ])
        await callback.message.edit_text(
            "⚠️ <b>Нельзя сформировать претензию</b>\n\n"
            + "\n".join(f"• {escape(item)}" for item in missing),
            reply_markup=kb,
            parse_mode="HTML",
        )
        return

    doc_path, claim_text = render_claim_docx(case, creditor or {}, signature["file_path"])
    await save_generated_document(
        case_id,
        callback.message.chat.id,
        doc_type="claim_docx",
        file_path=str(doc_path),
        text_content=claim_text,
        payload=serialize_case_payload(case, creditor),
    )
    await callback.message.answer_document(
        FSInputFile(str(doc_path), filename=doc_path.name),
        caption="📄 Претензия сформирована.",
    )
    await _show_case(callback, case_id)
