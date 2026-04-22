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
from bot.database import (
    get_creditor_profile,
    get_overdue_case,
    get_user_signature_asset,
    list_overdue_cases,
    save_generated_document,
    save_user_signature_asset,
    upsert_creditor_profile,
    update_overdue_case_contacts,
)
from bot.handlers.start import is_allowed
from bot.services.debtor_documents import (
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
        label = case.get("loan_number") or case.get("external_id") or f"case#{case['id']}"
        due = case.get("total_due")
        due_text = f"{float(due):.2f} BYN" if due is not None else "—"
        days = case.get("days_overdue") or 0
        icon = {"finkit": "🔵", "zaimis": "🟪", "kapusta": "🥬"}.get(case.get("service"), "📄")
        rows.append([
            InlineKeyboardButton(
                text=f"{icon} {label} • {days}д • {due_text}",
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
        [InlineKeyboardButton(text="👤 Мой профиль", callback_data="overdue_profile")],
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


def _format_case_text(case: dict) -> str:
    lines = [
        "⚖️ <b>Просроченный кейс</b>",
        "",
        f"<b>Сервис:</b> {_display_html(case.get('service'))}",
        f"<b>Аккаунт:</b> {_display_html(case.get('account_label') or case.get('credential_label') or case.get('credential_login'))}",
        f"<b>Займ / договор:</b> {_display_html(case.get('loan_number') or case.get('external_id'))}",
        f"<b>Статус:</b> {_display_html(case.get('status'))}",
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
        f"<b>Сумма займа:</b> {_display_html(case.get('amount'))}",
        f"<b>Основной долг:</b> {_display_html(case.get('principal_outstanding'))}",
        f"<b>Проценты:</b> {_display_html(case.get('accrued_percent'))}",
        f"<b>Пеня:</b> {_display_html(case.get('fine_outstanding'))}",
        f"<b>Итого:</b> {_display_html(case.get('total_due'))}",
        f"<b>Срок добровольного погашения:</b> {_display_html(case.get('voluntary_term_days'))} дн.",
    ]
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
        "Здесь можно открыть список просроченных займов, заполнить профиль кредитора, "
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
    profile = await get_creditor_profile(callback.message.chat.id)
    lines = ["👤 <b>Профиль кредитора</b>", ""]
    if not profile:
        lines.append("Пока не заполнен.")
    else:
        lines.extend([
            f"<b>ФИО / название:</b> {_display(profile.get('full_name'))}",
            f"<b>Адрес:</b> {_display(profile.get('address'))}",
            f"<b>Телефон:</b> {_display(profile.get('phone'))}",
            f"<b>Email:</b> {_display(profile.get('email'))}",
            f"<b>SMS-отправитель:</b> {_display(profile.get('sms_sender'))}",
        ])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Заполнить / обновить", callback_data="overdue_profile_edit")],
        [InlineKeyboardButton(text="↩ Просрочки", callback_data="overdue_menu")],
    ])
    await callback.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "overdue_profile_edit")
async def cb_overdue_profile_edit(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    await state.set_state(OverdueStates.waiting_creditor_profile)
    await callback.message.edit_text(
        "👤 <b>Профиль кредитора</b>\n\n"
        "Отправьте 5 строк:\n"
        "1. ФИО / название\n"
        "2. Адрес\n"
        "3. Телефон\n"
        "4. Email\n"
        "5. Имя отправителя для SMS (или '-' чтобы использовать ФИО)\n\n"
        "Пример:\n"
        "Кулич Святослав Петрович\n"
        "г. Минск, ул. Связистов, д. 8, кв. 36\n"
        "+375447465358\n"
        "svkulich@tut.by\n"
        "Святослав Кулич",
        reply_markup=_back_main_kb(),
        parse_mode="HTML",
    )


@router.message(OverdueStates.waiting_creditor_profile)
async def msg_overdue_profile(message: Message, state: FSMContext):
    if not await is_allowed(message.chat.id):
        return
    parts = [line.strip() for line in (message.text or "").splitlines() if line.strip()]
    if len(parts) < 4:
        await message.answer("Нужно минимум 4 строки: ФИО/название, адрес, телефон, email.")
        return
    full_name, address, phone, email = parts[:4]
    sms_sender = parts[4] if len(parts) >= 5 and parts[4] != "-" else full_name
    await upsert_creditor_profile(
        message.chat.id,
        full_name=full_name,
        address=address,
        phone=phone,
        email=email,
        sms_sender=sms_sender,
    )
    await state.clear()
    await message.answer("✅ Профиль кредитора сохранен.", reply_markup=_menu_kb())


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
        "Отправьте 5 строк:\n"
        "1. Адрес\n"
        "2. ZIP-код\n"
        "3. Телефон (или '-')\n"
        "4. Email (или '-')\n"
        "5. Срок добровольного погашения в днях\n\n"
        "Если ZIP не знаете, можно открыть поиск Белпочты по кнопке ниже.",
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
    address, zip_code, phone, email, voluntary_days_raw = parts[:5]
    try:
        voluntary_days = int(voluntary_days_raw)
    except ValueError:
        await message.answer("Срок добровольного погашения должен быть целым числом дней.")
        return
    if voluntary_days <= 0:
        await message.answer("Срок добровольного погашения должен быть больше нуля.")
        return
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
    await message.answer("✅ Данные должника сохранены.", reply_markup=_menu_kb())


@router.callback_query(F.data.startswith("overdue_sms_"))
async def cb_overdue_sms(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    case_id = int(callback.data.replace("overdue_sms_", ""))
    case = await get_overdue_case(case_id, callback.message.chat.id)
    creditor = await get_creditor_profile(callback.message.chat.id)
    if not case:
        await callback.answer("Кейс не найден", show_alert=True)
        return
    missing = collect_sms_missing_fields(case, creditor)
    if missing:
        await callback.message.edit_text(
            "⚠️ <b>Нельзя сформировать SMS</b>\n\n"
            + "\n".join(f"• {escape(item)}" for item in missing),
            reply_markup=_case_actions_kb(case_id),
            parse_mode="HTML",
        )
        return
    sms_text = build_sms_text(case, creditor or {})
    await save_generated_document(
        case_id,
        callback.message.chat.id,
        doc_type="sms",
        text_content=sms_text,
        payload=serialize_case_payload(case, creditor),
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
    creditor = await get_creditor_profile(callback.message.chat.id)
    signature = await get_user_signature_asset(callback.message.chat.id)
    if not case:
        await callback.answer("Кейс не найден", show_alert=True)
        return
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
            [InlineKeyboardButton(text="👤 Мой профиль", callback_data="overdue_profile")],
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
