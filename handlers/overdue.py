from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
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
from bot.domain.raw_payloads import format_raw_payload_preview
from bot.services.overdue.service import (
    clone_credential_creditor_profile,
    get_credential_by_id,
    get_credential_creditor_profile,
    get_generated_document,
    get_credential_signature_asset,
    get_overdue_case,
    list_overdue_case_actions,
    list_overdue_cases,
    log_overdue_case_action,
    list_user_credentials,
    copy_credential_signature_asset,
    save_generated_document,
    save_credential_signature_asset,
    update_overdue_case_contacts,
    upsert_credential_creditor_profile,
    upsert_overdue_case,
)
from bot.services.base.access import is_allowed
from bot.services.borrowers.source_labels import humanize_borrower_source
from bot.services.overdue.cases import (
    enrich_finkit_case_from_claims,
    get_latest_finkit_claim,
    refresh_finkit_case_for_claim,
    resolve_belarus_zip,
    resolve_belarus_zip_details,
    send_finkit_pretrial_claim,
)
from bot.services.overdue.documents import (
    build_case_address_summary,
    build_case_loan_ref,
    CLAIM_VOLUNTARY_TERM_DAYS,
    build_postal_address_text,
    build_sms_text,
    collect_claim_missing_fields,
    collect_sms_missing_fields,
    list_case_borrower_addresses,
    render_claim_docx,
    serialize_case_payload,
)

log = logging.getLogger(__name__)
router = Router(name="overdue")

SIGNATURES_DIR = Path(config.BASE_DIR) / "data" / "signatures"
BELPOST_INDEX_URL = "https://www.belpost.by/services/post-index.html"
CASES_PER_PAGE = 10
_PAGE_DELIMITER = "@"
SMS_FOLLOWUP_DAYS = 3


class OverdueStates(StatesGroup):
    waiting_creditor_profile = State()
    waiting_signature = State()
    waiting_case_contacts = State()
    waiting_case_field = State()


def _back_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")]
    ])


def _menu_kb(has_cases: bool = True) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🗂 Список просрочек", callback_data="overdue_cases")],
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _profile_status_icon(profile: dict | None) -> str:
    return "✅" if profile and profile.get("full_name") and profile.get("address") else "⚠️"


def _signature_status_icon(signature: dict | None) -> str:
    if not signature or not signature.get("file_path"):
        return "⚠️"
    return "✅"


def _paginate_cases(cases: list[dict], page: int) -> tuple[list[dict], int, int]:
    total_pages = max(1, (len(cases) + CASES_PER_PAGE - 1) // CASES_PER_PAGE)
    current_page = min(max(page, 0), total_pages - 1)
    start = current_page * CASES_PER_PAGE
    end = start + CASES_PER_PAGE
    return cases[start:end], current_page, total_pages


def _with_page(callback_data: str, page: int | None) -> str:
    if page is None or page < 0:
        return callback_data
    return f"{callback_data}{_PAGE_DELIMITER}{page}"


def _parse_page_callback(callback_data: str) -> tuple[str, int | None]:
    base, separator, page_raw = callback_data.rpartition(_PAGE_DELIMITER)
    if not separator:
        return callback_data, None
    try:
        return base, int(page_raw)
    except (TypeError, ValueError):
        return callback_data, None


def _case_list_callback(page: int | None) -> str:
    if page is None:
        return "overdue_cases"
    return f"overdue_cases_page_{max(page, 0)}"


def _case_list_kb(cases: list[dict], page: int) -> InlineKeyboardMarkup:
    page_items, current_page, total_pages = _paginate_cases(cases, page)
    rows: list[list[InlineKeyboardButton]] = []
    for case in page_items:
        rows.append([
            InlineKeyboardButton(
                text=_case_button_label(case),
                callback_data=_with_page(f"overdue_case_{case['id']}", current_page),
            )
        ])
    if total_pages > 1:
        nav: list[InlineKeyboardButton] = []
        if current_page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"overdue_cases_page_{current_page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{current_page + 1}/{total_pages}", callback_data=_case_list_callback(current_page)))
        if current_page + 1 < total_pages:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"overdue_cases_page_{current_page + 1}"))
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="↩ Просрочки", callback_data="overdue_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _case_actions_kb(case_id: int, credential_id: int | None = None, page: int | None = None) -> InlineKeyboardMarkup:
    del credential_id
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✉️ SMS", callback_data=_with_page(f"overdue_sms_soft_{case_id}", page)),
            InlineKeyboardButton(text="⚠️ Жесткое SMS", callback_data=_with_page(f"overdue_sms_hard_{case_id}", page)),
        ],
        [InlineKeyboardButton(text="📄 Сформировать претензию", callback_data=_with_page(f"overdue_claim_{case_id}", page))],
        [InlineKeyboardButton(text="🕓 История действий", callback_data=_with_page(f"overdue_history_{case_id}", page))],
        [InlineKeyboardButton(text="🧾 Данные API", callback_data=_with_page(f"overdue_raw_{case_id}", page))],
        [InlineKeyboardButton(text="↩ К списку просрочек", callback_data=_case_list_callback(page))],
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
        loan_ref = build_case_loan_ref(case)
        return loan_ref if loan_ref != "—" else f"case#{case['id']}"
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


def _now_action_at() -> str:
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


def _followup_action_at(days: int) -> str:
    return (datetime.now().astimezone() + timedelta(days=days)).replace(microsecond=0).isoformat()


def _parse_action_datetime(value: object | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _format_action_datetime(value: object | None) -> str:
    parsed = _parse_action_datetime(value)
    if not parsed:
        return _display(value)
    return parsed.strftime("%d.%m.%Y %H:%M")


def _action_sort_key(action: dict) -> datetime:
    return _parse_action_datetime(action.get("effective_at") or action.get("created_at")) or datetime.min


def _latest_action(actions: list[dict], *action_types: str) -> dict | None:
    allowed = set(action_types)
    for action in actions:
        if action.get("action_type") in allowed:
            return action
    return None


def _first_unsent_postal_address(case: dict, actions: list[dict]) -> int | None:
    targets = list_case_borrower_addresses(case)
    if not targets:
        return None
    sent_indexes = {
        int(action["target_index"])
        for action in actions
        if action.get("action_type") == "claim_posted" and action.get("target_index") is not None
    }
    for index, _target in enumerate(targets, start=1):
        if index not in sent_indexes:
            return index
    return None


def _tracking_notes(case: dict, actions: list[dict]) -> list[str]:
    notes: list[str] = []
    last_sms_sent = _latest_action(actions, "sms_soft_sent", "sms_hard_sent")
    last_sms_generated = _latest_action(actions, "sms_soft_generated", "sms_hard_generated")
    if last_sms_sent:
        label = "жесткое SMS" if last_sms_sent.get("action_type") == "sms_hard_sent" else "SMS"
        notes.append(f"<b>Последнее SMS:</b> {label} — {_display_html(_format_action_datetime(last_sms_sent.get('effective_at') or last_sms_sent.get('created_at')))}")
    elif last_sms_generated:
        label = "жесткое SMS" if last_sms_generated.get("action_type") == "sms_hard_generated" else "SMS"
        notes.append(f"<b>SMS подготовлено:</b> {label} — {_display_html(_format_action_datetime(last_sms_generated.get('effective_at') or last_sms_generated.get('created_at')))} (не отмечено отправленным)")

    postal_targets = list_case_borrower_addresses(case)
    for index, _target in enumerate(postal_targets, start=1):
        sent = next(
            (
                action
                for action in actions
                if action.get("action_type") == "claim_posted" and int(action.get("target_index") or 0) == index
            ),
            None,
        )
        generated = next(
            (
                action
                for action in actions
                if action.get("action_type") == "claim_generated" and int(action.get("target_index") or 0) == index
            ),
            None,
        )
        label = "Письмо" if len(postal_targets) == 1 else f"Письмо {index}"
        if sent:
            notes.append(f"<b>{label}:</b> отправлено {_display_html(_format_action_datetime(sent.get('effective_at') or sent.get('created_at')))}")
        elif generated:
            notes.append(f"<b>{label}:</b> подготовлено {_display_html(_format_action_datetime(generated.get('effective_at') or generated.get('created_at')))}")

    latest_finkit_sent = _latest_action(actions, "claim_finkit_sent")
    if latest_finkit_sent:
        notes.append(f"<b>FinKit-претензия:</b> отправлена {_display_html(_format_action_datetime(latest_finkit_sent.get('effective_at') or latest_finkit_sent.get('created_at')))}")
    else:
        payload = _parse_raw(case)
        detail = payload.get("detail") or {}
        claim_sent_at = detail.get("latest_claim_sent_at") or next(
            (claim.get("sent_at") for claim in detail.get("claims") or [] if claim.get("sent_at")),
            None,
        )
        if claim_sent_at:
            notes.append(f"<b>FinKit-претензия:</b> уже отправлялась {_display_html(_format_action_datetime(claim_sent_at))}")

    sent_actions = [
        action
        for action in actions
        if action.get("action_type") in {"sms_soft_sent", "sms_hard_sent", "claim_posted", "claim_finkit_sent"}
    ]
    sent_actions.sort(key=_action_sort_key, reverse=True)
    latest_sent = sent_actions[0] if sent_actions else None
    pending_postal_index = _first_unsent_postal_address(case, actions)
    pending_postal_generated = next(
        (
            action
            for action in actions
            if action.get("action_type") == "claim_generated"
            and int(action.get("target_index") or 0) == int(pending_postal_index or 0)
        ),
        None,
    )
    next_step = None
    if latest_sent:
        followup_due = latest_sent.get("followup_due_at")
        followup_dt = _parse_action_datetime(followup_due)
        now_dt = datetime.now(followup_dt.tzinfo) if followup_dt and followup_dt.tzinfo else datetime.now()
        if followup_dt and followup_dt > now_dt:
            next_step = f"следующий контакт ориентировочно после {_format_action_datetime(followup_due)}"
        elif pending_postal_index is not None:
            next_label = "отправить письмо" if len(postal_targets) == 1 else f"отправить письмо на адрес {pending_postal_index}"
            next_step = next_label
        else:
            next_step = "можно делать следующий контакт / повторное напоминание"
    elif last_sms_generated:
        next_step = "отправить подготовленное SMS и отметить отправку"
    elif pending_postal_generated:
        next_label = "отправить подготовленное письмо" if len(postal_targets) == 1 else f"отправить письмо на адрес {pending_postal_index}"
        next_step = next_label
    elif pending_postal_index is not None:
        next_step = "отправить soft SMS"
    else:
        next_step = "отправить soft SMS"
    if next_step:
        notes.append(f"<b>Следующий шаг:</b> {_display_html(next_step)}")
    return notes


def _action_title(action: dict) -> str:
    action_type = str(action.get("action_type") or "")
    titles = {
        "sms_soft_generated": "SMS подготовлено",
        "sms_soft_sent": "SMS отмечено отправленным",
        "sms_hard_generated": "Жесткое SMS подготовлено",
        "sms_hard_sent": "Жесткое SMS отмечено отправленным",
        "claim_generated": "Претензия подготовлена",
        "claim_posted": "Письмо отмечено отправленным",
        "claim_finkit_sent": "Претензия отправлена через FinKit",
    }
    return titles.get(action_type, action_type or "Действие")


def _format_action_history(case: dict, actions: list[dict]) -> str:
    lines = [
        "🕓 <b>История действий</b>",
        "",
        f"<b>Займ / договор:</b> {_display_html(build_case_loan_ref(case))}",
        f"<b>Заемщик:</b> {_display_html(case.get('full_name'))}",
    ]
    if not actions:
        lines.extend(["", "История действий пока пустая."])
        return "\n".join(lines)
    lines.append("")
    for action in actions:
        timestamp = _format_action_datetime(action.get("effective_at") or action.get("created_at"))
        title = _action_title(action)
        suffix_parts: list[str] = []
        if action.get("target_index"):
            suffix_parts.append(f"адрес {action['target_index']}")
        elif action.get("target_value"):
            suffix_parts.append(str(action["target_value"]))
        if action.get("followup_due_at"):
            suffix_parts.append(f"след. ориентир {_format_action_datetime(action['followup_due_at'])}")
        suffix = f" ({'; '.join(suffix_parts)})" if suffix_parts else ""
        lines.append(f"• <b>{_display_html(timestamp)}</b> — {_display_html(title)}{_display_html(suffix)}")
    return "\n".join(lines)


def _sms_result_kb(case: dict, generated_document_id: int, variant: str, page: int | None = None) -> InlineKeyboardMarkup:
    rows = [[
        InlineKeyboardButton(
            text="✅ Отметить SMS отправленной",
            callback_data=_with_page(f"overdue_mark_sms_sent_{case['id']}_{generated_document_id}_{variant}", page),
        )
    ]]
    rows.extend(_case_actions_kb(case["id"], int(case["credential_id"]) if case.get("credential_id") else None, page).inline_keyboard)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _claim_result_kb(
    case: dict,
    latest_claim: dict | None,
    generated_document_id: int | None = None,
    target_index: int | None = None,
    page: int | None = None,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if generated_document_id is not None:
        rows.append([
            InlineKeyboardButton(
                text="✅ Отметить письмо отправленным",
                callback_data=_with_page(f"overdue_mark_postal_sent_{case['id']}_{generated_document_id}", page),
            )
        ])
    if (
        case.get("service") == "finkit"
        and latest_claim
        and latest_claim.get("id")
        and latest_claim.get("can_send")
        and not latest_claim.get("sent_at")
    ):
        rows.append([
            InlineKeyboardButton(
                text="📨 Отправить претензию через FinKit",
                callback_data=_with_page(f"overdue_claim_send_{case['id']}_{latest_claim['id']}", page),
            )
        ])
    rows.extend(_case_actions_kb(case["id"], int(case["credential_id"]) if case.get("credential_id") else None, page).inline_keyboard)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _history_kb(case_id: int, credential_id: int | None = None, page: int | None = None) -> InlineKeyboardMarkup:
    del credential_id
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↩ К кейсу", callback_data=_with_page(f"overdue_case_{case_id}", page))],
        [InlineKeyboardButton(text="↩ К списку просрочек", callback_data=_case_list_callback(page))],
    ])


def _postal_lookup_note(case: dict) -> list[str]:
    payload = _parse_raw(case)
    lookup = payload.get("postal_lookup") or {}
    postcode = lookup.get("postcode")
    if not postcode:
        return []
    lines = [
        f"<b>ZIP найден:</b> {_display_html(postcode)}",
    ]
    if lookup.get("match_address"):
        lines.append(f"<b>Совпадение Белпочты:</b> {_display_html(lookup.get('match_address'))}")
    streets = [street for street in lookup.get("related_streets") or [] if street]
    if streets:
        preview = ", ".join(streets[:5])
        if len(streets) > 5:
            preview += ", ..."
        lines.append(f"<b>Улицы по этому ZIP:</b> {_display_html(preview)}")
    return lines


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

    contact_source = humanize_borrower_source(payload.get("contact_source"))
    if contact_source:
        notes.append(f"<b>Контакты подтянуты из:</b> {_display_html(contact_source)}")

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
    signature = await get_credential_signature_asset(callback.message.chat.id, credential_id)
    lines = [f"🏦 <b>Данные займодавца</b>", "", f"<b>Логин:</b> {_display_html(_credential_label(credential))}", ""]
    if not profile:
        lines.append("Пока не заполнен.")
    else:
        lines.extend([
            f"<b>ФИО:</b> {_display_html(profile.get('full_name'))}",
            f"<b>Адрес:</b> {_display_html(profile.get('address'))}",
            f"<b>Телефон:</b> {_display_html(profile.get('phone'))}",
            f"<b>Email:</b> {_display_html(profile.get('email'))}",
        ])
    lines.extend([
        "",
        f"<b>Подпись:</b> {'загружена' if signature and signature.get('file_path') else 'не загружена'}",
    ])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Заполнить / обновить", callback_data=f"overdue_profile_edit_{credential_id}")],
        [InlineKeyboardButton(text="♻️ Скопировать с другого логина", callback_data=f"overdue_profile_copy_{credential_id}")],
        [InlineKeyboardButton(text="✍️ Управление подписью", callback_data=f"overdue_signature_cred_{credential_id}")],
        [InlineKeyboardButton(text="↩ К профилям", callback_data="overdue_profile")],
    ])
    await callback.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="HTML")


async def _show_signature_message(callback: CallbackQuery, credential_id: int, state: FSMContext | None = None) -> None:
    credential = await get_credential_by_id(credential_id, callback.message.chat.id)
    if not credential:
        await callback.answer("Логин не найден", show_alert=True)
        return
    signature = await get_credential_signature_asset(callback.message.chat.id, credential_id)
    text = (
        "✍️ <b>Подпись для логина</b>\n\n"
        f"Логин: <b>{escape(_credential_label(credential))}</b>\n\n"
        + (
            "Подпись уже загружена. Можно прислать новую или переиспользовать из другого логина."
            if signature and signature.get("file_path")
            else "Подпись еще не загружена. Можно загрузить новую или переиспользовать из другого логина."
        )
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Загрузить подпись", callback_data=f"overdue_signature_upload_{credential_id}")],
        [InlineKeyboardButton(text="♻️ Скопировать с другого логина", callback_data=f"overdue_signature_copy_{credential_id}")],
        [InlineKeyboardButton(text="↩ К логину", callback_data=f"overdue_profile_cred_{credential_id}")],
    ])
    if state is not None:
        await state.clear()
    await callback.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


async def _enrich_finkit_case_from_claims(case: dict) -> dict:
    return await enrich_finkit_case_from_claims(case)


async def _ensure_case_claim_addresses(case_id: int, chat_id: int, case: dict) -> dict:
    addresses = list_case_borrower_addresses(case)
    if not addresses:
        return case
    updated = False
    merged_addresses: list[dict[str, str]] = []
    primary_lookup: dict | None = None
    for idx, item in enumerate(addresses):
        current = dict(item)
        if not str(current.get("zip") or "").strip() and str(current.get("address") or "").strip():
            lookup = await resolve_belarus_zip_details(current["address"])
            postcode = str((lookup or {}).get("postcode") or "").strip()
            if postcode:
                current["zip"] = postcode
                updated = True
                if idx == 0:
                    primary_lookup = lookup
        merged_addresses.append(current)
    if not updated:
        return case
    primary = merged_addresses[0] if merged_addresses else {}
    await update_overdue_case_contacts(
        case_id,
        chat_id,
        borrower_address=primary.get("address"),
        borrower_addresses=merged_addresses,
        borrower_zip=primary.get("zip"),
        postal_lookup=primary_lookup,
    )
    return await get_overdue_case(case_id, chat_id) or case


def _format_case_text(case: dict, actions: list[dict] | None = None) -> str:
    loan_ref = build_case_loan_ref(case)
    address_summary = build_case_address_summary(case)
    lines = [
        "⚖️ <b>Просроченный кейс</b>",
        "",
        f"<b>Сервис:</b> {_display_html(case.get('service'))}",
        f"<b>Аккаунт:</b> {_display_html(case.get('account_label') or case.get('credential_label') or case.get('credential_login'))}",
        f"<b>Займ / договор:</b> {_display_html(loan_ref)}",
        f"<b>Дней просрочки:</b> {_display_html(case.get('days_overdue'))}",
        f"<b>Дата выдачи:</b> {_display_html(case.get('issued_at'))}",
        f"<b>Срок возврата:</b> {_display_html(case.get('due_at'))}",
        "",
        f"<b>Заемщик:</b> {_display_html(case.get('full_name'))}",
        f"<b>ИН:</b> {_display_html(case.get('document_id'))}",
        f"<b>Адрес(а):</b> {_display_html(address_summary)}",
        f"<b>Телефон:</b> {_display_html(case.get('borrower_phone'))}",
        f"<b>Email:</b> {_display_html(case.get('borrower_email'))}",
        "",
        f"<b>Сумма займа:</b> {_money(case.get('amount'))}",
        f"<b>Основной долг:</b> {_money(case.get('principal_outstanding'))}",
        f"<b>Проценты:</b> {_money(case.get('accrued_percent'))}",
        f"<b>Пеня:</b> {_money(case.get('fine_outstanding'))}",
        f"<b>Итого:</b> {_money(case.get('total_due'))}",
        f"<b>Срок добровольного погашения:</b> {CLAIM_VOLUNTARY_TERM_DAYS} дн.",
    ]
    tracking_notes = _tracking_notes(case, actions or [])
    if tracking_notes:
        lines.extend(["", *tracking_notes])
    notes = _case_notes(case)
    if notes:
        lines.extend(["", *notes])
    postal_lookup = _postal_lookup_note(case)
    if postal_lookup:
        lines.extend(["", *postal_lookup])
    return "\n".join(lines)


def _missing_claim_kb(case: dict, missing: list[str], page: int | None = None) -> InlineKeyboardMarkup:
    missing_set = set(missing)
    rows: list[list[InlineKeyboardButton]] = []
    if "адрес заемщика" in missing_set:
        rows.append([InlineKeyboardButton(text="🏠 Указать адрес заемщика", callback_data=_with_page(f"overdue_fill_address_{case['id']}", page))])
    if "ZIP-код заемщика" in missing_set:
        rows.append([InlineKeyboardButton(text="📮 Указать / найти ZIP", callback_data=_with_page(f"overdue_fill_zip_{case['id']}", page))])
    if {"ФИО кредитора", "адрес кредитора", "подпись пользователя"} & missing_set and case.get("credential_id"):
        rows.append([InlineKeyboardButton(text="🏦 Настройки займодавца", callback_data=f"cred_creditor_{case['credential_id']}")])
    if "ИН заемщика" in missing_set and case.get("service") == "finkit":
        rows.append([InlineKeyboardButton(text="🔄 Повторно подтянуть данные FinKit", callback_data=_with_page(f"overdue_claim_retry_{case['id']}", page))])
    rows.append([InlineKeyboardButton(text="↩ К кейсу", callback_data=_with_page(f"overdue_case_{case['id']}", page))])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _show_case(callback: CallbackQuery, case_id: int, page: int | None = None) -> None:
    case = await get_overdue_case(case_id, callback.message.chat.id)
    if not case:
        await callback.answer("Кейс не найден", show_alert=True)
        return
    actions = await list_overdue_case_actions(case_id, callback.message.chat.id, limit=30)
    await callback.message.edit_text(
        _format_case_text(case, actions),
        reply_markup=_case_actions_kb(case_id, int(case["credential_id"]) if case.get("credential_id") else None, page),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


async def _show_case_history(callback: CallbackQuery, case_id: int, page: int | None = None) -> None:
    case = await get_overdue_case(case_id, callback.message.chat.id)
    if not case:
        await callback.answer("Кейс не найден", show_alert=True)
        return
    actions = await list_overdue_case_actions(case_id, callback.message.chat.id, limit=30)
    await callback.message.edit_text(
        _format_action_history(case, actions),
        reply_markup=_history_kb(case_id, int(case["credential_id"]) if case.get("credential_id") else None, page),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


@router.callback_query(F.data.startswith("overdue_history_"))
async def cb_overdue_history(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    callback_base, page = _parse_page_callback(callback.data or "")
    case_id = int(callback_base.replace("overdue_history_", ""))
    await _show_case_history(callback, case_id, page)


async def _handle_claim_generation(callback: CallbackQuery, case_id: int, page: int | None = None) -> None:
    case = await get_overdue_case(case_id, callback.message.chat.id)
    if not case:
        await callback.answer("Кейс не найден", show_alert=True)
        return
    latest_claim: dict | None = None
    if case.get("service") == "finkit":
        case, latest_claim = await refresh_finkit_case_for_claim(case, create_pretrial_claim=True)
        detail = (_parse_raw(case).get("detail") or {})
        if not latest_claim:
            reason = detail.get("claim_generation_reason") or "FinKit не дал сформировать актуальную претензию."
            await callback.message.edit_text(
                "⚠️ <b>Нельзя сформировать претензию</b>\n\n"
                f"{escape(str(reason))}",
                reply_markup=_case_actions_kb(case_id, int(case["credential_id"]) if case.get("credential_id") else None, page),
                parse_mode="HTML",
            )
            return
    else:
        case = await _enrich_finkit_case_from_claims(case)
    case = await _ensure_case_claim_addresses(case_id, callback.message.chat.id, case)
    creditor = await _get_case_creditor_profile(case)
    signature = (
        await get_credential_signature_asset(callback.message.chat.id, int(case["credential_id"]))
        if case.get("credential_id")
        else None
    )
    missing = collect_claim_missing_fields(case, creditor, signature)
    if missing:
        await save_generated_document(
            case_id,
            callback.message.chat.id,
            doc_type="claim_missing",
            payload=serialize_case_payload(case, creditor),
            missing_fields=missing,
        )
        await callback.message.edit_text(
            "⚠️ <b>Нельзя сформировать претензию</b>\n\n"
            + "\n".join(f"• {escape(item)}" for item in missing),
            reply_markup=_missing_claim_kb(case, missing, page),
            parse_mode="HTML",
        )
        return

    claim_targets = list_case_borrower_addresses(case)
    total_targets = len(claim_targets) or 1
    for idx, target in enumerate(claim_targets or [{}], start=1):
        doc_path, claim_text = render_claim_docx(
            case,
            creditor or {},
            signature["file_path"],
            target_address=target or None,
            address_index=idx,
            address_total=total_targets,
        )
        payload = serialize_case_payload(case, creditor)
        payload["target_address"] = target or {}
        payload["address_index"] = idx
        payload["address_total"] = total_targets
        generated_document_id = await save_generated_document(
            case_id,
            callback.message.chat.id,
            doc_type="claim_docx",
            file_path=str(doc_path),
            text_content=claim_text,
            payload=payload,
        )
        await log_overdue_case_action(
            case_id,
            callback.message.chat.id,
            action_type="claim_generated",
            channel="postal",
            target_value=str((target or {}).get("address") or "").strip() or None,
            target_index=idx,
            generated_document_id=generated_document_id,
            effective_at=_now_action_at(),
            meta={"address_total": total_targets, "doc_type": "claim_docx"},
        )
        caption = "📄 Претензия сформирована." if total_targets == 1 else f"📄 Претензия {idx}/{total_targets} сформирована."
        await callback.message.answer_document(
            FSInputFile(str(doc_path), filename=doc_path.name),
            caption=caption,
        )
        await callback.message.answer(
            build_postal_address_text(
                case,
                target or None,
                address_index=idx,
                address_total=total_targets,
            ),
            parse_mode="HTML",
            reply_markup=_claim_result_kb(case, latest_claim if idx == total_targets else None, generated_document_id, idx, page),
        )
    await _show_case(callback, case_id, page)


@router.callback_query(F.data.startswith("overdue_raw_"))
async def cb_overdue_raw(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    callback_base, page = _parse_page_callback(callback.data or "")
    case_id = int(callback_base.replace("overdue_raw_", ""))
    case = await get_overdue_case(case_id, callback.message.chat.id)
    if not case:
        await callback.answer("Кейс не найден", show_alert=True)
        return
    pretty = format_raw_payload_preview(case.get("raw_data"), limit=3500)
    await callback.message.edit_text(
        f"🧾 <b>Данные API</b>\n\n<pre>{escape(pretty)}</pre>",
        reply_markup=_case_actions_kb(case_id, int(case["credential_id"]) if case.get("credential_id") else None, page),
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
        "Здесь можно открыть список просроченных займов и вручную сформировать SMS или претензию."
    )
    await callback.message.edit_text(text, reply_markup=_menu_kb(), parse_mode="HTML")


@router.callback_query(F.data == "overdue_cases")
async def cb_overdue_cases(callback: CallbackQuery):
    await _show_overdue_cases_page(callback, 0)


async def _show_overdue_cases_page(callback: CallbackQuery, page: int) -> None:
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
    _, current_page, total_pages = _paginate_cases(cases, page)
    await callback.message.edit_text(
        f"🗂 <b>Список просроченных кейсов</b>\n\nВыберите кейс.\nСтраница: <b>{current_page + 1}/{total_pages}</b>",
        reply_markup=_case_list_kb(cases, current_page),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("overdue_cases_page_"))
async def cb_overdue_cases_page(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    page = int(callback.data.replace("overdue_cases_page_", ""))
    await _show_overdue_cases_page(callback, page)


@router.callback_query(F.data.startswith("overdue_case_"))
async def cb_overdue_case(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    callback_base, page = _parse_page_callback(callback.data or "")
    case_id = int(callback_base.replace("overdue_case_", ""))
    await _show_case(callback, case_id, page)


@router.callback_query(F.data == "overdue_profile")
async def cb_overdue_profile(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    credentials = await list_user_credentials(callback.message.chat.id, services=("finkit", "zaimis"))
    lines = ["🏦 <b>Данные займодавца по логинам</b>", ""]
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
        signature = await get_credential_signature_asset(callback.message.chat.id, int(credential["id"]))
        profile_status = _profile_status_icon(profile)
        signature_status = _signature_status_icon(signature)
        status = f"{profile_status}👤 {signature_status}✍️"
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
        "🏦 <b>Данные займодавца</b>\n\n"
        f"Логин: <b>{escape(_credential_label(credential))}</b>\n\n"
        "Отправьте 4 строки:\n"
        "1. ФИО займодавца\n"
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


@router.callback_query(F.data.startswith("overdue_profile_copy_"))
async def cb_overdue_profile_copy(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    if callback.data.startswith("overdue_profile_copy_from_"):
        return
    target_credential_id = int(callback.data.replace("overdue_profile_copy_", ""))
    credentials = await list_user_credentials(callback.message.chat.id, services=("finkit", "zaimis"))
    buttons: list[list[InlineKeyboardButton]] = []
    for credential in credentials:
        source_id = int(credential["id"])
        if source_id == target_credential_id:
            continue
        profile = await get_credential_creditor_profile(callback.message.chat.id, source_id)
        if not profile:
            continue
        buttons.append([
            InlineKeyboardButton(
                text=f"♻️ {_credential_label(credential)}",
                callback_data=f"overdue_profile_copy_from_{target_credential_id}_{source_id}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="↩ К логину", callback_data=f"overdue_profile_cred_{target_credential_id}")])
    text = (
        "♻️ <b>Скопировать данные займодавца</b>\n\n"
        "Выберите логин-источник, с которого нужно переиспользовать профиль."
    )
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")


@router.callback_query(F.data.startswith("overdue_profile_copy_from_"))
async def cb_overdue_profile_copy_from(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    payload = callback.data.replace("overdue_profile_copy_from_", "")
    target_id_raw, source_id_raw = payload.split("_", 1)
    target_credential_id = int(target_id_raw)
    source_credential_id = int(source_id_raw)
    copied = await clone_credential_creditor_profile(
        callback.message.chat.id,
        source_credential_id,
        target_credential_id,
    )
    if not copied:
        await callback.answer("Не удалось найти источник профиля", show_alert=True)
        return
    await _show_credential_profile_message(callback, target_credential_id)


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
async def cb_overdue_signature(callback: CallbackQuery):
    await cb_overdue_profile(callback)


@router.callback_query(F.data.startswith("overdue_signature_cred_"))
async def cb_overdue_signature_credential(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    credential_id = int(callback.data.replace("overdue_signature_cred_", ""))
    await _show_signature_message(callback, credential_id, state)


@router.callback_query(F.data.startswith("overdue_signature_upload_"))
async def cb_overdue_signature_upload(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    credential_id = int(callback.data.replace("overdue_signature_upload_", ""))
    credential = await get_credential_by_id(credential_id, callback.message.chat.id)
    if not credential:
        await callback.answer("Логин не найден", show_alert=True)
        return
    await state.update_data(credential_id=credential_id)
    await state.set_state(OverdueStates.waiting_signature)
    await callback.message.edit_text(
        "✍️ <b>Загрузка подписи</b>\n\n"
        f"Логин: <b>{escape(_credential_label(credential))}</b>\n\n"
        "Пришлите изображение подписи как фото или файл PNG/JPG.\n"
        "Мы сохраним его именно для этого логина и будем автоматически вставлять в документ претензии.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩ К подписи", callback_data=f"overdue_signature_cred_{credential_id}")],
        ]),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("overdue_signature_copy_"))
async def cb_overdue_signature_copy(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    if callback.data.startswith("overdue_signature_copy_from_"):
        return
    target_credential_id = int(callback.data.replace("overdue_signature_copy_", ""))
    credentials = await list_user_credentials(callback.message.chat.id, services=("finkit", "zaimis"))
    buttons: list[list[InlineKeyboardButton]] = []
    for credential in credentials:
        source_id = int(credential["id"])
        if source_id == target_credential_id:
            continue
        signature = await get_credential_signature_asset(callback.message.chat.id, source_id)
        if not signature or not signature.get("file_path"):
            continue
        buttons.append([
            InlineKeyboardButton(
                text=f"♻️ {_credential_label(credential)}",
                callback_data=f"overdue_signature_copy_from_{target_credential_id}_{source_id}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="↩ К подписи", callback_data=f"overdue_signature_cred_{target_credential_id}")])
    text = (
        "♻️ <b>Скопировать подпись</b>\n\nВыберите логин-источник."
        if len(buttons) > 1
        else "♻️ <b>Скопировать подпись</b>\n\nНет других логинов с загруженной подписью."
    )
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("overdue_signature_copy_from_"))
async def cb_overdue_signature_copy_from(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    payload = callback.data.replace("overdue_signature_copy_from_", "")
    target_raw, source_raw = payload.split("_", 1)
    target_credential_id = int(target_raw)
    source_credential_id = int(source_raw)
    copied = await copy_credential_signature_asset(
        callback.message.chat.id,
        source_credential_id,
        target_credential_id,
    )
    if not copied:
        await callback.answer("Не удалось найти подпись-источник", show_alert=True)
        return
    await _show_signature_message(callback, target_credential_id)


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


@router.message(OverdueStates.waiting_signature, F.photo | F.document)
async def msg_overdue_signature(message: Message, state: FSMContext):
    if not await is_allowed(message.chat.id):
        return
    data = await state.get_data()
    credential_id = data.get("credential_id")
    if not credential_id:
        await state.clear()
        await message.answer("Не удалось определить логин для подписи.", reply_markup=_menu_kb())
        return
    saved = await _save_signature_message(message, int(credential_id))
    if not saved:
        await message.answer("Не удалось сохранить подпись. Пришлите фото или PNG/JPG файл.")
        return
    credential = await get_credential_by_id(int(credential_id), message.chat.id)
    await state.clear()
    await message.answer(
        f"✅ Подпись сохранена для логина {_credential_label(credential) if credential else credential_id}.",
        reply_markup=_menu_kb(),
    )


@router.message(OverdueStates.waiting_signature)
async def msg_overdue_signature_invalid(message: Message):
    if not await is_allowed(message.chat.id):
        return
    await message.answer("Пришлите подпись как фото или изображение PNG/JPG.")


@router.callback_query(F.data.startswith("overdue_fill_address_"))
async def cb_overdue_fill_address(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    callback_base, page = _parse_page_callback(callback.data or "")
    case_id = int(callback_base.replace("overdue_fill_address_", ""))
    case = await get_overdue_case(case_id, callback.message.chat.id)
    if not case:
        await callback.answer("Кейс не найден", show_alert=True)
        return
    await state.set_state(OverdueStates.waiting_case_field)
    await state.update_data(case_id=case_id, field_name="borrower_address", case_page=page)
    await callback.message.edit_text(
        "🏠 <b>Адрес заемщика</b>\n\n"
        f"<b>Текущее значение:</b> {_display_html(case.get('borrower_address'))}\n\n"
        "Отправьте только адрес заемщика. После этого я попробую сразу найти ZIP.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩ К кейсу", callback_data=_with_page(f"overdue_case_{case_id}", page))],
        ]),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("overdue_fill_zip_"))
async def cb_overdue_fill_zip(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    callback_base, page = _parse_page_callback(callback.data or "")
    case_id = int(callback_base.replace("overdue_fill_zip_", ""))
    case = await get_overdue_case(case_id, callback.message.chat.id)
    if not case:
        await callback.answer("Кейс не найден", show_alert=True)
        return
    if not case.get("borrower_address"):
        await callback.message.edit_text(
            "⚠️ <b>Сначала нужен адрес заемщика</b>\n\nБез адреса я не смогу нормально найти ZIP.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🏠 Указать адрес", callback_data=_with_page(f"overdue_fill_address_{case_id}", page))],
                [InlineKeyboardButton(text="↩ К кейсу", callback_data=_with_page(f"overdue_case_{case_id}", page))],
            ]),
            parse_mode="HTML",
        )
        return
    await state.set_state(OverdueStates.waiting_case_field)
    await state.update_data(case_id=case_id, field_name="borrower_zip", case_page=page)
    await callback.message.edit_text(
        "📮 <b>ZIP-код заемщика</b>\n\n"
        f"<b>Адрес:</b> {_display_html(case.get('borrower_address'))}\n"
        f"<b>Текущее значение:</b> {_display_html(case.get('borrower_zip'))}\n\n"
        "Отправьте ZIP вручную или <code>-</code>, чтобы я попробовал найти его автоматически.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩ К кейсу", callback_data=_with_page(f"overdue_case_{case_id}", page))],
        ]),
        parse_mode="HTML",
    )


@router.message(OverdueStates.waiting_case_field)
async def msg_overdue_case_field(message: Message, state: FSMContext):
    if not await is_allowed(message.chat.id):
        return
    data = await state.get_data()
    case_id = int(data.get("case_id") or 0)
    field_name = str(data.get("field_name") or "")
    case = await get_overdue_case(case_id, message.chat.id)
    if not case or not field_name:
        await state.clear()
        await message.answer("Не удалось определить поле для обновления.", reply_markup=_menu_kb())
        return

    raw_value = (message.text or "").strip()
    if field_name == "borrower_address":
        lookup = await resolve_belarus_zip_details(raw_value)
        zip_code = str(lookup.get("postcode")) if lookup and lookup.get("postcode") else None
        await update_overdue_case_contacts(
            case_id,
            message.chat.id,
            borrower_address=raw_value,
            borrower_zip=zip_code,
            postal_lookup=lookup,
            contact_source="manual",
        )
        await state.clear()
        if zip_code and lookup:
            streets = ", ".join((lookup.get("related_streets") or [])[:4])
            suffix = f" ZIP найден: {zip_code}. Белпочта: {lookup.get('match_address') or '—'}."
            if streets:
                suffix += f" Улицы по индексу: {streets}."
        else:
            suffix = " ZIP автоматически не найден."
        await message.answer(f"✅ Адрес заемщика сохранён.{suffix}", reply_markup=_menu_kb())
        return

    if field_name == "borrower_zip":
        zip_code = raw_value
        lookup = None
        if raw_value == "-":
            lookup = await resolve_belarus_zip_details(case.get("borrower_address"))
            zip_code = str(lookup.get("postcode")) if lookup and lookup.get("postcode") else None
        await update_overdue_case_contacts(
            case_id,
            message.chat.id,
            borrower_zip=zip_code,
            postal_lookup=lookup,
        )
        await state.clear()
        if zip_code:
            details = ""
            if lookup:
                streets = ", ".join((lookup.get("related_streets") or [])[:5])
                details = f" Белпочта: {lookup.get('match_address') or '—'}."
                if streets:
                    details += f" Улицы по индексу: {streets}."
            await message.answer(f"✅ ZIP сохранён: {zip_code}.{details}", reply_markup=_menu_kb())
        else:
            await message.answer("⚠️ ZIP автоматически не найден.", reply_markup=_menu_kb())
        return

    await state.clear()
    await message.answer("Неизвестное поле для обновления.", reply_markup=_menu_kb())


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
        "Отправьте 4 строки именно по должнику:\n"
        "1. Адрес должника (не ваш)\n"
        "2. ZIP-код (или '-' для автопоиска)\n"
        "3. Телефон (или '-')\n"
        "4. Email (или '-')\n\n"
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
    if len(parts) < 4:
        await message.answer("Нужно 4 строки: адрес, ZIP, телефон, email.")
        return
    address, zip_code_raw, phone, email = parts[:4]
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
        contact_source="manual",
    )
    await state.clear()
    suffix = f" ZIP: {zip_code}." if zip_code else " ZIP не найден автоматически."
    await message.answer(f"✅ Данные должника сохранены.{suffix}", reply_markup=_menu_kb())


@router.callback_query(F.data.startswith("overdue_sms_"))
async def cb_overdue_sms(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    callback_base, page = _parse_page_callback(callback.data or "")
    payload = callback_base.replace("overdue_sms_", "")
    variant = "soft"
    if payload.startswith("soft_"):
        case_id = int(payload.replace("soft_", "", 1))
    elif payload.startswith("hard_"):
        variant = "hard"
        case_id = int(payload.replace("hard_", "", 1))
    else:
        case_id = int(payload)
    case = await get_overdue_case(case_id, callback.message.chat.id)
    if not case:
        await callback.answer("Кейс не найден", show_alert=True)
        return
    missing = collect_sms_missing_fields(case, None)
    if missing:
        await callback.message.edit_text(
            "⚠️ <b>Нельзя сформировать SMS</b>\n\n"
            + "\n".join(f"• {escape(item)}" for item in missing),
            reply_markup=_case_actions_kb(case_id, int(case["credential_id"]) if case.get("credential_id") else None, page),
            parse_mode="HTML",
        )
        return
    sms_text = build_sms_text(case, {}, variant=variant)
    generated_document_id = await save_generated_document(
        case_id,
        callback.message.chat.id,
        doc_type="sms_hard" if variant == "hard" else "sms",
        text_content=sms_text,
        payload=serialize_case_payload(case, None),
    )
    await log_overdue_case_action(
        case_id,
        callback.message.chat.id,
        action_type="sms_hard_generated" if variant == "hard" else "sms_soft_generated",
        channel="sms",
        target_value=str(case.get("borrower_phone") or "").strip() or None,
        generated_document_id=generated_document_id,
        effective_at=_now_action_at(),
        meta={"variant": variant},
    )
    title = "⚠️ <b>Жесткое SMS</b>" if variant == "hard" else "✉️ <b>SMS</b>"
    message_text = f"{title}\n\n<pre>{escape(sms_text)}</pre>"
    if case.get("borrower_phone"):
        message_text += f"\n\n<b>Номер:</b> <code>{escape(str(case['borrower_phone']))}</code>"
    await callback.message.edit_text(
        message_text,
        reply_markup=_sms_result_kb(case, generated_document_id, variant, page),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("overdue_mark_sms_sent_"))
async def cb_overdue_mark_sms_sent(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    callback_base, page = _parse_page_callback(callback.data or "")
    payload = callback_base.replace("overdue_mark_sms_sent_", "")
    case_id_raw, document_id_raw, variant = payload.split("_", 2)
    case_id = int(case_id_raw)
    generated_document_id = int(document_id_raw)
    case = await get_overdue_case(case_id, callback.message.chat.id)
    generated = await get_generated_document(generated_document_id, callback.message.chat.id)
    if not case or not generated or int(generated.get("overdue_case_id") or 0) != case_id:
        await callback.answer("Не удалось найти сохраненную SMS", show_alert=True)
        return
    await log_overdue_case_action(
        case_id,
        callback.message.chat.id,
        action_type="sms_hard_sent" if variant == "hard" else "sms_soft_sent",
        channel="sms",
        target_value=str(case.get("borrower_phone") or "").strip() or None,
        target_index=None,
        generated_document_id=generated_document_id,
        effective_at=_now_action_at(),
        followup_due_at=_followup_action_at(SMS_FOLLOWUP_DAYS),
        meta={"variant": variant},
    )
    await callback.answer("SMS отмечена отправленной")
    await _show_case(callback, case_id, page)


@router.callback_query(
    lambda callback: bool(callback.data)
    and callback.data.startswith("overdue_claim_")
    and not callback.data.startswith("overdue_claim_send_")
    and not callback.data.startswith("overdue_claim_retry_")
)
async def cb_overdue_claim(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    callback_base, page = _parse_page_callback(callback.data or "")
    case_id = int(callback_base.replace("overdue_claim_", ""))
    await _handle_claim_generation(callback, case_id, page)


@router.callback_query(F.data.startswith("overdue_claim_send_"))
async def cb_overdue_claim_send(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    callback_base, page = _parse_page_callback(callback.data or "")
    payload = callback_base.replace("overdue_claim_send_", "")
    case_id_raw, claim_id = payload.split("_", 1)
    case_id = int(case_id_raw)
    case = await get_overdue_case(case_id, callback.message.chat.id)
    if not case:
        await callback.answer("Кейс не найден", show_alert=True)
        return
    ok, refreshed = await send_finkit_pretrial_claim(case, claim_id)
    if not ok:
        await callback.answer("Не удалось отправить претензию через FinKit", show_alert=True)
        return
    latest_claim = get_latest_finkit_claim(refreshed)
    await log_overdue_case_action(
        refreshed["id"],
        callback.message.chat.id,
        action_type="claim_finkit_sent",
        channel="finkit",
        generated_document_id=None,
        effective_at=str((latest_claim or {}).get("sent_at") or _now_action_at()),
        followup_due_at=_followup_action_at(CLAIM_VOLUNTARY_TERM_DAYS),
        meta={"claim_id": claim_id},
    )
    await callback.message.answer("✅ Претензия отправлена заемщику через FinKit.")
    await _show_case(callback, refreshed["id"], page)


@router.callback_query(F.data.startswith("overdue_mark_postal_sent_"))
async def cb_overdue_mark_postal_sent(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    callback_base, page = _parse_page_callback(callback.data or "")
    payload = callback_base.replace("overdue_mark_postal_sent_", "")
    case_id_raw, document_id_raw = payload.split("_", 1)
    case_id = int(case_id_raw)
    generated_document_id = int(document_id_raw)
    case = await get_overdue_case(case_id, callback.message.chat.id)
    generated = await get_generated_document(generated_document_id, callback.message.chat.id)
    if not case or not generated or int(generated.get("overdue_case_id") or 0) != case_id:
        await callback.answer("Не удалось найти сохраненную претензию", show_alert=True)
        return
    payload_json = generated.get("payload_json") or {}
    target = payload_json.get("target_address") or {}
    target_value = str((target or {}).get("address") or "").strip() or None
    target_index = payload_json.get("address_index")
    await log_overdue_case_action(
        case_id,
        callback.message.chat.id,
        action_type="claim_posted",
        channel="postal",
        target_value=target_value,
        target_index=int(target_index) if target_index is not None else None,
        generated_document_id=generated_document_id,
        effective_at=_now_action_at(),
        followup_due_at=_followup_action_at(CLAIM_VOLUNTARY_TERM_DAYS),
        meta={"address_total": payload_json.get("address_total")},
    )
    await callback.answer("Письмо отмечено отправленным")
    await callback.message.edit_reply_markup(
        reply_markup=_history_kb(case_id, int(case["credential_id"]) if case.get("credential_id") else None, page)
    )


@router.callback_query(F.data.startswith("overdue_claim_retry_"))
async def cb_overdue_claim_retry(callback: CallbackQuery):
    if not await is_allowed(callback.message.chat.id):
        return
    callback_base, page = _parse_page_callback(callback.data or "")
    case_id = int(callback_base.replace("overdue_claim_retry_", ""))
    case = await get_overdue_case(case_id, callback.message.chat.id)
    if not case:
        await callback.answer("Кейс не найден", show_alert=True)
        return
    await _enrich_finkit_case_from_claims(case)
    await callback.answer("Данные обновлены, проверяю снова…")
    await _handle_claim_generation(callback, case_id, page)
