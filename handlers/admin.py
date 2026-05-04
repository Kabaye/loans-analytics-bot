"""Admin handler — whitelist management + polling controls + test/archive."""
from __future__ import annotations

import asyncio
import json
import logging
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

from bot.services.admin.service import (
    clear_api_change_alerts,
    delete_api_change_alert,
    get_all_site_settings,
    get_api_change_alert,
    get_missing_opi_candidates,
    get_test_notification_entry as get_test_notification_entry_service,
    get_test_parser as get_test_parser_service,
    list_api_change_alerts,
    update_site_setting,
    ensure_user,
    get_user,
    list_users,
    list_users_by_access,
    run_full_app_test as run_full_app_test_service,
    is_admin as is_admin_service,
    set_user_admin,
    set_user_allowed,
)
from bot.config import ADMIN_CHAT_ID
from bot.domain.borrower_views import NotificationEntryView

log = logging.getLogger(__name__)
router = Router(name="admin")

SVC_NAMES = {
    "kapusta": "🥬 Kapusta",
    "finkit": "🔵 FinKit",
    "zaimis": "🟪 ЗАЙМись",
}
MAIN_OWNER_USERNAME = "kabaye"


def _display_name(row) -> str:
    """Build a readable display name from user row."""
    parts = []
    fn = row["first_name"] if "first_name" in row.keys() else None
    ln = row["last_name"] if "last_name" in row.keys() else None
    if fn:
        parts.append(fn)
    if ln:
        parts.append(ln)
    name = " ".join(parts) if parts else None
    username = f"@{row['username']}" if row["username"] else None
    if name and username:
        return f"{name} ({username})"
    return name or username or str(row["chat_id"])


async def is_admin(chat_id: int) -> bool:
    return await is_admin_service(chat_id)


def _is_main_owner_row(row) -> bool:
    username = str(row["username"] or "").strip().lstrip("@").lower() if "username" in row.keys() else ""
    chat_id = row["chat_id"] if "chat_id" in row.keys() else None
    return chat_id == ADMIN_CHAT_ID or username == MAIN_OWNER_USERNAME


async def _get_test_parser(service: str, requester_chat_id: int):
    return await get_test_parser_service(service, requester_chat_id)


class AdminAddUser(StatesGroup):
    chat_id = State()


async def _show_admin_panel(target, chat_id: int, edit: bool = False):
    """Show admin panel. target is Message or CallbackQuery.message."""
    if not await is_admin(chat_id):
        return

    rows = await list_users()

    lines = ["<b>👑 Администрирование</b>\n"]
    for row in rows:
        status = "✅" if row["is_allowed"] else "⛔"
        admin_badge = " 👑" if row["is_admin"] else ""
        owner_badge = " ⭐" if _is_main_owner_row(row) else ""
        display = _display_name(row)
        lines.append(f"{status}{admin_badge}{owner_badge} <code>{row['chat_id']}</code> — {display}")

    buttons = [
        [InlineKeyboardButton(text="➕ Добавить пользователя", callback_data="adm_add")],
        [InlineKeyboardButton(text="✅ Разрешить доступ", callback_data="adm_allow_choose")],
        [InlineKeyboardButton(text="⛔ Заблокировать", callback_data="adm_block_choose")],
        [InlineKeyboardButton(text="👑 Назначить админом", callback_data="adm_promote_choose")],
        [InlineKeyboardButton(text="⬇️ Снять админа", callback_data="adm_demote_choose")],
        [InlineKeyboardButton(text="🧬 Изменения API", callback_data="adm_api_alerts")],
        [InlineKeyboardButton(text="⚙️ Настройки опроса", callback_data="adm_polling")],
        [InlineKeyboardButton(text="🧪 Тест", callback_data="adm_test_menu")],
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
    ]
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    text = "\n".join(lines)
    if edit:
        await target.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await target.answer(text, reply_markup=kb, parse_mode="HTML")


@router.message(Command("admin"))
async def cmd_admin(message: Message):
    await _show_admin_panel(message, message.chat.id, edit=False)


@router.callback_query(F.data == "admin_menu")
async def cb_admin_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await _show_admin_panel(callback.message, callback.message.chat.id, edit=True)


@router.callback_query(F.data == "adm_add")
async def adm_add_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.message.chat.id):
        return
    await callback.message.edit_text("Введите chat_id нового пользователя:")
    await state.set_state(AdminAddUser.chat_id)


@router.message(AdminAddUser.chat_id)
async def adm_add_user(message: Message, state: FSMContext):
    if not await is_admin(message.chat.id):
        return

    try:
        new_chat_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Некорректный chat_id. Введите число.")
        return

    await ensure_user(new_chat_id)
    await set_user_allowed(new_chat_id, True)

    await state.clear()
    await message.answer(
        f"✅ Пользователь {new_chat_id} добавлен и разрешён.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👑 Админ-панель", callback_data="admin_menu")],
            [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
        ]),
    )


@router.callback_query(F.data == "adm_allow_choose")
async def adm_allow_choose(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    await _show_user_list(callback, "adm_allow_", is_allowed=0)


@router.callback_query(F.data.startswith("adm_allow_"))
async def adm_allow_user(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    chat_id = int(callback.data.replace("adm_allow_", ""))
    await set_user_allowed(chat_id, True)
    row = await get_user(chat_id)
    display = _display_name(row) if row else str(chat_id)
    await callback.message.edit_text(f"✅ Пользователь {display} разрешён.")


@router.callback_query(F.data == "adm_block_choose")
async def adm_block_choose(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    # Exclude self from block list
    await _show_user_list(callback, "adm_block_", is_allowed=1,
                          exclude_chat_id=callback.message.chat.id,
                          exclude_main_owner=True)


@router.callback_query(F.data.startswith("adm_block_"))
async def adm_block_user(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    chat_id = int(callback.data.replace("adm_block_", ""))

    # Prevent self-block
    if chat_id == callback.message.chat.id:
        await callback.answer("❌ Нельзя заблокировать самого себя!", show_alert=True)
        return

    row = await get_user(chat_id)
    if row and _is_main_owner_row(row):
            await callback.answer("❌ Нельзя заблокировать главного владельца.", show_alert=True)
            return
    await set_user_allowed(chat_id, False)
    display = _display_name(row) if row else str(chat_id)
    await callback.message.edit_text(f"⛔ Пользователь {display} заблокирован.")


@router.callback_query(F.data == "adm_promote_choose")
async def adm_promote_choose(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    await _show_user_list(callback, "adm_prom_", is_allowed=1)


@router.callback_query(F.data.startswith("adm_prom_"))
async def adm_promote_user(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    chat_id = int(callback.data.replace("adm_prom_", ""))
    await set_user_admin(chat_id, True)
    row = await get_user(chat_id)
    display = _display_name(row) if row else str(chat_id)
    await callback.message.edit_text(f"👑 Пользователь {display} назначен админом.")


@router.callback_query(F.data == "adm_demote_choose")
async def adm_demote_choose(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    await _show_user_list(
        callback,
        "adm_demote_",
        is_allowed=1,
        exclude_chat_id=callback.message.chat.id,
        exclude_main_owner=True,
        require_admin=1,
    )


@router.callback_query(F.data.startswith("adm_demote_"))
async def adm_demote_user(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    chat_id = int(callback.data.replace("adm_demote_", ""))
    row = await get_user(chat_id)
    if row and _is_main_owner_row(row):
            await callback.answer("❌ Нельзя снять админа с главного владельца.", show_alert=True)
            return
    await set_user_admin(chat_id, False)
    display = _display_name(row) if row else str(chat_id)
    await callback.message.edit_text(f"⬇️ Пользователь {display} больше не админ.")


async def _show_user_list(callback: CallbackQuery, prefix: str, is_allowed: int,
                          exclude_chat_id: int | None = None,
                          exclude_main_owner: bool = False,
                          require_admin: int | None = None):
    rows = await list_users_by_access(is_allowed)

    # Filter out excluded user (self-protection)
    filtered = list(rows)
    if exclude_chat_id is not None:
        filtered = [r for r in filtered if r["chat_id"] != exclude_chat_id]
    if exclude_main_owner:
        filtered = [r for r in filtered if not _is_main_owner_row(r)]
    if require_admin is not None:
        filtered = [r for r in filtered if int(r["is_admin"] or 0) == require_admin]

    if not filtered:
        await callback.answer("Нет пользователей")
        return

    buttons = []
    for row in filtered:
        display = _display_name(row)
        buttons.append([
            InlineKeyboardButton(
                text=f"{display}",
                callback_data=f"{prefix}{row['chat_id']}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="↩ Назад", callback_data="adm_back")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.edit_text("Выберите пользователя:", reply_markup=kb)


@router.callback_query(F.data == "adm_back")
async def adm_back(callback: CallbackQuery):
    await _show_admin_panel(callback.message, callback.message.chat.id, edit=True)


# ============== API change alerts ==============

@router.callback_query(F.data == "adm_api_alerts")
async def adm_api_alerts(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    alerts = await list_api_change_alerts(limit=30)
    if not alerts:
        await callback.message.edit_text(
            "🧬 <b>Изменения API</b>\n\nПока нет сохранённых изменений.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="↩ Админ-панель", callback_data="admin_menu")],
            ]),
        )
        return

    lines = ["🧬 <b>Изменения API</b>", "", f"Накоплено: <b>{len(alerts)}</b>", ""]
    buttons: list[list[InlineKeyboardButton]] = []
    for alert in alerts[:20]:
        created = (alert.get("created_at") or "")[:16]
        service = SVC_NAMES.get(alert["service"], alert["service"])
        lines.append(f"• <code>#{alert['id']}</code> {service} — {escape(alert['title'])}")
        buttons.append([
            InlineKeyboardButton(
                text=f"#{alert['id']} {service} {created}",
                callback_data=f"adm_api_alert_{alert['id']}",
            )
        ])

    buttons.append([InlineKeyboardButton(text="🗑 Очистить всё", callback_data="adm_api_alerts_clear")])
    buttons.append([InlineKeyboardButton(text="↩ Админ-панель", callback_data="admin_menu")])
    await callback.message.edit_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data.startswith("adm_api_alert_"))
async def adm_api_alert_view(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    if callback.data == "adm_api_alerts_clear" or callback.data.startswith("adm_api_alert_del_"):
        return
    alert_id = int(callback.data.replace("adm_api_alert_", ""))
    alert = await get_api_change_alert(alert_id)
    if not alert:
        await callback.answer("Запись не найдена", show_alert=True)
        return
    parts = [
        "🧬 <b>Изменение API</b>",
        "",
        f"<b>ID:</b> {alert['id']}",
        f"<b>Сервис:</b> {escape(SVC_NAMES.get(alert['service'], alert['service']))}",
        f"<b>Время:</b> {escape(alert.get('created_at') or '—')}",
        "",
        escape(alert.get("details") or alert.get("title") or "—"),
    ]
    if alert.get("sample_json"):
        parts.extend(["", f"<pre>{escape(alert['sample_json'][:2500])}</pre>"])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"adm_del_api_alert_{alert_id}")],
        [InlineKeyboardButton(text="↩ К списку", callback_data="adm_api_alerts")],
    ])
    await callback.message.edit_text("\n".join(parts), parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data.startswith("adm_del_api_alert_"))
async def adm_api_alert_delete(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    alert_id = int(callback.data.replace("adm_del_api_alert_", ""))
    await delete_api_change_alert(alert_id)
    await adm_api_alerts(callback)


@router.callback_query(F.data == "adm_api_alerts_clear")
async def adm_api_alerts_clear(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    await clear_api_change_alerts()
    await callback.message.edit_text(
        "🧬 <b>Изменения API</b>\n\nВсе сохранённые записи удалены.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩ Админ-панель", callback_data="admin_menu")],
        ]),
    )


# ============== Polling settings panel ==============

class SetInterval(StatesGroup):
    service = State()
    value = State()


async def _show_polling_panel(msg, chat_id: int, edit: bool = True):
    if not await is_admin(chat_id):
        return

    settings = await get_all_site_settings()
    lines = ["<b>⚙️ Настройки опроса</b>\n"]

    buttons = []
    for s in settings:
        svc = s["service"]
        name = SVC_NAMES.get(svc, svc)
        enabled = "✅" if s["polling_enabled"] else "⛔"
        interval = s["poll_interval"]

        lines.append(f"{enabled} {name}: каждые <b>{interval}с</b>")

        toggle_text = f"{'⛔' if s['polling_enabled'] else '✅'} {name}"
        buttons.append([
            InlineKeyboardButton(text=toggle_text, callback_data=f"poll_toggle_{svc}"),
            InlineKeyboardButton(text=f"⏱ {interval}с", callback_data=f"poll_interval_{svc}"),
        ])

    buttons.append([InlineKeyboardButton(text="↩ Админ-панель", callback_data="admin_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)

    text = "\n".join(lines)
    if edit:
        await msg.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await msg.answer(text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(F.data == "adm_polling")
async def cb_polling_panel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await _show_polling_panel(callback.message, callback.message.chat.id)


@router.callback_query(F.data.startswith("poll_toggle_"))
async def cb_toggle_polling(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    svc = callback.data.replace("poll_toggle_", "")
    settings = await get_all_site_settings()
    current = next((s for s in settings if s["service"] == svc), None)
    if current:
        new_val = 0 if current["polling_enabled"] else 1
        await update_site_setting(svc, polling_enabled=new_val)
    await _show_polling_panel(callback.message, callback.message.chat.id)


@router.callback_query(F.data.startswith("poll_interval_"))
async def cb_set_interval(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.message.chat.id):
        return
    svc = callback.data.replace("poll_interval_", "")
    name = SVC_NAMES.get(svc, svc)

    buttons = []
    for val in [30, 60, 120, 300, 600]:
        label = f"{val}с" if val < 60 else f"{val // 60}мин" if val >= 60 else f"{val}с"
        if val == 30:
            label = "30с"
        elif val == 60:
            label = "1мин"
        elif val == 120:
            label = "2мин"
        elif val == 300:
            label = "5мин"
        elif val == 600:
            label = "10мин"
        buttons.append(InlineKeyboardButton(text=label, callback_data=f"poll_setint_{svc}_{val}"))

    kb = InlineKeyboardMarkup(inline_keyboard=[
        buttons[:3],
        buttons[3:],
        [InlineKeyboardButton(text="✏️ Своё значение (сек)", callback_data=f"poll_custint_{svc}")],
        [InlineKeyboardButton(text="↩ Назад", callback_data="adm_polling")],
    ])
    await callback.message.edit_text(
        f"⏱ Интервал опроса для {name}:", reply_markup=kb, parse_mode="HTML"
    )


@router.callback_query(F.data.startswith("poll_setint_"))
async def cb_apply_interval(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    parts = callback.data.split("_")
    svc = parts[2]
    val = int(parts[3])
    await update_site_setting(svc, poll_interval=val)
    await _show_polling_panel(callback.message, callback.message.chat.id)


@router.callback_query(F.data.startswith("poll_custint_"))
async def cb_custom_interval(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.message.chat.id):
        return
    svc = callback.data.replace("poll_custint_", "")
    await state.update_data(service=svc)
    await state.set_state(SetInterval.value)
    await callback.message.edit_text(f"Введите интервал в секундах для {SVC_NAMES.get(svc, svc)}:")


@router.message(SetInterval.value)
async def msg_custom_interval(message: Message, state: FSMContext):
    if not await is_admin(message.chat.id):
        return
    data = await state.get_data()
    svc = data.get("service", "")
    try:
        val = int(message.text.strip())
        if val < 10:
            await message.answer("❌ Минимум 10 секунд.")
            return
        await update_site_setting(svc, poll_interval=val)
    except ValueError:
        await message.answer("❌ Введите число.")
        return
    await state.clear()
    await message.answer(
        f"✅ Интервал для {SVC_NAMES.get(svc, svc)}: {val}с",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚙️ Настройки опроса", callback_data="adm_polling")],
        ]),
    )



# ============== Test menu ==============

@router.callback_query(F.data == "adm_test_menu")
async def cb_test_menu(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔬 Тест приложения", callback_data="adm_test_app")],
        [InlineKeyboardButton(text="🆔 Нет OPI 10+ дней", callback_data="adm_missing_opi")],
        [InlineKeyboardButton(text="📨 Тест уведомлений", callback_data="adm_test_notif_menu")],
        [InlineKeyboardButton(text="↩ Админ-панель", callback_data="admin_menu")],
    ])
    await callback.message.edit_text(
        "<b>🧪 Тестирование</b>",
        reply_markup=kb, parse_mode="HTML",
    )


@router.callback_query(F.data == "adm_test_app")
async def adm_test_app(callback: CallbackQuery):
    """Full app test: fetch entries from all services, check OPI, show borrowers stats."""
    if not await is_admin(callback.message.chat.id):
        return

    await callback.message.edit_text("🔄 Запускаю полный тест приложения... Подождите 30-90 секунд.")
    results = await run_full_app_test_service(callback.message.chat.id)

    text = "\n".join(results)
    if len(text) > 4000:
        text = text[:4000] + "\n…"
    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Перетестировать", callback_data="adm_test_app")],
            [InlineKeyboardButton(text="🆔 Отчёт без OPI", callback_data="adm_missing_opi")],
            [InlineKeyboardButton(text="🧪 Меню тестов", callback_data="adm_test_menu")],
            [InlineKeyboardButton(text="↩ Админ-панель", callback_data="admin_menu")],
        ]),
    )


@router.callback_query(F.data == "adm_missing_opi")
async def adm_missing_opi(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return

    rows = await get_missing_opi_candidates(min_age_days=10, limit=50)
    if not rows:
        text = "✅ Заёмщиков без OPI старше 10 дней не найдено."
    else:
        lines = [
            "<b>🆔 Заёмщики без OPI старше 10 дней</b>",
            f"Всего найдено: <b>{len(rows)}</b>",
            "",
        ]
        for row in rows[:20]:
            services = row.get("services") or "—"
            first_seen = (row.get("first_seen") or "—")[:10]
            full_name = row.get("full_name") or "—"
            lines.append(
                f"• <code>{row['document_id']}</code> — {full_name}\n"
                f"  {services} / first_seen: {first_seen}"
            )
        if len(rows) > 20:
            lines.append(f"\n…и ещё {len(rows) - 20}")
        text = "\n".join(lines)

    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="adm_missing_opi")],
            [InlineKeyboardButton(text="🧪 Меню тестов", callback_data="adm_test_menu")],
            [InlineKeyboardButton(text="↩ Админ-панель", callback_data="admin_menu")],
        ]),
    )


# ============== Test notifications ==============

@router.callback_query(F.data == "adm_test_notif_menu")
async def adm_test_notif_menu(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📨 Все сервисы (по 1 шт.)", callback_data="adm_test_notif_all")],
        [InlineKeyboardButton(text="🥬 Kapusta", callback_data="adm_test_notif_kapusta")],
        [InlineKeyboardButton(text="🔵 FinKit", callback_data="adm_test_notif_finkit")],
        [InlineKeyboardButton(text="🟪 ЗАЙМись", callback_data="adm_test_notif_zaimis")],
        [InlineKeyboardButton(text="🧪 Меню тестов", callback_data="adm_test_menu")],
    ])
    await callback.message.edit_text(
        "<b>📨 Тест уведомлений</b>\n\n"
        "Выберите сервис для отправки тестового уведомления:",
        reply_markup=kb, parse_mode="HTML",
    )


async def _send_test_notification(callback: CallbackQuery, services: list[str]):
    """Fetch one real entry from each service and send formatted notification."""
    from bot.services.notifications.sender import format_notification
    from bot.domain.subscriptions import Subscription

    chat_id = callback.message.chat.id
    await callback.message.edit_text("🔄 Загружаю заявки для тестовых уведомлений...")

    sent = 0
    errors = []

    for svc in services:
        try:
            entry = await get_test_notification_entry_service(svc, chat_id)
            if entry:
                dummy_sub = Subscription(
                    id=0, chat_id=chat_id, service=svc,
                    label="🧪 Тестовое уведомление",
                )
                text = format_notification(NotificationEntryView.from_entry(entry), dummy_sub)
                bot = callback.bot
                await bot.send_message(
                    chat_id, text, parse_mode="HTML",
                    disable_web_page_preview=True,
                )
                sent += 1
            else:
                errors.append(f"{SVC_NAMES.get(svc, svc)}: нет заявок или credentials")
        except Exception as ex:
            errors.append(f"{SVC_NAMES.get(svc, svc)}: {str(ex)[:80]}")

    summary = f"✅ Отправлено {sent} тестовых уведомлений."
    if errors:
        summary += "\n⚠️ " + "\n⚠️ ".join(errors)
    await callback.message.edit_text(
        summary,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📨 Ещё раз", callback_data="adm_test_notif_menu")],
            [InlineKeyboardButton(text="🧪 Меню тестов", callback_data="adm_test_menu")],
            [InlineKeyboardButton(text="↩ Админ-панель", callback_data="admin_menu")],
        ]),
    )


@router.callback_query(F.data == "adm_test_notif_all")
async def adm_test_notif_all(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    await _send_test_notification(callback, ["kapusta", "finkit", "zaimis"])


@router.callback_query(F.data.startswith("adm_test_notif_"))
async def adm_test_notif_single(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    svc = callback.data.replace("adm_test_notif_", "")
    if svc in ("menu", "all"):
        return  # handled above
    await _send_test_notification(callback, [svc])


# ============== Archive loading ==============

@router.callback_query(F.data == "adm_load_archive")
async def adm_load_archive(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return

    await callback.message.edit_text(
        "ℹ️ Архив инвестиций больше не хранится отдельно.\n"
        "Данные заёмщиков обновляются в таблице <b>borrowers</b> каждую ночь в 00:00.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩ Админ-панель", callback_data="admin_menu")],
        ]),
    )
