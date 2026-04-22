"""Admin handler — whitelist management + polling controls + test/archive."""
from __future__ import annotations

import asyncio
import json
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

from bot.database import (
    get_db, get_all_site_settings, update_site_setting,
    get_borrowers_stats, get_borrowers_count, get_missing_opi_candidates,
)
from bot.config import ADMIN_CHAT_ID
from bot.services.scheduler import get_export_parsers

log = logging.getLogger(__name__)
router = Router(name="admin")

SVC_NAMES = {
    "kapusta": "🥬 Kapusta",
    "finkit": "🔵 FinKit",
    "zaimis": "🟪 ЗАЙМись",
}


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


async def _get_test_parser(service: str, requester_chat_id: int):
    """Reuse the same parser/session acquisition path as export."""
    target_chat_id = requester_chat_id
    if service in ("finkit", "zaimis"):
        db = await get_db()
        try:
            rows = await db.execute_fetchall(
                "SELECT chat_id FROM credentials WHERE service = ? ORDER BY id LIMIT 1",
                (service,),
            )
        finally:
            await db.close()
        if not rows:
            return None
        target_chat_id = rows[0]["chat_id"]

    parsers = await get_export_parsers(service, target_chat_id)
    return parsers[0] if parsers else None


class AdminAddUser(StatesGroup):
    chat_id = State()


async def _show_admin_panel(target, chat_id: int, edit: bool = False):
    """Show admin panel. target is Message or CallbackQuery.message."""
    if not await is_admin(chat_id):
        return

    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT chat_id, username, first_name, last_name, is_allowed, is_admin FROM users ORDER BY created_at"
        )
    finally:
        await db.close()

    lines = ["<b>👑 Администрирование</b>\n"]
    for row in rows:
        status = "✅" if row["is_allowed"] else "⛔"
        admin_badge = " 👑" if row["is_admin"] else ""
        display = _display_name(row)
        lines.append(f"{status}{admin_badge} <code>{row['chat_id']}</code> — {display}")

    buttons = [
        [InlineKeyboardButton(text="➕ Добавить пользователя", callback_data="adm_add")],
        [InlineKeyboardButton(text="✅ Разрешить доступ", callback_data="adm_allow_choose")],
        [InlineKeyboardButton(text="⛔ Заблокировать", callback_data="adm_block_choose")],
        [InlineKeyboardButton(text="👑 Назначить админом", callback_data="adm_promote_choose")],
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

    db = await get_db()
    try:
        await db.execute(
            "INSERT OR IGNORE INTO users (chat_id, is_allowed) VALUES (?, 1)",
            (new_chat_id,),
        )
        await db.execute(
            "UPDATE users SET is_allowed=1 WHERE chat_id=?",
            (new_chat_id,),
        )
        await db.commit()
    finally:
        await db.close()

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
    db = await get_db()
    try:
        await db.execute("UPDATE users SET is_allowed=1 WHERE chat_id=?", (chat_id,))
        await db.commit()
        row = await db.execute_fetchall(
            "SELECT chat_id, username, first_name, last_name FROM users WHERE chat_id=?", (chat_id,)
        )
    finally:
        await db.close()
    display = _display_name(row[0]) if row else str(chat_id)
    await callback.message.edit_text(f"✅ Пользователь {display} разрешён.")


@router.callback_query(F.data == "adm_block_choose")
async def adm_block_choose(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    # Exclude self from block list
    await _show_user_list(callback, "adm_block_", is_allowed=1,
                          exclude_chat_id=callback.message.chat.id)


@router.callback_query(F.data.startswith("adm_block_"))
async def adm_block_user(callback: CallbackQuery):
    if not await is_admin(callback.message.chat.id):
        return
    chat_id = int(callback.data.replace("adm_block_", ""))

    # Prevent self-block
    if chat_id == callback.message.chat.id:
        await callback.answer("❌ Нельзя заблокировать самого себя!", show_alert=True)
        return

    db = await get_db()
    try:
        await db.execute("UPDATE users SET is_allowed=0 WHERE chat_id=?", (chat_id,))
        await db.commit()
        row = await db.execute_fetchall(
            "SELECT chat_id, username, first_name, last_name FROM users WHERE chat_id=?", (chat_id,)
        )
    finally:
        await db.close()
    display = _display_name(row[0]) if row else str(chat_id)
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
    db = await get_db()
    try:
        await db.execute("UPDATE users SET is_admin=1 WHERE chat_id=?", (chat_id,))
        await db.commit()
        row = await db.execute_fetchall(
            "SELECT chat_id, username, first_name, last_name FROM users WHERE chat_id=?", (chat_id,)
        )
    finally:
        await db.close()
    display = _display_name(row[0]) if row else str(chat_id)
    await callback.message.edit_text(f"👑 Пользователь {display} назначен админом.")


async def _show_user_list(callback: CallbackQuery, prefix: str, is_allowed: int,
                          exclude_chat_id: int | None = None):
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT chat_id, username, first_name, last_name FROM users WHERE is_allowed=?",
            (is_allowed,),
        )
    finally:
        await db.close()

    # Filter out excluded user (self-protection)
    filtered = [r for r in rows if r["chat_id"] != exclude_chat_id] if exclude_chat_id else list(rows)

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

    from bot.services.opi_checker import OPIChecker

    results: list[str] = []

    # --- Kapusta ---
    try:
        kp = await _get_test_parser("kapusta", callback.message.chat.id)
        if kp is None:
            entries_k = []
            results.append("🥬 <b>Kapusta</b>: ❌ parser unavailable")
        else:
            entries_k = await asyncio.wait_for(kp.fetch_borrows(), timeout=30)
            results.append(f"🥬 <b>Kapusta</b>: {len(entries_k)} заявок")
        if entries_k:
            e = entries_k[0] if hasattr(entries_k[0], "amount") else type("E", (), entries_k[0])()
            amt = e.amount if hasattr(e, "amount") else e.get("amount", 0) if isinstance(e, dict) else 0
            results.append(f"  └ первая: {amt:.0f} BYN")
    except Exception as ex:
        results.append(f"🥬 <b>Kapusta</b>: ❌ {ex}")

    # --- FinKit ---
    try:
        fp = await _get_test_parser("finkit", callback.message.chat.id)
        if fp is None:
            results.append("🔵 <b>FinKit</b>: ⚠️ Нет credentials")
        else:
            entries_f = await fp.fetch_borrows()
            results.append(f"🔵 <b>FinKit</b>: {len(entries_f)} заявок")
            if entries_f:
                e = entries_f[0]
                results.append(
                    f"  └ #{e.id}: {e.amount:.0f} BYN, {e.period_days}д, "
                    f"рейт {e.credit_score:.0f}, {e.interest_day:.2f}%/д"
                )
                # PDF + OPI enrichment on first with contract
                to_enrich = [ee for ee in entries_f if ee.contract_url][:1]
                if to_enrich:
                    await fp.enrich_with_pdf(to_enrich)
                    ee = to_enrich[0]
                    results.append(
                        f"  └ PDF: ФИО={ee.full_name or '—'}, ИН={ee.document_id or '—'}"
                    )
                    if ee.document_id:
                        opi = OPIChecker()
                        try:
                            res = await opi.check(ee.document_id, use_cache=False)
                            if res.error:
                                results.append(f"  └ ОПИ: ⚠️ {res.error}")
                            elif res.has_debt:
                                results.append(
                                    f"  └ ОПИ: 🔴 ДОЛГ {res.debt_amount:.2f} BYN ({res.full_name or '—'})"
                                )
                            else:
                                results.append("  └ ОПИ: 🟢 Нет задолженности")
                        finally:
                            await opi.close()
                    else:
                        results.append("  └ ОПИ: ⏭ нет ИН")
                else:
                    results.append("  └ PDF: нет contract_url")
    except Exception as ex:
        results.append(f"🔵 <b>FinKit</b>: ❌ {ex}")

    # --- ЗАЙМись ---
    try:
        zp = await _get_test_parser("zaimis", callback.message.chat.id)
        if zp is None:
            results.append("🟪 <b>ЗАЙМись</b>: ⚠️ Нет credentials")
        else:
            entries_z = await zp.fetch_borrows()
            results.append(f"🟪 <b>ЗАЙМись</b>: {len(entries_z)} заявок")
            if entries_z:
                e = entries_z[0]
                results.append(
                    f"  └ #{e.id[:8]}…: {e.amount:.0f} BYN, {e.period_days}д, "
                    f"рейт {e.credit_score:.0f}, {e.interest_day:.2f}%/д"
                )
    except Exception as ex:
        results.append(f"🟪 <b>ЗАЙМись</b>: ❌ {ex}")

    # --- Borrowers + borrower_info stats ---
    try:
        stats = await get_borrowers_stats()
        missing_opi = await get_missing_opi_candidates(min_age_days=10, limit=500)
        if stats and (stats.get("total") or stats.get("mappings")):
            results.append(
                f"\n📊 <b>Карточки заёмщиков (borrower_info)</b>:\n"
                f"  Всего карточек: {stats.get('total', 0)}\n"
                f"  OPI проверено: {stats.get('opi_checked', 0)}\n"
                f"  С долгами: {stats.get('with_debt', 0)}\n"
                f"  С инвестициями: {stats.get('with_investments', 0)}\n"
                f"  Маппингов (borrowers): {stats.get('mappings', 0)}\n"
                f"  С ИН: {stats.get('with_document', 0)}\n"
                f"  Нет OPI 10+ дней: {len(missing_opi)}"
            )
        else:
            results.append("\n📊 <b>Карточки заёмщиков</b>: пусто")
    except Exception as ex:
        results.append(f"\n📊 <b>Карточки заёмщиков</b>: ❌ {ex}")

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
    from bot.services.notifier import format_notification

    chat_id = callback.message.chat.id
    await callback.message.edit_text("🔄 Загружаю заявки для тестовых уведомлений...")

    sent = 0
    errors = []

    for svc in services:
        try:
            entry = None
            if svc == "kapusta":
                kp = await _get_test_parser("kapusta", chat_id)
                if kp:
                    entries = await kp.fetch_borrows()
                    if entries:
                        entry = entries[0]

            elif svc == "finkit":
                fp = await _get_test_parser("finkit", chat_id)
                if fp:
                    entries = await fp.fetch_borrows()
                    if entries:
                        entry = entries[0]

            elif svc == "zaimis":
                zp = await _get_test_parser("zaimis", chat_id)
                if zp:
                    entries = await zp.fetch_borrows()
                    if entries:
                        entry = entries[0]

            if entry:
                from bot.models import Subscription
                dummy_sub = Subscription(
                    id=0, chat_id=chat_id, service=svc,
                    label="🧪 Тестовое уведомление",
                )
                text = format_notification(entry, dummy_sub)
                from aiogram import Bot
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
