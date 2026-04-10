"""Subscription management — inline buttons for creating/editing/deleting filters."""
from __future__ import annotations

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

from bot.database import get_db
from bot.handlers.start import is_allowed

log = logging.getLogger(__name__)
router = Router(name="subscriptions")

SERVICES = {
    "kapusta": "🥔 Капуста",
    "finkit": "🏦 Финкит",
    "mongo": "🦊 Монго",
    "zaimis": "💎 Займись",
}


class SubForm(StatesGroup):
    service = State()
    label = State()
    sum_min = State()
    sum_max = State()
    rating_min = State()
    period_min = State()
    period_max = State()
    interest_min = State()
    require_employed = State()
    min_settled_loans = State()


class SubEditForm(StatesGroup):
    waiting_value = State()


# ---- List subscriptions (via button or command) ----

async def _show_subscriptions(target, chat_id: int, edit: bool = False):
    """Show subscriptions list. target is Message or CallbackQuery.message."""
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM subscriptions WHERE chat_id=? ORDER BY service, id",
            (chat_id,),
        )
    finally:
        await db.close()

    if not rows:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать подписку", callback_data="sub_new")],
            [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
        ])
        text = "📋 У вас нет подписок."
        if edit:
            await target.edit_text(text, reply_markup=kb)
        else:
            await target.answer(text, reply_markup=kb)
        return

    # Check if any are night-paused
    has_active = any(r["is_active"] for r in rows)
    has_night_paused = any(r["night_paused"] for r in rows)

    lines = ["<b>📋 Ваши подписки:</b>\n"]
    for row in rows:
        svc = SERVICES.get(row["service"], row["service"])
        label = row["label"] or f"#{row['id']}"
        if row["night_paused"]:
            status = "🌙"
        elif row["is_active"]:
            status = "✅"
        else:
            status = "⏸"
        filters = []
        if row["sum_min"] is not None:
            filters.append(f"≥{row['sum_min']:.0f}")
        if row["sum_max"] is not None:
            filters.append(f"≤{row['sum_max']:.0f}")
        if row["rating_min"] is not None:
            filters.append(f"рейт≥{row['rating_min']:.0f}")
        if row["period_min"] is not None:
            filters.append(f"срок≥{row['period_min']}д")
        if row["period_max"] is not None:
            filters.append(f"срок≤{row['period_max']}д")
        if row["interest_min"] is not None:
            filters.append(f"ставка≥{row['interest_min']:.1f}%")
        try:
            if row["require_employed"]:
                filters.append("👔")
        except (IndexError, KeyError):
            pass
        try:
            if row["min_settled_loans"]:
                filters.append(f"возвр≥{row['min_settled_loans']}")
        except (IndexError, KeyError):
            pass

        filters_str = ", ".join(filters) if filters else "без фильтров"
        lines.append(f"{status} <b>{svc}</b> — {label}\n   {filters_str}")

    buttons = [
        [InlineKeyboardButton(text="➕ Создать подписку", callback_data="sub_new")],
        [InlineKeyboardButton(text="✏️ Редактировать", callback_data="sub_edit_choose")],
        [InlineKeyboardButton(text="🗑 Удалить подписку", callback_data="sub_delete_choose")],
        [InlineKeyboardButton(text="⏸/▶ Вкл/Выкл подписку", callback_data="sub_toggle_choose")],
    ]

    # Night pause / resume buttons
    if has_active and not has_night_paused:
        buttons.append([InlineKeyboardButton(text="🌙 Ночная пауза", callback_data="sub_night_pause")])
    if has_night_paused:
        buttons.append([InlineKeyboardButton(text="☀️ Утренний старт", callback_data="sub_night_resume")])

    buttons.append([InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    text = "\n".join(lines)
    if edit:
        await target.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await target.answer(text, reply_markup=kb, parse_mode="HTML")

@router.message(Command("subscriptions"))
async def cmd_subscriptions(message: Message):
    if not await is_allowed(message.chat.id):
        return
    await _show_subscriptions(message, message.chat.id, edit=False)


@router.callback_query(F.data == "subs_menu")
async def cb_subs_menu(callback: CallbackQuery, state: FSMContext):
    if not await is_allowed(callback.message.chat.id):
        return
    await state.clear()
    await _show_subscriptions(callback.message, callback.message.chat.id, edit=True)


# ---- Night pause / resume ----

@router.callback_query(F.data == "sub_night_pause")
async def sub_night_pause(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    db = await get_db()
    try:
        await db.execute(
            "UPDATE subscriptions SET is_active=0, night_paused=1 WHERE chat_id=? AND is_active=1",
            (chat_id,),
        )
        await db.commit()
    finally:
        await db.close()
    await callback.answer("🌙 Все подписки на паузе. Спокойной ночи!")
    await _show_subscriptions(callback.message, chat_id, edit=True)


@router.callback_query(F.data == "sub_night_resume")
async def sub_night_resume(callback: CallbackQuery):
    chat_id = callback.message.chat.id
    db = await get_db()
    try:
        await db.execute(
            "UPDATE subscriptions SET is_active=1, night_paused=0 WHERE chat_id=? AND night_paused=1",
            (chat_id,),
        )
        await db.commit()
    finally:
        await db.close()
    await callback.answer("☀️ Подписки возобновлены!")
    await _show_subscriptions(callback.message, chat_id, edit=True)


# ---- Create new subscription (simplified — no max rate/rating/income) ----

@router.callback_query(F.data == "sub_new")
async def sub_new_start(callback: CallbackQuery, state: FSMContext):
    buttons = [
        [InlineKeyboardButton(text=name, callback_data=f"sub_svc_{key}")]
        for key, name in SERVICES.items()
    ]
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.edit_text("Выберите сайт:", reply_markup=kb)
    await state.set_state(SubForm.service)


@router.callback_query(F.data.startswith("sub_svc_"))
async def sub_pick_service(callback: CallbackQuery, state: FSMContext):
    service = callback.data.replace("sub_svc_", "")
    await state.update_data(service=service)
    await callback.message.edit_text(
        f"Сайт: <b>{SERVICES[service]}</b>\n\n"
        "Введите название подписки (или <code>-</code> пропустить):",
        parse_mode="HTML",
    )
    await state.set_state(SubForm.label)


@router.message(SubForm.label)
async def sub_set_label(message: Message, state: FSMContext):
    label = message.text.strip() if message.text.strip() != "-" else None
    await state.update_data(label=label)
    await message.answer("💰 Мин. сумма? (<code>-</code> пропустить)", parse_mode="HTML")
    await state.set_state(SubForm.sum_min)


@router.message(SubForm.sum_min)
async def sub_set_sum_min(message: Message, state: FSMContext):
    await state.update_data(sum_min=_parse_float(message.text))
    await message.answer("💰 Макс. сумма? (<code>-</code> пропустить)", parse_mode="HTML")
    await state.set_state(SubForm.sum_max)


@router.message(SubForm.sum_max)
async def sub_set_sum_max(message: Message, state: FSMContext):
    await state.update_data(sum_max=_parse_float(message.text))
    await message.answer("📊 Мин. рейтинг? (<code>-</code> пропустить)", parse_mode="HTML")
    await state.set_state(SubForm.rating_min)


@router.message(SubForm.rating_min)
async def sub_set_rating_min(message: Message, state: FSMContext):
    await state.update_data(rating_min=_parse_float(message.text))
    await message.answer("📅 Мин. срок (дней)? (<code>-</code> пропустить)", parse_mode="HTML")
    await state.set_state(SubForm.period_min)


@router.message(SubForm.period_min)
async def sub_set_period_min(message: Message, state: FSMContext):
    await state.update_data(period_min=_parse_int(message.text))
    await message.answer("📅 Макс. срок (дней)? (<code>-</code> пропустить)", parse_mode="HTML")
    await state.set_state(SubForm.period_max)


@router.message(SubForm.period_max)
async def sub_set_period_max(message: Message, state: FSMContext):
    await state.update_data(period_max=_parse_int(message.text))
    await message.answer("💵 Мин. ставка (%/день)? (<code>-</code> пропустить)", parse_mode="HTML")
    await state.set_state(SubForm.interest_min)


@router.message(SubForm.interest_min)
async def sub_set_interest_min(message: Message, state: FSMContext):
    await state.update_data(interest_min=_parse_float(message.text))

    data = await state.get_data()
    service = data.get("service")

    # Employment filter for zaimis/finkit
    if service in ("zaimis", "finkit"):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Только трудоустроенных", callback_data="sub_emp_yes"),
                InlineKeyboardButton(text="➡️ Не важно", callback_data="sub_emp_skip"),
            ],
        ])
        await message.answer("🏢 Фильтр по трудоустройству?", reply_markup=kb)
        await state.set_state(SubForm.require_employed)
    else:
        await state.update_data(require_employed=None, min_settled_loans=None)
        await _save_subscription(message, state)


@router.callback_query(F.data.startswith("sub_emp_"))
async def sub_set_employed(callback: CallbackQuery, state: FSMContext):
    val = True if callback.data == "sub_emp_yes" else None
    await state.update_data(require_employed=val)

    data = await state.get_data()
    service = data.get("service")

    # min_settled_loans for finkit
    if service == "finkit":
        await callback.message.edit_text(
            "📊 Мин. кол-во возвратов в срок? (<code>-</code> или <code>0</code> — не важно)",
            parse_mode="HTML",
        )
        await state.set_state(SubForm.min_settled_loans)
    else:
        await state.update_data(min_settled_loans=None)
        await _save_subscription(callback.message, state, edit=True)


@router.message(SubForm.min_settled_loans)
async def sub_set_min_settled(message: Message, state: FSMContext):
    val = _parse_int(message.text)
    if val is not None and val <= 0:
        val = None
    await state.update_data(min_settled_loans=val)
    await _save_subscription(message, state)


async def _save_subscription(target, state: FSMContext, edit: bool = False):
    """Save subscription to DB."""
    data = await state.get_data()
    chat_id = target.chat.id

    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO subscriptions
            (chat_id, service, label, sum_min, sum_max, rating_min,
             period_min, period_max, interest_min,
             require_employed, min_settled_loans)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                chat_id,
                data["service"],
                data.get("label"),
                data.get("sum_min"),
                data.get("sum_max"),
                data.get("rating_min"),
                data.get("period_min"),
                data.get("period_max"),
                data.get("interest_min"),
                1 if data.get("require_employed") else None,
                data.get("min_settled_loans"),
            ),
        )
        await db.commit()
    finally:
        await db.close()

    await state.clear()
    svc = SERVICES.get(data["service"], data["service"])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔔 Подписки", callback_data="subs_menu")],
        [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
    ])
    text = f"✅ Подписка создана!\nСайт: {svc}\nНазвание: {data.get('label') or '—'}"
    if edit:
        await target.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await target.answer(text, reply_markup=kb, parse_mode="HTML")


# ---- Edit subscription ----

EDITABLE_FIELDS = {
    "label": ("Название", "text"),
    "sum_min": ("Мин. сумма", "float"),
    "sum_max": ("Макс. сумма", "float"),
    "rating_min": ("Мин. рейтинг", "float"),
    "period_min": ("Мин. срок (дней)", "int"),
    "period_max": ("Макс. срок (дней)", "int"),
    "interest_min": ("Мин. ставка (%)", "float"),
    "require_employed": ("Трудоустройство", "bool"),
    "min_settled_loans": ("Мин. возвратов в срок", "int"),
}


@router.callback_query(F.data == "sub_edit_choose")
async def sub_edit_choose(callback: CallbackQuery):
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT id, service, label FROM subscriptions WHERE chat_id=?",
            (callback.message.chat.id,),
        )
    finally:
        await db.close()

    if not rows:
        await callback.answer("Нет подписок")
        return

    buttons = []
    for row in rows:
        svc = SERVICES.get(row["service"], row["service"])
        label = row["label"] or f"#{row['id']}"
        buttons.append([
            InlineKeyboardButton(
                text=f"✏️ {svc} — {label}",
                callback_data=f"sub_edit_{row['id']}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="↩ Подписки", callback_data="subs_menu")])
    await callback.message.edit_text("Выберите подписку для редактирования:",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.regexp(r"^sub_edit_(\d+)$"))
async def sub_edit_show(callback: CallbackQuery):
    sub_id = int(callback.data.split("_")[-1])
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM subscriptions WHERE id=? AND chat_id=?",
            (sub_id, callback.message.chat.id),
        )
    finally:
        await db.close()

    if not rows:
        await callback.answer("Подписка не найдена")
        return

    row = rows[0]
    svc = SERVICES.get(row["service"], row["service"])
    label = row["label"] or f"#{row['id']}"

    lines = [f"✏️ <b>Редактирование: {svc} — {label}</b>\n"]
    buttons = []

    for field, (name, _ftype) in EDITABLE_FIELDS.items():
        # Skip employment/settled for services that don't support them
        if field in ("require_employed", "min_settled_loans") and row["service"] not in ("finkit", "zaimis"):
            if field == "min_settled_loans":
                continue
            if field == "require_employed":
                continue

        try:
            val = row[field]
        except (KeyError, IndexError):
            val = None

        if field == "require_employed":
            display = "✅ Да" if val else "❌ Нет"
        elif val is None:
            display = "—"
        else:
            display = str(val)

        lines.append(f"  <b>{name}:</b> {display}")
        buttons.append([
            InlineKeyboardButton(
                text=f"✏️ {name}: {display}",
                callback_data=f"sub_ef_{sub_id}_{field}",
            )
        ])

    buttons.append([InlineKeyboardButton(text="↩ Подписки", callback_data="subs_menu")])
    await callback.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML",
    )


@router.callback_query(F.data.regexp(r"^sub_ef_(\d+)_(\w+)$"))
async def sub_edit_field(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("_", 3)
    sub_id = int(parts[2])
    field = parts[3]

    if field not in EDITABLE_FIELDS:
        await callback.answer("Неизвестное поле")
        return

    name, ftype = EDITABLE_FIELDS[field]

    if ftype == "bool":
        # Toggle directly
        db = await get_db()
        try:
            rows = await db.execute_fetchall(
                f"SELECT {field} FROM subscriptions WHERE id=?", (sub_id,)
            )
            current = rows[0][field] if rows else None
            new_val = None if current else 1
            await db.execute(
                f"UPDATE subscriptions SET {field}=? WHERE id=?",
                (new_val, sub_id),
            )
            await db.commit()
        finally:
            await db.close()
        await callback.answer(f"{name}: {'✅' if new_val else '❌'}")
        # Refresh edit view
        callback.data = f"sub_edit_{sub_id}"
        await sub_edit_show(callback)
        return

    await state.update_data(edit_sub_id=sub_id, edit_field=field, edit_ftype=ftype)
    await callback.message.edit_text(
        f"Введите новое значение для <b>{name}</b>\n"
        "(<code>-</code> чтобы убрать фильтр)",
        parse_mode="HTML",
    )
    await state.set_state(SubEditForm.waiting_value)


@router.message(SubEditForm.waiting_value)
async def sub_edit_save_value(message: Message, state: FSMContext):
    data = await state.get_data()
    sub_id = data["edit_sub_id"]
    field = data["edit_field"]
    ftype = data["edit_ftype"]

    text = message.text.strip() if message.text else "-"
    if text in ("-", "—", ""):
        val = None
    elif ftype == "float":
        val = _parse_float(text)
    elif ftype == "int":
        val = _parse_int(text)
    else:
        val = text

    if field == "label" and val is None:
        val = None  # allowed to clear label

    db = await get_db()
    try:
        await db.execute(
            f"UPDATE subscriptions SET {field}=? WHERE id=? AND chat_id=?",
            (val, sub_id, message.chat.id),
        )
        await db.commit()
    finally:
        await db.close()

    await state.clear()
    name = EDITABLE_FIELDS[field][0]
    display = str(val) if val is not None else "—"
    await message.answer(
        f"✅ {name} → {display}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Продолжить редактирование", callback_data=f"sub_edit_{sub_id}")],
            [InlineKeyboardButton(text="🔔 Подписки", callback_data="subs_menu")],
        ]),
    )


# ---- Delete subscription ----

@router.callback_query(F.data == "sub_delete_choose")
async def sub_delete_choose(callback: CallbackQuery):
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT id, service, label FROM subscriptions WHERE chat_id=?",
            (callback.message.chat.id,),
        )
    finally:
        await db.close()

    if not rows:
        await callback.answer("Нет подписок для удаления")
        return

    buttons = []
    for row in rows:
        svc = SERVICES.get(row["service"], row["service"])
        label = row["label"] or f"#{row['id']}"
        buttons.append([
            InlineKeyboardButton(
                text=f"🗑 {svc} — {label}",
                callback_data=f"sub_del_{row['id']}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="↩ Подписки", callback_data="subs_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.edit_text("Выберите подписку для удаления:", reply_markup=kb)


@router.callback_query(F.data.startswith("sub_del_"))
async def sub_delete_confirm(callback: CallbackQuery):
    sub_id = int(callback.data.replace("sub_del_", ""))
    db = await get_db()
    try:
        await db.execute(
            "DELETE FROM subscriptions WHERE id=? AND chat_id=?",
            (sub_id, callback.message.chat.id),
        )
        await db.commit()
    finally:
        await db.close()

    await callback.message.edit_text(
        "✅ Подписка удалена.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔔 Подписки", callback_data="subs_menu")],
            [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
        ]),
    )


# ---- Toggle subscription ----

@router.callback_query(F.data == "sub_toggle_choose")
async def sub_toggle_choose(callback: CallbackQuery):
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT id, service, label, is_active FROM subscriptions WHERE chat_id=?",
            (callback.message.chat.id,),
        )
    finally:
        await db.close()

    if not rows:
        await callback.answer("Нет подписок")
        return

    buttons = []
    for row in rows:
        svc = SERVICES.get(row["service"], row["service"])
        label = row["label"] or f"#{row['id']}"
        status = "✅" if row["is_active"] else "⏸"
        buttons.append([
            InlineKeyboardButton(
                text=f"{status} {svc} — {label}",
                callback_data=f"sub_tog_{row['id']}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="↩ Подписки", callback_data="subs_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.edit_text("Выберите подписку:", reply_markup=kb)


@router.callback_query(F.data.startswith("sub_tog_"))
async def sub_toggle(callback: CallbackQuery):
    sub_id = int(callback.data.replace("sub_tog_", ""))
    db = await get_db()
    try:
        await db.execute(
            """UPDATE subscriptions
               SET is_active = CASE WHEN is_active=1 THEN 0 ELSE 1 END,
                   night_paused = 0
               WHERE id=? AND chat_id=?""",
            (sub_id, callback.message.chat.id),
        )
        await db.commit()
    finally:
        await db.close()

    await callback.answer("Переключено!")
    await sub_toggle_choose(callback)


@router.callback_query(F.data == "sub_back")
async def sub_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await _show_subscriptions(callback.message, callback.message.chat.id, edit=True)


# ---- Helpers ----

def _parse_float(text: str | None) -> float | None:
    if not text or text.strip() in ("-", "—", ""):
        return None
    try:
        return float(text.strip().replace(",", "."))
    except ValueError:
        return None


def _parse_int(text: str | None) -> int | None:
    if not text or text.strip() in ("-", "—", ""):
        return None
    try:
        return int(text.strip())
    except ValueError:
        return None
