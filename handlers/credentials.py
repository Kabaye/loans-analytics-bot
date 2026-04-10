"""Credentials handler — users input their Finkit/Zaimis login+password."""
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

from bot.database import get_db, upsert_borrower_from_investment
from bot.handlers.start import is_allowed

log = logging.getLogger(__name__)
router = Router(name="credentials")

AUTH_SERVICES = {
    "finkit": "🏦 Финкит",
    "zaimis": "💎 Займись",
}


class CredForm(StatesGroup):
    service = State()
    login = State()
    password = State()


async def _show_credentials(target, chat_id: int, edit: bool = False):
    """Show credentials list. target is Message or CallbackQuery.message."""
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT service, login FROM credentials WHERE chat_id=?",
            (chat_id,),
        )
    finally:
        await db.close()

    lines = ["<b>🔑 Ваши учётные данные:</b>\n"]
    existing = {r["service"]: r["login"] for r in rows}
    for svc, name in AUTH_SERVICES.items():
        if svc in existing:
            lines.append(f"✅ {name}: {existing[svc]}")
        else:
            lines.append(f"❌ {name}: не настроено")

    buttons = [
        [InlineKeyboardButton(text=f"{'✏️' if svc in existing else '➕'} {name}",
                              callback_data=f"cred_set_{svc}")]
        for svc, name in AUTH_SERVICES.items()
    ]
    if existing:
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

    # Save to DB
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO credentials (chat_id, service, login, password)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id, service)
            DO UPDATE SET login=excluded.login, password=excluded.password
            """,
            (message.chat.id, data["service"], data["login"], data["password"]),
        )
        await db.commit()
    finally:
        await db.close()

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
        data["service"], data["login"], data["password"], msg
    ))


async def _autoload_investments(service: str, login: str, password: str, status_msg: Message):
    """Load investment archive after credential save."""
    try:
        log.info("Auto-load investments starting for %s", service)
        count = 0
        if service == "zaimis":
            count = await _load_zaimis_investments(login, password)
        elif service == "finkit":
            count = await _load_finkit_investments(login, password)

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


async def _load_zaimis_investments(login: str, password: str) -> int:
    """Fetch Zaimis investments and upsert into borrowers table."""
    from bot.parsers.zaimis import ZaimisParser
    zp = ZaimisParser()
    try:
        ok = await zp.login(login, password)
        if not ok:
            raise RuntimeError("Login failed")

        orders = await zp.fetch_investments()
        # Aggregate per borrower
        stats: dict[str, dict] = {}
        for order in orders:
            offer = order.get("offer", {}) or {}
            owner = offer.get("owner", {}) or {}
            buid = str(owner.get("id", ""))
            if not buid:
                continue
            if buid not in stats:
                stats[buid] = {
                    "full_name": owner.get("displayName", ""),
                    "total": 0, "settled": 0, "overdue": 0,
                    "ratings": [], "invested": 0.0,
                }
            s = stats[buid]
            s["total"] += 1
            state = order.get("state")
            if state == 3:
                s["settled"] += 1
            if state == 4:
                s["overdue"] += 1
            try:
                s["invested"] += float(order.get("amount", 0))
            except (ValueError, TypeError):
                pass
            score = offer.get("score")
            if score is not None:
                try:
                    s["ratings"].append(float(score))
                except (ValueError, TypeError):
                    pass

        for buid, s in stats.items():
            avg_r = sum(s["ratings"]) / len(s["ratings"]) if s["ratings"] else None
            await upsert_borrower_from_investment(
                service="zaimis", borrower_user_id=buid,
                full_name=s["full_name"] or None,
                total_loans=s["total"], settled_loans=s["settled"],
                overdue_loans=s["overdue"], avg_rating=avg_r,
                total_invested=s["invested"],
            )
        return len(orders)
    finally:
        await zp.close()


async def _load_finkit_investments(login: str, password: str) -> int:
    """Fetch Finkit investments and upsert into borrowers table."""
    from bot.parsers.finkit import FinkitParser
    fp = FinkitParser()
    try:
        ok = await fp.login(login, password)
        if not ok:
            raise RuntimeError("Login failed")

        session = await fp._get_session()
        cookie_str = "; ".join(f"{k}={v}" for k, v in fp._session_cookies.items())
        headers = {"Accept": "application/json", "Referer": "https://finkit.by/", "Cookie": cookie_str}

        # Aggregate per borrower from investment list
        borrower_stats: dict[str, dict] = {}
        total = 0
        page = 1
        while True:
            url = f"https://api-p2p.finkit.by/user/investments/?page={page}"
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    break
                data = await resp.json()

            for inv in data.get("results", []):
                total += 1
                buid = inv.get("user")
                if not buid:
                    buid = inv.get("loan")  # fallback to loan ID
                if not buid:
                    continue
                buid = str(buid)
                bname = inv.get("borrower_full_name", "")
                if buid not in borrower_stats:
                    borrower_stats[buid] = {
                        "full_name": bname, "total": 0, "settled": 0,
                        "overdue": 0, "ratings": [], "invested": 0.0,
                    }
                s = borrower_stats[buid]
                s["total"] += 1
                status = inv.get("status")
                if status == "settled":
                    s["settled"] += 1
                if inv.get("is_overdue"):
                    s["overdue"] += 1
                try:
                    s["invested"] += float(inv.get("amount", 0))
                except (ValueError, TypeError):
                    pass
                try:
                    rating = float(inv.get("borrower_score", 0))
                    if rating > 0:
                        s["ratings"].append(rating)
                except (ValueError, TypeError):
                    pass

            if not data.get("next"):
                break
            page += 1

        # Upsert into borrowers
        for buid, s in borrower_stats.items():
            avg_r = sum(s["ratings"]) / len(s["ratings"]) if s["ratings"] else None
            await upsert_borrower_from_investment(
                service="finkit", borrower_user_id=buid,
                full_name=s["full_name"] or None,
                total_loans=s["total"], settled_loans=s["settled"],
                overdue_loans=s["overdue"], avg_rating=avg_r,
                total_invested=s["invested"],
            )
        return total
    finally:
        await fp.close()


# ---- Delete credentials ----

@router.callback_query(F.data == "cred_delete_choose")
async def cred_delete_choose(callback: CallbackQuery):
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT service, login FROM credentials WHERE chat_id=?",
            (callback.message.chat.id,),
        )
    finally:
        await db.close()

    buttons = []
    for row in rows:
        name = AUTH_SERVICES.get(row["service"], row["service"])
        buttons.append([
            InlineKeyboardButton(
                text=f"🗑 {name} ({row['login']})",
                callback_data=f"cred_del_{row['service']}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="↩ Учёт. данные", callback_data="creds_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    await callback.message.edit_text("Выберите для удаления:", reply_markup=kb)


@router.callback_query(F.data.startswith("cred_del_"))
async def cred_delete(callback: CallbackQuery):
    service = callback.data.replace("cred_del_", "")
    db = await get_db()
    try:
        await db.execute(
            "DELETE FROM credentials WHERE chat_id=? AND service=?",
            (callback.message.chat.id, service),
        )
        await db.commit()
    finally:
        await db.close()

    name = AUTH_SERVICES.get(service, service)
    await callback.message.edit_text(
        f"✅ Учётные данные для {name} удалены.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔑 Учёт. данные", callback_data="creds_menu")],
            [InlineKeyboardButton(text="↩ Главное меню", callback_data="main_menu")],
        ]),
    )


@router.callback_query(F.data == "cred_back")
async def cred_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await _show_credentials(callback.message, callback.message.chat.id, edit=True)
