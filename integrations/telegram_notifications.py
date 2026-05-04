from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections import OrderedDict

from aiogram import Bot, F, Router
from aiogram.exceptions import TelegramRetryAfter
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from bot.domain.borrowers import BorrowEntry
from bot.domain.borrower_views import NotificationEntryView
from bot.domain.raw_payloads import extract_raw_payload, format_raw_payload_preview
from bot.domain.subscriptions import Subscription
from bot.services.notifications.sender import format_notification, prepare_notifications

log = logging.getLogger(__name__)
router = Router(name="notifier")

_RAW_DATA_CACHE: OrderedDict[str, dict] = OrderedDict()
_RAW_DATA_CACHE_MAX = 500

SentNotificationRef = tuple[str, int, int, list[Subscription]]


def _store_raw_data(raw_data: dict | None) -> str | None:
    if not raw_data:
        return None
    key = uuid.uuid4().hex[:12]
    _RAW_DATA_CACHE[key] = raw_data
    while len(_RAW_DATA_CACHE) > _RAW_DATA_CACHE_MAX:
        _RAW_DATA_CACHE.popitem(last=False)
    return key


def _build_raw_markup(raw_data: dict | None) -> InlineKeyboardMarkup | None:
    raw_key = _store_raw_data(raw_data)
    if not raw_key:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Исходные данные", callback_data=f"raw_{raw_key}")]
        ]
    )


@router.callback_query(F.data.startswith("raw_"))
async def cb_show_raw_data(callback: CallbackQuery):
    key = callback.data.replace("raw_", "")
    raw = _RAW_DATA_CACHE.get(key)
    if not raw:
        await callback.answer("⏳ Данные устарели (перезапуск бота)", show_alert=True)
        return

    import html as html_mod

    text = format_raw_payload_preview(raw, limit=3900)
    escaped = html_mod.escape(text)
    await callback.message.reply(f"<pre>{escaped}</pre>", parse_mode="HTML")
    await callback.answer()


async def notify_users(
    bot: Bot,
    entries: list[BorrowEntry],
    service: str,
    *,
    skip_enrichment: bool = False,
) -> list[SentNotificationRef]:
    from bot.integrations.fsm_guard import enqueue, is_busy

    plans = await prepare_notifications(entries, service, skip_enrichment=skip_enrichment)
    if not plans:
        return []

    sent_refs: list[SentNotificationRef] = []
    for plan in plans:
        kb = _build_raw_markup(plan.raw_payload)

        if is_busy(plan.chat_id):
            enqueue(plan.chat_id, plan.text, kb)
            continue

        try:
            msg = await bot.send_message(
                plan.chat_id,
                plan.text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=kb,
            )
            sent_refs.append((plan.entry_id, plan.chat_id, msg.message_id, plan.matched_subscriptions))
            await asyncio.sleep(0.1)
        except TelegramRetryAfter as exc:
            log.warning("Flood control for %s: retry after %ds", plan.chat_id, exc.retry_after)
            await asyncio.sleep(min(exc.retry_after, 30))
            try:
                msg = await bot.send_message(
                    plan.chat_id,
                    plan.text,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                    reply_markup=kb,
                )
                sent_refs.append((plan.entry_id, plan.chat_id, msg.message_id, plan.matched_subscriptions))
            except Exception:
                pass
        except Exception as exc:
            log.warning("Failed to send notification to %s: %s", plan.chat_id, exc)

    log.info("Sent %d notifications for %s", len(sent_refs), service)
    return sent_refs


async def update_sent_notifications(
    bot: Bot,
    entries: list[BorrowEntry],
    sent_refs: list[SentNotificationRef],
    service: str,
) -> int:
    if not sent_refs:
        return 0

    entry_map = {entry.id: entry for entry in entries}
    edited = 0
    for entry_id, chat_id, message_id, matched_subs in sent_refs:
        entry = entry_map.get(entry_id)
        if not entry:
            continue

        entry_view = NotificationEntryView.from_entry(entry)
        new_text = format_notification(entry_view, matched_subs)
        kb = _build_raw_markup(extract_raw_payload(entry))

        try:
            await bot.edit_message_text(
                new_text,
                chat_id=chat_id,
                message_id=message_id,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=kb,
            )
            edited += 1
            await asyncio.sleep(0.1)
        except TelegramRetryAfter as exc:
            await asyncio.sleep(min(exc.retry_after, 30))
            try:
                await bot.edit_message_text(
                    new_text,
                    chat_id=chat_id,
                    message_id=message_id,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                    reply_markup=kb,
                )
                edited += 1
            except Exception:
                pass
        except Exception as exc:
            if "message is not modified" not in str(exc).lower():
                log.warning("Failed to edit notification %s/%s: %s", chat_id, message_id, exc)

    if edited:
        log.info("Edited %d/%d notifications for %s", edited, len(sent_refs), service)
    return edited


__all__ = ["notify_users", "router", "SentNotificationRef", "update_sent_notifications"]
