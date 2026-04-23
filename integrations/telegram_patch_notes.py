from __future__ import annotations

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Message, TelegramObject

from bot.services.base.access import is_allowed
from bot.services.start.service import (
    ensure_chat_user,
    get_pending_patch_notes,
    mark_patch_notes_seen,
)


class PatchNotesMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data: dict):
        result = await handler(event, data)

        chat_id: int | None = None
        user = None
        bot = getattr(event, "bot", None)

        if isinstance(event, Message):
            chat_id = event.chat.id
            user = event.from_user
        elif isinstance(event, CallbackQuery) and event.message:
            chat_id = event.message.chat.id
            user = event.from_user

        if chat_id is None or user is None or bot is None:
            return result

        await ensure_chat_user(
            chat_id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
        )
        if not await is_allowed(chat_id):
            return result

        patch_notes = await get_pending_patch_notes(chat_id)
        if not patch_notes:
            return result

        try:
            for note in patch_notes:
                await bot.send_message(chat_id, note, disable_web_page_preview=True)
            await mark_patch_notes_seen(chat_id)
        except Exception:
            return result

        return result


__all__ = ["PatchNotesMiddleware"]
