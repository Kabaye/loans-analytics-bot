from __future__ import annotations

from bot import config


async def send_admin_html_message(bot, text: str) -> None:
    if not config.ADMIN_CHAT_ID:
        return
    await bot.send_message(config.ADMIN_CHAT_ID, text, parse_mode="HTML")


__all__ = ["send_admin_html_message"]
