from bot.config import ADMIN_CHAT_ID
from bot.repositories.users import is_chat_admin, is_chat_allowed


async def is_allowed(chat_id: int) -> bool:
    return await is_chat_allowed(chat_id)


async def is_admin(chat_id: int) -> bool:
    return await is_chat_admin(chat_id, ADMIN_CHAT_ID)


__all__ = ["is_admin", "is_allowed"]
