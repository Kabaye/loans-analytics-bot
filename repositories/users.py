from bot.repositories.db import get_db


async def is_chat_allowed(chat_id: int) -> bool:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT 1 FROM users WHERE chat_id=? AND is_allowed=1",
            (chat_id,),
        )
        return len(rows) > 0
    finally:
        await db.close()


async def is_chat_admin(chat_id: int, fallback_admin_chat_id: int | None = None) -> bool:
    if fallback_admin_chat_id and chat_id == fallback_admin_chat_id:
        return True
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT 1 FROM users WHERE chat_id=? AND is_admin=1",
            (chat_id,),
        )
        return len(rows) > 0
    finally:
        await db.close()


async def ensure_admin_user(chat_id: int) -> None:
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR IGNORE INTO users (chat_id, is_admin, is_allowed) VALUES (?, 1, 1)",
            (chat_id,),
        )
        await db.execute(
            "UPDATE users SET is_admin=1, is_allowed=1 WHERE chat_id=?",
            (chat_id,),
        )
        await db.commit()
    finally:
        await db.close()


__all__ = ["ensure_admin_user", "is_chat_admin", "is_chat_allowed"]
