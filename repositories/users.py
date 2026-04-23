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


async def ensure_user(
    chat_id: int,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
) -> None:
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR IGNORE INTO users (chat_id, username, first_name, last_name) VALUES (?, ?, ?, ?)",
            (chat_id, username, first_name, last_name),
        )
        await db.execute(
            """
            UPDATE users
            SET username=COALESCE(?, username),
                first_name=COALESCE(?, first_name),
                last_name=COALESCE(?, last_name)
            WHERE chat_id=?
            """,
            (username, first_name, last_name, chat_id),
        )
        await db.commit()
    finally:
        await db.close()


async def get_user(chat_id: int) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT chat_id, username, first_name, last_name, is_allowed, is_admin FROM users WHERE chat_id=?",
            (chat_id,),
        )
        return dict(rows[0]) if rows else None
    finally:
        await db.close()


async def get_user_seen_version(chat_id: int) -> str | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT last_seen_version FROM users WHERE chat_id=?",
            (chat_id,),
        )
        if not rows:
            return None
        return rows[0]["last_seen_version"]
    finally:
        await db.close()


async def set_user_seen_version(chat_id: int, version: str) -> None:
    db = await get_db()
    try:
        await db.execute(
            "UPDATE users SET last_seen_version=? WHERE chat_id=?",
            (version, chat_id),
        )
        await db.commit()
    finally:
        await db.close()


async def list_users() -> list[dict]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT chat_id, username, first_name, last_name, is_allowed, is_admin FROM users ORDER BY created_at"
        )
        return [dict(row) for row in rows]
    finally:
        await db.close()


async def list_users_by_access(is_allowed: int) -> list[dict]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT chat_id, username, first_name, last_name, is_admin
            FROM users
            WHERE is_allowed=?
            """,
            (is_allowed,),
        )
        return [dict(row) for row in rows]
    finally:
        await db.close()


async def set_user_allowed(chat_id: int, allowed: bool) -> None:
    db = await get_db()
    try:
        await db.execute(
            "UPDATE users SET is_allowed=? WHERE chat_id=?",
            (1 if allowed else 0, chat_id),
        )
        await db.commit()
    finally:
        await db.close()


async def set_user_admin(chat_id: int, is_admin: bool) -> None:
    db = await get_db()
    try:
        await db.execute(
            "UPDATE users SET is_admin=? WHERE chat_id=?",
            (1 if is_admin else 0, chat_id),
        )
        await db.commit()
    finally:
        await db.close()


__all__ = [
    "ensure_admin_user",
    "ensure_user",
    "get_user",
    "get_user_seen_version",
    "is_chat_admin",
    "is_chat_allowed",
    "list_users",
    "list_users_by_access",
    "set_user_seen_version",
    "set_user_admin",
    "set_user_allowed",
]
