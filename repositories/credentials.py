from bot.repositories.db import get_db
from bot.repositories.legacy_database import (
    delete_credential_session,
    get_credential_by_id,
    get_saved_credential_session,
    list_user_credentials,
    save_credential_session,
)


async def list_credentials_rows(chat_id: int) -> list[dict]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT id, service, login, label FROM credentials WHERE chat_id=? ORDER BY service, id",
            (chat_id,),
        )
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def upsert_credential(chat_id: int, service: str, login: str, password: str) -> int:
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO credentials (chat_id, service, login, password)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id, service, login)
            DO UPDATE SET password=excluded.password
            """,
            (chat_id, service, login, password),
        )
        rows = await db.execute_fetchall(
            "SELECT id FROM credentials WHERE chat_id = ? AND service = ? AND login = ?",
            (chat_id, service, login),
        )
        await db.commit()
        return rows[0]["id"]
    finally:
        await db.close()


async def list_credentials_for_delete(chat_id: int) -> list[dict]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT id, service, login FROM credentials WHERE chat_id=? ORDER BY service, id",
            (chat_id,),
        )
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def delete_credential(credential_id: int, chat_id: int) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT service, login FROM credentials WHERE id=? AND chat_id=?",
            (credential_id, chat_id),
        )
        if not rows:
            return None
        result = dict(rows[0])
        await db.execute(
            "DELETE FROM credentials WHERE id=? AND chat_id=?",
            (credential_id, chat_id),
        )
        await db.commit()
        return result
    finally:
        await db.close()

__all__ = [
    "delete_credential",
    "delete_credential_session",
    "get_credential_by_id",
    "get_saved_credential_session",
    "list_credentials_for_delete",
    "list_credentials_rows",
    "list_user_credentials",
    "save_credential_session",
    "upsert_credential",
]
