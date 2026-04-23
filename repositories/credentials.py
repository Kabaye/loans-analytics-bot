from bot.domain.models import UserCredentials
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


async def list_allowed_user_credentials(service: str, chat_id: int | None = None) -> list[UserCredentials]:
    db = await get_db()
    try:
        query = """
            SELECT c.id, c.chat_id, c.login, c.password, u.username
            FROM credentials c
            JOIN users u ON c.chat_id = u.chat_id
            WHERE c.service = ? AND u.is_allowed = 1
        """
        params: list = [service]
        if chat_id is not None:
            query += " AND c.chat_id = ?"
            params.append(chat_id)
        query += " ORDER BY c.id"
        rows = await db.execute_fetchall(query, tuple(params))
        return [
            UserCredentials(
                id=row["id"],
                chat_id=row["chat_id"],
                service=service,
                login=row["login"],
                password=row["password"],
                username=row["username"],
            )
            for row in rows
        ]
    finally:
        await db.close()


async def list_credential_services(chat_id: int) -> list[str]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT service FROM credentials WHERE chat_id=? ORDER BY service, id",
            (chat_id,),
        )
        return [row["service"] for row in rows]
    finally:
        await db.close()


async def get_first_credential_owner_chat_id(service: str) -> int | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT chat_id FROM credentials WHERE service = ? ORDER BY id LIMIT 1",
            (service,),
        )
        return rows[0]["chat_id"] if rows else None
    finally:
        await db.close()

__all__ = [
    "delete_credential",
    "delete_credential_session",
    "get_first_credential_owner_chat_id",
    "get_credential_by_id",
    "get_saved_credential_session",
    "list_allowed_user_credentials",
    "list_credential_services",
    "list_credentials_for_delete",
    "list_credentials_rows",
    "list_user_credentials",
    "save_credential_session",
    "upsert_credential",
]
