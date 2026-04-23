from __future__ import annotations

from bot.repositories.credentials import (
    delete_credential as _repo_delete_credential,
    list_credentials_for_delete as _repo_list_credentials_for_delete,
    list_credentials_rows as _repo_list_credentials_rows,
    upsert_credential as _repo_upsert_credential,
)
from bot.services.credentials.archive_loader import load_investments_archive


async def get_credentials_rows(chat_id: int):
    return await _repo_list_credentials_rows(chat_id)


async def save_credential(chat_id: int, service: str, login: str, password: str) -> int:
    return await _repo_upsert_credential(chat_id, service, login, password)


async def autoload_investments_archive(credential_id: int, service: str, login: str, password: str) -> int:
    return await load_investments_archive(credential_id, service, login, password)


async def get_credentials_for_delete(chat_id: int):
    return await _repo_list_credentials_for_delete(chat_id)


async def remove_credential(credential_id: int, chat_id: int):
    return await _repo_delete_credential(credential_id, chat_id)


async def list_credentials_rows(chat_id: int):
    return await get_credentials_rows(chat_id)


async def upsert_credential(chat_id: int, service: str, login: str, password: str) -> int:
    return await save_credential(chat_id, service, login, password)


async def list_credentials_for_delete(chat_id: int):
    return await get_credentials_for_delete(chat_id)


async def delete_credential(credential_id: int, chat_id: int):
    return await remove_credential(credential_id, chat_id)


async def load_investments_archive(credential_id: int, service: str, login: str, password: str) -> int:
    return await autoload_investments_archive(credential_id, service, login, password)


__all__ = [
    "autoload_investments_archive",
    "delete_credential",
    "get_credentials_for_delete",
    "get_credentials_rows",
    "list_credentials_for_delete",
    "list_credentials_rows",
    "load_investments_archive",
    "remove_credential",
    "save_credential",
    "upsert_credential",
]
