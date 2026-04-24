from __future__ import annotations

from bot.repositories.credentials import (
    get_credential_by_id as _repo_get_credential_by_id,
    delete_credential as _repo_delete_credential,
    list_credentials_for_delete as _repo_list_credentials_for_delete,
    list_credentials_rows as _repo_list_credentials_rows,
    upsert_credential as _repo_upsert_credential,
)
from bot.repositories.overdue import (
    copy_credential_creditor_profile as _repo_copy_credential_creditor_profile,
    copy_credential_signature_asset as _repo_copy_credential_signature_asset,
    get_credential_creditor_profile as _repo_get_credential_creditor_profile,
    get_credential_signature_asset as _repo_get_credential_signature_asset,
    save_credential_signature_asset as _repo_save_credential_signature_asset,
    upsert_credential_creditor_profile as _repo_upsert_credential_creditor_profile,
)
from bot.services.credentials.archive_loader import load_investments_archive


async def get_credentials_rows(chat_id: int):
    return await _repo_list_credentials_rows(chat_id)


async def get_credential(chat_id: int, credential_id: int):
    return await _repo_get_credential_by_id(credential_id, chat_id)


async def save_credential(chat_id: int, service: str, login: str, password: str) -> int:
    return await _repo_upsert_credential(chat_id, service, login, password)


async def autoload_investments_archive(credential_id: int, service: str, login: str, password: str) -> int:
    return await load_investments_archive(credential_id, service, login, password)


async def get_credentials_for_delete(chat_id: int):
    return await _repo_list_credentials_for_delete(chat_id)


async def get_creditor_profile(chat_id: int, credential_id: int):
    return await _repo_get_credential_creditor_profile(chat_id, credential_id)


async def save_creditor_profile(
    chat_id: int,
    credential_id: int,
    *,
    full_name: str | None,
    address: str | None,
    phone: str | None,
    email: str | None,
) -> None:
    await _repo_upsert_credential_creditor_profile(
        chat_id,
        credential_id,
        full_name=full_name,
        address=address,
        phone=phone,
        email=email,
    )


async def copy_creditor_profile(chat_id: int, source_credential_id: int, target_credential_id: int) -> bool:
    return await _repo_copy_credential_creditor_profile(chat_id, source_credential_id, target_credential_id)


async def get_signature_asset(chat_id: int, credential_id: int):
    return await _repo_get_credential_signature_asset(chat_id, credential_id)


async def save_signature_asset(
    chat_id: int,
    credential_id: int,
    *,
    file_path: str,
    mime_type: str | None = None,
    telegram_file_id: str | None = None,
    telegram_unique_id: str | None = None,
) -> None:
    await _repo_save_credential_signature_asset(
        chat_id,
        credential_id,
        file_path=file_path,
        mime_type=mime_type,
        telegram_file_id=telegram_file_id,
        telegram_unique_id=telegram_unique_id,
    )


async def copy_signature_asset(chat_id: int, source_credential_id: int, target_credential_id: int) -> bool:
    return await _repo_copy_credential_signature_asset(chat_id, source_credential_id, target_credential_id)


async def remove_credential(credential_id: int, chat_id: int):
    return await _repo_delete_credential(credential_id, chat_id)


async def list_credentials_rows(chat_id: int):
    return await get_credentials_rows(chat_id)


async def get_credential_by_id(chat_id: int, credential_id: int):
    return await get_credential(chat_id, credential_id)


async def upsert_credential(chat_id: int, service: str, login: str, password: str) -> int:
    return await save_credential(chat_id, service, login, password)


async def list_credentials_for_delete(chat_id: int):
    return await get_credentials_for_delete(chat_id)


async def get_credential_creditor_profile(chat_id: int, credential_id: int):
    return await get_creditor_profile(chat_id, credential_id)


async def upsert_credential_creditor_profile(
    chat_id: int,
    credential_id: int,
    *,
    full_name: str | None,
    address: str | None,
    phone: str | None,
    email: str | None,
) -> None:
    await save_creditor_profile(
        chat_id,
        credential_id,
        full_name=full_name,
        address=address,
        phone=phone,
        email=email,
    )


async def clone_credential_creditor_profile(chat_id: int, source_credential_id: int, target_credential_id: int) -> bool:
    return await copy_creditor_profile(chat_id, source_credential_id, target_credential_id)


async def get_credential_signature_asset(chat_id: int, credential_id: int):
    return await get_signature_asset(chat_id, credential_id)


async def save_credential_signature_asset(
    chat_id: int,
    credential_id: int,
    *,
    file_path: str,
    mime_type: str | None = None,
    telegram_file_id: str | None = None,
    telegram_unique_id: str | None = None,
) -> None:
    await save_signature_asset(
        chat_id,
        credential_id,
        file_path=file_path,
        mime_type=mime_type,
        telegram_file_id=telegram_file_id,
        telegram_unique_id=telegram_unique_id,
    )


async def clone_credential_signature_asset(chat_id: int, source_credential_id: int, target_credential_id: int) -> bool:
    return await copy_signature_asset(chat_id, source_credential_id, target_credential_id)


async def delete_credential(credential_id: int, chat_id: int):
    return await remove_credential(credential_id, chat_id)


async def load_investments_archive(credential_id: int, service: str, login: str, password: str) -> int:
    return await autoload_investments_archive(credential_id, service, login, password)


__all__ = [
    "autoload_investments_archive",
    "clone_credential_creditor_profile",
    "clone_credential_signature_asset",
    "copy_creditor_profile",
    "copy_signature_asset",
    "delete_credential",
    "get_creditor_profile",
    "get_credential",
    "get_credential_by_id",
    "get_credential_creditor_profile",
    "get_credential_signature_asset",
    "get_credentials_for_delete",
    "get_credentials_rows",
    "get_signature_asset",
    "list_credentials_for_delete",
    "list_credentials_rows",
    "load_investments_archive",
    "remove_credential",
    "save_credential",
    "save_creditor_profile",
    "save_signature_asset",
    "save_credential_signature_asset",
    "upsert_credential_creditor_profile",
    "upsert_credential",
]
