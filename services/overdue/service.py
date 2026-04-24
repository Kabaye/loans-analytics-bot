from __future__ import annotations

from bot.repositories.credentials import (
    get_credential_by_id as _repo_get_credential_by_id,
    list_user_credentials as _repo_list_user_credentials,
)
from bot.repositories.overdue import (
    copy_credential_creditor_profile as _repo_copy_credential_creditor_profile,
    copy_credential_signature_asset as _repo_copy_credential_signature_asset,
    get_credential_creditor_profile as _repo_get_credential_creditor_profile,
    get_credential_signature_asset as _repo_get_credential_signature_asset,
    get_overdue_case as _repo_get_overdue_case,
    get_user_signature_asset as _repo_get_user_signature_asset,
    list_overdue_cases as _repo_list_overdue_cases,
    save_credential_signature_asset as _repo_save_credential_signature_asset,
    save_generated_document as _repo_save_generated_document,
    save_user_signature_asset as _repo_save_user_signature_asset,
    update_overdue_case_contacts as _repo_update_overdue_case_contacts,
    upsert_credential_creditor_profile as _repo_upsert_credential_creditor_profile,
    upsert_overdue_case as _repo_upsert_overdue_case,
)


async def get_user_overdue_cases(chat_id: int):
    return await _repo_list_overdue_cases(chat_id)


async def get_user_overdue_case(case_id: int, chat_id: int):
    return await _repo_get_overdue_case(case_id, chat_id)


async def get_case_creditor_profile(chat_id: int, credential_id: int):
    return await _repo_get_credential_creditor_profile(chat_id, credential_id)


async def get_user_credential(credential_id: int, chat_id: int):
    return await _repo_get_credential_by_id(credential_id, chat_id)


async def get_user_service_credentials(chat_id: int, *, services: tuple[str, ...]):
    return await _repo_list_user_credentials(chat_id, services=services)


async def save_case_contacts(case_id: int, chat_id: int, **kwargs):
    await _repo_update_overdue_case_contacts(case_id, chat_id, **kwargs)


async def save_creditor_profile(chat_id: int, credential_id: int, **kwargs):
    await _repo_upsert_credential_creditor_profile(chat_id, credential_id, **kwargs)


async def copy_creditor_profile(chat_id: int, source_credential_id: int, target_credential_id: int) -> bool:
    return await _repo_copy_credential_creditor_profile(chat_id, source_credential_id, target_credential_id)


async def save_case_data(chat_id: int, credential_id: int, service: str, external_id: str, **kwargs):
    await _repo_upsert_overdue_case(
        chat_id=chat_id,
        credential_id=credential_id,
        service=service,
        external_id=external_id,
        **kwargs,
    )


async def get_signature_asset(chat_id: int):
    return await _repo_get_user_signature_asset(chat_id)


async def get_case_signature_asset(chat_id: int, credential_id: int):
    return await _repo_get_credential_signature_asset(chat_id, credential_id)


async def store_signature_asset(chat_id: int, **kwargs):
    await _repo_save_user_signature_asset(chat_id, **kwargs)


async def store_case_signature_asset(chat_id: int, credential_id: int, **kwargs):
    await _repo_save_credential_signature_asset(chat_id, credential_id, **kwargs)


async def copy_case_signature_asset(chat_id: int, source_credential_id: int, target_credential_id: int) -> bool:
    return await _repo_copy_credential_signature_asset(chat_id, source_credential_id, target_credential_id)


async def store_generated_document(case_id: int, chat_id: int, **kwargs):
    await _repo_save_generated_document(case_id, chat_id, **kwargs)


async def list_overdue_cases(chat_id: int):
    return await get_user_overdue_cases(chat_id)


async def get_overdue_case(case_id: int, chat_id: int):
    return await get_user_overdue_case(case_id, chat_id)


async def get_credential_creditor_profile(chat_id: int, credential_id: int):
    return await get_case_creditor_profile(chat_id, credential_id)


async def get_credential_by_id(credential_id: int, chat_id: int):
    return await get_user_credential(credential_id, chat_id)


async def list_user_credentials(chat_id: int, *, services: tuple[str, ...]):
    return await get_user_service_credentials(chat_id, services=services)


async def update_overdue_case_contacts(case_id: int, chat_id: int, **kwargs):
    await save_case_contacts(case_id, chat_id, **kwargs)


async def upsert_credential_creditor_profile(chat_id: int, credential_id: int, **kwargs):
    await save_creditor_profile(chat_id, credential_id, **kwargs)


async def clone_credential_creditor_profile(chat_id: int, source_credential_id: int, target_credential_id: int) -> bool:
    return await copy_creditor_profile(chat_id, source_credential_id, target_credential_id)


async def upsert_overdue_case(chat_id: int, credential_id: int, service: str, external_id: str, **kwargs):
    await save_case_data(chat_id, credential_id, service, external_id, **kwargs)


async def get_user_signature_asset(chat_id: int):
    return await get_signature_asset(chat_id)


async def get_credential_signature_asset(chat_id: int, credential_id: int):
    return await get_case_signature_asset(chat_id, credential_id)


async def save_user_signature_asset(chat_id: int, **kwargs):
    await store_signature_asset(chat_id, **kwargs)


async def save_credential_signature_asset(chat_id: int, credential_id: int, **kwargs):
    await store_case_signature_asset(chat_id, credential_id, **kwargs)


async def copy_credential_signature_asset(chat_id: int, source_credential_id: int, target_credential_id: int) -> bool:
    return await copy_case_signature_asset(chat_id, source_credential_id, target_credential_id)


async def save_generated_document(case_id: int, chat_id: int, **kwargs):
    await store_generated_document(case_id, chat_id, **kwargs)


__all__ = [
    "get_case_creditor_profile",
    "get_credential_by_id",
    "get_credential_creditor_profile",
    "get_credential_signature_asset",
    "get_signature_asset",
    "get_overdue_case",
    "get_user_credential",
    "get_user_overdue_case",
    "get_user_overdue_cases",
    "get_user_service_credentials",
    "get_user_signature_asset",
    "clone_credential_creditor_profile",
    "list_overdue_cases",
    "list_user_credentials",
    "copy_case_signature_asset",
    "copy_creditor_profile",
    "copy_credential_signature_asset",
    "save_generated_document",
    "save_case_contacts",
    "save_case_data",
    "save_credential_signature_asset",
    "save_creditor_profile",
    "save_user_signature_asset",
    "store_generated_document",
    "store_case_signature_asset",
    "store_signature_asset",
    "update_overdue_case_contacts",
    "upsert_credential_creditor_profile",
    "upsert_overdue_case",
]
