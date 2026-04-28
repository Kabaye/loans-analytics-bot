from __future__ import annotations

from bot.repositories.credentials import delete_credential
from bot.repositories.credentials import get_credential_by_id as _repo_get_credential_by_id
from bot.repositories.credentials import list_credentials_for_delete
from bot.repositories.credentials import list_credentials_rows
from bot.repositories.credentials import upsert_credential
from bot.repositories.overdue import copy_credential_creditor_profile as clone_credential_creditor_profile
from bot.repositories.overdue import copy_credential_signature_asset as clone_credential_signature_asset
from bot.repositories.overdue import get_credential_creditor_profile
from bot.repositories.overdue import get_credential_signature_asset
from bot.repositories.overdue import save_credential_signature_asset
from bot.repositories.overdue import upsert_credential_creditor_profile
from bot.services.credentials.archive_loader import load_investments_archive


async def get_credential_by_id(chat_id: int, credential_id: int):
    return await _repo_get_credential_by_id(credential_id, chat_id)


__all__ = [
    "clone_credential_creditor_profile",
    "clone_credential_signature_asset",
    "delete_credential",
    "get_credential_by_id",
    "get_credential_creditor_profile",
    "get_credential_signature_asset",
    "list_credentials_for_delete",
    "list_credentials_rows",
    "load_investments_archive",
    "save_credential_signature_asset",
    "upsert_credential",
    "upsert_credential_creditor_profile",
]
