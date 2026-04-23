from __future__ import annotations

from bot.repositories.credentials import list_credential_services
from bot.repositories.settings import get_all_site_settings
from bot.repositories.subscriptions import count_active_subscriptions_by_service
from bot.repositories.users import ensure_user


async def ensure_chat_user(
    chat_id: int,
    *,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
) -> None:
    await ensure_user(
        chat_id,
        username=username,
        first_name=first_name,
        last_name=last_name,
    )


async def get_status_snapshot(chat_id: int) -> dict[str, list]:
    return {
        "subscriptions": await count_active_subscriptions_by_service(chat_id),
        "credential_services": await list_credential_services(chat_id),
        "site_settings": await get_all_site_settings(),
    }


__all__ = ["ensure_chat_user", "get_status_snapshot"]
