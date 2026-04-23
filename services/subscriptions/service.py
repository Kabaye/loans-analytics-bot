from __future__ import annotations

from bot.repositories.subscriptions import (
    create_subscription as _repo_create_subscription,
    deactivate_all_subscriptions as _repo_deactivate_all_subscriptions,
    delete_subscription as _repo_delete_subscription,
    get_subscription as _repo_get_subscription,
    list_subscription_briefs as _repo_list_subscription_briefs,
    list_subscriptions as _repo_list_subscriptions,
    pause_active_subscriptions_for_night as _repo_pause_active_subscriptions_for_night,
    resume_night_paused_subscriptions as _repo_resume_night_paused_subscriptions,
    toggle_subscription_active as _repo_toggle_subscription_active,
    toggle_subscription_flag as _repo_toggle_subscription_flag,
    update_subscription_field as _repo_update_subscription_field,
)


async def get_subscriptions(chat_id: int):
    return await _repo_list_subscriptions(chat_id)


async def create_user_subscription(chat_id: int, data: dict):
    return await _repo_create_subscription(chat_id, data)


async def get_subscription_briefs(chat_id: int):
    return await _repo_list_subscription_briefs(chat_id)


async def get_user_subscription(subscription_id: int, chat_id: int):
    return await _repo_get_subscription(subscription_id, chat_id)


async def set_night_pause(chat_id: int) -> None:
    await _repo_pause_active_subscriptions_for_night(chat_id)


async def resume_night_pause(chat_id: int) -> None:
    await _repo_resume_night_paused_subscriptions(chat_id)


async def set_subscription_flag(subscription_id: int, field: str):
    return await _repo_toggle_subscription_flag(subscription_id, field)


async def set_subscription_field(subscription_id: int, chat_id: int, field: str, value):
    await _repo_update_subscription_field(subscription_id, chat_id, field, value)


async def remove_subscription(subscription_id: int, chat_id: int) -> None:
    await _repo_delete_subscription(subscription_id, chat_id)


async def toggle_user_subscription(subscription_id: int, chat_id: int) -> None:
    await _repo_toggle_subscription_active(subscription_id, chat_id)


async def stop_all_notifications(chat_id: int) -> None:
    await _repo_deactivate_all_subscriptions(chat_id)


async def list_subscriptions(chat_id: int):
    return await get_subscriptions(chat_id)


async def create_subscription(chat_id: int, data: dict):
    return await create_user_subscription(chat_id, data)


async def list_subscription_briefs(chat_id: int):
    return await get_subscription_briefs(chat_id)


async def get_subscription(subscription_id: int, chat_id: int):
    return await get_user_subscription(subscription_id, chat_id)


async def pause_active_subscriptions_for_night(chat_id: int) -> None:
    await set_night_pause(chat_id)


async def resume_night_paused_subscriptions(chat_id: int) -> None:
    await resume_night_pause(chat_id)


async def toggle_subscription_flag(subscription_id: int, field: str):
    return await set_subscription_flag(subscription_id, field)


async def update_subscription_field(subscription_id: int, chat_id: int, field: str, value):
    await set_subscription_field(subscription_id, chat_id, field, value)


async def delete_subscription(subscription_id: int, chat_id: int) -> None:
    await remove_subscription(subscription_id, chat_id)


async def toggle_subscription_active(subscription_id: int, chat_id: int) -> None:
    await toggle_user_subscription(subscription_id, chat_id)


async def deactivate_all_subscriptions(chat_id: int) -> None:
    await stop_all_notifications(chat_id)


__all__ = [
    "create_user_subscription",
    "create_subscription",
    "deactivate_all_subscriptions",
    "delete_subscription",
    "get_subscription",
    "get_subscription_briefs",
    "get_subscriptions",
    "get_user_subscription",
    "list_subscription_briefs",
    "list_subscriptions",
    "pause_active_subscriptions_for_night",
    "remove_subscription",
    "resume_night_paused_subscriptions",
    "resume_night_pause",
    "set_night_pause",
    "set_subscription_field",
    "set_subscription_flag",
    "stop_all_notifications",
    "toggle_subscription_active",
    "toggle_subscription_flag",
    "toggle_user_subscription",
    "update_subscription_field",
]
