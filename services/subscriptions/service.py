from __future__ import annotations

from bot.repositories.subscriptions import create_subscription
from bot.repositories.subscriptions import deactivate_all_subscriptions
from bot.repositories.subscriptions import delete_subscription
from bot.repositories.subscriptions import get_subscription
from bot.repositories.subscriptions import list_subscription_briefs
from bot.repositories.subscriptions import list_subscriptions
from bot.repositories.subscriptions import pause_active_subscriptions_for_night
from bot.repositories.subscriptions import resume_night_paused_subscriptions
from bot.repositories.subscriptions import toggle_subscription_active
from bot.repositories.subscriptions import toggle_subscription_flag
from bot.repositories.subscriptions import update_subscription_field


__all__ = [
    "create_subscription",
    "deactivate_all_subscriptions",
    "delete_subscription",
    "get_subscription",
    "list_subscription_briefs",
    "list_subscriptions",
    "pause_active_subscriptions_for_night",
    "resume_night_paused_subscriptions",
    "toggle_subscription_active",
    "toggle_subscription_flag",
    "update_subscription_field",
]
