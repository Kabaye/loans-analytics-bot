from __future__ import annotations

import logging
import traceback
from datetime import datetime, timezone
from typing import Optional

from bot import config
from bot.domain.borrowers import BorrowEntry
from bot.integrations.opi_client import OPIChecker
from bot.integrations.telegram_admin import send_admin_html_message
from bot.repositories.settings import get_site_settings
from bot.services.notifications.sender import has_active_subscriptions

log = logging.getLogger(__name__)

_opi_checker: Optional[OPIChecker] = None
_kapusta_backoff_until: Optional[datetime] = None
_last_poll: dict[str, datetime] = {}
_error_notified: dict[str, bool] = {"kapusta": False, "finkit": False, "zaimis": False}


def apply_opi_result(entry: BorrowEntry, result) -> None:
    entry.opi_checked = True
    entry.opi_checked_at = datetime.now(timezone.utc)
    entry.opi_error = result.error
    entry.opi_has_debt = result.has_debt
    entry.opi_debt_amount = result.debt_amount
    entry.opi_full_name = result.full_name


async def get_opi_checker() -> OPIChecker:
    global _opi_checker
    if _opi_checker is None:
        _opi_checker = OPIChecker()
    return _opi_checker


async def notify_error(bot, service: str, error: Exception) -> None:
    if _error_notified.get(service):
        return

    _error_notified[service] = True
    tb = traceback.format_exception(type(error), error, error.__traceback__)
    tb_str = "".join(tb)[-1500:]

    svc_names = {"kapusta": "🥬 Kapusta", "finkit": "🔵 FinKit", "zaimis": "🟪 ЗАЙМись"}
    text = (
        f"⚠️ <b>Ошибка парсера {svc_names.get(service, service)}</b>\n\n"
        f"<b>Тип:</b> {type(error).__name__}\n"
        f"<b>Сообщение:</b> {str(error)[:500]}\n\n"
        f"<pre>{tb_str}</pre>"
    )

    try:
        await send_admin_html_message(bot, text)
        log.info("Error notification sent to admin for %s", service)
    except Exception as exc:
        log.warning("Failed to send error notification: %s", exc)


def clear_error(service: str) -> None:
    _error_notified[service] = False


async def should_poll(service: str) -> bool:
    if not await has_active_subscriptions(service):
        return False

    settings = await get_site_settings(service)
    if not settings.get("polling_enabled", 1):
        return False

    now_utc = datetime.now(timezone.utc)
    interval = settings.get("poll_interval", 60)
    last = _last_poll.get(service)
    if last:
        elapsed = (now_utc - last).total_seconds()
        if elapsed < interval - 5:
            return False

    _last_poll[service] = now_utc
    return True


def get_kapusta_backoff_until() -> datetime | None:
    return _kapusta_backoff_until


def set_kapusta_backoff_until(value: datetime | None) -> None:
    global _kapusta_backoff_until
    _kapusta_backoff_until = value
