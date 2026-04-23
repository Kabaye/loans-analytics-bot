from __future__ import annotations

import asyncio
import logging

from bot.integrations.opi_client import OPIChecker
from bot.integrations.telegram_admin import send_admin_html_message
from bot.repositories.opi_cache import get_stale_opi_documents
from bot.services.credentials.investment_refresh import refresh_investments
from bot.services.overdue.sync import refresh_overdue_snapshot

log = logging.getLogger(__name__)


async def refresh_overdue_cases(bot) -> None:
    del bot
    log.info("🌙 Overdue sync: refreshing overdue cases from archive endpoints...")
    finkit_synced, zaimis_synced, errors = await refresh_overdue_snapshot()
    log.info("🌙 Overdue sync done: finkit=%d, zaimis=%d, errors=%d", finkit_synced, zaimis_synced, len(errors))
    for error in errors[:10]:
        log.warning("Overdue sync error: %s", error)


async def midnight_refresh_investments(bot) -> None:
    await refresh_investments(bot)


async def midnight_refresh_opi(bot) -> None:
    log.info("🌙 Midnight job: refreshing OPI data...")

    stale = await get_stale_opi_documents(max_age_days=3)
    if not stale:
        log.info("🌙 OPI refresh: nothing to check")
        return

    checker = OPIChecker()
    checked = 0
    errors = 0

    try:
        for row in stale:
            doc_id = row["document_id"]
            try:
                result = await checker.check(doc_id, use_cache=False)
                checked += 1
                if result.error:
                    errors += 1
                await asyncio.sleep(2)
            except Exception as exc:
                log.warning("OPI check error for %s: %s", doc_id, exc)
                errors += 1
    finally:
        await checker.close()

    log.info("🌙 OPI refresh done: checked=%d, errors=%d", checked, errors)
    try:
        await send_admin_html_message(
            bot,
            (
                f"🌙 <b>Ночная проверка ОПИ</b>\n"
                f"  Проверено: {checked}/{len(stale)}\n"
                f"  Ошибок: {errors}"
            )
        )
    except Exception:
        pass
