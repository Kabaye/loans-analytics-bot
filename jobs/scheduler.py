"""Scheduler — periodic parsing of all sites with caching, backoff, and error alerts."""
from __future__ import annotations

import asyncio
import logging
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional

from aiogram import Bot
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from bot import config
from bot.domain.models import BorrowEntry, UserCredentials
from bot.integrations.opi_client import OPIChecker
from bot.integrations.parsers.kapusta import KapustaBlockedError
from bot.repositories.borrowers import upsert_borrower
from bot.repositories.opi_cache import get_stale_opi_documents
from bot.repositories.settings import get_site_settings
from bot.services.base.providers import (
    _ensure_finkit_parser,
    _ensure_zaimis_parser,
    ensure_kapusta_parser,
    get_export_parsers as _provider_get_export_parsers,
    get_parser as _provider_get_parser,
    list_service_credentials,
    pick_round_robin_credential,
    reset_kapusta_parser,
    shutdown_parsers as _provider_shutdown_parsers,
)
from bot.services.credentials.investment_refresh import refresh_investments
from bot.services.notifications.fresh_tracker import compute_fresh
from bot.services.notifications.sender import (
    enrich_entry_from_borrowers,
    get_active_subscriptions,
    has_active_subscriptions,
    notify_users,
    update_sent_notifications,
)
from bot.services.overdue.sync import refresh_overdue_snapshot
from bot.services.settings.schema_monitor import notify_json_schema_change

log = logging.getLogger(__name__)

cached_loans: dict[str, list[dict]] = {"kapusta": [], "finkit": [], "zaimis": []}
cached_at: dict[str, str | None] = {"kapusta": None, "finkit": None, "zaimis": None}

_opi_checker: Optional[OPIChecker] = None
_scheduler: Optional[AsyncIOScheduler] = None
_kapusta_backoff_until: Optional[datetime] = None
_last_poll: dict[str, datetime] = {}
_error_notified: dict[str, bool] = {"kapusta": False, "finkit": False, "zaimis": False}


async def _notify_error(bot: Bot, service: str, error: Exception) -> None:
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
        if config.ADMIN_CHAT_ID:
            await bot.send_message(config.ADMIN_CHAT_ID, text, parse_mode="HTML")
            log.info("Error notification sent to admin for %s", service)
    except Exception as exc:
        log.warning("Failed to send error notification: %s", exc)


def _clear_error(service: str) -> None:
    _error_notified[service] = False


async def _should_poll(service: str) -> bool:
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


async def poll_kapusta(bot: Bot) -> None:
    global _kapusta_backoff_until

    if not await _should_poll("kapusta"):
        return

    if _kapusta_backoff_until and datetime.now(timezone.utc) < _kapusta_backoff_until:
        return

    try:
        parser = await ensure_kapusta_parser()
        if parser is None:
            return

        entries = await parser.fetch_borrows()
        cached_loans["kapusta"] = [entry.to_dict() for entry in entries]
        cached_at["kapusta"] = datetime.now(timezone.utc).isoformat()
        _clear_error("kapusta")
        _kapusta_backoff_until = None
        if entries:
            await notify_json_schema_change("kapusta", entries)
            fresh = await compute_fresh(entries, "kapusta")
            if fresh:
                refs = await notify_users(bot, fresh, "kapusta")
                log.info("Sent %d notifications for kapusta (%d fresh / %d total)", len(refs), len(fresh), len(entries))
    except KapustaBlockedError as exc:
        backoff_sec = config.KAPUSTA_BACKOFF_SECONDS
        _kapusta_backoff_until = datetime.now(timezone.utc) + timedelta(seconds=backoff_sec)
        log.warning("Kapusta 403 — backing off for %d minutes until %s", backoff_sec // 60, _kapusta_backoff_until.isoformat())
        await reset_kapusta_parser()
        await _notify_error(bot, "kapusta", exc)
    except Exception as exc:
        log.exception("Kapusta poll error: %s", exc)
        await reset_kapusta_parser()
        await _notify_error(bot, "kapusta", exc)


async def poll_finkit(bot: Bot) -> None:
    global _opi_checker
    if not await _should_poll("finkit"):
        return
    try:
        creds_list = await list_service_credentials("finkit")
        cred = pick_round_robin_credential("finkit", creds_list)
        if cred is None:
            return

        parser = await _ensure_finkit_parser(cred)
        if parser is None:
            log.warning("Finkit login failed for chat_id=%s login=%s", cred.chat_id, cred.login)
            return

        entries = await parser.fetch_borrows()
        if parser.needs_reauth:
            log.info("Finkit session expired for chat_id=%s login=%s — re-logging in", cred.chat_id, cred.login)
            parser = await _ensure_finkit_parser(cred, force_login=True)
            if parser is None:
                log.warning("Finkit re-login failed for chat_id=%s login=%s", cred.chat_id, cred.login)
                return
            entries = await parser.fetch_borrows()

        cached_loans["finkit"] = [entry.to_dict() for entry in entries]
        cached_at["finkit"] = datetime.now(timezone.utc).isoformat()

        if entries:
            await notify_json_schema_change("finkit", entries)
        all_entries: list[BorrowEntry] = list(entries)
        parser_entries = [(parser, entries)] if entries else []

        fresh_entries = await compute_fresh(all_entries, "finkit")
        sent_refs = []
        if fresh_entries:
            uncached_fresh = await parser.__class__.enrich_from_cache(fresh_entries)
            sent_refs = await notify_users(bot, fresh_entries, "finkit")
            log.info("Sent %d notifications for finkit (%d fresh, %d uncached)", len(sent_refs), len(fresh_entries), len(uncached_fresh))

            if uncached_fresh and sent_refs:
                uncached_fresh_ids = {entry.id for entry in uncached_fresh}
                for current_parser, _entries in parser_entries:
                    await current_parser.enrich_with_pdf(uncached_fresh)
                    break

                for entry in uncached_fresh:
                    if entry.borrower_user_id:
                        await upsert_borrower(
                            service="finkit",
                            borrower_user_id=entry.borrower_user_id,
                            full_name=entry.full_name,
                            document_id=entry.document_id,
                            source="finkit_borrow",
                        )

                pdf_enriched_refs = [ref for ref in sent_refs if ref[0] in uncached_fresh_ids]
                if pdf_enriched_refs:
                    edited = await update_sent_notifications(bot, fresh_entries, pdf_enriched_refs, "finkit")
                    log.info("Edit #1 (PDF): edited %d messages", edited)

            entries_needing_opi = [entry for entry in fresh_entries if entry.document_id and not entry.opi_checked]
            if entries_needing_opi and sent_refs:
                sent_ids = {entry_id for entry_id, _chat_id, _msg_id, _subs in sent_refs}
                entries_to_check = [entry for entry in entries_needing_opi if entry.id in sent_ids]
                if entries_to_check:
                    if _opi_checker is None:
                        _opi_checker = OPIChecker()
                    log.info("OPI check for %d entries", len(entries_to_check))
                    for entry in entries_to_check:
                        result = await _opi_checker.check(entry.document_id)
                        entry.opi_checked = True
                        entry.opi_checked_at = datetime.now(timezone.utc)
                        entry.opi_error = result.error
                        entry.opi_has_debt = result.has_debt
                        entry.opi_debt_amount = result.debt_amount
                        entry.opi_full_name = result.full_name

                    opi_ids = {entry.id for entry in entries_to_check}
                    opi_refs = [ref for ref in sent_refs if ref[0] in opi_ids]
                    if opi_refs:
                        edited = await update_sent_notifications(bot, fresh_entries, opi_refs, "finkit")
                        log.info("Edit #2 (OPI): edited %d messages", edited)

        for current_parser, current_entries in parser_entries:
            non_fresh = [entry for entry in current_entries if not entry.document_id]
            if non_fresh:
                await current_parser.__class__.enrich_from_cache(non_fresh)
                await current_parser.enrich_with_pdf(non_fresh)

        for entry in all_entries:
            if entry.borrower_user_id and entry.full_name:
                await upsert_borrower(
                    service="finkit",
                    borrower_user_id=entry.borrower_user_id,
                    full_name=entry.full_name,
                    document_id=entry.document_id,
                    source="finkit_borrow",
                )

        _clear_error("finkit")
    except Exception as exc:
        log.exception("Finkit poll error: %s", exc)
        await _notify_error(bot, "finkit", exc)


async def poll_zaimis(bot: Bot) -> None:
    global _opi_checker
    if not await _should_poll("zaimis"):
        return
    try:
        creds_list = await list_service_credentials("zaimis")
        if not creds_list:
            return

        subs = await get_active_subscriptions("zaimis")
        subs_list = [sub for _, sub in subs] if subs else None

        cred = pick_round_robin_credential("zaimis", creds_list)
        if cred is None:
            return

        parser = await _ensure_zaimis_parser(cred)
        if parser is None:
            log.warning("Zaimis login failed for chat_id=%s login=%s", cred.chat_id, cred.login)
            return

        all_entries = await parser.fetch_borrows(subscriptions=subs_list)
        if parser.needs_reauth:
            log.info("Zaimis token expired for chat_id=%s login=%s — re-logging in", cred.chat_id, cred.login)
            parser = await _ensure_zaimis_parser(cred, force_login=True)
            if parser is None:
                log.warning("Zaimis re-login failed for chat_id=%s login=%s", cred.chat_id, cred.login)
                return
            all_entries = await parser.fetch_borrows(subscriptions=subs_list)

        cached_loans["zaimis"] = [entry.to_dict() for entry in all_entries]
        cached_at["zaimis"] = datetime.now(timezone.utc).isoformat()

        if all_entries:
            await notify_json_schema_change("zaimis", all_entries)
        for entry in all_entries:
            if entry.borrower_user_id:
                await upsert_borrower(
                    service="zaimis",
                    borrower_user_id=entry.borrower_user_id,
                    full_name=entry.display_name,
                )

        fresh = await compute_fresh(all_entries, "zaimis")
        sent_refs = []
        if fresh:
            sent_refs = await notify_users(bot, fresh, "zaimis", skip_enrichment=True)
            log.info("Sent %d basic notifications for zaimis (%d fresh / %d total)", len(sent_refs), len(fresh), len(all_entries))

            if sent_refs:
                for entry in fresh:
                    await enrich_entry_from_borrowers(entry)

                sent_ids = {entry_id for entry_id, _chat_id, _msg_id, _subs in sent_refs}
                entries_needing_opi = [entry for entry in fresh if entry.id in sent_ids and entry.document_id and not entry.opi_checked]
                if entries_needing_opi:
                    if _opi_checker is None:
                        _opi_checker = OPIChecker()
                    for entry in entries_needing_opi:
                        result = await _opi_checker.check(entry.document_id)
                        entry.opi_checked = True
                        entry.opi_checked_at = datetime.now(timezone.utc)
                        entry.opi_error = result.error
                        entry.opi_has_debt = result.has_debt
                        entry.opi_debt_amount = result.debt_amount
                        entry.opi_full_name = result.full_name

                await update_sent_notifications(bot, fresh, sent_refs, "zaimis")

        _clear_error("zaimis")
    except Exception as exc:
        log.exception("Zaimis poll error: %s", exc)
        await _notify_error(bot, "zaimis", exc)


async def refresh_overdue_cases(bot: Bot) -> None:
    del bot
    log.info("🌙 Overdue sync: refreshing overdue cases from archive endpoints...")
    finkit_synced, zaimis_synced, errors = await refresh_overdue_snapshot()
    log.info("🌙 Overdue sync done: finkit=%d, zaimis=%d, errors=%d", finkit_synced, zaimis_synced, len(errors))
    for error in errors[:10]:
        log.warning("Overdue sync error: %s", error)


async def midnight_refresh_investments(bot: Bot) -> None:
    await refresh_investments(bot)


async def midnight_refresh_opi(bot: Bot) -> None:
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
        if config.ADMIN_CHAT_ID:
            await bot.send_message(
                config.ADMIN_CHAT_ID,
                (
                    f"🌙 <b>Ночная проверка ОПИ</b>\n"
                    f"  Проверено: {checked}/{len(stale)}\n"
                    f"  Ошибок: {errors}"
                ),
                parse_mode="HTML",
            )
    except Exception:
        pass


def setup_scheduler(bot: Bot) -> AsyncIOScheduler:
    global _scheduler
    _scheduler = AsyncIOScheduler()

    base_interval = 30
    _scheduler.add_job(poll_kapusta, "interval", seconds=base_interval, args=[bot], id="kapusta", name="Kapusta poll", misfire_grace_time=60)
    _scheduler.add_job(poll_finkit, "interval", seconds=base_interval, args=[bot], id="finkit", name="Finkit poll", misfire_grace_time=60)
    _scheduler.add_job(poll_zaimis, "interval", seconds=base_interval, args=[bot], id="zaimis", name="Zaimis poll", misfire_grace_time=60)
    _scheduler.add_job(
        midnight_refresh_investments,
        CronTrigger(hour=21, minute=0, timezone="UTC"),
        args=[bot],
        id="midnight_investments",
        name="Midnight investments refresh",
        misfire_grace_time=3600,
    )
    _scheduler.add_job(
        refresh_overdue_cases,
        CronTrigger(hour=21, minute=15, timezone="UTC"),
        args=[bot],
        id="midnight_overdue_cases",
        name="Midnight overdue refresh",
        misfire_grace_time=3600,
    )
    _scheduler.add_job(
        midnight_refresh_opi,
        CronTrigger(hour=21, minute=30, timezone="UTC"),
        args=[bot],
        id="midnight_opi",
        name="Midnight OPI refresh",
        misfire_grace_time=3600,
    )

    log.info("Scheduler configured (base=%ds, midnight cron at 00:00/00:15/00:30 Minsk)", base_interval)
    return _scheduler


async def shutdown_parsers():
    await _provider_shutdown_parsers()


async def get_export_parsers(service: str, chat_id: int) -> list:
    return await _provider_get_export_parsers(service, chat_id)


def get_parser(service: str, chat_id: int | None = None):
    return _provider_get_parser(service, chat_id)


__all__ = [
    "_ensure_finkit_parser",
    "_ensure_zaimis_parser",
    "cached_at",
    "cached_loans",
    "get_export_parsers",
    "get_parser",
    "midnight_refresh_investments",
    "midnight_refresh_opi",
    "poll_finkit",
    "poll_kapusta",
    "poll_zaimis",
    "refresh_overdue_cases",
    "setup_scheduler",
    "shutdown_parsers",
]
