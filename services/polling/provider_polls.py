from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from bot import config
from bot.domain.borrowers import BorrowEntry
from bot.integrations.parsers.kapusta import KapustaBlockedError
from bot.integrations.telegram_notifications import notify_users, update_sent_notifications
from bot.services.base.cache import set_cached_entries
from bot.services.base.providers import (
    ensure_finkit_parser,
    ensure_kapusta_parser,
    ensure_zaimis_parser,
    list_service_credentials,
    pick_round_robin_credential,
    reset_kapusta_parser,
)
from bot.services.borrowers.enrichment import (
    enrich_entry_from_borrowers,
    enrich_from_borrower_cache,
    persist_borrower_entries,
)
from bot.services.notifications.fresh_tracker import compute_fresh
from bot.services.notifications.sender import get_active_subscriptions
from bot.services.polling.common import (
    apply_opi_result,
    clear_error,
    get_kapusta_backoff_until,
    get_opi_checker,
    notify_error,
    set_kapusta_backoff_until,
    should_poll,
)
from bot.services.settings.schema_monitor import notify_json_schema_change

log = logging.getLogger(__name__)


async def poll_kapusta(bot) -> None:
    if not await should_poll("kapusta"):
        return

    backoff_until = get_kapusta_backoff_until()
    if backoff_until and datetime.now(timezone.utc) < backoff_until:
        return

    try:
        parser = await ensure_kapusta_parser()
        if parser is None:
            return

        entries = await parser.fetch_borrows()
        set_cached_entries("kapusta", [entry.to_dict() for entry in entries])
        clear_error("kapusta")
        set_kapusta_backoff_until(None)
        if entries:
            await notify_json_schema_change("kapusta", entries)
            fresh = await compute_fresh(entries, "kapusta")
            if fresh:
                refs = await notify_users(bot, fresh, "kapusta")
                log.info(
                    "Sent %d notifications for kapusta (%d fresh / %d total)",
                    len(refs),
                    len(fresh),
                    len(entries),
                )
    except KapustaBlockedError as exc:
        backoff_sec = config.KAPUSTA_BACKOFF_SECONDS
        backoff_until = datetime.now(timezone.utc) + timedelta(seconds=backoff_sec)
        set_kapusta_backoff_until(backoff_until)
        log.warning(
            "Kapusta 403 — backing off for %d minutes until %s",
            backoff_sec // 60,
            backoff_until.isoformat(),
        )
        await reset_kapusta_parser()
        await notify_error(bot, "kapusta", exc)
    except Exception as exc:
        log.exception("Kapusta poll error: %s", exc)
        await reset_kapusta_parser()
        await notify_error(bot, "kapusta", exc)


async def poll_finkit(bot) -> None:
    if not await should_poll("finkit"):
        return
    try:
        creds_list = await list_service_credentials("finkit")
        cred = pick_round_robin_credential("finkit", creds_list)
        if cred is None:
            return

        parser = await ensure_finkit_parser(cred)
        if parser is None:
            log.warning("Finkit login failed for chat_id=%s login=%s", cred.chat_id, cred.login)
            return

        entries = await parser.fetch_borrows()
        if parser.needs_reauth:
            log.info(
                "Finkit session expired for chat_id=%s login=%s — re-logging in",
                cred.chat_id,
                cred.login,
            )
            parser = await ensure_finkit_parser(cred, force_login=True)
            if parser is None:
                log.warning("Finkit re-login failed for chat_id=%s login=%s", cred.chat_id, cred.login)
                return
            entries = await parser.fetch_borrows()

        set_cached_entries("finkit", [entry.to_dict() for entry in entries])

        if entries:
            await notify_json_schema_change("finkit", entries)
        all_entries: list[BorrowEntry] = list(entries)

        fresh_entries = await compute_fresh(all_entries, "finkit")
        sent_refs = []
        if fresh_entries:
            uncached_fresh = await enrich_from_borrower_cache(fresh_entries, require_document_id=True)
            sent_refs = await notify_users(bot, fresh_entries, "finkit")
            log.info(
                "Sent %d notifications for finkit (%d fresh, %d uncached)",
                len(sent_refs),
                len(fresh_entries),
                len(uncached_fresh),
            )

            if uncached_fresh and sent_refs:
                await parser.enrich_with_pdf(uncached_fresh)
                await persist_borrower_entries(uncached_fresh, source="finkit_borrow")

                uncached_fresh_ids = {entry.id for entry in uncached_fresh}
                pdf_enriched_refs = [ref for ref in sent_refs if ref[0] in uncached_fresh_ids]
                if pdf_enriched_refs:
                    edited = await update_sent_notifications(bot, fresh_entries, pdf_enriched_refs, "finkit")
                    log.info("Edit #1 (PDF): edited %d messages", edited)

            entries_needing_opi = [entry for entry in fresh_entries if entry.document_id and not entry.opi_checked]
            if entries_needing_opi and sent_refs:
                sent_ids = {entry_id for entry_id, _chat_id, _msg_id, _subs in sent_refs}
                entries_to_check = [entry for entry in entries_needing_opi if entry.id in sent_ids]
                if entries_to_check:
                    checker = await get_opi_checker()
                    log.info("OPI check for %d entries", len(entries_to_check))
                    for entry in entries_to_check:
                        result = await checker.check(entry.document_id)
                        apply_opi_result(entry, result)

                    opi_ids = {entry.id for entry in entries_to_check}
                    opi_refs = [ref for ref in sent_refs if ref[0] in opi_ids]
                    if opi_refs:
                        edited = await update_sent_notifications(bot, fresh_entries, opi_refs, "finkit")
                        log.info("Edit #2 (OPI): edited %d messages", edited)

        non_fresh = [entry for entry in all_entries if not entry.document_id]
        uncached_non_fresh = await enrich_from_borrower_cache(non_fresh, require_document_id=True)
        if uncached_non_fresh:
            await parser.enrich_with_pdf(uncached_non_fresh)

        await persist_borrower_entries(all_entries, source="finkit_borrow")
        clear_error("finkit")
    except Exception as exc:
        log.exception("Finkit poll error: %s", exc)
        await notify_error(bot, "finkit", exc)


async def poll_zaimis(bot) -> None:
    if not await should_poll("zaimis"):
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

        parser = await ensure_zaimis_parser(cred)
        if parser is None:
            log.warning("Zaimis login failed for chat_id=%s login=%s", cred.chat_id, cred.login)
            return

        all_entries = await parser.fetch_borrows(subscriptions=subs_list)
        if parser.needs_reauth:
            log.info("Zaimis token expired for chat_id=%s login=%s — re-logging in", cred.chat_id, cred.login)
            parser = await ensure_zaimis_parser(cred, force_login=True)
            if parser is None:
                log.warning("Zaimis re-login failed for chat_id=%s login=%s", cred.chat_id, cred.login)
                return
            all_entries = await parser.fetch_borrows(subscriptions=subs_list)

        set_cached_entries("zaimis", [entry.to_dict() for entry in all_entries])

        if all_entries:
            await notify_json_schema_change("zaimis", all_entries)

        await persist_borrower_entries(all_entries, source="zaimis_borrow")

        fresh = await compute_fresh(all_entries, "zaimis")
        sent_refs = []
        if fresh:
            sent_refs = await notify_users(bot, fresh, "zaimis", skip_enrichment=True)
            log.info(
                "Sent %d basic notifications for zaimis (%d fresh / %d total)",
                len(sent_refs),
                len(fresh),
                len(all_entries),
            )

            if sent_refs:
                for entry in fresh:
                    await enrich_entry_from_borrowers(entry)

                sent_ids = {entry_id for entry_id, _chat_id, _msg_id, _subs in sent_refs}
                entries_needing_opi = [
                    entry for entry in fresh if entry.id in sent_ids and entry.document_id and not entry.opi_checked
                ]
                if entries_needing_opi:
                    checker = await get_opi_checker()
                    for entry in entries_needing_opi:
                        result = await checker.check(entry.document_id)
                        apply_opi_result(entry, result)

                await update_sent_notifications(bot, fresh, sent_refs, "zaimis")

        clear_error("zaimis")
    except Exception as exc:
        log.exception("Zaimis poll error: %s", exc)
        await notify_error(bot, "zaimis", exc)
