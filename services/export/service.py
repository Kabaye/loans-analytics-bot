from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from bot.domain.borrowers import BorrowEntry
from bot.domain.borrower_views import serialize_export_entry
from bot.integrations.opi_client import OPIChecker
from bot.services.base.providers import get_export_parsers as get_live_export_parsers
from bot.services.borrowers.enrichment import enrich_entries_from_borrowers

log = logging.getLogger(__name__)


def _extend_unique_entries(target: list[BorrowEntry], entries: list[BorrowEntry]) -> None:
    seen = {(entry.service, entry.request_type, entry.id) for entry in target}
    for entry in entries:
        key = (entry.service, entry.request_type, entry.id)
        if key in seen:
            continue
        target.append(entry)
        seen.add(key)


async def _get_export_parsers(service: str, chat_id: int):
    if service == "mongo":
        from bot.integrations.parsers.mongo import MongoParser

        parser = MongoParser()
        await parser.login()
        return [parser], [parser]

    parsers = await get_live_export_parsers(service, chat_id)
    return parsers, []


async def collect_export_entries(services: list[str], chat_id: int) -> list[dict]:
    all_entries: list[BorrowEntry] = []

    for service in services:
        parsers, owned = await _get_export_parsers(service, chat_id)
        if not parsers:
            log.info("Export: no parsers for %s (chat_id=%s)", service, chat_id)
            continue

        try:
            for parser in parsers:
                try:
                    borrows = await parser.fetch_borrows()
                    if borrows:
                        if service == "finkit" and hasattr(parser, "enrich_with_pdf"):
                            await parser.enrich_with_pdf(borrows)
                        _extend_unique_entries(all_entries, borrows)
                except Exception as exc:
                    log.warning("Export: failed to fetch borrows for %s: %s", service, exc)

                try:
                    lends = await parser.fetch_lends()
                    if lends:
                        _extend_unique_entries(all_entries, lends)
                except Exception as exc:
                    log.warning("Export: failed to fetch lends for %s: %s", service, exc)
        finally:
            for parser in owned:
                try:
                    await parser.close()
                except Exception:
                    pass

    try:
        await enrich_entries_from_borrowers(all_entries)
    except Exception as exc:
        log.warning("Export: borrowers enrichment batch error: %s", exc)

    entries_with_id = [entry for entry in all_entries if entry.document_id]
    if entries_with_id:
        opi = OPIChecker()
        try:
            for entry in entries_with_id:
                try:
                    result = await asyncio.wait_for(opi.check(entry.document_id), timeout=30)
                    entry.opi_checked = True
                    entry.opi_checked_at = datetime.now(timezone.utc)
                    if result.error:
                        entry.opi_error = result.error
                        entry.opi_has_debt = None
                    else:
                        entry.opi_error = None
                        entry.opi_has_debt = result.has_debt
                        entry.opi_debt_amount = result.debt_amount
                        entry.opi_full_name = result.full_name
                except asyncio.TimeoutError:
                    entry.opi_checked = True
                    entry.opi_checked_at = datetime.now(timezone.utc)
                    entry.opi_error = "таймаут (30с)"
                except Exception as exc:
                    entry.opi_checked = True
                    entry.opi_checked_at = datetime.now(timezone.utc)
                    entry.opi_error = str(exc)
        finally:
            await opi.close()

    return [serialize_export_entry(entry) for entry in all_entries]


__all__ = ["collect_export_entries"]
