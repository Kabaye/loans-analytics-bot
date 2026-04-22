"""Scheduler — periodic parsing of all sites with caching, backoff, and error alerts."""
from __future__ import annotations

import asyncio
import logging
import traceback
from datetime import datetime, timezone, timedelta
from typing import Optional

from aiogram import Bot
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from bot import config
import io
import re

import pdfplumber

from bot.database import (
    get_db, get_site_settings,
    upsert_borrower_from_investment, get_stale_opi_documents,
    save_opi_result, upsert_borrower,
)

_NAME_ID_RE = re.compile(
    r"Я,\s+([А-ЯЁA-Z]+(?:\s+[А-ЯЁA-Z]+){2}),\s+идентификационный\s+номер\s*([0-9A-Z]+)",
    re.IGNORECASE | re.UNICODE | re.MULTILINE | re.DOTALL,
)
from bot.models import BorrowEntry, UserCredentials
from bot.parsers.kapusta import KapustaParser, KapustaBlockedError
from bot.parsers.finkit import FinkitParser
from bot.parsers.zaimis import ZaimisParser
from bot.services.notifier import notify_users, update_sent_notifications, enrich_entry_from_borrowers, get_active_subscriptions, has_active_subscriptions
from bot.services.opi_checker import OPIChecker


def _aware(dt: datetime | None) -> datetime | None:
    """Ensure a datetime is timezone-aware (UTC). Returns None if input is None."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

log = logging.getLogger(__name__)

# Global parser instances (reused across runs)
_kapusta: Optional[KapustaParser] = None
_opi_checker: Optional[OPIChecker] = None

# Per-user parser instances for authenticated services (keyed by (chat_id, login))
_finkit_parsers: dict[tuple[int, str], FinkitParser] = {}
_zaimis_parsers: dict[tuple[int, str], ZaimisParser] = {}

# Exposed scheduler instance for dynamic reconfiguration
_scheduler: Optional[AsyncIOScheduler] = None

# === Kapusta backoff on 403 ===
_kapusta_backoff_until: Optional[datetime] = None

# === Investment refresh tracking (every 3 days) ===
_last_investment_refresh: Optional[datetime] = None
INVESTMENT_REFRESH_INTERVAL_DAYS = 3

# === Per-site last poll timestamps (for custom intervals + time-based filtering) ===
_last_poll: dict[str, datetime] = {}

# === Per-site seen entry IDs (in-memory cache, backed by DB) ===
_seen_ids: dict[str, set[str]] = {}
_seen_ids_loaded: dict[str, bool] = {}

# === Error tracking — notify admin once per site failure ===
_error_notified: dict[str, bool] = {
    "kapusta": False,
    "finkit": False,
    "zaimis": False,
}


def _get_cutoff(service: str) -> datetime | None:
    """Get cutoff time for filtering fresh entries.
    Returns the time of the last poll start, or None if first poll."""
    return _last_poll.get(service)


async def _compute_fresh(entries: list[BorrowEntry], service: str) -> list[BorrowEntry]:
    """Compute fresh entries using ID-based tracking backed by DB.
    First poll loads from DB; entries not in DB are fresh (unless first-ever run).
    Updates DB with current entry IDs and prunes entries older than 7 days."""
    current_ids = {e.id for e in entries}

    # Load from DB on first call for this service
    if not _seen_ids_loaded.get(service):
        db = await get_db()
        try:
            rows = await db.execute_fetchall(
                "SELECT entry_id FROM seen_entries WHERE service = ?", (service,)
            )
            _seen_ids[service] = {r["entry_id"] for r in rows}
        finally:
            await db.close()
        _seen_ids_loaded[service] = True

        if not _seen_ids[service]:
            # First-ever run: seed DB, no notifications
            await _save_seen_ids(service, current_ids)
            _seen_ids[service] = current_ids
            return []

    prev = _seen_ids[service]
    fresh = [e for e in entries if e.id not in prev]
    _seen_ids[service] = current_ids

    # Persist changes to DB
    new_ids = current_ids - prev
    gone_ids = prev - current_ids
    if new_ids or gone_ids:
        db = await get_db()
        try:
            if new_ids:
                await db.executemany(
                    "INSERT OR IGNORE INTO seen_entries (service, entry_id) VALUES (?, ?)",
                    [(service, eid) for eid in new_ids],
                )
            if gone_ids:
                await db.executemany(
                    "DELETE FROM seen_entries WHERE service = ? AND entry_id = ?",
                    [(service, eid) for eid in gone_ids],
                )
            # Prune entries older than 7 days
            await db.execute(
                "DELETE FROM seen_entries WHERE service = ? AND first_seen < datetime('now', '-7 days')",
                (service,),
            )
            await db.commit()
        finally:
            await db.close()

    return fresh


async def _save_seen_ids(service: str, ids: set[str]) -> None:
    """Bulk-insert seen IDs for initial seeding."""
    db = await get_db()
    try:
        await db.executemany(
            "INSERT OR IGNORE INTO seen_entries (service, entry_id) VALUES (?, ?)",
            [(service, eid) for eid in ids],
        )
        await db.commit()
    finally:
        await db.close()


def _is_fresh(entry: BorrowEntry, cutoff: datetime | None) -> bool:
    """Check if entry is new/modified since cutoff. On first poll (no cutoff) returns False."""
    if cutoff is None:
        return False
    created = _aware(entry.created_at)
    updated = _aware(entry.updated_at)
    if created and created >= cutoff:
        return True
    if updated and updated >= cutoff:
        return True
    return False


async def _notify_error(bot: Bot, service: str, error: Exception) -> None:
    """Notify admin about a parser error (once per failure streak)."""
    if _error_notified.get(service):
        return  # already notified

    _error_notified[service] = True
    tb = traceback.format_exception(type(error), error, error.__traceback__)
    tb_str = "".join(tb)[-1500:]  # last 1500 chars of traceback

    svc_names = {"kapusta": "🥬 Kapusta", "finkit": "🔵 FinKit", "zaimis": "🟪 ЗАЙМись"}
    svc = svc_names.get(service, service)

    text = (
        f"⚠️ <b>Ошибка парсера {svc}</b>\n\n"
        f"<b>Тип:</b> {type(error).__name__}\n"
        f"<b>Сообщение:</b> {str(error)[:500]}\n\n"
        f"<pre>{tb_str}</pre>"
    )

    try:
        if config.ADMIN_CHAT_ID:
            await bot.send_message(config.ADMIN_CHAT_ID, text, parse_mode="HTML")
            log.info("Error notification sent to admin for %s", service)
    except Exception as e:
        log.warning("Failed to send error notification: %s", e)


def _clear_error(service: str) -> None:
    """Clear error flag on successful poll."""
    _error_notified[service] = False


async def _should_poll(service: str) -> bool:
    """Check site_settings: enabled, custom interval, active subscriptions."""
    # Skip polling if no users have active subscriptions for this service
    if not await has_active_subscriptions(service):
        return False

    settings = await get_site_settings(service)

    if not settings.get("polling_enabled", 1):
        return False

    # Custom interval: skip if polled too recently
    now_utc = datetime.now(timezone.utc)
    interval = settings.get("poll_interval", 60)
    last = _last_poll.get(service)
    if last:
        elapsed = (now_utc - last).total_seconds()
        if elapsed < interval - 5:  # 5s grace
            return False

    _last_poll[service] = now_utc
    return True


async def _get_user_credentials(service: str) -> list[UserCredentials]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT c.id, c.chat_id, c.login, c.password
            FROM credentials c
            JOIN users u ON c.chat_id = u.chat_id
            WHERE c.service = ? AND u.is_allowed = 1
            """,
            (service,),
        )
        return [
            UserCredentials(id=r["id"], chat_id=r["chat_id"], service=service, login=r["login"], password=r["password"])
            for r in rows
        ]
    finally:
        await db.close()


async def poll_kapusta(bot: Bot) -> None:
    global _kapusta, _kapusta_backoff_until

    if not await _should_poll("kapusta"):
        return

    # Check backoff
    if _kapusta_backoff_until and datetime.now(timezone.utc) < _kapusta_backoff_until:
        return

    try:
        if _kapusta is None:
            _kapusta = KapustaParser()
            await _kapusta.login()

        entries = await _kapusta.fetch_borrows()
        _clear_error("kapusta")
        _kapusta_backoff_until = None
        if entries:
            fresh = await _compute_fresh(entries, "kapusta")
            if fresh:
                refs = await notify_users(bot, fresh, "kapusta")
                log.info("Sent %d notifications for kapusta (%d fresh / %d total)", len(refs), len(fresh), len(entries))
    except KapustaBlockedError as e:
        backoff_sec = config.KAPUSTA_BACKOFF_SECONDS
        _kapusta_backoff_until = datetime.now(timezone.utc) + timedelta(seconds=backoff_sec)
        log.warning("Kapusta 403 — backing off for %d minutes until %s",
                     backoff_sec // 60, _kapusta_backoff_until.isoformat())
        _kapusta = None
        await _notify_error(bot, "kapusta", e)
    except Exception as e:
        log.exception("Kapusta poll error: %s", e)
        _kapusta = None
        await _notify_error(bot, "kapusta", e)


async def poll_finkit(bot: Bot) -> None:
    global _opi_checker
    if not await _should_poll("finkit"):
        return
    try:
        creds_list = await _get_user_credentials("finkit")
        if not creds_list:
            return

        # ── Stage 1: FETCH entries from all credentials ──
        all_entries: list[BorrowEntry] = []
        seen_entry_ids: set[str] = set()
        parser_entries: list[tuple] = []  # (parser, entries) for PDF enrichment later

        for cred in creds_list:
            pkey = (cred.chat_id, cred.login)
            parser = _finkit_parsers.get(pkey)
            if parser is None:
                parser = FinkitParser()
                ok = await parser.login(cred.login, cred.password)
                if not ok:
                    log.warning("Finkit login failed for chat_id=%s login=%s", cred.chat_id, cred.login)
                    continue
                _finkit_parsers[pkey] = parser

            entries = await parser.fetch_borrows()

            # Auto re-login on session expiry
            if parser.needs_reauth:
                log.info("Finkit session expired for chat_id=%s login=%s — re-logging in", cred.chat_id, cred.login)
                await parser.close()
                new_parser = FinkitParser()
                ok = await new_parser.login(cred.login, cred.password)
                if ok:
                    _finkit_parsers[pkey] = new_parser
                    parser = new_parser
                    entries = await new_parser.fetch_borrows()
                else:
                    log.warning("Finkit re-login failed for chat_id=%s login=%s", cred.chat_id, cred.login)
                    _finkit_parsers.pop(pkey, None)
                    continue

            if not entries:
                continue

            parser_entries.append((parser, entries))
            for entry in entries:
                if entry.id not in seen_entry_ids:
                    seen_entry_ids.add(entry.id)
                    all_entries.append(entry)

        # ── Stage 2: Compute fresh + DB cache enrichment (fast) + SEND ──
        fresh_entries = await _compute_fresh(all_entries, "finkit")
        sent_refs = []
        if fresh_entries:
            # Fast DB-only enrichment — entries with cached ФИО/ИН get them immediately
            uncached_fresh = await FinkitParser.enrich_from_cache(fresh_entries)
            sent_refs = await notify_users(bot, fresh_entries, "finkit")
            log.info("Sent %d notifications for finkit (%d fresh, %d uncached)",
                     len(sent_refs), len(fresh_entries), len(uncached_fresh))

            # ── Stage 3: PDF enrichment for uncached entries + EDIT #1 ──
            if uncached_fresh and sent_refs:
                uncached_fresh_ids = {e.id for e in uncached_fresh}
                # Find a parser to download PDFs (any logged-in parser works)
                for parser, _entries in parser_entries:
                    await parser.enrich_with_pdf(uncached_fresh)
                    break  # one parser is enough — same cookies access same PDFs

                # Save newly enriched borrowers
                for entry in uncached_fresh:
                    if entry.borrower_user_id:
                        await upsert_borrower(
                            service="finkit",
                            borrower_user_id=entry.borrower_user_id,
                            full_name=entry.full_name,
                            document_id=entry.document_id,
                        )

                # Edit messages for entries that got new data from PDF
                pdf_enriched_refs = [r for r in sent_refs if r[0] in uncached_fresh_ids]
                if pdf_enriched_refs:
                    edited = await update_sent_notifications(bot, fresh_entries, pdf_enriched_refs, "finkit")
                    log.info("Edit #1 (PDF): edited %d messages", edited)

            # ── Stage 4: OPI check + EDIT #2 ──
            entries_needing_opi = [e for e in fresh_entries if e.document_id and not e.opi_checked]
            if entries_needing_opi and sent_refs:
                sent_ids = {entry_id for entry_id, _chat_id, _msg_id, _subs in sent_refs}
                entries_to_check = [e for e in entries_needing_opi if e.id in sent_ids]
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

                    opi_ids = {e.id for e in entries_to_check}
                    opi_refs = [r for r in sent_refs if r[0] in opi_ids]
                    if opi_refs:
                        edited = await update_sent_notifications(bot, fresh_entries, opi_refs, "finkit")
                        log.info("Edit #2 (OPI): edited %d messages", edited)

        # Enrich remaining (non-fresh) entries for cache building
        for parser, entries in parser_entries:
            non_fresh = [e for e in entries if not e.document_id]
            if non_fresh:
                await FinkitParser.enrich_from_cache(non_fresh)
                await parser.enrich_with_pdf(non_fresh)

        # Save all borrowers (including non-fresh for cache)
        for entry in all_entries:
            if entry.borrower_user_id and entry.full_name:
                await upsert_borrower(
                    service="finkit",
                    borrower_user_id=entry.borrower_user_id,
                    full_name=entry.full_name,
                    document_id=entry.document_id,
                )

        _clear_error("finkit")
    except Exception as e:
        log.exception("Finkit poll error: %s", e)
        await _notify_error(bot, "finkit", e)


async def poll_zaimis(bot: Bot) -> None:
    global _opi_checker
    if not await _should_poll("zaimis"):
        return
    try:
        creds_list = await _get_user_credentials("zaimis")
        if not creds_list:
            return

        subs = await get_active_subscriptions("zaimis")
        subs_list = [sub for _, sub in subs] if subs else None

        all_entries: list[BorrowEntry] = []
        seen_entry_ids: set[str] = set()
        for cred in creds_list:
            pkey = (cred.chat_id, cred.login)
            parser = _zaimis_parsers.get(pkey)
            if parser is None:
                parser = ZaimisParser()
                ok = await parser.login(cred.login, cred.password)
                if not ok:
                    log.warning("Zaimis login failed for chat_id=%s login=%s", cred.chat_id, cred.login)
                    continue
                _zaimis_parsers[pkey] = parser

            entries = await parser.fetch_borrows(subscriptions=subs_list)

            # Auto re-login on token expiry
            if parser.needs_reauth:
                log.info("Zaimis token expired for chat_id=%s login=%s — re-logging in", cred.chat_id, cred.login)
                await parser.close()
                new_parser = ZaimisParser()
                ok = await new_parser.login(cred.login, cred.password)
                if ok:
                    _zaimis_parsers[pkey] = new_parser
                    entries = await new_parser.fetch_borrows(subscriptions=subs_list)
                else:
                    log.warning("Zaimis re-login failed for chat_id=%s login=%s", cred.chat_id, cred.login)
                    _zaimis_parsers.pop(pkey, None)
                    continue

            if entries:
                for entry in entries:
                    if entry.id not in seen_entry_ids:
                        seen_entry_ids.add(entry.id)
                        all_entries.append(entry)

        # Save all Zaimis borrowers we see (track nicknames)
        for entry in all_entries:
            if entry.borrower_user_id:
                await upsert_borrower(
                    service="zaimis",
                    borrower_user_id=entry.borrower_user_id,
                    full_name=entry.display_name,
                )

        # ── Stage 1: Compute fresh + SEND basic notifications ──
        fresh = await _compute_fresh(all_entries, "zaimis")
        sent_refs = []
        if fresh:
            sent_refs = await notify_users(bot, fresh, "zaimis", skip_enrichment=True)
            log.info("Sent %d basic notifications for zaimis (%d fresh / %d total)",
                     len(sent_refs), len(fresh), len(all_entries))

            # ── Stage 2: Enrich from DB + OPI + EDIT ──
            if sent_refs:
                for entry in fresh:
                    await enrich_entry_from_borrowers(entry)

                # OPI for entries with document_id
                sent_ids = {entry_id for entry_id, _chat_id, _msg_id, _subs in sent_refs}
                entries_needing_opi = [e for e in fresh if e.id in sent_ids and e.document_id and not e.opi_checked]
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
    except Exception as e:
        log.exception("Zaimis poll error: %s", e)
        await _notify_error(bot, "zaimis", e)


# ====== Midnight cron jobs ======

async def midnight_refresh_investments(bot: Bot) -> None:
    """Nightly job: refresh investment history → upsert into borrowers table.
    Runs only every 3 days to reduce load."""
    global _last_investment_refresh

    now = datetime.now(timezone.utc)
    if _last_investment_refresh is not None:
        elapsed = now - _last_investment_refresh
        if elapsed < timedelta(days=INVESTMENT_REFRESH_INTERVAL_DAYS):
            log.info("🌙 Investment refresh skipped (last run %s ago, interval %d days)",
                     elapsed, INVESTMENT_REFRESH_INTERVAL_DAYS)
            return

    _last_investment_refresh = now
    log.info("🌙 Midnight job: refreshing investments → borrowers...")
    total_finkit = 0
    total_zaimis = 0
    errors: list[str] = []

    # --- Finkit investments ---
    try:
        db = await get_db()
        try:
            creds = await db.execute_fetchall(
                "SELECT chat_id, login, password FROM credentials WHERE service='finkit'"
            )
        finally:
            await db.close()

        for cred in creds:
            try:
                chat_id = cred["chat_id"] if "chat_id" in cred.keys() else None
                fp = FinkitParser()
                ok = await fp.login(cred["login"], cred["password"])
                if not ok:
                    errors.append(f"Finkit login failed: {cred['login']}")
                    continue

                session = await fp._get_session()
                cookie_str = "; ".join(f"{k}={v}" for k, v in fp._session_cookies.items())
                headers = {"Accept": "application/json", "Referer": "https://finkit.by/", "Cookie": cookie_str}

                # Step 1: collect all investments from list endpoint
                all_investments: list[dict] = []
                page = 1
                while True:
                    url = f"https://api-p2p.finkit.by/user/investments/?page={page}"
                    async with session.get(url, headers=headers) as resp:
                        if resp.status != 200:
                            break
                        data = await resp.json()
                    all_investments.extend(data.get("results", []))
                    if not data.get("next"):
                        break
                    page += 1

                log.info("Finkit %s: fetched %d investments from list",
                         cred["login"], len(all_investments))

                # Step 2: aggregate stats per borrower_full_name
                name_stats: dict[str, dict] = {}
                name_to_inv_ids: dict[str, list[str]] = {}
                for inv in all_investments:
                    bname = (inv.get("borrower_full_name") or "").strip()
                    if not bname:
                        continue
                    if bname not in name_stats:
                        name_stats[bname] = {
                            "total": 0, "settled": 0, "overdue": 0,
                            "ratings": [], "invested": 0.0,
                        }
                        name_to_inv_ids[bname] = []
                    s = name_stats[bname]
                    s["total"] += 1
                    status = inv.get("status", "")
                    if status == "settled" and inv.get("closed", False):
                        s["settled"] += 1
                    if inv.get("is_overdue"):
                        s["overdue"] += 1
                    try:
                        s["invested"] += float(inv.get("amount", 0))
                    except (ValueError, TypeError):
                        pass
                    try:
                        rating = float(inv.get("borrower_score", 0))
                        if rating > 0:
                            s["ratings"].append(rating)
                    except (ValueError, TypeError):
                        pass
                    total_finkit += 1
                    inv_id = inv.get("id")
                    if inv_id:
                        name_to_inv_ids[bname].append(inv_id)

                # Step 3: resolve borrower_user_id for each unique borrower
                db2 = await get_db()
                try:
                    existing = await db2.execute_fetchall(
                        "SELECT borrower_user_id, full_name FROM borrowers WHERE service='finkit'"
                    )
                finally:
                    await db2.close()
                name_to_buid: dict[str, str] = {
                    row["full_name"]: row["borrower_user_id"]
                    for row in existing if row["full_name"]
                }

                pdf_enriched = 0
                for bname, s in name_stats.items():
                    buid = name_to_buid.get(bname)

                    # Unknown borrower → fetch detail + PDF to extract ИН
                    if not buid and name_to_inv_ids.get(bname):
                        inv_id = name_to_inv_ids[bname][0]
                        detail_url = f"https://api-p2p.finkit.by/user/investments/{inv_id}/"
                        try:
                            async with session.get(detail_url, headers=headers) as resp:
                                if resp.status == 200:
                                    detail = await resp.json()
                                    # 'loan' is the loan UUID — use as borrower key
                                    # ('user' is the investor UUID, not the borrower)
                                    loan_uuid = detail.get("loan", inv_id)
                                    buid = str(loan_uuid)
                                    # Try PDF extraction for ИН
                                    pdf_url = detail.get("latest_contract_url")
                                    if pdf_url:
                                        try:
                                            async with session.get(pdf_url) as pdf_resp:
                                                if pdf_resp.status == 200:
                                                    pdf_bytes = await pdf_resp.read()
                                                    doc_id = None
                                                    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                                                        for pg in pdf.pages:
                                                            text = pg.extract_text() or ""
                                                            m = _NAME_ID_RE.search(text)
                                                            if m:
                                                                doc_id = m.group(2).strip()
                                                                break
                                                    if doc_id and len(doc_id) == 14:
                                                        await upsert_borrower(
                                                            service="finkit",
                                                            borrower_user_id=buid,
                                                            full_name=bname,
                                                            document_id=doc_id,
                                                        )
                                                        pdf_enriched += 1
                                                        log.info("Finkit midnight PDF: %s → ИН %s", bname, doc_id)
                                        except Exception as ex:
                                            log.debug("Finkit midnight PDF error for %s: %s", bname, ex)
                            await asyncio.sleep(0.2)
                        except Exception as ex:
                            log.warning("Finkit detail fetch error for %s: %s", bname, ex)

                    if not buid:
                        continue

                    avg_r = sum(s["ratings"]) / len(s["ratings"]) if s["ratings"] else None
                    await upsert_borrower_from_investment(
                        service="finkit", borrower_user_id=buid,
                        full_name=bname or None,
                        total_loans=s["total"], settled_loans=s["settled"],
                        overdue_loans=s["overdue"], avg_rating=avg_r,
                        total_invested=s["invested"],
                    )

                if pdf_enriched:
                    log.info("Finkit midnight: enriched %d new borrowers with ИН from PDF", pdf_enriched)

                await fp.close()
            except Exception as ex:
                errors.append(f"Finkit {cred['login']}: {ex}")
    except Exception as ex:
        errors.append(f"Finkit global: {ex}")

    # --- Zaimis investments → borrowers table ---
    try:
        db = await get_db()
        try:
            creds = await db.execute_fetchall(
                "SELECT chat_id, login, password FROM credentials WHERE service='zaimis'"
            )
        finally:
            await db.close()

        for cred in creds:
            try:
                chat_id = cred["chat_id"] if "chat_id" in cred.keys() else None
                zp = ZaimisParser()
                ok = await zp.login(cred["login"], cred["password"])
                if not ok:
                    errors.append(f"Zaimis login failed: {cred['login']}")
                    continue

                orders = await zp.fetch_investments()
                # Aggregate per borrower (counterparty is the actual borrower)
                zaimis_stats: dict[str, dict] = {}
                for order in orders:
                    cp = order.get("counterparty", {}) or {}
                    buid = str(cp.get("id", ""))
                    if not buid:
                        continue
                    offer = order.get("offer", {}) or {}
                    if buid not in zaimis_stats:
                        zaimis_stats[buid] = {
                            "full_name": cp.get("displayName", ""),
                            "total": 0, "settled": 0, "overdue": 0,
                            "ratings": [], "invested": 0.0,
                        }
                    s = zaimis_stats[buid]
                    s["total"] += 1
                    state = order.get("state")
                    if state == 3:
                        s["settled"] += 1
                    if state == 4:
                        s["overdue"] += 1
                    try:
                        s["invested"] += float(order.get("amount", 0))
                    except (ValueError, TypeError):
                        pass
                    score = offer.get("score")
                    if score is not None:
                        try:
                            s["ratings"].append(float(score))
                        except (ValueError, TypeError):
                            pass
                    total_zaimis += 1

                # Upsert into borrowers table
                for buid, s in zaimis_stats.items():
                    avg_r = sum(s["ratings"]) / len(s["ratings"]) if s["ratings"] else None
                    await upsert_borrower_from_investment(
                        service="zaimis",
                        borrower_user_id=buid,
                        full_name=s["full_name"] or None,
                        total_loans=s["total"],
                        settled_loans=s["settled"],
                        overdue_loans=s["overdue"],
                        avg_rating=avg_r,
                        total_invested=s["invested"],
                    )

                # Enrich borrowers with PDF data (ФИО + ИН)
                try:
                    pdf_results = await zp.enrich_borrowers_from_orders(orders)
                    for cp_id, (full_name, doc_id) in pdf_results.items():
                        await upsert_borrower(
                            service="zaimis",
                            borrower_user_id=cp_id,
                            full_name=full_name,
                            document_id=doc_id,
                        )
                    if pdf_results:
                        log.info("Zaimis PDF: saved %d borrowers with ИН", len(pdf_results))
                except Exception as ex:
                    errors.append(f"Zaimis PDF enrichment: {ex}")

                await zp.close()
            except Exception as ex:
                errors.append(f"Zaimis {cred['login']}: {ex}")
    except Exception as ex:
        errors.append(f"Zaimis global: {ex}")

    log.info("🌙 Investments refresh done: finkit=%d items, zaimis=%d items",
             total_finkit, total_zaimis)

    # Notify admin
    try:
        from bot.database import get_borrowers_count
        b_count = await get_borrowers_count()
        lines = [
            "🌙 <b>Ночное обновление инвестиций</b>",
            f"  Finkit: {total_finkit} записей",
            f"  Zaimis: {total_zaimis} записей",
            f"  Borrowers в БД: {b_count}",
        ]
        if errors:
            lines.append(f"\n⚠️ Ошибки ({len(errors)}):")
            for e in errors[:5]:
                lines.append(f"  • {str(e)[:200]}")
        if config.ADMIN_CHAT_ID:
            await bot.send_message(config.ADMIN_CHAT_ID, "\n".join(lines), parse_mode="HTML")
    except Exception:
        pass


async def midnight_refresh_opi(bot: Bot) -> None:
    """Nightly job: check OPI for borrowers where check is stale (>3 days) or never done."""
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
            except Exception as ex:
                log.warning("OPI check error for %s: %s", doc_id, ex)
                errors += 1
    finally:
        await checker.close()

    log.info("🌙 OPI refresh done: checked=%d, errors=%d", checked, errors)

    try:
        text = (
            f"🌙 <b>Ночная проверка ОПИ</b>\n"
            f"  Проверено: {checked}/{len(stale)}\n"
            f"  Ошибок: {errors}"
        )
        if config.ADMIN_CHAT_ID:
            await bot.send_message(config.ADMIN_CHAT_ID, text, parse_mode="HTML")
    except Exception:
        pass


def setup_scheduler(bot: Bot) -> AsyncIOScheduler:
    global _scheduler
    _scheduler = AsyncIOScheduler()

    # Base interval 30s — actual poll frequency controlled by site_settings
    BASE = 30
    _scheduler.add_job(poll_kapusta, "interval", seconds=BASE,
                      args=[bot], id="kapusta", name="Kapusta poll",
                      misfire_grace_time=60)

    _scheduler.add_job(poll_finkit, "interval", seconds=BASE,
                      args=[bot], id="finkit", name="Finkit poll",
                      misfire_grace_time=60)

    _scheduler.add_job(poll_zaimis, "interval", seconds=BASE,
                      args=[bot], id="zaimis", name="Zaimis poll",
                      misfire_grace_time=60)

    # Midnight cron jobs (00:00 Minsk = 21:00 UTC)
    _scheduler.add_job(
        midnight_refresh_investments,
        CronTrigger(hour=21, minute=0, timezone="UTC"),  # 00:00 Minsk
        args=[bot], id="midnight_investments",
        name="Midnight investments refresh",
        misfire_grace_time=3600,
    )

    _scheduler.add_job(
        midnight_refresh_opi,
        CronTrigger(hour=21, minute=30, timezone="UTC"),  # 00:30 Minsk
        args=[bot], id="midnight_opi",
        name="Midnight OPI refresh",
        misfire_grace_time=3600,
    )

    log.info("Scheduler configured (base=%ds, actual intervals from DB, midnight cron at 00:00+00:30 Minsk)", BASE)
    return _scheduler


async def shutdown_parsers():
    """Clean up parser sessions on shutdown."""
    global _kapusta, _opi_checker
    for p in [_kapusta, _opi_checker]:
        if p:
            try:
                await p.close()
            except Exception:
                pass
    for p in list(_finkit_parsers.values()) + list(_zaimis_parsers.values()):
        try:
            await p.close()
        except Exception:
            pass
    _finkit_parsers.clear()
    _zaimis_parsers.clear()


def get_parser(service: str, chat_id: int | None = None):
    """Get an active parser instance for on-demand use (e.g., export)."""
    if service == "kapusta":
        return _kapusta
    elif service == "finkit" and chat_id:
        # Find any parser for this chat_id
        for (cid, _login), p in _finkit_parsers.items():
            if cid == chat_id:
                return p
        return None
    elif service == "zaimis" and chat_id:
        for (cid, _login), p in _zaimis_parsers.items():
            if cid == chat_id:
                return p
        return None
    elif service == "mongo":
        from bot.parsers.mongo import MongoParser
        return MongoParser()
    return None
