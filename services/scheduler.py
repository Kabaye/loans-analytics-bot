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
from bot.database import (
    get_db, get_site_settings,
    upsert_borrower_from_investment, get_stale_opi_documents,
    save_opi_result, upsert_borrower,
)
from bot.models import BorrowEntry, UserCredentials
from bot.parsers.kapusta import KapustaParser, KapustaBlockedError
from bot.parsers.finkit import FinkitParser
from bot.parsers.mongo import MongoParser
from bot.parsers.zaimis import ZaimisParser
from bot.services.notifier import notify_users, get_active_subscriptions, has_active_subscriptions
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
_mongo: Optional[MongoParser] = None
_opi_checker: Optional[OPIChecker] = None

# Per-user parser instances for authenticated services
_finkit_parsers: dict[int, FinkitParser] = {}
_zaimis_parsers: dict[int, ZaimisParser] = {}

# Exposed scheduler instance for dynamic reconfiguration
_scheduler: Optional[AsyncIOScheduler] = None

# === Kapusta backoff on 403 ===
_kapusta_backoff_until: Optional[datetime] = None

# === Per-site last poll timestamps (for custom intervals + time-based filtering) ===
_last_poll: dict[str, datetime] = {}

# === Per-site seen entry IDs (for reliable freshness detection) ===
_seen_ids: dict[str, set[str]] = {}

# === Error tracking — notify admin once per site failure ===
_error_notified: dict[str, bool] = {
    "kapusta": False,
    "finkit": False,
    "mongo": False,
    "zaimis": False,
}


def _get_cutoff(service: str) -> datetime | None:
    """Get cutoff time for filtering fresh entries.
    Returns the time of the last poll start, or None if first poll."""
    return _last_poll.get(service)


def _compute_fresh(entries: list[BorrowEntry], service: str) -> list[BorrowEntry]:
    """Compute fresh entries using ID-based tracking.
    First poll initialises the set and returns empty (no flood on startup).
    Subsequent polls return entries whose ID was not in the previous set."""
    current_ids = {e.id for e in entries}
    if service not in _seen_ids:
        _seen_ids[service] = current_ids
        return []
    prev = _seen_ids[service]
    fresh = [e for e in entries if e.id not in prev]
    _seen_ids[service] = current_ids
    return fresh


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

    svc_names = {"kapusta": "🥔 Капуста", "finkit": "🏦 Финкит", "mongo": "🍃 Монго", "zaimis": "💰 Займись"}
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
            SELECT c.chat_id, c.login, c.password
            FROM credentials c
            JOIN users u ON c.chat_id = u.chat_id
            WHERE c.service = ? AND u.is_allowed = 1
            """,
            (service,),
        )
        return [
            UserCredentials(chat_id=r["chat_id"], service=service, login=r["login"], password=r["password"])
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
            fresh = _compute_fresh(entries, "kapusta")
            if fresh:
                cnt = await notify_users(bot, fresh, "kapusta")
                log.info("Sent %d notifications for kapusta (%d fresh / %d total)", cnt, len(fresh), len(entries))
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


async def poll_mongo(bot: Bot) -> None:
    global _mongo
    if not await _should_poll("mongo"):
        return
    try:
        if _mongo is None:
            _mongo = MongoParser()

        entries = await _mongo.fetch_borrows()
        _clear_error("mongo")
        if entries:
            fresh = _compute_fresh(entries, "mongo")
            if fresh:
                cnt = await notify_users(bot, fresh, "mongo")
                log.info("Sent %d notifications for mongo (%d fresh / %d total)", cnt, len(fresh), len(entries))
    except Exception as e:
        log.exception("Mongo poll error: %s", e)
        _mongo = None


async def poll_finkit(bot: Bot) -> None:
    global _opi_checker
    if not await _should_poll("finkit"):
        return
    try:
        creds_list = await _get_user_credentials("finkit")
        if not creds_list:
            return

        all_entries: list[BorrowEntry] = []
        seen_entry_ids: set[str] = set()
        for cred in creds_list:
            parser = _finkit_parsers.get(cred.chat_id)
            if parser is None:
                parser = FinkitParser()
                ok = await parser.login(cred.login, cred.password)
                if not ok:
                    log.warning("Finkit login failed for chat_id=%s", cred.chat_id)
                    continue
                _finkit_parsers[cred.chat_id] = parser

            entries = await parser.fetch_borrows()

            # Auto re-login on session expiry
            if parser.needs_reauth:
                log.info("Finkit session expired for chat_id=%s — re-logging in", cred.chat_id)
                await parser.close()
                new_parser = FinkitParser()
                ok = await new_parser.login(cred.login, cred.password)
                if ok:
                    _finkit_parsers[cred.chat_id] = new_parser
                    entries = await new_parser.fetch_borrows()
                else:
                    log.warning("Finkit re-login failed for chat_id=%s", cred.chat_id)
                    _finkit_parsers.pop(cred.chat_id, None)
                    continue

            if not entries:
                continue

            # Enrich with PDF data (full name + document ID + cached OPI/history)
            await parser.enrich_with_pdf(entries)

            # Save all borrowers we see (even without ИН) for future enrichment
            for entry in entries:
                if entry.borrower_user_id:
                    await upsert_borrower(
                        service="finkit",
                        borrower_user_id=entry.borrower_user_id,
                        full_name=entry.full_name,
                        document_id=entry.document_id,
                    )

            # OPI check ONLY for entries with document_id that don't already have OPI from cache
            # (actual freshness is computed below after aggregation)
            for entry in entries:
                if entry.id not in seen_entry_ids:
                    seen_entry_ids.add(entry.id)
                    all_entries.append(entry)

        # ID-based freshness for aggregated entries
        fresh_entries = _compute_fresh(all_entries, "finkit")

        # OPI check for fresh entries that have document_id but no cached OPI
        if fresh_entries:
            entries_needing_opi = [e for e in fresh_entries if e.document_id and not e.opi_checked]
            if entries_needing_opi:
                subs = await get_active_subscriptions("finkit")
                if subs:
                    matched_ids = set()
                    for entry in entries_needing_opi:
                        for _chat_id, sub in subs:
                            if sub.matches(entry):
                                matched_ids.add(entry.id)
                                break
                    entries_to_check = [e for e in entries_needing_opi if e.id in matched_ids]
                    if entries_to_check:
                        if _opi_checker is None:
                            _opi_checker = OPIChecker()
                        log.info("OPI check for %d entries (fresh + matched)", len(entries_to_check))
                        for entry in entries_to_check:
                            result = await _opi_checker.check(entry.document_id)
                            entry.opi_checked = True
                            entry.opi_has_debt = result.has_debt
                            entry.opi_debt_amount = result.debt_amount
                            entry.opi_full_name = result.full_name

            cnt = await notify_users(bot, fresh_entries, "finkit")
            log.info("Sent %d notifications for finkit (%d fresh / %d total)", cnt, len(fresh_entries), len(all_entries))

        _clear_error("finkit")
    except Exception as e:
        log.exception("Finkit poll error: %s", e)
        await _notify_error(bot, "finkit", e)


async def poll_zaimis(bot: Bot) -> None:
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
            parser = _zaimis_parsers.get(cred.chat_id)
            if parser is None:
                parser = ZaimisParser()
                ok = await parser.login(cred.login, cred.password)
                if not ok:
                    log.warning("Zaimis login failed for chat_id=%s", cred.chat_id)
                    continue
                _zaimis_parsers[cred.chat_id] = parser

            entries = await parser.fetch_borrows(subscriptions=subs_list)

            # Auto re-login on token expiry
            if parser.needs_reauth:
                log.info("Zaimis token expired for chat_id=%s — re-logging in", cred.chat_id)
                await parser.close()
                new_parser = ZaimisParser()
                ok = await new_parser.login(cred.login, cred.password)
                if ok:
                    _zaimis_parsers[cred.chat_id] = new_parser
                    entries = await new_parser.fetch_borrows(subscriptions=subs_list)
                else:
                    log.warning("Zaimis re-login failed for chat_id=%s", cred.chat_id)
                    _zaimis_parsers.pop(cred.chat_id, None)
                    continue

            if entries:
                for entry in entries:
                    if entry.id not in seen_entry_ids:
                        seen_entry_ids.add(entry.id)
                        all_entries.append(entry)

        # ID-based freshness for aggregated entries
        fresh = _compute_fresh(all_entries, "zaimis")
        if fresh:
            cnt = await notify_users(bot, fresh, "zaimis")
            log.info("Sent %d notifications for zaimis (%d fresh / %d total)", cnt, len(fresh), len(all_entries))

        _clear_error("zaimis")
    except Exception as e:
        log.exception("Zaimis poll error: %s", e)
        await _notify_error(bot, "zaimis", e)


# ====== Midnight cron jobs ======

async def midnight_refresh_investments(bot: Bot) -> None:
    """Nightly job: refresh investment history → upsert into borrowers table."""
    log.info("🌙 Midnight job: refreshing investments → borrowers...")
    total_finkit = 0
    total_zaimis = 0
    errors: list[str] = []

    # --- Finkit investments ---
    try:
        db = await get_db()
        try:
            creds = await db.execute_fetchall(
                "SELECT login, password FROM credentials WHERE service='finkit'"
            )
        finally:
            await db.close()

        for cred in creds:
            try:
                fp = FinkitParser()
                ok = await fp.login(cred["login"], cred["password"])
                if not ok:
                    errors.append(f"Finkit login failed: {cred['login']}")
                    continue

                session = await fp._get_session()
                cookie_str = "; ".join(f"{k}={v}" for k, v in fp._session_cookies.items())
                headers = {"Accept": "application/json", "Referer": "https://finkit.by/", "Cookie": cookie_str}

                # Aggregate per borrower from investments
                borrower_stats: dict[str, dict] = {}
                page = 1
                while True:
                    url = f"https://api-p2p.finkit.by/user/investments/?page={page}"
                    async with session.get(url, headers=headers) as resp:
                        if resp.status != 200:
                            break
                        data = await resp.json()

                    for inv in data.get("results", []):
                        buid = inv.get("user")
                        if not buid:
                            buid = inv.get("loan")  # fallback
                        if not buid:
                            continue
                        buid = str(buid)
                        bname = inv.get("borrower_full_name", "")
                        if buid not in borrower_stats:
                            borrower_stats[buid] = {
                                "full_name": bname, "total": 0, "settled": 0,
                                "overdue": 0, "ratings": [], "invested": 0.0,
                            }
                        s = borrower_stats[buid]
                        s["total"] += 1
                        if inv.get("status") == "settled":
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

                    if not data.get("next"):
                        break
                    page += 1

                for buid, s in borrower_stats.items():
                    avg_r = sum(s["ratings"]) / len(s["ratings"]) if s["ratings"] else None
                    await upsert_borrower_from_investment(
                        service="finkit", borrower_user_id=buid,
                        full_name=s["full_name"] or None,
                        total_loans=s["total"], settled_loans=s["settled"],
                        overdue_loans=s["overdue"], avg_rating=avg_r,
                        total_invested=s["invested"],
                    )

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
                "SELECT login, password FROM credentials WHERE service='zaimis'"
            )
        finally:
            await db.close()

        for cred in creds:
            try:
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

    _scheduler.add_job(poll_mongo, "interval", seconds=BASE,
                      args=[bot], id="mongo", name="Mongo poll",
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
    global _kapusta, _mongo, _opi_checker
    for p in [_kapusta, _mongo, _opi_checker]:
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
    elif service == "mongo":
        return _mongo
    elif service == "finkit" and chat_id:
        return _finkit_parsers.get(chat_id)
    elif service == "zaimis" and chat_id:
        return _zaimis_parsers.get(chat_id)
    return None
