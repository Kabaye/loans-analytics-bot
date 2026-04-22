"""Scheduler — periodic parsing of all sites with caching, backoff, and error alerts."""
from __future__ import annotations

import asyncio
import json
import logging
import traceback
from datetime import datetime, timezone, timedelta
from html import escape
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
    get_saved_credential_session, save_credential_session, delete_credential_session,
    get_json_schema_state, save_json_schema_state,
    deactivate_missing_overdue_cases, lookup_borrower, upsert_overdue_case, update_overdue_case_contacts,
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

# Per-credential parser instances for authenticated services
_finkit_parsers: dict[int, FinkitParser] = {}
_zaimis_parsers: dict[int, ZaimisParser] = {}
_poll_rotation_index: dict[str, int] = {"finkit": 0, "zaimis": 0}

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


def _telegram_user_tag(cred: UserCredentials) -> str:
    username = (cred.username or "").strip().lstrip("@")
    return username or f"chat_{cred.chat_id}"


def _safe_float(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _coalesce(*values):
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _total_due(principal: float | None, accrued: float | None, fine: float | None, fallback) -> float | None:
    parts = [part for part in (principal, accrued, fine) if part is not None]
    if parts:
        return sum(parts)
    return _safe_float(fallback)


def _days_overdue_from_due(due_at: str | None) -> int | None:
    if not due_at:
        return None
    try:
        parsed = datetime.fromisoformat(due_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)
    return max(int(delta.total_seconds() // 86400), 0)


def _json_type_name(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return type(value).__name__


def _collect_schema_types(value, path: str, acc: dict[str, set[str]]) -> None:
    acc.setdefault(path, set()).add(_json_type_name(value))
    if isinstance(value, dict):
        for key, nested in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            _collect_schema_types(nested, child_path, acc)
    elif isinstance(value, list):
        item_path = f"{path}[]" if path else "[]"
        if not value:
            acc.setdefault(item_path, set()).add("empty")
        for item in value:
            _collect_schema_types(item, item_path, acc)


def _build_entries_schema(entries: list[BorrowEntry]) -> dict[str, list[str]]:
    acc: dict[str, set[str]] = {}
    for entry in entries:
        if entry.raw_data is not None:
            _collect_schema_types(entry.raw_data, "", acc)
    return {k: sorted(v) for k, v in sorted(acc.items()) if k}


def _non_null_schema_types(types: list[str] | None) -> set[str]:
    return {t for t in (types or []) if t not in {"null", "empty"}}


def _merge_schema(prev: dict[str, list[str]] | None, current: dict[str, list[str]]) -> dict[str, list[str]]:
    merged: dict[str, set[str]] = {}
    for source in (prev or {}, current):
        for path, types in source.items():
            merged.setdefault(path, set()).update(types)
    return {path: sorted(types) for path, types in sorted(merged.items())}


def _schema_diff(prev: dict[str, list[str]] | None, current: dict[str, list[str]]) -> tuple[list[str], list[str]]:
    prev = prev or {}
    added_paths: list[str] = []
    changed_types: list[str] = []
    for path, types in current.items():
        if path not in prev:
            non_null_types = sorted(_non_null_schema_types(types))
            if non_null_types:
                added_paths.append(f"{path} ({', '.join(non_null_types)})")
            continue
        prev_types = sorted(_non_null_schema_types(prev[path]))
        current_types = sorted(_non_null_schema_types(types))
        if not prev_types:
            continue
        new_types = [t for t in current_types if t not in prev_types]
        if new_types:
            before = ", ".join(prev_types) if prev_types else "no-non-null-types"
            changed_types.append(f"{path}: {before} -> {', '.join(current_types)}")
    return added_paths, changed_types


async def _notify_json_schema_change(bot: Bot, service: str, entries: list[BorrowEntry]) -> None:
    current = _build_entries_schema(entries)
    if not current:
        return

    prev = await get_json_schema_state(service)
    if prev is None:
        await save_json_schema_state(service, current)
        return

    added_paths, changed_types = _schema_diff(prev, current)
    merged = _merge_schema(prev, current)
    if not added_paths and not changed_types:
        if merged != prev:
            await save_json_schema_state(service, merged)
        return

    await save_json_schema_state(service, merged)
    svc_names = {"kapusta": "🥬 Kapusta", "finkit": "🔵 FinKit", "zaimis": "🟪 ЗАЙМись"}
    lines = [f"⚠️ <b>Изменилась JSON-структура {svc_names.get(service, service)}</b>"]
    if added_paths:
        lines.append("")
        lines.append("<b>Новые поля:</b>")
        lines.extend(f"  • {escape(item)}" for item in added_paths[:20])
    if changed_types:
        lines.append("")
        lines.append("<b>Изменились типы:</b>")
        lines.extend(f"  • {escape(item)}" for item in changed_types[:20])
    sample = next((e.raw_data for e in entries if e.raw_data), None)
    if sample:
        sample_text = escape(json.dumps(sample, ensure_ascii=False, default=str)[:1200])
        lines.append("")
        lines.append(f"<pre>{sample_text}</pre>")
    try:
        if config.ADMIN_CHAT_ID:
            await bot.send_message(config.ADMIN_CHAT_ID, "\n".join(lines), parse_mode="HTML")
            log.info("JSON schema change notification sent for %s", service)
    except Exception as e:
        log.warning("Failed to send JSON schema notification: %s", e)


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


async def _get_user_credentials(service: str, chat_id: int | None = None) -> list[UserCredentials]:
    db = await get_db()
    try:
        query = """
            SELECT c.id, c.chat_id, c.login, c.password, u.username
            FROM credentials c
            JOIN users u ON c.chat_id = u.chat_id
            WHERE c.service = ? AND u.is_allowed = 1
        """
        params: list = [service]
        if chat_id is not None:
            query += " AND c.chat_id = ?"
            params.append(chat_id)
        query += " ORDER BY c.id"
        rows = await db.execute_fetchall(query, tuple(params))
        return [
            UserCredentials(
                id=r["id"],
                chat_id=r["chat_id"],
                service=service,
                login=r["login"],
                password=r["password"],
                username=r["username"],
            )
            for r in rows
        ]
    finally:
        await db.close()


def _pick_round_robin_credential(service: str, creds_list: list[UserCredentials]) -> UserCredentials | None:
    if not creds_list:
        return None
    idx = _poll_rotation_index.get(service, 0) % len(creds_list)
    _poll_rotation_index[service] = (idx + 1) % len(creds_list)
    return creds_list[idx]


def _remember_parser_owner(parser, cred: UserCredentials) -> None:
    setattr(parser, "_owner_chat_id", cred.chat_id)
    setattr(parser, "_owner_credential_id", cred.id)


async def _login_and_persist(service: str, parser, cred: UserCredentials) -> bool:
    ok = await parser.login(cred.login, cred.password)
    if not ok:
        await delete_credential_session(cred.id)
        return False
    export = getattr(parser, "export_session", None)
    if callable(export):
        session_data = export()
        if session_data:
            await save_credential_session(cred.id, service, session_data)
    return True


async def _ensure_finkit_parser(cred: UserCredentials, force_login: bool = False) -> FinkitParser | None:
    parser = _finkit_parsers.get(cred.id)
    if parser is None or force_login:
        if parser is not None:
            try:
                await parser.close()
            except Exception:
                pass
        parser = FinkitParser()
        _remember_parser_owner(parser, cred)
        _finkit_parsers[cred.id] = parser

    if not force_login:
        export = getattr(parser, "export_session", None)
        if callable(export) and export():
            return parser
        saved = await get_saved_credential_session(cred.id)
        restore = getattr(parser, "restore_session", None)
        if callable(restore) and restore(saved):
            return parser

    ok = await _login_and_persist("finkit", parser, cred)
    if ok:
        return parser
    _finkit_parsers.pop(cred.id, None)
    return None


async def _ensure_zaimis_parser(cred: UserCredentials, force_login: bool = False) -> ZaimisParser | None:
    parser = _zaimis_parsers.get(cred.id)
    if parser is None or force_login:
        if parser is not None:
            try:
                await parser.close()
            except Exception:
                pass
        parser = ZaimisParser()
        _remember_parser_owner(parser, cred)
        _zaimis_parsers[cred.id] = parser

    if not force_login:
        export = getattr(parser, "export_session", None)
        if callable(export) and export():
            return parser
        saved = await get_saved_credential_session(cred.id)
        restore = getattr(parser, "restore_session", None)
        if callable(restore) and restore(saved):
            return parser

    ok = await _login_and_persist("zaimis", parser, cred)
    if ok:
        return parser
    _zaimis_parsers.pop(cred.id, None)
    return None


async def get_export_parsers(service: str, chat_id: int) -> list:
    """Return reusable parsers for export without creating extra logins when a saved session exists."""
    if service == "kapusta":
        global _kapusta
        if _kapusta is None:
            _kapusta = KapustaParser()
            ok = await _kapusta.login()
            if not ok:
                await _kapusta.close()
                _kapusta = None
                return []
        return [_kapusta]

    if service == "finkit":
        cred = _pick_round_robin_credential(service, await _get_user_credentials(service, chat_id=chat_id))
        if not cred:
            return []
        parser = await _ensure_finkit_parser(cred)
        return [parser] if parser else []

    if service == "zaimis":
        cred = _pick_round_robin_credential(service, await _get_user_credentials(service, chat_id=chat_id))
        if not cred:
            return []
        parser = await _ensure_zaimis_parser(cred)
        return [parser] if parser else []

    return []


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
            await _notify_json_schema_change(bot, "kapusta", entries)
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

        cred = _pick_round_robin_credential("finkit", creds_list)
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

        if entries:
            await _notify_json_schema_change(bot, "finkit", entries)
        all_entries: list[BorrowEntry] = list(entries)
        parser_entries: list[tuple[FinkitParser, list[BorrowEntry]]] = [(parser, entries)] if entries else []

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
                            source="finkit_borrow",
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
                    source="finkit_borrow",
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

        cred = _pick_round_robin_credential("zaimis", creds_list)
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

        if all_entries:
            await _notify_json_schema_change(bot, "zaimis", all_entries)
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


async def _sync_finkit_overdue_cases() -> tuple[int, list[str]]:
    synced = 0
    errors: list[str] = []
    creds = await _get_user_credentials("finkit")

    for cred in creds:
        try:
            parser = await _ensure_finkit_parser(cred)
            if parser is None:
                errors.append(f"Finkit login failed: {cred.login}")
                continue

            items = await parser.fetch_investments()
            if parser.needs_reauth:
                parser = await _ensure_finkit_parser(cred, force_login=True)
                if parser is None:
                    errors.append(f"Finkit re-login failed: {cred.login}")
                    continue
                items = await parser.fetch_investments()

            credential_seen: set[str] = set()
            for item in items:
                if not item.get("is_overdue"):
                    continue
                external_id = str(item.get("id", "")).strip()
                if not external_id:
                    continue
                credential_seen.add(external_id)

                detail = await parser.fetch_investment_detail(external_id) or {}
                borrower_user_id = str(_coalesce(detail.get("loan"), item.get("user"), item.get("loan")) or "")
                cached = await lookup_borrower("finkit", borrower_user_id) if borrower_user_id else None

                principal = _safe_float(_coalesce(detail.get("principal_outstanding"), item.get("principal_outstanding"), item.get("amount")))
                accrued = _safe_float(_coalesce(detail.get("accrued_percent"), item.get("accrued_percent"), detail.get("expected_return")))
                fine = _safe_float(_coalesce(detail.get("fine_outstanding"), item.get("fine_outstanding")))
                due_at = _coalesce(detail.get("maturity_date"), detail.get("payment_date"), detail.get("due_at"), detail.get("due_date"), item.get("payment_date"), item.get("due_at"), item.get("due_date"))
                schedule_days = None
                schedules = detail.get("schedules") or []
                if schedules:
                    schedule_days = _safe_int(schedules[0].get("days_delayed"))
                days_overdue = _safe_int(_coalesce(detail.get("overdue_days"), item.get("overdue_days"), schedule_days)) or _days_overdue_from_due(due_at)
                address = None
                zip_code = None
                msi = detail.get("msi_registration_address") or {}
                if isinstance(msi, dict):
                    address = _coalesce(msi.get("formatted_address"))
                    zip_code = _coalesce(msi.get("postal_code"))

                case_id = await upsert_overdue_case(
                    chat_id=cred.chat_id,
                    credential_id=cred.id,
                    service="finkit",
                    external_id=external_id,
                    loan_id=str(_coalesce(detail.get("loan"), item.get("loan")) or "") or None,
                    loan_number=str(_coalesce(detail.get("loan_number"), item.get("loan_number")) or "") or None,
                    account_label=cred.login,
                    borrower_user_id=borrower_user_id or None,
                    document_id=(cached or {}).get("document_id"),
                    full_name=(cached or {}).get("full_name") or _coalesce(detail.get("borrower_full_name"), item.get("borrower_full_name")),
                    display_name=_coalesce(detail.get("borrower_short_name"), item.get("borrower_short_name")),
                    issued_at=_coalesce(detail.get("created"), item.get("created")),
                    due_at=due_at,
                    overdue_started_at=_coalesce(detail.get("overdue_started_at"), due_at, item.get("overdue_started_at")),
                    days_overdue=days_overdue,
                    amount=_safe_float(_coalesce(detail.get("amount"), item.get("amount"))),
                    principal_outstanding=principal,
                    accrued_percent=accrued,
                    fine_outstanding=fine,
                    total_due=_total_due(principal, accrued, fine, _coalesce(detail.get("total_due"), item.get("total_due"), item.get("amount"))),
                    status=str(_coalesce(detail.get("status"), item.get("status")) or "") or None,
                    contract_url=_coalesce(detail.get("latest_contract_url"), item.get("latest_contract_url")),
                    loan_url=None,
                    raw_data={"list": item, "detail": detail},
                )
                if address or zip_code or detail.get("borrower_phone_number") or detail.get("borrower_email"):
                    await update_overdue_case_contacts(
                        case_id,
                        cred.chat_id,
                        borrower_address=address,
                        borrower_zip=zip_code,
                        borrower_phone=detail.get("borrower_phone_number"),
                        borrower_email=detail.get("borrower_email"),
                    )
                synced += 1
                await asyncio.sleep(0.05)
            await deactivate_missing_overdue_cases(
                cred.chat_id,
                "finkit",
                sorted(credential_seen),
                credential_id=cred.id,
            )
        except Exception as ex:
            errors.append(f"Finkit overdue {cred.login}: {ex}")

    return synced, errors


async def _sync_zaimis_overdue_cases() -> tuple[int, list[str]]:
    synced = 0
    errors: list[str] = []
    creds = await _get_user_credentials("zaimis")

    for cred in creds:
        try:
            parser = await _ensure_zaimis_parser(cred)
            if parser is None:
                errors.append(f"Zaimis login failed: {cred.login}")
                continue

            orders = await parser.fetch_investments()
            if parser.needs_reauth:
                parser = await _ensure_zaimis_parser(cred, force_login=True)
                if parser is None:
                    errors.append(f"Zaimis re-login failed: {cred.login}")
                    continue
                orders = await parser.fetch_investments()

            credential_seen: set[str] = set()
            for order in orders:
                state = order.get("state")
                if state not in (4, "4", "overdue") and not order.get("isOverdue"):
                    continue
                external_id = str(order.get("id", "")).strip()
                if not external_id:
                    continue
                credential_seen.add(external_id)

                detail = await parser.fetch_order_detail(external_id) or {}
                cp = detail.get("counterparty", {}) or order.get("counterparty", {}) or {}
                offer = detail.get("offer", {}) or order.get("offer", {}) or {}
                model = detail.get("modelData", {}) or order.get("modelData", {}) or {}
                borrower_user_id = str(cp.get("id", "")).strip()
                cached = await lookup_borrower("zaimis", borrower_user_id) if borrower_user_id else None

                principal = _safe_float(_coalesce(detail.get("principalOutstanding"), order.get("principalOutstanding"), detail.get("amount"), order.get("amount"), offer.get("amount")))
                accrued = _safe_float(_coalesce(detail.get("interestOutstanding"), order.get("interestOutstanding"), model.get("profit"), model.get("interestWithOverdue")))
                fine = _safe_float(_coalesce(detail.get("penaltyOutstanding"), order.get("penaltyOutstanding"), model.get("penaltyAmount")))
                due_at = _coalesce(detail.get("returnDate"), order.get("returnDate"), detail.get("deadline"), order.get("deadline"), detail.get("dueAt"), order.get("dueAt"))
                overdue_started_at = _coalesce(detail.get("expiredDate"), order.get("expiredDate"), detail.get("overdueStartedAt"), order.get("overdueStartedAt"))
                actual_duration = _safe_int(_coalesce(detail.get("actualDuration"), order.get("actualDuration")))
                loan_term = _safe_int(_coalesce(detail.get("loanTerm"), order.get("loanTerm")))
                explicit_days = _safe_int(_coalesce(detail.get("daysOverdue"), order.get("daysOverdue")))
                duration_days = actual_duration - loan_term if actual_duration is not None and loan_term is not None else None
                if duration_days is not None and duration_days < 0:
                    duration_days = 0
                days_overdue = explicit_days or duration_days or _days_overdue_from_due(overdue_started_at or due_at)
                total_due = _safe_float(_coalesce(detail.get("totalOutstanding"), order.get("totalOutstanding"), model.get("closeSend"), order.get("returnAmount")))

                await upsert_overdue_case(
                    chat_id=cred.chat_id,
                    credential_id=cred.id,
                    service="zaimis",
                    external_id=external_id,
                    loan_id=str(_coalesce(detail.get("id"), offer.get("id"), order.get("offerId")) or "") or None,
                    loan_number=str(_coalesce(detail.get("number"), offer.get("id"), external_id) or "") or None,
                    account_label=cred.login,
                    borrower_user_id=borrower_user_id or None,
                    document_id=(cached or {}).get("document_id"),
                    full_name=(cached or {}).get("full_name") or cp.get("fullName") or cp.get("displayName"),
                    display_name=cp.get("displayName"),
                    issued_at=_coalesce(detail.get("createdAt"), order.get("createdAt"), offer.get("createdAt")),
                    due_at=due_at,
                    overdue_started_at=overdue_started_at,
                    days_overdue=days_overdue,
                    amount=_safe_float(_coalesce(order.get("amount"), detail.get("amount"), offer.get("amount"))),
                    principal_outstanding=principal,
                    accrued_percent=accrued,
                    fine_outstanding=fine,
                    total_due=total_due if total_due is not None else _total_due(principal, accrued, fine, _coalesce(order.get("amount"), detail.get("amount"))),
                    status=str(_coalesce(detail.get("state"), order.get("state")) or "") or None,
                    contract_url=None,
                    loan_url=None,
                    raw_data={"order": order, "detail": detail},
                )
                synced += 1
                await asyncio.sleep(0.05)
            await deactivate_missing_overdue_cases(
                cred.chat_id,
                "zaimis",
                sorted(credential_seen),
                credential_id=cred.id,
            )
        except Exception as ex:
            errors.append(f"Zaimis overdue {cred.login}: {ex}")

    return synced, errors


async def refresh_overdue_cases(bot: Bot) -> None:
    del bot
    log.info("🌙 Overdue sync: refreshing overdue cases from archive endpoints...")
    finkit_synced = 0
    zaimis_synced = 0
    errors: list[str] = []

    try:
        finkit_synced, finkit_errors = await _sync_finkit_overdue_cases()
        errors.extend(finkit_errors)
    except Exception as ex:
        errors.append(f"Finkit overdue global: {ex}")

    try:
        zaimis_synced, zaimis_errors = await _sync_zaimis_overdue_cases()
        errors.extend(zaimis_errors)
    except Exception as ex:
        errors.append(f"Zaimis overdue global: {ex}")

    log.info(
        "🌙 Overdue sync done: finkit=%d, zaimis=%d, errors=%d",
        finkit_synced,
        zaimis_synced,
        len(errors),
    )
    for err in errors[:10]:
        log.warning("Overdue sync error: %s", err)


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
    finkit_by_user: dict[str, int] = {}
    zaimis_by_user: dict[str, int] = {}

    # --- Finkit investments ---
    try:
        creds = await _get_user_credentials("finkit")

        for cred in creds:
            try:
                chat_id = cred.chat_id
                user_tag = _telegram_user_tag(cred)
                fp = await _ensure_finkit_parser(cred)
                if fp is None:
                    errors.append(f"Finkit login failed: {cred.login}")
                    continue

                session = await fp._get_session()
                cookie_str = "; ".join(f"{k}={v}" for k, v in fp._session_cookies.items())
                headers = {"Accept": "application/json", "Referer": "https://finkit.by/", "Cookie": cookie_str}

                # Step 1: collect all investments from list endpoint
                all_investments: list[dict] = []
                page = 1
                relogged = False
                while True:
                    url = f"https://api-p2p.finkit.by/user/investments/?page={page}"
                    async with session.get(url, headers=headers) as resp:
                        if resp.status in (401, 403) and not relogged:
                            fp = await _ensure_finkit_parser(cred, force_login=True)
                            if fp is None:
                                errors.append(f"Finkit re-login failed: {cred.login}")
                                break
                            session = await fp._get_session()
                            cookie_str = "; ".join(f"{k}={v}" for k, v in fp._session_cookies.items())
                            headers = {"Accept": "application/json", "Referer": "https://finkit.by/", "Cookie": cookie_str}
                            relogged = True
                            continue
                        if resp.status != 200:
                            break
                        data = await resp.json()
                    all_investments.extend(data.get("results", []))
                    if not data.get("next"):
                        break
                    page += 1

                log.info("Finkit %s: fetched %d investments from list",
                         cred.login, len(all_investments))

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
                    finkit_by_user[user_tag] = finkit_by_user.get(user_tag, 0) + 1
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
                                                            source=f"finkit_archive_{user_tag}",
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

            except Exception as ex:
                errors.append(f"Finkit {cred.login}: {ex}")
    except Exception as ex:
        errors.append(f"Finkit global: {ex}")

    # --- Zaimis investments → borrowers table ---
    try:
        creds = await _get_user_credentials("zaimis")

        for cred in creds:
            try:
                chat_id = cred.chat_id
                user_tag = _telegram_user_tag(cred)
                zp = await _ensure_zaimis_parser(cred)
                if zp is None:
                    errors.append(f"Zaimis login failed: {cred.login}")
                    continue

                orders = await zp.fetch_investments()
                if zp.needs_reauth:
                    zp = await _ensure_zaimis_parser(cred, force_login=True)
                    if zp is None:
                        errors.append(f"Zaimis re-login failed: {cred.login}")
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
                    zaimis_by_user[user_tag] = zaimis_by_user.get(user_tag, 0) + 1

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
                            source=f"zaimis_archive_{user_tag}",
                        )
                    if pdf_results:
                        log.info("Zaimis PDF: saved %d borrowers with ИН", len(pdf_results))
                except Exception as ex:
                    errors.append(f"Zaimis PDF enrichment: {ex}")

            except Exception as ex:
                errors.append(f"Zaimis {cred.login}: {ex}")
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
        if finkit_by_user:
            lines.append("")
            lines.append("<b>FinKit по Telegram-пользователям:</b>")
            for user_tag, count in sorted(finkit_by_user.items(), key=lambda item: (-item[1], item[0])):
                label = f"@{user_tag}" if not user_tag.startswith("chat_") else user_tag
                lines.append(f"  {escape(label)}: {count}")
        if zaimis_by_user:
            lines.append("")
            lines.append("<b>ЗАЙМись по Telegram-пользователям:</b>")
            for user_tag, count in sorted(zaimis_by_user.items(), key=lambda item: (-item[1], item[0])):
                label = f"@{user_tag}" if not user_tag.startswith("chat_") else user_tag
                lines.append(f"  {escape(label)}: {count}")
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
        refresh_overdue_cases,
        CronTrigger(hour=21, minute=15, timezone="UTC"),  # 00:15 Minsk
        args=[bot], id="midnight_overdue_cases",
        name="Midnight overdue refresh",
        misfire_grace_time=3600,
    )

    _scheduler.add_job(
        midnight_refresh_opi,
        CronTrigger(hour=21, minute=30, timezone="UTC"),  # 00:30 Minsk
        args=[bot], id="midnight_opi",
        name="Midnight OPI refresh",
        misfire_grace_time=3600,
    )

    log.info("Scheduler configured (base=%ds, midnight cron at 00:00/00:15/00:30 Minsk)", BASE)
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
    """Get an already active parser instance, without creating new logins."""
    if service == "kapusta":
        return _kapusta
    elif service == "finkit" and chat_id:
        for p in _finkit_parsers.values():
            if getattr(p, "_owner_chat_id", None) == chat_id:
                return p
        return None
    elif service == "zaimis" and chat_id:
        for p in _zaimis_parsers.values():
            if getattr(p, "_owner_chat_id", None) == chat_id:
                return p
        return None
    return None
