from __future__ import annotations

import asyncio

from bot.domain.borrower_views import AdminTestEntryView
from bot.config import ADMIN_CHAT_ID
from bot.repositories.borrowers import get_borrowers_stats
from bot.repositories.credentials import get_first_credential_owner_chat_id
from bot.repositories.opi_cache import get_missing_opi_candidates
from bot.repositories.settings import (
    clear_api_change_alerts as _repo_clear_api_change_alerts,
    delete_api_change_alert as _repo_delete_api_change_alert,
    get_all_site_settings as _repo_get_all_site_settings,
    get_api_change_alert as _repo_get_api_change_alert,
    list_api_change_alerts as _repo_list_api_change_alerts,
    update_site_setting as _repo_update_site_setting,
)
from bot.repositories.users import (
    ensure_user as _repo_ensure_user,
    get_user as _repo_get_user,
    is_chat_admin,
    list_users as _repo_list_users,
    list_users_by_access as _repo_list_users_by_access,
    set_user_admin as _repo_set_user_admin,
    set_user_allowed as _repo_set_user_allowed,
)
from bot.services.base.providers import get_export_parsers

MAIN_OWNER_USERNAME = "kabaye"


async def is_admin(chat_id: int) -> bool:
    return await is_chat_admin(chat_id, ADMIN_CHAT_ID)


def is_main_owner_row(row) -> bool:
    username = str(row["username"] or "").strip().lstrip("@").lower() if "username" in row.keys() else ""
    chat_id = row["chat_id"] if "chat_id" in row.keys() else None
    return chat_id == ADMIN_CHAT_ID or username == MAIN_OWNER_USERNAME


async def get_admin_users():
    return await _repo_list_users()


async def add_allowed_user(chat_id: int) -> None:
    await _repo_ensure_user(chat_id)
    await _repo_set_user_allowed(chat_id, True)


async def allow_user(chat_id: int):
    await _repo_set_user_allowed(chat_id, True)
    return await _repo_get_user(chat_id)


async def block_user(chat_id: int):
    row = await _repo_get_user(chat_id)
    await _repo_set_user_allowed(chat_id, False)
    return row


async def promote_user(chat_id: int):
    await _repo_set_user_admin(chat_id, True)
    return await _repo_get_user(chat_id)


async def demote_user(chat_id: int):
    row = await _repo_get_user(chat_id)
    await _repo_set_user_admin(chat_id, False)
    return row


async def get_user_row(chat_id: int):
    return await _repo_get_user(chat_id)


async def list_filtered_users(
    is_allowed: int,
    *,
    exclude_chat_id: int | None = None,
    exclude_main_owner: bool = False,
    require_admin: int | None = None,
):
    rows = await _repo_list_users_by_access(is_allowed)
    filtered = list(rows)
    if exclude_chat_id is not None:
        filtered = [row for row in filtered if row["chat_id"] != exclude_chat_id]
    if exclude_main_owner:
        filtered = [row for row in filtered if not is_main_owner_row(row)]
    if require_admin is not None:
        filtered = [row for row in filtered if int(row["is_admin"] or 0) == require_admin]
    return filtered


async def get_saved_api_alerts(limit: int = 30):
    return await _repo_list_api_change_alerts(limit=limit)


async def get_saved_api_alert(alert_id: int):
    return await _repo_get_api_change_alert(alert_id)


async def remove_api_alert(alert_id: int) -> None:
    await _repo_delete_api_change_alert(alert_id)


async def clear_all_api_alerts() -> None:
    await _repo_clear_api_change_alerts()


async def get_polling_settings():
    return await _repo_get_all_site_settings()


async def toggle_polling(service: str) -> None:
    settings = await get_all_site_settings()
    current = next((row for row in settings if row["service"] == service), None)
    if current:
        new_value = 0 if current["polling_enabled"] else 1
        await update_site_setting(service, polling_enabled=new_value)


async def set_poll_interval(service: str, seconds: int) -> None:
    await _repo_update_site_setting(service, poll_interval=seconds)


async def get_test_parser(service: str, requester_chat_id: int):
    target_chat_id = requester_chat_id
    if service in ("finkit", "zaimis"):
        owner_chat_id = await get_first_credential_owner_chat_id(service)
        if owner_chat_id is None:
            return None
        target_chat_id = owner_chat_id

    parsers = await get_export_parsers(service, target_chat_id)
    return parsers[0] if parsers else None


async def get_test_notification_entry(service: str, requester_chat_id: int):
    parser = await get_test_parser(service, requester_chat_id)
    if parser is None:
        return None
    entries = await parser.fetch_borrows()
    return entries[0] if entries else None


async def run_full_app_test(requester_chat_id: int) -> list[str]:
    from bot.integrations.opi_client import OPIChecker

    results: list[str] = []

    try:
        parser = await get_test_parser("kapusta", requester_chat_id)
        if parser is None:
            entries = []
            results.append("🥬 <b>Kapusta</b>: ❌ parser unavailable")
        else:
            entries = await asyncio.wait_for(parser.fetch_borrows(), timeout=30)
            results.append(f"🥬 <b>Kapusta</b>: {len(entries)} заявок")
        if entries:
            entry = AdminTestEntryView.from_entry(entries[0])
            results.append(f"  └ первая: {entry.amount:.0f} BYN")
    except Exception as exc:
        results.append(f"🥬 <b>Kapusta</b>: ❌ {exc}")

    try:
        parser = await get_test_parser("finkit", requester_chat_id)
        if parser is None:
            results.append("🔵 <b>FinKit</b>: ⚠️ Нет credentials")
        else:
            entries = await parser.fetch_borrows()
            results.append(f"🔵 <b>FinKit</b>: {len(entries)} заявок")
            if entries:
                entry = AdminTestEntryView.from_entry(entries[0])
                results.append(
                    f"  └ #{entry.id}: {entry.amount:.0f} BYN, {entry.period_days}д, "
                    f"рейт {entry.credit_score:.0f}, {entry.interest_day:.2f}%/д"
                )
                to_enrich = [candidate for candidate in entries if getattr(candidate, 'contract_url', None)][:1]
                if to_enrich:
                    await parser.enrich_with_pdf(to_enrich)
                    enriched = AdminTestEntryView.from_entry(to_enrich[0])
                    results.append(f"  └ PDF: ФИО={enriched.full_name or '—'}, ИН={enriched.document_id or '—'}")
                    if enriched.document_id:
                        opi = OPIChecker()
                        try:
                            result = await opi.check(enriched.document_id, use_cache=False)
                            if result.error:
                                results.append(f"  └ ОПИ: ⚠️ {result.error}")
                            elif result.has_debt:
                                results.append(
                                    f"  └ ОПИ: 🔴 ДОЛГ {result.debt_amount:.2f} BYN ({result.full_name or '—'})"
                                )
                            else:
                                results.append("  └ ОПИ: 🟢 Нет задолженности")
                        finally:
                            await opi.close()
                    else:
                        results.append("  └ ОПИ: ⏭ нет ИН")
                else:
                    results.append("  └ PDF: нет contract_url")
    except Exception as exc:
        results.append(f"🔵 <b>FinKit</b>: ❌ {exc}")

    try:
        parser = await get_test_parser("zaimis", requester_chat_id)
        if parser is None:
            results.append("🟪 <b>ЗАЙМись</b>: ⚠️ Нет credentials")
        else:
            entries = await parser.fetch_borrows()
            results.append(f"🟪 <b>ЗАЙМись</b>: {len(entries)} заявок")
            if entries:
                entry = AdminTestEntryView.from_entry(entries[0])
                results.append(
                    f"  └ #{entry.id[:8]}…: {entry.amount:.0f} BYN, {entry.period_days}д, "
                    f"рейт {entry.credit_score:.0f}, {entry.interest_day:.2f}%/д"
                )
    except Exception as exc:
        results.append(f"🟪 <b>ЗАЙМись</b>: ❌ {exc}")

    try:
        stats = await get_borrowers_stats()
        missing_opi = await get_missing_opi_candidates(min_age_days=10, limit=500)
        if stats and (stats.get("total") or stats.get("mappings")):
            results.append(
                f"\n📊 <b>Карточки заёмщиков (borrower_info)</b>:\n"
                f"  Всего карточек: {stats.get('total', 0)}\n"
                f"  OPI проверено: {stats.get('opi_checked', 0)}\n"
                f"  С долгами: {stats.get('with_debt', 0)}\n"
                f"  С инвестициями: {stats.get('with_investments', 0)}\n"
                f"  Маппингов (borrowers): {stats.get('mappings', 0)}\n"
                f"  С ИН: {stats.get('with_document', 0)}\n"
                f"  Нет OPI 10+ дней: {len(missing_opi)}"
            )
        else:
            results.append("\n📊 <b>Карточки заёмщиков</b>: пусто")
    except Exception as exc:
        results.append(f"\n📊 <b>Карточки заёмщиков</b>: ❌ {exc}")

    return results


async def get_missing_opi_rows(*, min_age_days: int, limit: int):
    return await get_missing_opi_candidates(min_age_days=min_age_days, limit=limit)


async def list_users():
    return await get_admin_users()


async def ensure_user(chat_id: int):
    await _repo_ensure_user(chat_id)


async def get_user(chat_id: int):
    return await get_user_row(chat_id)


async def list_users_by_access(is_allowed: int):
    return await list_filtered_users(is_allowed)


async def set_user_allowed(chat_id: int, allowed: bool):
    if allowed:
        await allow_user(chat_id)
    else:
        await block_user(chat_id)


async def set_user_admin(chat_id: int, is_admin_value: bool):
    if is_admin_value:
        await promote_user(chat_id)
    else:
        await demote_user(chat_id)


async def list_api_change_alerts(limit: int = 30):
    return await get_saved_api_alerts(limit=limit)


async def get_api_change_alert(alert_id: int):
    return await get_saved_api_alert(alert_id)


async def delete_api_change_alert(alert_id: int):
    await remove_api_alert(alert_id)


async def clear_api_change_alerts():
    await clear_all_api_alerts()


async def get_all_site_settings():
    return await get_polling_settings()


async def update_site_setting(service: str, *, polling_enabled: int | None = None, poll_interval: int | None = None):
    if polling_enabled is not None:
        settings = await get_polling_settings()
        current = next((row for row in settings if row["service"] == service), None)
        if current and int(bool(current["polling_enabled"])) != int(bool(polling_enabled)):
            await toggle_polling(service)
    if poll_interval is not None:
        await set_poll_interval(service, poll_interval)


__all__ = [
    "add_allowed_user",
    "allow_user",
    "block_user",
    "clear_all_api_alerts",
    "clear_api_change_alerts",
    "delete_api_change_alert",
    "demote_user",
    "ensure_user",
    "get_all_site_settings",
    "get_admin_users",
    "get_api_change_alert",
    "get_missing_opi_rows",
    "get_polling_settings",
    "get_saved_api_alert",
    "get_saved_api_alerts",
    "get_test_notification_entry",
    "get_test_parser",
    "get_user",
    "get_user_row",
    "get_borrowers_stats",
    "get_missing_opi_candidates",
    "is_admin",
    "is_main_owner_row",
    "list_api_change_alerts",
    "list_filtered_users",
    "list_users",
    "list_users_by_access",
    "promote_user",
    "remove_api_alert",
    "run_full_app_test",
    "set_poll_interval",
    "set_user_admin",
    "set_user_allowed",
    "toggle_polling",
    "update_site_setting",
]
