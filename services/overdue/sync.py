from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from bot.integrations.geolocation_client import lookup_belarus_zip
from bot.repositories.borrowers import (
    lookup_borrower,
    refresh_borrower_statuses,
    upsert_borrower,
)
from bot.repositories.overdue import (
    clear_finkit_suspect_address,
    deactivate_missing_overdue_cases,
    update_overdue_case_contacts,
    upsert_overdue_case,
)
from bot.services.base.providers import (
    ensure_finkit_parser,
    list_service_credentials,
    telegram_user_tag,
)
from bot.services.zaimis_sync import sync_zaimis_account

log = logging.getLogger(__name__)


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


async def sync_finkit_overdue_cases() -> tuple[int, list[str]]:
    synced = 0
    errors: list[str] = []
    creds = await list_service_credentials("finkit")

    for cred in creds:
        try:
            parser = await ensure_finkit_parser(cred)
            if parser is None:
                errors.append(f"Finkit login failed: {cred.login}")
                continue

            items = await parser.fetch_investments()
            if parser.needs_reauth:
                parser = await ensure_finkit_parser(cred, force_login=True)
                if parser is None:
                    errors.append(f"Finkit re-login failed: {cred.login}")
                    continue
                items = await parser.fetch_investments()

            credential_seen: set[str] = set()
            touched_document_ids: set[str] = set()
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
                due_at = _coalesce(
                    detail.get("maturity_date"),
                    detail.get("payment_date"),
                    detail.get("due_at"),
                    detail.get("due_date"),
                    item.get("payment_date"),
                    item.get("due_at"),
                    item.get("due_date"),
                )
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
                claim_address = None
                claim_zip = None
                claim_phone = None
                claim_email = None
                claims = detail.get("claims") or []
                claim_document_url = next((claim.get("document_url") for claim in claims if claim.get("document_url")), None)
                if claim_document_url:
                    claim_pdf_bytes = await parser.fetch_contract_pdf(claim_document_url)
                    if claim_pdf_bytes:
                        claim_data = parser.parse_claim_document_pdf(claim_pdf_bytes)
                        claim_address = _coalesce(claim_data.get("debtor_address"))
                        claim_phone = _coalesce(claim_data.get("debtor_phone"))
                        claim_email = _coalesce(claim_data.get("debtor_email"))
                        if claim_address:
                            claim_zip = await lookup_belarus_zip(claim_address)

                resolved_document_id = (cached or {}).get("document_id")
                sync_full_name = (cached or {}).get("full_name") or _coalesce(detail.get("borrower_full_name"), item.get("borrower_full_name"))
                sync_display_name = _coalesce(detail.get("borrower_short_name"), item.get("borrower_short_name"))
                if borrower_user_id and (sync_full_name or sync_display_name or resolved_document_id):
                    await upsert_borrower(
                        service="finkit",
                        borrower_user_id=borrower_user_id,
                        full_name=sync_full_name,
                        document_id=resolved_document_id,
                        source="finkit_borrow",
                        display_name=sync_display_name,
                    )

                case_id = await upsert_overdue_case(
                    chat_id=cred.chat_id,
                    credential_id=cred.id,
                    service="finkit",
                    external_id=external_id,
                    loan_id=str(_coalesce(detail.get("loan"), item.get("loan")) or "") or None,
                    loan_number=str(_coalesce(detail.get("loan_number"), item.get("loan_number")) or "") or None,
                    account_label=cred.login,
                    borrower_user_id=borrower_user_id or None,
                    document_id=resolved_document_id,
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
                    raw_data={"list": item, "detail": detail},
                )
                await clear_finkit_suspect_address(case_id, address, zip_code)
                if claim_address or claim_zip or claim_phone or claim_email or detail.get("borrower_phone_number") or detail.get("borrower_email"):
                    await update_overdue_case_contacts(
                        case_id,
                        cred.chat_id,
                        borrower_address=claim_address,
                        borrower_zip=claim_zip,
                        borrower_phone=claim_phone or detail.get("borrower_phone_number"),
                        borrower_email=claim_email or detail.get("borrower_email"),
                        contact_source="finkit_investment_detail",
                        source=f"finkit_investment_detail_{telegram_user_tag(cred)}",
                    )
                    if resolved_document_id:
                        touched_document_ids.add(resolved_document_id)
                synced += 1
                await asyncio.sleep(0.05)
            await deactivate_missing_overdue_cases(
                cred.chat_id,
                "finkit",
                sorted(credential_seen),
                credential_id=cred.id,
            )
            await refresh_borrower_statuses(touched_document_ids)
        except Exception as exc:
            errors.append(f"Finkit overdue {cred.login}: {exc}")

    return synced, errors


async def sync_zaimis_overdue_cases() -> tuple[int, list[str]]:
    synced = 0
    errors: list[str] = []
    creds = await list_service_credentials("zaimis")

    for cred in creds:
        try:
            result = await sync_zaimis_account(
                cred,
                include_pdf=False,
                sync_overdue_cases=True,
            )
            synced += result.synced_overdue_cases
        except Exception as exc:
            errors.append(f"Zaimis overdue {cred.login}: {exc}")

    return synced, errors


async def refresh_overdue_snapshot() -> tuple[int, int, list[str]]:
    finkit_synced = 0
    zaimis_synced = 0
    errors: list[str] = []

    try:
        finkit_synced, finkit_errors = await sync_finkit_overdue_cases()
        errors.extend(finkit_errors)
    except Exception as exc:
        errors.append(f"Finkit overdue global: {exc}")

    try:
        zaimis_synced, zaimis_errors = await sync_zaimis_overdue_cases()
        errors.extend(zaimis_errors)
    except Exception as exc:
        errors.append(f"Zaimis overdue global: {exc}")

    return finkit_synced, zaimis_synced, errors


__all__ = [
    "refresh_overdue_snapshot",
    "sync_finkit_overdue_cases",
    "sync_zaimis_overdue_cases",
]
