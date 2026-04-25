from __future__ import annotations

import json
from datetime import datetime

from bot.domain.credentials import UserCredentials
from bot.integrations.geolocation_client import lookup_belarus_zip, lookup_belarus_zip_details
from bot.repositories.credentials import get_credential_by_id
from bot.repositories.overdue import get_overdue_case, update_overdue_case_contacts, upsert_overdue_case
from bot.services.base.providers import ensure_finkit_parser


def _coalesce(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _safe_float(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_case_payload(case: dict) -> dict:
    raw = case.get("raw_data")
    if isinstance(raw, dict):
        return dict(raw)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _claim_sort_key(claim: dict) -> tuple[str, str, str]:
    return (
        str(claim.get("claim_date") or ""),
        str(claim.get("sent_at") or ""),
        str(claim.get("id") or ""),
    )


def get_latest_finkit_claim(case: dict) -> dict | None:
    payload = _parse_case_payload(case)
    detail = payload.get("detail") or {}
    claims = detail.get("claims") or payload.get("claims") or []
    if not claims:
        return None
    rows = sorted((claim for claim in claims if isinstance(claim, dict)), key=_claim_sort_key, reverse=True)
    return rows[0] if rows else None


async def resolve_belarus_zip(address: str | None) -> str | None:
    return await lookup_belarus_zip(address)


async def resolve_belarus_zip_details(address: str | None) -> dict | None:
    return await lookup_belarus_zip_details(address)


async def _get_finkit_parser_for_case(case: dict):
    credential_row = await get_credential_by_id(int(case["credential_id"]), case["chat_id"])
    if not credential_row:
        return None
    cred = UserCredentials(
        id=int(credential_row["id"]),
        chat_id=int(credential_row["chat_id"]),
        service=str(credential_row["service"]),
        login=str(credential_row["login"]),
        password=str(credential_row["password"]),
    )
    return await ensure_finkit_parser(cred)


async def _persist_finkit_detail(case: dict, payload: dict, detail: dict) -> None:
    principal = _safe_float(_coalesce(detail.get("principal_outstanding"), case.get("principal_outstanding"), case.get("amount")))
    accrued = _safe_float(_coalesce(detail.get("accrued_percent"), case.get("accrued_percent")))
    fine = _safe_float(_coalesce(detail.get("fine_outstanding"), case.get("fine_outstanding")))
    total_due = _safe_float(_coalesce(detail.get("total_due"), case.get("total_due")))
    if total_due is None:
        total_due = sum(part or 0 for part in (principal, accrued, fine)) or None

    payload["detail"] = detail
    await upsert_overdue_case(
        chat_id=int(case["chat_id"]),
        credential_id=int(case["credential_id"]),
        service="finkit",
        external_id=str(case["external_id"]),
        loan_id=str(_coalesce(detail.get("loan"), case.get("loan_id")) or "") or None,
        loan_number=str(_coalesce(detail.get("loan_number"), case.get("loan_number")) or "") or None,
        issued_at=_coalesce(detail.get("created"), case.get("issued_at")),
        due_at=_coalesce(detail.get("payment_date"), detail.get("due_at"), detail.get("due_date"), case.get("due_at")),
        amount=_safe_float(_coalesce(detail.get("amount"), case.get("amount"))),
        principal_outstanding=principal,
        accrued_percent=accrued,
        fine_outstanding=fine,
        total_due=total_due,
        status=str(_coalesce(detail.get("status"), case.get("status")) or "") or None,
        contract_url=_coalesce(detail.get("latest_contract_url"), case.get("contract_url")),
        raw_data=payload,
    )


async def _refresh_finkit_contacts(case: dict, parser, detail: dict) -> None:
    claims = detail.get("claims") or []
    claim_document_url = next((claim.get("document_url") for claim in claims if claim.get("document_url")), None)
    if claim_document_url:
        claim_pdf_bytes = await parser.fetch_contract_pdf(claim_document_url)
        if claim_pdf_bytes:
            claim_data = parser.parse_claim_document_pdf(claim_pdf_bytes)
            postal_lookup = await resolve_belarus_zip_details(claim_data.get("debtor_address"))
            zipcode = str(postal_lookup.get("postcode")) if postal_lookup and postal_lookup.get("postcode") else case.get("borrower_zip")
            await update_overdue_case_contacts(
                int(case["id"]),
                int(case["chat_id"]),
                borrower_address=claim_data.get("debtor_address"),
                borrower_zip=zipcode,
                borrower_phone=claim_data.get("debtor_phone"),
                borrower_email=claim_data.get("debtor_email"),
                postal_lookup=postal_lookup,
                contact_source="finkit_claim_pdf",
            )
            return

    if detail.get("borrower_phone_number") or detail.get("borrower_email"):
        await update_overdue_case_contacts(
            int(case["id"]),
            int(case["chat_id"]),
            borrower_phone=detail.get("borrower_phone_number"),
            borrower_email=detail.get("borrower_email"),
            contact_source="finkit_investment_detail",
        )


async def _refresh_finkit_document_id(case: dict, parser, payload: dict, detail: dict) -> None:
    if case.get("document_id") and case.get("full_name"):
        return
    contract_url = _coalesce(detail.get("latest_contract_url"), case.get("contract_url"))
    if not contract_url:
        return
    contract_pdf = await parser.fetch_contract_pdf(str(contract_url))
    if not contract_pdf:
        return
    pdf_full_name, pdf_document_id = parser.parse_borrower_from_contract_pdf(contract_pdf)
    if not pdf_full_name and not pdf_document_id:
        return
    await upsert_overdue_case(
        chat_id=int(case["chat_id"]),
        credential_id=int(case["credential_id"]),
        service="finkit",
        external_id=str(case["external_id"]),
        full_name=pdf_full_name,
        document_id=pdf_document_id,
        contract_url=str(contract_url),
        raw_data=payload,
    )


async def refresh_finkit_case_for_claim(case: dict, *, create_pretrial_claim: bool = False) -> tuple[dict, dict | None]:
    if case.get("service") != "finkit" or not case.get("credential_id"):
        return case, None

    parser = await _get_finkit_parser_for_case(case)
    if parser is None:
        return case, get_latest_finkit_claim(case)

    try:
        payload = _parse_case_payload(case)
        detail = await parser.fetch_investment_detail(str(case.get("external_id") or "")) or payload.get("detail") or {}

        if create_pretrial_claim and detail.get("can_generate_claim") is not False:
            claims = await parser.create_pretrial_claims(str(case.get("external_id") or ""))
            refreshed_detail = await parser.fetch_investment_detail(str(case.get("external_id") or ""))
            if refreshed_detail:
                detail = refreshed_detail
            elif claims:
                detail["claims"] = claims

        await _persist_finkit_detail(case, payload, detail)
        refreshed = await get_overdue_case(int(case["id"]), int(case["chat_id"])) or case

        await _refresh_finkit_contacts(refreshed, parser, detail)
        await _refresh_finkit_document_id(refreshed, parser, payload, detail)

        refreshed = await get_overdue_case(int(case["id"]), int(case["chat_id"])) or refreshed
        return refreshed, get_latest_finkit_claim(refreshed)
    finally:
        await parser.close()


async def enrich_finkit_case_from_claims(case: dict) -> dict:
    if case.get("service") != "finkit" or not case.get("credential_id"):
        return case
    if case.get("borrower_address") and case.get("borrower_zip") and case.get("document_id"):
        return case
    refreshed, _ = await refresh_finkit_case_for_claim(case, create_pretrial_claim=False)
    return refreshed


async def send_finkit_pretrial_claim(case: dict, claim_id: str) -> tuple[bool, dict]:
    if case.get("service") != "finkit" or not case.get("credential_id") or not claim_id:
        return False, case

    parser = await _get_finkit_parser_for_case(case)
    if parser is None:
        return False, case

    try:
        result = await parser.send_pretrial_claim(claim_id)
        if not result:
            return False, case
    finally:
        await parser.close()

    refreshed, _ = await refresh_finkit_case_for_claim(case, create_pretrial_claim=False)
    return True, refreshed


__all__ = [
    "enrich_finkit_case_from_claims",
    "get_latest_finkit_claim",
    "refresh_finkit_case_for_claim",
    "resolve_belarus_zip",
    "resolve_belarus_zip_details",
    "send_finkit_pretrial_claim",
]
