from __future__ import annotations

import json

from bot.domain.credentials import UserCredentials
from bot.integrations.geolocation_client import lookup_belarus_zip
from bot.repositories.credentials import get_credential_by_id
from bot.repositories.overdue import get_overdue_case, update_overdue_case_contacts, upsert_overdue_case
from bot.services.base.providers import ensure_finkit_parser


async def resolve_belarus_zip(address: str | None) -> str | None:
    return await lookup_belarus_zip(address)


async def enrich_finkit_case_from_claims(case: dict) -> dict:
    if case.get("service") != "finkit" or not case.get("credential_id"):
        return case
    if case.get("borrower_address") and case.get("borrower_zip") and case.get("document_id"):
        return case

    credential_row = await get_credential_by_id(int(case["credential_id"]), case["chat_id"])
    if not credential_row:
        return case
    cred = UserCredentials(
        id=int(credential_row["id"]),
        chat_id=int(credential_row["chat_id"]),
        service=str(credential_row["service"]),
        login=str(credential_row["login"]),
        password=str(credential_row["password"]),
    )
    parser = await ensure_finkit_parser(cred)
    if parser is None:
        return case

    raw = case.get("raw_data")
    if isinstance(raw, dict):
        payload = dict(raw)
    else:
        try:
            payload = json.loads(raw) if raw else {}
        except Exception:
            payload = {}
    detail = payload.get("detail") or {}
    claims = detail.get("claims") or []
    if not claims:
        claims = await parser.fetch_claims(str(case.get("external_id") or ""), create_if_missing=True)
        if claims:
            detail["claims"] = claims
            payload["detail"] = detail
            await upsert_overdue_case(
                chat_id=int(case["chat_id"]),
                credential_id=int(case["credential_id"]),
                service="finkit",
                external_id=str(case["external_id"]),
                raw_data=payload,
            )

    claim_document_url = next((claim.get("document_url") for claim in claims if claim.get("document_url")), None)
    if claim_document_url:
        claim_pdf_bytes = await parser.fetch_contract_pdf(claim_document_url)
        if claim_pdf_bytes:
            claim_data = parser.parse_claim_document_pdf(claim_pdf_bytes)
            zipcode = case.get("borrower_zip") or await resolve_belarus_zip(claim_data.get("debtor_address"))
            await update_overdue_case_contacts(
                int(case["id"]),
                int(case["chat_id"]),
                borrower_address=claim_data.get("debtor_address"),
                borrower_zip=zipcode,
                borrower_phone=claim_data.get("debtor_phone"),
                borrower_email=claim_data.get("debtor_email"),
            )

    if not case.get("document_id") and case.get("contract_url"):
        contract_pdf = await parser.fetch_contract_pdf(str(case["contract_url"]))
        if contract_pdf:
            pdf_full_name, pdf_document_id = parser.parse_borrower_from_contract_pdf(contract_pdf)
            if pdf_full_name or pdf_document_id:
                await upsert_overdue_case(
                    chat_id=int(case["chat_id"]),
                    credential_id=int(case["credential_id"]),
                    service="finkit",
                    external_id=str(case["external_id"]),
                    full_name=pdf_full_name,
                    document_id=pdf_document_id,
                    raw_data=payload,
                )

    refreshed = await get_overdue_case(int(case["id"]), int(case["chat_id"]))
    return refreshed or case


__all__ = ["enrich_finkit_case_from_claims", "resolve_belarus_zip"]
