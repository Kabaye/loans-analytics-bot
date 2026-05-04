from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Protocol

from bot.domain.borrowers import BorrowEntry
from bot.domain.raw_payloads import extract_raw_payload


def _serialize_datetime(value: datetime | str | None) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _serialize_entry_components(entry: BorrowEntry | dict) -> dict[str, Any]:
    if isinstance(entry, BorrowEntry):
        return {
            "snapshot": {
                "id": entry.snapshot.id,
                "service": entry.snapshot.service,
                "request_type": entry.snapshot.request_type,
                "amount": entry.snapshot.amount,
                "period_days": entry.snapshot.period_days,
                "interest_day": entry.snapshot.interest_day,
                "interest_year": entry.snapshot.interest_year,
                "penalty_interest": entry.snapshot.penalty_interest,
                "credit_score": entry.snapshot.credit_score,
                "created_at": _serialize_datetime(entry.snapshot.created_at),
                "updated_at": _serialize_datetime(entry.snapshot.updated_at),
                "profit_gross": entry.snapshot.profit_gross,
                "profit_net": entry.snapshot.profit_net,
                "amount_return": entry.snapshot.amount_return,
                "platform_fee_open": entry.snapshot.platform_fee_open,
                "platform_fee_close": entry.snapshot.platform_fee_close,
                "note": entry.snapshot.note,
                "status": entry.snapshot.status,
                "loans_count": entry.snapshot.loans_count,
                "loans_count_settled": entry.snapshot.loans_count_settled,
                "loans_count_overdue": entry.snapshot.loans_count_overdue,
            },
            "borrower": {
                "full_name": entry.borrower.full_name,
                "document_id": entry.borrower.document_id,
                "display_name": entry.borrower.display_name,
                "display_names": list(entry.borrower.display_names),
                "borrower_user_id": entry.borrower.borrower_user_id,
            },
            "enrichment": {
                "is_income_confirmed": entry.enrichment.is_income_confirmed,
                "is_employed": entry.enrichment.is_employed,
                "scoring_assessed_at": _serialize_datetime(entry.enrichment.scoring_assessed_at),
                "debt_load_score": entry.enrichment.debt_load_score,
                "has_active_loan": entry.enrichment.has_active_loan,
                "has_overdue": entry.enrichment.has_overdue,
                "opi_checked": entry.enrichment.opi_checked,
                "opi_has_debt": entry.enrichment.opi_has_debt,
                "opi_debt_amount": entry.enrichment.opi_debt_amount,
                "opi_full_name": entry.enrichment.opi_full_name,
                "opi_checked_at": _serialize_datetime(entry.enrichment.opi_checked_at),
                "opi_error": entry.enrichment.opi_error,
                "kb_known": entry.enrichment.kb_known,
                "kb_total_loans": entry.enrichment.kb_total_loans,
                "kb_cancelled": entry.enrichment.kb_cancelled,
                "kb_has_claims": entry.enrichment.kb_has_claims,
                "kb_avg_rating": entry.enrichment.kb_avg_rating,
                "kb_last_rating": entry.enrichment.kb_last_rating,
                "bi_loan_status": entry.enrichment.bi_loan_status,
                "bi_loan_status_details_json": entry.enrichment.bi_loan_status_details_json,
                "bi_sum_category": entry.enrichment.bi_sum_category,
                "bi_rating": entry.enrichment.bi_rating,
                "enrichment_source": entry.enrichment.enrichment_source,
                "source_account_tag": entry.enrichment.source_account_tag,
            },
            "documents": {
                "contract_url": entry.documents.contract_url,
                "loan_url": entry.documents.loan_url,
            },
        }
    payload = dict(entry)
    components = payload.get("components")
    return dict(components) if isinstance(components, dict) else {}


def _serialize_flat_entry(entry: BorrowEntry | dict) -> dict[str, Any]:
    if isinstance(entry, BorrowEntry):
        payload = {
            "id": entry.id,
            "service": entry.service,
            "request_type": entry.request_type,
            "amount": entry.amount,
            "period_days": entry.period_days,
            "interest_day": entry.interest_day,
            "interest_year": entry.interest_year,
            "penalty_interest": entry.penalty_interest,
            "credit_score": entry.credit_score,
            "created_at": _serialize_datetime(entry.created_at),
            "updated_at": _serialize_datetime(entry.updated_at),
            "profit_gross": entry.profit_gross,
            "profit_net": entry.profit_net,
            "amount_return": entry.amount_return,
            "platform_fee_open": entry.platform_fee_open,
            "platform_fee_close": entry.platform_fee_close,
            "full_name": entry.full_name,
            "document_id": entry.document_id,
            "display_name": entry.display_name,
            "display_names": list(entry.display_names),
            "current_display_name": entry.current_display_name,
            "is_income_confirmed": entry.is_income_confirmed,
            "is_employed": entry.is_employed,
            "scoring_assessed_at": _serialize_datetime(entry.scoring_assessed_at),
            "debt_load_score": entry.debt_load_score,
            "has_active_loan": entry.has_active_loan,
            "has_overdue": entry.has_overdue,
            "note": entry.note,
            "status": entry.status,
            "contract_url": entry.contract_url,
            "loans_count": entry.loans_count,
            "loans_count_settled": entry.loans_count_settled,
            "loans_count_overdue": entry.loans_count_overdue,
            "borrower_user_id": entry.borrower_user_id,
            "opi_checked": entry.opi_checked,
            "opi_has_debt": entry.opi_has_debt,
            "opi_debt_amount": entry.opi_debt_amount,
            "opi_full_name": entry.opi_full_name,
            "opi_checked_at": _serialize_datetime(entry.opi_checked_at),
            "opi_error": entry.opi_error,
            "kb_known": entry.kb_known,
            "kb_total_loans": entry.kb_total_loans,
            "kb_cancelled": entry.kb_cancelled,
            "kb_has_claims": entry.kb_has_claims,
            "kb_avg_rating": entry.kb_avg_rating,
            "kb_last_rating": entry.kb_last_rating,
            "loan_status_details_json": entry.bi_loan_status_details_json,
            "enrichment_source": entry.enrichment_source,
            "source_account_tag": entry.source_account_tag,
            "loan_url": entry.loan_url,
        }
    else:
        payload = dict(entry)
        payload.setdefault("current_display_name", payload.get("display_name"))
    return payload


def serialize_export_entry(entry: BorrowEntry | dict) -> dict[str, Any]:
    payload = _serialize_flat_entry(entry)
    payload["components"] = _serialize_entry_components(entry)
    raw_payload = extract_raw_payload(entry)
    if raw_payload is not None:
        payload["raw_data"] = raw_payload
    return payload


def export_entry_payload(entry: BorrowEntry | dict) -> dict:
    return serialize_export_entry(entry)


def serialize_polling_cache_entry(entry: BorrowEntry | dict) -> dict[str, Any]:
    payload = _serialize_flat_entry(entry)
    payload["components"] = _serialize_entry_components(entry)
    raw_payload = extract_raw_payload(entry)
    if raw_payload is not None:
        payload["raw_data"] = raw_payload
    return payload


@dataclass(frozen=True)
class AdminTestEntryView:
    id: str
    amount: float
    period_days: int
    credit_score: float
    interest_day: float
    contract_url: str | None
    document_id: str | None
    full_name: str | None

    @classmethod
    def from_entry(cls, entry: BorrowEntry | dict) -> "AdminTestEntryView":
        if isinstance(entry, BorrowEntry):
            return cls(
                id=str(entry.id),
                amount=float(entry.amount or 0),
                period_days=int(entry.period_days or 0),
                credit_score=float(entry.credit_score or 0),
                interest_day=float(entry.interest_day or 0),
                contract_url=entry.contract_url,
                document_id=entry.document_id,
                full_name=entry.full_name,
            )
        payload = dict(entry)
        return cls(
            id=str(payload.get("id") or ""),
            amount=float(payload.get("amount") or 0),
            period_days=int(payload.get("period_days") or 0),
            credit_score=float(payload.get("credit_score") or 0),
            interest_day=float(payload.get("interest_day") or 0),
            contract_url=payload.get("contract_url"),
            document_id=payload.get("document_id"),
            full_name=payload.get("full_name"),
        )


class SubscriptionEntryView(Protocol):
    amount: float
    credit_score: float
    period_days: int
    interest_day: float
    is_employed: Optional[bool]
    is_income_confirmed: Optional[bool]
    loans_count_settled: Optional[int]


@dataclass(frozen=True)
class NotificationEntryView:
    id: str
    service: str
    request_type: str
    amount: float
    period_days: int
    interest_day: float
    interest_year: float
    penalty_interest: float
    credit_score: float
    created_at: datetime | str | None
    updated_at: datetime | str | None
    full_name: str | None
    document_id: str | None
    display_name: str | None
    display_names: list[str]
    is_income_confirmed: bool | None
    is_employed: bool | None
    scoring_assessed_at: datetime | str | None
    debt_load_score: float | None
    has_active_loan: bool | None
    has_overdue: bool | None
    note: str | None
    status: str | None
    loans_count: int | None
    loans_count_settled: int | None
    loans_count_overdue: int | None
    opi_checked: bool
    opi_has_debt: bool | None
    opi_debt_amount: float | None
    opi_full_name: str | None
    opi_checked_at: datetime | str | None
    opi_error: str | None
    kb_known: bool
    kb_total_loans: int | None
    kb_cancelled: int | None
    kb_has_claims: int | None
    kb_avg_rating: float | None
    kb_last_rating: float | None
    enrichment_source: str | None
    source_account_tag: str | None

    @classmethod
    def from_entry(cls, entry: BorrowEntry | dict) -> "NotificationEntryView":
        if isinstance(entry, BorrowEntry):
            return cls(
                id=str(entry.id),
                service=str(entry.service),
                request_type=str(entry.request_type or "borrow"),
                amount=float(entry.amount or 0),
                period_days=int(entry.period_days or 0),
                interest_day=float(entry.interest_day or 0),
                interest_year=float(entry.interest_year or 0),
                penalty_interest=float(entry.penalty_interest or 0),
                credit_score=float(entry.credit_score or 0),
                created_at=entry.created_at,
                updated_at=entry.updated_at,
                full_name=entry.full_name,
                document_id=entry.document_id,
                display_name=entry.display_name,
                display_names=list(entry.display_names),
                is_income_confirmed=entry.is_income_confirmed,
                is_employed=entry.is_employed,
                scoring_assessed_at=entry.scoring_assessed_at,
                debt_load_score=entry.debt_load_score,
                has_active_loan=entry.has_active_loan,
                has_overdue=entry.has_overdue,
                note=entry.note,
                status=entry.status,
                loans_count=entry.loans_count,
                loans_count_settled=entry.loans_count_settled,
                loans_count_overdue=entry.loans_count_overdue,
                opi_checked=bool(entry.opi_checked),
                opi_has_debt=entry.opi_has_debt,
                opi_debt_amount=entry.opi_debt_amount,
                opi_full_name=entry.opi_full_name,
                opi_checked_at=entry.opi_checked_at,
                opi_error=entry.opi_error,
                kb_known=bool(entry.kb_known),
                kb_total_loans=entry.kb_total_loans,
                kb_cancelled=entry.kb_cancelled,
                kb_has_claims=entry.kb_has_claims,
                kb_avg_rating=entry.kb_avg_rating,
                kb_last_rating=entry.kb_last_rating,
                enrichment_source=entry.enrichment_source,
                source_account_tag=entry.source_account_tag,
            )
        payload = dict(entry)
        return cls(
            id=str(payload.get("id") or ""),
            service=str(payload.get("service") or ""),
            request_type=str(payload.get("request_type") or "borrow"),
            amount=float(payload.get("amount") or 0),
            period_days=int(payload.get("period_days") or 0),
            interest_day=float(payload.get("interest_day") or 0),
            interest_year=float(payload.get("interest_year") or 0),
            penalty_interest=float(payload.get("penalty_interest") or 0),
            credit_score=float(payload.get("credit_score") or 0),
            created_at=payload.get("created_at"),
            updated_at=payload.get("updated_at"),
            full_name=payload.get("full_name"),
            document_id=payload.get("document_id"),
            display_name=payload.get("display_name"),
            display_names=[str(item) for item in payload.get("display_names") or [] if str(item or "").strip()],
            is_income_confirmed=payload.get("is_income_confirmed"),
            is_employed=payload.get("is_employed"),
            scoring_assessed_at=payload.get("scoring_assessed_at"),
            debt_load_score=payload.get("debt_load_score"),
            has_active_loan=payload.get("has_active_loan"),
            has_overdue=payload.get("has_overdue"),
            note=payload.get("note"),
            status=payload.get("status"),
            loans_count=payload.get("loans_count"),
            loans_count_settled=payload.get("loans_count_settled"),
            loans_count_overdue=payload.get("loans_count_overdue"),
            opi_checked=bool(payload.get("opi_checked")),
            opi_has_debt=payload.get("opi_has_debt"),
            opi_debt_amount=payload.get("opi_debt_amount"),
            opi_full_name=payload.get("opi_full_name"),
            opi_checked_at=payload.get("opi_checked_at"),
            opi_error=payload.get("opi_error"),
            kb_known=bool(payload.get("kb_known")),
            kb_total_loans=payload.get("kb_total_loans"),
            kb_cancelled=payload.get("kb_cancelled"),
            kb_has_claims=payload.get("kb_has_claims"),
            kb_avg_rating=payload.get("kb_avg_rating"),
            kb_last_rating=payload.get("kb_last_rating"),
            enrichment_source=payload.get("enrichment_source"),
            source_account_tag=payload.get("source_account_tag"),
        )

    @property
    def current_display_name(self) -> str | None:
        return self.display_name or (self.display_names[-1] if self.display_names else None)


__all__ = [
    "AdminTestEntryView",
    "NotificationEntryView",
    "SubscriptionEntryView",
    "export_entry_payload",
    "serialize_export_entry",
    "serialize_polling_cache_entry",
]
