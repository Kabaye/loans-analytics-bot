from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
from typing import Optional


def _serialize_datetime(value: datetime | str | None) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


@dataclass
class EntrySnapshot:
    id: str
    service: str  # 'kapusta' | 'finkit' | 'zaimis'
    request_type: str = "borrow"  # 'borrow' or 'lend'
    amount: float = 0.0
    period_days: int = 0
    interest_day: float = 0.0
    interest_year: float = 0.0
    penalty_interest: float = 0.0
    credit_score: float = 0.0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    profit_gross: float = 0.0
    profit_net: float = 0.0
    amount_return: float = 0.0
    platform_fee_open: float = 0.0
    platform_fee_close: float = 0.0
    note: Optional[str] = None
    status: Optional[str] = None
    loans_count: Optional[int] = None
    loans_count_settled: Optional[int] = None
    loans_count_overdue: Optional[int] = None


@dataclass
class BorrowerIdentityHint:
    full_name: Optional[str] = None
    document_id: Optional[str] = None
    display_name: Optional[str] = None
    display_names: list[str] = field(default_factory=list)
    borrower_user_id: Optional[str] = None

    @property
    def current_display_name(self) -> str | None:
        return self.display_name or (self.display_names[-1] if self.display_names else None)


@dataclass
class BorrowerEnrichmentSnapshot:
    is_income_confirmed: Optional[bool] = None
    is_employed: Optional[bool] = None
    scoring_assessed_at: Optional[datetime | str] = None
    debt_load_score: Optional[float] = None
    has_active_loan: Optional[bool] = None
    has_overdue: Optional[bool] = None
    opi_checked: bool = False
    opi_has_debt: Optional[bool] = None
    opi_debt_amount: Optional[float] = None
    opi_full_name: Optional[str] = None
    opi_checked_at: Optional[datetime | str] = None
    opi_error: Optional[str] = None
    kb_known: bool = False
    kb_total_loans: Optional[int] = None
    kb_cancelled: Optional[int] = None
    kb_has_claims: Optional[int] = None
    kb_avg_rating: Optional[float] = None
    kb_last_rating: Optional[float] = None
    bi_loan_status: Optional[str] = None
    bi_loan_status_details_json: Optional[str] = None
    bi_sum_category: Optional[str] = None
    bi_rating: Optional[float] = None
    enrichment_source: Optional[str] = None
    source_account_tag: Optional[str] = None


@dataclass
class DocumentRefs:
    contract_url: Optional[str] = None
    loan_url: Optional[str] = None


_ENTRY_FIELDS = {
    "id": "id",
    "service": "service",
    "request_type": "request_type",
    "amount": "amount",
    "period_days": "period_days",
    "interest_day": "interest_day",
    "interest_year": "interest_year",
    "penalty_interest": "penalty_interest",
    "credit_score": "credit_score",
    "created_at": "created_at",
    "updated_at": "updated_at",
    "profit_gross": "profit_gross",
    "profit_net": "profit_net",
    "amount_return": "amount_return",
    "platform_fee_open": "platform_fee_open",
    "platform_fee_close": "platform_fee_close",
    "note": "note",
    "status": "status",
    "loans_count": "loans_count",
    "loans_count_settled": "loans_count_settled",
    "loans_count_overdue": "loans_count_overdue",
}
_BORROWER_FIELDS = {
    "full_name": "full_name",
    "document_id": "document_id",
    "display_name": "display_name",
    "display_names": "display_names",
    "borrower_user_id": "borrower_user_id",
}
_ENRICHMENT_FIELDS = {
    "is_income_confirmed": "is_income_confirmed",
    "is_employed": "is_employed",
    "scoring_assessed_at": "scoring_assessed_at",
    "debt_load_score": "debt_load_score",
    "has_active_loan": "has_active_loan",
    "has_overdue": "has_overdue",
    "opi_checked": "opi_checked",
    "opi_has_debt": "opi_has_debt",
    "opi_debt_amount": "opi_debt_amount",
    "opi_full_name": "opi_full_name",
    "opi_checked_at": "opi_checked_at",
    "opi_error": "opi_error",
    "kb_known": "kb_known",
    "kb_total_loans": "kb_total_loans",
    "kb_cancelled": "kb_cancelled",
    "kb_has_claims": "kb_has_claims",
    "kb_avg_rating": "kb_avg_rating",
    "kb_last_rating": "kb_last_rating",
    "bi_loan_status": "bi_loan_status",
    "bi_loan_status_details_json": "bi_loan_status_details_json",
    "bi_sum_category": "bi_sum_category",
    "bi_rating": "bi_rating",
    "enrichment_source": "enrichment_source",
    "source_account_tag": "source_account_tag",
}
_DOCUMENT_FIELDS = {
    "contract_url": "contract_url",
    "loan_url": "loan_url",
}


@dataclass
class BorrowEntry:
    snapshot: EntrySnapshot
    borrower: BorrowerIdentityHint = field(default_factory=BorrowerIdentityHint)
    enrichment: BorrowerEnrichmentSnapshot = field(default_factory=BorrowerEnrichmentSnapshot)
    documents: DocumentRefs = field(default_factory=DocumentRefs)
    raw_data: Optional[dict] = field(default=None, repr=False)

    def __getattr__(self, name: str):
        if name in _ENTRY_FIELDS:
            return getattr(self.snapshot, _ENTRY_FIELDS[name])
        if name in _BORROWER_FIELDS:
            return getattr(self.borrower, _BORROWER_FIELDS[name])
        if name in _ENRICHMENT_FIELDS:
            return getattr(self.enrichment, _ENRICHMENT_FIELDS[name])
        if name in _DOCUMENT_FIELDS:
            return getattr(self.documents, _DOCUMENT_FIELDS[name])
        if name == "current_display_name":
            return self.borrower.current_display_name
        raise AttributeError(name)

    def __setattr__(self, name: str, value):
        if name in {"snapshot", "borrower", "enrichment", "documents", "raw_data"}:
            object.__setattr__(self, name, value)
            return
        if name in _ENTRY_FIELDS and "snapshot" in self.__dict__:
            setattr(self.snapshot, _ENTRY_FIELDS[name], value)
            return
        if name in _BORROWER_FIELDS and "borrower" in self.__dict__:
            setattr(self.borrower, _BORROWER_FIELDS[name], value)
            return
        if name in _ENRICHMENT_FIELDS and "enrichment" in self.__dict__:
            setattr(self.enrichment, _ENRICHMENT_FIELDS[name], value)
            return
        if name in _DOCUMENT_FIELDS and "documents" in self.__dict__:
            setattr(self.documents, _DOCUMENT_FIELDS[name], value)
            return
        object.__setattr__(self, name, value)

    @property
    def current_display_name(self) -> str | None:
        return self.borrower.current_display_name

    def freshness_fingerprint(self) -> str:
        payload = {
            "id": self.id,
            "service": self.service,
            "request_type": self.request_type,
            "amount": self.amount,
            "period_days": self.period_days,
            "interest_day": self.interest_day,
            "interest_year": self.interest_year,
            "credit_score": self.credit_score,
            "created_at": _serialize_datetime(self.created_at),
            "updated_at": _serialize_datetime(self.updated_at),
            "status": self.status,
            "document_id": self.document_id,
            "borrower_user_id": self.borrower_user_id,
            "full_name": self.full_name,
        }
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha1(encoded).hexdigest()

    def to_dict(self) -> dict:
        payload = {
            "id": self.id,
            "service": self.service,
            "request_type": self.request_type,
            "amount": self.amount,
            "period_days": self.period_days,
            "interest_day": self.interest_day,
            "interest_year": self.interest_year,
            "penalty_interest": self.penalty_interest,
            "credit_score": self.credit_score,
            "created_at": _serialize_datetime(self.created_at),
            "updated_at": _serialize_datetime(self.updated_at),
            "profit_gross": self.profit_gross,
            "profit_net": self.profit_net,
            "amount_return": self.amount_return,
            "platform_fee_open": self.platform_fee_open,
            "platform_fee_close": self.platform_fee_close,
            "full_name": self.full_name,
            "document_id": self.document_id,
            "display_name": self.display_name,
            "display_names": list(self.display_names),
            "current_display_name": self.current_display_name,
            "is_income_confirmed": self.is_income_confirmed,
            "is_employed": self.is_employed,
            "scoring_assessed_at": _serialize_datetime(self.scoring_assessed_at),
            "debt_load_score": self.debt_load_score,
            "has_active_loan": self.has_active_loan,
            "has_overdue": self.has_overdue,
            "note": self.note,
            "status": self.status,
            "contract_url": self.contract_url,
            "loans_count": self.loans_count,
            "loans_count_settled": self.loans_count_settled,
            "loans_count_overdue": self.loans_count_overdue,
            "borrower_user_id": self.borrower_user_id,
            "opi_checked": self.opi_checked,
            "opi_has_debt": self.opi_has_debt,
            "opi_debt_amount": self.opi_debt_amount,
            "opi_full_name": self.opi_full_name,
            "opi_checked_at": _serialize_datetime(self.opi_checked_at),
            "opi_error": self.opi_error,
            "kb_known": self.kb_known,
            "kb_total_loans": self.kb_total_loans,
            "kb_cancelled": self.kb_cancelled,
            "kb_has_claims": self.kb_has_claims,
            "kb_avg_rating": self.kb_avg_rating,
            "kb_last_rating": self.kb_last_rating,
            "loan_status_details_json": self.bi_loan_status_details_json,
            "enrichment_source": self.enrichment_source,
            "source_account_tag": self.source_account_tag,
            "loan_url": self.loan_url,
            "components": {
                "snapshot": {
                    "id": self.snapshot.id,
                    "service": self.snapshot.service,
                    "request_type": self.snapshot.request_type,
                    "amount": self.snapshot.amount,
                    "period_days": self.snapshot.period_days,
                    "interest_day": self.snapshot.interest_day,
                    "interest_year": self.snapshot.interest_year,
                    "penalty_interest": self.snapshot.penalty_interest,
                    "credit_score": self.snapshot.credit_score,
                    "created_at": _serialize_datetime(self.snapshot.created_at),
                    "updated_at": _serialize_datetime(self.snapshot.updated_at),
                    "profit_gross": self.snapshot.profit_gross,
                    "profit_net": self.snapshot.profit_net,
                    "amount_return": self.snapshot.amount_return,
                    "platform_fee_open": self.snapshot.platform_fee_open,
                    "platform_fee_close": self.snapshot.platform_fee_close,
                    "note": self.snapshot.note,
                    "status": self.snapshot.status,
                    "loans_count": self.snapshot.loans_count,
                    "loans_count_settled": self.snapshot.loans_count_settled,
                    "loans_count_overdue": self.snapshot.loans_count_overdue,
                },
                "borrower": {
                    "full_name": self.borrower.full_name,
                    "document_id": self.borrower.document_id,
                    "display_name": self.borrower.display_name,
                    "display_names": list(self.borrower.display_names),
                    "borrower_user_id": self.borrower.borrower_user_id,
                },
                "enrichment": {
                    "is_income_confirmed": self.enrichment.is_income_confirmed,
                    "is_employed": self.enrichment.is_employed,
                    "scoring_assessed_at": _serialize_datetime(self.enrichment.scoring_assessed_at),
                    "debt_load_score": self.enrichment.debt_load_score,
                    "has_active_loan": self.enrichment.has_active_loan,
                    "has_overdue": self.enrichment.has_overdue,
                    "opi_checked": self.enrichment.opi_checked,
                    "opi_has_debt": self.enrichment.opi_has_debt,
                    "opi_debt_amount": self.enrichment.opi_debt_amount,
                    "opi_full_name": self.enrichment.opi_full_name,
                    "opi_checked_at": _serialize_datetime(self.enrichment.opi_checked_at),
                    "opi_error": self.enrichment.opi_error,
                    "kb_known": self.enrichment.kb_known,
                    "kb_total_loans": self.enrichment.kb_total_loans,
                    "kb_cancelled": self.enrichment.kb_cancelled,
                    "kb_has_claims": self.enrichment.kb_has_claims,
                    "kb_avg_rating": self.enrichment.kb_avg_rating,
                    "kb_last_rating": self.enrichment.kb_last_rating,
                    "bi_loan_status": self.enrichment.bi_loan_status,
                    "bi_loan_status_details_json": self.enrichment.bi_loan_status_details_json,
                    "bi_sum_category": self.enrichment.bi_sum_category,
                    "bi_rating": self.enrichment.bi_rating,
                    "enrichment_source": self.enrichment.enrichment_source,
                    "source_account_tag": self.enrichment.source_account_tag,
                },
                "documents": {
                    "contract_url": self.documents.contract_url,
                    "loan_url": self.documents.loan_url,
                },
            },
        }
        if self.raw_data:
            payload["raw_data"] = self.raw_data
        return payload


__all__ = [
    "BorrowEntry",
    "BorrowerEnrichmentSnapshot",
    "BorrowerIdentityHint",
    "DocumentRefs",
    "EntrySnapshot",
]
