from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
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

__all__ = [
    "BorrowEntry",
    "BorrowerEnrichmentSnapshot",
    "BorrowerIdentityHint",
    "DocumentRefs",
    "EntrySnapshot",
]
