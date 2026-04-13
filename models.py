from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class BorrowEntry:
    """Unified borrow request from any platform."""
    id: str
    service: str  # 'kapusta' | 'finkit' | 'mongo' | 'zaimis'
    request_type: str = "borrow"  # 'borrow' or 'lend'
    amount: float = 0.0
    period_days: int = 0
    interest_day: float = 0.0
    interest_year: float = 0.0
    penalty_interest: float = 0.0
    credit_score: float = 0.0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # profit calculations
    profit_gross: float = 0.0       # total interest earned
    profit_net: float = 0.0         # after platform fee
    amount_return: float = 0.0      # total borrower pays back
    platform_fee_open: float = 0.0  # fee on loan issuance
    platform_fee_close: float = 0.0 # fee on loan closure

    # borrower info
    full_name: Optional[str] = None
    document_id: Optional[str] = None  # identification number for OPI
    display_name: Optional[str] = None

    # extra flags
    is_income_confirmed: Optional[bool] = None
    is_employed: Optional[bool] = None
    has_active_loan: Optional[bool] = None
    has_overdue: Optional[bool] = None
    note: Optional[str] = None
    status: Optional[str] = None
    contract_url: Optional[str] = None
    loans_count: Optional[int] = None
    loans_count_settled: Optional[int] = None
    loans_count_overdue: Optional[int] = None

    # borrower platform-specific user ID (for cross-loan matching)
    borrower_user_id: Optional[str] = None

    # OPI enrichment
    opi_checked: bool = False
    opi_has_debt: Optional[bool] = None
    opi_debt_amount: Optional[float] = None
    opi_full_name: Optional[str] = None
    opi_error: Optional[str] = None

    # Known borrower enrichment (from investment history)
    kb_known: bool = False
    kb_total_loans: Optional[int] = None
    kb_settled: Optional[int] = None
    kb_overdue: Optional[int] = None
    kb_cancelled: Optional[int] = None
    kb_has_claims: Optional[int] = None
    kb_avg_rating: Optional[float] = None
    kb_last_rating: Optional[float] = None
    kb_total_invested: Optional[float] = None

    # Borrower info card (from borrower_info table / Google Sheets import)
    bi_loan_status: Optional[str] = None    # в срок / просрочка / все плохо
    bi_sum_category: Optional[str] = None   # до 300 / 301-799 / больше 800
    bi_rating: Optional[float] = None

    # Direct link to the loan on the platform
    loan_url: Optional[str] = None

    # Raw API data — preserves all original fields including new/unknown ones
    raw_data: Optional[dict] = field(default=None, repr=False)

    def to_dict(self) -> dict:
        """Serialize to dict for JSON API. Includes raw_data as extra fields."""
        d = {
            "id": self.id,
            "service": self.service,
            "request_type": self.request_type,
            "amount": self.amount,
            "period_days": self.period_days,
            "interest_day": self.interest_day,
            "interest_year": self.interest_year,
            "penalty_interest": self.penalty_interest,
            "credit_score": self.credit_score,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "profit_gross": self.profit_gross,
            "profit_net": self.profit_net,
            "amount_return": self.amount_return,
            "platform_fee_open": self.platform_fee_open,
            "platform_fee_close": self.platform_fee_close,
            "full_name": self.full_name,
            "document_id": self.document_id,
            "display_name": self.display_name,
            "is_income_confirmed": self.is_income_confirmed,
            "is_employed": self.is_employed,
            "has_active_loan": self.has_active_loan,
            "has_overdue": self.has_overdue,
            "note": self.note,
            "status": self.status,
            "contract_url": self.contract_url,
            "loans_count": self.loans_count,
            "opi_checked": self.opi_checked,
            "opi_has_debt": self.opi_has_debt,
            "opi_debt_amount": self.opi_debt_amount,
            "opi_full_name": self.opi_full_name,
            "opi_error": self.opi_error,
            "kb_known": self.kb_known,
            "kb_total_loans": self.kb_total_loans,
            "kb_settled": self.kb_settled,
            "kb_overdue": self.kb_overdue,
            "kb_cancelled": self.kb_cancelled,
            "kb_has_claims": self.kb_has_claims,
            "kb_avg_rating": self.kb_avg_rating,
            "kb_last_rating": self.kb_last_rating,
            "kb_total_invested": self.kb_total_invested,
            "loan_url": self.loan_url,
        }
        if self.raw_data:
            d["raw_data"] = self.raw_data
        return d


@dataclass
class Subscription:
    id: int
    chat_id: int
    service: str
    label: Optional[str] = None
    sum_min: Optional[float] = None
    sum_max: Optional[float] = None
    rating_min: Optional[float] = None
    rating_max: Optional[float] = None
    period_min: Optional[int] = None
    period_max: Optional[int] = None
    interest_min: Optional[float] = None
    interest_max: Optional[float] = None
    require_employed: Optional[bool] = None
    require_income_confirmed: Optional[bool] = None
    is_active: bool = True
    night_paused: bool = False
    min_settled_loans: Optional[int] = None

    def matches(self, entry: BorrowEntry) -> bool:
        if self.sum_min is not None and entry.amount < self.sum_min:
            return False
        if self.sum_max is not None and entry.amount > self.sum_max:
            return False
        if self.rating_min is not None and entry.credit_score < self.rating_min:
            return False
        if self.rating_max is not None and entry.credit_score > self.rating_max:
            return False
        if self.period_min is not None and entry.period_days < self.period_min:
            return False
        if self.period_max is not None and entry.period_days > self.period_max:
            return False
        if self.interest_min is not None and entry.interest_day < self.interest_min:
            return False
        if self.interest_max is not None and entry.interest_day > self.interest_max:
            return False
        if self.require_employed and not entry.is_employed:
            return False
        if self.require_income_confirmed and not entry.is_income_confirmed:
            return False
        if self.min_settled_loans is not None and self.min_settled_loans > 0:
            settled = entry.loans_count_settled or entry.kb_settled or 0
            if settled < self.min_settled_loans:
                return False
        return True


@dataclass
class UserCredentials:
    chat_id: int
    service: str
    login: str
    password: str
    id: int = 0
