from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class BorrowEntry:
    """Unified borrow request from any platform."""

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

    full_name: Optional[str] = None
    document_id: Optional[str] = None
    display_name: Optional[str] = None

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

    borrower_user_id: Optional[str] = None

    opi_checked: bool = False
    opi_has_debt: Optional[bool] = None
    opi_debt_amount: Optional[float] = None
    opi_full_name: Optional[str] = None
    opi_checked_at: Optional[datetime | str] = None
    opi_error: Optional[str] = None

    kb_known: bool = False
    kb_total_loans: Optional[int] = None
    kb_settled: Optional[int] = None
    kb_overdue: Optional[int] = None
    kb_cancelled: Optional[int] = None
    kb_has_claims: Optional[int] = None
    kb_avg_rating: Optional[float] = None
    kb_last_rating: Optional[float] = None
    kb_total_invested: Optional[float] = None

    bi_loan_status: Optional[str] = None
    bi_sum_category: Optional[str] = None
    bi_rating: Optional[float] = None

    loan_url: Optional[str] = None
    raw_data: Optional[dict] = field(default=None, repr=False)

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
            "opi_checked_at": self.opi_checked_at.isoformat() if isinstance(self.opi_checked_at, datetime) else self.opi_checked_at,
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
            payload["raw_data"] = self.raw_data
        return payload


__all__ = ["BorrowEntry"]
