from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from bot.domain.borrower_views import SubscriptionEntryView


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
    created_at: Optional[datetime] = None

    def matches(self, entry: SubscriptionEntryView) -> bool:
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
            settled = entry.loans_count_settled or 0
            if settled < self.min_settled_loans:
                return False
        return True


__all__ = ["Subscription"]
