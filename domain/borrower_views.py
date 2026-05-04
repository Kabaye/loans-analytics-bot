from __future__ import annotations

from dataclasses import dataclass

from bot.domain.borrowers import BorrowEntry


def export_entry_payload(entry: BorrowEntry | dict) -> dict:
    if isinstance(entry, BorrowEntry):
        return entry.to_dict()
    return dict(entry)


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


__all__ = ["AdminTestEntryView", "export_entry_payload"]
