from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import re

from bot.domain.credentials import UserCredentials
from bot.integrations.parsers.zaimis import (
    ZAIMIS_STATUS_LABELS,
    ZaimisOrderStatus,
    ZaimisParser,
    resolve_order_borrower_identity,
)
from bot.repositories.borrowers import (
    lookup_borrower,
    refresh_borrower_statuses,
    upsert_borrower,
    upsert_borrower_contacts,
    upsert_borrower_from_investment,
    upsert_borrower_info,
)
from bot.repositories.overdue import (
    deactivate_missing_overdue_cases,
    update_overdue_case_contacts,
    upsert_overdue_case,
)
from bot.services.base.providers import ensure_zaimis_parser, telegram_user_tag
from bot.services.borrowers.enrichment import list_borrower_ids_with_documents
from bot.utils.borrower_addresses import (
    merge_primary_borrower_address,
    normalize_borrower_addresses,
    primary_borrower_address,
)

log = logging.getLogger(__name__)

_ZIP_RE = re.compile(r"\b(\d{6})\b")
_PRE_CONTRACT_STATUSES = {
    ZaimisOrderStatus.NOT_SIGNED,
    ZaimisOrderStatus.DRAFT,
    ZaimisOrderStatus.APPROVAL,
    ZaimisOrderStatus.PAYMENT_AWAITING,
}
_REJECTED_STATUSES = {ZaimisOrderStatus.REJECTED}
_ACTIVE_STATUSES = {ZaimisOrderStatus.ACTIVE}
_OVERDUE_ACTIVE_STATUSES = {
    ZaimisOrderStatus.EXPIRED,
    ZaimisOrderStatus.COURT,
    ZaimisOrderStatus.CLAIM,
}
_OVERDUE_CLOSED_STATUSES = {ZaimisOrderStatus.CLOSED_EXPIRED}
_CLOSED_STATUSES = {ZaimisOrderStatus.CLOSED}


@dataclass(slots=True)
class ZaimisNormalizedOrder:
    order_id: str
    list_row: dict
    detail: dict | None = None
    expired_info: dict | None = None
    status_code: int | None = None
    status_label: str | None = None
    status_policy: str | None = None
    type_code: int | None = None
    borrower_user_id: str | None = None
    borrower_display_name: str | None = None
    borrower_full_name: str | None = None
    document_file_id: str | None = None
    document_id: str | None = None
    borrower_phone: str | None = None
    borrower_email: str | None = None
    borrower_address: str | None = None
    borrower_addresses: list[dict[str, str]] = field(default_factory=list)
    borrower_zip: str | None = None
    amount: float | None = None
    rating: float | None = None
    issued_at: str | None = None
    due_at: str | None = None
    overdue_started_at: str | None = None
    closed_at: str | None = None
    days_overdue: int | None = None
    principal_outstanding: float | None = None
    accrued_percent: float | None = None
    fine_outstanding: float | None = None
    total_due: float | None = None
    should_fetch_expired_info: bool = False
    should_try_pdf: bool = False


@dataclass(slots=True)
class ZaimisSyncResult:
    total_orders: int = 0
    synced_overdue_cases: int = 0
    pdf_enriched: int = 0
    touched_document_ids: set[str] = field(default_factory=set)


@dataclass(slots=True)
class _BorrowerAggregate:
    borrower_user_id: str
    display_name: str | None = None
    full_name: str | None = None
    document_id: str | None = None
    funded_orders: int = 0
    clean_closed_orders: int = 0
    overdue_history_orders: int = 0
    has_active: bool = False
    has_overdue_active: bool = False
    max_days_overdue: int | None = None
    ratings: list[float] = field(default_factory=list)


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


def _first_text(value) -> str | None:
    if isinstance(value, (list, tuple)):
        for item in value:
            text = str(item or "").strip()
            if text:
                return text
        return None
    text = str(value or "").strip()
    return text or None


def _zaimis_full_name(expired_info: dict | None) -> str | None:
    payload = expired_info or {}
    parts = [
        str(payload.get("lastName") or "").strip(),
        str(payload.get("firstName") or "").strip(),
        str(payload.get("secondName") or "").strip(),
    ]
    name = " ".join(part for part in parts if part)
    return name or None


def _zaimis_address(expired_info: dict | None) -> str | None:
    return _first_text((expired_info or {}).get("addressStr"))


def _zaimis_zip(address: str | None) -> str | None:
    if not address:
        return None
    match = _ZIP_RE.search(address)
    return match.group(1) if match else None


def _zaimis_addresses(expired_info: dict | None) -> list[dict[str, str]]:
    return normalize_borrower_addresses((expired_info or {}).get("addressStr"))


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


def _status_code(value) -> int | None:
    return _safe_int(value)


def _status_policy(status_code: int | None) -> str | None:
    if status_code is None:
        return None
    if status_code in _PRE_CONTRACT_STATUSES:
        return "pre-contract"
    if status_code in _REJECTED_STATUSES:
        return "rejected"
    if status_code in _ACTIVE_STATUSES:
        return "active"
    if status_code in _OVERDUE_ACTIVE_STATUSES:
        return "overdue-active"
    if status_code in _OVERDUE_CLOSED_STATUSES:
        return "overdue-closed"
    if status_code in _CLOSED_STATUSES:
        return "closed"
    return None


def _status_from_overdue_days(days: int | None) -> str:
    if days is None:
        return "просрочка"
    if days <= 5:
        return "просрочка до 5 дней"
    if days <= 30:
        return "просрочка 6-30 дней"
    return "просрочка > 30 дней"


def normalize_zaimis_order(
    list_row: dict,
    detail: dict | None = None,
    expired_info: dict | None = None,
) -> ZaimisNormalizedOrder:
    payload = detail or list_row or {}
    offer = payload.get("offer") or list_row.get("offer") or {}
    borrower_payload = resolve_order_borrower_identity(list_row, detail)
    model = payload.get("modelData") or list_row.get("modelData") or {}
    expired_info = expired_info or None
    status_code = _status_code(_coalesce(payload.get("state"), list_row.get("state")))
    detail_document = (detail or {}).get("document") or {}
    expired_document_id = str((expired_info or {}).get("documentId") or "").strip() or None
    detail_amount = _safe_float(_coalesce(payload.get("amount"), list_row.get("amount"), offer.get("amount")))
    principal = _safe_float(
        _coalesce(
            payload.get("principalOutstanding"),
            list_row.get("principalOutstanding"),
            payload.get("amount"),
            list_row.get("amount"),
            offer.get("amount"),
        )
    )
    accrued = _safe_float(
        _coalesce(
            payload.get("interestOutstanding"),
            list_row.get("interestOutstanding"),
            model.get("profit"),
            model.get("interestWithOverdue"),
        )
    )
    fine = _safe_float(
        _coalesce(
            payload.get("penaltyOutstanding"),
            list_row.get("penaltyOutstanding"),
            model.get("penaltyAmount"),
        )
    )
    due_at = _coalesce(
        payload.get("returnDate"),
        list_row.get("returnDate"),
        payload.get("deadline"),
        list_row.get("deadline"),
        payload.get("dueAt"),
        list_row.get("dueAt"),
    )
    overdue_started_at = _coalesce(
        payload.get("expiredDate"),
        list_row.get("expiredDate"),
        payload.get("overdueStartedAt"),
        list_row.get("overdueStartedAt"),
    )
    actual_duration = _safe_int(_coalesce(payload.get("actualDuration"), list_row.get("actualDuration")))
    loan_term = _safe_int(_coalesce(payload.get("loanTerm"), list_row.get("loanTerm")))
    explicit_days = _safe_int(_coalesce(payload.get("daysOverdue"), list_row.get("daysOverdue")))
    duration_days = actual_duration - loan_term if actual_duration is not None and loan_term is not None else None
    if duration_days is not None and duration_days < 0:
        duration_days = 0
    days_overdue = explicit_days or duration_days or _days_overdue_from_due(overdue_started_at or due_at)
    total_due = _safe_float(
        _coalesce(
            payload.get("totalOutstanding"),
            list_row.get("totalOutstanding"),
            model.get("closeSend"),
            list_row.get("returnAmount"),
        )
    )
    borrower_addresses = _zaimis_addresses(expired_info)
    borrower_address, borrower_zip = primary_borrower_address(borrower_addresses)
    if not borrower_address:
        borrower_address = _zaimis_address(expired_info)
        borrower_zip = _zaimis_zip(borrower_address)
    return ZaimisNormalizedOrder(
        order_id=str(_coalesce(payload.get("id"), list_row.get("id")) or "").strip(),
        list_row=list_row,
        detail=detail,
        expired_info=expired_info,
        status_code=status_code,
        status_label=ZAIMIS_STATUS_LABELS.get(status_code),
        status_policy=_status_policy(status_code),
        type_code=_status_code(_coalesce(payload.get("type"), list_row.get("type"))),
        borrower_user_id=str(borrower_payload.get("id", "")).strip() or None,
        borrower_display_name=_first_text(borrower_payload.get("displayName")),
        borrower_full_name=_zaimis_full_name(expired_info) or _first_text(borrower_payload.get("fullName")),
        document_file_id=str(detail_document.get("documentId") or "").strip() or None,
        document_id=expired_document_id,
        borrower_phone=_first_text((expired_info or {}).get("phone")) or _first_text((expired_info or {}).get("otherPhones")),
        borrower_email=_first_text((expired_info or {}).get("email")),
        borrower_address=borrower_address,
        borrower_addresses=borrower_addresses,
        borrower_zip=borrower_zip,
        amount=detail_amount,
        rating=_safe_float(_coalesce(offer.get("score"), payload.get("score"), list_row.get("score"))),
        issued_at=_coalesce(payload.get("createdAt"), list_row.get("createdAt"), offer.get("createdAt")),
        due_at=due_at,
        overdue_started_at=overdue_started_at,
        closed_at=_coalesce(payload.get("closedDate"), list_row.get("closedDate"), payload.get("actualReturnDate")),
        days_overdue=days_overdue,
        principal_outstanding=principal,
        accrued_percent=accrued,
        fine_outstanding=fine,
        total_due=total_due if total_due is not None else _safe_float(_coalesce(detail_amount, principal)),
        should_fetch_expired_info=status_code in _OVERDUE_ACTIVE_STATUSES,
        should_try_pdf=bool(detail_document.get("documentId")),
    )


def _touch_aggregate(aggregate: _BorrowerAggregate, order: ZaimisNormalizedOrder) -> None:
    if order.borrower_display_name:
        aggregate.display_name = aggregate.display_name or order.borrower_display_name
    if order.borrower_full_name:
        aggregate.full_name = order.borrower_full_name
    if order.document_id:
        aggregate.document_id = order.document_id
    if order.rating is not None and order.rating > 0:
        aggregate.ratings.append(order.rating)
    if order.status_policy in {"active", "overdue-active", "overdue-closed", "closed"}:
        aggregate.funded_orders += 1
    if order.status_policy == "closed":
        aggregate.clean_closed_orders += 1
    if order.status_policy in {"overdue-active", "overdue-closed"}:
        aggregate.overdue_history_orders += 1
    if order.status_policy in {"active", "overdue-active"}:
        aggregate.has_active = True
    if order.status_policy == "overdue-active":
        aggregate.has_overdue_active = True
        if order.days_overdue is not None:
            aggregate.max_days_overdue = max(
                order.days_overdue,
                aggregate.max_days_overdue or 0,
            )


def _derive_aggregate_status(aggregate: _BorrowerAggregate) -> tuple[str | None, list[str]]:
    details: list[str] = []
    if aggregate.overdue_history_orders > 0:
        details.append("были просрочки в истории")
    if aggregate.has_active:
        details.append("есть текущий займ")
    if aggregate.has_overdue_active:
        return _status_from_overdue_days(aggregate.max_days_overdue), details
    if aggregate.has_active:
        return "текущий", details
    if aggregate.overdue_history_orders > 0:
        return "закрыт, были просрочки", details or ["были просрочки в истории"]
    if aggregate.funded_orders > 0:
        return "в срок", ["закрыт без просрочек"]
    return None, []


async def _load_orders(parser: ZaimisParser, cred: UserCredentials) -> list[dict]:
    orders = await parser.list_orders()
    if parser.needs_reauth:
        ok = await parser.login(cred.login, cred.password)
        if not ok:
            raise RuntimeError(f"Zaimis re-login failed: {cred.login}")
        orders = await parser.list_orders()
    return orders


async def sync_zaimis_account(
    cred: UserCredentials,
    *,
    parser: ZaimisParser | None = None,
    include_pdf: bool,
    sync_overdue_cases: bool,
) -> ZaimisSyncResult:
    own_parser = parser is None
    if parser is None:
        parser = await ensure_zaimis_parser(cred)
        if parser is None:
            raise RuntimeError(f"Zaimis login failed: {cred.login}")

    result = ZaimisSyncResult()
    user_tag = telegram_user_tag(cred)
    aggregates: dict[str, _BorrowerAggregate] = {}

    try:
        orders = await _load_orders(parser, cred)
        result.total_orders = len(orders)
        normalized_orders: list[ZaimisNormalizedOrder] = []
        detail_cache: dict[str, dict | None] = {}

        for order in orders:
            normalized = normalize_zaimis_order(order)
            normalized_orders.append(normalized)
            if not normalized.borrower_user_id:
                continue
            aggregate = aggregates.setdefault(
                normalized.borrower_user_id,
                _BorrowerAggregate(borrower_user_id=normalized.borrower_user_id),
            )
            _touch_aggregate(aggregate, normalized)

        if include_pdf and aggregates:
            skip_borrower_ids = await list_borrower_ids_with_documents("zaimis", list(aggregates.keys()))
            pdf_results = await parser.enrich_borrowers_from_orders(
                orders,
                skip_borrower_ids=skip_borrower_ids,
            )
            result.pdf_enriched = len(pdf_results)
            for borrower_user_id, (full_name, document_id) in pdf_results.items():
                aggregate = aggregates.get(borrower_user_id)
                if not aggregate:
                    continue
                if full_name:
                    aggregate.full_name = full_name
                if document_id:
                    aggregate.document_id = document_id

        credential_seen: set[str] = set()
        if sync_overdue_cases:
            for normalized in normalized_orders:
                if normalized.status_policy != "overdue-active":
                    continue
                if not normalized.order_id:
                    continue
                credential_seen.add(normalized.order_id)

                detail = detail_cache.get(normalized.order_id)
                if detail is None:
                    detail = await parser.get_order_detail(normalized.order_id)
                    detail_cache[normalized.order_id] = detail
                expired_info = await parser.get_order_expired_info(normalized.order_id)
                normalized = normalize_zaimis_order(normalized.list_row, detail=detail, expired_info=expired_info)

                if normalized.borrower_user_id:
                    aggregate = aggregates.setdefault(
                        normalized.borrower_user_id,
                        _BorrowerAggregate(borrower_user_id=normalized.borrower_user_id),
                    )
                    _touch_aggregate(aggregate, normalized)

                existing = await lookup_borrower("zaimis", normalized.borrower_user_id) if normalized.borrower_user_id else None
                effective_document_id = normalized.document_id or (existing or {}).get("document_id")
                effective_full_name = normalized.borrower_full_name or (existing or {}).get("full_name")
                if normalized.borrower_user_id and (
                    effective_full_name or normalized.borrower_display_name or effective_document_id
                ):
                    await upsert_borrower(
                        service="zaimis",
                        borrower_user_id=normalized.borrower_user_id,
                        full_name=effective_full_name,
                        document_id=effective_document_id,
                        source="zaimis_borrow",
                        display_name=normalized.borrower_display_name,
                    )

                offer = (detail or {}).get("offer") or normalized.list_row.get("offer") or {}
                case_id = await upsert_overdue_case(
                    chat_id=cred.chat_id,
                    credential_id=cred.id,
                    service="zaimis",
                    external_id=normalized.order_id,
                    loan_id=str(_coalesce((detail or {}).get("id"), offer.get("id"), normalized.list_row.get("offerId")) or "") or None,
                    loan_number=str(
                        _coalesce((detail or {}).get("code"), normalized.list_row.get("code"), offer.get("code"), normalized.order_id)
                        or ""
                    )
                    or None,
                    account_label=cred.login,
                    borrower_user_id=normalized.borrower_user_id,
                    document_id=effective_document_id,
                    issued_at=normalized.issued_at,
                    due_at=normalized.due_at,
                    overdue_started_at=normalized.overdue_started_at,
                    days_overdue=normalized.days_overdue,
                    amount=normalized.amount,
                    principal_outstanding=normalized.principal_outstanding,
                    accrued_percent=normalized.accrued_percent,
                    fine_outstanding=normalized.fine_outstanding,
                    total_due=normalized.total_due,
                    status=str(normalized.status_code) if normalized.status_code is not None else None,
                    raw_data={
                        "order": normalized.list_row,
                        "detail": detail,
                        "expired_info": expired_info or None,
                        "normalized": {
                            "status_label": normalized.status_label,
                            "status_policy": normalized.status_policy,
                        },
                    },
                )
                if effective_document_id and any(
                    (
                        normalized.borrower_phone,
                        normalized.borrower_email,
                        normalized.borrower_address,
                        normalized.borrower_addresses,
                        normalized.borrower_zip,
                    )
                ):
                    contact_source = f"zaimis_investment_detail_{user_tag}"
                    merged_addresses = merge_primary_borrower_address(
                        normalized.borrower_address,
                        normalized.borrower_zip,
                        normalized.borrower_addresses,
                        full_name=effective_full_name,
                    )
                    await upsert_borrower_contacts(
                        effective_document_id,
                        full_name=effective_full_name,
                        borrower_phone=normalized.borrower_phone,
                        borrower_email=normalized.borrower_email,
                        borrower_address=normalized.borrower_address,
                        borrower_addresses=merged_addresses,
                        borrower_zip=normalized.borrower_zip,
                        source=contact_source,
                        source_account_tag=user_tag,
                    )
                    await update_overdue_case_contacts(
                        case_id,
                        cred.chat_id,
                        borrower_address=normalized.borrower_address,
                        borrower_addresses=merged_addresses,
                        borrower_zip=normalized.borrower_zip,
                        borrower_phone=normalized.borrower_phone,
                        borrower_email=normalized.borrower_email,
                        contact_source=contact_source,
                        source=contact_source,
                    )
                if effective_document_id:
                    result.touched_document_ids.add(str(effective_document_id))
                result.synced_overdue_cases += 1

            deactivated_document_ids = await deactivate_missing_overdue_cases(
                cred.chat_id,
                "zaimis",
                sorted(credential_seen),
                credential_id=cred.id,
            )
            result.touched_document_ids.update(deactivated_document_ids)
            await refresh_borrower_statuses(result.touched_document_ids)

        for aggregate in aggregates.values():
            existing = await lookup_borrower("zaimis", aggregate.borrower_user_id)
            effective_document_id = aggregate.document_id or (existing or {}).get("document_id")
            effective_full_name = aggregate.full_name or (existing or {}).get("full_name")
            avg_rating = (
                sum(aggregate.ratings) / len(aggregate.ratings)
                if aggregate.ratings
                else None
            )

            await upsert_borrower(
                service="zaimis",
                borrower_user_id=aggregate.borrower_user_id,
                full_name=effective_full_name,
                document_id=effective_document_id,
                source=f"zaimis_archive_{user_tag}",
                display_name=aggregate.display_name,
            )
            await upsert_borrower_from_investment(
                service="zaimis",
                borrower_user_id=aggregate.borrower_user_id,
                full_name=effective_full_name,
                display_name=aggregate.display_name,
                total_loans=aggregate.funded_orders,
                settled_loans=aggregate.clean_closed_orders,
                overdue_loans=aggregate.overdue_history_orders,
                avg_rating=avg_rating,
            )
            if effective_document_id:
                loan_status, details = _derive_aggregate_status(aggregate)
                await upsert_borrower_info(
                    effective_document_id,
                    full_name=effective_full_name,
                    loan_status=loan_status,
                    loan_status_details_json=details,
                    loan_count=aggregate.funded_orders,
                    rating=avg_rating,
                    source="zaimis_borrow",
                    source_account_tag=user_tag,
                )
        return result
    finally:
        if own_parser and parser is not None:
            await parser.close()


__all__ = [
    "ZaimisNormalizedOrder",
    "ZaimisSyncResult",
    "normalize_zaimis_order",
    "sync_zaimis_account",
]
