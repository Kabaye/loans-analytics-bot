"""Zaimis.by parser — JWT Bearer auth, parallel fetching, server-side filters."""
from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
import re
from datetime import datetime

import pdfplumber

from bot.models import BorrowEntry, Subscription
from bot.parsers.base import BaseParser
from bot.database import save_session_cookies, load_session_cookies

log = logging.getLogger(__name__)

LOGIN_URL = "https://zaimis.by/app/api/auth/login"
OFFERS_URL = "https://zaimis.by/app/api/offer"
ORDER_DETAIL_URL = "https://zaimis.by/app/api/order"
DOCUMENT_URL = "https://zaimis.by/app/api/documents"

# Belarus personal ID: 7 digits + letter + 3 digits + 2 letters + 1 digit
BELARUS_ID_RE = re.compile(r"([0-9]{7}[A-Z][0-9]{3}[A-Z]{2}[0-9])")

# Pattern: "и ФИО, ИН (Ф.И.О., идентификационный номер Заемщика)"
BORROWER_RE = re.compile(
    r"и\s+([А-ЯЁA-Z][А-ЯЁA-Zа-яёa-z\s]+?),\s*"
    r"([0-9]{7}[A-Z][0-9]{3}[A-Z]{2}[0-9])\s*"
    r"\(Ф\.И\.О\.,\s*идентификационный\s*\n?\s*номер\s+Заемщика\)",
    re.IGNORECASE | re.MULTILINE,
)
LOAN_BASE_URL = "https://zaimis.by/app/all-loans?tab=giveLoan"

HEADERS = {
    "Accept": "application/json;charset=UTF-8",
    "Access-Control-Allow-Origin": "*",
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}

PARALLEL_BATCH = 10  # concurrent page fetches


def merge_filters(subs: list[Subscription]) -> tuple[dict, dict]:
    """Merge subscription filters into Zaimis API filters + query params.
    Only applies a server-side filter if ALL subscriptions define it."""
    if not subs:
        return {}, {}

    filters: dict = {}
    query_params: dict = {}

    def _merge_min(api_field: str, get_min):
        mins = [get_min(s) for s in subs if get_min(s) is not None]
        if mins and len(mins) == len(subs):
            filters.setdefault(api_field, {})["min"] = str(min(mins))

    def _merge_range(api_field: str, get_min, get_max):
        mins = [get_min(s) for s in subs if get_min(s) is not None]
        maxs = [get_max(s) for s in subs if get_max(s) is not None]
        if mins and len(mins) == len(subs):
            filters.setdefault(api_field, {})["min"] = str(min(mins))
        if maxs and len(maxs) == len(subs):
            filters.setdefault(api_field, {})["max"] = str(max(maxs))

    _merge_min("amount", lambda s: s.sum_min)
    _merge_min("score", lambda s: s.rating_min)
    _merge_range("loanTerm", lambda s: s.period_min, lambda s: s.period_max)
    _merge_min("loanRate", lambda s: s.interest_min)

    if all(s.require_employed for s in subs):
        query_params["isUserEmployed"] = "true"
    if all(s.require_income_confirmed for s in subs):
        query_params["isIncomeConfirmed"] = "true"

    return filters, query_params


class ZaimisParser(BaseParser):
    SERVICE_NAME = "zaimis"

    def __init__(self, chat_id: int | None = None, **kwargs):
        super().__init__(**kwargs)
        self._token: str | None = None
        self._chat_id: int | None = chat_id

    async def try_restore_session(self) -> bool:
        """Try to restore JWT token from persisted storage."""
        if not self._chat_id:
            return False
        result = await load_session_cookies("zaimis", self._chat_id)
        if not result:
            return False
        cookies, _ = result
        token = cookies.get("token")
        if not token:
            return False

        # Test if token is still valid
        session = await self._get_session()
        try:
            async with session.get(
                f"{OFFERS_URL}?tab=give&skip=0&take=1",
                headers={**HEADERS, "Authorization": f"Bearer {token}"},
            ) as resp:
                if resp.status == 200:
                    self._token = token
                    log.info("Zaimis session restored from DB for chat_id=%s", self._chat_id)
                    return True
                log.info("Zaimis saved token expired (status %s), will re-login", resp.status)
        except Exception as e:
            log.debug("Zaimis session restore check failed: %s", e)
        return False

    async def login(self, username: str = "", password: str = "") -> bool:
        self._needs_reauth = False
        session = await self._get_session()
        payload = {"login": username, "password": password}
        try:
            async with session.post(
                LOGIN_URL,
                json=payload,
                headers={**HEADERS, "Authorization": "null"},
            ) as resp:
                if resp.status != 200:
                    log.error("Zaimis login failed: %s", resp.status)
                    return False
                data = await resp.json()
                self._token = data.get("token")
                if not self._token:
                    log.error("Zaimis login: no token in response")
                    return False
                # Persist token for restart survival
                if self._chat_id:
                    await save_session_cookies(
                        "zaimis", self._chat_id, {"token": self._token},
                    )
                log.info("Zaimis login OK")
                return True
        except Exception as e:
            log.exception("Zaimis login error: %s", e)
            return False

    async def _fetch_page(self, page: int, filters_json: str,
                          query_params: dict) -> dict:
        """Fetch a single page of offers."""
        session = await self._get_session()
        params = {
            "query": "",
            "page": str(page),
            "perPage": "100",
            "filters": filters_json,
            "sortBy": "score",
            "sortOrder": "ascend",
            **query_params,
        }
        headers = {**HEADERS, "Authorization": f"Bearer {self._token}"}
        try:
            async with session.get(OFFERS_URL, params=params, headers=headers) as resp:
                if resp.status == 401:
                    self._needs_reauth = True
                    return {"data": [], "total": 0}
                if resp.status != 200:
                    log.error("Zaimis page %d failed: %s", page, resp.status)
                    return {"data": [], "total": 0}
                return await resp.json()
        except Exception as e:
            log.exception("Zaimis page %d error: %s", page, e)
            return {"data": [], "total": 0}

    @staticmethod
    def _parse_dt(s: str | None) -> datetime | None:
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None

    def _parse_entry(self, item: dict, request_type: str) -> BorrowEntry | None:
        """Parse a single API item into BorrowEntry."""
        try:
            model = item.get("modelData", {})
            amount = float(item.get("amount", 0))
            term = int(item.get("loanTerm", 0))
            rate_day = float(item.get("loanRate", 0))
            rate_year = rate_day * 360
            penalty = float(item.get("penaltyRate", 0))
            score = float(item.get("score", 0))

            if model:
                profit_gross = float(model.get("profit", 0))
                profit_net = float(model.get("realProfit", 0))
                amount_return = float(model.get("closeSend", 0))
                fee_open = float(model.get("openPlatform", 0))
                fee_close = float(model.get("closePlatform", 0))
            else:
                profit_gross = amount * rate_day / 100 * term
                amount_return = amount + profit_gross
                fee_open = amount * 0.07
                fee_close = amount_return * 0.05
                profit_net = profit_gross - fee_close

            offer_id = str(item.get("id", ""))

            return BorrowEntry(
                id=offer_id,
                service=self.SERVICE_NAME,
                request_type=request_type,
                amount=amount,
                period_days=term,
                interest_day=rate_day,
                interest_year=rate_year,
                penalty_interest=penalty,
                credit_score=score,
                created_at=self._parse_dt(item.get("createdAt")),
                updated_at=self._parse_dt(item.get("updatedAt")),
                profit_gross=profit_gross,
                profit_net=profit_net,
                amount_return=amount_return,
                platform_fee_open=fee_open,
                platform_fee_close=fee_close,
                is_income_confirmed=item.get("isIncomeConfirmed"),
                is_employed=item.get("isUserEmployed"),
                loans_count=item.get("count"),
                display_name=item.get("owner", {}).get("displayName") if isinstance(item.get("owner"), dict) else None,
                borrower_user_id=item.get("owner", {}).get("id") if isinstance(item.get("owner"), dict) else None,
                note=item.get("note"),
                status=str(item.get("state", "")),
                loan_url=LOAN_BASE_URL,
                raw_data=item,
            )
        except Exception as e:
            log.warning("Zaimis parse error: %s", e)
            return None

    async def _fetch_offers(self, type_filter: dict, request_type: str, label: str,
                            extra_filters: dict | None = None,
                            query_params: dict | None = None) -> list[BorrowEntry]:
        """Fetch offers with parallel page loading."""
        if not self._token:
            log.error("Zaimis: not logged in")
            return []

        # Build filters JSON
        filters = {**type_filter}
        if extra_filters:
            filters.update(extra_filters)
        filters_json = json.dumps(filters)
        qp = query_params or {}

        # Page 1 — get total and actual page size
        data = await self._fetch_page(1, filters_json, qp)
        total = data.get("total", 0)
        items = data.get("data", [])
        if not items:
            log.info("Zaimis: fetched 0 %s entries (total=%d)", label, total)
            return []

        page_size = len(items)
        total_pages = (total + page_size - 1) // page_size if page_size else 1

        # Fetch remaining pages in parallel batches
        all_items = list(items)
        if total_pages > 1:
            for batch_start in range(2, total_pages + 1, PARALLEL_BATCH):
                batch_end = min(batch_start + PARALLEL_BATCH, total_pages + 1)
                tasks = [
                    self._fetch_page(p, filters_json, qp)
                    for p in range(batch_start, batch_end)
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, dict):
                        all_items.extend(r.get("data", []))

        # Parse entries
        entries = [e for item in all_items if (e := self._parse_entry(item, request_type))]

        log.info("Zaimis: fetched %d %s entries (total=%d, pages=%d)",
                 len(entries), label, total, total_pages)
        return entries

    async def fetch_borrows(self, subscriptions: list[Subscription] | None = None) -> list[BorrowEntry]:
        extra_filters, query_params = {}, {}
        if subscriptions:
            extra_filters, query_params = merge_filters(subscriptions)
        return await self._fetch_offers(
            {"type": {"min": "1"}}, "borrow", "borrow",
            extra_filters=extra_filters, query_params=query_params,
        )

    async def fetch_lends(self) -> list[BorrowEntry]:
        return await self._fetch_offers({"type": {"max": "0"}}, "lend", "lend")

    async def fetch_investments(self, page: int = 1, per_page: int = 100) -> list[dict]:
        """Fetch user's investment orders (history of funded loans).
        Returns raw dicts (not BorrowEntry) since these are investments, not borrow requests."""
        if not self._token:
            log.error("Zaimis: not logged in, cannot fetch investments")
            return []

        session = await self._get_session()
        headers = {**HEADERS, "Authorization": f"Bearer {self._token}"}
        all_orders: list[dict] = []
        current_page = page

        while True:
            params = {"page": str(current_page), "perPage": str(per_page)}
            try:
                async with session.get(
                    "https://zaimis.by/app/api/user/orders",
                    params=params,
                    headers=headers,
                ) as resp:
                    if resp.status == 401:
                        self._needs_reauth = True
                        break
                    if resp.status != 200:
                        log.error("Zaimis investments page %d: HTTP %s", current_page, resp.status)
                        break
                    data = await resp.json()
            except Exception as e:
                log.exception("Zaimis investments page %d error: %s", current_page, e)
                break

            items = data.get("data", [])
            if not items:
                break
            all_orders.extend(items)

            total = data.get("total", 0)
            if len(all_orders) >= total:
                break
            current_page += 1

        log.info("Zaimis: fetched %d investment orders", len(all_orders))
        return all_orders

    async def fetch_order_detail(self, order_id: str) -> dict | None:
        """Fetch single order detail (includes document info)."""
        if not self._token:
            return None
        session = await self._get_session()
        headers = {**HEADERS, "Authorization": f"Bearer {self._token}"}
        try:
            async with session.get(
                f"{ORDER_DETAIL_URL}/{order_id}", headers=headers
            ) as resp:
                if resp.status != 200:
                    return None
                return await resp.json()
        except Exception as e:
            log.warning("Zaimis order detail error %s: %s", order_id, e)
            return None

    async def fetch_document_pdf(self, document_id: str) -> bytes | None:
        """Download PDF by documentId (base64 in JSON response)."""
        if not self._token:
            return None
        session = await self._get_session()
        headers = {**HEADERS, "Authorization": f"Bearer {self._token}"}
        try:
            async with session.get(
                f"{DOCUMENT_URL}/{document_id}/pdf", headers=headers
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                content = data.get("content", "")
                if not content:
                    return None
                return base64.b64decode(content)
        except Exception as e:
            log.warning("Zaimis document PDF error %s: %s", document_id, e)
            return None

    @staticmethod
    def parse_borrower_from_pdf(pdf_bytes: bytes) -> tuple[str | None, str | None]:
        """Extract borrower full_name and document_id (ИН) from Zaimis contract PDF.
        Returns (full_name, document_id) or (None, None)."""
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                text = "\n".join(page.extract_text() or "" for page in pdf.pages[:2])

            m = BORROWER_RE.search(text)
            if m:
                full_name = m.group(1).strip()
                doc_id = m.group(2).strip()
                return full_name, doc_id

            # Fallback: find all Belarus IDs, second one is borrower
            ids = BELARUS_ID_RE.findall(text)
            if len(ids) >= 2:
                # First ID = lender, second = borrower (per contract format)
                doc_id = ids[1]
                # Try to extract name before the ID
                idx = text.find(doc_id)
                if idx > 0:
                    before = text[max(0, idx - 200):idx]
                    name_match = re.search(
                        r"и\s+([А-ЯЁ][А-ЯЁа-яё]+\s+[А-ЯЁ][А-ЯЁа-яё]+\s+[А-ЯЁ][А-ЯЁа-яё]+),?\s*$",
                        before.strip(),
                    )
                    if name_match:
                        return name_match.group(1).strip(), doc_id
                return None, doc_id

            return None, None
        except Exception as e:
            log.warning("Zaimis PDF parse error: %s", e)
            return None, None

    async def enrich_borrowers_from_orders(
        self, orders: list[dict], max_concurrent: int = 4
    ) -> dict[str, tuple[str | None, str | None]]:
        """For each unique counterparty, fetch one order detail → PDF → extract ИН.
        Returns {counterparty_id: (full_name, document_id)}."""
        from bot.database import lookup_borrower

        # Group orders by counterparty, pick one order per borrower (prefer settled)
        borrower_orders: dict[str, str] = {}  # cp_id → order_id
        for order in orders:
            cp = order.get("counterparty", {}) or {}
            cp_id = str(cp.get("id", ""))
            if not cp_id:
                continue
            # Check if already in DB with document_id
            cached = await lookup_borrower("zaimis", cp_id)
            if cached and cached.get("document_id"):
                continue
            state = order.get("state")
            if cp_id not in borrower_orders or state == 3:
                borrower_orders[cp_id] = order["id"]

        if not borrower_orders:
            log.info("Zaimis PDF: all borrowers already enriched")
            return {}

        log.info("Zaimis PDF: enriching %d borrowers from PDFs", len(borrower_orders))
        results: dict[str, tuple[str | None, str | None]] = {}
        sem = asyncio.Semaphore(max_concurrent)

        async def _enrich_one(cp_id: str, order_id: str):
            async with sem:
                detail = await self.fetch_order_detail(order_id)
                if not detail:
                    return
                doc = detail.get("document", {}) or {}
                doc_file_id = doc.get("documentId")
                if not doc_file_id:
                    return
                pdf_bytes = await self.fetch_document_pdf(doc_file_id)
                if not pdf_bytes:
                    return
                full_name, doc_id = self.parse_borrower_from_pdf(pdf_bytes)
                if doc_id:
                    results[cp_id] = (full_name, doc_id)
                    log.info(
                        "Zaimis PDF enriched: %s → %s / %s",
                        cp_id[:8], full_name, doc_id,
                    )

        await asyncio.gather(
            *[_enrich_one(cp_id, oid) for cp_id, oid in borrower_orders.items()],
            return_exceptions=True,
        )
        log.info("Zaimis PDF: enriched %d/%d borrowers", len(results), len(borrower_orders))
        return results
