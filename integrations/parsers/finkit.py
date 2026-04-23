"""Finkit.by parser — email/password login + PDF enrichment for ID."""
from __future__ import annotations

import asyncio
import io
import json
import logging
import re
from datetime import datetime

import aiohttp
import pdfplumber

from bot.domain.models import BorrowEntry
from bot.integrations.parsers.base import BROWSER_UA, BaseParser
from bot.repositories.borrowers import lookup_borrower, upsert_borrower

log = logging.getLogger(__name__)

CONFIG_URL = "https://api-p2p.finkit.by/_allauth/browser/v1/config"
LOGIN_URL = "https://api-p2p.finkit.by/_allauth/browser/v1/auth/login"
BORROWS_URL = "https://api-p2p.finkit.by/loans-to-invest/"
LOAN_URL = "https://finkit.by/invest"

NAME_ID_RE = re.compile(
    r"Я,\s+([А-ЯЁA-Z]+(?:\s+[А-ЯЁA-Z]+){2}),\s+идентификационный\s+номер\s*([0-9A-Z]+)",
    re.IGNORECASE | re.UNICODE | re.MULTILINE | re.DOTALL,
)
NAME_ID_FALLBACK_RE = re.compile(
    r"([А-ЯЁA-Z]+(?:\s+[А-ЯЁA-Z]+){2}),\s+идентификационный\s+номер\s*([0-9A-Z]+)",
    re.IGNORECASE | re.UNICODE | re.MULTILINE | re.DOTALL,
)
CLAIM_DEBTOR_BLOCK_RE = re.compile(
    r"Должник:\s*(?P<name>[^\n]+)\s*\n"
    r"\(заемщик по договору займа\)\s*(?P<address>.+?)\s*\n"
    r"тел\.:?\s*(?P<phone>[^\n]+)\s*\n"
    r"(?:эл\.почта|эл\. почта|эл\.почта:|эл\. почта:):?\s*(?P<email>[^\n]+)",
    re.IGNORECASE | re.UNICODE | re.MULTILINE | re.DOTALL,
)

BORROWER_WORK_MAP = {
    "worker": "Рабочий / служащий",
    "contract": "По договору подряда / услуг",
    "maternity": "Декретный отпуск",
    "student": "Студент",
    "entrepreneur": "ИП",
    "professional": "Самозанятый",
    "pensioner": "Пенсионер",
    "unemployed": "Безработный",
}


class FinkitParser(BaseParser):
    SERVICE_NAME = "finkit"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._authenticated = False
        self._csrf_token: str | None = None
        self._session_cookies: dict[str, str] = {}

    async def login(self, username: str = "", password: str = "") -> bool:
        self._needs_reauth = False
        session = await self._get_session()

        # Step 1: get csrftoken cookie
        try:
            async with session.get(CONFIG_URL, headers={"Accept": "application/json"}) as resp:
                # extract csrftoken from cookies
                for cookie in resp.cookies.values():
                    if cookie.key == "csrftoken":
                        self._csrf_token = cookie.value
                if not self._csrf_token:
                    # try from Set-Cookie header
                    for h in resp.headers.getall("Set-Cookie", []):
                        if "csrftoken=" in h:
                            self._csrf_token = h.split("csrftoken=")[1].split(";")[0]
        except Exception as e:
            log.exception("Finkit config fetch error: %s", e)
            return False

        if not self._csrf_token:
            log.error("Finkit: could not get csrftoken")
            return False

        # Step 2: login
        payload = {"email": username, "password": password}
        headers = {
            "Content-Type": "application/json",
            "x-csrftoken": self._csrf_token,
            "Referer": "https://finkit.by/",
            "Cookie": f"csrftoken={self._csrf_token}",
        }
        try:
            async with session.post(LOGIN_URL, json=payload, headers=headers) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.error("Finkit login failed: %s — %s", resp.status, body[:300])
                    return False
                # store session cookies
                for cookie in resp.cookies.values():
                    self._session_cookies[cookie.key] = cookie.value
                # also pick up from Set-Cookie
                for h in resp.headers.getall("Set-Cookie", []):
                    for name in ("csrftoken", "sessionid"):
                        if f"{name}=" in h:
                            self._session_cookies[name] = h.split(f"{name}=")[1].split(";")[0]
                self._authenticated = True
                log.info("Finkit login OK")
                return True
        except Exception as e:
            log.exception("Finkit login error: %s", e)
            return False

    def export_session(self) -> dict | None:
        if not self._authenticated or not self._session_cookies:
            return None
        return {
            "csrf_token": self._csrf_token,
            "session_cookies": self._session_cookies,
        }

    def restore_session(self, session_data: dict | None) -> bool:
        if not session_data:
            return False
        cookies = session_data.get("session_cookies") or {}
        if not cookies:
            return False
        self._csrf_token = session_data.get("csrf_token")
        self._session_cookies = dict(cookies)
        self._authenticated = True
        self._needs_reauth = False
        return True

    def _api_headers(self) -> dict[str, str]:
        cookie_str = "; ".join(f"{k}={v}" for k, v in self._session_cookies.items())
        return {
            "Accept": "application/json",
            "Referer": "https://finkit.by/",
            "User-Agent": BROWSER_UA,
            "Cookie": cookie_str,
        }

    async def fetch_investments(self, page: int = 1) -> list[dict]:
        if not self._authenticated:
            log.error("Finkit: not logged in, cannot fetch investments")
            return []

        session = await self._get_session()
        all_items: list[dict] = []
        current_page = page

        while True:
            url = f"https://api-p2p.finkit.by/user/investments/?page={current_page}"
            try:
                async with session.get(url, headers=self._api_headers()) as resp:
                    if resp.status in (401, 403):
                        self._authenticated = False
                        self._needs_reauth = True
                        break
                    if resp.status != 200:
                        log.error("Finkit investments page %d: HTTP %s", current_page, resp.status)
                        break
                    data = await resp.json()
            except Exception as e:
                log.exception("Finkit investments page %d error: %s", current_page, e)
                break

            items = data.get("results", [])
            if not items:
                break
            all_items.extend(items)
            if not data.get("next"):
                break
            current_page += 1

        log.info("Finkit: fetched %d investment items", len(all_items))
        return all_items

    async def fetch_investment_detail(self, investment_id: str) -> dict | None:
        if not self._authenticated:
            return None

        session = await self._get_session()
        try:
            async with session.get(
                f"https://api-p2p.finkit.by/user/investments/{investment_id}/",
                headers=self._api_headers(),
            ) as resp:
                if resp.status in (401, 403):
                    self._authenticated = False
                    self._needs_reauth = True
                    return None
                if resp.status != 200:
                    return None
                return await resp.json()
        except Exception as e:
            log.warning("Finkit investment detail error %s: %s", investment_id, e)
            return None

    async def fetch_contract_pdf(self, contract_url: str) -> bytes | None:
        if not self._authenticated or not contract_url:
            return None

        session = await self._get_session()
        try:
            async with session.get(contract_url, headers=self._api_headers()) as resp:
                if resp.status in (401, 403):
                    self._authenticated = False
                    self._needs_reauth = True
                    return None
                if resp.status != 200:
                    log.warning("Finkit contract PDF fetch failed: %s", resp.status)
                    return None
                return await resp.read()
        except Exception as e:
            log.warning("Finkit contract PDF error: %s", e)
            return None

    async def fetch_claims(self, investment_id: str, create_if_missing: bool = False) -> list[dict]:
        if not self._authenticated or not investment_id:
            return []

        session = await self._get_session()
        csrf_token = self._session_cookies.get("csrftoken") or self._csrf_token or ""
        headers = {
            **self._api_headers(),
            "Content-Type": "application/json",
            "Origin": "https://finkit.by",
            "Referer": "https://finkit.by/",
            "x-csrftoken": csrf_token,
        }
        method = session.post if create_if_missing else session.get
        kwargs = {"json": {}} if create_if_missing else {}

        try:
            async with method(
                f"https://api-p2p.finkit.by/user/investments/{investment_id}/claims/",
                headers=headers,
                **kwargs,
            ) as resp:
                if resp.status in (401, 403):
                    self._authenticated = False
                    self._needs_reauth = True
                    log.warning("Finkit claims auth failed: %s", resp.status)
                    return []
                if resp.status not in (200, 201):
                    log.warning("Finkit claims request failed %s: %s", investment_id, resp.status)
                    return []
                data = await resp.json()
                return data if isinstance(data, list) else []
        except Exception as e:
            log.warning("Finkit claims request error %s: %s", investment_id, e)
            return []

    @staticmethod
    def parse_borrower_from_contract_pdf(pdf_bytes: bytes) -> tuple[str | None, str | None]:
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                text = "\n".join(page.extract_text() or "" for page in pdf.pages)
            matches = list(NAME_ID_RE.finditer(text))
            if not matches:
                matches = list(NAME_ID_FALLBACK_RE.finditer(text))
            if not matches:
                return None, None
            borrower_name = matches[0].group(1).strip()
            borrower_document_id = matches[0].group(2).strip()
            return borrower_name, borrower_document_id
        except Exception as e:
            log.warning("Finkit contract PDF parse error: %s", e)
            return None, None

    @staticmethod
    def parse_claim_document_pdf(pdf_bytes: bytes) -> dict[str, str | None]:
        result = {
            "debtor_name": None,
            "debtor_address": None,
            "debtor_phone": None,
            "debtor_email": None,
        }
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                text = "\n".join(page.extract_text() or "" for page in pdf.pages[:4])
            match = CLAIM_DEBTOR_BLOCK_RE.search(text)
            if not match:
                return result
            result["debtor_name"] = " ".join(match.group("name").split()).strip()
            result["debtor_address"] = " ".join(match.group("address").replace("\n", " ").split()).strip(" ,")
            result["debtor_phone"] = " ".join(match.group("phone").split()).strip()
            result["debtor_email"] = " ".join(match.group("email").split()).strip()
            return result
        except Exception as e:
            log.warning("Finkit claim PDF parse error: %s", e)
            return result

    async def fetch_borrows(self) -> list[BorrowEntry]:
        if not self._authenticated:
            log.error("Finkit: not logged in")
            return []

        session = await self._get_session()
        all_entries: list[BorrowEntry] = []
        url: str | None = f"{BORROWS_URL}?page=1&ordering=borrower_score_value"

        while url:
            try:
                async with session.get(url, headers=self._api_headers()) as resp:
                    if resp.status in (401, 403):
                        log.warning("Finkit auth expired (status %s)", resp.status)
                        self._authenticated = False
                        self._needs_reauth = True
                        break
                    if resp.status != 200:
                        log.error("Finkit fetch failed: %s", resp.status)
                        break
                    data = await resp.json()
            except Exception as e:
                log.exception("Finkit fetch error: %s", e)
                break

            items = data.get("results", [])
            url = data.get("next")

            for item in items:
                try:
                    amount = float(item.get("amount", 0))
                    interest_day = float(item.get("interest_rate", 0))
                    interest_year = interest_day * 365
                    term = int(item.get("term", 0))
                    score_str = item.get("borrower_score", "0")
                    score = float(str(score_str).split(".")[0]) if score_str else 0

                    # return amount = amount * (1 + rate * term / 100)
                    amount_return = amount * (1 + interest_day * term / 100)
                    profit_gross = amount_return - amount
                    profit_net = amount_return * 0.95 - amount
                    platform_fee = amount_return * 0.05

                    created_str = item.get("created")
                    created_at = None
                    if created_str:
                        try:
                            created_at = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                        except Exception:
                            pass

                    loan_id = str(item.get("loan_number", item.get("id", "")))
                    entry = BorrowEntry(
                        id=loan_id,
                        service=self.SERVICE_NAME,
                        amount=amount,
                        period_days=term,
                        interest_day=interest_day,
                        interest_year=interest_year,
                        penalty_interest=1.5,  # fixed for finkit
                        credit_score=score,
                        created_at=created_at,
                        profit_gross=profit_gross,
                        profit_net=profit_net,
                        amount_return=amount_return,
                        platform_fee_open=platform_fee,
                        platform_fee_close=0,
                        contract_url=item.get("latest_contract_url"),
                        status=item.get("status"),
                        is_employed=_parse_work(item.get("borrower_work")),
                        has_active_loan=item.get("borrower_has_active_loan_now"),
                        has_overdue=item.get("borrower_has_overdue_history_gt_1_day"),
                        display_name=BORROWER_WORK_MAP.get(
                            item.get("borrower_work", ""), item.get("borrower_work")
                        ),
                        borrower_user_id=item.get("user"),
                        loans_count=None,
                        loans_count_settled=item.get("borrower_repaid_on_time_loans_count"),
                        loans_count_overdue=item.get("borrower_repaid_with_overdue_loans_count"),
                        loan_url=f"{LOAN_URL}/{loan_id}",
                        raw_data=item,
                    )
                    all_entries.append(entry)
                except Exception as e:
                    log.warning("Finkit parse item error: %s", e)

        log.info("Finkit: fetched %d borrow entries", len(all_entries))
        return all_entries

    @staticmethod
    async def enrich_from_cache(entries: list[BorrowEntry]) -> list[BorrowEntry]:
        """Fast DB-only enrichment. Returns list of entries NOT found in cache
        (i.e. entries that still need PDF download)."""
        uncached: list[BorrowEntry] = []
        for entry in entries:
            if not entry.borrower_user_id:
                uncached.append(entry)
                continue
            cached = await lookup_borrower("finkit", entry.borrower_user_id)
            if cached and cached.get("document_id"):
                entry.full_name = cached.get("full_name")
                entry.document_id = cached.get("document_id")
                if cached.get("opi_checked_at"):
                    entry.opi_checked = True
                    entry.opi_has_debt = bool(cached.get("opi_has_debt"))
                    entry.opi_debt_amount = cached.get("opi_debt_amount")
                    entry.opi_full_name = cached.get("opi_full_name")
                    entry.opi_checked_at = cached.get("opi_checked_at")
                if cached.get("total_loans") and cached["total_loans"] > 0:
                    entry.kb_known = True
                    entry.kb_total_loans = cached.get("total_loans")
                    entry.kb_settled = cached.get("settled_loans")
                    entry.kb_overdue = cached.get("overdue_loans")
                    entry.kb_avg_rating = cached.get("avg_rating")
                    entry.kb_total_invested = cached.get("total_invested")
                log.debug("Finkit cache hit for %s → %s", entry.id, entry.borrower_user_id)
            else:
                uncached.append(entry)
        return uncached

    async def enrich_with_pdf(self, entries: list[BorrowEntry], max_concurrent: int = 6) -> None:
        """Download PDFs for entries NOT in cache and extract full name + document ID."""
        entries_to_download = [e for e in entries if e.contract_url and not e.document_id]
        if not entries_to_download:
            return

        sem = asyncio.Semaphore(max_concurrent)

        async def _download_one(entry: BorrowEntry):
            async with sem:
                try:
                    session = await self._get_session()
                    cookie_str = "; ".join(f"{k}={v}" for k, v in self._session_cookies.items())
                    headers = {
                        "Cookie": cookie_str,
                        "User-Agent": BROWSER_UA,
                    }
                    async with session.get(entry.contract_url, headers=headers) as resp:
                        if resp.status != 200:
                            log.warning("PDF fetch failed for %s: %s", entry.id, resp.status)
                            return
                        pdf_bytes = await resp.read()

                    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                        text = "\n".join(page.extract_text() or "" for page in pdf.pages)

                    m = NAME_ID_RE.search(text)
                    if m:
                        entry.full_name = m.group(1).strip()
                        entry.document_id = m.group(2).strip()
                        log.info("Finkit PDF enriched: %s → %s / %s", entry.id, entry.full_name, entry.document_id)

                        if entry.borrower_user_id:
                            await upsert_borrower(
                                service="finkit",
                                borrower_user_id=entry.borrower_user_id,
                                full_name=entry.full_name,
                                document_id=entry.document_id,
                                source="finkit_borrow",
                            )
                    else:
                        log.warning("Finkit PDF: no name/ID found for %s", entry.id)
                except Exception as e:
                    log.warning("Finkit PDF enrich error for %s: %s", entry.id, e)

        await asyncio.gather(*[_download_one(e) for e in entries_to_download])


def _parse_work(code: str | None) -> bool | None:
    if code is None:
        return None
    return code in ("worker", "contract", "entrepreneur", "professional")
