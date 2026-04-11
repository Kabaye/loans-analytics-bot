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

from bot.models import BorrowEntry
from bot.parsers.base import BaseParser
from bot.database import upsert_borrower, lookup_borrower, save_session_cookies, load_session_cookies

log = logging.getLogger(__name__)

CONFIG_URL = "https://api-p2p.finkit.by/_allauth/browser/v1/config"
LOGIN_URL = "https://api-p2p.finkit.by/_allauth/browser/v1/auth/login"
BORROWS_URL = "https://api-p2p.finkit.by/loans-to-invest/"
LOAN_URL = "https://finkit.by/invest"

NAME_ID_RE = re.compile(
    r"Я,\s+([А-ЯЁA-Z]+(?:\s+[А-ЯЁA-Z]+){2}),\s+идентификационный\s+номер\s*\n?\s*([0-9A-Z]+)",
    re.IGNORECASE | re.UNICODE | re.MULTILINE | re.DOTALL,
)

BORROWER_WORK_MAP = {
    "worker": "Рабочий / служащий",
    "contract": "По договору подряда / услуг",
    "maternity": "Декретный отпуск",
    "student": "Студент",
    "entrepreneur": "ИП",
    "pensioner": "Пенсионер",
    "unemployed": "",
    "other": "Другое",
}


class FinkitParser(BaseParser):
    SERVICE_NAME = "finkit"

    def __init__(self, chat_id: int | None = None, **kwargs):
        super().__init__(**kwargs)
        self._authenticated = False
        self._csrf_token: str | None = None
        self._session_cookies: dict[str, str] = {}
        self._chat_id = chat_id

    async def try_restore_session(self) -> bool:
        """Try to restore session cookies from persisted storage."""
        if not self._chat_id:
            return False
        result = await load_session_cookies("finkit", self._chat_id)
        if not result:
            return False
        cookies, csrf_token = result
        if not cookies or not cookies.get("sessionid"):
            return False

        # Test if session is still valid
        session = await self._get_session()
        cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())
        try:
            async with session.get(
                f"{BORROWS_URL}?page=1&ordering=borrower_score_value",
                headers={
                    "Accept": "application/json",
                    "Referer": "https://finkit.by/",
                    "User-Agent": "Mozilla/5.0",
                    "Cookie": cookie_str,
                },
            ) as resp:
                if resp.status == 200:
                    self._session_cookies = cookies
                    self._csrf_token = csrf_token
                    self._authenticated = True
                    log.info("Finkit session restored from DB for chat_id=%s", self._chat_id)
                    return True
                log.info("Finkit saved session expired (status %s), will re-login", resp.status)
        except Exception as e:
            log.debug("Finkit session restore check failed: %s", e)
        return False

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
                # Persist session cookies
                if self._chat_id:
                    await save_session_cookies(
                        "finkit", self._chat_id,
                        self._session_cookies, self._csrf_token,
                    )
                return True
        except Exception as e:
            log.exception("Finkit login error: %s", e)
            return False

    async def fetch_borrows(self) -> list[BorrowEntry]:
        if not self._authenticated:
            log.error("Finkit: not logged in")
            return []

        session = await self._get_session()
        all_entries: list[BorrowEntry] = []
        url: str | None = f"{BORROWS_URL}?page=1&ordering=borrower_score_value"

        cookie_str = "; ".join(f"{k}={v}" for k, v in self._session_cookies.items())

        while url:
            headers = {
                "Accept": "application/json",
                "Referer": "https://finkit.by/",
                "User-Agent": "Mozilla/5.0",
                "Cookie": cookie_str,
            }
            try:
                async with session.get(url, headers=headers) as resp:
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
                        loans_count=item.get("borrower_loans_count"),
                        loans_count_settled=item.get("borrower_loans_count_settled"),
                        loans_count_overdue=item.get("borrower_loans_count_overdue"),
                        loan_url=f"{LOAN_URL}/{loan_id}",
                        raw_data=item,
                    )
                    all_entries.append(entry)
                except Exception as e:
                    log.warning("Finkit parse item error: %s", e)

        log.info("Finkit: fetched %d borrow entries", len(all_entries))
        return all_entries

    async def enrich_with_pdf(self, entries: list[BorrowEntry], max_concurrent: int = 6) -> None:
        """Download PDFs and extract full name + document ID (for OPI check).
        Uses borrowers table cache to avoid re-downloading PDFs."""
        sem = asyncio.Semaphore(max_concurrent)

        async def _enrich_one(entry: BorrowEntry):
            # Try cache by borrower_user_id (primary key)
            if entry.borrower_user_id:
                cached = await lookup_borrower("finkit", entry.borrower_user_id)
                if cached and cached.get("document_id"):
                    entry.full_name = cached.get("full_name")
                    entry.document_id = cached.get("document_id")
                    # OPI data from borrower_info (via JOIN)
                    if cached.get("opi_checked_at"):
                        entry.opi_checked = True
                        entry.opi_has_debt = bool(cached.get("opi_has_debt"))
                        entry.opi_debt_amount = cached.get("opi_debt_amount")
                        entry.opi_full_name = cached.get("opi_full_name")
                    # Investment stats from borrowers
                    if cached.get("total_loans") and cached["total_loans"] > 0:
                        entry.kb_known = True
                        entry.kb_total_loans = cached.get("total_loans")
                        entry.kb_settled = cached.get("settled_loans")
                        entry.kb_overdue = cached.get("overdue_loans")
                        entry.kb_avg_rating = cached.get("avg_rating")
                        entry.kb_total_invested = cached.get("total_invested")
                    log.debug("Finkit cache hit for %s → %s", entry.id, entry.borrower_user_id)
                    return

            if not entry.contract_url:
                return

            async with sem:
                try:
                    session = await self._get_session()
                    cookie_str = "; ".join(f"{k}={v}" for k, v in self._session_cookies.items())
                    headers = {
                        "Cookie": cookie_str,
                        "User-Agent": "Mozilla/5.0",
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

                        # Save by borrower_user_id (main key)
                        if entry.borrower_user_id:
                            await upsert_borrower(
                                service="finkit",
                                borrower_user_id=entry.borrower_user_id,
                                full_name=entry.full_name,
                                document_id=entry.document_id,
                            )
                    else:
                        log.warning("Finkit PDF: no name/ID found for %s", entry.id)
                except Exception as e:
                    log.warning("Finkit PDF enrich error for %s: %s", entry.id, e)

        await asyncio.gather(*[_enrich_one(e) for e in entries])


def _parse_work(code: str | None) -> bool | None:
    if code is None:
        return None
    return code in ("worker", "contract", "entrepreneur")
