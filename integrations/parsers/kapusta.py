"""Kapusta.by parser — anti-bot cookie challenge via curl_cffi (TLS fingerprint)."""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone

from curl_cffi.requests import AsyncSession

from bot.domain.borrowers import BorrowEntry, DocumentRefs, EntrySnapshot
from bot.integrations.parsers.base import BaseParser

log = logging.getLogger(__name__)

API_BASE = "https://kapusta.by/api/internal/v1/public/loans"
BORROWS_URL = f"{API_BASE}/borrow_request/"
LENDS_URL = f"{API_BASE}/lend_request/"
LOAN_URL = "https://kapusta.by/borrow-request"
HG_SECURITY_RE = re.compile(r"hg-security=([^;]+)")


class KapustaBlockedError(Exception):
    """Raised when Kapusta returns 403 — anti-bot block."""
    pass


class KapustaParser(BaseParser):
    SERVICE_NAME = "kapusta"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._hg_client_security: str | None = None
        self._hg_security: str | None = None
        self._cffi_session: AsyncSession | None = None

    async def _get_cffi_session(self) -> AsyncSession:
        if self._cffi_session is None:
            self._cffi_session = AsyncSession(impersonate="chrome131", verify=False)
        return self._cffi_session

    async def close(self) -> None:
        if self._cffi_session is not None:
            await self._cffi_session.close()
            self._cffi_session = None
        await super().close()

    async def login(self, username: str = "", password: str = "") -> bool:
        """Bypass anti-bot challenge using curl_cffi with Chrome TLS fingerprint."""
        session = await self._get_cffi_session()
        probe_url = f"{API_BASE}/lend_request/?page=1&page_size=1&status=active"

        headers = {
            "Accept": "text/html,application/json",
            "Accept-Language": "ru,en;q=0.9",
        }

        # Step 1: trigger the challenge page
        try:
            r = await session.get(probe_url, headers=headers)
            body = r.text
            # extract hg-client-security from Set-Cookie header
            for name, val in r.headers.items():
                if name.lower() == "set-cookie" and "hg-client-security" in val:
                    self._hg_client_security = val.split(";")[0].split("=", 1)[1]
            # extract hg-security from JS body
            m = HG_SECURITY_RE.search(body)
            if m:
                self._hg_security = m.group(1)
        except Exception as e:
            log.exception("Kapusta step1 error: %s", e)
            return False

        if r.status_code == 200 and body.lstrip().startswith("{"):
            log.info("Kapusta direct API access OK without HTML challenge")
            return True

        if not self._hg_security:
            log.warning("Kapusta: could not extract hg-security from challenge page")
            return False

        log.info("Kapusta step1 OK: hg-client=%s, hg-sec=%s…",
                 self._hg_client_security, (self._hg_security or "")[:30])

        # Step 2: reload with cookies
        await asyncio.sleep(0.3)
        try:
            r2 = await session.get(
                probe_url,
                headers={**headers, "Cookie": self._cookie_string()},
            )
            body2 = r2.text

            # If we get another challenge, extract and retry
            if "hg-security=" in body2:
                log.info("Kapusta: got second challenge, retrying...")
                for name, val in r2.headers.items():
                    if name.lower() == "set-cookie" and "hg-client-security" in val:
                        self._hg_client_security = val.split(";")[0].split("=", 1)[1]
                m2 = HG_SECURITY_RE.search(body2)
                if m2:
                    self._hg_security = m2.group(1)

                await asyncio.sleep(0.3)
                r3 = await session.get(
                    probe_url,
                    headers={**headers, "Cookie": self._cookie_string()},
                )
                log.info("Kapusta step3 status: %s", r3.status_code)

            if r2.status_code == 403:
                log.error("Kapusta: 403 on step2 despite cookies")
                return False

        except Exception as e:
            log.exception("Kapusta step2 error: %s", e)
            return False

        log.info("Kapusta anti-bot bypass done")
        return True

    async def fetch_borrows(self) -> list[BorrowEntry]:
        session = await self._get_cffi_session()
        url = f"{BORROWS_URL}?page=1&page_size=10000&status=active"

        headers = {
            "Accept": "application/json",
            "Accept-Language": "ru,en;q=0.9",
            "Referer": "https://kapusta.by/",
            "Cookie": self._cookie_string(),
        }

        try:
            r = await session.get(url, headers=headers)
            if r.status_code == 403:
                raise KapustaBlockedError("Kapusta returned 403 — anti-bot block")
            if r.status_code != 200:
                log.error("Kapusta fetch failed: %s", r.status_code)
                return []
            data = r.json()
        except KapustaBlockedError:
            raise
        except Exception as e:
            log.exception("Kapusta fetch error: %s", e)
            return []

        items = data.get("results", [])
        results: list[BorrowEntry] = []

        for item in items:
            try:
                amount = float(item.get("amount", 0))
                interest_year = float(item.get("interest_rate", 0))
                interest_day = interest_year / 365
                penalty = interest_year * 2 / 365
                period = int(item.get("period_days", 0))
                rating = float(item.get("rating", 0))
                percent_amount = float(item.get("percent_amount", 0))
                loans_count = item.get("loans_count")

                amount_return = amount + percent_amount
                profit_net = amount_return * 0.955 - amount
                platform_fee = amount_return * 0.045

                created_str = item.get("created") or item.get("createdAt") or item.get("created_at")
                created_at = None
                if created_str:
                    try:
                        created_at = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                    except Exception:
                        pass

                entry = BorrowEntry(
                    snapshot=EntrySnapshot(
                        id=str(item.get("id", "")),
                        service=self.SERVICE_NAME,
                        amount=amount,
                        period_days=period,
                        interest_day=interest_day,
                        interest_year=interest_year,
                        penalty_interest=penalty,
                        credit_score=rating,
                        created_at=created_at,
                        profit_gross=percent_amount,
                        profit_net=profit_net,
                        amount_return=amount_return,
                        platform_fee_open=platform_fee,
                        platform_fee_close=0,
                        loans_count=loans_count,
                    ),
                    documents=DocumentRefs(loan_url=f"{LOAN_URL}/{item.get('id', '')}"),
                    raw_data=item,
                )
                results.append(entry)
            except Exception as e:
                log.warning("Kapusta parse item error: %s", e)

        log.info("Kapusta: fetched %d borrow entries", len(results))
        return results

    async def fetch_lends(self) -> list[BorrowEntry]:
        """Fetch active lend requests (investor offers)."""
        session = await self._get_cffi_session()
        url = f"{LENDS_URL}?page=1&page_size=10000&status=active"

        headers = {
            "Accept": "application/json",
            "Accept-Language": "ru,en;q=0.9",
            "Referer": "https://kapusta.by/",
            "Cookie": self._cookie_string(),
        }

        try:
            r = await session.get(url, headers=headers)
            if r.status_code == 403:
                raise KapustaBlockedError("Kapusta lend_request returned 403")
            if r.status_code != 200:
                log.error("Kapusta lend fetch failed: %s", r.status_code)
                return []
            data = r.json()
        except KapustaBlockedError:
            raise
        except Exception as e:
            log.exception("Kapusta lend fetch error: %s", e)
            return []

        items = data.get("results", [])
        results: list[BorrowEntry] = []

        for item in items:
            try:
                amount = float(item.get("amount", 0))
                interest_year = float(item.get("interest_rate", 0))
                interest_day = interest_year / 365
                period = int(item.get("period_days", 0))
                rating = float(item.get("rating", 0))
                percent_amount = float(item.get("percent_amount", 0))

                entry = BorrowEntry(
                    snapshot=EntrySnapshot(
                        id=str(item.get("id", "")),
                        service=self.SERVICE_NAME,
                        request_type="lend",
                        amount=amount,
                        period_days=period,
                        interest_day=interest_day,
                        interest_year=interest_year,
                        credit_score=rating,
                        profit_gross=percent_amount,
                        loans_count=item.get("loans_count"),
                    ),
                    raw_data=item,
                )
                results.append(entry)
            except Exception as e:
                log.warning("Kapusta lend parse error: %s", e)

        log.info("Kapusta: fetched %d lend entries", len(results))
        return results

    def _cookie_string(self) -> str:
        parts = []
        if self._hg_client_security:
            parts.append(f"hg-client-security={self._hg_client_security}")
        if self._hg_security:
            parts.append(f"hg-security={self._hg_security}")
        return "; ".join(parts)
