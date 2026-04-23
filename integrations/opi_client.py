"""OPI (enforcement proceedings) checker via ERIP/bePaid API.

Optimized: uses direct shortcut (1 API call to get session + 1 to check = 2 total).
Results are cached in DB (borrower_identities table).
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

import aiohttp

from bot.repositories.opi_cache import get_opi_cache, save_opi_result

log = logging.getLogger(__name__)

API_URL = "https://api.bepaid.by/beyag/komplat/v2/get_pay_list"
HEADERS = {
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Origin": "https://erip.paritetbank.by",
    "Referer": "https://erip.paritetbank.by/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}

# Direct shortcut to "Физические лица" terminal node — skips 3 intermediate ERIP tree steps
DIRECT_PAY_CODE = "11001027421"
DIRECT_DI_TYPE = "9120"

# Cache OPI results for 24 hours
OPI_CACHE_TTL = timedelta(hours=24)


@dataclass
class OPIResult:
    has_debt: Optional[bool]
    debt_amount: float = 0.0
    full_name: Optional[str] = None
    error: Optional[str] = None


class OPIChecker:
    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self._session = session
        self._owns_session = session is None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=False),
                timeout=aiohttp.ClientTimeout(total=60),
            )
            self._owns_session = True
        return self._session

    async def close(self):
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()

    async def _get_erip_session(self) -> tuple[str | None, str | None]:
        """Get fresh ERIP session via direct shortcut (1 API call)."""
        session = await self._get_session()
        payload = {
            "version": "3",
            "test": False,
            "di_type": DIRECT_DI_TYPE,
            "pay_code": DIRECT_PAY_CODE,
        }
        for attempt in range(3):
            try:
                async with session.post(API_URL, json=payload, headers=HEADERS) as resp:
                    if resp.status != 200:
                        log.error("ERIP session request failed: HTTP %s", resp.status)
                        if attempt < 2:
                            await asyncio.sleep(2)
                            continue
                        return None, None
                    data = await resp.json()
                break
            except (asyncio.CancelledError, asyncio.TimeoutError, aiohttp.ClientError) as e:
                log.warning("ERIP session attempt %d: %s", attempt + 1, e)
                if attempt < 2:
                    await asyncio.sleep(2)
                else:
                    return None, None

        if data.get("error_code") != "0":
            log.error("ERIP session error: %s", data)
            return None, None

        pay_record = data.get("pay_record")
        if isinstance(pay_record, dict) and "erip_session_id" in pay_record:
            return pay_record["erip_session_id"], pay_record["code"]
        log.error("ERIP session: unexpected response format")
        return None, None

    async def check(self, document_id: str, use_cache: bool = True) -> OPIResult:
        """Check if a person has enforcement proceedings by their ID number.
        
        Checks DB cache first (24h TTL). Falls back to ERIP API.
        """
        # Check DB cache first
        if use_cache:
            cached = await get_opi_cache(document_id)
            if cached and cached.get("opi_checked_at"):
                try:
                    checked_at = datetime.fromisoformat(cached["opi_checked_at"])
                    if datetime.now(timezone.utc) - checked_at < OPI_CACHE_TTL:
                        has_debt = bool(cached.get("opi_has_debt"))
                        log.info("OPI cache hit for %s: debt=%s", document_id, has_debt)
                        return OPIResult(
                            has_debt=has_debt,
                            debt_amount=cached.get("opi_debt_amount") or 0.0,
                            full_name=cached.get("opi_full_name"),
                        )
                except (ValueError, TypeError):
                    pass

        # Step 1: get fresh ERIP session (1 API call)
        erip_session_id, terminal_code = await self._get_erip_session()
        if not erip_session_id:
            return OPIResult(has_debt=None, error="Failed to get ERIP session")

        # Step 2: check document_id (1 API call)
        session = await self._get_session()
        payload = {
            "version": "3",
            "test": False,
            "di_type": "9191",
            "pay_code": terminal_code,
            "erip_session_id": erip_session_id,
            "attr_records": [
                {
                    "name": "Идентификационный номер",
                    "code": "0",
                    "code_out": "1001",
                    "value": document_id,
                    "edit": "1",
                    "change": "1",
                }
            ],
        }

        data = None
        for attempt in range(3):
            try:
                async with session.post(API_URL, json=payload, headers=HEADERS) as resp:
                    if resp.status != 200:
                        log.error("OPI check failed: HTTP %s", resp.status)
                        if attempt < 2:
                            await asyncio.sleep(2)
                            continue
                        return OPIResult(has_debt=None, error=f"HTTP {resp.status}")
                    data = await resp.json()
                break
            except (asyncio.CancelledError, asyncio.TimeoutError, aiohttp.ClientError) as e:
                log.warning("OPI check attempt %d for %s: %s", attempt + 1, document_id, e)
                if attempt < 2:
                    await asyncio.sleep(2)
                else:
                    return OPIResult(has_debt=None, error=str(e))

        if data is None:
            return OPIResult(has_debt=None, error="No response")

        error_code = data.get("error_code")

        # 199 = nothing found → no debts
        if error_code == "199":
            log.info("OPI check for %s: no debts", document_id)
            await save_opi_result(document_id, has_debt=False)
            return OPIResult(has_debt=False)

        if error_code != "0":
            error_text = data.get("error_text", "Unknown error")
            log.warning("OPI check error for %s: %s", document_id, error_text)
            return OPIResult(has_debt=None, error=error_text)

        # Has debts
        pay_record = data.get("pay_record", {})
        debt_amount = float(pay_record.get("summa", pay_record.get("total_amount", 0)))

        full_name_parts = []
        for attr in pay_record.get("attr_records", []):
            if attr.get("code_out") in ("1001", "1002", "1003"):
                val = attr.get("value", "")
                if val:
                    full_name_parts.append(val)
        full_name = " ".join(full_name_parts) if full_name_parts else None

        log.info("OPI check for %s: DEBT %.2f BYN, name=%s", document_id, debt_amount, full_name)
        await save_opi_result(document_id, has_debt=True, debt_amount=debt_amount, full_name=full_name)
        return OPIResult(has_debt=True, debt_amount=debt_amount, full_name=full_name)
