"""Mongo.by parser — no authentication required."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from bot.domain.models import BorrowEntry
from bot.integrations.parsers.base import BaseParser

log = logging.getLogger(__name__)

API_BASE = "https://gateway.mongo.by/api/v1/l"
BORROWS_URL = f"{API_BASE}/requests/list"
OFFERS_URL = f"{API_BASE}/offers/list"
LOAN_URL = "https://mongo.by/p2p/request"


class MongoParser(BaseParser):
    SERVICE_NAME = "mongo"

    async def login(self, username: str = "", password: str = "") -> bool:
        return True  # no auth needed

    async def fetch_borrows(self) -> list[BorrowEntry]:
        session = await self._get_session()
        params = {
            "pageNumber": "1",
            "pageSize": "1000000",
            "sortField": "Created",
            "sortOrder": "1",
        }
        try:
            async with session.get(BORROWS_URL, params=params) as resp:
                if resp.status != 200:
                    log.error("Mongo fetch failed: %s", resp.status)
                    return []
                data = await resp.json()
        except Exception as e:
            log.exception("Mongo fetch error: %s", e)
            return []

        items = data if isinstance(data, list) else data.get("items", data.get("data", []))
        results: list[BorrowEntry] = []
        for item in items:
            try:
                amount = float(item.get("amount", 0))
                interest_day = float(item.get("interestValue", 0))
                interest_year = interest_day * 365
                penalty = float(item.get("penalty", 0))
                term = int(item.get("termValue", 0))
                amount_return = float(item.get("amoutReturn", 0))
                credit_score = float(item.get("creditScore", 0))

                profit_gross = amount_return - amount
                profit_net = profit_gross * 0.95

                created_str = item.get("created") or item.get("validityDate")
                created_at = None
                if created_str:
                    try:
                        created_at = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                    except Exception:
                        pass

                entry = BorrowEntry(
                    id=str(item.get("id", "")),
                    service=self.SERVICE_NAME,
                    amount=amount,
                    period_days=term,
                    interest_day=interest_day,
                    interest_year=interest_year,
                    penalty_interest=penalty,
                    credit_score=credit_score,
                    created_at=created_at,
                    profit_gross=profit_gross,
                    profit_net=profit_net,
                    amount_return=amount_return,
                    platform_fee_open=amount * 0.05,
                    platform_fee_close=amount_return * 0.05,
                    status=str(item.get("status", "")),
                    loan_url=f"{LOAN_URL}/{item.get('id', '')}",
                    raw_data=item,
                )
                results.append(entry)
            except Exception as e:
                log.warning("Mongo parse item error: %s", e)
        log.info("Mongo: fetched %d borrow entries", len(results))
        return results

    async def fetch_lends(self) -> list[BorrowEntry]:
        """Fetch lend offers (people offering to lend money)."""
        session = await self._get_session()
        params = {
            "pageNumber": "1",
            "pageSize": "1000000",
            "sortField": "Created",
            "sortOrder": "1",
        }
        try:
            async with session.get(OFFERS_URL, params=params) as resp:
                if resp.status != 200:
                    log.error("Mongo lends fetch failed: %s", resp.status)
                    return []
                data = await resp.json()
        except Exception as e:
            log.exception("Mongo lends fetch error: %s", e)
            return []

        items = data if isinstance(data, list) else data.get("items", data.get("data", []))
        results: list[BorrowEntry] = []
        for item in items:
            try:
                amount = float(item.get("amount", 0))
                interest_day = float(item.get("interestValue", 0))
                interest_year = interest_day * 365
                penalty = float(item.get("penalty", 0))
                term = int(item.get("termValue", 0))
                amount_return = float(item.get("amoutReturn", 0))
                credit_score = float(item.get("creditScore", 0))

                profit_gross = amount_return - amount
                profit_net = profit_gross * 0.95

                created_str = item.get("created") or item.get("validityDate")
                created_at = None
                if created_str:
                    try:
                        created_at = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                    except Exception:
                        pass

                entry = BorrowEntry(
                    id=str(item.get("id", "")),
                    service=self.SERVICE_NAME,
                    request_type="lend",
                    amount=amount,
                    period_days=term,
                    interest_day=interest_day,
                    interest_year=interest_year,
                    penalty_interest=penalty,
                    credit_score=credit_score,
                    created_at=created_at,
                    profit_gross=profit_gross,
                    profit_net=profit_net,
                    amount_return=amount_return,
                    platform_fee_open=amount * 0.05,
                    platform_fee_close=amount_return * 0.05,
                    status=str(item.get("status", "")),
                    raw_data=item,
                )
                results.append(entry)
            except Exception as e:
                log.warning("Mongo lend parse item error: %s", e)
        log.info("Mongo: fetched %d lend entries", len(results))
        return results
