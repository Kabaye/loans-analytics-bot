from __future__ import annotations

from bot.integrations.parsers.finkit import FinkitParser
from bot.integrations.parsers.zaimis import ZaimisParser
from bot.repositories.borrowers import upsert_borrower_from_investment
from bot.repositories.credentials import save_credential_session


async def load_zaimis_investments(credential_id: int, login: str, password: str) -> int:
    zp = ZaimisParser()
    try:
        ok = await zp.login(login, password)
        if not ok:
            raise RuntimeError("Login failed")
        if export := zp.export_session():
            await save_credential_session(credential_id, "zaimis", export)

        orders = await zp.fetch_investments()
        stats: dict[str, dict] = {}
        for order in orders:
            offer = order.get("offer", {}) or {}
            owner = offer.get("owner", {}) or {}
            buid = str(owner.get("id", ""))
            if not buid:
                continue
            if buid not in stats:
                stats[buid] = {
                    "full_name": None,
                    "display_name": owner.get("displayName", ""),
                    "total": 0,
                    "settled": 0,
                    "overdue": 0,
                    "ratings": [],
                    "invested": 0.0,
                }
            borrower = stats[buid]
            borrower["total"] += 1
            state = order.get("state")
            if state == 3:
                borrower["settled"] += 1
            if state == 4:
                borrower["overdue"] += 1
            try:
                borrower["invested"] += float(order.get("amount", 0))
            except (ValueError, TypeError):
                pass
            score = offer.get("score")
            if score is not None:
                try:
                    borrower["ratings"].append(float(score))
                except (ValueError, TypeError):
                    pass

        for buid, borrower in stats.items():
            avg_rating = (
                sum(borrower["ratings"]) / len(borrower["ratings"])
                if borrower["ratings"]
                else None
            )
            await upsert_borrower_from_investment(
                service="zaimis",
                borrower_user_id=buid,
                full_name=borrower["full_name"] or None,
                display_name=borrower["display_name"] or None,
                total_loans=borrower["total"],
                settled_loans=borrower["settled"],
                overdue_loans=borrower["overdue"],
                avg_rating=avg_rating,
                total_invested=borrower["invested"],
            )
        return len(orders)
    finally:
        await zp.close()


async def load_finkit_investments(credential_id: int, login: str, password: str) -> int:
    fp = FinkitParser()
    try:
        ok = await fp.login(login, password)
        if not ok:
            raise RuntimeError("Login failed")
        if export := fp.export_session():
            await save_credential_session(credential_id, "finkit", export)

        session = await fp._get_session()
        cookie_str = "; ".join(f"{k}={v}" for k, v in fp._session_cookies.items())
        headers = {"Accept": "application/json", "Referer": "https://finkit.by/", "Cookie": cookie_str}

        borrower_stats: dict[str, dict] = {}
        total = 0
        page = 1
        while True:
            url = f"https://api-p2p.finkit.by/user/investments/?page={page}"
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    break
                data = await resp.json()

            for inv in data.get("results", []):
                total += 1
                buid = inv.get("user") or inv.get("loan")
                if not buid:
                    continue
                buid = str(buid)
                bname = inv.get("borrower_full_name", "")
                if buid not in borrower_stats:
                    borrower_stats[buid] = {
                        "full_name": bname,
                        "total": 0,
                        "settled": 0,
                        "overdue": 0,
                        "ratings": [],
                        "invested": 0.0,
                    }
                borrower = borrower_stats[buid]
                borrower["total"] += 1
                status = inv.get("status")
                if inv.get("closed") is True:
                    borrower["settled"] += 1
                if inv.get("is_overdue") and not inv.get("closed"):
                    borrower["overdue"] += 1
                try:
                    borrower["invested"] += float(inv.get("amount", 0))
                except (ValueError, TypeError):
                    pass
                try:
                    rating = float(inv.get("borrower_score", 0))
                    if rating > 0:
                        borrower["ratings"].append(rating)
                except (ValueError, TypeError):
                    pass

            if not data.get("next"):
                break
            page += 1

        for buid, borrower in borrower_stats.items():
            avg_rating = (
                sum(borrower["ratings"]) / len(borrower["ratings"])
                if borrower["ratings"]
                else None
            )
            await upsert_borrower_from_investment(
                service="finkit",
                borrower_user_id=buid,
                full_name=borrower["full_name"] or None,
                total_loans=borrower["total"],
                settled_loans=borrower["settled"],
                overdue_loans=borrower["overdue"],
                avg_rating=avg_rating,
                total_invested=borrower["invested"],
            )
        return total
    finally:
        await fp.close()


async def load_investments_archive(
    credential_id: int,
    service: str,
    login: str,
    password: str,
) -> int:
    if service == "zaimis":
        return await load_zaimis_investments(credential_id, login, password)
    if service == "finkit":
        return await load_finkit_investments(credential_id, login, password)
    return 0


__all__ = [
    "load_finkit_investments",
    "load_investments_archive",
    "load_zaimis_investments",
]
