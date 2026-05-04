from __future__ import annotations

from bot.domain.credentials import UserCredentials
from bot.integrations.parsers.finkit import FinkitParser, finkit_has_overdue_history, finkit_is_settled_on_time
from bot.integrations.parsers.zaimis import ZaimisParser
from bot.repositories.borrowers import upsert_borrower_from_investment
from bot.repositories.credentials import get_credential_by_id, save_credential_session
from bot.services.zaimis_sync import sync_zaimis_account


async def load_zaimis_investments(credential_id: int, login: str, password: str) -> int:
    zp = ZaimisParser()
    try:
        ok = await zp.login(login, password)
        if not ok:
            raise RuntimeError("Login failed")
        if export := zp.export_session():
            await save_credential_session(credential_id, "zaimis", export)
        cred_row = await get_credential_by_id(credential_id)
        if not cred_row:
            raise RuntimeError(f"Credential not found: {credential_id}")
        cred = UserCredentials(
            id=int(cred_row["id"]),
            chat_id=int(cred_row["chat_id"]),
            service="zaimis",
            login=str(cred_row["login"]),
            password=str(cred_row["password"]),
            username=cred_row.get("label"),
        )
        result = await sync_zaimis_account(
            cred,
            parser=zp,
            include_pdf=True,
            sync_overdue_cases=False,
        )
        return result.total_orders
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
                if finkit_is_settled_on_time(inv):
                    borrower["settled"] += 1
                if finkit_has_overdue_history(inv):
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
