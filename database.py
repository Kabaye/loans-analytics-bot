from __future__ import annotations

import json
import aiosqlite
import logging
from datetime import datetime, timezone

from bot.config import DB_PATH

log = logging.getLogger(__name__)

_BORROWER_INFO_FULL_NAME_SQL = """
CASE
    WHEN excluded.full_name IS NULL OR excluded.full_name = '' THEN borrower_info.full_name
    WHEN borrower_info.full_name IS NULL OR borrower_info.full_name = '' THEN excluded.full_name
    WHEN instr(excluded.full_name, '*') > 0 AND instr(COALESCE(borrower_info.full_name, ''), '*') = 0
        THEN borrower_info.full_name
    ELSE excluded.full_name
END
"""

_BORROWER_INFO_SOURCE_SQL = """
CASE
    WHEN excluded.source IS NULL OR excluded.source = '' THEN borrower_info.source
    WHEN excluded.source = 'opi' AND borrower_info.source IS NOT NULL AND borrower_info.source != ''
        THEN borrower_info.source
    ELSE excluded.source
END
"""


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    return db


async def init_db() -> None:
    db = await get_db()
    try:
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            chat_id     INTEGER PRIMARY KEY,
            username    TEXT,
            is_admin    INTEGER DEFAULT 0,
            is_allowed  INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS credentials (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id     INTEGER NOT NULL,
            service     TEXT NOT NULL,
            login       TEXT NOT NULL,
            password    TEXT NOT NULL,
            label       TEXT,
            UNIQUE(chat_id, service, login),
            FOREIGN KEY (chat_id) REFERENCES users(chat_id)
        );

        CREATE TABLE IF NOT EXISTS subscriptions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id     INTEGER NOT NULL,
            service     TEXT NOT NULL,
            label       TEXT,
            sum_min     REAL,
            sum_max     REAL,
            rating_min  REAL,
            rating_max  REAL,
            period_min  INTEGER,
            period_max  INTEGER,
            interest_min REAL,
            interest_max REAL,
            is_active   INTEGER DEFAULT 1,
            created_at  TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (chat_id) REFERENCES users(chat_id)
        );

        -- Borrower mapping: service user ID → document_id (ИН) + investment stats
        CREATE TABLE IF NOT EXISTS borrowers (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            service             TEXT NOT NULL,
            borrower_user_id    TEXT NOT NULL,
            document_id         TEXT,
            full_name           TEXT,
            total_loans         INTEGER DEFAULT 0,
            settled_loans       INTEGER DEFAULT 0,
            overdue_loans       INTEGER DEFAULT 0,
            avg_rating          REAL,
            total_invested      REAL,
            first_seen          TEXT DEFAULT (datetime('now')),
            last_seen           TEXT DEFAULT (datetime('now')),
            UNIQUE(service, borrower_user_id)
        );

        CREATE INDEX IF NOT EXISTS idx_borrowers_doc ON borrowers(document_id);

        -- Central borrower card keyed by document_id (ИН)
        CREATE TABLE IF NOT EXISTS borrower_info (
            document_id         TEXT PRIMARY KEY,
            full_name           TEXT,
            loan_status         TEXT,
            sum_category        TEXT,
            rating              REAL,
            notes               TEXT,
            last_loan_date      TEXT,
            loan_count          INTEGER DEFAULT 0,
            opi_has_debt        INTEGER,
            opi_debt_amount     REAL,
            opi_checked_at      TEXT,
            opi_full_name       TEXT,
            total_invested      REAL,
            source              TEXT DEFAULT 'auto',
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        );

        -- Per-site polling settings (admin-configurable)
        CREATE TABLE IF NOT EXISTS site_settings (
            service             TEXT PRIMARY KEY,
            polling_enabled     INTEGER DEFAULT 1,
            poll_interval       INTEGER DEFAULT 60,
            active_hour_start   INTEGER DEFAULT 0,
            active_hour_end     INTEGER DEFAULT 24
        );

        CREATE TABLE IF NOT EXISTS seen_entries (
            service             TEXT NOT NULL,
            entry_id            TEXT NOT NULL,
            first_seen          TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (service, entry_id)
        );

        CREATE INDEX IF NOT EXISTS idx_seen_entries_service_first_seen
            ON seen_entries(service, first_seen);

        CREATE TABLE IF NOT EXISTS credential_sessions (
            credential_id       INTEGER PRIMARY KEY,
            service             TEXT NOT NULL,
            session_data        TEXT NOT NULL,
            updated_at          TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (credential_id) REFERENCES credentials(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS json_schema_state (
            service             TEXT PRIMARY KEY,
            schema_json         TEXT NOT NULL,
            updated_at          TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS api_change_alerts (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            service             TEXT NOT NULL,
            title               TEXT NOT NULL,
            details             TEXT,
            sample_json         TEXT,
            created_at          TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS overdue_cases (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id             INTEGER NOT NULL,
            credential_id       INTEGER,
            service             TEXT NOT NULL,
            external_id         TEXT NOT NULL,
            loan_id             TEXT,
            loan_number         TEXT,
            account_label       TEXT,
            borrower_user_id    TEXT,
            document_id         TEXT,
            full_name           TEXT,
            display_name        TEXT,
            borrower_address    TEXT,
            borrower_zip        TEXT,
            borrower_phone      TEXT,
            borrower_email      TEXT,
            voluntary_term_days INTEGER,
            issued_at           TEXT,
            due_at              TEXT,
            overdue_started_at  TEXT,
            days_overdue        INTEGER DEFAULT 0,
            amount              REAL,
            principal_outstanding REAL,
            accrued_percent     REAL,
            fine_outstanding    REAL,
            total_due           REAL,
            status              TEXT,
            contract_url        TEXT,
            loan_url            TEXT,
            raw_data            TEXT NOT NULL DEFAULT '{}',
            is_active           INTEGER DEFAULT 1,
            last_synced_at      TEXT DEFAULT (datetime('now')),
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now')),
            UNIQUE(chat_id, service, external_id),
            FOREIGN KEY (chat_id) REFERENCES users(chat_id),
            FOREIGN KEY (credential_id) REFERENCES credentials(id) ON DELETE SET NULL
        );

        CREATE INDEX IF NOT EXISTS idx_overdue_cases_chat_active
            ON overdue_cases(chat_id, is_active, updated_at);

        CREATE TABLE IF NOT EXISTS creditor_profiles (
            chat_id             INTEGER PRIMARY KEY,
            full_name           TEXT,
            address             TEXT,
            phone               TEXT,
            email               TEXT,
            payment_details     TEXT,
            sms_sender          TEXT,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (chat_id) REFERENCES users(chat_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS credential_creditor_profiles (
            chat_id             INTEGER NOT NULL,
            credential_id       INTEGER NOT NULL,
            full_name           TEXT,
            address             TEXT,
            phone               TEXT,
            email               TEXT,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (chat_id, credential_id),
            FOREIGN KEY (chat_id) REFERENCES users(chat_id) ON DELETE CASCADE,
            FOREIGN KEY (credential_id) REFERENCES credentials(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS user_signature_assets (
            chat_id             INTEGER PRIMARY KEY,
            file_path           TEXT NOT NULL,
            mime_type           TEXT,
            telegram_file_id    TEXT,
            telegram_unique_id  TEXT,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (chat_id) REFERENCES users(chat_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS generated_documents (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            overdue_case_id     INTEGER NOT NULL,
            chat_id             INTEGER NOT NULL,
            doc_type            TEXT NOT NULL,
            file_path           TEXT,
            text_content        TEXT,
            payload_json        TEXT,
            missing_fields      TEXT,
            created_at          TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (overdue_case_id) REFERENCES overdue_cases(id) ON DELETE CASCADE,
            FOREIGN KEY (chat_id) REFERENCES users(chat_id) ON DELETE CASCADE
        );
        """)

        # Migration: add first_name/last_name to users
        for col in ("first_name TEXT", "last_name TEXT"):
            try:
                await db.execute(f"ALTER TABLE users ADD COLUMN {col}")
            except Exception:
                pass

        # Migration: add employment/income filters to subscriptions
        for col in ("require_employed INTEGER", "require_income_confirmed INTEGER"):
            try:
                await db.execute(f"ALTER TABLE subscriptions ADD COLUMN {col}")
            except Exception:
                pass

        # Migration: add night_paused and min_settled_loans to subscriptions
        for col in ("night_paused INTEGER DEFAULT 0", "min_settled_loans INTEGER"):
            try:
                await db.execute(f"ALTER TABLE subscriptions ADD COLUMN {col}")
            except Exception:
                pass

        # Migration: add label column to credentials
        try:
            await db.execute("ALTER TABLE credentials ADD COLUMN label TEXT")
        except Exception:
            pass

        await db.execute("""
            INSERT OR IGNORE INTO credential_creditor_profiles (
                chat_id, credential_id, full_name, address, phone, email, created_at, updated_at
            )
            SELECT c.chat_id, c.id, cp.full_name, cp.address, cp.phone, cp.email, datetime('now'), datetime('now')
            FROM credentials c
            JOIN creditor_profiles cp ON cp.chat_id = c.chat_id
        """)

        # Migration: change credentials UNIQUE from (chat_id, service) to (chat_id, service, login)
        try:
            await db.execute("SELECT sql FROM sqlite_master WHERE name='credentials'")
            # Recreate table with new constraint if old one exists
            rows = await db.execute_fetchall(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='credentials'"
            )
            if rows and "UNIQUE(chat_id, service)" in (rows[0]["sql"] or "") \
               and "UNIQUE(chat_id, service, login)" not in (rows[0]["sql"] or ""):
                await db.execute("""
                    CREATE TABLE IF NOT EXISTS credentials_new (
                        id          INTEGER PRIMARY KEY AUTOINCREMENT,
                        chat_id     INTEGER NOT NULL,
                        service     TEXT NOT NULL,
                        login       TEXT NOT NULL,
                        password    TEXT NOT NULL,
                        label       TEXT,
                        UNIQUE(chat_id, service, login),
                        FOREIGN KEY (chat_id) REFERENCES users(chat_id)
                    )
                """)
                await db.execute("""
                    INSERT OR IGNORE INTO credentials_new (id, chat_id, service, login, password)
                    SELECT id, chat_id, service, login, password FROM credentials
                """)
                await db.execute("DROP TABLE credentials")
                await db.execute("ALTER TABLE credentials_new RENAME TO credentials")
                log.info("Migrated credentials: UNIQUE(chat_id, service) → UNIQUE(chat_id, service, login)")
        except Exception:
            pass

        # Migration: old borrower_identities → borrowers (already ran)
        for tbl in ("borrower_identities", "known_borrowers", "investment_history",
                     "notified_loans", "last_check", "session_cookies"):
            await db.execute(f"DROP TABLE IF EXISTS {tbl}")

        # Migration: rename old manual source to added
        await db.execute("UPDATE borrower_info SET source = 'added' WHERE source = 'manual'")

        # Migration: reclassify part of legacy auto borrower_info rows where service is unambiguous.
        await db.execute("""
            UPDATE borrower_info
            SET source = 'finkit_borrow'
            WHERE source = 'auto'
              AND document_id IN (
                    SELECT document_id
                    FROM borrowers
                    WHERE document_id IS NOT NULL AND document_id != ''
                    GROUP BY document_id
                    HAVING COUNT(DISTINCT service) = 1 AND MAX(service) = 'finkit'
              )
        """)
        await db.execute("""
            UPDATE borrower_info
            SET source = 'zaimis_borrow'
            WHERE source = 'auto'
              AND document_id IN (
                    SELECT document_id
                    FROM borrowers
                    WHERE document_id IS NOT NULL AND document_id != ''
                    GROUP BY document_id
                    HAVING COUNT(DISTINCT service) = 1 AND MAX(service) = 'zaimis'
              )
        """)

        # Migration: move OPI data from old borrowers → borrower_info
        # Old borrowers had: opi_has_debt, opi_debt_amount, opi_checked_at, opi_full_name, source
        # New: these live in borrower_info, borrowers keeps investment stats
        try:
            await db.execute("SELECT opi_has_debt FROM borrowers LIMIT 1")
            # Old schema still has OPI columns — migrate to borrower_info
            rows = await db.execute_fetchall(
                """SELECT document_id, full_name, opi_has_debt, opi_debt_amount,
                          opi_checked_at, opi_full_name, total_invested
                   FROM borrowers
                   WHERE document_id IS NOT NULL AND LENGTH(document_id) = 14"""
            )
            migrated = 0
            for r in rows:
                await db.execute(
                    """INSERT INTO borrower_info
                           (document_id, full_name, opi_has_debt, opi_debt_amount,
                            opi_checked_at, opi_full_name, total_invested, source)
                       VALUES (?, ?, ?, ?, ?, ?, ?, 'auto')
                       ON CONFLICT(document_id) DO UPDATE SET
                           full_name = COALESCE(excluded.full_name, borrower_info.full_name),
                           opi_has_debt = COALESCE(excluded.opi_has_debt, borrower_info.opi_has_debt),
                           opi_debt_amount = COALESCE(excluded.opi_debt_amount, borrower_info.opi_debt_amount),
                           opi_checked_at = COALESCE(excluded.opi_checked_at, borrower_info.opi_checked_at),
                           opi_full_name = COALESCE(excluded.opi_full_name, borrower_info.opi_full_name),
                           total_invested = COALESCE(excluded.total_invested, borrower_info.total_invested),
                           updated_at = datetime('now')
                    """,
                    (r["document_id"], r["full_name"], r["opi_has_debt"],
                     r["opi_debt_amount"], r["opi_checked_at"], r["opi_full_name"],
                     r["total_invested"]),
                )
                migrated += 1

            # Recreate borrowers without OPI columns, keeping investment stats
            await db.execute("""
                CREATE TABLE IF NOT EXISTS borrowers_new (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    service             TEXT NOT NULL,
                    borrower_user_id    TEXT NOT NULL,
                    document_id         TEXT,
                    full_name           TEXT,
                    total_loans         INTEGER DEFAULT 0,
                    settled_loans       INTEGER DEFAULT 0,
                    overdue_loans       INTEGER DEFAULT 0,
                    avg_rating          REAL,
                    total_invested      REAL,
                    first_seen          TEXT DEFAULT (datetime('now')),
                    last_seen           TEXT DEFAULT (datetime('now')),
                    UNIQUE(service, borrower_user_id)
                )
            """)
            await db.execute("""
                INSERT OR IGNORE INTO borrowers_new
                    (id, service, borrower_user_id, document_id, full_name,
                     total_loans, settled_loans, overdue_loans, avg_rating, total_invested,
                     first_seen, last_seen)
                SELECT id, service, borrower_user_id, document_id, full_name,
                       total_loans, settled_loans, overdue_loans, avg_rating, total_invested,
                       first_seen, last_seen
                FROM borrowers
            """)
            await db.execute("DROP TABLE borrowers")
            await db.execute("ALTER TABLE borrowers_new RENAME TO borrowers")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_borrowers_doc ON borrowers(document_id)")
            log.info("Migrated %d borrowers → borrower_info, dropped OPI columns from borrowers", migrated)
        except Exception:
            pass  # Already migrated or fresh DB

        # Seed default site_settings
        for svc, enabled, interval, h_start, h_end in [
            ("kapusta", 1, 600, 8, 23),
            ("finkit", 1, 60, 0, 24),
            ("zaimis", 1, 60, 0, 24),
        ]:
            await db.execute(
                """INSERT OR IGNORE INTO site_settings
                   (service, polling_enabled, poll_interval, active_hour_start, active_hour_end)
                   VALUES (?, ?, ?, ?, ?)""",
                (svc, enabled, interval, h_start, h_end),
            )

        # Mongo is export-only and must not appear in polling settings.
        await db.execute("DELETE FROM site_settings WHERE service = 'mongo'")

        await db.commit()
        log.info("Database initialized at %s", DB_PATH)
    finally:
        await db.close()


async def upsert_borrower(
    service: str,
    borrower_user_id: str,
    full_name: str | None = None,
    document_id: str | None = None,
    source: str | None = None,
) -> None:
    """Insert or update a borrower mapping record."""
    if full_name:
        full_name = full_name.upper()
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO borrowers (service, borrower_user_id, full_name, document_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(service, borrower_user_id) DO UPDATE SET
                full_name = COALESCE(excluded.full_name, borrowers.full_name),
                document_id = COALESCE(excluded.document_id, borrowers.document_id),
                last_seen = datetime('now')
            """,
            (service, borrower_user_id, full_name, document_id),
        )
        # Auto-create borrower_info if we have a document_id
        if document_id and len(document_id) == 14:
            await db.execute(
                f"""INSERT INTO borrower_info (document_id, full_name, source)
                    VALUES (?, ?, ?)
                    ON CONFLICT(document_id) DO UPDATE SET
                        full_name = {_BORROWER_INFO_FULL_NAME_SQL},
                        source = {_BORROWER_INFO_SOURCE_SQL},
                        updated_at = datetime('now')
                """,
                (document_id, full_name, source or f"{service}_borrow"),
            )
        await db.commit()
    finally:
        await db.close()


async def lookup_borrower(service: str, borrower_user_id: str) -> dict | None:
    """Lookup a borrower mapping + enriched info from borrower_info + investment stats."""
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """SELECT b.service, b.borrower_user_id, b.document_id, b.full_name,
                      b.total_loans, b.settled_loans, b.overdue_loans,
                      b.avg_rating, b.total_invested,
                      bi.loan_status, bi.sum_category, bi.rating AS bi_rating,
                      bi.notes, bi.last_loan_date, bi.loan_count,
                      bi.opi_has_debt, bi.opi_debt_amount, bi.opi_checked_at, bi.opi_full_name
               FROM borrowers b
               LEFT JOIN borrower_info bi ON b.document_id = bi.document_id
               WHERE b.service = ? AND b.borrower_user_id = ?""",
            (service, borrower_user_id),
        )
        return dict(rows[0]) if rows else None
    finally:
        await db.close()


async def lookup_borrower_info(document_id: str) -> dict | None:
    """Lookup borrower_info by document_id (ИН)."""
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM borrower_info WHERE document_id = ?",
            (document_id,),
        )
        return dict(rows[0]) if rows else None
    finally:
        await db.close()


async def search_borrower_info(query: str, limit: int = 10) -> list[dict]:
    """Search borrower_info + borrowers by ФИО or ИН (partial match)."""
    db = await get_db()
    try:
        q = query.strip()
        # SQLite COLLATE NOCASE only works for ASCII;
        # Cyrillic names are stored uppercase, so uppercase the query.
        # Normalize Ё→Е for search (Russians rarely type Ё).
        q_upper = q.upper().replace("Ё", "Е")
        # Detect ИН: contains digits (Cyrillic names don't)
        has_digits = any(c.isdigit() for c in q)
        if has_digits and len(q) >= 7 and q.replace(" ", "").isalnum():
            rows = await db.execute_fetchall(
                "SELECT * FROM borrower_info WHERE document_id LIKE ? LIMIT ?",
                (f"%{q_upper}%", limit),
            )
        else:
            # Search borrower_info by full_name (normalize Ё in stored names too)
            rows = await db.execute_fetchall(
                "SELECT * FROM borrower_info WHERE REPLACE(full_name, 'Ё', 'Е') LIKE ? LIMIT ?",
                (f"%{q_upper}%", limit),
            )
            # Also search borrowers table for entries not in borrower_info
            found_docs = {r["document_id"] for r in rows if r["document_id"]}
            extra = await db.execute_fetchall(
                """SELECT DISTINCT full_name, document_id, service,
                          total_loans, settled_loans, overdue_loans
                   FROM borrowers
                   WHERE REPLACE(full_name, 'Ё', 'Е') LIKE ?
                   LIMIT ?""",
                (f"%{q_upper}%", limit),
            )
            for r in extra:
                doc = r["document_id"]
                if not doc or doc in found_docs:
                    continue
                # Build a pseudo borrower_info dict
                rows.append({
                    "document_id": doc,
                    "full_name": r["full_name"],
                    "loan_status": None,
                    "sum_category": None,
                    "rating": None,
                    "notes": f"из {r['service']}" if r["service"] else None,
                    "last_loan_date": None,
                    "loan_count": r["total_loans"],
                    "source": r["service"],
                    "opi_has_debt": None,
                    "opi_debt_amount": None,
                    "opi_checked_at": None,
                    "opi_full_name": None,
                    "total_invested": None,
                })
                found_docs.add(doc)
        return [dict(r) for r in rows[:limit]]
    finally:
        await db.close()


async def upsert_borrower_info(
    document_id: str,
    full_name: str | None = None,
    loan_status: str | None = None,
    sum_category: str | None = None,
    rating: float | None = None,
    notes: str | None = None,
    last_loan_date: str | None = None,
    loan_count: int | None = None,
    total_invested: float | None = None,
    source: str = "added",
) -> None:
    """Insert or update a borrower_info record."""
    if full_name:
        full_name = full_name.upper()
    db = await get_db()
    try:
        await db.execute(
            f"""INSERT INTO borrower_info
                    (document_id, full_name, loan_status, sum_category, rating,
                     notes, last_loan_date, loan_count, total_invested, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(document_id) DO UPDATE SET
                    full_name = {_BORROWER_INFO_FULL_NAME_SQL},
                    loan_status = COALESCE(excluded.loan_status, borrower_info.loan_status),
                    sum_category = COALESCE(excluded.sum_category, borrower_info.sum_category),
                    rating = COALESCE(excluded.rating, borrower_info.rating),
                    notes = COALESCE(excluded.notes, borrower_info.notes),
                    last_loan_date = COALESCE(excluded.last_loan_date, borrower_info.last_loan_date),
                    loan_count = CASE WHEN excluded.loan_count IS NOT NULL
                                      THEN excluded.loan_count
                                      ELSE borrower_info.loan_count END,
                    total_invested = COALESCE(excluded.total_invested, borrower_info.total_invested),
                    source = {_BORROWER_INFO_SOURCE_SQL},
                    updated_at = datetime('now')
             """,
            (document_id, full_name, loan_status, sum_category, rating,
             notes, last_loan_date, loan_count, total_invested, source),
        )
        await db.commit()
    finally:
        await db.close()


async def get_site_settings(service: str) -> dict:
    """Get polling settings for a site."""
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM site_settings WHERE service=?", (service,)
        )
        if rows:
            return dict(rows[0])
        return {
            "service": service,
            "polling_enabled": 1,
            "poll_interval": 60,
            "active_hour_start": 0,
            "active_hour_end": 24,
        }
    finally:
        await db.close()


async def get_saved_credential_session(credential_id: int) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT session_data FROM credential_sessions WHERE credential_id = ?",
            (credential_id,),
        )
        if not rows:
            return None
        return json.loads(rows[0]["session_data"])
    finally:
        await db.close()


async def save_credential_session(credential_id: int, service: str, session_data: dict) -> None:
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO credential_sessions (credential_id, service, session_data, updated_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(credential_id) DO UPDATE SET
                service = excluded.service,
                session_data = excluded.session_data,
                updated_at = datetime('now')
            """,
            (credential_id, service, json.dumps(session_data, ensure_ascii=False)),
        )
        await db.commit()
    finally:
        await db.close()


async def delete_credential_session(credential_id: int) -> None:
    db = await get_db()
    try:
        await db.execute(
            "DELETE FROM credential_sessions WHERE credential_id = ?",
            (credential_id,),
        )
        await db.commit()
    finally:
        await db.close()


async def list_user_credentials(chat_id: int, services: tuple[str, ...] | None = None) -> list[dict]:
    db = await get_db()
    try:
        params: list[object] = [chat_id]
        where = "WHERE chat_id = ?"
        if services:
            placeholders = ",".join("?" for _ in services)
            where += f" AND service IN ({placeholders})"
            params.extend(services)
        rows = await db.execute_fetchall(
            f"""
            SELECT id, chat_id, service, login, password, label
            FROM credentials
            {where}
            ORDER BY service, id
            """,
            params,
        )
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def get_credential_by_id(credential_id: int, chat_id: int | None = None) -> dict | None:
    db = await get_db()
    try:
        sql = "SELECT id, chat_id, service, login, password, label FROM credentials WHERE id = ?"
        params: list[object] = [credential_id]
        if chat_id is not None:
            sql += " AND chat_id = ?"
            params.append(chat_id)
        rows = await db.execute_fetchall(sql, params)
        return dict(rows[0]) if rows else None
    finally:
        await db.close()


async def get_json_schema_state(service: str) -> dict[str, list[str]] | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT schema_json FROM json_schema_state WHERE service = ?",
            (service,),
        )
        if not rows:
            return None
        return json.loads(rows[0]["schema_json"])
    finally:
        await db.close()


async def save_json_schema_state(service: str, schema: dict[str, list[str]]) -> None:
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO json_schema_state (service, schema_json, updated_at)
            VALUES (?, ?, datetime('now'))
            ON CONFLICT(service) DO UPDATE SET
                schema_json = excluded.schema_json,
                updated_at = datetime('now')
            """,
            (service, json.dumps(schema, ensure_ascii=False, sort_keys=True)),
        )
        await db.commit()
    finally:
        await db.close()


async def save_api_change_alert(
    service: str,
    title: str,
    details: str | None = None,
    sample_json: str | None = None,
) -> int:
    db = await get_db()
    try:
        cursor = await db.execute(
            """
            INSERT INTO api_change_alerts (service, title, details, sample_json)
            VALUES (?, ?, ?, ?)
            """,
            (service, title, details, sample_json),
        )
        await db.commit()
        return int(cursor.lastrowid)
    finally:
        await db.close()


async def list_api_change_alerts(limit: int = 50) -> list[dict]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT *
            FROM api_change_alerts
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def get_api_change_alert(alert_id: int) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM api_change_alerts WHERE id = ?",
            (alert_id,),
        )
        return dict(rows[0]) if rows else None
    finally:
        await db.close()


async def delete_api_change_alert(alert_id: int) -> None:
    db = await get_db()
    try:
        await db.execute("DELETE FROM api_change_alerts WHERE id = ?", (alert_id,))
        await db.commit()
    finally:
        await db.close()


async def clear_api_change_alerts() -> None:
    db = await get_db()
    try:
        await db.execute("DELETE FROM api_change_alerts")
        await db.commit()
    finally:
        await db.close()


async def get_all_site_settings() -> list[dict]:
    """Get polling settings for all sites."""
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT * FROM site_settings
            WHERE service IN ('kapusta', 'finkit', 'zaimis')
            ORDER BY service
            """
        )
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def update_site_setting(service: str, **kwargs) -> None:
    """Update one or more settings for a site."""
    db = await get_db()
    try:
        sets = []
        vals = []
        for k, v in kwargs.items():
            if k in ("polling_enabled", "poll_interval", "active_hour_start", "active_hour_end"):
                sets.append(f"{k}=?")
                vals.append(v)
        if not sets:
            return
        vals.append(service)
        await db.execute(
            f"UPDATE site_settings SET {', '.join(sets)} WHERE service=?",
            vals,
        )
        await db.commit()
    finally:
        await db.close()


async def get_opi_cache(document_id: str) -> dict | None:
    """Get cached OPI result from borrower_info."""
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """SELECT opi_has_debt, opi_debt_amount, opi_checked_at, opi_full_name
               FROM borrower_info
               WHERE document_id = ? AND opi_checked_at IS NOT NULL""",
            (document_id,),
        )
        if rows:
            return dict(rows[0])
        return None
    finally:
        await db.close()


async def save_opi_result(
    document_id: str,
    has_debt: bool,
    debt_amount: float | None = None,
    full_name: str | None = None,
) -> None:
    """Save OPI check result to borrower_info."""
    now = datetime.now(timezone.utc).isoformat()
    db = await get_db()
    try:
        await db.execute(
            f"""INSERT INTO borrower_info (document_id, full_name, source)
                VALUES (?, ?, 'opi')
                ON CONFLICT(document_id) DO UPDATE SET
                    full_name = {_BORROWER_INFO_FULL_NAME_SQL},
                    source = {_BORROWER_INFO_SOURCE_SQL},
                    updated_at = datetime('now')
            """,
            (document_id, full_name),
        )
        await db.execute(
            """UPDATE borrower_info
               SET opi_has_debt = ?, opi_debt_amount = ?,
                   opi_checked_at = ?, opi_full_name = ?,
                   updated_at = datetime('now')
               WHERE document_id = ?""",
            (int(has_debt), debt_amount, now, full_name, document_id),
        )
        await db.commit()
    finally:
        await db.close()


async def upsert_borrower_from_investment(
    service: str,
    borrower_user_id: str,
    full_name: str | None = None,
    total_loans: int = 0,
    settled_loans: int = 0,
    overdue_loans: int = 0,
    avg_rating: float | None = None,
    total_invested: float | None = None,
) -> None:
    """Upsert borrower mapping + investment stats from investment history data."""
    if full_name:
        full_name = full_name.upper()
    db = await get_db()
    try:
        await db.execute(
            """INSERT INTO borrowers
                   (service, borrower_user_id, full_name, total_loans, settled_loans,
                    overdue_loans, avg_rating, total_invested)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(service, borrower_user_id) DO UPDATE SET
                   full_name = COALESCE(excluded.full_name, borrowers.full_name),
                   total_loans = excluded.total_loans,
                   settled_loans = excluded.settled_loans,
                   overdue_loans = excluded.overdue_loans,
                   avg_rating = excluded.avg_rating,
                   total_invested = excluded.total_invested,
                   last_seen = datetime('now')
            """,
            (service, borrower_user_id, full_name, total_loans, settled_loans,
             overdue_loans, avg_rating, total_invested),
        )
        await db.commit()
    finally:
        await db.close()


async def get_stale_opi_documents(max_age_days: int = 3) -> list[dict]:
    """Get borrower_info entries where OPI check is stale or never done."""
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """SELECT document_id, full_name
               FROM borrower_info
               WHERE document_id IS NOT NULL AND LENGTH(document_id) = 14
                 AND (opi_checked_at IS NULL
                      OR opi_checked_at < datetime('now', ?))
               ORDER BY opi_checked_at ASC NULLS FIRST""",
            (f"-{max_age_days} days",),
        )
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def get_missing_opi_candidates(min_age_days: int = 10, limit: int = 200) -> list[dict]:
    """Return borrowers with a known document_id but no OPI data after enough time has passed."""
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT
                b.document_id,
                MAX(b.full_name) AS full_name,
                GROUP_CONCAT(DISTINCT b.service) AS services,
                MIN(b.first_seen) AS first_seen,
                MAX(b.last_seen) AS last_seen,
                SUM(COALESCE(b.total_loans, 0)) AS total_loans,
                SUM(COALESCE(b.total_invested, 0)) AS total_invested,
                MAX(bi.loan_status) AS loan_status,
                MAX(bi.source) AS source
            FROM borrowers b
            LEFT JOIN borrower_info bi ON bi.document_id = b.document_id
            WHERE b.document_id IS NOT NULL
              AND LENGTH(b.document_id) = 14
              AND (bi.opi_checked_at IS NULL OR bi.opi_checked_at = '')
              AND b.first_seen <= datetime('now', ?)
            GROUP BY b.document_id
            ORDER BY MIN(b.first_seen) ASC, b.document_id ASC
            LIMIT ?
            """,
            (f"-{min_age_days} days", limit),
        )
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def get_borrowers_count() -> int:
    db = await get_db()
    try:
        rows = await db.execute_fetchall("SELECT COUNT(*) as cnt FROM borrower_info")
        return rows[0]["cnt"] if rows else 0
    finally:
        await db.close()


async def get_borrowers_stats() -> dict:
    """Return summary stats for borrower_info + borrowers tables."""
    db = await get_db()
    try:
        # borrower_info stats
        rows = await db.execute_fetchall(
            """SELECT COUNT(*) as total,
                      SUM(CASE WHEN opi_has_debt = 1 THEN 1 ELSE 0 END) as with_debt,
                      SUM(CASE WHEN opi_has_debt = 0 THEN 1 ELSE 0 END) as no_debt,
                      SUM(CASE WHEN opi_checked_at IS NOT NULL THEN 1 ELSE 0 END) as opi_checked,
                      SUM(CASE WHEN total_invested > 0 THEN 1 ELSE 0 END) as with_investments
               FROM borrower_info"""
        )
        result = dict(rows[0]) if rows else {}
        # borrowers mapping count
        rows2 = await db.execute_fetchall(
            """SELECT COUNT(*) as mappings,
                      SUM(CASE WHEN document_id IS NOT NULL THEN 1 ELSE 0 END) as with_document
               FROM borrowers"""
        )
        if rows2:
            result["mappings"] = rows2[0]["mappings"]
            result["with_document"] = rows2[0]["with_document"]
        return result
    finally:
        await db.close()


async def upsert_overdue_case(
    *,
    chat_id: int,
    credential_id: int | None,
    service: str,
    external_id: str,
    loan_id: str | None = None,
    loan_number: str | None = None,
    account_label: str | None = None,
    borrower_user_id: str | None = None,
    document_id: str | None = None,
    full_name: str | None = None,
    display_name: str | None = None,
    issued_at: str | None = None,
    due_at: str | None = None,
    overdue_started_at: str | None = None,
    days_overdue: int | None = None,
    amount: float | None = None,
    principal_outstanding: float | None = None,
    accrued_percent: float | None = None,
    fine_outstanding: float | None = None,
    total_due: float | None = None,
    status: str | None = None,
    contract_url: str | None = None,
    loan_url: str | None = None,
    raw_data: dict | None = None,
) -> int:
    if full_name:
        full_name = full_name.upper()
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO overdue_cases (
                chat_id, credential_id, service, external_id, loan_id, loan_number,
                account_label, borrower_user_id, document_id, full_name, display_name,
                issued_at, due_at, overdue_started_at, days_overdue, amount,
                principal_outstanding, accrued_percent, fine_outstanding, total_due,
                status, contract_url, loan_url, raw_data, is_active, last_synced_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))
            ON CONFLICT(chat_id, service, external_id) DO UPDATE SET
                credential_id = excluded.credential_id,
                loan_id = COALESCE(excluded.loan_id, overdue_cases.loan_id),
                loan_number = COALESCE(excluded.loan_number, overdue_cases.loan_number),
                account_label = COALESCE(excluded.account_label, overdue_cases.account_label),
                borrower_user_id = COALESCE(excluded.borrower_user_id, overdue_cases.borrower_user_id),
                document_id = COALESCE(excluded.document_id, overdue_cases.document_id),
                full_name = COALESCE(excluded.full_name, overdue_cases.full_name),
                display_name = COALESCE(excluded.display_name, overdue_cases.display_name),
                issued_at = COALESCE(excluded.issued_at, overdue_cases.issued_at),
                due_at = COALESCE(excluded.due_at, overdue_cases.due_at),
                overdue_started_at = COALESCE(excluded.overdue_started_at, overdue_cases.overdue_started_at),
                days_overdue = COALESCE(excluded.days_overdue, overdue_cases.days_overdue),
                amount = COALESCE(excluded.amount, overdue_cases.amount),
                principal_outstanding = COALESCE(excluded.principal_outstanding, overdue_cases.principal_outstanding),
                accrued_percent = COALESCE(excluded.accrued_percent, overdue_cases.accrued_percent),
                fine_outstanding = COALESCE(excluded.fine_outstanding, overdue_cases.fine_outstanding),
                total_due = COALESCE(excluded.total_due, overdue_cases.total_due),
                status = COALESCE(excluded.status, overdue_cases.status),
                contract_url = COALESCE(excluded.contract_url, overdue_cases.contract_url),
                loan_url = COALESCE(excluded.loan_url, overdue_cases.loan_url),
                raw_data = excluded.raw_data,
                is_active = 1,
                last_synced_at = datetime('now'),
                updated_at = datetime('now')
            """,
            (
                chat_id,
                credential_id,
                service,
                external_id,
                loan_id,
                loan_number,
                account_label,
                borrower_user_id,
                document_id,
                full_name,
                display_name,
                issued_at,
                due_at,
                overdue_started_at,
                days_overdue,
                amount,
                principal_outstanding,
                accrued_percent,
                fine_outstanding,
                total_due,
                status,
                contract_url,
                loan_url,
                json.dumps(raw_data or {}, ensure_ascii=False),
            ),
        )
        await db.commit()
        rows = await db.execute_fetchall(
            "SELECT id FROM overdue_cases WHERE chat_id = ? AND service = ? AND external_id = ?",
            (chat_id, service, external_id),
        )
        return int(rows[0]["id"])
    finally:
        await db.close()


async def deactivate_missing_overdue_cases(
    chat_id: int,
    service: str,
    active_external_ids: list[str],
    credential_id: int | None = None,
) -> None:
    db = await get_db()
    try:
        where = "WHERE chat_id = ? AND service = ?"
        params: list = [chat_id, service]
        if credential_id is not None:
            where += " AND credential_id = ?"
            params.append(credential_id)
        if active_external_ids:
            placeholders = ",".join("?" for _ in active_external_ids)
            await db.execute(
                f"""
                UPDATE overdue_cases
                SET is_active = 0, updated_at = datetime('now')
                {where}
                  AND external_id NOT IN ({placeholders})
                """,
                (*params, *active_external_ids),
            )
        else:
            await db.execute(
                """
                UPDATE overdue_cases
                SET is_active = 0, updated_at = datetime('now')
                """ + where,
                params,
            )
        await db.commit()
    finally:
        await db.close()


async def list_overdue_cases(chat_id: int, active_only: bool = True, limit: int = 100) -> list[dict]:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            f"""
            SELECT *
            FROM overdue_cases
            WHERE chat_id = ?
              {'AND is_active = 1' if active_only else ''}
            ORDER BY COALESCE(days_overdue, 0) DESC, COALESCE(total_due, 0) DESC, updated_at DESC
            LIMIT ?
            """,
            (chat_id, limit),
        )
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def get_overdue_case(case_id: int, chat_id: int) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT oc.*, c.login AS credential_login, c.label AS credential_label
            FROM overdue_cases oc
            LEFT JOIN credentials c ON c.id = oc.credential_id
            WHERE oc.id = ? AND oc.chat_id = ?
            """,
            (case_id, chat_id),
        )
        return dict(rows[0]) if rows else None
    finally:
        await db.close()


async def update_overdue_case_contacts(
    case_id: int,
    chat_id: int,
    *,
    borrower_address: str | None = None,
    borrower_zip: str | None = None,
    borrower_phone: str | None = None,
    borrower_email: str | None = None,
    voluntary_term_days: int | None = None,
) -> None:
    db = await get_db()
    try:
        await db.execute(
            """
            UPDATE overdue_cases
            SET borrower_address = COALESCE(?, borrower_address),
                borrower_zip = COALESCE(?, borrower_zip),
                borrower_phone = COALESCE(?, borrower_phone),
                borrower_email = COALESCE(?, borrower_email),
                voluntary_term_days = COALESCE(?, voluntary_term_days),
                updated_at = datetime('now')
            WHERE id = ? AND chat_id = ?
            """,
            (
                borrower_address,
                borrower_zip,
                borrower_phone,
                borrower_email,
                voluntary_term_days,
                case_id,
                chat_id,
            ),
        )
        await db.commit()
    finally:
        await db.close()


async def get_creditor_profile(chat_id: int) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM creditor_profiles WHERE chat_id = ?",
            (chat_id,),
        )
        return dict(rows[0]) if rows else None
    finally:
        await db.close()


async def get_credential_creditor_profile(chat_id: int, credential_id: int) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT ccp.*, c.service, c.login, c.label
            FROM credential_creditor_profiles ccp
            JOIN credentials c ON c.id = ccp.credential_id
            WHERE ccp.chat_id = ? AND ccp.credential_id = ?
            """,
            (chat_id, credential_id),
        )
        return dict(rows[0]) if rows else None
    finally:
        await db.close()


async def upsert_credential_creditor_profile(
    chat_id: int,
    credential_id: int,
    *,
    full_name: str | None,
    address: str | None,
    phone: str | None,
    email: str | None,
) -> None:
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO credential_creditor_profiles (
                chat_id, credential_id, full_name, address, phone, email, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(chat_id, credential_id) DO UPDATE SET
                full_name = excluded.full_name,
                address = excluded.address,
                phone = excluded.phone,
                email = excluded.email,
                updated_at = datetime('now')
            """,
            (chat_id, credential_id, full_name, address, phone, email),
        )
        await db.commit()
    finally:
        await db.close()


async def upsert_creditor_profile(
    chat_id: int,
    *,
    full_name: str | None,
    address: str | None,
    phone: str | None,
    email: str | None,
    payment_details: str | None = None,
    sms_sender: str | None = None,
) -> None:
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO creditor_profiles (
                chat_id, full_name, address, phone, email, payment_details, sms_sender, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(chat_id) DO UPDATE SET
                full_name = excluded.full_name,
                address = excluded.address,
                phone = excluded.phone,
                email = excluded.email,
                payment_details = excluded.payment_details,
                sms_sender = excluded.sms_sender,
                updated_at = datetime('now')
            """,
            (chat_id, full_name, address, phone, email, payment_details, sms_sender),
        )
        await db.commit()
    finally:
        await db.close()


async def get_user_signature_asset(chat_id: int) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            "SELECT * FROM user_signature_assets WHERE chat_id = ?",
            (chat_id,),
        )
        return dict(rows[0]) if rows else None
    finally:
        await db.close()


async def save_user_signature_asset(
    chat_id: int,
    *,
    file_path: str,
    mime_type: str | None = None,
    telegram_file_id: str | None = None,
    telegram_unique_id: str | None = None,
) -> None:
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO user_signature_assets (
                chat_id, file_path, mime_type, telegram_file_id, telegram_unique_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(chat_id) DO UPDATE SET
                file_path = excluded.file_path,
                mime_type = excluded.mime_type,
                telegram_file_id = excluded.telegram_file_id,
                telegram_unique_id = excluded.telegram_unique_id,
                updated_at = datetime('now')
            """,
            (chat_id, file_path, mime_type, telegram_file_id, telegram_unique_id),
        )
        await db.commit()
    finally:
        await db.close()


async def save_generated_document(
    overdue_case_id: int,
    chat_id: int,
    *,
    doc_type: str,
    file_path: str | None = None,
    text_content: str | None = None,
    payload: dict | None = None,
    missing_fields: list[str] | None = None,
) -> None:
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO generated_documents (
                overdue_case_id, chat_id, doc_type, file_path, text_content, payload_json, missing_fields
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                overdue_case_id,
                chat_id,
                doc_type,
                file_path,
                text_content,
                json.dumps(payload or {}, ensure_ascii=False),
                json.dumps(missing_fields or [], ensure_ascii=False),
            ),
        )
        await db.commit()
    finally:
        await db.close()
