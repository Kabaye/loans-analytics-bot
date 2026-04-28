from __future__ import annotations

import json
import logging
import re

import aiosqlite

from bot.config import DB_PATH

log = logging.getLogger(__name__)

_LEGAL_FULL_NAME_RE = re.compile(r"^[A-ZА-ЯЁ]+(?:[-'][A-ZА-ЯЁ]+)?(?:\s+[A-ZА-ЯЁ]+(?:[-'][A-ZА-ЯЁ]+)?){2,}$")
_DOCUMENT_ID_RE = re.compile(r"\b[0-9A-Z]{14}\b")
_EMAIL_RE = re.compile(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", re.IGNORECASE)


def _normalize_compare(value: str | None) -> str:
    return (value or "").strip().upper().replace("Ё", "Е")


def _is_probable_legal_full_name(value: str | None) -> bool:
    normalized = (value or "").strip().upper()
    if not normalized or any(char.isdigit() for char in normalized):
        return False
    return bool(_LEGAL_FULL_NAME_RE.fullmatch(normalized))


def _extract_document_id(value: str | None) -> str | None:
    text = str(value or "").strip().upper()
    if not text:
        return None
    match = _DOCUMENT_ID_RE.search(text)
    return match.group(0) if match else None


def _extract_email(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = _EMAIL_RE.search(text)
    return match.group(1) if match else None


def _extract_registration_address(value: str | None) -> str | None:
    for raw_line in str(value or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        upper_line = line.upper()
        if upper_line.startswith("АДРЕС РЕГИСТРАЦИИ:"):
            return line.split(":", 1)[1].strip() or None
    return None


def _extract_document_notes(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    notes: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        upper_line = line.upper()
        if _DOCUMENT_ID_RE.fullmatch(upper_line):
            continue
        if _EMAIL_RE.search(line):
            continue
        if upper_line.startswith("АДРЕС РЕГИСТРАЦИИ:"):
            continue
        notes.append(line)
    return "\n".join(notes) if notes else None


def _merge_notes(existing: str | None, incoming: str | None) -> str | None:
    existing_parts = [part.strip() for part in str(existing or "").splitlines() if part.strip()]
    merged = list(existing_parts)
    seen = {_normalize_compare(part) for part in merged}
    for part in [segment.strip() for segment in str(incoming or "").splitlines() if segment.strip()]:
        key = _normalize_compare(part)
        if key in seen:
            continue
        seen.add(key)
        merged.append(part)
    return "\n".join(merged) if merged else None


def _parse_display_names(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        raw = json.loads(value)
    except json.JSONDecodeError:
        raw = [value]
    result: list[str] = []
    seen: set[str] = set()
    for item in raw if isinstance(raw, list) else [value]:
        text = str(item or "").strip()
        key = _normalize_compare(text)
        if not text or not key or key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


def _merge_display_names(existing: str | None, incoming: str | None) -> str | None:
    items = _parse_display_names(existing)
    seen = {_normalize_compare(item) for item in items}
    text = str(incoming or "").strip()
    key = _normalize_compare(text)
    if text and key:
        if key in seen:
            items = [item for item in items if _normalize_compare(item) != key]
        items.append(text)
    return json.dumps(items, ensure_ascii=False) if items else None


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    return db


async def _try_add_column(db: aiosqlite.Connection, table: str, column_definition: str) -> None:
    try:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column_definition}")
    except aiosqlite.OperationalError:
        pass


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

        CREATE TABLE IF NOT EXISTS borrowers (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            service             TEXT NOT NULL,
            borrower_user_id    TEXT NOT NULL,
            document_id         TEXT,
            full_name           TEXT,
            display_names       TEXT,
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

        CREATE TABLE IF NOT EXISTS borrower_info (
            document_id         TEXT PRIMARY KEY,
            full_name           TEXT,
            loan_status         TEXT,
            loan_status_details_json TEXT,
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
            source_account_tag  TEXT,
            borrower_address    TEXT,
            borrower_zip        TEXT,
            contact_source      TEXT,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        );

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

        CREATE TABLE IF NOT EXISTS credential_signature_assets (
            chat_id             INTEGER NOT NULL,
            credential_id       INTEGER NOT NULL,
            file_path           TEXT NOT NULL,
            mime_type           TEXT,
            telegram_file_id    TEXT,
            telegram_unique_id  TEXT,
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (chat_id, credential_id),
            FOREIGN KEY (chat_id) REFERENCES users(chat_id) ON DELETE CASCADE,
            FOREIGN KEY (credential_id) REFERENCES credentials(id) ON DELETE CASCADE
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

        for definition in ("first_name TEXT", "last_name TEXT"):
            await _try_add_column(db, "users", definition)
        await _try_add_column(db, "users", "last_seen_version TEXT")

        for definition in (
            "borrower_phone TEXT",
            "borrower_email TEXT",
            "borrower_address TEXT",
            "borrower_zip TEXT",
            "contact_source TEXT",
            "loan_status_details_json TEXT",
            "source_account_tag TEXT",
        ):
            await _try_add_column(db, "borrower_info", definition)

        await _try_add_column(db, "borrowers", "display_names TEXT")

        for definition in ("require_employed INTEGER", "require_income_confirmed INTEGER"):
            await _try_add_column(db, "subscriptions", definition)

        for definition in ("night_paused INTEGER DEFAULT 0", "min_settled_loans INTEGER"):
            await _try_add_column(db, "subscriptions", definition)

        await _try_add_column(db, "credentials", "label TEXT")

        await db.execute("""
            INSERT OR IGNORE INTO credential_creditor_profiles (
                chat_id, credential_id, full_name, address, phone, email, created_at, updated_at
            )
            SELECT c.chat_id, c.id, cp.full_name, cp.address, cp.phone, cp.email, datetime('now'), datetime('now')
            FROM credentials c
            JOIN creditor_profiles cp ON cp.chat_id = c.chat_id
        """)

        try:
            rows = await db.execute_fetchall(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='credentials'"
            )
            table_sql = (rows[0]["sql"] or "") if rows else ""
            if "UNIQUE(chat_id, service)" in table_sql and "UNIQUE(chat_id, service, login)" not in table_sql:
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
                log.info("Migrated credentials: UNIQUE(chat_id, service) -> UNIQUE(chat_id, service, login)")
        except aiosqlite.OperationalError:
            pass

        for table_name in (
            "borrower_identities",
            "known_borrowers",
            "investment_history",
            "notified_loans",
            "last_check",
            "session_cookies",
        ):
            await db.execute(f"DROP TABLE IF EXISTS {table_name}")

        await db.execute("UPDATE borrower_info SET source = 'search' WHERE source IN ('added', 'opi')")
        await db.execute("UPDATE borrower_info SET contact_source = 'manual' WHERE contact_source = 'added'")

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

        await db.execute("""
            UPDATE borrower_info
            SET source_account_tag = substr(source, length('finkit_archive_') + 1),
                source = 'finkit_borrow'
            WHERE source LIKE 'finkit_archive_%'
        """)
        await db.execute("""
            UPDATE borrower_info
            SET source_account_tag = substr(source, length('zaimis_archive_') + 1),
                source = 'zaimis_borrow'
            WHERE source LIKE 'zaimis_archive_%'
        """)
        await db.execute("""
            UPDATE borrower_info
            SET source_account_tag = substr(source, length('finkit_overdue_pdf_') + 1),
                source = 'finkit_investment_detail'
            WHERE source LIKE 'finkit_overdue_pdf_%'
        """)
        await db.execute("""
            UPDATE borrower_info
            SET source_account_tag = substr(source, length('finkit_investment_detail_') + 1),
                source = 'finkit_investment_detail'
            WHERE source LIKE 'finkit_investment_detail_%'
        """)
        await db.execute("""
            UPDATE borrower_info
            SET source = 'finkit_investment_detail'
            WHERE source = 'finkit_claim_pdf'
        """)
        await db.execute("""
            UPDATE borrower_info
            SET source_account_tag = substr(contact_source, length('finkit_investment_detail_') + 1),
                contact_source = 'finkit_investment_detail'
            WHERE contact_source LIKE 'finkit_investment_detail_%'
        """)
        await db.execute("""
            UPDATE borrower_info
            SET contact_source = 'finkit_investment_detail'
            WHERE contact_source = 'finkit_claim_pdf'
        """)

        try:
            await db.execute("SELECT opi_has_debt FROM borrowers LIMIT 1")
            rows = await db.execute_fetchall(
                """
                SELECT document_id, full_name, opi_has_debt, opi_debt_amount,
                       opi_checked_at, opi_full_name, total_invested
                FROM borrowers
                WHERE document_id IS NOT NULL AND LENGTH(document_id) = 14
                """
            )
            migrated = 0
            for row in rows:
                await db.execute(
                    """
                    INSERT INTO borrower_info
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
                    (
                        row["document_id"],
                        row["full_name"],
                        row["opi_has_debt"],
                        row["opi_debt_amount"],
                        row["opi_checked_at"],
                        row["opi_full_name"],
                        row["total_invested"],
                    ),
                )
                migrated += 1

            await db.execute("""
                CREATE TABLE IF NOT EXISTS borrowers_new (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    service             TEXT NOT NULL,
                    borrower_user_id    TEXT NOT NULL,
                    document_id         TEXT,
                    full_name           TEXT,
                    display_names       TEXT,
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
                    (id, service, borrower_user_id, document_id, full_name, display_names,
                     total_loans, settled_loans, overdue_loans, avg_rating, total_invested,
                     first_seen, last_seen)
                SELECT id, service, borrower_user_id, document_id, full_name, display_names,
                       total_loans, settled_loans, overdue_loans, avg_rating, total_invested,
                       first_seen, last_seen
                FROM borrowers
            """)
            await db.execute("DROP TABLE borrowers")
            await db.execute("ALTER TABLE borrowers_new RENAME TO borrowers")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_borrowers_doc ON borrowers(document_id)")
            log.info("Migrated %d borrowers -> borrower_info, dropped OPI columns from borrowers", migrated)
        except aiosqlite.OperationalError:
            pass

        rows = await db.execute_fetchall(
            """
            SELECT id, service, document_id, full_name, display_names
            FROM borrowers
            WHERE service = 'zaimis'
            """
        )
        migrated_display_names = 0
        for row in rows:
            full_name = str(row["full_name"] or "").strip()
            if not full_name or _is_probable_legal_full_name(full_name):
                continue
            merged = _merge_display_names(row["display_names"], full_name)
            await db.execute(
                """
                UPDATE borrowers
                SET display_names = ?,
                    full_name = NULL
                WHERE id = ?
                """,
                (merged, row["id"]),
            )
            migrated_display_names += 1
        if migrated_display_names:
            log.info("Backfilled display_names for %d Zaimis borrowers", migrated_display_names)

        rows = await db.execute_fetchall(
            """
            SELECT *
            FROM borrower_info
            WHERE document_id IS NOT NULL
              AND LENGTH(TRIM(document_id)) != 14
            """
        )
        cleaned_document_ids = 0
        for row in rows:
            raw_document_id = str(row["document_id"] or "").strip()
            clean_document_id = _extract_document_id(raw_document_id)
            if not clean_document_id:
                continue
            existing_rows = await db.execute_fetchall(
                "SELECT * FROM borrower_info WHERE document_id = ? LIMIT 1",
                (clean_document_id,),
            )
            existing = dict(existing_rows[0]) if existing_rows else None
            merged_notes = _merge_notes(
                (existing or {}).get("notes"),
                row["notes"],
            )
            merged_notes = _merge_notes(merged_notes, _extract_document_notes(raw_document_id))
            merged_email = (existing or {}).get("borrower_email") or _extract_email(raw_document_id)
            merged_address = (existing or {}).get("borrower_address") or _extract_registration_address(raw_document_id)
            merged_zip = (existing or {}).get("borrower_zip")
            if merged_address and not merged_zip:
                zip_match = re.search(r"\b\d{6}\b", merged_address)
                if zip_match:
                    merged_zip = zip_match.group(0)
            await db.execute(
                """
                INSERT INTO borrower_info (
                    document_id, full_name, loan_status, loan_status_details_json, sum_category, rating,
                    notes, last_loan_date, loan_count, opi_has_debt, opi_debt_amount, opi_checked_at,
                    opi_full_name, total_invested, source, source_account_tag, borrower_address,
                    borrower_zip, contact_source, borrower_email, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(document_id) DO UPDATE SET
                    full_name = COALESCE(borrower_info.full_name, excluded.full_name),
                    loan_status = COALESCE(borrower_info.loan_status, excluded.loan_status),
                    loan_status_details_json = COALESCE(borrower_info.loan_status_details_json, excluded.loan_status_details_json),
                    sum_category = COALESCE(borrower_info.sum_category, excluded.sum_category),
                    rating = COALESCE(borrower_info.rating, excluded.rating),
                    notes = COALESCE(excluded.notes, borrower_info.notes),
                    last_loan_date = COALESCE(borrower_info.last_loan_date, excluded.last_loan_date),
                    loan_count = CASE
                        WHEN borrower_info.loan_count IS NULL OR borrower_info.loan_count = 0 THEN excluded.loan_count
                        WHEN excluded.loan_count IS NULL THEN borrower_info.loan_count
                        ELSE MAX(borrower_info.loan_count, excluded.loan_count)
                    END,
                    opi_has_debt = COALESCE(borrower_info.opi_has_debt, excluded.opi_has_debt),
                    opi_debt_amount = COALESCE(borrower_info.opi_debt_amount, excluded.opi_debt_amount),
                    opi_checked_at = COALESCE(borrower_info.opi_checked_at, excluded.opi_checked_at),
                    opi_full_name = COALESCE(borrower_info.opi_full_name, excluded.opi_full_name),
                    total_invested = COALESCE(borrower_info.total_invested, excluded.total_invested),
                    source = COALESCE(borrower_info.source, excluded.source),
                    source_account_tag = COALESCE(borrower_info.source_account_tag, excluded.source_account_tag),
                    borrower_address = COALESCE(borrower_info.borrower_address, excluded.borrower_address),
                    borrower_zip = COALESCE(borrower_info.borrower_zip, excluded.borrower_zip),
                    contact_source = COALESCE(borrower_info.contact_source, excluded.contact_source),
                    borrower_email = COALESCE(borrower_info.borrower_email, excluded.borrower_email),
                    updated_at = datetime('now')
                """,
                (
                    clean_document_id,
                    (existing or {}).get("full_name") or row["full_name"],
                    (existing or {}).get("loan_status") or row["loan_status"],
                    (existing or {}).get("loan_status_details_json") or row["loan_status_details_json"],
                    (existing or {}).get("sum_category") or row["sum_category"],
                    (existing or {}).get("rating") if (existing or {}).get("rating") is not None else row["rating"],
                    merged_notes,
                    (existing or {}).get("last_loan_date") or row["last_loan_date"],
                    max(int((existing or {}).get("loan_count") or 0), int(row["loan_count"] or 0)) or None,
                    (existing or {}).get("opi_has_debt") if (existing or {}).get("opi_has_debt") is not None else row["opi_has_debt"],
                    (existing or {}).get("opi_debt_amount") if (existing or {}).get("opi_debt_amount") is not None else row["opi_debt_amount"],
                    (existing or {}).get("opi_checked_at") or row["opi_checked_at"],
                    (existing or {}).get("opi_full_name") or row["opi_full_name"],
                    (existing or {}).get("total_invested") if (existing or {}).get("total_invested") is not None else row["total_invested"],
                    (existing or {}).get("source") or row["source"],
                    (existing or {}).get("source_account_tag") or row["source_account_tag"],
                    merged_address or row["borrower_address"],
                    merged_zip or row["borrower_zip"],
                    (existing or {}).get("contact_source") or row["contact_source"],
                    merged_email or row["borrower_email"],
                ),
            )
            if raw_document_id != clean_document_id:
                await db.execute("DELETE FROM borrower_info WHERE document_id = ?", (raw_document_id,))
            cleaned_document_ids += 1
        if cleaned_document_ids:
            log.info("Cleaned %d dirty borrower_info document_id rows", cleaned_document_ids)

        for service, enabled, interval, hour_start, hour_end in [
            ("kapusta", 1, 600, 8, 23),
            ("finkit", 1, 60, 0, 24),
            ("zaimis", 1, 60, 0, 24),
        ]:
            await db.execute(
                """
                INSERT OR IGNORE INTO site_settings
                   (service, polling_enabled, poll_interval, active_hour_start, active_hour_end)
                   VALUES (?, ?, ?, ?, ?)
                """,
                (service, enabled, interval, hour_start, hour_end),
            )

        await db.execute("DELETE FROM site_settings WHERE service = 'mongo'")

        await db.commit()
        log.info("Database initialized at %s", DB_PATH)
    finally:
        await db.close()


__all__ = ["get_db", "init_db"]
