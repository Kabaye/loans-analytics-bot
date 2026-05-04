from __future__ import annotations

import logging

import aiosqlite

from bot.config import DB_PATH

log = logging.getLogger(__name__)


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
            first_name  TEXT,
            last_name   TEXT,
            last_seen_version TEXT,
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
            require_employed INTEGER,
            require_income_confirmed INTEGER,
            night_paused INTEGER DEFAULT 0,
            min_settled_loans INTEGER,
            is_active   INTEGER DEFAULT 1,
            created_at  TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (chat_id) REFERENCES users(chat_id)
        );

        CREATE TABLE IF NOT EXISTS borrowers (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            service             TEXT NOT NULL,
            borrower_user_id    TEXT NOT NULL,
            document_id         TEXT,
            display_names       TEXT,
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
            source              TEXT DEFAULT 'auto',
            source_account_tag  TEXT,
            borrower_phone      TEXT,
            borrower_email      TEXT,
            borrower_address    TEXT,
            borrower_addresses_json TEXT,
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

        await db.commit()
        log.info("Database initialized at %s", DB_PATH)
    finally:
        await db.close()


__all__ = ["get_db", "init_db"]
