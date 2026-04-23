import json

from bot.repositories.db import get_db


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
        params: list[object] = [chat_id, service]
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
        return [dict(row) for row in rows]
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


async def clear_finkit_suspect_address(
    case_id: int,
    imported_address: str | None,
    imported_zip: str | None,
) -> None:
    if not imported_address and not imported_zip:
        return
    db = await get_db()
    try:
        await db.execute(
            """
            UPDATE overdue_cases
            SET borrower_address = CASE
                    WHEN ? IS NOT NULL AND borrower_address = ? THEN NULL
                    ELSE borrower_address
                END,
                borrower_zip = CASE
                    WHEN ? IS NOT NULL AND borrower_zip = ? THEN NULL
                    ELSE borrower_zip
                END,
                updated_at = datetime('now')
            WHERE id = ?
            """,
            (
                imported_address,
                imported_address,
                imported_zip,
                imported_zip,
                case_id,
            ),
        )
        await db.commit()
    finally:
        await db.close()

__all__ = [
    "clear_finkit_suspect_address",
    "deactivate_missing_overdue_cases",
    "get_creditor_profile",
    "get_credential_creditor_profile",
    "get_overdue_case",
    "get_user_signature_asset",
    "list_overdue_cases",
    "save_generated_document",
    "save_user_signature_asset",
    "update_overdue_case_contacts",
    "upsert_creditor_profile",
    "upsert_credential_creditor_profile",
    "upsert_overdue_case",
]
