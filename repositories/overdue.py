import json

from bot.repositories.db import get_db
from bot.utils.borrower_address import sanitize_borrower_address


def _coalesce_non_empty(*values):
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def _parse_raw_payload(value) -> dict:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        return json.loads(value)
    except Exception:
        return {}


def _raw_borrower_full_name(payload: dict) -> str | None:
    detail = payload.get("detail") or {}
    list_payload = payload.get("list") or payload.get("order") or {}
    counterparty = detail.get("counterparty") or list_payload.get("counterparty") or {}
    full_name = _coalesce_non_empty(
        detail.get("borrower_full_name"),
        list_payload.get("borrower_full_name"),
        counterparty.get("fullName"),
    )
    return full_name.upper() if full_name else None


def _raw_borrower_display_name(payload: dict) -> str | None:
    detail = payload.get("detail") or {}
    list_payload = payload.get("list") or payload.get("order") or {}
    counterparty = detail.get("counterparty") or list_payload.get("counterparty") or {}
    return _coalesce_non_empty(
        detail.get("borrower_short_name"),
        list_payload.get("borrower_short_name"),
        counterparty.get("displayName"),
    )


def _raw_contact_overrides(payload: dict) -> dict[str, str]:
    value = payload.get("contact_overrides")
    if not isinstance(value, dict):
        return {}
    result: dict[str, str] = {}
    for key in ("borrower_address", "borrower_zip", "borrower_phone", "borrower_email"):
        normalized = _coalesce_non_empty(value.get(key))
        if normalized:
            result[key] = normalized
    return result


def _apply_contact_overrides(
    payload: dict,
    *,
    borrower_address: str | None = None,
    borrower_zip: str | None = None,
    borrower_phone: str | None = None,
    borrower_email: str | None = None,
) -> None:
    overrides = _raw_contact_overrides(payload)
    for key, value in {
        "borrower_address": borrower_address,
        "borrower_zip": borrower_zip,
        "borrower_phone": borrower_phone,
        "borrower_email": borrower_email,
    }.items():
        normalized = _coalesce_non_empty(value)
        if normalized:
            overrides[key] = normalized
    if overrides:
        payload["contact_overrides"] = overrides


def _current_display_name(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        items = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            items = [text]
        else:
            items = parsed if isinstance(parsed, list) else [text]
    normalized = [str(item).strip() for item in items if str(item or "").strip()]
    return normalized[-1] if normalized else None


def _display_names(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw_items = value
    else:
        text = str(value).strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            raw_items = [text]
        else:
            raw_items = parsed if isinstance(parsed, list) else [text]
    result: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _apply_borrower_overlay(payload: dict) -> dict:
    raw_payload = _parse_raw_payload(payload.get("raw_data"))
    raw_full_name = _raw_borrower_full_name(raw_payload)
    raw_display_name = _raw_borrower_display_name(raw_payload)
    contact_overrides = _raw_contact_overrides(raw_payload)
    mapped_display_name = _current_display_name(payload.get("borrower_display_names"))
    payload["display_name"] = mapped_display_name or raw_display_name or payload.get("display_name")
    payload["full_name"] = payload.get("borrower_full_name") or payload.get("display_name") or raw_full_name or payload.get("full_name")
    payload["borrower_address"] = sanitize_borrower_address(
        payload.get("effective_borrower_address") or contact_overrides.get("borrower_address") or payload.get("borrower_address"),
        payload.get("full_name") or raw_full_name,
    )
    payload["borrower_zip"] = payload.get("effective_borrower_zip") or contact_overrides.get("borrower_zip") or payload.get("borrower_zip")
    payload["borrower_phone"] = payload.get("effective_borrower_phone") or contact_overrides.get("borrower_phone") or payload.get("borrower_phone")
    payload["borrower_email"] = payload.get("effective_borrower_email") or contact_overrides.get("borrower_email") or payload.get("borrower_email")
    payload.pop("borrower_full_name", None)
    payload.pop("borrower_display_names", None)
    payload.pop("effective_borrower_address", None)
    payload.pop("effective_borrower_zip", None)
    payload.pop("effective_borrower_phone", None)
    payload.pop("effective_borrower_email", None)
    return payload


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
    raw_data: dict | None = None,
) -> int:
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO overdue_cases (
                chat_id, credential_id, service, external_id, loan_id, loan_number,
                account_label, borrower_user_id, document_id,
                issued_at, due_at, overdue_started_at, days_overdue, amount,
                principal_outstanding, accrued_percent, fine_outstanding, total_due,
                status, raw_data, is_active, last_synced_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))
            ON CONFLICT(chat_id, service, external_id) DO UPDATE SET
                credential_id = excluded.credential_id,
                loan_id = COALESCE(excluded.loan_id, overdue_cases.loan_id),
                loan_number = COALESCE(excluded.loan_number, overdue_cases.loan_number),
                account_label = COALESCE(excluded.account_label, overdue_cases.account_label),
                borrower_user_id = COALESCE(excluded.borrower_user_id, overdue_cases.borrower_user_id),
                document_id = COALESCE(excluded.document_id, overdue_cases.document_id),
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
            SELECT oc.*,
                   NULLIF(TRIM(bi.full_name), '') AS borrower_full_name,
                   b.display_names AS borrower_display_names,
                   NULLIF(TRIM(bi.borrower_address), '') AS effective_borrower_address,
                   NULLIF(TRIM(bi.borrower_zip), '') AS effective_borrower_zip,
                   NULLIF(TRIM(bi.borrower_phone), '') AS effective_borrower_phone,
                   NULLIF(TRIM(bi.borrower_email), '') AS effective_borrower_email
            FROM overdue_cases oc
            LEFT JOIN borrowers b ON b.service = oc.service AND b.borrower_user_id = oc.borrower_user_id
            LEFT JOIN borrower_info bi ON bi.document_id = oc.document_id
            WHERE oc.chat_id = ?
              {'AND is_active = 1' if active_only else ''}
            ORDER BY COALESCE(oc.days_overdue, 0) DESC, COALESCE(oc.total_due, 0) DESC, oc.updated_at DESC
            LIMIT ?
            """,
            (chat_id, limit),
        )
        return [_apply_borrower_overlay(dict(row)) for row in rows]
    finally:
        await db.close()


async def get_overdue_case(case_id: int, chat_id: int) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT oc.*, c.login AS credential_login, c.label AS credential_label,
                   NULLIF(TRIM(bi.full_name), '') AS borrower_full_name,
                   b.display_names AS borrower_display_names,
                   NULLIF(TRIM(bi.borrower_address), '') AS effective_borrower_address,
                   NULLIF(TRIM(bi.borrower_zip), '') AS effective_borrower_zip,
                   NULLIF(TRIM(bi.borrower_phone), '') AS effective_borrower_phone,
                   NULLIF(TRIM(bi.borrower_email), '') AS effective_borrower_email
            FROM overdue_cases oc
            LEFT JOIN credentials c ON c.id = oc.credential_id
            LEFT JOIN borrowers b ON b.service = oc.service AND b.borrower_user_id = oc.borrower_user_id
            LEFT JOIN borrower_info bi ON bi.document_id = oc.document_id
            WHERE oc.id = ? AND oc.chat_id = ?
            """,
            (case_id, chat_id),
        )
        return _apply_borrower_overlay(dict(rows[0])) if rows else None
    finally:
        await db.close()


async def lookup_latest_borrower_contacts(document_id: str) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT oc.*,
                   NULLIF(TRIM(bi.full_name), '') AS full_name,
                   b.display_names AS borrower_display_names
            FROM overdue_cases oc
            LEFT JOIN borrowers b ON b.service = oc.service AND b.borrower_user_id = oc.borrower_user_id
            LEFT JOIN borrower_info bi ON bi.document_id = oc.document_id
            WHERE oc.document_id = ?
            ORDER BY oc.is_active DESC, oc.updated_at DESC, oc.id DESC
            """,
            (document_id,),
        )
        if not rows:
            return None
        for raw_row in rows:
            row = dict(raw_row)
            display_names = _display_names(row.get("borrower_display_names"))
            payload = _parse_raw_payload(row.get("raw_data"))
            contact_overrides = _raw_contact_overrides(payload)
            phone = contact_overrides.get("borrower_phone") or row.get("borrower_phone")
            email = contact_overrides.get("borrower_email") or row.get("borrower_email")
            full_name = row.get("full_name") or _raw_borrower_full_name(payload)
            address = sanitize_borrower_address(
                contact_overrides.get("borrower_address") or row.get("borrower_address"),
                full_name,
            )
            zip_code = contact_overrides.get("borrower_zip") or row.get("borrower_zip")
            if not any((phone, email, address, zip_code)):
                continue
            source = payload.get("contact_source") or row.get("service")
            return {
                "document_id": row.get("document_id"),
                "full_name": full_name,
                "display_names": display_names,
                "current_display_name": _current_display_name(display_names) or _raw_borrower_display_name(payload),
                "service": row.get("service"),
                "phone": phone,
                "email": email,
                "address": address,
                "zip": zip_code,
                "source": source,
            }
        return None
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
    postal_lookup: dict | None = None,
    contact_source: str | None = None,
    source: str | None = None,
) -> None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT oc.document_id,
                   oc.raw_data,
                   NULLIF(TRIM(bi.full_name), '') AS borrower_full_name
            FROM overdue_cases oc
            LEFT JOIN borrowers b ON b.service = oc.service AND b.borrower_user_id = oc.borrower_user_id
            LEFT JOIN borrower_info bi ON bi.document_id = oc.document_id
            WHERE oc.id = ? AND oc.chat_id = ?
            """,
            (case_id, chat_id),
        )
        if not rows:
            return
        document_id = str(rows[0]["document_id"] or "").strip() or None
        raw_payload = _parse_raw_payload(rows[0]["raw_data"])
        case_full_name = _coalesce_non_empty(
            rows[0]["borrower_full_name"],
            _raw_borrower_full_name(raw_payload),
        )
        existing_info = None
        info_full_name = None
        persist_in_info = False
        if document_id:
            info_rows = await db.execute_fetchall(
                "SELECT document_id, full_name, source FROM borrower_info WHERE document_id = ? LIMIT 1",
                (document_id,),
            )
            existing_info = dict(info_rows[0]) if info_rows else None
            info_full_name = str((existing_info or {}).get("full_name") or case_full_name or "").strip() or None
            persist_in_info = bool(existing_info or info_full_name)
        borrower_address = sanitize_borrower_address(
            borrower_address,
            info_full_name or case_full_name,
        )
        if postal_lookup is not None:
            raw_payload["postal_lookup"] = postal_lookup
        if contact_source:
            raw_payload["contact_source"] = contact_source
        _apply_contact_overrides(
            raw_payload,
            borrower_address=borrower_address,
            borrower_zip=borrower_zip,
            borrower_phone=borrower_phone,
            borrower_email=borrower_email,
        )
        await db.execute(
            """
            UPDATE overdue_cases
            SET raw_data = ?,
                updated_at = datetime('now')
            WHERE id = ? AND chat_id = ?
            """,
            (
                json.dumps(raw_payload, ensure_ascii=False),
                case_id,
                chat_id,
            ),
        )
        if persist_in_info:
            await db.execute(
                """
                INSERT INTO borrower_info (
                    document_id, full_name, borrower_address, borrower_zip, borrower_phone, borrower_email,
                    contact_source, source, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(document_id) DO UPDATE SET
                    full_name = COALESCE(borrower_info.full_name, excluded.full_name),
                    borrower_address = COALESCE(excluded.borrower_address, borrower_info.borrower_address),
                    borrower_zip = COALESCE(excluded.borrower_zip, borrower_info.borrower_zip),
                    borrower_phone = COALESCE(excluded.borrower_phone, borrower_info.borrower_phone),
                    borrower_email = COALESCE(excluded.borrower_email, borrower_info.borrower_email),
                    contact_source = COALESCE(excluded.contact_source, borrower_info.contact_source),
                    source = COALESCE(excluded.source, borrower_info.source),
                    updated_at = datetime('now')
                """,
                (
                    document_id,
                    info_full_name,
                    borrower_address,
                    borrower_zip,
                    borrower_phone,
                    borrower_email,
                    contact_source,
                    (existing_info or {}).get("source") or source or contact_source or "manual",
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


async def copy_credential_creditor_profile(
    chat_id: int,
    source_credential_id: int,
    target_credential_id: int,
) -> bool:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT full_name, address, phone, email
            FROM credential_creditor_profiles
            WHERE chat_id = ? AND credential_id = ?
            """,
            (chat_id, source_credential_id),
        )
        if not rows:
            return False
        source = rows[0]
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
            (
                chat_id,
                target_credential_id,
                source["full_name"],
                source["address"],
                source["phone"],
                source["email"],
            ),
        )
        await db.commit()
        return True
    finally:
        await db.close()


async def upsert_creditor_profile(
    chat_id: int,
    *,
    full_name: str | None,
    address: str | None,
    phone: str | None,
    email: str | None,
    sms_sender: str | None = None,
) -> None:
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT INTO creditor_profiles (
                chat_id, full_name, address, phone, email, sms_sender, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(chat_id) DO UPDATE SET
                full_name = excluded.full_name,
                address = excluded.address,
                phone = excluded.phone,
                email = excluded.email,
                sms_sender = excluded.sms_sender,
                updated_at = datetime('now')
            """,
            (chat_id, full_name, address, phone, email, sms_sender),
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


async def get_credential_signature_asset(chat_id: int, credential_id: int) -> dict | None:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT csa.*, c.service, c.login, c.label
            FROM credential_signature_assets csa
            JOIN credentials c ON c.id = csa.credential_id
            WHERE csa.chat_id = ? AND csa.credential_id = ?
            """,
            (chat_id, credential_id),
        )
        if rows:
            data = dict(rows[0])
            data["source"] = "credential"
            return data

        legacy_rows = await db.execute_fetchall(
            "SELECT * FROM user_signature_assets WHERE chat_id = ?",
            (chat_id,),
        )
        if not legacy_rows:
            return None
        data = dict(legacy_rows[0])
        data["credential_id"] = credential_id
        data["source"] = "legacy"
        return data
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


async def save_credential_signature_asset(
    chat_id: int,
    credential_id: int,
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
            INSERT INTO credential_signature_assets (
                chat_id, credential_id, file_path, mime_type, telegram_file_id, telegram_unique_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(chat_id, credential_id) DO UPDATE SET
                file_path = excluded.file_path,
                mime_type = excluded.mime_type,
                telegram_file_id = excluded.telegram_file_id,
                telegram_unique_id = excluded.telegram_unique_id,
                updated_at = datetime('now')
            """,
            (chat_id, credential_id, file_path, mime_type, telegram_file_id, telegram_unique_id),
        )
        await db.commit()
    finally:
        await db.close()


async def copy_credential_signature_asset(
    chat_id: int,
    source_credential_id: int,
    target_credential_id: int,
) -> bool:
    db = await get_db()
    try:
        rows = await db.execute_fetchall(
            """
            SELECT file_path, mime_type, telegram_file_id, telegram_unique_id
            FROM credential_signature_assets
            WHERE chat_id = ? AND credential_id = ?
            """,
            (chat_id, source_credential_id),
        )
        if rows:
            source = rows[0]
        else:
            legacy_rows = await db.execute_fetchall(
                """
                SELECT file_path, mime_type, telegram_file_id, telegram_unique_id
                FROM user_signature_assets
                WHERE chat_id = ?
                """,
                (chat_id,),
            )
            if not legacy_rows:
                return False
            source = legacy_rows[0]

        await db.execute(
            """
            INSERT INTO credential_signature_assets (
                chat_id, credential_id, file_path, mime_type, telegram_file_id, telegram_unique_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(chat_id, credential_id) DO UPDATE SET
                file_path = excluded.file_path,
                mime_type = excluded.mime_type,
                telegram_file_id = excluded.telegram_file_id,
                telegram_unique_id = excluded.telegram_unique_id,
                updated_at = datetime('now')
            """,
            (
                chat_id,
                target_credential_id,
                source["file_path"],
                source["mime_type"],
                source["telegram_file_id"],
                source["telegram_unique_id"],
            ),
        )
        await db.commit()
        return True
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
    "copy_credential_creditor_profile",
    "copy_credential_signature_asset",
    "deactivate_missing_overdue_cases",
    "get_creditor_profile",
    "get_credential_creditor_profile",
    "get_credential_signature_asset",
    "lookup_latest_borrower_contacts",
    "get_overdue_case",
    "get_user_signature_asset",
    "list_overdue_cases",
    "save_credential_signature_asset",
    "save_generated_document",
    "save_user_signature_asset",
    "update_overdue_case_contacts",
    "upsert_creditor_profile",
    "upsert_credential_creditor_profile",
    "upsert_overdue_case",
]
