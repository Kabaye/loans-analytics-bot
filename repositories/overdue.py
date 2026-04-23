from bot.repositories.db import get_db
from bot.repositories.legacy_database import (
    deactivate_missing_overdue_cases,
    get_creditor_profile,
    get_credential_creditor_profile,
    get_overdue_case,
    get_user_signature_asset,
    list_overdue_cases,
    save_generated_document,
    save_user_signature_asset,
    update_overdue_case_contacts,
    upsert_creditor_profile,
    upsert_credential_creditor_profile,
    upsert_overdue_case,
)


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
