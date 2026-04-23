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

__all__ = [
    "_BORROWER_INFO_FULL_NAME_SQL",
    "_BORROWER_INFO_SOURCE_SQL",
]
