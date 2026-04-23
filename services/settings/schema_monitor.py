from __future__ import annotations

import json
import logging

from bot.domain.models import BorrowEntry
from bot.repositories.settings import (
    get_json_schema_state,
    save_api_change_alert,
    save_json_schema_state,
)

log = logging.getLogger(__name__)


def _json_type_name(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return type(value).__name__


def _collect_schema_types(value, path: str, acc: dict[str, set[str]]) -> None:
    acc.setdefault(path, set()).add(_json_type_name(value))
    if isinstance(value, dict):
        for key, nested in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            _collect_schema_types(nested, child_path, acc)
    elif isinstance(value, list):
        item_path = f"{path}[]" if path else "[]"
        if not value:
            acc.setdefault(item_path, set()).add("empty")
        for item in value:
            _collect_schema_types(item, item_path, acc)


def _build_entries_schema(entries: list[BorrowEntry]) -> dict[str, list[str]]:
    acc: dict[str, set[str]] = {}
    for entry in entries:
        if entry.raw_data is not None:
            _collect_schema_types(entry.raw_data, "", acc)
    return {path: sorted(types) for path, types in sorted(acc.items()) if path}


def _non_null_schema_types(types: list[str] | None) -> set[str]:
    return {item for item in (types or []) if item not in {"null", "empty"}}


def _merge_schema(prev: dict[str, list[str]] | None, current: dict[str, list[str]]) -> dict[str, list[str]]:
    merged: dict[str, set[str]] = {}
    for source in (prev or {}, current):
        for path, types in source.items():
            merged.setdefault(path, set()).update(types)
    return {path: sorted(types) for path, types in sorted(merged.items())}


def _schema_diff(prev: dict[str, list[str]] | None, current: dict[str, list[str]]) -> tuple[list[str], list[str]]:
    prev = prev or {}
    added_paths: list[str] = []
    changed_types: list[str] = []
    for path, types in current.items():
        if path not in prev:
            non_null_types = sorted(_non_null_schema_types(types))
            if non_null_types:
                added_paths.append(f"{path} ({', '.join(non_null_types)})")
            continue
        prev_types = sorted(_non_null_schema_types(prev[path]))
        current_types = sorted(_non_null_schema_types(types))
        if not prev_types:
            continue
        new_types = [item for item in current_types if item not in prev_types]
        if new_types:
            before = ", ".join(prev_types) if prev_types else "no-non-null-types"
            changed_types.append(f"{path}: {before} -> {', '.join(current_types)}")
    return added_paths, changed_types


async def notify_json_schema_change(service: str, entries: list[BorrowEntry]) -> None:
    current = _build_entries_schema(entries)
    if not current:
        return

    prev = await get_json_schema_state(service)
    if prev is None:
        await save_json_schema_state(service, current)
        return

    added_paths, changed_types = _schema_diff(prev, current)
    merged = _merge_schema(prev, current)
    if not added_paths and not changed_types:
        if merged != prev:
            await save_json_schema_state(service, merged)
        return

    await save_json_schema_state(service, merged)
    svc_names = {"kapusta": "🥬 Kapusta", "finkit": "🔵 FinKit", "zaimis": "🟪 ЗАЙМись"}
    lines = [f"Изменилась JSON-структура {svc_names.get(service, service)}"]
    if added_paths:
        lines.append("")
        lines.append("Новые поля:")
        lines.extend(f"  • {item}" for item in added_paths[:20])
    if changed_types:
        lines.append("")
        lines.append("Изменились типы:")
        lines.extend(f"  • {item}" for item in changed_types[:20])
    sample = next((entry.raw_data for entry in entries if entry.raw_data), None)
    sample_text = json.dumps(sample, ensure_ascii=False, default=str)[:4000] if sample else None
    try:
        await save_api_change_alert(
            service=service,
            title=f"JSON change: {svc_names.get(service, service)}",
            details="\n".join(lines),
            sample_json=sample_text,
        )
        log.info("JSON schema change saved for %s", service)
    except Exception as exc:
        log.warning("Failed to save JSON schema alert: %s", exc)


__all__ = ["notify_json_schema_change"]
