from __future__ import annotations

import json
from typing import Any, Protocol


class RawPayloadCarrier(Protocol):
    raw_data: dict | None


def extract_raw_payload(value: RawPayloadCarrier | dict | str | None) -> Any | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value if value else None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except Exception:
            return text
    raw_data = getattr(value, "raw_data", None)
    if isinstance(raw_data, dict):
        return raw_data if raw_data else None
    if isinstance(raw_data, str):
        return extract_raw_payload(raw_data)
    return None


def format_raw_payload_preview(value: RawPayloadCarrier | dict | str | None, *, limit: int = 3900) -> str:
    payload = extract_raw_payload(value)
    if payload is None:
        return "{}"
    if isinstance(payload, (dict, list)):
        text = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    else:
        text = str(payload)
    if len(text) > limit:
        return text[:limit] + "\n... (обрезано)"
    return text


__all__ = ["RawPayloadCarrier", "extract_raw_payload", "format_raw_payload_preview"]
