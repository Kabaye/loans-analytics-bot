from __future__ import annotations

import logging
import re
from urllib.parse import quote

import aiohttp

log = logging.getLogger(__name__)

_POSTCODE_RE = re.compile(r"\b(\d{6})\b")


def _normalize_query(address: str) -> str:
    text = " ".join((address or "").replace("\n", " ").split()).strip(" ,")
    if not text:
        return ""
    if "беларус" not in text.lower():
        text = f"Беларусь, {text}"
    return text


async def lookup_belarus_zip(address: str | None) -> str | None:
    query = _normalize_query(address or "")
    if not query:
        return None

    url = (
        "https://nominatim.openstreetmap.org/search"
        f"?format=jsonv2&addressdetails=1&countrycodes=by&limit=3&q={quote(query)}"
    )
    headers = {"User-Agent": "loans-bot/1.0 (postal lookup)"}

    try:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    log.warning("ZIP lookup failed for %s: HTTP %s", query, resp.status)
                    return None
                payload = await resp.json()
    except Exception as exc:
        log.warning("ZIP lookup error for %s: %s", query, exc)
        return None

    if not isinstance(payload, list):
        return None

    for item in payload:
        address_data = item.get("address") or {}
        postcode = address_data.get("postcode")
        if postcode and _POSTCODE_RE.fullmatch(str(postcode)):
            return str(postcode)
        display_name = str(item.get("display_name") or "")
        match = _POSTCODE_RE.search(display_name)
        if match:
            return match.group(1)
    return None
