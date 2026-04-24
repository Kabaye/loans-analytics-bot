from __future__ import annotations

from difflib import SequenceMatcher
import logging
import re
from urllib.parse import quote

import aiohttp

log = logging.getLogger(__name__)

_POSTCODE_RE = re.compile(r"\b(\d{6})\b")
_HOUSE_RE = re.compile(r"\b\d+[A-Za-zА-Яа-я\-\/]*\b")


def _normalize_query(address: str) -> str:
    text = " ".join((address or "").replace("\n", " ").split()).strip(" ,")
    return text


def _normalize_compare_text(text: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-zА-Яа-я]+", " ", text.lower())
    return " ".join(cleaned.split())


def _candidate_label(item: dict) -> str:
    parts = [
        item.get("region"),
        item.get("district"),
        item.get("city_type"),
        item.get("city"),
        item.get("street_type"),
        item.get("street"),
        f"д. {item.get('house')}" if item.get("house") else None,
        f"кв. {item.get('flat')}" if item.get("flat") else None,
    ]
    return ", ".join(str(part).strip() for part in parts if part)


def _candidate_score(query: str, candidate: dict) -> float:
    query_norm = _normalize_compare_text(query)
    label_norm = _normalize_compare_text(_candidate_label(candidate))
    if not query_norm or not label_norm:
        return 0.0
    score = SequenceMatcher(None, query_norm, label_norm).ratio()
    query_houses = set(_HOUSE_RE.findall(query_norm))
    candidate_houses = set(_HOUSE_RE.findall(label_norm))
    if query_houses and candidate_houses and query_houses & candidate_houses:
        score += 0.25
    city_value = _normalize_compare_text(str(candidate.get("city") or ""))
    if city_value:
        score += 0.2 if city_value in query_norm else -0.1
    street_value = _normalize_compare_text(" ".join(
        part for part in (str(candidate.get("street_type") or "").strip(), str(candidate.get("street") or "").strip())
        if part
    ))
    if street_value:
        if street_value in query_norm:
            score += 0.45
        else:
            street_tokens = [token for token in street_value.split() if len(token) > 2]
            overlap = sum(1 for token in street_tokens if token in query_norm)
            if overlap:
                score += min(0.25, overlap * 0.12)
            else:
                score -= 0.45
    return score


async def _fetch_json(session: aiohttp.ClientSession, url: str):
    async with session.get(url) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}")
        return await resp.json()


async def _normalize_with_nominatim(session: aiohttp.ClientSession, query: str) -> str | None:
    nominatim_query = query if "беларус" in query.lower() else f"Беларусь, {query}"
    url = (
        "https://nominatim.openstreetmap.org/search"
        f"?format=jsonv2&addressdetails=1&countrycodes=by&limit=1&q={quote(nominatim_query)}"
    )
    try:
        payload = await _fetch_json(session, url)
    except Exception as exc:
        log.warning("Address normalize error for %s: %s", query, exc)
        return None

    if not isinstance(payload, list) or not payload:
        return None
    display_name = str(payload[0].get("display_name") or "").strip()
    return display_name or None


async def _search_belpost(session: aiohttp.ClientSession, query: str) -> list[dict]:
    url = f"https://api.belpost.by/api/v1/postcodes/autocomplete?search={quote(query)}"
    try:
        payload = await _fetch_json(session, url)
    except Exception as exc:
        log.warning("Belpost ZIP lookup failed for %s: %s", query, exc)
        return []
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _street_name(item: dict) -> str | None:
    street = str(item.get("street") or "").strip()
    if not street:
        return None
    street_type = str(item.get("street_type") or "").strip()
    return f"{street_type} {street}".strip() if street_type else street


async def lookup_belarus_zip_details(address: str | None) -> dict | None:
    query = _normalize_query(address or "")
    if not query:
        return None

    headers = {"User-Agent": "loans-bot/1.0 (postal lookup)"}
    timeout = aiohttp.ClientTimeout(total=20)
    try:
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            queries = [query]
            normalized = await _normalize_with_nominatim(session, query)
            if normalized and normalized not in queries:
                queries.append(normalized)

            seen_keys: set[str] = set()
            candidates: list[dict] = []
            for search_query in queries:
                for item in await _search_belpost(session, search_query):
                    key = str(item.get("id") or item.get("postcode") or _candidate_label(item))
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    candidates.append(item)
            if not candidates:
                return None

            best = max(candidates, key=lambda item: _candidate_score(query, item))
            postcode = str(best.get("postcode") or "").strip()
            if not _POSTCODE_RE.fullmatch(postcode):
                return None

            same_postcode_candidates = [
                item for item in await _search_belpost(session, postcode)
                if str(item.get("postcode") or "").strip() == postcode
            ]
    except Exception as exc:
        log.warning("ZIP lookup error for %s: %s", query, exc)
        return None

    related_streets: list[str] = []
    for item in same_postcode_candidates:
        street_name = _street_name(item)
        if not street_name or street_name in related_streets:
            continue
        related_streets.append(street_name)
        if len(related_streets) >= 8:
            break

    return {
        "postcode": postcode,
        "query": query,
        "normalized_query": normalized,
        "match_address": best.get("autocomplete_address") or best.get("short_address") or _candidate_label(best),
        "match_street": _street_name(best),
        "match_buildings": best.get("buildings"),
        "related_streets": related_streets,
    }


async def lookup_belarus_zip(address: str | None) -> str | None:
    details = await lookup_belarus_zip_details(address)
    return str(details.get("postcode")) if details and details.get("postcode") else None
