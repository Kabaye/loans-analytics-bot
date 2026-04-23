"""HTTP API — serves cached loans as JSON."""
from __future__ import annotations

import json
import logging

from aiohttp import web

from bot.services.base.cache import VALID_SERVICES, get_cached_snapshot

log = logging.getLogger(__name__)


async def handle_loans(request: web.Request) -> web.Response:
    """GET /api/loans?services=kapusta,finkit,zaimis

    Query params:
        services — comma-separated list of services (default: all)
    """
    services_param = request.query.get("services", "")
    if services_param:
        requested = {s.strip().lower() for s in services_param.split(",")}
        invalid = requested - VALID_SERVICES
        if invalid:
            return web.json_response(
                {"error": f"Unknown services: {', '.join(invalid)}. Valid: {', '.join(sorted(VALID_SERVICES))}"},
                status=400,
            )
    else:
        requested = VALID_SERVICES

    result = {}
    for svc in sorted(requested):
        result[svc] = get_cached_snapshot(svc)

    return web.Response(
        text=json.dumps(result, ensure_ascii=False, indent=2),
        content_type="application/json",
        charset="utf-8",
    )


async def handle_health(request: web.Request) -> web.Response:
    """GET /api/health — simple health check."""
    status = {}
    for svc in sorted(VALID_SERVICES):
        snapshot = get_cached_snapshot(svc)
        status[svc] = {"count": snapshot["count"], "cached_at": snapshot["cached_at"]}
    return web.json_response({"status": "ok", "services": status})


def create_api_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/api/loans", handle_loans)
    app.router.add_get("/api/health", handle_health)
    log.info("API app created — endpoints: /api/loans, /api/health")
    return app
