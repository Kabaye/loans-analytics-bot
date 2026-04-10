"""HTTP API — health check endpoint."""
from __future__ import annotations

import logging

from aiohttp import web

log = logging.getLogger(__name__)


async def handle_health(request: web.Request) -> web.Response:
    """GET /api/health — simple health check."""
    return web.json_response({"status": "ok"})


def create_api_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/api/health", handle_health)
    log.info("API app created — endpoint: /api/health")
    return app
