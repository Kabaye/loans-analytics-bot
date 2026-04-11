from __future__ import annotations

import abc
import logging
from typing import Optional

import aiohttp

from bot.models import BorrowEntry

log = logging.getLogger(__name__)


class BaseParser(abc.ABC):
    """Abstract parser for a P2P lending site."""

    SERVICE_NAME: str = ""

    def __init__(self, session: Optional[aiohttp.ClientSession] = None, **kwargs):
        self._session = session
        self._owns_session = session is None
        self._needs_reauth = False

    @property
    def needs_reauth(self) -> bool:
        return self._needs_reauth

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=False),
                timeout=aiohttp.ClientTimeout(total=60),
            )
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()

    @abc.abstractmethod
    async def login(self, username: str = "", password: str = "") -> bool:
        """Authenticate if needed. Return True on success."""
        ...

    @abc.abstractmethod
    async def fetch_borrows(self) -> list[BorrowEntry]:
        """Fetch all active borrow requests."""
        ...

    async def fetch_lends(self) -> list[BorrowEntry]:
        """Fetch all active lend offers. Override in subclasses that support it."""
        return []
