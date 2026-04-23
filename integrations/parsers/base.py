from __future__ import annotations

import abc
import logging
from typing import Optional

import aiohttp

from bot.domain.models import BorrowEntry

log = logging.getLogger(__name__)

BROWSER_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/18.7 Mobile/15E148 Safari/604.1 OPT/6.4.1"
)


class BaseParser(abc.ABC):
    """Abstract parser for a P2P lending site."""

    SERVICE_NAME: str = ""

    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
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
                headers={"User-Agent": BROWSER_UA},
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
