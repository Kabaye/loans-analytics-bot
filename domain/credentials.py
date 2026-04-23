from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class UserCredentials:
    chat_id: int
    service: str
    login: str
    password: str
    id: int = 0
    username: Optional[str] = None


__all__ = ["UserCredentials"]
