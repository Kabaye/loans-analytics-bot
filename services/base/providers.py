from __future__ import annotations

from bot.domain.models import UserCredentials
from bot.integrations.parsers.finkit import FinkitParser
from bot.integrations.parsers.kapusta import KapustaParser
from bot.integrations.parsers.zaimis import ZaimisParser
from bot.repositories.credentials import (
    delete_credential_session,
    get_saved_credential_session,
    list_allowed_user_credentials,
    save_credential_session,
)

_kapusta: KapustaParser | None = None
_finkit_parsers: dict[int, FinkitParser] = {}
_zaimis_parsers: dict[int, ZaimisParser] = {}
_poll_rotation_index: dict[str, int] = {"finkit": 0, "zaimis": 0}


def telegram_user_tag(cred: UserCredentials) -> str:
    username = (cred.username or "").strip().lstrip("@")
    return username or f"chat_{cred.chat_id}"


async def list_service_credentials(service: str, chat_id: int | None = None) -> list[UserCredentials]:
    return await list_allowed_user_credentials(service, chat_id=chat_id)


def pick_round_robin_credential(service: str, creds_list: list[UserCredentials]) -> UserCredentials | None:
    if not creds_list:
        return None
    idx = _poll_rotation_index.get(service, 0) % len(creds_list)
    _poll_rotation_index[service] = (idx + 1) % len(creds_list)
    return creds_list[idx]


def _remember_parser_owner(parser, cred: UserCredentials) -> None:
    setattr(parser, "_owner_chat_id", cred.chat_id)
    setattr(parser, "_owner_credential_id", cred.id)


async def _login_and_persist(service: str, parser, cred: UserCredentials) -> bool:
    ok = await parser.login(cred.login, cred.password)
    if not ok:
        await delete_credential_session(cred.id)
        return False
    export = getattr(parser, "export_session", None)
    if callable(export):
        session_data = export()
        if session_data:
            await save_credential_session(cred.id, service, session_data)
    return True


async def ensure_kapusta_parser(force_login: bool = False) -> KapustaParser | None:
    global _kapusta
    if _kapusta is None or force_login:
        if _kapusta is not None:
            try:
                await _kapusta.close()
            except Exception:
                pass
        _kapusta = KapustaParser()
        ok = await _kapusta.login()
        if not ok:
            try:
                await _kapusta.close()
            except Exception:
                pass
            _kapusta = None
            return None
    return _kapusta


async def reset_kapusta_parser() -> None:
    global _kapusta
    if _kapusta is not None:
        try:
            await _kapusta.close()
        except Exception:
            pass
    _kapusta = None


async def ensure_finkit_parser(cred: UserCredentials, force_login: bool = False) -> FinkitParser | None:
    parser = _finkit_parsers.get(cred.id)
    if parser is None or force_login:
        if parser is not None:
            try:
                await parser.close()
            except Exception:
                pass
        parser = FinkitParser()
        _remember_parser_owner(parser, cred)
        _finkit_parsers[cred.id] = parser

    if not force_login:
        export = getattr(parser, "export_session", None)
        if callable(export) and export():
            return parser
        saved = await get_saved_credential_session(cred.id)
        restore = getattr(parser, "restore_session", None)
        if callable(restore) and restore(saved):
            return parser

    ok = await _login_and_persist("finkit", parser, cred)
    if ok:
        return parser
    _finkit_parsers.pop(cred.id, None)
    return None


async def ensure_zaimis_parser(cred: UserCredentials, force_login: bool = False) -> ZaimisParser | None:
    parser = _zaimis_parsers.get(cred.id)
    if parser is None or force_login:
        if parser is not None:
            try:
                await parser.close()
            except Exception:
                pass
        parser = ZaimisParser()
        _remember_parser_owner(parser, cred)
        _zaimis_parsers[cred.id] = parser

    if not force_login:
        export = getattr(parser, "export_session", None)
        if callable(export) and export():
            return parser
        saved = await get_saved_credential_session(cred.id)
        restore = getattr(parser, "restore_session", None)
        if callable(restore) and restore(saved):
            return parser

    ok = await _login_and_persist("zaimis", parser, cred)
    if ok:
        return parser
    _zaimis_parsers.pop(cred.id, None)
    return None


async def get_export_parsers(service: str, chat_id: int) -> list:
    if service == "kapusta":
        parser = await ensure_kapusta_parser()
        return [parser] if parser else []

    if service == "finkit":
        cred = pick_round_robin_credential(service, await list_service_credentials(service, chat_id=chat_id))
        if not cred:
            return []
        parser = await ensure_finkit_parser(cred)
        return [parser] if parser else []

    if service == "zaimis":
        cred = pick_round_robin_credential(service, await list_service_credentials(service, chat_id=chat_id))
        if not cred:
            return []
        parser = await ensure_zaimis_parser(cred)
        return [parser] if parser else []

    return []


def get_parser(service: str, chat_id: int | None = None):
    if service == "kapusta":
        return _kapusta
    if service == "finkit" and chat_id:
        for parser in _finkit_parsers.values():
            if getattr(parser, "_owner_chat_id", None) == chat_id:
                return parser
        return None
    if service == "zaimis" and chat_id:
        for parser in _zaimis_parsers.values():
            if getattr(parser, "_owner_chat_id", None) == chat_id:
                return parser
        return None
    return None


async def shutdown_parsers():
    global _kapusta
    if _kapusta is not None:
        try:
            await _kapusta.close()
        except Exception:
            pass
        _kapusta = None

    for parser in list(_finkit_parsers.values()) + list(_zaimis_parsers.values()):
        try:
            await parser.close()
        except Exception:
            pass
    _finkit_parsers.clear()
    _zaimis_parsers.clear()


_ensure_finkit_parser = ensure_finkit_parser
_ensure_zaimis_parser = ensure_zaimis_parser


__all__ = [
    "_ensure_finkit_parser",
    "_ensure_zaimis_parser",
    "ensure_finkit_parser",
    "ensure_kapusta_parser",
    "ensure_zaimis_parser",
    "get_export_parsers",
    "get_parser",
    "list_service_credentials",
    "pick_round_robin_credential",
    "reset_kapusta_parser",
    "shutdown_parsers",
    "telegram_user_tag",
]
