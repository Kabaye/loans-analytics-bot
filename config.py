import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "CHANGE_ME")
ADMIN_CHAT_ID: int = int(os.getenv("ADMIN_CHAT_ID", "0"))

KAPUSTA_INTERVAL: int = int(os.getenv("KAPUSTA_INTERVAL", "300"))
KAPUSTA_BACKOFF_SECONDS: int = int(os.getenv("KAPUSTA_BACKOFF_SECONDS", "3600"))
FINKIT_INTERVAL: int = int(os.getenv("FINKIT_INTERVAL", "60"))
ZAIMIS_INTERVAL: int = int(os.getenv("ZAIMIS_INTERVAL", "60"))

DB_PATH: str = str(BASE_DIR / os.getenv("DB_PATH", "data/loans.db"))
APP_VERSION: str = os.getenv("APP_VERSION", "2.0.0")
PATCH_NOTES_PATH: str = str(BASE_DIR / os.getenv("PATCH_NOTES_PATH", "PATCHLIST-2.0.0.md"))
