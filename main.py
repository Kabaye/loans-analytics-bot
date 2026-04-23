"""Loans Bot — main entry point."""
from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BotCommand, MenuButtonCommands

from bot import config
from bot.repositories.db import init_db
from bot.repositories.users import ensure_admin_user
from bot.handlers import start, subscriptions, credentials, admin, export, search, overdue
from bot.jobs.scheduler import setup_scheduler
from bot.services.base.providers import shutdown_parsers
from bot.services.notifications.sender import router as notifier_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


async def on_startup(bot: Bot) -> None:
    # Set bot commands (creates the menu button in Telegram)
    await bot.set_my_commands([
        BotCommand(command="start", description="📋 Главное меню"),
        BotCommand(command="help", description="❓ Помощь"),
    ])
    await bot.set_chat_menu_button(menu_button=MenuButtonCommands())

    # Ensure admin user exists and is allowed
    if config.ADMIN_CHAT_ID:
        await ensure_admin_user(config.ADMIN_CHAT_ID)

    log.info("Bot started. Admin chat_id=%s", config.ADMIN_CHAT_ID)


async def main() -> None:
    # Ensure data directory exists
    Path(config.DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    # Init database
    await init_db()

    # Create bot and dispatcher
    bot = Bot(token=config.BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    # Register handlers
    dp.include_router(start.router)
    dp.include_router(subscriptions.router)
    dp.include_router(credentials.router)
    dp.include_router(export.router)
    dp.include_router(search.router)
    dp.include_router(overdue.router)
    dp.include_router(admin.router)
    dp.include_router(notifier_router)

    # Startup hook
    dp.startup.register(on_startup)

    # Setup scheduler
    scheduler = setup_scheduler(bot)
    scheduler.start()

    try:
        log.info("Starting polling...")
        await dp.start_polling(bot)
    finally:
        log.info("Shutting down...")
        scheduler.shutdown(wait=False)
        await shutdown_parsers()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
