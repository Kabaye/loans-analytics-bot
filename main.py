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
from bot.database import init_db
from bot.handlers import start, subscriptions, credentials, admin, export, search
from bot.services.scheduler import setup_scheduler, shutdown_parsers

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
        from bot.database import get_db
        db = await get_db()
        try:
            await db.execute(
                "INSERT OR IGNORE INTO users (chat_id, is_admin, is_allowed) VALUES (?, 1, 1)",
                (config.ADMIN_CHAT_ID,),
            )
            await db.execute(
                "UPDATE users SET is_admin=1, is_allowed=1 WHERE chat_id=?",
                (config.ADMIN_CHAT_ID,),
            )
            await db.commit()
        finally:
            await db.close()

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
    dp.include_router(admin.router)

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
