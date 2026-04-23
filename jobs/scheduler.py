"""Scheduler wiring for periodic jobs."""
from __future__ import annotations

import logging

from aiogram import Bot
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from typing import Optional

from bot.jobs.investments_refresh_job import midnight_refresh_investments
from bot.jobs.opi_refresh_job import midnight_refresh_opi
from bot.jobs.overdue_refresh_job import refresh_overdue_cases
from bot.jobs.polling_job import poll_finkit, poll_kapusta, poll_zaimis

log = logging.getLogger(__name__)

_scheduler: Optional[AsyncIOScheduler] = None


def setup_scheduler(bot: Bot) -> AsyncIOScheduler:
    global _scheduler
    _scheduler = AsyncIOScheduler()

    base_interval = 30
    _scheduler.add_job(poll_kapusta, "interval", seconds=base_interval, args=[bot], id="kapusta", name="Kapusta poll", misfire_grace_time=60)
    _scheduler.add_job(poll_finkit, "interval", seconds=base_interval, args=[bot], id="finkit", name="Finkit poll", misfire_grace_time=60)
    _scheduler.add_job(poll_zaimis, "interval", seconds=base_interval, args=[bot], id="zaimis", name="Zaimis poll", misfire_grace_time=60)
    _scheduler.add_job(
        midnight_refresh_investments,
        CronTrigger(hour=21, minute=0, timezone="UTC"),
        args=[bot],
        id="midnight_investments",
        name="Midnight investments refresh",
        misfire_grace_time=3600,
    )
    _scheduler.add_job(
        refresh_overdue_cases,
        CronTrigger(hour=21, minute=15, timezone="UTC"),
        args=[bot],
        id="midnight_overdue_cases",
        name="Midnight overdue refresh",
        misfire_grace_time=3600,
    )
    _scheduler.add_job(
        midnight_refresh_opi,
        CronTrigger(hour=21, minute=30, timezone="UTC"),
        args=[bot],
        id="midnight_opi",
        name="Midnight OPI refresh",
        misfire_grace_time=3600,
    )

    log.info("Scheduler configured (base=%ds, midnight cron at 00:00/00:15/00:30 Minsk)", base_interval)
    return _scheduler


__all__ = ["setup_scheduler"]
