"""Collect Finam daily candles while the market-data worker is running."""

from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from worker.pipeline import MarketDataPipeline
from worker.russian_market import RU_SESSION_TIMEZONE, RU_WATCHLIST
from worker.types import AdjustmentMode, RunStatus

_MOSCOW = ZoneInfo(RU_SESSION_TIMEZONE)
_RUN_AT = time(8, 30)
_LOOKBACK = timedelta(days=7)
_RETRY_DELAY = timedelta(minutes=15)
_MAX_ATTEMPTS = 4
_TRANSIENT_ERRORS = {"FINAM_NETWORK_ERROR", "FINAM_SOURCE_TRANSIENT", "FINAM_RATE_LIMITED", "DB_UNAVAILABLE"}


def scheduled_window_end(now: datetime) -> date:
    """The most recent due date, used as the exclusive window end."""
    moscow_now = now.astimezone(_MOSCOW)
    if moscow_now.time() < _RUN_AT:
        return moscow_now.date() - timedelta(days=1)
    return moscow_now.date()


async def run_finam_schedule(pipeline: MarketDataPipeline) -> None:
    print("Finam daily collection: 08:30 MSK. Stop with Ctrl+C.", flush=True)
    window_end: date | None = None
    retry_at: datetime | None = None
    attempts = 0
    while True:
        now = datetime.now(_MOSCOW)
        due = scheduled_window_end(now)
        if window_end is None or due > window_end:
            window_end = due
            retry_at = now
            attempts = 0
        if retry_at is not None and now >= retry_at:
            attempts += 1
            report = await pipeline.run(
                symbols=list(RU_WATCHLIST),
                start=window_end - _LOOKBACK,
                end=window_end,
                adjustment_mode=AdjustmentMode.RAW,
                interval="1d",
            )
            print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2), flush=True)
            retry_at = None
            if (
                report.status is not RunStatus.READY
                and attempts < _MAX_ATTEMPTS
                and any(error.get("code") in _TRANSIENT_ERRORS for error in report.errors)
            ):
                retry_at = datetime.now(_MOSCOW) + _RETRY_DELAY
                print(f"Finam retry at {retry_at:%H:%M:%S} MSK.", flush=True)
        await asyncio.sleep(30)
