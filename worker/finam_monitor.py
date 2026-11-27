"""Keep live Finam data current for the registry and measure event latency.

The stream and the news feed run as independent tasks: a news failure does not
touch quotes, and one instrument's REST failure does not touch the others.
No account, order or trading method is used.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import aclosing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from worker.adapters.finam_stream import stream_finam_market
from worker.event_reaction import measure_reaction
from worker.finam_live_state import LiveMarketState
from worker.finam_news import NewsLedger, ingest_history
from worker.io_retry import PermanentIOError, TransientIOError
from worker.news_signals import active_events
from worker.russian_market import RU_CONTEXT_SYMBOLS, RU_WATCHLIST

FATAL_CODES = {"FINAM_NOT_CONFIGURED", "FINAM_AUTHORIZATION_FAILED", "FINAM_INVALID_AUTH_RESPONSE"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def backfill(adapter, state: LiveMarketState, now: datetime) -> dict:
    """Read missed closed minutes per instrument; failures stay per instrument."""
    results = {}
    for symbol, (start, end) in state.backfill_windows(now).items():
        try:
            history = await adapter.fetch_minute_history(symbol, start, end)
        except (PermanentIOError, TransientIOError) as exc:
            state.mark_symbol_error(symbol, exc.code)
            results[symbol] = {"status": "ERROR", "code": exc.code}
            continue
        counts = state.merge_bars(symbol, history.bars, now, source="rest")
        state.mark_recovered(symbol)
        results[symbol] = {"status": "OK", "window": [start.isoformat(), end.isoformat()], **counts}
    for symbol in state.instruments:
        if symbol not in results and state.instruments[symbol].status != "ERROR":
            state.mark_recovered(symbol)
    return results


async def run_stream(
    adapter,
    state: LiveMarketState,
    *,
    stream=stream_finam_market,
    clock: Callable[[], datetime] = utc_now,
    sleep=asyncio.sleep,
    max_connections: int | None = None,
    max_delay: float = 30.0,
) -> None:
    delay = 1.0
    connections = 0
    while max_connections is None or connections < max_connections:
        connections += 1
        code = "FINAM_STREAM_CLOSED"
        retry_after = None
        try:
            jwt = await adapter.open_market_session()
            async with aclosing(stream(jwt)) as events:
                async for event in events:
                    now = clock()
                    if event.get("type") == "connected":
                        state.mark_connected()
                        await backfill(adapter, state, now)
                        delay = 1.0
                    else:
                        state.on_event(event, now)
        except PermanentIOError as exc:
            if exc.code in FATAL_CODES:
                state.mark_disconnected(clock(), exc.code)
                raise
            code = exc.code
        except TransientIOError as exc:
            code, retry_after = exc.code, exc.retry_after
        state.mark_disconnected(clock(), code)
        if max_connections is not None and connections >= max_connections:
            return
        await sleep(max(delay, retry_after or 0.0))
        delay = min(delay * 2, max_delay)


class NewsMonitor:
    def __init__(self, adapter, symbols=RU_WATCHLIST, *, clock: Callable[[], datetime] = utc_now):
        self.adapter = adapter
        self.symbols = tuple(symbols)
        self.clock = clock
        self.ledger = NewsLedger()
        self.status = {"status": "NOT_CHECKED", "code": None, "checked_at": None}
        self.sources: list[dict] = []
        self.last_counts: dict = {}

    async def poll(self, lookback: timedelta = timedelta(hours=24)) -> dict:
        now = self.clock()
        try:
            if not self.sources:
                self.sources = await self.adapter.fetch_news_sources()
            page = await self.adapter.fetch_news_history(list(self.symbols), now - lookback, now)
        except (PermanentIOError, TransientIOError) as exc:
            self.status = {"status": news_status_for(exc.code), "code": exc.code, "checked_at": now.isoformat()}
            return self.status
        self.last_counts = {**ingest_history(self.ledger, page.items, now), "pages": page.pages, "truncated": page.truncated}
        self.status = {"status": "OK", "code": None, "checked_at": now.isoformat()}
        return self.status


def news_status_for(code: str) -> str:
    if code == "FINAM_NOT_CONFIGURED":
        return "NOT_CONFIGURED"
    if code == "FINAM_AUTHORIZATION_FAILED":
        return "NO_ACCESS"
    if code == "FINAM_RATE_LIMITED":
        return "RATE_LIMITED"
    return "UNAVAILABLE"


def latency_report(state: LiveMarketState, news: NewsMonitor, now: datetime, window_minutes: int = 5) -> list[dict]:
    """Event status plus price reaction on closed M1 after AEGIS saw the event."""
    index = RU_CONTEXT_SYMBOLS[0] if RU_CONTEXT_SYMBOLS else None
    index_bars = state.closed_bars(index, now) if index in state.instruments else None
    reports = []
    for event in active_events(news.ledger, news.symbols, now):
        if not event["transitions"]:
            continue
        first = event["transitions"][0]
        reaction = measure_reaction(
            first, state.closed_bars(event["symbol"], now), now=now,
            window_minutes=window_minutes, index_bars=index_bars,
        )
        reports.append({**event, "reaction": reaction})
    return reports


async def run_monitor(adapter, *, status_file: Path | None, news_interval: float = 60.0, snapshot_interval: float = 5.0):
    state = LiveMarketState((*RU_WATCHLIST, *RU_CONTEXT_SYMBOLS))
    news = NewsMonitor(adapter)

    async def poll_news():
        while True:
            await news.poll()
            await asyncio.sleep(news_interval)

    async def write_snapshots():
        while True:
            now = utc_now()
            snapshot = {
                **state.snapshot(now),
                "news": {**news.status, "sources": news.sources, "counts": news.last_counts},
                "events": latency_report(state, news, now),
            }
            if status_file is not None:
                status_file.parent.mkdir(parents=True, exist_ok=True)
                temporary = status_file.with_suffix(".tmp")
                temporary.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
                temporary.replace(status_file)
            await asyncio.sleep(snapshot_interval)

    await asyncio.gather(run_stream(adapter, state), poll_news(), write_snapshots())
