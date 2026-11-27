"""Live read-only check of the MOEX watchlist through Finam Trade API.

For every registry ticker and the market context (IMOEX) it reports:
reference data from GetAsset, the last D1 bar, the last closed M1 bar,
the QUOTES/BARS stream state after a short subscription, and news-feed access.

FINAM_API_TOKEN comes from the process environment only; the script never
reads .env and never prints the token, the session JWT or response bodies.
No account, order or trading endpoint is called.

    $env:FINAM_API_TOKEN = '<read-only token>'
    .\\.venv\\Scripts\\python.exe scripts\\finam_watch_check.py --stream-seconds 60
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from worker.adapters.finam_adapter import FinamAdapter
from worker.adapters.finam_stream import stream_finam_market
from worker.finam_live_state import LiveMarketState
from worker.finam_monitor import news_status_for, run_stream
from worker.io_retry import PermanentIOError, TransientIOError
from worker.russian_market import RU_CONTEXT_SYMBOLS, RU_WATCHLIST, finam_symbol, ru_instrument
from worker.types import AdjustmentMode

MOSCOW = ZoneInfo("Europe/Moscow")
ASSET_FIELDS = ("ticker", "mic", "board", "isin", "name", "type", "lot_size", "quote_currency")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def trading_hours(now: datetime) -> bool:
    """MOEX stock market sessions (morning, main, evening) on weekdays, MSK."""
    local = now.astimezone(MOSCOW)
    return local.weekday() < 5 and time(6, 50) <= local.time() <= time(23, 50)


def _failure(exc: Exception) -> dict:
    code = getattr(exc, "code", type(exc).__name__)
    return {"status": "ERROR", "code": code}


def _iso(stamp: datetime | None) -> str | None:
    return stamp.astimezone(timezone.utc).isoformat() if stamp else None


def _msk(stamp: datetime | None) -> str | None:
    return stamp.astimezone(MOSCOW).strftime("%Y-%m-%d %H:%M:%S МСК") if stamp else None


async def check_asset(adapter, symbol: str) -> dict:
    try:
        asset = await adapter.fetch_asset(symbol)
    except (PermanentIOError, TransientIOError) as exc:
        return {**_failure(exc), "expected": finam_symbol(symbol)}
    fields = {key: asset.get(key) for key in ASSET_FIELDS if asset.get(key) is not None}
    expected_ticker, expected_mic = finam_symbol(symbol).split("@")
    matches = fields.get("ticker") == expected_ticker and fields.get("mic", expected_mic) == expected_mic
    return {"status": "VERIFIED" if matches else "MISMATCH", "expected": finam_symbol(symbol), **fields}


async def check_daily(adapter, symbol: str, today: date) -> dict:
    try:
        result = await adapter.fetch_daily(symbol, today - timedelta(days=14), today + timedelta(days=1), AdjustmentMode.RAW)
    except (PermanentIOError, TransientIOError) as exc:
        return _failure(exc)
    if result.error_code:
        return {"status": "ERROR", "code": result.error_code}
    if not result.bars:
        return {"status": "EMPTY", "rejected": len(result.parse_failures)}
    last = max(result.bars, key=lambda bar: bar.quote_time)
    return {"status": "OK", "last_d1": _iso(last.quote_time), "last_d1_msk": _msk(last.quote_time), "close": str(last.close)}


async def check_minutes(adapter, symbol: str, now: datetime) -> dict:
    end = now.replace(second=0, microsecond=0)
    try:
        history = await adapter.fetch_minute_history(symbol, end - timedelta(hours=4), end)
    except (PermanentIOError, TransientIOError) as exc:
        return _failure(exc)
    closed = [bar for bar in history.bars if bar.quote_time + timedelta(minutes=1) <= now]
    if not closed:
        return {"status": "EMPTY", "rejected": history.rejected}
    last = closed[-1].quote_time
    return {
        "status": "OK", "last_closed_m1": _iso(last), "last_closed_m1_msk": _msk(last),
        "lag_seconds": round((now - (last + timedelta(minutes=1))).total_seconds(), 1), "rejected": history.rejected,
    }


async def check_stream(adapter, symbols, seconds: float, *, stream=stream_finam_market, clock=utc_now) -> dict:
    state = LiveMarketState(symbols)
    outcome = {"status": "OK", "code": None}
    try:
        async with asyncio.timeout(seconds):
            await run_stream(adapter, state, stream=stream, clock=clock, max_connections=1)
            outcome = {"status": "CLOSED_EARLY", "code": "FINAM_STREAM_CLOSED"}
    except TimeoutError:
        pass
    except (PermanentIOError, TransientIOError) as exc:
        outcome = _failure(exc)
    snapshot = state.snapshot(clock())
    return {**outcome, "connected": snapshot.get("connected"), "instruments": snapshot["instruments"]}


async def check_news(adapter, now: datetime) -> dict:
    try:
        sources = await adapter.fetch_news_sources()
        page = await adapter.fetch_news_history(list(RU_WATCHLIST), now - timedelta(hours=24), now)
    except (PermanentIOError, TransientIOError) as exc:
        return {"status": news_status_for(exc.code), "code": exc.code}
    per_symbol = {symbol: 0 for symbol in RU_WATCHLIST}
    for row in page.items:
        for value in row.get("symbols") or []:
            ticker = str(value).split("@")[0]
            if ticker in per_symbol:
                per_symbol[ticker] += 1
    return {
        "status": "OK", "sources": len(sources), "items_24h": len(page.items),
        "pages": page.pages, "truncated": page.truncated, "per_symbol": per_symbol,
    }


async def run_check(adapter, *, stream_seconds: float, stream=stream_finam_market, clock=utc_now) -> dict:
    now = clock()
    symbols = (*RU_WATCHLIST, *RU_CONTEXT_SYMBOLS)
    instruments = {}
    for symbol in symbols:
        instruments[symbol] = {
            "name": ru_instrument(symbol)["name"],
            "kind": ru_instrument(symbol).get("kind", "equity"),
            "asset": await check_asset(adapter, symbol),
            "d1": await check_daily(adapter, symbol, now.astimezone(MOSCOW).date()),
            "m1": await check_minutes(adapter, symbol, now),
        }
    stream_report = await check_stream(adapter, symbols, stream_seconds, stream=stream, clock=clock)
    for symbol, state in stream_report.pop("instruments").items():
        instruments[symbol]["stream"] = {
            key: state.get(key) for key in (
                "status", "error_code", "last_update_at", "last_quote_time", "last_closed_m1", "bars", "backfilled",
            )
        }
    news = await check_news(adapter, now)
    ok = (
        all(row["asset"]["status"] == "VERIFIED" and row["d1"]["status"] == "OK" and row["m1"]["status"] == "OK"
            for row in instruments.values())
        and stream_report["status"] == "OK"
    )
    return {
        "status": "LIVE_CHECKED" if ok else "LIVE_CHECK_FAILED",
        "checked_at": now.isoformat(),
        "checked_at_msk": _msk(now),
        "trading_hours": trading_hours(now),
        "stream": stream_report,
        "instruments": instruments,
        "news": news,
    }


def dump(report: dict, secrets: tuple[str, ...]) -> str:
    text = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[redacted]")
    return text


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--stream-seconds", type=float, default=60.0)
    args = parser.parse_args(argv)
    # httpx logs request lines; keep them out so nothing about sessions reaches the console.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    token = os.environ.get("FINAM_API_TOKEN", "").strip()
    if not token:
        print(json.dumps({"status": "NOT_CONFIGURED", "code": "FINAM_NOT_CONFIGURED",
                          "message": "Set FINAM_API_TOKEN in the environment to run the live check."}))
        return 2
    report = asyncio.run(run_check(FinamAdapter(token), stream_seconds=args.stream_seconds))
    print(dump(report, (token,)))
    if not report["trading_hours"]:
        print("NOTE: outside MOEX trading hours; M1 and stream freshness are not representative.", file=sys.stderr)
    return 0 if report["status"] == "LIVE_CHECKED" else 1


if __name__ == "__main__":
    sys.exit(main())
