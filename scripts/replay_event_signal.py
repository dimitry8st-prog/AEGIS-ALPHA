"""Replay an opening-range breakout on closed M1 candles."""

from __future__ import annotations

import argparse
import asyncio
from getpass import GetPassWarning, getpass
import json
import os
import sys
import warnings
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import median
from urllib.request import Request, urlopen

EXAMPLE_DIR = Path(__file__).resolve().parents[1] / "docs/examples/anthropic-ipo"
HYPERLIQUID_URL = "https://api.hyperliquid.xyz/info"
MINUTE_MS = 60_000
RANGE_MINUTES = 5
WATCH_MINUTES = 30


def parse_time(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value)
    if timestamp.utcoffset() is None:
        raise ValueError("timestamps must include a timezone")
    return timestamp.astimezone(timezone.utc)


def window_start_ms(value: str) -> int:
    event_time = parse_time(value)
    minute = event_time.replace(second=0, microsecond=0)
    # Exclude the bar straddling the start of observation.
    if minute < event_time:
        minute += timedelta(minutes=1)
    return int(minute.timestamp()) * 1000


def iso_utc(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000, timezone.utc).isoformat(timespec="seconds")


def fetch_hyperliquid(symbol: str, cases: list[dict]) -> list[dict]:
    candles_by_open: dict[int, dict] = {}
    for case in cases:
        start = window_start_ms(case["watch_from"])
        end = start + WATCH_MINUTES * MINUTE_MS
        if end > datetime.now(timezone.utc).timestamp() * 1000:
            raise ValueError("the historical observation window has not closed")
        body = {"type": "candleSnapshot", "req": {
            "coin": symbol, "interval": "1m", "startTime": start, "endTime": end - 1,
        }}
        request = Request(
            HYPERLIQUID_URL, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}
        )
        with urlopen(request, timeout=15) as response:
            rows = json.load(response)
        if not isinstance(rows, list):
            raise ValueError("Hyperliquid did not return a candle list")
        for row in rows:
            opened = row["t"]
            if candles_by_open.setdefault(opened, row) != row:
                raise ValueError(f"conflicting responses for candle at {iso_utc(opened)}")
    return list(candles_by_open.values())


async def fetch_finam(symbol: str, cases: list[dict]) -> list[dict]:
    from worker.adapters.finam_adapter import FinamAdapter
    from worker.io_retry import PermanentIOError, TransientIOError
    from worker.russian_market import require_ru_symbol

    symbol = require_ru_symbol(symbol)
    token = os.environ.get("FINAM_API_TOKEN", "")
    if not token.strip() and sys.stdin.isatty():
        with warnings.catch_warnings():
            warnings.simplefilter("error", GetPassWarning)
            token = getpass("Finam read-only API key: ")
    adapter = FinamAdapter(token)
    candles: dict[int, dict] = {}
    try:
        for case in cases:
            start = datetime.fromtimestamp(window_start_ms(case["watch_from"]) / 1000, timezone.utc)
            end = start + timedelta(minutes=WATCH_MINUTES)
            for bar in await adapter.fetch_minutes(symbol, start, end):
                opened = int(bar.quote_time.timestamp()) * 1000
                row = {
                    "t": opened, "T": opened + MINUTE_MS - 1, "s": symbol, "i": "1m",
                    "o": str(bar.open), "h": str(bar.high), "l": str(bar.low),
                    "c": str(bar.close), "v": str(bar.volume),
                }
                if candles.setdefault(opened, row) != row:
                    raise ValueError(f"conflicting Finam candles at {iso_utc(opened)}")
    except (PermanentIOError, TransientIOError) as exc:
        raise ValueError(f"{exc.code}: {exc}") from None
    return list(candles.values())


def replay(symbol: str, case: dict, rows: list[dict]) -> dict:
    start = window_start_ms(case["watch_from"])
    end = start + WATCH_MINUTES * MINUTE_MS
    expected = range(start, end, MINUTE_MS)
    bars: dict[int, tuple[Decimal, Decimal, Decimal, Decimal]] = {}
    for row in rows:
        if row["s"] != symbol or row["i"] != "1m" or not start <= row["t"] < end:
            continue
        opened = row["t"]
        if opened not in expected or row["T"] != opened + MINUTE_MS - 1 or opened in bars:
            raise ValueError(f"{case['id']}: invalid or duplicate M1 timestamp at {opened}")
        high, low, close, volume = (Decimal(row[key]) for key in ("h", "l", "c", "v"))
        if not (0 < low <= close <= high < Decimal("Infinity") and 0 <= volume < Decimal("Infinity")):
            raise ValueError(f"{case['id']}: invalid price or volume at {opened}")
        bars[opened] = (high, low, close, volume)
    missing = [iso_utc(ts) for ts in expected if ts not in bars]
    if missing:
        raise ValueError(f"{case['id']}: missing {len(missing)} M1 candles, first at {missing[0]}")

    opening = [bars[ts] for ts in expected[:RANGE_MINUTES]]
    ceiling = max(bar[0] for bar in opening)
    floor = min(bar[1] for bar in opening)
    baseline_volume = median(bar[3] for bar in opening)
    watch_from = parse_time(case["watch_from"])
    context = [
        item for item in case.get("public_context", [])
        if parse_time(item["available_by"]) <= watch_from
    ]
    report = {
        **case,
        "public_context": context,
        "observation_start": iso_utc(start),
        "expires_at": iso_utc(end),
        "candles_checked": len(bars),
        "range_high": str(ceiling),
        "range_low": str(floor),
        "candidate": None,
    }
    # A close at the expiry leaves no time for entry.
    for opened in expected[RANGE_MINUTES:-1]:
        _, _, close, volume = bars[opened]
        if floor <= close <= ceiling:
            continue
        short = close < floor
        report["candidate"] = {
            "direction": "SHORT" if short else "LONG",
            "available_at": iso_utc(opened + MINUTE_MS),
            "trigger_level": str(floor if short else ceiling),
            "observed_close": str(close),
            "invalidation_level": str(ceiling if short else floor),
            "breakout_volume_vs_opening_median": (
                str(round(volume / baseline_volume, 2)) if baseline_volume else None
            ),
        }
        break
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, default=EXAMPLE_DIR / "events.json")
    parser.add_argument("--symbol", help="Override the instrument in the event file")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--candles", type=Path, help="Local M1 snapshot JSON array")
    source.add_argument("--live", action="store_true", help="Read the historical windows from the configured provider")
    parser.add_argument("--save-candles", type=Path, help="Save the live response to a new file")
    args = parser.parse_args()
    if args.save_candles and not args.live:
        parser.error("--save-candles requires --live")
    try:
        specification = json.loads(args.events.read_text(encoding="utf-8"))
        symbol, cases = args.symbol or specification["symbol"], specification["cases"]
        if not isinstance(symbol, str) or not symbol or not isinstance(cases, list) or not cases:
            raise ValueError("an instrument and at least one event are required")
        provider = specification.get("provider", "hyperliquid")
        if provider not in ("hyperliquid", "finam"):
            raise ValueError("unsupported market data provider")
        if provider == "finam":
            symbol = symbol.strip().upper()
        if args.live and provider == "finam":
            rows = asyncio.run(fetch_finam(symbol, cases))
        elif args.live:
            rows = fetch_hyperliquid(symbol, cases)
        else:
            if provider == "finam" and args.candles is None:
                raise ValueError("Finam requires --live or a --candles snapshot")
            snapshot = args.candles or EXAMPLE_DIR / "candles.json"
            rows = json.loads(snapshot.read_text(encoding="utf-8"))
        cases_report = [replay(symbol, case, rows) for case in cases]
        if args.save_candles:
            with args.save_candles.open("x", encoding="utf-8") as output:
                records = ",\n".join(json.dumps(row, separators=(",", ":")) for row in rows)
                output.write(f"[\n{records}\n]\n")
    except (OSError, ValueError, KeyError, TypeError, InvalidOperation, EOFError, GetPassWarning) as exc:
        print(f"Replay failed: {exc}", file=sys.stderr)
        return 1
    report = {
        "symbol": symbol,
        "provider": provider,
        "source_url": HYPERLIQUID_URL if provider == "hyperliquid" else "https://api.finam.ru",
        "input": "live" if args.live else "snapshot",
        "range_minutes": RANGE_MINUTES,
        "watch_minutes": WATCH_MINUTES,
        "cases": cases_report,
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
