"""Measure how fast AEGIS saw an event and how price reacted afterwards.

Only closed M1 bars are used. The baseline is the last bar that had closed by
the moment the event became available to AEGIS; the reaction window starts at
the first minute that opens at or after that moment. A stock is compared with
the index only on minutes where both have a closed bar.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

MINUTE = timedelta(minutes=1)


def _stamp(value) -> datetime:
    stamp = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if stamp.utcoffset() is None:
        raise ValueError("timestamps must include a timezone")
    return stamp.astimezone(timezone.utc)


def _closes(bars, now: datetime) -> dict[datetime, Decimal]:
    closes = {}
    for bar in bars:
        opened = _stamp(bar["quote_time"])
        if opened + MINUTE <= now:
            closes[opened] = Decimal(str(bar["close"]))
    return closes


def _first_minute_at_or_after(stamp: datetime) -> datetime:
    floor = stamp.replace(second=0, microsecond=0)
    return floor if floor == stamp else floor + MINUTE


def _change(base: Decimal, close: Decimal) -> float:
    return round(float((close - base) / base * 100), 4)


def measure_reaction(
    event: dict,
    bars,
    *,
    now: datetime,
    window_minutes: int = 5,
    index_bars=None,
) -> dict:
    """`event` needs published_at and available_at; provider_received_at is optional."""
    now = _stamp(now)
    published = _stamp(event["published_at"])
    available = _stamp(event["available_at"])
    if available < published:
        raise ValueError("an event cannot be available before publication")
    provider = _stamp(event["provider_received_at"]) if event.get("provider_received_at") else None
    report = {
        "published_at": published.isoformat(),
        "provider_received_at": provider.isoformat() if provider else None,
        "available_at": available.isoformat(),
        "latency_seconds": {
            "publication_to_provider": (provider - published).total_seconds() if provider else None,
            "publication_to_aegis": (available - published).total_seconds(),
        },
        "window_minutes": window_minutes,
        "status": "WAITING_FOR_BARS",
        "baseline": None,
        "reaction": None,
        "index": None,
    }
    closes = _closes(bars, now)
    before = [stamp for stamp in closes if stamp + MINUTE <= available]
    if not before:
        report["status"] = "NO_BASELINE"
        return report
    base_time = max(before)
    start = _first_minute_at_or_after(available)
    window = [start + MINUTE * step for step in range(window_minutes)]
    after = [stamp for stamp in window if stamp in closes]
    report["baseline"] = {"bar_open": base_time.isoformat(), "close": str(closes[base_time])}
    if not after:
        return report
    last = max(after)
    report["status"] = "MEASURED" if len(after) == window_minutes else "PARTIAL"
    report["reaction"] = {
        "first_bar_open": after[0].isoformat(),
        "first_bar_closed_at": (after[0] + MINUTE).isoformat(),
        "seconds_from_availability_to_first_close": (after[0] + MINUTE - available).total_seconds(),
        "last_bar_open": last.isoformat(),
        "bars": len(after),
        "change_pct": _change(closes[base_time], closes[last]),
    }
    if index_bars is not None:
        report["index"] = _index_comparison(_closes(index_bars, now), closes, base_time, after)
    return report


def _index_comparison(index: dict, stock: dict, base_time: datetime, after: list[datetime]) -> dict:
    common = [stamp for stamp in after if stamp in index]
    if base_time not in index or not common:
        return {"status": "NO_COMMON_INTERVAL"}
    last = max(common)
    stock_change = _change(stock[base_time], stock[last])
    index_change = _change(index[base_time], index[last])
    return {
        "status": "MEASURED",
        "common_minutes": len(common),
        "last_bar_open": last.isoformat(),
        "stock_change_pct": stock_change,
        "index_change_pct": index_change,
        "excess_change_pct": round(stock_change - index_change, 4),
    }
