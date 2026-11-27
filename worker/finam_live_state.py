"""Per-instrument live market state for the Finam monitor.

All times are UTC. Data survives a disconnect: the last quote and bars stay
with their own timestamps and the instrument becomes STALE. After a reconnect
each instrument is RECOVERING until its missing closed M1 bars are read back
through REST; a failure there marks only that instrument.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal

MINUTE = timedelta(minutes=1)
WAITING = "WAITING"
LIVE = "LIVE"
STALE = "STALE"
RECOVERING = "RECOVERING"
ERROR = "ERROR"
DEFAULT_LOOKBACK = timedelta(minutes=120)
MAX_BACKFILL = timedelta(days=1)
_OHLCV = ("open", "high", "low", "close", "volume")


def floor_minute(stamp: datetime) -> datetime:
    return stamp.astimezone(timezone.utc).replace(second=0, microsecond=0)


def _aware(stamp: datetime) -> datetime:
    if stamp.utcoffset() is None:
        raise ValueError("timestamps must include a timezone")
    return stamp.astimezone(timezone.utc)


def _parse(value) -> datetime:
    return _aware(value if isinstance(value, datetime) else datetime.fromisoformat(value))


def _bar_values(bar) -> tuple:
    getter = (lambda name: bar[name]) if isinstance(bar, dict) else (lambda name: getattr(bar, name))
    return tuple(Decimal(str(getter(name))) for name in _OHLCV)


@dataclass
class InstrumentState:
    symbol: str
    status: str = WAITING
    last_price: str | None = None
    last_quote_time: datetime | None = None
    last_quote_received_at: datetime | None = None
    bars: dict[datetime, tuple] = field(default_factory=dict)
    bar_received_at: dict[datetime, datetime] = field(default_factory=dict)
    last_update_at: datetime | None = None
    error_code: str | None = None
    duplicates: int = 0
    replaced: int = 0
    backfilled: int = 0


class LiveMarketState:
    def __init__(self, symbols, *, max_bars: int = 600):
        self.instruments = {symbol: InstrumentState(symbol) for symbol in symbols}
        self.max_bars = max_bars
        self.connected = False
        self.disconnects = 0
        self.last_disconnect_at: datetime | None = None
        self.last_disconnect_code: str | None = None

    def on_event(self, event: dict, received_at: datetime) -> None:
        kind = event.get("type")
        symbol = event.get("symbol")
        if symbol not in self.instruments:
            return
        if kind == "quote":
            self.on_quote(symbol, event["last"], _parse(event["quote_time"]), received_at)
        elif kind == "bar":
            self.merge_bars(symbol, [event], received_at)
        elif kind == "symbol_error":
            self.mark_symbol_error(symbol, event.get("code") or "FINAM_STREAM_REJECTED")

    def on_quote(self, symbol: str, price: str, quote_time: datetime, received_at: datetime) -> None:
        item = self.instruments[symbol]
        quote_time = _aware(quote_time)
        if item.last_quote_time is not None and quote_time < item.last_quote_time:
            return
        item.last_price = str(price)
        item.last_quote_time = quote_time
        item.last_quote_received_at = _aware(received_at)
        self._touch(item, received_at)

    def merge_bars(self, symbol: str, bars, received_at: datetime, *, source: str = "stream") -> dict:
        """Insert new minutes, count identical repeats, let a changed bar replace the old one."""
        item = self.instruments[symbol]
        counts = {"inserted": 0, "duplicates": 0, "replaced": 0}
        for bar in bars:
            opened = _parse(bar["quote_time"] if isinstance(bar, dict) else bar.quote_time)
            if opened.second or opened.microsecond:
                raise ValueError("M1 bars must open on a minute boundary")
            values = _bar_values(bar)
            previous = item.bars.get(opened)
            if previous == values:
                counts["duplicates"] += 1
                continue
            counts["replaced" if previous is not None else "inserted"] += 1
            item.bars[opened] = values
            item.bar_received_at[opened] = _aware(received_at)
        excess = len(item.bars) - self.max_bars
        for stamp in sorted(item.bars)[:max(excess, 0)]:
            del item.bars[stamp]
            item.bar_received_at.pop(stamp, None)
        item.duplicates += counts["duplicates"]
        item.replaced += counts["replaced"]
        if source == "rest":
            item.backfilled += counts["inserted"]
        if counts["inserted"] or counts["replaced"]:
            self._touch(item, received_at)
        return counts

    def last_closed_bar(self, symbol: str, now: datetime) -> datetime | None:
        cutoff = _aware(now)
        closed = [stamp for stamp in self.instruments[symbol].bars if stamp + MINUTE <= cutoff]
        return max(closed) if closed else None

    def mark_connected(self) -> None:
        # Every subscription is renewed on a new connection, so earlier errors are retried.
        self.connected = True
        for item in self.instruments.values():
            item.status = RECOVERING

    def mark_disconnected(self, at: datetime, code: str) -> None:
        self.connected = False
        self.disconnects += 1
        self.last_disconnect_at = _aware(at)
        self.last_disconnect_code = code
        for item in self.instruments.values():
            if item.status in (LIVE, WAITING, RECOVERING):
                item.status = STALE

    def mark_symbol_error(self, symbol: str, code: str) -> None:
        item = self.instruments[symbol]
        item.status = ERROR
        item.error_code = code

    def mark_recovered(self, symbol: str) -> None:
        item = self.instruments[symbol]
        item.status = LIVE if self.connected else STALE
        item.error_code = None

    def backfill_windows(self, now: datetime, lookback: timedelta = DEFAULT_LOOKBACK) -> dict:
        """Closed-minute REST windows [start, end) that the stream may have missed."""
        end = floor_minute(_aware(now))
        windows = {}
        for symbol in self.instruments:
            last = self.last_closed_bar(symbol, end)
            start = last + MINUTE if last is not None else end - lookback
            start = max(start, end - MAX_BACKFILL)
            if start < end:
                windows[symbol] = (start, end)
        return windows

    def snapshot(self, now: datetime) -> dict:
        now = _aware(now)
        rows = {}
        for symbol, item in self.instruments.items():
            closed = self.last_closed_bar(symbol, now)
            rows[symbol] = {
                "status": item.status,
                "error_code": item.error_code,
                "last_price": item.last_price,
                "last_quote_time": _iso(item.last_quote_time),
                "last_quote_received_at": _iso(item.last_quote_received_at),
                "last_closed_m1": _iso(closed),
                "last_closed_m1_close": str(item.bars[closed][3]) if closed else None,
                "last_update_at": _iso(item.last_update_at),
                "bars": len(item.bars),
                "duplicates": item.duplicates,
                "replaced": item.replaced,
                "backfilled": item.backfilled,
            }
        return {
            "as_of": now.isoformat(),
            "connected": self.connected,
            "disconnects": self.disconnects,
            "last_disconnect_at": _iso(self.last_disconnect_at),
            "last_disconnect_code": self.last_disconnect_code,
            "instruments": rows,
        }

    def closed_bars(self, symbol: str, now: datetime) -> list[dict]:
        cutoff = _aware(now)
        item = self.instruments[symbol]
        return [
            {"quote_time": stamp, **dict(zip(_OHLCV, values)), "received_at": item.bar_received_at.get(stamp)}
            for stamp, values in sorted(item.bars.items()) if stamp + MINUTE <= cutoff
        ]

    @staticmethod
    def _touch(item: InstrumentState, at: datetime) -> None:
        item.last_update_at = _aware(at)


def _iso(stamp: datetime | None) -> str | None:
    return stamp.isoformat() if stamp else None
