"""Deterministic offline source for tests and demos (no network)."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from worker.types import (
    SESSION_TIMEZONE,
    AdjustmentMode,
    FetchResult,
    FetchStatus,
    QuoteBar,
)
from worker.adapters.base import MarketDataAdapter


# Fixed trading days with known OHLCV (skips weekend-like gaps by listing dates).
_FIXTURE: dict[str, list[tuple[date, Decimal, Decimal, Decimal, Decimal, int]]] = {
    "AAPL": [
        (date(2024, 1, 2), Decimal("185.00"), Decimal("187.50"), Decimal("184.20"), Decimal("186.80"), 50_000_000),
        (date(2024, 1, 3), Decimal("186.50"), Decimal("188.00"), Decimal("185.00"), Decimal("187.10"), 48_000_000),
        (date(2024, 1, 4), Decimal("187.00"), Decimal("189.20"), Decimal("186.40"), Decimal("188.50"), 52_000_000),
        (date(2024, 1, 5), Decimal("188.40"), Decimal("190.00"), Decimal("187.80"), Decimal("189.20"), 45_000_000),
    ],
    "MSFT": [
        (date(2024, 1, 2), Decimal("370.00"), Decimal("375.00"), Decimal("368.50"), Decimal("373.20"), 22_000_000),
        (date(2024, 1, 3), Decimal("373.00"), Decimal("376.50"), Decimal("371.00"), Decimal("374.80"), 21_000_000),
        (date(2024, 1, 4), Decimal("374.50"), Decimal("378.00"), Decimal("373.00"), Decimal("377.10"), 23_000_000),
        (date(2024, 1, 5), Decimal("377.00"), Decimal("379.50"), Decimal("375.50"), Decimal("378.40"), 20_000_000),
    ],
    "NVDA": [
        (date(2024, 1, 2), Decimal("480.00"), Decimal("495.00"), Decimal("478.00"), Decimal("490.00"), 40_000_000),
        (date(2024, 1, 3), Decimal("490.00"), Decimal("505.00"), Decimal("488.00"), Decimal("500.00"), 42_000_000),
        (date(2024, 1, 4), Decimal("500.00"), Decimal("510.00"), Decimal("495.00"), Decimal("508.00"), 44_000_000),
        (date(2024, 1, 5), Decimal("508.00"), Decimal("515.00"), Decimal("502.00"), Decimal("512.00"), 41_000_000),
    ],
}


class OfflineAdapter(MarketDataAdapter):
    provider_name = "offline"

    def __init__(
        self,
        *,
        fail_symbols: set[str] | None = None,
        empty_symbols: set[str] | None = None,
        corrupt_bars: dict[str, list[date]] | None = None,
        revised_close: dict[tuple[str, date], Decimal] | None = None,
    ) -> None:
        self.fail_symbols = {s.upper() for s in (fail_symbols or set())}
        self.empty_symbols = {s.upper() for s in (empty_symbols or set())}
        self.corrupt_bars = {
            k.upper(): set(v) for k, v in (corrupt_bars or {}).items()
        }
        self.revised_close = {
            (sym.upper(), d): price for (sym, d), price in (revised_close or {}).items()
        }

    async def fetch_daily(
        self,
        symbol: str,
        start: date,
        end: date,
        adjustment_mode: AdjustmentMode,
    ) -> FetchResult:
        sym = symbol.upper()
        if sym in self.fail_symbols:
            return FetchResult(
                status=FetchStatus.UNAVAILABLE,
                symbol=sym,
                error_code="SOURCE_UNAVAILABLE",
                error_message=f"offline adapter forced failure for {sym}",
            )
        if sym in self.empty_symbols:
            return FetchResult(status=FetchStatus.EMPTY, symbol=sym, bars=[])

        rows = _FIXTURE.get(sym)
        if rows is None:
            return FetchResult(
                status=FetchStatus.UNAVAILABLE,
                symbol=sym,
                error_code="UNKNOWN_SYMBOL",
                error_message=f"no offline fixture for {sym}",
            )

        tz = ZoneInfo(SESSION_TIMEZONE)
        bars: list[QuoteBar] = []
        for session_day, o, h, l, c, vol in rows:
            if not (start <= session_day < end):
                continue
            close = self.revised_close.get((sym, session_day), c)
            if session_day in self.corrupt_bars.get(sym, set()):
                # Invalid OHLC: high < low
                h, l = Decimal("1.00"), Decimal("10.00")
            quote_time = datetime.combine(
                session_day, time(hour=16, minute=0), tzinfo=tz
            )
            bars.append(
                QuoteBar(
                    provider=self.provider_name,
                    symbol=sym,
                    exchange="XNAS",
                    interval="1d",
                    quote_time=quote_time,
                    open=o,
                    high=h,
                    low=l,
                    close=close,
                    volume=vol,
                    adjustment_mode=adjustment_mode,
                )
            )

        if not bars:
            return FetchResult(status=FetchStatus.EMPTY, symbol=sym, bars=[])
        return FetchResult(status=FetchStatus.OK, symbol=sym, bars=bars)
