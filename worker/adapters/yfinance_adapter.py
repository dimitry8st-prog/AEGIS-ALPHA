"""Yahoo Finance adapter via yfinance (unofficial research/education library)."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from worker.adapters.base import MarketDataAdapter
from worker.types import (
    SESSION_TIMEZONE,
    AdjustmentMode,
    FetchResult,
    FetchStatus,
    QuoteBar,
)


def _to_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _download_sync(
    symbol: str,
    start: date,
    end: date,
    auto_adjust: bool,
):
    import yfinance as yf

    # Official yfinance docs: start inclusive, end exclusive; interval 1d.
    return yf.download(
        tickers=symbol,
        start=start.isoformat(),
        end=end.isoformat(),
        interval="1d",
        auto_adjust=auto_adjust,
        progress=False,
        threads=False,
        group_by="column",
        multi_level_index=False,
    )


class YFinanceAdapter(MarketDataAdapter):
    provider_name = "yfinance"

    async def fetch_daily(
        self,
        symbol: str,
        start: date,
        end: date,
        adjustment_mode: AdjustmentMode,
    ) -> FetchResult:
        sym = symbol.upper()
        auto_adjust = adjustment_mode is AdjustmentMode.AUTO_ADJUSTED
        try:
            frame = await asyncio.to_thread(
                _download_sync, sym, start, end, auto_adjust
            )
        except Exception as exc:  # network / library errors
            return FetchResult(
                status=FetchStatus.UNAVAILABLE,
                symbol=sym,
                error_code="SOURCE_ERROR",
                error_message=str(exc)[:500],
            )

        if frame is None or getattr(frame, "empty", True):
            return FetchResult(status=FetchStatus.EMPTY, symbol=sym, bars=[])

        tz = ZoneInfo(SESSION_TIMEZONE)
        bars: list[QuoteBar] = []
        for idx, row in frame.iterrows():
            try:
                if hasattr(idx, "to_pydatetime"):
                    dt = idx.to_pydatetime()
                else:
                    dt = idx
                if getattr(dt, "tzinfo", None) is not None:
                    session_day = dt.astimezone(tz).date()
                else:
                    session_day = dt.date() if hasattr(dt, "date") else date.fromisoformat(str(dt)[:10])
                quote_time = datetime.combine(session_day, time(16, 0), tzinfo=tz)
                bar = QuoteBar(
                    provider=self.provider_name,
                    symbol=sym,
                    exchange="XNAS",
                    interval="1d",
                    quote_time=quote_time,
                    open=_to_decimal(row["Open"]),
                    high=_to_decimal(row["High"]),
                    low=_to_decimal(row["Low"]),
                    close=_to_decimal(row["Close"]),
                    volume=int(row["Volume"]),
                    adjustment_mode=adjustment_mode,
                )
                bars.append(bar)
            except (KeyError, InvalidOperation, ValueError, TypeError) as exc:
                # Leave invalid rows for validator via a sentinel payload bar skipped;
                # pipeline validates after fetch — drop unparseable rows here as EMPTY-ish.
                continue

        if not bars:
            return FetchResult(status=FetchStatus.EMPTY, symbol=sym, bars=[])
        return FetchResult(status=FetchStatus.OK, symbol=sym, bars=bars)
