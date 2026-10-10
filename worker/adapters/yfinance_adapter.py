"""Yahoo Finance adapter via yfinance (unofficial research/education library)."""

from __future__ import annotations

import asyncio
import math
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from worker.adapters.base import MarketDataAdapter
from worker.io_retry import PermanentIOError, TransientIOError
from worker.types import (
    SESSION_TIMEZONE,
    AdjustmentMode,
    FetchResult,
    FetchStatus,
    QuarantineRow,
    QuoteBar,
)


def _json_safe(value: Any) -> Any:
    """Serialize source cells for quarantine JSON (preserve NaN / missing)."""
    if value is None:
        return None
    try:
        import pandas as pd

        if value is pd.NA or value is pd.NaT:
            return None
        if isinstance(value, pd.Timestamp):
            if pd.isna(value):
                return None
            return value.isoformat()
    except Exception:
        pass
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value
    try:
        # numpy scalars / pandas NA via isna
        import pandas as pd

        if pd.isna(value):
            # Distinguish float NaN already handled; other NA → null
            if isinstance(value, (float, int)):
                return "NaN"
            return None
    except Exception:
        pass
    if isinstance(value, (Decimal, int, str, bool)):
        return str(value) if isinstance(value, Decimal) else value
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        raise InvalidOperation("missing")
    try:
        import pandas as pd

        if pd.isna(value):
            raise InvalidOperation("NaN")
    except InvalidOperation:
        raise
    except Exception:
        pass
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        raise InvalidOperation(str(value))
    return Decimal(str(value))


def _raw_cell(row: Any, column: str) -> Any:
    try:
        if column in row.index:
            return row[column]
    except Exception:
        pass
    try:
        return row[column]
    except Exception:
        return None


def _session_day_from_index(idx: Any, tz: ZoneInfo) -> date:
    if hasattr(idx, "to_pydatetime"):
        dt = idx.to_pydatetime()
    else:
        dt = idx
    if getattr(dt, "tzinfo", None) is not None:
        return dt.astimezone(tz).date()
    if hasattr(dt, "date"):
        return dt.date()
    return date.fromisoformat(str(dt)[:10])


def _row_payload(
    *,
    symbol: str,
    idx: Any,
    row: Any,
    adjustment_mode: AdjustmentMode,
    quote_time: datetime | None,
) -> dict[str, Any]:
    return {
        "provider": "yfinance",
        "symbol": symbol,
        "exchange": "XNAS",
        "interval": "1d",
        "index": _json_safe(idx),
        "quote_time": quote_time.isoformat() if quote_time else None,
        "Open": _json_safe(_raw_cell(row, "Open")),
        "High": _json_safe(_raw_cell(row, "High")),
        "Low": _json_safe(_raw_cell(row, "Low")),
        "Close": _json_safe(_raw_cell(row, "Close")),
        "Volume": _json_safe(_raw_cell(row, "Volume")),
        "adjustment_mode": adjustment_mode.value,
        "raw_columns": [str(c) for c in getattr(row, "index", [])],
    }


def _is_transient_source_error(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError, ConnectionError, OSError)):
        return True
    name = type(exc).__name__.lower()
    if any(tok in name for tok in ("timeout", "connection", "chunked", "protocol")):
        return True
    text = str(exc).lower()
    markers = (
        "timeout",
        "timed out",
        "connection",
        "temporarily",
        "rate limit",
        "too many requests",
        "429",
        "503",
        "502",
        "504",
        "reset by peer",
        "name or service not known",
        "temporary failure",
        "unavailable",
    )
    return any(m in text for m in markers)


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
        except PermanentIOError:
            raise
        except TransientIOError:
            raise
        except Exception as exc:
            msg = str(exc)[:500]
            if _is_transient_source_error(exc):
                raise TransientIOError("SOURCE_TRANSIENT", msg) from exc
            raise PermanentIOError("SOURCE_ERROR", msg) from exc

        # Empty frame is a normal outcome (holiday / no data), not a network error.
        if frame is None or getattr(frame, "empty", True):
            return FetchResult(status=FetchStatus.EMPTY, symbol=sym, bars=[])

        tz = ZoneInfo(SESSION_TIMEZONE)
        bars: list[QuoteBar] = []
        parse_failures: list[QuarantineRow] = []

        for idx, row in frame.iterrows():
            quote_time: datetime | None = None
            try:
                session_day = _session_day_from_index(idx, tz)
                quote_time = datetime.combine(session_day, time(16, 0), tzinfo=tz)
                bar = QuoteBar(
                    provider=self.provider_name,
                    symbol=sym,
                    exchange="XNAS",
                    interval="1d",
                    quote_time=quote_time,
                    open=_to_decimal(_raw_cell(row, "Open")),
                    high=_to_decimal(_raw_cell(row, "High")),
                    low=_to_decimal(_raw_cell(row, "Low")),
                    close=_to_decimal(_raw_cell(row, "Close")),
                    volume=int(_to_decimal(_raw_cell(row, "Volume"))),
                    adjustment_mode=adjustment_mode,
                )
                bars.append(bar)
            except (KeyError, InvalidOperation, ValueError, TypeError, OverflowError) as exc:
                payload = _row_payload(
                    symbol=sym,
                    idx=idx,
                    row=row,
                    adjustment_mode=adjustment_mode,
                    quote_time=quote_time,
                )
                parse_failures.append(
                    QuarantineRow(
                        provider=self.provider_name,
                        symbol=sym,
                        exchange="XNAS",
                        interval="1d",
                        quote_time=quote_time,
                        adjustment_mode=adjustment_mode.value,
                        reason_code="PARSE_ERROR",
                        reason_detail=f"{type(exc).__name__}: {exc}"[:500],
                        payload=payload,
                    )
                )

        if not bars and not parse_failures:
            return FetchResult(status=FetchStatus.EMPTY, symbol=sym, bars=[])
        return FetchResult(
            status=FetchStatus.OK,
            symbol=sym,
            bars=bars,
            parse_failures=parse_failures,
        )
