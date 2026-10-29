"""Read-only MOEX ISS TQBR daily candles for GAZP and GMKN.

ISS interval 24 uses calendar-day candle labels. ``quote_time`` is our
canonical 23:59:59 Europe/Moscow label, not an exchange closing instant.
Only unadjusted source prices are supported, including across stock splits.
Retries belong to the collection pipeline; this adapter bounds each request
and the total number of pages. See https://iss.moex.com/iss/reference/ and
https://www.moex.com/a2193 for the source interface and usage conditions.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from worker.adapters.base import MarketDataAdapter
from worker.io_retry import PermanentIOError, TransientIOError
from worker.russian_market import (
    RU_CURRENCY,
    RU_EXCHANGE,
    RU_SESSION_TIMEZONE,
    require_ru_symbol,
    russian_quote_time,
)
from worker.types import AdjustmentMode, FetchResult, FetchStatus, QuarantineRow, QuoteBar


ISS_URL = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities"
REQUIRED_COLUMNS = frozenset({"begin", "end", "open", "high", "low", "close", "volume"})


def _safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe(item) for item in value]
    return value


def _decimal(value: Any) -> Decimal:
    if value is None or isinstance(value, bool):
        raise ValueError("missing or non-numeric candle value")
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("non-finite candle value")
    return result


def _local_source_time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("source candle timestamp must be a string")
    stamp = datetime.fromisoformat(value)
    # ISS candles carry local, timezone-naive calendar labels. Do not guess
    # the session day of a changed response contract with an explicit offset.
    if stamp.tzinfo is not None:
        raise ValueError("unexpected timezone on ISS local candle label")
    return stamp


class MoexAdapter(MarketDataAdapter):
    provider_name = "moex"

    def __init__(
        self,
        *,
        client: httpx.AsyncClient | None = None,
        timeout_seconds: float = 15.0,
        max_pages: int = 1000,
    ):
        if timeout_seconds <= 0 or max_pages < 1:
            raise ValueError("timeout_seconds and max_pages must be positive")
        self.client = client
        self.timeout_seconds = timeout_seconds
        self.max_pages = max_pages

    async def fetch_daily(
        self,
        symbol: str,
        start: date,
        end: date,
        adjustment_mode: AdjustmentMode,
    ) -> FetchResult:
        try:
            sym = require_ru_symbol(symbol)
        except ValueError as exc:
            raise PermanentIOError("UNSUPPORTED_INSTRUMENT", str(exc)) from exc
        if adjustment_mode != AdjustmentMode.RAW:
            raise PermanentIOError(
                "UNSUPPORTED_ADJUSTMENT", "MOEX ISS daily candles support raw prices only"
            )
        if end <= start:
            raise PermanentIOError("INVALID_WINDOW", "end must be later than start")
        if self.client is not None:
            return await self._fetch(self.client, sym, start, end)
        async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=False) as client:
            return await self._fetch(client, sym, start, end)

    async def _fetch(self, client: httpx.AsyncClient, symbol: str, start: date, end: date) -> FetchResult:
        offset = 0
        seen_pages: set[str] = set()
        bars: dict[date, QuoteBar] = {}
        source_payloads: dict[date, dict] = {}
        conflicted_days: set[date] = set()
        failures: list[QuarantineRow] = []
        url = f"{ISS_URL}/{symbol}/candles.json"
        for _ in range(self.max_pages):
            try:
                response = await client.get(
                    url,
                    params={
                        "iss.meta": "off",
                        "iss.only": "candles",
                        "interval": 24,
                        "from": start.isoformat(),
                        "till": (end - timedelta(days=1)).isoformat(),
                        "start": offset,
                    },
                    timeout=self.timeout_seconds,
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                raise TransientIOError("SOURCE_TRANSPORT", f"MOEX ISS {type(exc).__name__}") from exc
            if response.status_code == 429 or response.status_code == 408 or response.status_code >= 500:
                raise TransientIOError("SOURCE_HTTP", f"MOEX ISS HTTP {response.status_code}")
            if response.status_code != 200:
                raise PermanentIOError("SOURCE_HTTP", f"MOEX ISS HTTP {response.status_code}")
            columns, rows = self._page(response)
            if not rows:
                return FetchResult(
                    status=FetchStatus.OK if bars or failures else FetchStatus.EMPTY,
                    symbol=symbol,
                    bars=[bars[day] for day in sorted(bars)],
                    parse_failures=failures,
                )
            fingerprint = hashlib.sha256(
                json.dumps([columns, rows], default=str, sort_keys=True).encode("utf-8")
            ).hexdigest()
            if fingerprint in seen_pages:
                raise PermanentIOError("SOURCE_PAGINATION", "MOEX ISS repeated a non-empty page")
            seen_pages.add(fingerprint)
            for row in rows:
                payload = {"board": "TQBR", "columns": columns, "row": _safe(row)}
                quote_time = None
                try:
                    if not isinstance(row, list) or len(row) != len(columns):
                        raise ValueError("candle row does not match columns")
                    fields = dict(zip(columns, row))
                    begin = _local_source_time(fields["begin"])
                    finish = _local_source_time(fields["end"])
                    day = begin.date()
                    quote_time = russian_quote_time(day)
                    if finish.date() != day or finish < begin:
                        raise ValueError("daily candle must begin and end on the same local date")
                    if not start <= day < end:
                        failures.append(self._quarantine(symbol, quote_time, "OUTSIDE_WINDOW", "source candle is outside [start, end)", payload))
                        continue
                    volume = _decimal(fields["volume"])
                    if volume < 0 or volume != volume.to_integral_value():
                        raise ValueError("volume must be a non-negative integer number of securities")
                    values = [_decimal(fields[key]) for key in ("open", "high", "low", "close")]
                    open_, high, low, close = values
                    if any(value <= 0 for value in values) or not (low <= open_ <= high and low <= close <= high):
                        raise ValueError("invalid daily OHLC prices")
                    bar = QuoteBar(
                        provider=self.provider_name, symbol=symbol, exchange=RU_EXCHANGE,
                        interval="1d", quote_time=quote_time, open=open_, high=high,
                        low=low, close=close, volume=int(volume), adjustment_mode=AdjustmentMode.RAW,
                        session_timezone=RU_SESSION_TIMEZONE, currency=RU_CURRENCY,
                    )
                    if day in conflicted_days:
                        failures.append(self._quarantine(symbol, quote_time, "CONFLICTING_CANDLE", "more than one source value for this daily candle", payload))
                        continue
                    if day in bars:
                        if bars[day].ohlcv_tuple() != bar.ohlcv_tuple():
                            failures.append(self._quarantine(symbol, quote_time, "CONFLICTING_CANDLE", "more than one source value for this daily candle", source_payloads[day]))
                            bars.pop(day)
                            conflicted_days.add(day)
                            reason = "CONFLICTING_CANDLE"
                        else:
                            reason = "DUPLICATE_CANDLE"
                        failures.append(self._quarantine(symbol, quote_time, reason, "repeated source daily candle", payload))
                        continue
                    bars[day] = bar
                    source_payloads[day] = payload
                except (InvalidOperation, ValueError, TypeError, KeyError, OverflowError) as exc:
                    failures.append(self._quarantine(symbol, quote_time, "PARSE_ERROR", str(exc)[:300], payload))
            # ISS offsets count source rows, not accepted/valid candles.
            offset += len(rows)
        raise PermanentIOError("SOURCE_PAGE_LIMIT", f"MOEX ISS exceeded {self.max_pages} pages")

    @staticmethod
    def _page(response: httpx.Response) -> tuple[list[str], list]:
        try:
            payload = json.loads(response.text, parse_float=Decimal, parse_constant=Decimal)
            candles = payload["candles"]
            columns, rows = candles["columns"], candles["data"]
            if not isinstance(columns, list) or not all(isinstance(item, str) for item in columns):
                raise ValueError("invalid columns")
            if len(columns) != len(set(columns)) or not REQUIRED_COLUMNS.issubset(columns):
                raise ValueError("missing or duplicated candle columns")
            if not isinstance(rows, list):
                raise ValueError("invalid candle data")
            return columns, rows
        except (ValueError, TypeError, KeyError) as exc:
            raise PermanentIOError("SOURCE_SCHEMA", "MOEX ISS did not return the expected candles table") from exc

    def _quarantine(self, symbol: str, quote_time, reason: str, detail: str, payload: dict) -> QuarantineRow:
        return QuarantineRow(
            provider=self.provider_name, symbol=symbol, exchange=RU_EXCHANGE,
            interval="1d", quote_time=quote_time, adjustment_mode="raw",
            reason_code=reason, reason_detail=detail, payload=payload,
        )
