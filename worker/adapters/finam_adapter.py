"""Read-only Finam REST daily bars; the only POST exchanges a secret for JWT.

No account, order or trading methods are exposed. The source timestamp is
converted to its Moscow calendar date and kept in quarantine payloads. The
stored 23:59:59 label identifies that day; it is not the exchange close.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
import math
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from worker.adapters.base import MarketDataAdapter
from worker.io_retry import PermanentIOError, TransientIOError
from worker.russian_market import (
    RU_CURRENCY, RU_EXCHANGE, RU_SESSION_TIMEZONE,
    require_ru_symbol, russian_quote_time,
)
from worker.types import AdjustmentMode, FetchResult, FetchStatus, QuarantineRow, QuoteBar

FINAM_BASE_URL = "https://api.finam.ru"
CHUNK_DAYS = 31
MAX_WINDOW_DAYS = 365
MAX_CHUNKS = 12
_MOSCOW = ZoneInfo(RU_SESSION_TIMEZONE)
_BAR_FIELDS = ("timestamp", "open", "high", "low", "close", "volume", "is_data_snapshot")


class FinamAdapter(MarketDataAdapter):
    provider_name = "finam"

    def __init__(
        self,
        api_token: str | None = None,
        *,
        client: httpx.AsyncClient | None = None,
        timeout_seconds: float = 15.0,
    ):
        self._api_token = api_token
        self._client = client
        if not 0 < timeout_seconds <= 60:
            raise ValueError("Finam timeout must be in (0, 60] seconds")
        self._timeout = httpx.Timeout(timeout_seconds)

    async def fetch_daily(self, symbol, start, end, adjustment_mode) -> FetchResult:
        if not self._api_token or not self._api_token.strip():
            raise PermanentIOError("FINAM_NOT_CONFIGURED", "FINAM_API_TOKEN is required")
        try:
            sym = require_ru_symbol(symbol)
        except ValueError:
            raise PermanentIOError("UNSUPPORTED_INSTRUMENT", "Finam research symbols are GAZP, GMKN") from None
        if adjustment_mode is not AdjustmentMode.RAW:
            raise PermanentIOError("UNSUPPORTED_ADJUSTMENT", "Finam supports raw source bars only")
        if end <= start or (end - start).days > MAX_WINDOW_DAYS:
            raise PermanentIOError("INVALID_WINDOW", "Finam requires 0 < window <= 365 calendar days")

        if self._client is not None:
            return await self._fetch(self._client, sym, start, end)
        async with httpx.AsyncClient(follow_redirects=False) as client:
            return await self._fetch(client, sym, start, end)

    async def _fetch(self, client, symbol: str, start: date, end: date) -> FetchResult:
        # A fresh session per fetch avoids assuming a hard-coded JWT lifetime.
        auth = await self._request(client, "POST", "/v1/sessions", json={"secret": self._api_token})
        jwt = auth.get("token") if isinstance(auth, dict) else None
        if not isinstance(jwt, str) or not jwt or not jwt.isascii() or any(c.isspace() for c in jwt):
            raise PermanentIOError("FINAM_INVALID_AUTH_RESPONSE", "Finam returned no usable session token")

        bars: list[QuoteBar] = []
        failures: list[QuarantineRow] = []
        seen: set[date] = set()
        cursor = start
        for _ in range(MAX_CHUNKS):
            if cursor >= end:
                break
            chunk_end = cursor + timedelta(days=min(CHUNK_DAYS, (end - cursor).days))
            payload = await self._request(
                client, "GET", f"/v1/instruments/{symbol}@{RU_EXCHANGE}/bars",
                headers={"Authorization": f"Bearer {jwt}"},
                params={
                    "timeframe": "TIME_FRAME_D",
                    "interval.start_time": _midnight(cursor),
                    "interval.end_time": _midnight(chunk_end),
                },
            )
            if not isinstance(payload, dict) or not isinstance(payload.get("bars"), list):
                raise PermanentIOError("FINAM_INVALID_RESPONSE", "Finam bars response has an invalid schema")
            if payload.get("symbol") != f"{symbol}@{RU_EXCHANGE}":
                raise PermanentIOError("FINAM_SYMBOL_MISMATCH", "Finam returned a different instrument")
            if len(payload["bars"]) > CHUNK_DAYS:
                raise PermanentIOError("FINAM_TOO_MANY_BARS", "Daily response exceeds requested chunk bounds")
            for row in payload["bars"]:
                quote_time = None
                try:
                    if not isinstance(row, dict):
                        raise ValueError("invalid row")
                    source_time = _timestamp(row.get("timestamp"))
                    session_day = source_time.astimezone(_MOSCOW).date()
                    if not cursor <= session_day < chunk_end:
                        raise ValueError("outside requested chunk")
                    if session_day in seen:
                        raise ValueError("duplicate daily source row")
                    quote_time = russian_quote_time(session_day)
                    prices = [_decimal(row.get(name)) for name in ("open", "high", "low", "close")]
                    volume = _decimal(row.get("volume"))
                    if volume < 0 or volume != volume.to_integral_value():
                        raise ValueError("invalid share volume")
                    bars.append(QuoteBar(
                        provider=self.provider_name, symbol=symbol, exchange=RU_EXCHANGE,
                        interval="1d", quote_time=quote_time,
                        open=prices[0], high=prices[1], low=prices[2], close=prices[3],
                        volume=int(volume), adjustment_mode=AdjustmentMode.RAW,
                        session_timezone=RU_SESSION_TIMEZONE, currency=RU_CURRENCY,
                    ))
                    seen.add(session_day)
                except (ValueError, TypeError, InvalidOperation, OverflowError):
                    failures.append(QuarantineRow(
                        provider=self.provider_name, symbol=symbol, exchange=RU_EXCHANGE,
                        interval="1d", quote_time=quote_time, adjustment_mode="raw",
                        reason_code="FINAM_PARSE_ERROR",
                        reason_detail="Invalid, duplicate, or out-of-window Finam daily bar",
                        payload=_safe_payload(row, self._api_token, jwt),
                    ))
            cursor = chunk_end
        if cursor < end:
            raise PermanentIOError("FINAM_CHUNK_LIMIT", "Finam request exceeded the bounded chunk limit")
        bars.sort(key=lambda bar: bar.quote_time)
        status = FetchStatus.OK if bars or failures else FetchStatus.EMPTY
        return FetchResult(status=status, symbol=symbol, bars=bars, parse_failures=failures)

    async def _request(self, client, method, path, **kwargs):
        try:
            response = await client.request(
                method, FINAM_BASE_URL + path, timeout=self._timeout,
                follow_redirects=False, **kwargs,
            )
        except httpx.RequestError:
            # Never include the exception/request/body: an auth request contains a secret.
            raise TransientIOError("FINAM_NETWORK_ERROR", "Finam request failed or timed out") from None
        status = response.status_code
        if status == 429 or status >= 500:
            raise TransientIOError("FINAM_SOURCE_TRANSIENT", f"Finam temporary HTTP {status}")
        if status in (401, 403):
            raise PermanentIOError("FINAM_AUTHORIZATION_FAILED", "Finam token or market-data permissions rejected")
        if status != 200:
            raise PermanentIOError("FINAM_HTTP_ERROR", f"Finam request rejected (HTTP {status})")
        try:
            return response.json()
        except ValueError:
            raise PermanentIOError("FINAM_INVALID_RESPONSE", "Finam returned invalid JSON") from None


def _midnight(day: date) -> str:
    return datetime.combine(day, time.min, tzinfo=_MOSCOW).isoformat()


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be RFC3339")
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return stamp


def _decimal(value: Any) -> Decimal:
    if not isinstance(value, dict) or not isinstance(value.get("value"), str):
        raise ValueError("Finam decimals require string value")
    number = Decimal(value["value"])
    if not number.is_finite():
        raise ValueError("non-finite decimal")
    return number


def _safe_payload(row, secret: str, jwt: str) -> dict:
    """Keep source bar cells for quarantine, never an echoed auth secret/token."""
    def safe(value):
        if isinstance(value, str):
            return value.replace(secret, "[redacted]").replace(jwt, "[redacted]")
        if isinstance(value, dict):
            return {str(key): safe(item) for key, item in value.items() if key == "value"}
        if isinstance(value, float) and not math.isfinite(value):
            return str(value)
        if value is None or isinstance(value, (int, float, bool)):
            return value
        return "[invalid source cell]"
    if not isinstance(row, dict):
        return {"row_type": type(row).__name__}
    return {key: safe(row[key]) for key in _BAR_FIELDS if key in row}
