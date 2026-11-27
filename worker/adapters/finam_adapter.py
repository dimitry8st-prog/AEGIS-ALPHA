"""Read-only Finam REST bars; the only POST exchanges a secret for JWT.

No account, order or trading methods are exposed. The source timestamp is
converted to its Moscow calendar date and kept in quarantine payloads. The
stored 23:59:59 label identifies that day; it is not the exchange close.
"""

from __future__ import annotations

import asyncio
from collections import Counter, deque
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
import math
import time as monotonic_time
from typing import Any, Callable
from zoneinfo import ZoneInfo

import httpx

from worker.adapters.base import MarketDataAdapter
from worker.io_retry import PermanentIOError, TransientIOError
from worker.russian_market import (
    RU_CURRENCY, RU_EXCHANGE, RU_SESSION_TIMEZONE, RU_WATCHLIST,
    finam_symbol, require_ru_symbol, ru_instrument, russian_quote_time,
)
from worker.types import AdjustmentMode, FetchResult, FetchStatus, QuarantineRow, QuoteBar

FINAM_BASE_URL = "https://api.finam.ru"
CHUNK_DAYS = 31
MAX_WINDOW_DAYS = 365
MAX_CHUNKS = 12
# Documented limit for every REST method: 200 requests per minute.
REQUESTS_PER_MINUTE = 200
MAX_RETRY_AFTER_SECONDS = 120.0
NEWS_PAGE_SIZE = 100
NEWS_MAX_PAGES = 20
_MOSCOW = ZoneInfo(RU_SESSION_TIMEZONE)
_BAR_FIELDS = ("timestamp", "open", "high", "low", "close", "volume", "is_data_snapshot")
_UNSUPPORTED = "Finam research symbols are " + ", ".join(RU_WATCHLIST)


@dataclass(frozen=True)
class MinuteHistory:
    bars: list[QuoteBar]
    rejected: dict[str, int]


@dataclass(frozen=True)
class NewsHistoryPage:
    items: list[dict]
    pages: int
    truncated: bool


class RequestBudget:
    """Sliding one-minute window shared by every adapter in the process."""

    def __init__(
        self,
        per_minute: int = REQUESTS_PER_MINUTE,
        *,
        clock: Callable[[], float] = monotonic_time.monotonic,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ):
        self.per_minute = per_minute
        self._clock = clock
        self._sleep = sleep
        self._stamps: deque[float] = deque()
        self._blocked_until = 0.0

    async def acquire(self) -> None:
        while True:
            now = self._clock()
            while self._stamps and now - self._stamps[0] >= 60:
                self._stamps.popleft()
            wait = self._blocked_until - now
            if len(self._stamps) >= self.per_minute:
                wait = max(wait, self._stamps[0] + 60 - now)
            if wait <= 0:
                # No await between the check and the append: safe within one event loop.
                self._stamps.append(now)
                return
            await self._sleep(wait)

    def defer(self, seconds: float) -> None:
        self._blocked_until = max(self._blocked_until, self._clock() + seconds)


FINAM_REQUEST_BUDGET = RequestBudget()


def retry_after_seconds(value: str | None, now: datetime | None = None) -> float | None:
    """Parse Retry-After as delta-seconds or an HTTP date; cap unreasonable waits."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        seconds = float(value)
    else:
        try:
            moment = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        seconds = (moment - (now or datetime.now(timezone.utc))).total_seconds()
    return min(max(seconds, 0.0), MAX_RETRY_AFTER_SECONDS)


class FinamAdapter(MarketDataAdapter):
    provider_name = "finam"

    def __init__(
        self,
        api_token: str | None = None,
        *,
        client: httpx.AsyncClient | None = None,
        timeout_seconds: float = 15.0,
        budget: RequestBudget | None = None,
    ):
        self._api_token = api_token
        self._client = client
        self._budget = budget or FINAM_REQUEST_BUDGET
        if not 0 < timeout_seconds <= 60:
            raise ValueError("Finam timeout must be in (0, 60] seconds")
        self._timeout = httpx.Timeout(timeout_seconds)

    async def fetch_daily(self, symbol, start, end, adjustment_mode) -> FetchResult:
        if not self._api_token or not self._api_token.strip():
            raise PermanentIOError("FINAM_NOT_CONFIGURED", "FINAM_API_TOKEN is required")
        try:
            sym = require_ru_symbol(symbol, include_context=True)
        except ValueError:
            raise PermanentIOError("UNSUPPORTED_INSTRUMENT", _UNSUPPORTED) from None
        if adjustment_mode is not AdjustmentMode.RAW:
            raise PermanentIOError("UNSUPPORTED_ADJUSTMENT", "Finam supports raw source bars only")
        if end <= start or (end - start).days > MAX_WINDOW_DAYS:
            raise PermanentIOError("INVALID_WINDOW", "Finam requires 0 < window <= 365 calendar days")

        if self._client is not None:
            return await self._fetch(self._client, sym, start, end)
        async with httpx.AsyncClient(follow_redirects=False) as client:
            return await self._fetch(client, sym, start, end)

    async def _fetch(self, client, symbol: str, start: date, end: date) -> FetchResult:
        jwt = await self._open_session(client)

        bars: list[QuoteBar] = []
        failures: list[QuarantineRow] = []
        seen: set[date] = set()
        ignored_boundary_rows = 0
        cursor = start
        for _ in range(MAX_CHUNKS):
            if cursor >= end:
                break
            chunk_end = cursor + timedelta(days=min(CHUNK_DAYS, (end - cursor).days))
            rows = await self._read_bars(
                client, symbol, jwt, "TIME_FRAME_D", _midnight(cursor), _midnight(chunk_end)
            )
            if len(rows) > CHUNK_DAYS + 1:
                raise PermanentIOError("FINAM_TOO_MANY_BARS", "Daily response exceeds requested chunk bounds")
            ignored_in_chunk = 0
            for row in rows:
                quote_time = None
                try:
                    if not isinstance(row, dict):
                        raise ValueError("invalid row")
                    source_time = _timestamp(row.get("timestamp"))
                    session_day = source_time.astimezone(_MOSCOW).date()
                    # Finam can include one candle from the day before the requested start.
                    preceding_day = (cursor - session_day).days == 1
                    boundary_row = preceding_day and ignored_in_chunk == 0
                    if not (cursor <= session_day < chunk_end or boundary_row):
                        raise ValueError("outside requested chunk")
                    if not boundary_row and session_day in seen:
                        raise ValueError("duplicate daily source row")
                    if not boundary_row:
                        quote_time = russian_quote_time(session_day)
                    prices = [_decimal(row.get(name)) for name in ("open", "high", "low", "close")]
                    volume = _volume(row, symbol)
                    if volume < 0 or volume != volume.to_integral_value():
                        raise ValueError("invalid share volume")
                    if boundary_row:
                        ignored_in_chunk = 1
                        continue
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
            if len(rows) - ignored_in_chunk > CHUNK_DAYS:
                raise PermanentIOError("FINAM_TOO_MANY_BARS", "Daily response exceeds requested chunk bounds")
            ignored_boundary_rows += ignored_in_chunk
            cursor = chunk_end
        if cursor < end:
            raise PermanentIOError("FINAM_CHUNK_LIMIT", "Finam request exceeded the bounded chunk limit")
        bars.sort(key=lambda bar: bar.quote_time)
        status = FetchStatus.OK if bars or failures else FetchStatus.EMPTY
        return FetchResult(
            status=status, symbol=symbol, bars=bars, parse_failures=failures,
            ignored_boundary_rows=ignored_boundary_rows,
        )

    async def fetch_minutes(self, symbol: str, start: datetime, end: datetime) -> list[QuoteBar]:
        """Read a strict research window. Any rejected row invalidates it."""
        history = await self.fetch_minute_history(symbol, start, end)
        if history.rejected:
            raise PermanentIOError(
                "FINAM_INVALID_M1_BAR", "Finam returned an invalid, duplicate or out-of-window minute"
            )
        return history.bars

    async def fetch_minute_history(self, symbol: str, start: datetime, end: datetime) -> MinuteHistory:
        """Return validated bars and safe rejection counts for the dashboard."""
        if not self._api_token or not self._api_token.strip():
            raise PermanentIOError("FINAM_NOT_CONFIGURED", "FINAM_API_TOKEN is required")
        try:
            symbol = require_ru_symbol(symbol, include_context=True)
        except ValueError:
            raise PermanentIOError("UNSUPPORTED_INSTRUMENT", _UNSUPPORTED) from None
        if any(stamp.utcoffset() is None or stamp.second or stamp.microsecond for stamp in (start, end)):
            raise PermanentIOError("INVALID_WINDOW", "M1 requires timezone-aware minute boundaries")
        start, end = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
        if not start < end <= datetime.now(timezone.utc) or end - start > timedelta(days=1):
            raise PermanentIOError("INVALID_WINDOW", "M1 requires a closed window of at most one day")

        if self._client is not None:
            return await self._fetch_minutes(self._client, symbol, start, end)
        async with httpx.AsyncClient(follow_redirects=False) as client:
            return await self._fetch_minutes(client, symbol, start, end)

    async def _fetch_minutes(self, client, symbol: str, start: datetime, end: datetime) -> MinuteHistory:
        jwt = await self._open_session(client)
        rows = await self._read_bars(client, symbol, jwt, "TIME_FRAME_M1", start.isoformat(), end.isoformat())
        received: dict[datetime, QuoteBar] = {}
        conflicted: set[datetime] = set()
        rejected: Counter[str] = Counter()
        for row in rows:
            try:
                stamp = _timestamp(row["timestamp"]).astimezone(timezone.utc)
            except (ValueError, TypeError, KeyError, OverflowError):
                rejected["timestamp"] += 1
                continue
            if stamp.second or stamp.microsecond:
                rejected["timestamp"] += 1
                continue
            if not start <= stamp < end:
                rejected["outside_window"] += 1
                continue
            try:
                open_, high, low, close = map(_decimal, (row[key] for key in ("open", "high", "low", "close")))
                volume = _volume(row, symbol)
                if not (
                    0 < low <= min(open_, close) <= max(open_, close) <= high
                    and volume >= 0 and volume == volume.to_integral_value()
                ):
                    raise ValueError("invalid M1 price or volume")
                bar = QuoteBar(
                    provider=self.provider_name, symbol=symbol, exchange=RU_EXCHANGE,
                    interval="1m", quote_time=stamp, open=open_, high=high, low=low,
                    close=close, volume=int(volume), adjustment_mode=AdjustmentMode.RAW,
                    session_timezone=RU_SESSION_TIMEZONE, currency=RU_CURRENCY,
                )
            except (ValueError, TypeError, KeyError, InvalidOperation, OverflowError):
                rejected["ohlcv"] += 1
                continue
            if stamp in conflicted:
                rejected["conflict"] += 1
            elif stamp not in received:
                received[stamp] = bar
            elif received[stamp].ohlcv_tuple() == bar.ohlcv_tuple():
                rejected["duplicate"] += 1
            else:
                del received[stamp]
                conflicted.add(stamp)
                rejected["conflict"] += 2
        return MinuteHistory([received[stamp] for stamp in sorted(received)], dict(rejected))

    async def open_market_session(self) -> str:
        """Create a session for read-only live market subscriptions."""
        if not self._api_token or not self._api_token.strip():
            raise PermanentIOError("FINAM_NOT_CONFIGURED", "FINAM_API_TOKEN is required")
        if self._client is not None:
            return await self._open_session(self._client)
        async with httpx.AsyncClient(follow_redirects=False) as client:
            return await self._open_session(client)

    async def _open_session(self, client) -> str:
        # A fresh session per fetch avoids assuming a hard-coded JWT lifetime.
        auth = await self._request(client, "POST", "/v1/sessions", json={"secret": self._api_token})
        jwt = auth.get("token") if isinstance(auth, dict) else None
        if not isinstance(jwt, str) or not jwt or not jwt.isascii() or any(c.isspace() for c in jwt):
            raise PermanentIOError("FINAM_INVALID_AUTH_RESPONSE", "Finam returned no usable session token")
        return jwt

    async def fetch_asset(self, symbol: str) -> dict:
        """Reference data for one registry instrument (GET /v1/assets/{symbol})."""
        instrument = finam_symbol(symbol)
        payload = await self._authorized_get(f"/v1/assets/{instrument}")
        if not isinstance(payload, dict):
            raise PermanentIOError("FINAM_INVALID_RESPONSE", "Finam asset response has an invalid schema")
        return payload

    async def fetch_news_sources(self) -> list[dict]:
        payload = await self._authorized_get("/v1/news/sources")
        sources = payload.get("sources") if isinstance(payload, dict) else None
        if not isinstance(sources, list):
            raise PermanentIOError("FINAM_INVALID_RESPONSE", "Finam news sources have an invalid schema")
        return [row for row in sources if isinstance(row, dict) and isinstance(row.get("id"), str)]

    async def fetch_news_history(
        self,
        symbols: list[str],
        start: datetime,
        end: datetime,
        *,
        page_size: int = NEWS_PAGE_SIZE,
        max_pages: int = NEWS_MAX_PAGES,
    ) -> NewsHistoryPage:
        """Follow next_cursor until the feed ends; duplicates are left to the caller."""
        if any(stamp.utcoffset() is None for stamp in (start, end)) or not start < end:
            raise PermanentIOError("INVALID_WINDOW", "News history requires an aware window with start < end")
        if not 1 <= page_size <= 100:
            raise PermanentIOError("INVALID_WINDOW", "News page_size must be in 1..100")
        instruments = [finam_symbol(symbol) for symbol in symbols]
        params: dict[str, Any] = {
            "filter.symbols": instruments,
            "time_range.from": _rfc3339(start),
            "time_range.to": _rfc3339(end),
            "page_size": page_size,
        }

        async def read(client, jwt):
            items: list[dict] = []
            cursor = ""
            seen_cursors: set[str] = set()
            for page in range(1, max_pages + 1):
                payload = await self._request(
                    client, "GET", "/v1/news/history",
                    headers={"Authorization": f"Bearer {jwt}"}, params={**params, "cursor": cursor},
                )
                rows = payload.get("items") if isinstance(payload, dict) else None
                if not isinstance(rows, list):
                    raise PermanentIOError("FINAM_INVALID_RESPONSE", "Finam news history has an invalid schema")
                items.extend(row for row in rows if isinstance(row, dict))
                cursor = payload.get("next_cursor") or ""
                if not cursor:
                    return NewsHistoryPage(items, page, False)
                if cursor in seen_cursors:
                    raise PermanentIOError("FINAM_INVALID_RESPONSE", "Finam repeated a news cursor")
                seen_cursors.add(cursor)
            return NewsHistoryPage(items, max_pages, True)

        return await self._with_session(read)

    async def _authorized_get(self, path: str, **params):
        async def read(client, jwt):
            return await self._request(
                client, "GET", path, headers={"Authorization": f"Bearer {jwt}"}, params=params or None,
            )
        return await self._with_session(read)

    async def _with_session(self, operation):
        if not self._api_token or not self._api_token.strip():
            raise PermanentIOError("FINAM_NOT_CONFIGURED", "FINAM_API_TOKEN is required")
        if self._client is not None:
            return await operation(self._client, await self._open_session(self._client))
        async with httpx.AsyncClient(follow_redirects=False) as client:
            return await operation(client, await self._open_session(client))

    async def _read_bars(self, client, symbol: str, jwt: str, timeframe: str, start: str, end: str) -> list:
        instrument = finam_symbol(symbol)
        payload = await self._request(
            client, "GET", f"/v1/instruments/{instrument}/bars",
            headers={"Authorization": f"Bearer {jwt}"},
            params={"timeframe": timeframe, "interval.start_time": start, "interval.end_time": end},
        )
        if not isinstance(payload, dict) or not isinstance(payload.get("bars"), list):
            raise PermanentIOError("FINAM_INVALID_RESPONSE", "Finam bars response has an invalid schema")
        if payload.get("symbol") != instrument:
            raise PermanentIOError("FINAM_SYMBOL_MISMATCH", "Finam returned a different instrument")
        return payload["bars"]

    async def _request(self, client, method, path, **kwargs):
        await self._budget.acquire()
        try:
            response = await client.request(
                method, FINAM_BASE_URL + path, timeout=self._timeout,
                follow_redirects=False, **kwargs,
            )
        except httpx.RequestError:
            # Never include the exception/request/body: an auth request contains a secret.
            raise TransientIOError("FINAM_NETWORK_ERROR", "Finam request failed or timed out") from None
        status = response.status_code
        if status == 429:
            wait = retry_after_seconds(response.headers.get("Retry-After"))
            # Without the header, pause the shared budget for one second.
            self._budget.defer(wait if wait is not None else 1.0)
            raise TransientIOError("FINAM_RATE_LIMITED", "Finam rate limit reached (HTTP 429)", retry_after=wait)
        if status >= 500:
            raise TransientIOError(
                "FINAM_SOURCE_TRANSIENT", f"Finam temporary HTTP {status}",
                retry_after=retry_after_seconds(response.headers.get("Retry-After")),
            )
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


def _rfc3339(stamp: datetime) -> str:
    return stamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _volume(row: dict, symbol: str) -> Decimal:
    # Index values have no traded share volume.
    if ru_instrument(symbol).get("kind") == "index" and row.get("volume") is None:
        return Decimal(0)
    return _decimal(row.get("volume"))


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
