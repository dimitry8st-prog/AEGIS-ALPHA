"""Optional Redis cache wrapper — same FetchResult on hit and miss."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal

from worker.adapters.base import MarketDataAdapter
from worker.types import AdjustmentMode, FetchResult, FetchStatus, QuoteBar


class CachedAdapter(MarketDataAdapter):
    def __init__(self, inner: MarketDataAdapter, redis_client, ttl_seconds: int = 3600):
        self.inner = inner
        self.redis = redis_client
        self.ttl = ttl_seconds
        self.provider_name = inner.provider_name

    def _key(self, symbol: str, start: date, end: date, mode: AdjustmentMode) -> str:
        return f"marketbars:{self.provider_name}:{symbol}:{start}:{end}:{mode.value}"

    async def fetch_daily(
        self,
        symbol: str,
        start: date,
        end: date,
        adjustment_mode: AdjustmentMode,
    ) -> FetchResult:
        key = self._key(symbol.upper(), start, end, adjustment_mode)
        cached = await self.redis.get(key)
        if cached:
            return _deserialize(cached)

        result = await self.inner.fetch_daily(symbol, start, end, adjustment_mode)
        # Cache successful OK/EMPTY outcomes only (not UNAVAILABLE)
        if result.status in (FetchStatus.OK, FetchStatus.EMPTY):
            await self.redis.setex(key, self.ttl, _serialize(result))
        return result


def _serialize(result: FetchResult) -> str:
    payload = {
        "status": result.status.value,
        "symbol": result.symbol,
        "error_code": result.error_code,
        "error_message": result.error_message,
        "bars": [
            {
                "provider": b.provider,
                "symbol": b.symbol,
                "exchange": b.exchange,
                "interval": b.interval,
                "quote_time": b.quote_time.isoformat(),
                "open": str(b.open),
                "high": str(b.high),
                "low": str(b.low),
                "close": str(b.close),
                "volume": b.volume,
                "adjustment_mode": b.adjustment_mode.value,
                "session_timezone": b.session_timezone,
                "currency": b.currency,
            }
            for b in result.bars
        ],
    }
    return json.dumps(payload)


def _deserialize(raw: str | bytes) -> FetchResult:
    data = json.loads(raw)
    bars = [
        QuoteBar(
            provider=item["provider"],
            symbol=item["symbol"],
            exchange=item["exchange"],
            interval=item["interval"],
            quote_time=datetime.fromisoformat(item["quote_time"]),
            open=Decimal(item["open"]),
            high=Decimal(item["high"]),
            low=Decimal(item["low"]),
            close=Decimal(item["close"]),
            volume=int(item["volume"]),
            adjustment_mode=AdjustmentMode(item["adjustment_mode"]),
            session_timezone=item.get("session_timezone", "America/New_York"),
            currency=item.get("currency", "USD"),
        )
        for item in data.get("bars", [])
    ]
    return FetchResult(
        status=FetchStatus(data["status"]),
        bars=bars,
        error_code=data.get("error_code"),
        error_message=data.get("error_message"),
        symbol=data.get("symbol"),
    )
