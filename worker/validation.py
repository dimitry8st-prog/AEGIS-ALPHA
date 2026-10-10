from __future__ import annotations

import math
from decimal import Decimal
from typing import Any

from worker.types import QuarantineRow, QuoteBar


REQUIRED_FIELDS = (
    "provider",
    "symbol",
    "exchange",
    "interval",
    "quote_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "adjustment_mode",
)


def _finite(value: Decimal) -> bool:
    try:
        f = float(value)
    except Exception:
        return False
    return math.isfinite(f)


def validate_bar(bar: QuoteBar) -> QuarantineRow | None:
    payload: dict[str, Any] = {
        "provider": bar.provider,
        "symbol": bar.symbol,
        "exchange": bar.exchange,
        "interval": bar.interval,
        "quote_time": bar.quote_time.isoformat() if bar.quote_time else None,
        "open": str(bar.open),
        "high": str(bar.high),
        "low": str(bar.low),
        "close": str(bar.close),
        "volume": bar.volume,
        "adjustment_mode": bar.adjustment_mode.value,
    }

    for name in REQUIRED_FIELDS:
        if getattr(bar, name, None) is None:
            return QuarantineRow(
                provider=bar.provider,
                symbol=bar.symbol,
                exchange=bar.exchange,
                interval=bar.interval,
                quote_time=bar.quote_time,
                adjustment_mode=bar.adjustment_mode.value,
                reason_code="MISSING_FIELD",
                reason_detail=f"missing {name}",
                payload=payload,
            )

    for name in ("open", "high", "low", "close"):
        value: Decimal = getattr(bar, name)
        if not _finite(value) or value <= 0:
            return QuarantineRow(
                provider=bar.provider,
                symbol=bar.symbol,
                exchange=bar.exchange,
                interval=bar.interval,
                quote_time=bar.quote_time,
                adjustment_mode=bar.adjustment_mode.value,
                reason_code="INVALID_PRICE",
                reason_detail=f"{name}={value}",
                payload=payload,
            )

    if bar.volume is None or bar.volume < 0:
        return QuarantineRow(
            provider=bar.provider,
            symbol=bar.symbol,
            exchange=bar.exchange,
            interval=bar.interval,
            quote_time=bar.quote_time,
            adjustment_mode=bar.adjustment_mode.value,
            reason_code="INVALID_VOLUME",
            reason_detail=f"volume={bar.volume}",
            payload=payload,
        )

    if not (bar.low <= bar.open <= bar.high and bar.low <= bar.close <= bar.high):
        return QuarantineRow(
            provider=bar.provider,
            symbol=bar.symbol,
            exchange=bar.exchange,
            interval=bar.interval,
            quote_time=bar.quote_time,
            adjustment_mode=bar.adjustment_mode.value,
            reason_code="OHLC_INCONSISTENT",
            reason_detail=(
                f"O={bar.open} H={bar.high} L={bar.low} C={bar.close}"
            ),
            payload=payload,
        )

    if bar.high < bar.low:
        return QuarantineRow(
            provider=bar.provider,
            symbol=bar.symbol,
            exchange=bar.exchange,
            interval=bar.interval,
            quote_time=bar.quote_time,
            adjustment_mode=bar.adjustment_mode.value,
            reason_code="OHLC_INCONSISTENT",
            reason_detail="high < low",
            payload=payload,
        )

    return None
