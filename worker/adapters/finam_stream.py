"""Translate Finam's read-only WebSocket feed to AEGIS market events."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from websockets import connect
from websockets.exceptions import WebSocketException

from worker.io_retry import PermanentIOError, TransientIOError
from worker.russian_market import RU_EXCHANGE, RU_SYMBOLS

FINAM_WS_URL = "wss://api.finam.ru/ws"
INSTRUMENTS = {f"{symbol}@{RU_EXCHANGE}": symbol for symbol in sorted(RU_SYMBOLS)}


def _stamp(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("invalid timestamp")
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None:
        raise ValueError("timestamp has no timezone")
    return stamp.astimezone(timezone.utc)


def _number(value: str) -> Decimal:
    if not isinstance(value, str):
        raise ValueError("invalid decimal")
    result = Decimal(value)
    if not result.is_finite():
        raise ValueError("non-finite decimal")
    return result


def _market_events(message: dict) -> list[dict]:
    if message.get("type") != "DATA":
        return []
    payload = message["payload"]
    if (payload.get("error") or {}).get("code"):
        raise TransientIOError("FINAM_STREAM_REJECTED", "Finam rejected a market data subscription")
    events = []
    if message.get("subscription_type") == "QUOTES":
        for quote in payload.get("quote") or []:
            symbol = INSTRUMENTS.get(quote.get("symbol"))
            # Some quote updates carry only bid/ask; retain the last known trade price.
            if not symbol or not quote.get("last"):
                continue
            last = _number(quote["last"])
            if last > 0:
                events.append({
                    "type": "quote", "symbol": symbol, "last": str(last),
                    "quote_time": _stamp(quote["timestamp"]).isoformat(),
                })
    if message.get("subscription_type") == "BARS":
        symbol = INSTRUMENTS.get(payload.get("symbol"))
        if symbol:
            for row in payload.get("bars") or []:
                stamp = _stamp(row["timestamp"])
                opening, high, low, close, volume = (
                    _number(row[field]) for field in ("open", "high", "low", "close", "volume")
                )
                if stamp.second or stamp.microsecond or not (
                    0 < low <= min(opening, close) <= max(opening, close) <= high
                    and volume >= 0 and volume == volume.to_integral_value()
                ):
                    raise ValueError("invalid minute bar")
                events.append({
                    "type": "bar", "symbol": symbol, "quote_time": stamp.isoformat(),
                    "open": str(opening), "high": str(high), "low": str(low), "close": str(close),
                    "volume": int(volume),
                })
    return events


def _decode(raw: str | bytes) -> dict:
    message = json.loads(raw)
    if not isinstance(message, dict):
        raise ValueError("invalid envelope")
    error = message.get("error_info") or {}
    if message.get("type") == "ERROR":
        if error.get("type") in {"UNAUTHENTICATED", "PERMISSION_DENIED"}:
            raise PermanentIOError("FINAM_AUTHORIZATION_FAILED", "Finam rejected live data permissions")
        raise TransientIOError("FINAM_STREAM_REJECTED", "Finam rejected the live data stream")
    return message


async def stream_finam_market(jwt: str):
    """One upstream connection carries quotes and M1 bars for both instruments."""
    try:
        async with connect(FINAM_WS_URL, open_timeout=8, close_timeout=2, max_size=1_000_000) as socket:
            handshake = _decode(await asyncio.wait_for(socket.recv(), timeout=8))
            if (handshake.get("event_info") or {}).get("event") != "HANDSHAKE_SUCCESS":
                raise ValueError("Finam did not confirm the handshake")
            subscriptions = [
                ("QUOTES", {"symbols": list(INSTRUMENTS)}),
                *(("BARS", {"symbol": symbol, "timeframe": "TIME_FRAME_M1"}) for symbol in INSTRUMENTS),
            ]
            for kind, params in subscriptions:
                await socket.send(json.dumps({"action": "SUBSCRIBE", "type": kind, "data": params, "token": jwt}))
            yield {"type": "connected"}
            while True:
                try:
                    raw = await asyncio.wait_for(socket.recv(), timeout=12)
                except TimeoutError:
                    yield {"type": "heartbeat"}
                    continue
                message = _decode(raw)
                if (message.get("event_info") or {}).get("event") == "CONNECTION_CLOSED":
                    raise TransientIOError("FINAM_STREAM_UNAVAILABLE", "Finam closed the live data stream")
                for event in _market_events(message):
                    yield event
    except (WebSocketException, OSError, TimeoutError):
        raise TransientIOError("FINAM_STREAM_UNAVAILABLE", "Finam live data connection unavailable") from None
    except (ValueError, TypeError, KeyError, AttributeError, InvalidOperation):
        raise TransientIOError("FINAM_STREAM_INVALID_DATA", "Unexpected Finam live market data") from None
