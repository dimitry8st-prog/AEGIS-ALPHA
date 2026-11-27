"""Translate Finam's read-only WebSocket feed to AEGIS market events."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from websockets import connect
from websockets.exceptions import WebSocketException

from worker.io_retry import PermanentIOError, TransientIOError
from worker.russian_market import RU_CONTEXT_SYMBOLS, RU_WATCHLIST, finam_symbol

FINAM_WS_URL = "wss://api.finam.ru/ws"
INSTRUMENTS = {finam_symbol(symbol): symbol for symbol in (*RU_WATCHLIST, *RU_CONTEXT_SYMBOLS)}


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


def _quote_event(row: dict) -> dict | None:
    symbol = INSTRUMENTS.get(row.get("symbol"))
    # Some updates carry only bid and ask.
    if not symbol or not row.get("last"):
        return None
    last = _number(row["last"])
    if last <= 0:
        return None
    return {
        "type": "quote", "symbol": symbol, "last": str(last),
        "quote_time": _stamp(row["timestamp"]).isoformat(),
    }


def _bar_event(row: dict, symbol: str) -> dict:
    stamp = _stamp(row["timestamp"])
    opening, high, low, close, volume = (
        _number(row[field]) for field in ("open", "high", "low", "close", "volume")
    )
    if stamp.second or stamp.microsecond or not (
        0 < low <= min(opening, close) <= max(opening, close) <= high
        and volume >= 0 and volume == volume.to_integral_value()
    ):
        raise ValueError("invalid minute bar")
    return {
        "type": "bar", "symbol": symbol, "quote_time": stamp.isoformat(),
        "open": str(opening), "high": str(high), "low": str(low), "close": str(close),
        "volume": int(volume),
    }


def _market_events(message: dict) -> list[dict]:
    if message.get("type") != "DATA":
        return []
    payload = message["payload"]
    kind = message.get("subscription_type")
    symbol = INSTRUMENTS.get(payload.get("symbol"))
    if (payload.get("error") or {}).get("code"):
        if symbol:
            # One rejected instrument must not stop the other subscriptions.
            return [{"type": "symbol_error", "symbol": symbol, "feed": kind, "code": "FINAM_STREAM_REJECTED"}]
        raise TransientIOError("FINAM_STREAM_REJECTED", "Finam rejected a market data subscription")
    if kind == "QUOTES":
        rows = payload.get("quote") or []
    elif kind == "BARS" and symbol:
        rows = payload.get("bars") or []
    else:
        return []
    if not isinstance(rows, list):
        return [{"type": "data_warning", "feed": kind, "rejected": 1}]

    events = []
    rejected = 0
    for row in rows:
        try:
            event = _quote_event(row) if kind == "QUOTES" else _bar_event(row, symbol)
        except (ValueError, TypeError, KeyError, AttributeError, InvalidOperation, OverflowError):
            rejected += 1
            continue
        if event is not None:
            events.append(event)
    if rejected:
        events.append({"type": "data_warning", "feed": kind, "rejected": rejected})
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
    """One upstream connection carries quotes and M1 bars for every registry instrument."""
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
