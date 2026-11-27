"""Russian research universe and provider-specific session-date conventions.

A Russian daily quote is labelled by its session date at 23:59:59 Moscow time.
This is a stable candle label, not a claim about the actual exchange close.
No US calendar or fixed 16:00 New York close is applied to Russian quotes.
"""

import json
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

RU_SESSION_TIMEZONE = "Europe/Moscow"
RU_EXCHANGE = "MISX"
RU_CURRENCY = "RUB"
RU_BOARD = "TQBR"
RU_QUOTE_TIME_RULE = "session-date label at 23:59:59 Europe/Moscow; not actual exchange close"
SUPPORTED_PROVIDERS = ("offline", "yfinance", "moex", "finam")
REGISTRY_FILE = Path(__file__).resolve().parents[1] / "config/portfolios/russian-research.json"


def _load_registry() -> tuple[dict[str, dict], dict[str, dict]]:
    registry = json.loads(REGISTRY_FILE.read_text(encoding="utf-8"))
    watchlist = {item["symbol"]: {**item, "kind": "equity"} for item in registry["instruments"]}
    context = {item["symbol"]: item for item in registry.get("market_context", [])}
    if set(watchlist) & set(context) or len(watchlist) != len(registry["instruments"]):
        raise ValueError("instrument symbols in the registry must be unique")
    return watchlist, context


RU_INSTRUMENTS, RU_CONTEXT = _load_registry()
# Ordered as in the registry; the dashboard and reports keep this order.
RU_WATCHLIST = tuple(RU_INSTRUMENTS)
RU_SYMBOLS = frozenset(RU_WATCHLIST)
RU_CONTEXT_SYMBOLS = tuple(RU_CONTEXT)


def require_ru_symbol(symbol: str, *, include_context: bool = False) -> str:
    normalized = symbol.strip().upper()
    if normalized not in RU_SYMBOLS and not (include_context and normalized in RU_CONTEXT):
        raise ValueError("Russian research universe is " + ", ".join(RU_WATCHLIST))
    return normalized


def ru_instrument(symbol: str) -> dict:
    normalized = require_ru_symbol(symbol, include_context=True)
    return RU_INSTRUMENTS.get(normalized) or RU_CONTEXT[normalized]


def finam_symbol(symbol: str) -> str:
    return ru_instrument(symbol)["finam_symbol"]


def russian_quote_time(session_day: date) -> datetime:
    return datetime.combine(session_day, time(23, 59, 59), tzinfo=ZoneInfo(RU_SESSION_TIMEZONE))


def provider_timezone(provider: str) -> str:
    if provider not in SUPPORTED_PROVIDERS:
        raise ValueError("Unsupported market data provider")
    return RU_SESSION_TIMEZONE if provider in {"moex", "finam"} else "America/New_York"


def provider_quote_time_rule(provider: str) -> str:
    if provider_timezone(provider) == RU_SESSION_TIMEZONE:
        return RU_QUOTE_TIME_RULE
    return "America/New_York 16:00 regular, 13:00 early close"
