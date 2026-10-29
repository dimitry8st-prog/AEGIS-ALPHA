"""Russian research universe and provider-specific session-date conventions.

A Russian daily quote is labelled by its session date at 23:59:59 Moscow time.
This is a stable candle label, not a claim about the actual exchange close.
No US calendar or fixed 16:00 New York close is applied to Russian quotes.
"""

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

RU_SESSION_TIMEZONE = "Europe/Moscow"
RU_EXCHANGE = "MISX"
RU_CURRENCY = "RUB"
RU_BOARD = "TQBR"
RU_SYMBOLS = frozenset({"GAZP", "GMKN"})
RU_QUOTE_TIME_RULE = "session-date label at 23:59:59 Europe/Moscow; not actual exchange close"
SUPPORTED_PROVIDERS = ("offline", "yfinance", "moex", "finam")


def require_ru_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if normalized not in RU_SYMBOLS:
        raise ValueError("Russian research universe is GAZP, GMKN")
    return normalized


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
