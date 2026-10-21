"""NASDAQ names this research stage is allowed to store.

Exchange is the ISO 10383 MIC XNAS. It is not read from a price row and is not
guessed for other listings. Add a symbol only after the listing is confirmed.
"""

from __future__ import annotations

SUPPORTED_XNAS_SYMBOLS = frozenset({"AAPL", "MSFT", "NVDA"})
EXCHANGE_MIC = "XNAS"
CURRENCY = "USD"


class UnsupportedInstrument(Exception):
    def __init__(self, symbol: str):
        self.symbol = symbol
        super().__init__(symbol)


def require_xnas(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if normalized not in SUPPORTED_XNAS_SYMBOLS:
        raise UnsupportedInstrument(normalized)
    return normalized
