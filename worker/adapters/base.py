from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

from worker.types import AdjustmentMode, FetchResult


class MarketDataAdapter(ABC):
    provider_name: str

    @abstractmethod
    async def fetch_daily(
        self,
        symbol: str,
        start: date,
        end: date,
        adjustment_mode: AdjustmentMode,
    ) -> FetchResult:
        """Fetch daily OHLCV bars for [start, end) in session calendar dates."""
