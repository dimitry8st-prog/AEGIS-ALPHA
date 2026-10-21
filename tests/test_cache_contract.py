from __future__ import annotations

from datetime import date

import pytest

from worker.adapters.cached import CachedAdapter
from worker.adapters.offline import OfflineAdapter
from worker.types import AdjustmentMode


class FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}

    async def get(self, key: str):
        return self.store.get(key)

    async def setex(self, key: str, ttl: int, value: str):
        self.store[key] = value


@pytest.mark.asyncio
async def test_cache_hit_matches_miss_contract():
    inner = OfflineAdapter()
    redis = FakeRedis()
    adapter = CachedAdapter(inner, redis, ttl_seconds=60)
    miss = await adapter.fetch_daily(
        "AAPL", date(2024, 1, 2), date(2024, 1, 6), AdjustmentMode.AUTO_ADJUSTED
    )
    hit = await adapter.fetch_daily(
        "AAPL", date(2024, 1, 2), date(2024, 1, 6), AdjustmentMode.AUTO_ADJUSTED
    )
    assert miss.status == hit.status
    assert len(miss.bars) == len(hit.bars)
    assert miss.bars[0].ohlcv_tuple() == hit.bars[0].ohlcv_tuple()
    assert miss.bars[0].quote_time == hit.bars[0].quote_time


class BrokenRedis(FakeRedis):
    async def get(self, key: str):
        raise ConnectionError("redis down")

    async def setex(self, key: str, ttl: int, value: str):
        raise ConnectionError("redis down")


@pytest.mark.asyncio
async def test_worker_cache_falls_through_on_redis_and_corrupt_payload():
    inner = OfflineAdapter()
    down = BrokenRedis()
    adapter = CachedAdapter(inner, down, ttl_seconds=60)
    missed = await adapter.fetch_daily(
        "AAPL", date(2024, 1, 2), date(2024, 1, 3), AdjustmentMode.RAW
    )
    assert len(missed.bars) == 1

    corrupt = FakeRedis()
    corrupt.store["marketbars:offline:AAPL:2024-01-02:2024-01-03:raw"] = "{not-json"
    adapter = CachedAdapter(inner, corrupt, ttl_seconds=60)
    again = await adapter.fetch_daily(
        "AAPL", date(2024, 1, 2), date(2024, 1, 3), AdjustmentMode.RAW
    )
    assert again.bars[0].close == missed.bars[0].close
