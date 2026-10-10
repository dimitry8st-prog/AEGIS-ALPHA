from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import text

from worker.adapters.offline import OfflineAdapter
from worker.config import WorkerSettings
from worker.pipeline import MarketDataPipeline
from worker.repository import QuoteRepository
from worker.types import AdjustmentMode, RunStatus


@pytest.fixture
def settings(database_url):
    return WorkerSettings(
        DATABASE_URL=database_url,
        IO_RETRY_WAIT_SECONDS=0.01,
        IO_MAX_ATTEMPTS=3,
        RUN_DEADLINE_SECONDS=60,
    )


async def _count(engine, table: str) -> int:
    async with engine.connect() as conn:
        return int((await conn.execute(text(f"SELECT COUNT(*) FROM {table}"))).scalar())


async def _cleanup(engine):
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM market_quote_revisions"))
        await conn.execute(text("DELETE FROM market_quote_quarantine"))
        await conn.execute(text("DELETE FROM market_quotes"))
        await conn.execute(text("DELETE FROM market_collection_runs"))


@pytest.mark.asyncio
async def test_normal_load(migrated_engine, settings):
    await _cleanup(migrated_engine)
    repo = QuoteRepository(migrated_engine)
    pipeline = MarketDataPipeline(OfflineAdapter(), repo, settings)
    report = await pipeline.run(
        symbols=["AAPL", "MSFT", "NVDA"],
        start=date(2024, 1, 2),
        end=date(2024, 1, 6),
        adjustment_mode=AdjustmentMode.AUTO_ADJUSTED,
    )
    assert report.status is RunStatus.READY
    assert report.counters.inserted == 12  # 3 symbols * 4 days
    assert report.counters.duplicates == 0
    assert await _count(migrated_engine, "market_quotes") == 12


@pytest.mark.asyncio
async def test_repeat_same_period_idempotent(migrated_engine, settings):
    await _cleanup(migrated_engine)
    repo = QuoteRepository(migrated_engine)
    pipeline = MarketDataPipeline(OfflineAdapter(), repo, settings)
    first = await pipeline.run(
        symbols=["AAPL"],
        start=date(2024, 1, 2),
        end=date(2024, 1, 6),
    )
    second = await pipeline.run(
        symbols=["AAPL"],
        start=date(2024, 1, 2),
        end=date(2024, 1, 6),
    )
    assert first.status is RunStatus.READY
    assert second.status is RunStatus.READY
    assert second.counters.duplicates == 4
    assert second.counters.inserted == 0
    assert await _count(migrated_engine, "market_quotes") == 4


@pytest.mark.asyncio
async def test_overlapping_period(migrated_engine, settings):
    await _cleanup(migrated_engine)
    repo = QuoteRepository(migrated_engine)
    pipeline = MarketDataPipeline(OfflineAdapter(), repo, settings)
    await pipeline.run(symbols=["AAPL"], start=date(2024, 1, 2), end=date(2024, 1, 5))
    report = await pipeline.run(
        symbols=["AAPL"], start=date(2024, 1, 3), end=date(2024, 1, 6)
    )
    assert report.counters.duplicates == 2  # Jan 3,4
    assert report.counters.inserted == 1  # Jan 5
    assert await _count(migrated_engine, "market_quotes") == 4


@pytest.mark.asyncio
async def test_revised_quote_creates_revision(migrated_engine, settings):
    await _cleanup(migrated_engine)
    repo = QuoteRepository(migrated_engine)
    first = MarketDataPipeline(OfflineAdapter(), repo, settings)
    await first.run(symbols=["AAPL"], start=date(2024, 1, 2), end=date(2024, 1, 3))
    # Keep revised close inside original high/low so validation still passes.
    revised = MarketDataPipeline(
        OfflineAdapter(revised_close={("AAPL", date(2024, 1, 2)): Decimal("186.25")}),
        repo,
        settings,
    )
    report = await revised.run(
        symbols=["AAPL"], start=date(2024, 1, 2), end=date(2024, 1, 3)
    )
    assert report.counters.updated == 1
    assert await _count(migrated_engine, "market_quote_revisions") == 1
    async with migrated_engine.connect() as conn:
        close = (
            await conn.execute(
                text("SELECT close FROM market_quotes WHERE symbol='AAPL'")
            )
        ).scalar()
    assert Decimal(str(close)) == Decimal("186.25")


@pytest.mark.asyncio
async def test_invalid_bar_quarantined(migrated_engine, settings):
    await _cleanup(migrated_engine)
    repo = QuoteRepository(migrated_engine)
    pipeline = MarketDataPipeline(
        OfflineAdapter(corrupt_bars={"AAPL": [date(2024, 1, 2)]}),
        repo,
        settings,
    )
    report = await pipeline.run(
        symbols=["AAPL"], start=date(2024, 1, 2), end=date(2024, 1, 4)
    )
    assert report.status is RunStatus.PARTIAL
    assert report.counters.quarantined >= 1
    assert await _count(migrated_engine, "market_quote_quarantine") >= 1


@pytest.mark.asyncio
async def test_empty_response(migrated_engine, settings):
    await _cleanup(migrated_engine)
    repo = QuoteRepository(migrated_engine)
    pipeline = MarketDataPipeline(
        OfflineAdapter(empty_symbols={"AAPL"}),
        repo,
        settings,
    )
    report = await pipeline.run(
        symbols=["AAPL"], start=date(2024, 1, 2), end=date(2024, 1, 6)
    )
    assert report.status is RunStatus.PARTIAL
    assert report.counters.received == 0
    assert any("0 bars" in n for n in report.notes)


@pytest.mark.asyncio
async def test_source_failure(migrated_engine, settings):
    await _cleanup(migrated_engine)
    repo = QuoteRepository(migrated_engine)
    pipeline = MarketDataPipeline(
        OfflineAdapter(fail_symbols={"AAPL", "MSFT", "NVDA"}),
        repo,
        settings,
    )
    report = await pipeline.run(
        symbols=["AAPL", "MSFT", "NVDA"],
        start=date(2024, 1, 2),
        end=date(2024, 1, 6),
    )
    assert report.status is RunStatus.FAILED
    assert len(report.errors) >= 3


@pytest.mark.asyncio
async def test_db_failure_on_run_create(settings):
    class BoomRepo(QuoteRepository):
        async def create_run(self, **kwargs):
            raise ConnectionError("connection refused")

    from worker.repository import create_engine

    eng = create_engine(settings)
    pipeline = MarketDataPipeline(OfflineAdapter(), BoomRepo(eng), settings)
    report = await pipeline.run(
        symbols=["AAPL"], start=date(2024, 1, 2), end=date(2024, 1, 3)
    )
    await eng.dispose()
    assert report.status is RunStatus.FAILED
    assert report.errors[0]["code"] == "DB_UNAVAILABLE"
