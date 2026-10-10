"""Regression tests for parse quarantine, retries, deadlines, and run statuses."""

from __future__ import annotations

import asyncio
import time
from datetime import date
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest
from sqlalchemy import text

from worker import __main__ as worker_main
from worker.adapters.base import MarketDataAdapter
from worker.adapters.offline import OfflineAdapter
from worker.adapters.yfinance_adapter import YFinanceAdapter
from worker.config import WorkerSettings
from worker.io_retry import PermanentIOError, TransientIOError, with_retries
from worker.pipeline import MarketDataPipeline
from worker.repository import QuoteRepository
from worker.types import AdjustmentMode, FetchStatus, RunStatus


@pytest.fixture
def settings(database_url):
    return WorkerSettings(
        DATABASE_URL=database_url,
        IO_RETRY_WAIT_SECONDS=0.01,
        IO_MAX_ATTEMPTS=3,
        IO_OPERATION_TIMEOUT_SECONDS=5.0,
        RUN_DEADLINE_SECONDS=60,
    )


async def _cleanup(engine):
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM market_quote_revisions"))
        await conn.execute(text("DELETE FROM market_quote_quarantine"))
        await conn.execute(text("DELETE FROM market_quotes"))
        await conn.execute(text("DELETE FROM market_collection_runs"))


async def _count(engine, table: str) -> int:
    async with engine.connect() as conn:
        return int((await conn.execute(text(f"SELECT COUNT(*) FROM {table}"))).scalar())


def _mixed_frame() -> pd.DataFrame:
    """One valid OHLCV row and one NaN-close row that cannot become QuoteBar."""
    idx = pd.to_datetime(["2024-01-02", "2024-01-03"])
    return pd.DataFrame(
        {
            "Open": [185.0, 186.0],
            "High": [187.5, 188.0],
            "Low": [184.2, 185.0],
            "Close": [186.8, float("nan")],
            "Volume": [50_000_000, 48_000_000],
        },
        index=idx,
    )


@pytest.mark.asyncio
async def test_yfinance_mixed_parse_quarantine(migrated_engine, settings):
    await _cleanup(migrated_engine)
    repo = QuoteRepository(migrated_engine)
    adapter = YFinanceAdapter()
    with patch(
        "worker.adapters.yfinance_adapter._download_sync",
        return_value=_mixed_frame(),
    ):
        pipeline = MarketDataPipeline(adapter, repo, settings)
        report = await pipeline.run(
            symbols=["AAPL"],
            start=date(2024, 1, 2),
            end=date(2024, 1, 4),
            adjustment_mode=AdjustmentMode.AUTO_ADJUSTED,
        )
    assert report.counters.received == 2
    assert report.counters.inserted == 1
    assert report.counters.quarantined == 1
    assert report.status is RunStatus.PARTIAL
    assert await _count(migrated_engine, "market_quotes") == 1
    assert await _count(migrated_engine, "market_quote_quarantine") == 1
    async with migrated_engine.connect() as conn:
        payload = (
            await conn.execute(
                text("SELECT payload FROM market_quote_quarantine LIMIT 1")
            )
        ).scalar()
    assert payload["Close"] == "NaN"
    assert "Open" in payload


@pytest.mark.asyncio
async def test_connection_error_then_success_retries():
    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] < 2:
            raise TransientIOError("SOURCE_TRANSIENT", "connection reset by peer")
        return "ok"

    result = await with_retries(
        flaky,
        max_attempts=3,
        wait_seconds=0.01,
        deadline_monotonic=time.monotonic() + 5,
        operation_timeout=2,
    )
    assert result == "ok"
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_transient_exhaustion_limited_attempts(migrated_engine, settings):
    await _cleanup(migrated_engine)
    calls = {"n": 0}

    class FlakyAdapter(MarketDataAdapter):
        provider_name = "mock"

        async def fetch_daily(self, symbol, start, end, adjustment_mode):
            calls["n"] += 1
            raise TransientIOError("SOURCE_TRANSIENT", "timeout talking to source")

    settings = WorkerSettings(
        DATABASE_URL=settings.DATABASE_URL,
        IO_RETRY_WAIT_SECONDS=0.01,
        IO_MAX_ATTEMPTS=3,
        RUN_DEADLINE_SECONDS=30,
    )
    repo = QuoteRepository(migrated_engine)
    pipeline = MarketDataPipeline(FlakyAdapter(), repo, settings)
    report = await pipeline.run(
        symbols=["AAPL"], start=date(2024, 1, 2), end=date(2024, 1, 3)
    )
    assert calls["n"] == 3
    assert report.status is RunStatus.FAILED
    assert report.errors
    assert report.errors[0]["code"] == "SOURCE_TRANSIENT"


@pytest.mark.asyncio
async def test_permanent_error_no_retries(migrated_engine, settings):
    await _cleanup(migrated_engine)
    calls = {"n": 0}

    class PermanentAdapter(MarketDataAdapter):
        provider_name = "mock"

        async def fetch_daily(self, symbol, start, end, adjustment_mode):
            calls["n"] += 1
            raise PermanentIOError("SOURCE_ERROR", "invalid ticker syntax")

    repo = QuoteRepository(migrated_engine)
    pipeline = MarketDataPipeline(PermanentAdapter(), repo, settings)
    report = await pipeline.run(
        symbols=["AAPL"], start=date(2024, 1, 2), end=date(2024, 1, 3)
    )
    assert calls["n"] == 1
    assert report.status is RunStatus.FAILED
    assert report.errors[0]["code"] == "SOURCE_ERROR"


@pytest.mark.asyncio
async def test_yfinance_adapter_raises_transient_not_unavailable():
    adapter = YFinanceAdapter()
    with patch(
        "worker.adapters.yfinance_adapter._download_sync",
        side_effect=ConnectionError("connection refused"),
    ):
        with pytest.raises(TransientIOError):
            await adapter.fetch_daily(
                "AAPL",
                date(2024, 1, 2),
                date(2024, 1, 3),
                AdjustmentMode.AUTO_ADJUSTED,
            )


@pytest.mark.asyncio
async def test_yfinance_empty_frame_is_not_network_error():
    adapter = YFinanceAdapter()
    empty = pd.DataFrame(
        columns=["Open", "High", "Low", "Close", "Volume"]
    )
    with patch(
        "worker.adapters.yfinance_adapter._download_sync",
        return_value=empty,
    ):
        result = await adapter.fetch_daily(
            "AAPL",
            date(2024, 1, 2),
            date(2024, 1, 3),
            AdjustmentMode.AUTO_ADJUSTED,
        )
    assert result.status is FetchStatus.EMPTY
    assert result.bars == []
    assert result.parse_failures == []


@pytest.mark.asyncio
async def test_all_quote_writes_fail_failed_and_cli_nonzero(migrated_engine, settings):
    await _cleanup(migrated_engine)

    class FailWritesRepo(QuoteRepository):
        async def upsert_bar(self, bar, run_id):
            raise ConnectionError("connection refused")

        async def get_existing(self, bar):
            return None

        async def quarantine(self, row, run_id):
            raise ConnectionError("connection refused")

    repo = FailWritesRepo(migrated_engine)
    pipeline = MarketDataPipeline(OfflineAdapter(), repo, settings)
    report = await pipeline.run(
        symbols=["AAPL"], start=date(2024, 1, 2), end=date(2024, 1, 3)
    )
    assert report.status is RunStatus.FAILED
    assert report.counters.inserted == 0
    assert report.counters.failed >= 1

    mock_engine = AsyncMock()
    mock_engine.dispose = AsyncMock()

    class BoomPipeline:
        def __init__(self, *args, **kwargs):
            pass

        async def run(self, **kwargs):
            return report

    args = type(
        "A",
        (),
        {
            "symbols": "AAPL",
            "start": date(2024, 1, 2),
            "end": date(2024, 1, 3),
            "provider": "offline",
            "adjustment": "auto_adjusted",
            "interval": "1d",
        },
    )()
    with patch.object(worker_main, "WorkerSettings", return_value=settings), patch.object(
        worker_main, "create_engine", return_value=mock_engine
    ), patch.object(worker_main, "MarketDataPipeline", BoomPipeline):
        code = await worker_main._run_collect(args)
    assert code == 1


@pytest.mark.asyncio
async def test_partial_writes_saved(migrated_engine, settings):
    await _cleanup(migrated_engine)
    calls = {"n": 0}

    class PartialRepo(QuoteRepository):
        async def upsert_bar(self, bar, run_id):
            calls["n"] += 1
            if calls["n"] == 1:
                return await super().upsert_bar(bar, run_id)
            raise ConnectionError("connection refused")

        async def get_existing(self, bar):
            # After first insert, uncertain failures must not invent success.
            return await super().get_existing(bar)

    repo = PartialRepo(migrated_engine)
    pipeline = MarketDataPipeline(OfflineAdapter(), repo, settings)
    report = await pipeline.run(
        symbols=["AAPL"], start=date(2024, 1, 2), end=date(2024, 1, 6)
    )
    assert report.status is RunStatus.PARTIAL
    assert report.counters.inserted >= 1
    assert report.counters.failed >= 1
    assert await _count(migrated_engine, "market_quotes") >= 1


@pytest.mark.asyncio
async def test_deadline_caps_operation_timeout():
    started = time.monotonic()
    deadline = started + 0.15
    slept = {"v": 0.0}

    async def slow():
        t0 = time.monotonic()
        try:
            await asyncio.sleep(2.0)
        finally:
            slept["v"] = time.monotonic() - t0
        return "done"

    with pytest.raises((TimeoutError, asyncio.TimeoutError)):
        await with_retries(
            slow,
            max_attempts=1,
            wait_seconds=0.01,
            deadline_monotonic=deadline,
            operation_timeout=30.0,  # larger than remaining deadline
        )
    # Must not wait the full 30s operation_timeout
    assert slept["v"] < 1.0


@pytest.mark.asyncio
async def test_empty_window_partial(migrated_engine, settings):
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
async def test_all_quarantined_is_partial(migrated_engine, settings):
    await _cleanup(migrated_engine)
    repo = QuoteRepository(migrated_engine)
    pipeline = MarketDataPipeline(
        OfflineAdapter(
            corrupt_bars={
                "AAPL": [
                    date(2024, 1, 2),
                    date(2024, 1, 3),
                    date(2024, 1, 4),
                    date(2024, 1, 5),
                ]
            }
        ),
        repo,
        settings,
    )
    report = await pipeline.run(
        symbols=["AAPL"], start=date(2024, 1, 2), end=date(2024, 1, 6)
    )
    assert report.status is RunStatus.PARTIAL
    assert report.counters.quarantined == 4
    assert report.counters.inserted == 0
    assert await _count(migrated_engine, "market_quote_quarantine") == 4


@pytest.mark.asyncio
async def test_quarantined_only_after_db_confirm_for_parse_failure(
    migrated_engine, settings
):
    await _cleanup(migrated_engine)

    class QuarantineBoom(QuoteRepository):
        async def quarantine(self, row, run_id):
            raise ConnectionError("connection refused")

        async def upsert_bar(self, bar, run_id):
            return await QuoteRepository.upsert_bar(self, bar, run_id)

    repo = QuarantineBoom(migrated_engine)
    adapter = YFinanceAdapter()
    with patch(
        "worker.adapters.yfinance_adapter._download_sync",
        return_value=_mixed_frame(),
    ):
        report = await MarketDataPipeline(adapter, repo, settings).run(
            symbols=["AAPL"],
            start=date(2024, 1, 2),
            end=date(2024, 1, 4),
        )
    assert report.counters.received == 2
    assert report.counters.inserted == 1
    assert report.counters.quarantined == 0
    assert report.counters.failed >= 1
    assert report.status is RunStatus.PARTIAL
