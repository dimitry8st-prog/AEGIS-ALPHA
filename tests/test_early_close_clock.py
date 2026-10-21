"""A legacy 16:00 early close becomes one 13:00 quote and keeps its history."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text

from worker.adapters.base import MarketDataAdapter
from worker.clock_migration import migrate_legacy_early_closes
from worker.config import WorkerSettings
from worker.pipeline import MarketDataPipeline
from worker.repository import QuoteRepository, _payload_hash
from worker.session_calendar import quote_time_for_session
from worker.types import AdjustmentMode, FetchResult, FetchStatus, QuoteBar, RunStatus

DAY = date(2024, 7, 3)
LEGACY = datetime(2024, 7, 3, 16, 0, tzinfo=ZoneInfo("America/New_York"))
OPEN = Decimal("10.00")
HIGH = Decimal("11.00")
LOW = Decimal("9.00")
CLOSE = Decimal("10.50")
VOLUME = 1000


@pytest.fixture
def settings(database_url):
    return WorkerSettings(
        DATABASE_URL=database_url,
        IO_RETRY_WAIT_SECONDS=0.01,
        IO_MAX_ATTEMPTS=3,
        RUN_DEADLINE_SECONDS=60,
    )


class EarlyCloseAdapter(MarketDataAdapter):
    provider_name = "offline"

    async def fetch_daily(self, symbol, start, end, adjustment_mode):
        if not (start <= DAY < end):
            return FetchResult(status=FetchStatus.EMPTY, symbol=symbol.upper(), bars=[])
        bar = _bar(symbol.upper(), adjustment_mode, quote_time_for_session(DAY))
        return FetchResult(status=FetchStatus.OK, symbol=symbol.upper(), bars=[bar])


def _bar(symbol: str, mode: AdjustmentMode, quote_time: datetime) -> QuoteBar:
    return QuoteBar(
        provider="offline",
        symbol=symbol,
        exchange="XNAS",
        interval="1d",
        quote_time=quote_time,
        open=OPEN,
        high=HIGH,
        low=LOW,
        close=CLOSE,
        volume=VOLUME,
        adjustment_mode=mode,
    )


async def _cleanup(engine):
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM market_quote_revisions"))
        await conn.execute(text("DELETE FROM market_quote_quarantine"))
        await conn.execute(text("DELETE FROM market_quotes"))
        await conn.execute(text("DELETE FROM market_collection_runs"))


async def _insert_legacy(engine, quote_time: datetime, symbol: str = "AAPL") -> str:
    bar = _bar(symbol, AdjustmentMode.AUTO_ADJUSTED, quote_time)
    quote_id = uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO market_quotes (
                    id, provider, symbol, exchange, interval, quote_time,
                    open, high, low, close, volume, adjustment_mode,
                    session_timezone, currency, received_at, payload_hash
                ) VALUES (
                    :id, :provider, :symbol, :exchange, :interval, :quote_time,
                    :open, :high, :low, :close, :volume, :adjustment_mode,
                    :session_timezone, :currency, CURRENT_TIMESTAMP, :payload_hash
                )
                """
            ),
            {
                "id": quote_id,
                "provider": bar.provider,
                "symbol": bar.symbol,
                "exchange": bar.exchange,
                "interval": bar.interval,
                "quote_time": quote_time,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "adjustment_mode": bar.adjustment_mode.value,
                "session_timezone": bar.session_timezone,
                "currency": bar.currency,
                "payload_hash": _payload_hash(bar),
            },
        )
    return str(quote_id)


async def _quotes(engine, symbol: str = "AAPL"):
    async with engine.connect() as conn:
        return (
            await conn.execute(
                text(
                    """
                    SELECT id::text AS id,
                           to_char(quote_time AT TIME ZONE 'America/New_York', 'HH24:MI') AS et
                    FROM market_quotes
                    WHERE symbol = :symbol
                    ORDER BY quote_time
                    """
                ),
                {"symbol": symbol},
            )
        ).mappings().all()


async def _revisions(engine):
    async with engine.connect() as conn:
        return (
            await conn.execute(
                text(
                    """
                    SELECT id::text AS id,
                           quote_id::text AS quote_id,
                           to_char(previous_quote_time AT TIME ZONE 'America/New_York', 'HH24:MI') AS previous_et,
                           to_char(quote_time AT TIME ZONE 'America/New_York', 'HH24:MI') AS current_et,
                           previous_close::text AS previous_close
                    FROM market_quote_revisions
                    ORDER BY id
                    """
                )
            )
        ).mappings().all()


@pytest.mark.asyncio
async def test_recollect_moves_legacy_early_close_without_a_second_row(migrated_engine, settings):
    await _cleanup(migrated_engine)
    legacy_id = await _insert_legacy(migrated_engine, LEGACY)
    repo = QuoteRepository(migrated_engine)
    pipeline = MarketDataPipeline(EarlyCloseAdapter(), repo, settings)
    report = await pipeline.run(
        symbols=["AAPL"],
        start=date(2024, 7, 3),
        end=date(2024, 7, 4),
        adjustment_mode=AdjustmentMode.AUTO_ADJUSTED,
    )
    again = await pipeline.run(
        symbols=["AAPL"],
        start=date(2024, 7, 3),
        end=date(2024, 7, 4),
        adjustment_mode=AdjustmentMode.AUTO_ADJUSTED,
    )
    rows = await _quotes(migrated_engine)
    history = await _revisions(migrated_engine)
    assert report.status is RunStatus.READY
    assert report.counters.inserted == 0
    assert report.counters.updated == 1
    assert again.counters.inserted == 0
    assert again.counters.duplicates == 1
    assert [(row["id"], row["et"]) for row in rows] == [(legacy_id, "13:00")]
    assert len(history) == 1
    assert history[0]["quote_id"] == legacy_id
    assert history[0]["previous_et"] == "16:00"
    assert history[0]["current_et"] == "13:00"
    assert Decimal(history[0]["previous_close"]) == CLOSE


@pytest.mark.asyncio
async def test_existing_pair_keeps_one_quote_and_the_old_close(migrated_engine, settings):
    await _cleanup(migrated_engine)
    legacy_id = await _insert_legacy(migrated_engine, LEGACY, symbol="MSFT")
    current_id = await _insert_legacy(
        migrated_engine, quote_time_for_session(DAY), symbol="MSFT"
    )
    repo = QuoteRepository(migrated_engine)
    report = await MarketDataPipeline(EarlyCloseAdapter(), repo, settings).run(
        symbols=["MSFT"],
        start=DAY,
        end=date(2024, 7, 4),
        adjustment_mode=AdjustmentMode.AUTO_ADJUSTED,
    )
    rows = await _quotes(migrated_engine, "MSFT")
    history = await _revisions(migrated_engine)
    assert report.status is RunStatus.READY
    assert [row["id"] for row in rows] == [current_id]
    assert rows[0]["et"] == "13:00"
    assert legacy_id not in {row["id"] for row in rows}
    assert any(item["previous_et"] == "16:00" and item["quote_id"] == current_id for item in history)


async def _insert_prior_revision(engine, quote_id: str, previous_close: Decimal) -> str:
    revision_id = uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO market_quote_revisions (
                    id, quote_id, provider, symbol, exchange, interval, quote_time,
                    adjustment_mode, open, high, low, close, volume,
                    previous_open, previous_high, previous_low, previous_close, previous_volume,
                    changed_at
                ) VALUES (
                    :id, :quote_id, 'offline', 'AAPL', 'XNAS', '1d', :quote_time,
                    'auto_adjusted', :open, :high, :low, :close, :volume,
                    :open, :high, :low, :previous_close, :volume,
                    CURRENT_TIMESTAMP
                )
                """
            ),
            {
                "id": revision_id,
                "quote_id": quote_id,
                "quote_time": LEGACY,
                "open": OPEN,
                "high": HIGH,
                "low": LOW,
                "close": CLOSE,
                "volume": VOLUME,
                "previous_close": previous_close,
            },
        )
    return str(revision_id)


@pytest.mark.asyncio
async def test_conflict_keeps_prior_revisions_on_the_surviving_quote(migrated_engine, settings):
    await _cleanup(migrated_engine)
    legacy_id = await _insert_legacy(migrated_engine, LEGACY)
    current_id = await _insert_legacy(migrated_engine, quote_time_for_session(DAY))
    prior_ids = [
        await _insert_prior_revision(migrated_engine, legacy_id, Decimal("8.00")),
        await _insert_prior_revision(migrated_engine, legacy_id, Decimal("9.25")),
        await _insert_prior_revision(migrated_engine, legacy_id, Decimal("9.75")),
    ]
    pipeline = MarketDataPipeline(EarlyCloseAdapter(), QuoteRepository(migrated_engine), settings)
    window = dict(
        symbols=["AAPL"],
        start=DAY,
        end=date(2024, 7, 4),
        adjustment_mode=AdjustmentMode.AUTO_ADJUSTED,
    )
    first = await pipeline.run(**window)
    after_first = await _revisions(migrated_engine)
    second = await pipeline.run(**window)
    after_second = await _revisions(migrated_engine)
    rows = await _quotes(migrated_engine)

    assert first.status is RunStatus.READY
    assert second.counters.duplicates == 1
    assert [row["id"] for row in rows] == [current_id]
    kept = [item for item in after_first if Decimal(item["previous_close"]) != CLOSE]
    assert sorted(item["id"] for item in kept) == sorted(prior_ids)
    assert {item["quote_id"] for item in after_first} == {current_id}
    assert legacy_id not in {item["quote_id"] for item in after_first}
    assert len(after_second) == len(after_first)
    assert [item["id"] for item in after_second] == [item["id"] for item in after_first]


@pytest.mark.asyncio
async def test_migration_moves_legacy_stamp_and_keeps_history(migrated_engine):
    await _cleanup(migrated_engine)
    legacy_id = await _insert_legacy(migrated_engine, LEGACY, symbol="NVDA")
    async with migrated_engine.begin() as conn:
        changed = await conn.run_sync(migrate_legacy_early_closes)
    rows = await _quotes(migrated_engine, "NVDA")
    history = await _revisions(migrated_engine)
    assert changed == 1
    assert [(row["id"], row["et"]) for row in rows] == [(legacy_id, "13:00")]
    assert history[0]["previous_et"] == "16:00"
    assert history[0]["quote_id"] == legacy_id
