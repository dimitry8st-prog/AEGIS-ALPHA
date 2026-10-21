"""A quote export is a fixed snapshot of the current research database."""

from __future__ import annotations

import hashlib
import json
from datetime import date

import pytest
from sqlalchemy import text

from worker.adapters.offline import OfflineAdapter
from worker.config import WorkerSettings
from worker.export import ExportError, export_quotes
from worker.pipeline import MarketDataPipeline
from worker.repository import QuoteRepository
from worker.types import AdjustmentMode


@pytest.fixture
def settings(database_url):
    return WorkerSettings(
        DATABASE_URL=database_url,
        IO_RETRY_WAIT_SECONDS=0.01,
        IO_MAX_ATTEMPTS=3,
        RUN_DEADLINE_SECONDS=60,
    )


async def _cleanup(engine):
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM market_quote_revisions"))
        await conn.execute(text("DELETE FROM market_quote_quarantine"))
        await conn.execute(text("DELETE FROM market_quotes"))
        await conn.execute(text("DELETE FROM market_collection_runs"))


async def _load(engine, settings, mode: AdjustmentMode):
    await MarketDataPipeline(OfflineAdapter(), QuoteRepository(engine), settings).run(
        symbols=["AAPL", "MSFT", "NVDA"],
        start=date(2024, 1, 2),
        end=date(2024, 1, 6),
        adjustment_mode=mode,
    )


def _sha(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.asyncio
async def test_same_selection_repeats_the_csv_hash(migrated_engine, settings, tmp_path):
    await _cleanup(migrated_engine)
    await _load(migrated_engine, settings, AdjustmentMode.RAW)
    await _load(migrated_engine, settings, AdjustmentMode.AUTO_ADJUSTED)
    first = tmp_path / "first"
    second = tmp_path / "second"
    kwargs = dict(
        provider="offline",
        adjustment_mode="raw",
        symbols=["NVDA", "AAPL", "MSFT"],
        start=date(2024, 1, 2),
        end=date(2024, 1, 6),
    )
    one = await export_quotes(migrated_engine, output_dir=first, **kwargs)
    two = await export_quotes(migrated_engine, output_dir=second, **kwargs)
    assert (first / "quotes.csv").read_bytes() == (second / "quotes.csv").read_bytes()
    assert one["csv_sha256"] == two["csv_sha256"] == _sha(first / "quotes.csv")
    assert one["dataset_id"] == f"sha256:{one['csv_sha256']}"
    assert one["row_count"] == 12
    assert one["row_count"] == sum(item["rows"] for item in one["coverage"].values())
    assert one["gaps"] == []
    assert one["selection"]["symbols"] == ["NVDA", "AAPL", "MSFT"]
    raw_text = (first / "quotes.csv").read_text(encoding="utf-8")
    assert "auto_adjusted" not in raw_text
    assert "." in raw_text.splitlines()[1].split(",")[5]
    adjusted = tmp_path / "adjusted"
    other = await export_quotes(
        migrated_engine,
        output_dir=adjusted,
        provider="offline",
        adjustment_mode="auto_adjusted",
        symbols=["AAPL", "MSFT", "NVDA"],
        start=date(2024, 1, 2),
        end=date(2024, 1, 6),
    )
    assert other["csv_sha256"] != one["csv_sha256"]
    assert "yfinance" not in (adjusted / "quotes.csv").read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_later_quote_change_does_not_rewrite_the_snapshot(migrated_engine, settings, tmp_path):
    await _cleanup(migrated_engine)
    await _load(migrated_engine, settings, AdjustmentMode.RAW)
    original = tmp_path / "original"
    manifest = await export_quotes(
        migrated_engine,
        output_dir=original,
        provider="offline",
        adjustment_mode="raw",
        symbols=["AAPL"],
        start=date(2024, 1, 2),
        end=date(2024, 1, 6),
    )
    before = (original / "quotes.csv").read_bytes()
    async with migrated_engine.begin() as conn:
        await conn.execute(
            text(
                """
                UPDATE market_quotes
                SET close = close + 1
                WHERE symbol = 'AAPL' AND adjustment_mode = 'raw'
                """
            )
        )
    assert (original / "quotes.csv").read_bytes() == before
    changed = tmp_path / "changed"
    again = await export_quotes(
        migrated_engine,
        output_dir=changed,
        provider="offline",
        adjustment_mode="raw",
        symbols=["AAPL"],
        start=date(2024, 1, 2),
        end=date(2024, 1, 6),
    )
    assert again["csv_sha256"] != manifest["csv_sha256"]
    assert again["row_count"] == 4
    loaded = json.loads((changed / "manifest.json").read_text(encoding="utf-8"))
    assert loaded["row_count"] == 4
    assert loaded["status"] == "complete"


@pytest.mark.asyncio
async def test_empty_and_invalid_windows_are_explicit(migrated_engine, tmp_path):
    await _cleanup(migrated_engine)
    missing = tmp_path / "missing"
    with pytest.raises(ExportError) as empty:
        await export_quotes(
            migrated_engine,
            output_dir=missing,
            provider="offline",
            adjustment_mode="raw",
            symbols=["AAPL"],
            start=date(2024, 1, 2),
            end=date(2024, 1, 6),
        )
    assert empty.value.code == "EMPTY_SELECTION"
    assert not missing.exists()
    invalid = tmp_path / "invalid"
    with pytest.raises(ExportError) as bad:
        await export_quotes(
            migrated_engine,
            output_dir=invalid,
            provider="offline",
            adjustment_mode="raw",
            symbols=["AAPL"],
            start=date(2024, 1, 6),
            end=date(2024, 1, 2),
        )
    assert bad.value.code == "INVALID_WINDOW"
    assert not invalid.exists()


@pytest.mark.asyncio
async def test_failed_publish_does_not_leave_a_complete_snapshot(migrated_engine, settings, tmp_path, monkeypatch):
    await _cleanup(migrated_engine)
    await _load(migrated_engine, settings, AdjustmentMode.RAW)
    target = tmp_path / "snap"

    def fail_replace(source, destination):
        raise OSError("disk full")

    monkeypatch.setattr("worker.export.os.replace", fail_replace)
    with pytest.raises(OSError):
        await export_quotes(
            migrated_engine,
            output_dir=target,
            provider="offline",
            adjustment_mode="raw",
            symbols=["AAPL"],
            start=date(2024, 1, 2),
            end=date(2024, 1, 6),
        )
    assert not target.exists()
    assert not (tmp_path / "snap.partial").exists()


@pytest.mark.asyncio
async def test_existing_export_is_not_replaced(migrated_engine, settings, tmp_path):
    await _cleanup(migrated_engine)
    await _load(migrated_engine, settings, AdjustmentMode.RAW)
    target = tmp_path / "snap"
    await export_quotes(
        migrated_engine,
        output_dir=target,
        provider="offline",
        adjustment_mode="raw",
        symbols=["AAPL"],
        start=date(2024, 1, 2),
        end=date(2024, 1, 6),
    )
    original = (target / "quotes.csv").read_bytes()
    with pytest.raises(ExportError) as caught:
        await export_quotes(
            migrated_engine,
            output_dir=target,
            provider="offline",
            adjustment_mode="raw",
            symbols=["AAPL"],
            start=date(2024, 1, 2),
            end=date(2024, 1, 6),
        )
    assert caught.value.code == "EXPORT_EXISTS"
    assert (target / "quotes.csv").read_bytes() == original
