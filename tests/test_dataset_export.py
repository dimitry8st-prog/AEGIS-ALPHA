"""A quote export is a fixed snapshot of the current research database."""

from __future__ import annotations

import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text

import worker.export as export_module
from worker.adapters.offline import OfflineAdapter
from worker.config import WorkerSettings
from worker.export import ExportError, export_quotes
from worker.pipeline import MarketDataPipeline
from worker.repository import QuoteRepository
from worker.types import AdjustmentMode, QuoteBar

MOSCOW = ZoneInfo("Europe/Moscow")


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
    assert raw_text.splitlines()[1].split(",")[6] == "185.00000000"
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
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_failed_manifest_write_leaves_no_snapshot(migrated_engine, settings, tmp_path, monkeypatch):
    await _cleanup(migrated_engine)
    await _load(migrated_engine, settings, AdjustmentMode.RAW)
    target = tmp_path / "snap"
    original = export_module._write_synced
    calls = []

    def fail_second(path, data):
        calls.append(path.name)
        if path.name == "manifest.json":
            raise OSError("disk full")
        original(path, data)

    monkeypatch.setattr(export_module, "_write_synced", fail_second)
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
    assert calls == ["quotes.csv", "manifest.json"]
    assert not target.exists()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_empty_existing_directory_is_left_alone(migrated_engine, settings, tmp_path):
    await _cleanup(migrated_engine)
    await _load(migrated_engine, settings, AdjustmentMode.RAW)
    target = tmp_path / "snap"
    target.mkdir()
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
    assert target.is_dir() and list(target.iterdir()) == []
    assert [path.name for path in tmp_path.iterdir()] == ["snap"]


@pytest.mark.asyncio
async def test_directory_created_during_export_is_not_touched(migrated_engine, settings, tmp_path, monkeypatch):
    await _cleanup(migrated_engine)
    await _load(migrated_engine, settings, AdjustmentMode.RAW)
    target = tmp_path / "snap"
    read_rows = export_module._read_rows

    async def foreign_writer(*args, **kwargs):
        rows = await read_rows(*args, **kwargs)
        target.mkdir()
        (target / "owner.txt").write_text("another process", encoding="utf-8")
        return rows

    monkeypatch.setattr(export_module, "_read_rows", foreign_writer)
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
    assert sorted(path.name for path in target.iterdir()) == ["owner.txt"]
    assert [path.name for path in tmp_path.iterdir()] == ["snap"]


def test_concurrent_publish_to_one_target_keeps_one_snapshot(tmp_path):
    target = tmp_path / "snap"
    barrier = threading.Barrier(8)

    def attempt(index: int):
        csv_text = f"provider,symbol\noffline,ATTEMPT{index}\n"
        digest = hashlib.sha256(csv_text.encode("utf-8")).hexdigest()
        barrier.wait()
        try:
            export_module.publish(target, csv_text, {"csv_sha256": digest, "status": "complete"})
            return "published"
        except ExportError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(attempt, range(8)))
    assert outcomes.count("published") == 1
    assert outcomes.count("EXPORT_EXISTS") == 7
    manifest = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["csv_sha256"] == _sha(target / "quotes.csv")
    assert sorted(path.name for path in target.iterdir()) == ["manifest.json", "quotes.csv"]
    assert [path.name for path in tmp_path.iterdir()] == ["snap"]


def _moex_bar(symbol: str, day: date, close: str, *, hour=23, minute=59, second=59) -> QuoteBar:
    return QuoteBar(
        provider="moex",
        symbol=symbol,
        exchange="MISX",
        interval="1d",
        quote_time=datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=MOSCOW),
        open=Decimal("100.10"),
        high=Decimal("110.00"),
        low=Decimal("99.00"),
        close=Decimal(close),
        volume=1000,
        adjustment_mode=AdjustmentMode.RAW,
        session_timezone="Europe/Moscow",
        currency="RUB",
    )


async def _seed_moex(engine, bars: list[QuoteBar]):
    repo = QuoteRepository(engine)
    run_id = await repo.create_run(
        provider="moex",
        symbols=sorted({bar.symbol for bar in bars}),
        interval="1d",
        adjustment_mode="raw",
        window_start=date(2024, 1, 9),
        window_end=date(2024, 1, 13),
        params={},
    )
    for bar in bars:
        await repo.upsert_bar(bar, run_id)
    return run_id


@pytest.mark.asyncio
async def test_moex_snapshot_uses_moscow_dates_and_unknown_coverage(migrated_engine, tmp_path):
    await _cleanup(migrated_engine)
    run_id = await _seed_moex(
        migrated_engine,
        [
            _moex_bar("GAZP", date(2024, 1, 9), "105.12345678"),
            _moex_bar("GMKN", date(2024, 1, 9), "105.50"),
            # 00:00:01 MSK is still January 9 in New York and UTC.
            _moex_bar("GAZP", date(2024, 1, 10), "106.00", hour=0, minute=0, second=1),
        ],
    )
    kwargs = dict(
        provider="moex",
        adjustment_mode="raw",
        symbols=["GAZP", "GMKN"],
        start=date(2024, 1, 10),
        end=date(2024, 1, 11),
    )
    boundary = await export_quotes(migrated_engine, output_dir=tmp_path / "jan10", **kwargs)
    lines = (tmp_path / "jan10" / "quotes.csv").read_text(encoding="utf-8").splitlines()
    assert boundary["row_count"] == 1
    assert lines[1].split(",")[4:6] == ["2024-01-10", "2024-01-10T00:00:01+03:00"]

    window = dict(kwargs, start=date(2024, 1, 9), end=date(2024, 1, 10))
    first = await export_quotes(migrated_engine, output_dir=tmp_path / "a", **window)
    second = await export_quotes(migrated_engine, output_dir=tmp_path / "b", **window)
    assert (tmp_path / "a" / "quotes.csv").read_bytes() == (tmp_path / "b" / "quotes.csv").read_bytes()
    assert first["csv_sha256"] == second["csv_sha256"] == _sha(tmp_path / "a" / "quotes.csv")
    assert first["row_count"] == 2
    assert first["run_ids"] == [str(run_id)]
    assert first["coverage_status"] == "UNKNOWN"
    assert first["gaps"] is None
    assert first["time"]["timezone"] == "Europe/Moscow"
    assert first["time"]["calendar"] is None
    assert "not actual exchange close" in first["time"]["quote_time_rule"]
    assert not any("официальное закрытие" in item for item in first["limitations"])
    rows = (tmp_path / "a" / "quotes.csv").read_text(encoding="utf-8").splitlines()
    assert rows[1].split(",")[4:6] == ["2024-01-09", "2024-01-09T23:59:59+03:00"]
    assert rows[1].split(",")[9] == "105.12345678"
    assert {line.split(",")[13] for line in rows[1:]} == {"RUB"}

    async with migrated_engine.begin() as conn:
        await conn.execute(text("UPDATE market_quotes SET close = close + 1 WHERE symbol = 'GMKN'"))
    before = (tmp_path / "a" / "quotes.csv").read_bytes()
    changed = await export_quotes(migrated_engine, output_dir=tmp_path / "c", **window)
    assert changed["csv_sha256"] != first["csv_sha256"]
    assert (tmp_path / "a" / "quotes.csv").read_bytes() == before
    assert _sha(tmp_path / "a" / "quotes.csv") == first["csv_sha256"]


@pytest.mark.asyncio
async def test_us_snapshot_keeps_the_new_york_contract(migrated_engine, settings, tmp_path):
    await _cleanup(migrated_engine)
    await _load(migrated_engine, settings, AdjustmentMode.RAW)
    manifest = await export_quotes(
        migrated_engine,
        output_dir=tmp_path / "us",
        provider="offline",
        adjustment_mode="raw",
        symbols=["AAPL"],
        start=date(2024, 1, 2),
        end=date(2024, 1, 6),
    )
    assert manifest["coverage_status"] == "CHECKED_AGAINST_CALENDAR"
    assert manifest["gaps"] == []
    assert manifest["time"]["timezone"] == "America/New_York"
    line = (tmp_path / "us" / "quotes.csv").read_text(encoding="utf-8").splitlines()[1]
    assert line.split(",")[4:6] == ["2024-01-02", "2024-01-02T16:00:00-05:00"]


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
