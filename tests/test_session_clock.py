"""Session clock, yfinance call boundary, and XNAS allowlist."""

from __future__ import annotations

import inspect
from datetime import date, datetime, time, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from worker.adapters.yfinance_adapter import YFinanceAdapter, _download_sync
from worker.io_retry import PermanentIOError, TransientIOError
from worker.session_calendar import quote_time_for_session, session_close
from worker.types import AdjustmentMode


def test_regular_and_dst_closes():
    winter = quote_time_for_session(date(2024, 1, 2))
    summer = quote_time_for_session(date(2024, 7, 5))
    after_fall = quote_time_for_session(date(2024, 11, 4))
    assert winter == datetime(2024, 1, 2, 16, 0, tzinfo=ZoneInfo("America/New_York"))
    assert winter.utcoffset() == timedelta(hours=-5)
    assert summer is not None and summer.hour == 16
    assert summer.utcoffset() == timedelta(hours=-4)
    assert after_fall is not None and after_fall.utcoffset() == timedelta(hours=-5)


def test_weekend_holiday_and_observed_new_year():
    assert session_close(date(2024, 1, 6)) is None
    assert session_close(date(2024, 1, 1)) is None
    assert session_close(date(2024, 1, 15)) is None
    assert session_close(date(2024, 3, 29)) is None
    assert session_close(date(2024, 6, 19)) is None
    assert session_close(date(2024, 7, 4)) is None
    assert session_close(date(2024, 11, 28)) is None
    assert session_close(date(2020, 7, 3)) is None
    # Sunday New Year's Day is observed on Monday.
    assert session_close(date(2023, 1, 2)) is None
    assert session_close(date(2017, 1, 2)) is None


def test_new_year_saturday_stays_a_full_session():
    # Nasdaq Release 34-93675 footnote 9: January 1, 2022 was Saturday,
    # so December 31, 2021 remained a full business day.
    stamp = quote_time_for_session(date(2021, 12, 31))
    assert stamp == datetime(2021, 12, 31, 16, 0, tzinfo=ZoneInfo("America/New_York"))
    assert session_close(date(2010, 12, 31)) == time(16, 0)


def test_shortened_sessions_close_at_13_et():
    early = quote_time_for_session(date(2024, 7, 3))
    assert session_close(date(2024, 7, 3)) == time(13, 0)
    assert session_close(date(2024, 11, 29)) == time(13, 0)
    assert session_close(date(2024, 12, 24)) == time(13, 0)
    assert early is not None and early.utcoffset() == timedelta(hours=-4)


def test_installed_yfinance_accepts_download_arguments():
    import yfinance as yf

    signature = inspect.signature(yf.download)
    signature.bind(
        tickers="AAPL",
        start="2024-01-02",
        end="2024-01-03",
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
        group_by="column",
        multi_level_index=False,
    )
    assert "multi_level_index" in signature.parameters


def test_download_error_is_not_an_empty_window(monkeypatch):
    import yfinance as yf
    from yfinance import shared

    def fake_download(**kwargs):
        shared._ERRORS["AAPL"] = "possibly delisted; no timezone found"
        return pd.DataFrame()

    monkeypatch.setattr(yf, "download", fake_download)
    with pytest.raises(TransientIOError) as caught:
        _download_sync("AAPL", date(2024, 1, 2), date(2024, 1, 3), False)
    assert caught.value.code == "SOURCE_TRANSIENT"
    assert "timezone" in str(caught.value)


def test_download_sync_binds_real_signature(monkeypatch):
    import yfinance as yf

    signature = inspect.signature(yf.download)
    seen: dict = {}

    def fake_download(**kwargs):
        signature.bind(**kwargs)
        seen.update(kwargs)
        return pd.DataFrame()

    monkeypatch.setattr(yf, "download", fake_download)
    frame = _download_sync("AAPL", date(2024, 1, 2), date(2024, 1, 3), False)
    assert frame.empty
    assert seen["start"] == "2024-01-02"
    assert seen["end"] == "2024-01-03"
    assert seen["auto_adjust"] is False
    assert seen["interval"] == "1d"
    assert seen["multi_level_index"] is False


@pytest.mark.asyncio
async def test_yfinance_rejects_non_xnas_without_download():
    adapter = YFinanceAdapter()

    def explode(*args, **kwargs):
        raise AssertionError("download must not run")

    with patch("worker.adapters.yfinance_adapter._download_sync", explode):
        with pytest.raises(PermanentIOError) as caught:
            await adapter.fetch_daily(
                "JPM", date(2024, 1, 2), date(2024, 1, 3), AdjustmentMode.RAW
            )
    assert caught.value.code == "UNSUPPORTED_INSTRUMENT"


@pytest.mark.asyncio
async def test_yfinance_early_close_and_holiday_rows():
    frame = pd.DataFrame(
        {
            "Open": [10.0, 11.0, 12.0],
            "High": [11.0, 12.0, 13.0],
            "Low": [9.0, 10.0, 11.0],
            "Close": [10.5, 11.5, 12.5],
            "Volume": [100, 100, 100],
        },
        index=pd.to_datetime(["2024-07-03", "2024-07-04", "2024-01-02"]),
    )
    adapter = YFinanceAdapter()
    with patch("worker.adapters.yfinance_adapter._download_sync", return_value=frame):
        result = await adapter.fetch_daily(
            "AAPL", date(2024, 1, 2), date(2024, 7, 5), AdjustmentMode.RAW
        )
    by_day = {bar.quote_time.date(): bar for bar in result.bars}
    assert by_day[date(2024, 7, 3)].quote_time.hour == 13
    assert by_day[date(2024, 1, 2)].quote_time.hour == 16
    assert all(bar.exchange == "XNAS" for bar in result.bars)
    assert date(2024, 7, 4) not in by_day
    assert result.parse_failures[0].reason_code == "UNSUPPORTED_SESSION"


@pytest.mark.asyncio
async def test_raw_and_auto_adjusted_are_separate_calls():
    seen = []

    def record(symbol, start, end, auto_adjust):
        seen.append(auto_adjust)
        return pd.DataFrame(
            {
                "Open": [1.0],
                "High": [2.0],
                "Low": [0.5],
                "Close": [1.5 if auto_adjust else 1.4],
                "Volume": [10],
            },
            index=pd.to_datetime(["2024-01-02"]),
        )

    adapter = YFinanceAdapter()
    with patch("worker.adapters.yfinance_adapter._download_sync", side_effect=record):
        raw = await adapter.fetch_daily(
            "AAPL", date(2024, 1, 2), date(2024, 1, 3), AdjustmentMode.RAW
        )
        adjusted = await adapter.fetch_daily(
            "AAPL", date(2024, 1, 2), date(2024, 1, 3), AdjustmentMode.AUTO_ADJUSTED
        )
    assert seen == [False, True]
    assert raw.bars[0].adjustment_mode is AdjustmentMode.RAW
    assert adjusted.bars[0].adjustment_mode is AdjustmentMode.AUTO_ADJUSTED
    assert raw.bars[0].close != adjusted.bars[0].close
