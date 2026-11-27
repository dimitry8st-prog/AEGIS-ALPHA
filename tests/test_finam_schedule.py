from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from worker import finam_schedule
from worker.types import AdjustmentMode, RunStatus


@pytest.mark.parametrize(
    ("hour", "minute", "expected"),
    [(5, 29, date(2026, 11, 21)), (5, 30, date(2026, 11, 22))],
)
def test_schedule_boundary_in_moscow_time(hour, minute, expected):
    utc = datetime(2026, 11, 22, hour, minute, tzinfo=timezone.utc)
    assert finam_schedule.scheduled_window_end(utc) == expected


def report(status, *codes):
    return SimpleNamespace(
        status=status,
        errors=[{"code": code} for code in codes],
        to_dict=lambda: {"status": status.value},
    )


@pytest.mark.asyncio
async def test_schedule_catches_up_and_retries_only_the_transient_window(monkeypatch):
    now = datetime(2026, 11, 22, 5, 29, 50, tzinfo=timezone.utc)
    tick_offsets = iter((10, 899, 1, 60, 86_400, 900, 900))
    pipeline = SimpleNamespace(run=AsyncMock(side_effect=[
        report(RunStatus.READY),
        report(RunStatus.PARTIAL, "FINAM_NETWORK_ERROR"),
        report(RunStatus.READY),
        report(RunStatus.FAILED, "FINAM_AUTHORIZATION_FAILED"),
    ]))

    class FrozenClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz)

    class StopSchedule(Exception):
        pass

    async def advance_clock(_seconds):
        nonlocal now
        offset = next(tick_offsets, None)
        if offset is None:
            raise StopSchedule
        now += timedelta(seconds=offset)

    monkeypatch.setattr(finam_schedule, "datetime", FrozenClock)
    monkeypatch.setattr(finam_schedule.asyncio, "sleep", advance_clock)
    with pytest.raises(StopSchedule):
        await finam_schedule.run_finam_schedule(pipeline)

    expected = [date(2026, 11, day) for day in (21, 22, 22, 23)]
    assert [call.kwargs["end"] for call in pipeline.run.await_args_list] == expected
    for call, end in zip(pipeline.run.await_args_list, expected, strict=True):
        assert call.kwargs == {
            "symbols": ["GAZP", "GMKN", "SBER", "YDEX", "X5"],
            "start": end - timedelta(days=7),
            "end": end,
            "adjustment_mode": AdjustmentMode.RAW,
            "interval": "1d",
        }
