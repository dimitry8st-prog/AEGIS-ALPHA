from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from worker.types import AdjustmentMode, QuoteBar
from worker.validation import validate_bar


def _bar(**overrides) -> QuoteBar:
    base = dict(
        provider="offline",
        symbol="AAPL",
        exchange="XNAS",
        interval="1d",
        quote_time=datetime(2024, 1, 2, 16, 0, tzinfo=ZoneInfo("America/New_York")),
        open=Decimal("10"),
        high=Decimal("12"),
        low=Decimal("9"),
        close=Decimal("11"),
        volume=1000,
        adjustment_mode=AdjustmentMode.AUTO_ADJUSTED,
    )
    base.update(overrides)
    return QuoteBar(**base)


def test_valid_bar_passes():
    assert validate_bar(_bar()) is None


def test_ohlc_inconsistent_quarantined():
    bad = validate_bar(_bar(high=Decimal("8"), low=Decimal("9")))
    assert bad is not None
    assert bad.reason_code == "OHLC_INCONSISTENT"


def test_negative_volume_quarantined():
    bad = validate_bar(_bar(volume=-1))
    assert bad is not None
    assert bad.reason_code == "INVALID_VOLUME"
