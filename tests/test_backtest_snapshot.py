from argparse import Namespace
from pathlib import Path
from shutil import copytree

import pytest

pytest.importorskip("backtrader")

from scripts.backtest_snapshot import read_snapshot, run_backtest


SNAPSHOT = Path(__file__).resolve().parents[1] / "docs/examples/offline-raw-2024-01-02"


def test_signal_uses_next_open_and_charges_commission():
    report = run_backtest(Namespace(
        snapshot=SNAPSHOT, symbol="AAPL", fast=1, slow=2, units=1, cash=10_000, commission_bps=10,
    ))
    assert report["fills"] == [{
        "side": "buy", "units": 1, "signal_session": "2024-01-03",
        "fill_session": "2024-01-04", "price": 187.0, "commission": 0.187,
    }]
    assert report["open_units"] == 1
    assert report["equity_end"] == pytest.approx(10002.013)


def test_snapshot_with_modified_prices_is_rejected(tmp_path):
    copytree(SNAPSHOT, tmp_path, dirs_exist_ok=True)
    csv = tmp_path / "quotes.csv"
    csv.write_bytes(csv.read_bytes().replace(b"185.00000000", b"184.00000000", 1))
    with pytest.raises(ValueError, match="SHA-256"):
        read_snapshot(tmp_path, "AAPL")
