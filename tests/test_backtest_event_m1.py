import json

import pytest

pytest.importorskip("backtrader")

from scripts.backtest_event_m1 import main, run_case, snapshot_id
from scripts.replay_event_signal import EXAMPLE_DIR, MINUTE_MS, window_start_ms

EVENTS = EXAMPLE_DIR / "events.json"
CANDLES = EXAMPLE_DIR / "candles.json"
BENCHMARKS = EXAMPLE_DIR / "benchmarks.json"


def _payload():
    events = json.loads(EVENTS.read_text(encoding="utf-8"))
    rows = json.loads(CANDLES.read_text(encoding="utf-8"))
    benchmarks = json.loads(BENCHMARKS.read_text(encoding="utf-8"))["cases"]
    case = next(item for item in events["cases"] if item["id"] == "anthropic-ipo-first-print")
    sunlit = next(item for item in benchmarks if item["trader"] == "Sunlit Fathom")
    return events["symbol"], case, rows, sunlit


def _run(rows=None, case=None, delay_seconds=0, **kwargs):
    symbol, default_case, default_rows, sunlit = _payload()
    return run_case(
        symbol=symbol,
        case=case or default_case,
        rows=default_rows if rows is None else rows,
        dataset_id=snapshot_id(CANDLES),
        candles_path=CANDLES,
        benchmark=sunlit,
        delay_seconds=delay_seconds,
        units=kwargs.get("units", 1.0),
        cash=kwargs.get("cash", 10_000.0),
        commission_bps=kwargs.get("commission_bps", 1.0),
        spread_bps=kwargs.get("spread_bps", 2.0),
        slippage_bps=kwargs.get("slippage_bps", 1.0),
    )


def test_sunlit_fathom_offline_run_reports_lag_and_fill():
    report = _run()
    assert report["status"] == "FILLED"
    assert report["signal"]["direction"] == "SHORT"
    assert report["signal"]["signal_available_at"] == "2026-11-19T17:00:00+00:00"
    assert report["signal"]["aegis_available_at_msk"] == "2026-11-19T20:00:00+03:00"
    assert report["signal"]["benchmark_entry_at_msk"] == "2026-11-19T19:54:56+03:00"
    assert report["signal"]["lag_seconds_vs_benchmark"] == 304
    entry = next(row for row in report["executions"] if row["role"] == "entry")
    assert entry["status"] == "FILLED"
    assert entry["execution_time"] == "2026-11-19T17:00:00+00:00"
    assert entry["bar_open"] == entry["execution_time"]
    assert entry["raw_open"] == 105.06
    assert entry["signal_available_at"] == "2026-11-19T17:00:00+00:00"
    assert report["dataset_id"].startswith("sha256:")


def test_missing_execution_bar_has_no_invented_price():
    report = _run(delay_seconds=60 * 60)
    assert report["status"] == "MISSING_EXECUTION_BAR"
    assert report["executions"] == []
    assert report["pnl_after_costs"]["realized"] == 0.0
    assert "no fill price invented" in report["pnl_after_costs"]["note"]


def test_absence_of_signal_is_reported():
    symbol, case, rows, _ = _payload()
    muted = []
    start = 1795106940000
    for row in rows:
        item = dict(row)
        if start <= item["t"] < start + 30 * 60_000 and item["t"] >= start + 5 * 60_000:
            # Keep the opening range intact; force later closes inside 105.1–114.73.
            item["o"] = "110.0"
            item["c"] = "110.0"
            item["h"] = "110.5"
            item["l"] = "109.5"
        muted.append(item)
    report = run_case(
        symbol=symbol,
        case=case,
        rows=muted,
        dataset_id="sha256:test",
        candles_path=CANDLES,
        benchmark=None,
        delay_seconds=0,
        units=1.0,
        cash=10_000.0,
        commission_bps=1.0,
        spread_bps=2.0,
        slippage_bps=1.0,
    )
    assert report["status"] == "NO_SIGNAL"
    assert report["signal"] is None
    assert report["executions"] == []


@pytest.mark.parametrize(("delay", "minute", "price"), [(1, "01", 105.07), (120, "02", 104.69)])
def test_execution_respects_delay_before_first_open(delay, minute, price):
    report = _run(delay_seconds=delay)
    entry = next(row for row in report["executions"] if row["role"] == "entry")
    assert entry["execution_time"] == f"2026-11-19T17:{minute}:00+00:00"
    assert entry["signal_available_at"] == "2026-11-19T17:00:00+00:00"
    assert entry["raw_open"] == price


def test_invalidation_exits_on_open_after_invalidating_close():
    _, _, rows, _ = _payload()
    invalidating_bar = window_start_ms("2026-11-19T17:02:00Z")
    rows = [
        {**row, "h": "116", "c": "115"} if row["t"] == invalidating_bar else row
        for row in rows
    ]
    report = _run(rows=rows)
    entry, exit_fill = report["executions"]
    assert entry["role"] == "entry"
    assert (
        exit_fill["role"], exit_fill["reason"], exit_fill["execution_time"], exit_fill["raw_open"]
    ) == ("exit", "invalidation", "2026-11-19T17:03:00+00:00", 102.82)
    assert report["open_position"] is None


def test_expiry_open_is_for_exits_only():
    symbol, _, rows, _ = _payload()
    expiry = window_start_ms("2026-11-19T17:19:00Z")
    # A synthetic boundary candle, distinct from the preceding close in the saved snapshot.
    rows.append({
        "s": symbol, "i": "1m", "t": expiry, "T": expiry + MINUTE_MS - 1,
        "o": "96", "h": "98", "l": "94", "c": "97", "v": "10",
    })
    report = _run(rows=rows, delay_seconds=18 * 60)
    entry, exit_fill = report["executions"]
    assert entry["execution_time"] == "2026-11-19T17:18:00+00:00"
    assert (
        exit_fill["role"], exit_fill["reason"], exit_fill["execution_time"], exit_fill["raw_open"]
    ) == ("exit", "window_end", "2026-11-19T17:19:00+00:00", 96)
    assert report["pnl_after_costs"]["realized"] is not None
    assert report["open_position"] is None

    expired_entry = _run(rows=rows, delay_seconds=19 * 60)
    assert expired_entry["status"] == "MISSING_EXECUTION_BAR"
    assert expired_entry["executions"] == []


def test_missing_expiry_open_keeps_position_open():
    report = _run()
    assert [fill["role"] for fill in report["executions"]] == ["entry"]
    assert report["open_position"]["bar_open"] == "2026-11-19T17:18:00+00:00"
    assert report["open_position"]["exit_pending_reason"] == "window_end"
    assert report["pnl_after_costs"]["realized"] is None


@pytest.mark.parametrize("symbol", ["GAZP", "GMKN"])
def test_finam_event_reports_rub(symbol, tmp_path, monkeypatch, capsys):
    _, case, rows, _ = _payload()
    candidate = {"provider": "finam", "symbol": "GAZP", "cases": [{**case, "id": "moex-test"}]}
    events = tmp_path / "events.json"
    candles = tmp_path / "candles.json"
    events.write_text(json.dumps(candidate), encoding="utf-8")
    candles.write_text(json.dumps([{**row, "s": symbol} for row in rows]), encoding="utf-8")
    monkeypatch.setattr("sys.argv", [
        "backtest_event_m1", "--events", str(events), "--candles", str(candles), "--case-id", "moex-test",
        "--symbol", symbol.lower(),
    ])
    assert main() == 0
    report = json.loads(capsys.readouterr().out)
    assert report["symbol"] == symbol
    assert report["status"] == "FILLED"
    assert report["pnl_after_costs"]["currency"] == "RUB"


def test_same_snapshot_is_deterministic():
    first = _run()
    second = _run()
    assert first == second


def test_slate_brume_marked_unverified_in_benchmark_file():
    secondary = next(
        item for item in json.loads(BENCHMARKS.read_text(encoding="utf-8"))["cases"]
        if item["trader"] == "Slate Brume"
    )
    assert secondary["status"] == "UNVERIFIED"
    assert secondary["unverified"]
