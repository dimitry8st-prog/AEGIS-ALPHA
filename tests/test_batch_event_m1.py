import json
from datetime import datetime, timezone

import pytest

pytest.importorskip("backtrader")

from scripts.batch_event_m1 import DEFAULT_MANIFEST, freeze, lock_path, run_batch
from scripts.replay_event_signal import EXAMPLE_DIR, MINUTE_MS, replay, window_start_ms

CANDLES = EXAMPLE_DIR / "candles.json"
REPORT = DEFAULT_MANIFEST.with_name("report.json")
FIRST_PRINT = "2026-11-19T19:48:24+03:00"
SECOND_SESSION = "2026-11-20T17:30:00+03:00"


def _rows():
    return json.loads(CANDLES.read_text(encoding="utf-8"))


def _case(case_id, published_at, snapshot, **extra):
    return {
        "id": case_id, "role": "exploratory", "ticker": "xyz:ANT", "venue": "Hyperliquid",
        "currency": "USD", "event": "test", "published_at": published_at,
        "source_url": "https://example.com", "snapshot": str(snapshot), **extra,
    }


def _manifest(tmp_path, cases, rows=None, **costs):
    snapshot = tmp_path / "candles.json"
    snapshot.write_text(json.dumps(_rows() if rows is None else rows), encoding="utf-8")
    parameters = {"commission_bps": 1.0, "spread_bps": 2.0, "slippage_bps": 1.0, "units": 1.0, "cash": 10_000.0}
    parameters.update(costs)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({
        "parameters": parameters,
        "cases": [{**case, "snapshot": case["snapshot"].replace("SNAPSHOT", str(snapshot))}
                  if case.get("snapshot") else case for case in cases],
    }), encoding="utf-8")
    return path


def _with_expiry_candle(rows, symbol="xyz:ANT"):
    expiry = window_start_ms("2026-11-19T17:19:00Z")
    return rows + [{
        "s": symbol, "i": "1m", "t": expiry, "T": expiry + MINUTE_MS - 1,
        "o": "96", "h": "98", "l": "94", "c": "97", "v": "10",
    }]


def _runs(report, case_id):
    return {
        (run["range_minutes"], run["delay_seconds"]): run
        for run in next(case for case in report["cases"] if case["id"] == case_id)["runs"]
    }


def test_committed_report_is_reproducible():
    assert run_batch(DEFAULT_MANIFEST) == json.loads(REPORT.read_text(encoding="utf-8"))


def test_control_signal_times_in_msk():
    report = run_batch(DEFAULT_MANIFEST)
    assert report["summary"]["control_checks"] == "PASS"
    first = _runs(report, "anthropic-ipo-first-print")
    second = _runs(report, "anthropic-second-session")
    for delay in (0, 1):
        assert first[(2, delay)]["signal"]["available_at_msk"] == "2026-11-19T19:52:00+03:00"
        assert first[(5, delay)]["signal"]["available_at_msk"] == "2026-11-19T20:00:00+03:00"
        for range_minutes in (2, 5):
            assert second[(range_minutes, delay)]["signal"]["available_at_msk"] == "2026-11-20T17:47:00+03:00"
    assert first[(2, 0)]["entry"]["signal_to_execution_seconds"] == 0
    assert first[(2, 1)]["entry"]["signal_to_execution_seconds"] == 60
    assert first[(2, 1)]["entry"]["execution_time_msk"] == "2026-11-19T19:53:00+03:00"


def test_anthropic_cases_are_exploratory_and_russian_data_is_unavailable():
    report = run_batch(DEFAULT_MANIFEST)
    roles = {case["id"]: (case["role"], case["status"]) for case in report["cases"]}
    assert roles["anthropic-ipo-first-print"] == ("exploratory", "EVALUATED")
    assert roles["anthropic-second-session"] == ("exploratory", "EVALUATED")
    for case_id in ("gazp-m1-control-window", "gmkn-m1-control-window", "mtss-disclosure"):
        assert roles[case_id] == ("independent", "DATA_UNAVAILABLE")
    assert report["cases"][0]["snapshot_sha256"].startswith("sha256:")
    assert report["manifest_sha256"].startswith("sha256:")


@pytest.mark.parametrize("range_minutes", [2, 5])
@pytest.mark.parametrize("published_at", [FIRST_PRINT, SECOND_SESSION])
def test_signal_ignores_candles_after_it_became_available(range_minutes, published_at):
    case = {"id": "future", "event": "test", "watch_from": published_at}
    rows = _rows()
    original = replay("xyz:ANT", case, rows, range_minutes=range_minutes)["candidate"]
    cutoff = window_start_ms(original["available_at"])
    # Later candles swing far outside both boundaries; only the past may decide.
    rewritten = [
        {**row, "o": "500", "h": "999", "l": "1", "c": "1" if original["direction"] == "LONG" else "999"}
        if row["t"] >= cutoff else row
        for row in rows
    ]
    assert replay("xyz:ANT", case, rewritten, range_minutes=range_minutes)["candidate"] == original


def test_missing_exit_candle_leaves_pnl_undefined(tmp_path):
    report = run_batch(_manifest(tmp_path, [_case("first", FIRST_PRINT, "SNAPSHOT")]))
    for run in _runs(report, "first").values():
        assert run["outcome"] == "OPEN_POSITION"
        assert run["exit"] is None
        assert run["pnl_before_costs"] is None and run["pnl_after_costs"] is None
        assert run["open_position"]["note"] == "position remains open; no exit fill invented"
    bucket = report["summary"]["variants"][0]["by_role"]["exploratory"]
    assert (bucket["open_position"], bucket["closed"], bucket["pnl_by_currency"]) == (1, 0, {})


def test_costs_reduce_pnl_by_their_exact_amount(tmp_path):
    rows = _with_expiry_candle(_rows())
    case = _case("first", FIRST_PRINT, "SNAPSHOT")
    with_costs = run_batch(_manifest(tmp_path, [case], rows=rows))
    free = run_batch(_manifest(tmp_path, [case], rows=rows, commission_bps=0, spread_bps=0, slippage_bps=0))
    for key, run in _runs(with_costs, "first").items():
        free_run = _runs(free, "first")[key]
        assert run["outcome"] == free_run["outcome"] == "CLOSED"
        assert run["exit"]["reason"] == "window_end"
        assert run["exit"]["execution_time"] == "2026-11-19T17:19:00+00:00"
        assert run["pnl_before_costs"] == free_run["pnl_before_costs"] == free_run["pnl_after_costs"]
        assert run["costs"]["total"] > 0
        assert run["pnl_after_costs"] == pytest.approx(run["pnl_before_costs"] - run["costs"]["total"], abs=1e-6)
    # Short from 106.99 to the 96 expiry open on the 2-minute range with no delay.
    assert _runs(with_costs, "first")[(2, 0)]["pnl_before_costs"] == pytest.approx(10.99)


def test_no_signal_counts_in_the_signal_rate_denominator(tmp_path):
    rows = _rows()
    start = window_start_ms(FIRST_PRINT)
    flat = [
        {**row, "o": "110", "h": "110.5", "l": "110", "c": "110"}
        if start + 2 * MINUTE_MS <= row["t"] < start + 30 * MINUTE_MS else row
        for row in rows
    ]
    report = run_batch(_manifest(tmp_path, [
        _case("quiet", FIRST_PRINT, "SNAPSHOT"), _case("second", SECOND_SESSION, "SNAPSHOT"),
    ], rows=flat))
    for variant in report["summary"]["variants"]:
        bucket = variant["by_role"]["exploratory"]
        assert (bucket["cases_with_data"], bucket["signals"], bucket["no_signal"]) == (2, 1, 1)
        assert bucket["signal_rate"] == 0.5
    assert {run["outcome"] for run in _runs(report, "quiet").values()} == {"NO_SIGNAL"}


def test_missing_candle_is_a_data_gap_outside_the_denominator(tmp_path):
    gap = window_start_ms("2026-11-20T14:40:00Z")
    rows = [row for row in _rows() if row["t"] != gap]
    report = run_batch(_manifest(tmp_path, [
        _case("first", FIRST_PRINT, "SNAPSHOT"), _case("second", SECOND_SESSION, "SNAPSHOT"),
    ], rows=rows))
    second = next(case for case in report["cases"] if case["id"] == "second")
    assert second["status"] == "DATA_GAP" and second["runs"] == []
    assert report["summary"]["data_issues"]["DATA_GAP"] == ["second"]
    assert report["summary"]["variants"][0]["by_role"]["exploratory"]["cases_with_data"] == 1


def test_independent_case_requires_freeze_before_results(tmp_path):
    rows = _with_expiry_candle([{**row, "s": "GAZP"} for row in _rows()], symbol="GAZP")
    independent = {
        **_case("gazp", FIRST_PRINT, "SNAPSHOT"), "role": "independent",
        "ticker": "GAZP", "venue": "MOEX", "currency": "RUB",
    }
    manifest = _manifest(tmp_path, [independent], rows=rows)
    hidden = run_batch(manifest)["cases"][0]
    assert hidden["status"] == "NOT_PREREGISTERED" and hidden["runs"] == []

    freeze(manifest, now=datetime(2026, 11, 25, tzinfo=timezone.utc))
    with pytest.raises(FileExistsError):
        freeze(manifest)
    report = run_batch(manifest)
    assert report["cases"][0]["status"] == "EVALUATED"
    assert report["cases"][0]["frozen_at"] == "2026-11-25T00:00:00+00:00"
    pnl = report["summary"]["variants"][0]["by_role"]["independent"]["pnl_by_currency"]
    assert list(pnl) == ["RUB"] and pnl["RUB"]["closed_trades"] == 1

    changed = json.loads(manifest.read_text(encoding="utf-8"))
    changed["cases"][0]["published_at"] = "2026-11-19T19:49:00+03:00"
    manifest.write_text(json.dumps(changed), encoding="utf-8")
    assert run_batch(manifest)["cases"][0]["status"] == "NOT_PREREGISTERED"
    assert lock_path(manifest).exists()


def test_currencies_are_aggregated_separately(tmp_path):
    rows = _with_expiry_candle(_rows())
    rows += [{**row, "s": "GAZP"} for row in rows]
    rub = {**_case("gazp", FIRST_PRINT, "SNAPSHOT"), "ticker": "GAZP", "venue": "MOEX", "currency": "RUB"}
    report = run_batch(_manifest(tmp_path, [_case("ant", FIRST_PRINT, "SNAPSHOT"), rub], rows=rows))
    pnl = report["summary"]["variants"][0]["by_role"]["exploratory"]["pnl_by_currency"]
    assert sorted(pnl) == ["RUB", "USD"]
    assert pnl["RUB"]["closed_trades"] == pnl["USD"]["closed_trades"] == 1
    assert pnl["RUB"]["pnl_after_costs"] == pnl["USD"]["pnl_after_costs"]
