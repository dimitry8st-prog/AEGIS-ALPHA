"""Batch opening-range breakout check on frozen M1 snapshots.

Every manifest case runs on the same candles with pre-set 2- and 5-minute
ranges and 0/1 second delays. Signals come from
``scripts.replay_event_signal.replay``; fills come from Backtrader through
``scripts.backtest_event_m1.run_case``. Independent cases are evaluated only
after ``freeze`` has recorded their specification.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import backtrader as bt

from scripts.backtest_event_m1 import MSK, REPO_ROOT, _display_path, run_case, snapshot_id
from scripts.replay_event_signal import WATCH_MINUTES, parse_time

RANGE_MINUTES = (2, 5)
DELAY_SECONDS = (0, 1)
ROLES = ("exploratory", "independent")
CURRENCIES = ("USD", "RUB")
SPEC_FIELDS = (
    "id", "ticker", "venue", "currency", "published_at", "source_url", "event", "role",
    "public_context", "expected_signal_msk",
)
COST_FIELDS = ("commission_bps", "spread_bps", "slippage_bps", "units", "cash")
DEFAULT_MANIFEST = REPO_ROOT / "docs/examples/batch-m1/manifest.json"


def _digest(value) -> str:
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _resolve(path: str) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else REPO_ROOT / candidate


def lock_path(manifest_path: Path) -> Path:
    return manifest_path.with_name(manifest_path.stem + ".lock.json")


def load_manifest(path: Path) -> dict:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    parameters = manifest.get("parameters", {})
    for field in COST_FIELDS:
        value = parameters.get(field)
        if not isinstance(value, (int, float)) or value < 0 or (field in ("units", "cash") and value <= 0):
            raise ValueError(f"parameters.{field} must be a non-negative number")
    cases = manifest.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("the manifest needs at least one case")
    seen = set()
    for case in cases:
        case_id = case.get("id")
        if not isinstance(case_id, str) or not case_id or case_id in seen:
            raise ValueError(f"missing or duplicate case id: {case_id!r}")
        seen.add(case_id)
        if case.get("role") not in ROLES:
            raise ValueError(f"{case_id}: role must be one of {ROLES}")
        if case.get("currency") not in CURRENCIES:
            raise ValueError(f"{case_id}: currency must be one of {CURRENCIES}")
        for field in ("ticker", "venue", "source_url", "event"):
            if not isinstance(case.get(field), str) or not case[field]:
                raise ValueError(f"{case_id}: {field} is required")
        if case.get("snapshot") is not None or case.get("published_at") is not None:
            # Raises when the timezone is missing.
            parse_time(case["published_at"])
        if case.get("snapshot") is None and not case.get("unavailable_reason"):
            raise ValueError(f"{case_id}: a case without a snapshot needs unavailable_reason")
    return manifest


def case_spec_hash(case: dict, parameters: dict) -> str:
    spec = {field: case.get(field) for field in SPEC_FIELDS}
    grid = {
        "range_minutes": list(RANGE_MINUTES), "delay_seconds": list(DELAY_SECONDS),
        "watch_minutes": WATCH_MINUTES, **{field: parameters[field] for field in COST_FIELDS},
    }
    return _digest({"case": spec, "grid": grid})


def freeze(manifest_path: Path, now: datetime | None = None) -> dict:
    manifest = load_manifest(manifest_path)
    frozen_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat(timespec="seconds")
    lock = {
        "manifest": _display_path(manifest_path),
        "frozen_at": frozen_at,
        "independent_cases": {
            case["id"]: case_spec_hash(case, manifest["parameters"])
            for case in manifest["cases"] if case["role"] == "independent"
        },
    }
    with lock_path(manifest_path).open("x", encoding="utf-8") as output:
        output.write(json.dumps(lock, indent=2, ensure_ascii=False) + "\n")
    return lock


def _costs(fills: list[dict]) -> dict:
    commission = spread = slippage = 0.0
    for fill in fills:
        if fill.get("status") != "FILLED":
            continue
        notional = fill["raw_open"] * fill["units"]
        commission += fill["commission"]
        spread += notional * fill["spread_bps"] / 20_000
        slippage += notional * fill["slippage_bps"] / 10_000
    return {
        "commission": round(commission, 8),
        "spread": round(spread, 8),
        "slippage": round(slippage, 8),
        "total": round(commission + spread + slippage, 8),
    }


def summarize_run(report: dict, case: dict) -> dict:
    signal = report["signal"]
    fills = report["executions"]
    entry = next((row for row in fills if row.get("role") == "entry" and row.get("status") == "FILLED"), None)
    exit_fill = next((row for row in fills if row.get("role") == "exit" and row.get("status") == "FILLED"), None)
    if report["status"] == "NO_SIGNAL":
        outcome = "NO_SIGNAL"
    elif entry is None:
        outcome = "NO_EXECUTION"
    elif exit_fill is None:
        outcome = "OPEN_POSITION"
    else:
        outcome = "CLOSED"

    published = parse_time(case["published_at"])
    run = {
        "range_minutes": report["assumptions"]["range_minutes"],
        "delay_seconds": report["assumptions"]["delay_seconds"],
        "scenario": "idealized_lower_bound" if report["assumptions"]["delay_seconds"] == 0 else "delayed",
        "simulation": "M1",
        "outcome": outcome,
        "engine_status": report["status"],
        "side": signal["direction"] if signal else None,
        "signal": None,
        "entry": None,
        "exit": None,
        "costs": _costs(fills),
        "pnl_before_costs": None,
        "pnl_after_costs": None,
        "currency": case["currency"],
        "open_position": report["open_position"],
    }
    if signal:
        available = parse_time(signal["signal_available_at"])
        run["signal"] = {
            "signal_candle_open": signal["signal_candle_open"],
            "available_at": signal["signal_available_at"],
            "available_at_msk": available.astimezone(MSK).isoformat(),
            "event_to_signal_seconds": int((available - published).total_seconds()),
            "trigger_level": signal["trigger_level"],
            "observed_close": signal["observed_close"],
            "invalidation_level": signal["invalidation_level"],
            "reason": f"close {'below' if signal['direction'] == 'SHORT' else 'above'} {run['range_minutes']}-minute range",
        }
    for role, fill in (("entry", entry), ("exit", exit_fill)):
        if fill is None:
            continue
        executed = parse_time(fill["execution_time"])
        run[role] = {
            "side": fill["side"],
            "execution_time": fill["execution_time"],
            "execution_time_msk": executed.astimezone(MSK).isoformat(),
            "raw_open": fill["raw_open"],
            "price_after_costs_model": fill["price_after_costs_model"],
            "reason": fill["reason"],
        }
        if role == "entry":
            run["entry"]["signal_to_execution_seconds"] = int(
                (executed - parse_time(signal["signal_available_at"])).total_seconds()
            )
    if outcome == "NO_EXECUTION":
        run["note"] = report["pnl_after_costs"]["note"]
    if entry and exit_fill:
        move = exit_fill["raw_open"] - entry["raw_open"]
        gross = (-move if signal["direction"] == "SHORT" else move) * entry["units"]
        run["pnl_before_costs"] = round(gross, 8)
        run["pnl_after_costs"] = report["pnl_after_costs"]["realized"]
    return run


def control_checks(case: dict, runs: list[dict]) -> list[dict]:
    checks = []
    for range_key, expected in sorted(case.get("expected_signal_msk", {}).items()):
        for run in runs:
            if str(run["range_minutes"]) != range_key:
                continue
            actual = run["signal"]["available_at_msk"][11:16] if run["signal"] else None
            checks.append({
                "range_minutes": run["range_minutes"],
                "delay_seconds": run["delay_seconds"],
                "expected_signal_msk": expected,
                "actual_signal_msk": actual,
                "status": "PASS" if actual == expected else "FAIL",
            })
    return checks


def evaluate_case(case: dict, parameters: dict, lock: dict | None) -> dict:
    result = {
        "id": case["id"],
        "role": case["role"],
        "ticker": case["ticker"],
        "venue": case["venue"],
        "currency": case["currency"],
        "event": case["event"],
        "published_at": case.get("published_at"),
        "source_url": case["source_url"],
        "snapshot": case.get("snapshot"),
        "snapshot_sha256": None,
        "status": "EVALUATED",
        "runs": [],
    }
    if case["role"] == "exploratory":
        result["note"] = case.get("note", "exploratory: parameters were chosen after viewing this episode")
    if case.get("snapshot") is None:
        result.update(status="DATA_UNAVAILABLE", reason=case["unavailable_reason"])
        return result
    if case["role"] == "independent":
        registered = (lock or {}).get("independent_cases", {}).get(case["id"])
        if registered != case_spec_hash(case, parameters):
            result.update(
                status="NOT_PREREGISTERED",
                reason="run `freeze` before evaluation; the case or grid changed after freezing",
            )
            return result
        result["frozen_at"] = lock["frozen_at"]
    path = _resolve(case["snapshot"])
    try:
        payload = path.read_bytes()
    except FileNotFoundError:
        result.update(status="DATA_UNAVAILABLE", reason=f"snapshot not found: {case['snapshot']}")
        return result
    result["snapshot_sha256"] = snapshot_id(path, payload)
    rows = json.loads(payload.decode("utf-8"))
    replay_case = {
        "id": case["id"],
        "event": case["event"],
        "watch_from": case["published_at"],
        "sources": [case["source_url"]],
        "public_context": case.get("public_context", []),
    }
    for range_minutes in RANGE_MINUTES:
        for delay in DELAY_SECONDS:
            try:
                report = run_case(
                    symbol=case["ticker"], case=replay_case, rows=rows,
                    dataset_id=result["snapshot_sha256"], candles_path=path, benchmark=None,
                    delay_seconds=delay, units=parameters["units"], cash=parameters["cash"],
                    commission_bps=parameters["commission_bps"], spread_bps=parameters["spread_bps"],
                    slippage_bps=parameters["slippage_bps"], currency=case["currency"],
                    range_minutes=range_minutes,
                )
            except ValueError as exc:
                status = "DATA_GAP" if "missing" in str(exc) else "DATA_INVALID"
                result.update(status=status, reason=str(exc), runs=[])
                return result
            result["runs"].append(summarize_run(report, case))
    result["control_checks"] = control_checks(case, result["runs"])
    return result


def _empty_bucket() -> dict:
    return {
        "cases_with_data": 0, "signals": 0, "signal_rate": None,
        "no_signal": 0, "no_execution": 0, "open_position": 0, "closed": 0,
        "pnl_by_currency": {},
    }


def summarize(cases: list[dict]) -> dict:
    data_issues = {
        status: sorted(case["id"] for case in cases if case["status"] == status)
        for status in ("DATA_UNAVAILABLE", "DATA_GAP", "DATA_INVALID", "NOT_PREREGISTERED")
    }
    variants = []
    for range_minutes in RANGE_MINUTES:
        for delay in DELAY_SECONDS:
            by_role = {role: _empty_bucket() for role in ROLES}
            for case in cases:
                bucket = by_role[case["role"]]
                for run in case["runs"]:
                    if (run["range_minutes"], run["delay_seconds"]) != (range_minutes, delay):
                        continue
                    bucket["cases_with_data"] += 1
                    key = run["outcome"].lower()
                    bucket[key] += 1
                    if run["outcome"] != "NO_SIGNAL":
                        bucket["signals"] += 1
                    if run["outcome"] != "CLOSED":
                        continue
                    pnl = bucket["pnl_by_currency"].setdefault(run["currency"], {
                        "closed_trades": 0, "pnl_before_costs": 0.0, "costs": 0.0, "pnl_after_costs": 0.0,
                    })
                    pnl["closed_trades"] += 1
                    pnl["pnl_before_costs"] = round(pnl["pnl_before_costs"] + run["pnl_before_costs"], 8)
                    pnl["costs"] = round(pnl["costs"] + run["costs"]["total"], 8)
                    pnl["pnl_after_costs"] = round(pnl["pnl_after_costs"] + run["pnl_after_costs"], 8)
            for bucket in by_role.values():
                if bucket["cases_with_data"]:
                    bucket["signal_rate"] = round(bucket["signals"] / bucket["cases_with_data"], 4)
            variants.append({
                "range_minutes": range_minutes,
                "delay_seconds": delay,
                "scenario": "idealized_lower_bound" if delay == 0 else "delayed",
                "by_role": by_role,
            })
    checks = [check for case in cases for check in case.get("control_checks", [])]
    return {
        "signal_rate_denominator": "cases with complete candles, including cases without a signal",
        "pnl_note": "sums cover closed trades only; open positions keep P&L undefined",
        "data_issues": data_issues,
        "control_checks": "PASS" if checks and all(c["status"] == "PASS" for c in checks) else (
            "FAIL" if checks else "NONE"
        ),
        "variants": variants,
    }


def run_batch(manifest_path: Path) -> dict:
    manifest_bytes = manifest_path.read_bytes()
    manifest = load_manifest(manifest_path)
    parameters = manifest["parameters"]
    lock_file = lock_path(manifest_path)
    lock = json.loads(lock_file.read_text(encoding="utf-8")) if lock_file.exists() else None
    cases = [evaluate_case(case, parameters, lock) for case in manifest["cases"]]
    return {
        "engine": f"Backtrader {bt.__version__}",
        "strategy": "event_opening_range_breakout_m1",
        "manifest": _display_path(manifest_path),
        "manifest_sha256": f"sha256:{hashlib.sha256(manifest_bytes).hexdigest()}",
        "lock": {"path": _display_path(lock_file), "frozen_at": lock["frozen_at"]} if lock else None,
        "parameters": {
            "range_minutes": list(RANGE_MINUTES),
            "delay_seconds": list(DELAY_SECONDS),
            "watch_minutes": WATCH_MINUTES,
            **{field: parameters[field] for field in COST_FIELDS},
            "signal": "closed M1 candles only; available at the next minute open",
            "execution": "Backtrader fill at the first M1 open at or after signal + delay",
            "delay_0": "idealized lower bound",
            "simulation": "M1 bars; no intrabar path",
        },
        "summary": summarize(cases),
        "cases": cases,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    for name, text in (("freeze", "Record independent case specifications"), ("run", "Run the batch")):
        command = commands.add_parser(name, help=text)
        command.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    commands.choices["run"].add_argument("--output", type=Path, help="Also write the JSON report here")
    args = parser.parse_args()
    try:
        if args.command == "freeze":
            result = freeze(args.manifest)
        else:
            result = run_batch(args.manifest)
        text = json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
        if args.command == "run" and args.output:
            args.output.write_text(text, encoding="utf-8")
        sys.stdout.write(text)
        return 0
    except FileExistsError:
        print("Batch failed: the manifest is already frozen; keep the existing lock", file=sys.stderr)
        return 1
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f"Batch failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
