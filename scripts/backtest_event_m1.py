"""Simulate AEGIS event-signal execution on a frozen M1 snapshot.

Reuses ``scripts.replay_event_signal.replay`` for the opening-range rule and
Backtrader for delayed next-open fills. The daily snapshot runner in
``scripts.backtest_snapshot`` stays unchanged.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import backtrader as bt

from scripts.replay_event_signal import EXAMPLE_DIR, RANGE_MINUTES, WATCH_MINUTES, parse_time, replay

MSK = timezone(timedelta(hours=3))
DEFAULT_BENCHMARKS = EXAMPLE_DIR / "benchmarks.json"
REPO_ROOT = Path(__file__).resolve().parents[1]


def _display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


@dataclass(frozen=True)
class MinuteBar:
    opened: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


def snapshot_id(path: Path, payload: bytes | None = None) -> str:
    data = path.read_bytes() if payload is None else payload
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


def load_minute_bars(symbol: str, rows: list[dict], start: datetime, end: datetime) -> list[MinuteBar]:
    bars = []
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    seen = set()
    for row in rows:
        if row.get("s") != symbol or row.get("i") != "1m":
            continue
        opened_ms = int(row["t"])
        if not start_ms <= opened_ms < end_ms:
            continue
        if opened_ms in seen:
            raise ValueError(f"duplicate M1 bar at {opened_ms}")
        seen.add(opened_ms)
        if int(row["T"]) != opened_ms + 59_999:
            raise ValueError(f"invalid M1 close stamp at {opened_ms}")
        prices = [float(Decimal(row[key])) for key in ("o", "h", "l", "c")]
        opening, high, low, close = prices
        volume = float(Decimal(row["v"]))
        if not all(map(math.isfinite, (*prices, volume))) or not (
            0 < low <= min(opening, close) <= max(opening, close) <= high and volume >= 0
        ):
            raise ValueError(f"invalid OHLCV at {opened_ms}")
        bars.append(MinuteBar(
            datetime.fromtimestamp(opened_ms / 1000, timezone.utc),
            opening, high, low, close, volume,
        ))
    bars.sort(key=lambda bar: bar.opened)
    return bars


class MinuteSnapshotFeed(bt.feed.DataBase):
    params = (("timeframe", bt.TimeFrame.Minutes), ("compression", 1))

    def __init__(self, bars: list[MinuteBar]):
        super().__init__()
        self._bars = iter(bars)

    def _load(self):
        bar = next(self._bars, None)
        if bar is None:
            return False
        self.lines.datetime[0] = bt.date2num(bar.opened)
        self.lines.open[0] = bar.open
        self.lines.high[0] = bar.high
        self.lines.low[0] = bar.low
        self.lines.close[0] = bar.close
        self.lines.volume[0] = bar.volume
        self.lines.openinterest[0] = 0
        return True


def adverse_price(side: str, price: float, spread_bps: float, slippage_bps: float) -> float:
    bump = spread_bps / 20_000 + slippage_bps / 10_000
    return price * (1 + bump) if side == "buy" else price * (1 - bump)


class EventBreakoutExecution(bt.Strategy):
    """Enter on the first open at/after the delayed signal; exit on the next open."""

    params = (
        ("direction", "SHORT"),
        ("available_at", None),
        ("delay_seconds", 0),
        ("invalidation", None),
        ("window_end", None),
        ("units", 1.0),
        ("spread_bps", 0.0),
        ("slippage_bps", 0.0),
        ("commission_bps", 0.0),
    )

    def __init__(self):
        self.fills = []
        self.failed_orders = 0
        self.pending = None
        self.entry_attempted = False
        self.exit_reason = None
        self.open_mark = None

    def _bar_time(self, index: int = 0) -> datetime:
        return bt.num2date(self.data.datetime[index], tz=timezone.utc, naive=False)

    def _earliest(self) -> datetime:
        return self.p.available_at + timedelta(seconds=self.p.delay_seconds)

    def next_open(self):
        now = self._bar_time()
        if self.pending is not None:
            return

        if self.position and self.exit_reason is not None:
            self.pending = self.close()
            self.pending.addinfo(role="exit", bar_open=now.isoformat(), reason=self.exit_reason)
            self.exit_reason = None
            return

        if not self.entry_attempted and self._earliest() <= now < self.p.window_end:
            self.entry_attempted = True
            self.pending = (
                self.sell(size=self.p.units) if self.p.direction == "SHORT" else self.buy(size=self.p.units)
            )
            self.pending.addinfo(role="entry", bar_open=now.isoformat(), reason="delayed_signal_open")

    def next(self):
        now = self._bar_time()
        if not self.position or now >= self.p.window_end:
            return

        invalidate = (
            self.data.close[0] >= self.p.invalidation
            if self.p.direction == "SHORT"
            else self.data.close[0] <= self.p.invalidation
        )
        if invalidate:
            self.exit_reason = "invalidation"
        elif now + timedelta(minutes=1) >= self.p.window_end:
            self.exit_reason = "window_end"

    def stop(self):
        if self.position:
            # The expiry candle is eligible for its open only.
            mark_index = -1 if self._bar_time() >= self.p.window_end else 0
            self.open_mark = {
                "direction": self.p.direction,
                "units": abs(float(self.position.size)),
                "avg_price": round(float(self.position.price), 8),
                "mark_close": round(float(self.data.close[mark_index]), 8),
                "bar_open": self._bar_time(mark_index).isoformat(),
                "exit_pending_reason": self.exit_reason,
                "note": "position remains open; no exit fill invented",
            }

    def notify_order(self, order):
        if order.alive():
            return
        role = getattr(order.info, "role", None)
        bar_open = getattr(order.info, "bar_open", None)
        reason = getattr(order.info, "reason", None)
        self.pending = None
        if order.status != order.Completed:
            self.failed_orders += 1
            self.fills.append({
                "status": "FAILED", "role": role, "bar_open": bar_open, "reason": reason,
            })
            return
        side = "buy" if order.isbuy() else "sell"
        execution_time = bt.num2date(order.executed.dt, tz=timezone.utc, naive=False).isoformat()
        raw = float(order.executed.price)
        adjusted = adverse_price(side, raw, self.p.spread_bps, self.p.slippage_bps)
        commission = abs(adjusted * float(order.executed.size)) * (self.p.commission_bps / 10_000)
        self.fills.append({
            "status": "FILLED",
            "role": role,
            "side": side,
            "units": abs(float(order.executed.size)),
            "signal_candle_close": self.p.available_at.isoformat(),
            "signal_available_at": self.p.available_at.isoformat(),
            "execution_time": execution_time,
            "bar_open": execution_time,
            "raw_open": round(raw, 8),
            "price_after_costs_model": round(adjusted, 8),
            "commission": round(commission, 8),
            "spread_bps": self.p.spread_bps,
            "slippage_bps": self.p.slippage_bps,
            "reason": reason,
        })


def signal_times(candidate: dict) -> tuple[datetime, datetime, datetime]:
    available = parse_time(candidate["available_at"])
    return available - timedelta(minutes=1), available, available


def realized_pnl(fills: list[dict], direction: str) -> float | None:
    entry = next((row for row in fills if row.get("role") == "entry" and row.get("status") == "FILLED"), None)
    exit_fill = next((row for row in fills if row.get("role") == "exit" and row.get("status") == "FILLED"), None)
    if entry is None or exit_fill is None:
        return None
    entry_px = entry["price_after_costs_model"]
    exit_px = exit_fill["price_after_costs_model"]
    units = entry["units"]
    gross = (entry_px - exit_px) * units if direction == "SHORT" else (exit_px - entry_px) * units
    costs = entry["commission"] + exit_fill["commission"]
    return round(gross - costs, 8)


def build_report(
    *,
    symbol: str,
    case: dict,
    signal_report: dict,
    bars: list[MinuteBar],
    dataset_id: str,
    candles_path: Path,
    benchmark: dict | None,
    delay_seconds: int,
    units: float,
    cash: float,
    commission_bps: float,
    spread_bps: float,
    slippage_bps: float,
    currency: str = "USD",
) -> dict:
    assumptions = {
        "range_minutes": RANGE_MINUTES,
        "watch_minutes": WATCH_MINUTES,
        "entry": "first minute open at or after signal_available_at + delay_seconds, before expiry",
        "exit": (
            "next minute open after an invalidating close; "
            "otherwise the open of the expiry minute when present"
        ),
        "commission_bps": commission_bps,
        "spread_bps": spread_bps,
        "slippage_bps": slippage_bps,
        "delay_seconds": delay_seconds,
        "no_invented_prices": True,
    }
    candidate = signal_report.get("candidate")
    report = {
        "engine": f"Backtrader {bt.__version__}",
        "strategy": "event_opening_range_breakout_m1",
        "dataset_id": dataset_id,
        "candles_path": _display_path(candles_path),
        "symbol": symbol,
        "event_source": {
            "id": case["id"],
            "event": case["event"],
            "watch_from": case["watch_from"],
            "sources": case.get("sources", []),
            "public_context": signal_report.get("public_context", []),
            "observation_start": signal_report["observation_start"],
            "expires_at": signal_report["expires_at"],
        },
        "benchmark": benchmark,
        "assumptions": assumptions,
        "signal": None,
        "executions": [],
        "open_position": None,
        "pnl_after_costs": None,
        "status": "NO_SIGNAL",
    }
    if candidate is None:
        report["pnl_after_costs"] = {
            "realized": 0.0, "currency": currency, "note": "no breakout candidate in the watch window",
        }
        return report

    candle_open, candle_close, available_at = signal_times(candidate)
    signal = {
        "direction": candidate["direction"],
        "signal_candle_open": candle_open.isoformat(),
        "signal_candle_close": candle_close.isoformat(),
        "signal_available_at": available_at.isoformat(),
        "trigger_level": candidate["trigger_level"],
        "observed_close": candidate["observed_close"],
        "invalidation_level": candidate["invalidation_level"],
        "breakout_volume_vs_opening_median": candidate.get("breakout_volume_vs_opening_median"),
    }
    if benchmark and benchmark.get("entry_at"):
        entry_at = parse_time(benchmark["entry_at"])
        signal["benchmark_entry_at"] = entry_at.isoformat()
        signal["benchmark_entry_at_msk"] = entry_at.astimezone(MSK).isoformat()
        signal["aegis_available_at_msk"] = available_at.astimezone(MSK).isoformat()
        signal["lag_seconds_vs_benchmark"] = int((available_at - entry_at).total_seconds())

    window_start = parse_time(signal_report["observation_start"])
    window_end = parse_time(signal_report["expires_at"])
    feed_bars = [bar for bar in bars if window_start <= bar.opened <= window_end]
    earliest = available_at + timedelta(seconds=delay_seconds)
    if not any(earliest <= bar.opened < window_end for bar in feed_bars):
        report.update({
            "signal": signal,
            "status": "MISSING_EXECUTION_BAR",
            "pnl_after_costs": {
                "realized": 0.0,
                "currency": currency,
                "note": "no minute open available for delayed entry; no fill price invented",
            },
        })
        return report

    engine = bt.Cerebro(stdstats=False, cheat_on_open=True)
    engine.adddata(MinuteSnapshotFeed(feed_bars), name=case["id"])
    engine.addstrategy(
        EventBreakoutExecution,
        direction=candidate["direction"],
        available_at=available_at,
        delay_seconds=delay_seconds,
        invalidation=float(candidate["invalidation_level"]),
        window_end=window_end,
        units=units,
        spread_bps=spread_bps,
        slippage_bps=slippage_bps,
        commission_bps=commission_bps,
    )
    engine.broker.setcash(cash)
    # Broker commission stays zero; costs are applied explicitly in the fill model.
    engine.broker.setcommission(commission=0.0, stocklike=True)
    strategy, = engine.run()
    pnl = realized_pnl(strategy.fills, candidate["direction"])
    status = "FILLED" if any(
        row.get("status") == "FILLED" and row.get("role") == "entry" for row in strategy.fills
    ) else "NO_FILL"
    report.update({
        "signal": signal,
        "status": status,
        "executions": strategy.fills,
        "open_position": strategy.open_mark,
        "pnl_after_costs": {
            "realized": pnl,
            "currency": currency,
            "cash_start": cash,
            "broker_cash_end": round(engine.broker.getcash(), 8),
            "broker_equity_end": round(engine.broker.getvalue(), 8),
            "failed_orders": strategy.failed_orders,
            "note": (
                "realized uses adverse spread/slippage prices and explicit commission; "
                "broker equity uses raw opens for cross-check only"
                if pnl is not None else
                "no completed entry/exit pair; open position reported separately when present"
            ),
        },
    })
    return report


def run_case(
    *,
    symbol: str,
    case: dict,
    rows: list[dict],
    dataset_id: str,
    candles_path: Path,
    benchmark: dict | None,
    delay_seconds: int,
    units: float,
    cash: float,
    commission_bps: float,
    spread_bps: float,
    slippage_bps: float,
    currency: str = "USD",
) -> dict:
    signal_report = replay(symbol, case, rows)
    window_start = parse_time(signal_report["observation_start"])
    window_end = parse_time(signal_report["expires_at"])
    bars = load_minute_bars(symbol, rows, window_start, window_end + timedelta(minutes=1))
    return build_report(
        symbol=symbol,
        case=case,
        signal_report=signal_report,
        bars=bars,
        dataset_id=dataset_id,
        candles_path=candles_path,
        benchmark=benchmark,
        delay_seconds=delay_seconds,
        units=units,
        cash=cash,
        commission_bps=commission_bps,
        spread_bps=spread_bps,
        slippage_bps=slippage_bps,
        currency=currency,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, default=EXAMPLE_DIR / "events.json")
    parser.add_argument("--candles", type=Path, default=EXAMPLE_DIR / "candles.json")
    parser.add_argument("--benchmarks", type=Path, default=DEFAULT_BENCHMARKS)
    parser.add_argument("--case-id", default="anthropic-ipo-first-print")
    parser.add_argument("--symbol", help="Override the instrument in the event file")
    parser.add_argument("--currency", choices=("USD", "RUB"), help="Currency of prices and starting cash")
    parser.add_argument("--delay-seconds", type=int, default=0)
    parser.add_argument("--units", type=float, default=1.0)
    parser.add_argument("--cash", type=float, default=10_000.0)
    parser.add_argument("--commission-bps", type=float, default=1.0)
    parser.add_argument("--spread-bps", type=float, default=2.0)
    parser.add_argument("--slippage-bps", type=float, default=1.0)
    args = parser.parse_args()
    try:
        if args.delay_seconds < 0 or args.units <= 0 or args.cash <= 0:
            raise ValueError("delay must be >= 0 and units/cash must be positive")
        if min(args.commission_bps, args.spread_bps, args.slippage_bps) < 0:
            raise ValueError("cost parameters must be non-negative")
        events = json.loads(args.events.read_text(encoding="utf-8"))
        symbol, cases = args.symbol or events["symbol"], events["cases"]
        is_finam = events.get("provider") == "finam"
        if is_finam:
            symbol = symbol.strip().upper()
        currency = args.currency or ("RUB" if is_finam else "USD")
        case = next(item for item in cases if item["id"] == args.case_id)
        candle_bytes = args.candles.read_bytes()
        rows = json.loads(candle_bytes.decode("utf-8"))
        dataset_id = snapshot_id(args.candles, candle_bytes)
        benchmarks = json.loads(args.benchmarks.read_text(encoding="utf-8"))["cases"]
        matched = [
            row for row in benchmarks
            if row.get("event_case_id") == args.case_id and row.get("instrument") == symbol
        ]
        primary = next((row for row in matched if row.get("trader") == "Sunlit Fathom"), None)
        secondary = next((row for row in matched if row.get("trader") == "Slate Brume"), None)
        report = run_case(
            symbol=symbol,
            case=case,
            rows=rows,
            dataset_id=dataset_id,
            candles_path=args.candles,
            benchmark=primary,
            delay_seconds=args.delay_seconds,
            units=args.units,
            cash=args.cash,
            commission_bps=args.commission_bps,
            spread_bps=args.spread_bps,
            slippage_bps=args.slippage_bps,
            currency=currency,
        )
        report["secondary_benchmark"] = secondary
        print(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False))
        return 0
    except (OSError, ValueError, KeyError, TypeError, StopIteration) as exc:
        print(f"Event M1 backtest failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
