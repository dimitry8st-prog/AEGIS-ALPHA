"""Run a reference long-only strategy on a frozen, daily AEGIS snapshot."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path

import backtrader as bt


@dataclass(frozen=True)
class DayBar:
    session: date
    open: float
    high: float
    low: float
    close: float
    volume: int


def read_snapshot(directory: Path, symbol: str) -> tuple[dict, str, list[DayBar]]:
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or (
        manifest.get("format_version"), manifest.get("status"), manifest.get("csv_filename")
    ) != ("2", "complete", "quotes.csv"):
        raise ValueError("a complete version 2 AEGIS snapshot is required")
    selection = manifest["selection"]
    if selection["interval"] != "1d" or symbol not in selection["symbols"]:
        raise ValueError(f"{symbol} must be present in a daily snapshot")

    data = (directory / "quotes.csv").read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if manifest.get("csv_sha256") != digest or manifest.get("dataset_id") != f"sha256:{digest}":
        raise ValueError("quotes.csv SHA-256 does not match the manifest")
    reader = csv.DictReader(io.StringIO(data.decode("utf-8"), newline=""))
    rows = list(reader)
    if len(rows) != manifest["row_count"]:
        raise ValueError("CSV row count does not match the manifest")

    start, end = date.fromisoformat(selection["start"]), date.fromisoformat(selection["end"])
    bars = []
    currencies = set()
    for row in rows:
        if row["symbol"] != symbol:
            continue
        if (row["provider"], row["interval"], row["adjustment_mode"]) != (
            selection["provider"], "1d", selection["adjustment_mode"]
        ):
            raise ValueError(f"CSV series metadata differs from the manifest for {symbol}")
        session = date.fromisoformat(row["session_date"])
        if not start <= session < end or (bars and session <= bars[-1].session):
            raise ValueError(f"duplicate, out-of-order, or out-of-window session for {symbol}: {session}")
        prices = [float(row[column]) for column in ("open", "high", "low", "close")]
        opening, high, low, close = prices
        volume = int(row["volume"])
        if not all(map(math.isfinite, prices)) or not (
            0 < low <= min(opening, close) <= max(opening, close) <= high and volume >= 0
        ):
            raise ValueError(f"invalid OHLCV for {symbol}: {session}")
        bars.append(DayBar(session, opening, high, low, close, volume))
        currencies.add(row["currency"])
    if not bars or len(currencies) != 1 or "" in currencies:
        raise ValueError(f"{symbol} must have daily bars in one currency")
    return manifest, currencies.pop(), bars


class DailySnapshotFeed(bt.feed.DataBase):
    params = (("timeframe", bt.TimeFrame.Days),)

    def __init__(self, bars):
        super().__init__()
        self.bars = iter(bars)

    def _load(self):
        bar = next(self.bars, None)
        if bar is None:
            return False
        self.lines.datetime[0] = bt.date2num(datetime.combine(bar.session, time.min))
        self.lines.open[0] = bar.open
        self.lines.high[0] = bar.high
        self.lines.low[0] = bar.low
        self.lines.close[0] = bar.close
        self.lines.volume[0] = bar.volume
        self.lines.openinterest[0] = 0
        return True


class SmaTrend(bt.Strategy):
    params = (("fast", 5), ("slow", 20), ("units", 1))

    def __init__(self):
        self.fast = bt.indicators.SMA(self.data.close, period=self.p.fast)
        self.slow = bt.indicators.SMA(self.data.close, period=self.p.slow)
        self.pending = None
        self.fills = []
        self.failed_orders = 0

    def next(self):
        if self.pending is not None:
            return
        if not self.position and self.fast[0] > self.slow[0]:
            self.pending = self.buy(size=self.p.units)
        elif self.position and self.fast[0] <= self.slow[0]:
            self.pending = self.close()
        if self.pending is not None:
            self.pending.addinfo(signal_session=self.data.datetime.date(0).isoformat())

    def notify_order(self, order):
        if order.alive():
            return
        self.pending = None
        if order.status != order.Completed:
            self.failed_orders += 1
            return
        self.fills.append({
            "side": "buy" if order.isbuy() else "sell",
            "units": abs(order.executed.size),
            "signal_session": order.info.signal_session,
            "fill_session": bt.num2date(order.executed.dt).date().isoformat(),
            "price": round(order.executed.price, 8),
            "commission": round(order.executed.comm, 8),
        })


def run_backtest(args: argparse.Namespace) -> dict:
    if not (0 < args.fast < args.slow and args.units > 0):
        raise ValueError("require 0 < fast < slow and positive units")
    if not (math.isfinite(args.cash) and args.cash > 0 and 0 <= args.commission_bps < 10_000):
        raise ValueError("require finite positive cash and commission in [0, 10000) bps")
    symbol = args.symbol.upper()
    manifest, currency, bars = read_snapshot(args.snapshot, symbol)
    if len(bars) <= args.slow:
        raise ValueError(f"{symbol} needs at least {args.slow + 1} bars for warm-up and execution")

    engine = bt.Cerebro(stdstats=False)
    engine.adddata(DailySnapshotFeed(bars), name=symbol)
    engine.addstrategy(SmaTrend, fast=args.fast, slow=args.slow, units=args.units)
    engine.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    engine.broker.setcash(args.cash)
    engine.broker.setcommission(commission=args.commission_bps / 10_000, stocklike=True)
    strategy, = engine.run()
    equity = engine.broker.getvalue()
    return {
        "engine": f"Backtrader {bt.__version__}",
        "strategy": "long_only_sma_trend",
        "dataset_id": manifest["dataset_id"],
        "provider": manifest["selection"]["provider"],
        "coverage_status": manifest["coverage_status"],
        "adjustment_mode": manifest["selection"]["adjustment_mode"],
        "symbol": symbol,
        "currency": currency,
        "first_session": bars[0].session.isoformat(),
        "last_session": bars[-1].session.isoformat(),
        "bars": len(bars),
        "fast": args.fast,
        "slow": args.slow,
        "units": args.units,
        "commission_bps": args.commission_bps,
        "cash_start": args.cash,
        "cash_end": round(engine.broker.getcash(), 8),
        "equity_end": round(equity, 8),
        "return_pct": round((equity / args.cash - 1) * 100, 8),
        "max_drawdown_pct": round(strategy.analyzers.drawdown.get_analysis().max.drawdown, 8),
        "open_units": strategy.position.size,
        "failed_orders": strategy.failed_orders,
        "pending_order_at_end": strategy.pending is not None,
        "fills": strategy.fills,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True, type=Path, help="directory containing quotes.csv and manifest.json")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--fast", type=int, default=5)
    parser.add_argument("--slow", type=int, default=20)
    parser.add_argument("--units", type=int, default=1)
    parser.add_argument("--cash", required=True, type=float, help="starting cash in the symbol's currency")
    parser.add_argument("--commission-bps", required=True, type=float, help="simulated commission per fill in basis points")
    args = parser.parse_args()
    try:
        report = run_backtest(args)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    print(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
