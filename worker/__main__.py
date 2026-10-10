"""CLI: python -m worker collect --symbols AAPL,MSFT,NVDA --start 2024-01-02 --end 2024-01-06 --provider offline"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date

from worker.adapters.offline import OfflineAdapter
from worker.adapters.yfinance_adapter import YFinanceAdapter
from worker.config import WorkerSettings
from worker.pipeline import MarketDataPipeline
from worker.repository import QuoteRepository, create_engine
from worker.types import AdjustmentMode


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="AEGIS market-data collection worker")
    sub = p.add_subparsers(dest="command", required=True)

    collect = sub.add_parser("collect", help="Fetch and store daily OHLCV bars")
    collect.add_argument(
        "--symbols",
        default="AAPL,MSFT,NVDA",
        help="Comma-separated tickers",
    )
    collect.add_argument("--start", required=True, type=_parse_date)
    collect.add_argument(
        "--end",
        required=True,
        type=_parse_date,
        help="Exclusive end date (YYYY-MM-DD), same as yfinance",
    )
    collect.add_argument(
        "--provider",
        choices=("offline", "yfinance"),
        default="offline",
        help="Source adapter",
    )
    collect.add_argument(
        "--adjustment",
        choices=("auto_adjusted", "raw"),
        default="auto_adjusted",
    )
    collect.add_argument(
        "--interval",
        default="1d",
        help="Bar interval (only 1d supported in this stage)",
    )
    return p


async def _run_collect(args: argparse.Namespace) -> int:
    settings = WorkerSettings()
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    adjustment = AdjustmentMode(args.adjustment)

    if args.provider == "offline":
        adapter = OfflineAdapter()
    else:
        adapter = YFinanceAdapter()

    engine = create_engine(settings)
    repo = QuoteRepository(engine)
    pipeline = MarketDataPipeline(adapter, repo, settings)
    try:
        report = await pipeline.run(
            symbols=symbols,
            start=args.start,
            end=args.end,
            adjustment_mode=adjustment,
            interval=args.interval,
        )
    finally:
        await engine.dispose()

    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    return 0 if report.status.value != "FAILED" else 1


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "collect":
        return asyncio.run(_run_collect(args))
    parser.error(f"unknown command {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
