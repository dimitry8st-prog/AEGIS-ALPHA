"""CLI: python -m worker collect --symbols AAPL,MSFT,NVDA --start 2024-01-02 --end 2024-01-06 --provider offline

CachedAdapter is not constructed here. Worker Redis is not part of this command.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date, datetime
from pathlib import Path

from worker.adapters.offline import OfflineAdapter
from worker.adapters.yfinance_adapter import YFinanceAdapter
from worker.adapters.moex_adapter import MoexAdapter
from worker.adapters.finam_adapter import FinamAdapter
from worker.config import WorkerSettings
from worker.export import ExportError, export_quotes
from worker.finam_monitor import run_monitor
from worker.finam_schedule import run_finam_schedule
from worker.io_retry import PermanentIOError
from worker.ipo import search_ipos
from worker.pipeline import MarketDataPipeline
from worker.repository import QuoteRepository, create_engine
from worker.types import AdjustmentMode
from worker.russian_market import RU_WATCHLIST, SUPPORTED_PROVIDERS
from worker.portfolio import research_portfolio


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_month(value: str) -> str:
    try:
        normalized = datetime.strptime(value, "%Y-%m").strftime("%Y-%m")
    except ValueError:
        raise argparse.ArgumentTypeError("expected YYYY-MM") from None
    if value != normalized:
        raise argparse.ArgumentTypeError("expected YYYY-MM")
    return normalized


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="AEGIS market-data collection worker")
    sub = p.add_subparsers(dest="command", required=True)

    collect = sub.add_parser("collect", help="Fetch and store daily OHLCV bars")
    collect.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated tickers (US: AAPL,MSFT,NVDA; MOEX/Finam default: " + ",".join(RU_WATCHLIST) + ")",
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
        choices=SUPPORTED_PROVIDERS,
        default="offline",
        help="Source adapter",
    )
    collect.add_argument(
        "--adjustment",
        choices=("auto_adjusted", "raw"),
        default=None,
        help="Defaults to raw for MOEX/Finam, auto_adjusted for US providers",
    )
    collect.add_argument(
        "--interval",
        default="1d",
        help="Bar interval (only 1d supported in this stage)",
    )

    export = sub.add_parser("export", help="Write one stored quote series to CSV")
    export.add_argument("--provider", required=True, choices=SUPPORTED_PROVIDERS)
    export.add_argument("--adjustment", required=True, choices=("auto_adjusted", "raw"))
    export.add_argument("--symbols", required=True, help="Comma-separated tickers")
    export.add_argument("--start", required=True, type=_parse_date)
    export.add_argument("--end", required=True, type=_parse_date, help="Exclusive end date")
    export.add_argument("--output-dir", required=True, type=Path)

    sub.add_parser("portfolio", help="Show the MOEX research watchlist (no brokerage positions)")
    sub.add_parser("finam-daily", help="Collect Finam daily candles at 08:30 Moscow time until stopped")
    monitor = sub.add_parser(
        "finam-monitor",
        help="Watch registry tickers and IMOEX (QUOTES/BARS + REST M1 backfill) and Finam news until stopped",
    )
    monitor.add_argument("--status-file", type=Path, default=Path("data/finam-monitor.json"))
    monitor.add_argument("--news-interval", type=float, default=60.0, help="Seconds between news polls")
    ipo = sub.add_parser("ipo", help="Find companies in the MOEX and US IPO calendars")
    ipo.add_argument("--market", choices=("all", "moex", "us"), default="all")
    ipo.add_argument("--month", type=_parse_month, help="US calendar month YYYY-MM")
    ipo.add_argument("--query", help="Company name or ticker")
    return p


async def _run_collect(args: argparse.Namespace) -> int:
    settings = WorkerSettings()
    russian = args.provider in {"moex", "finam"}
    default_symbols = ",".join(RU_WATCHLIST) if russian else "AAPL,MSFT,NVDA"
    symbols = [s.strip().upper() for s in (args.symbols or default_symbols).split(",") if s.strip()]
    adjustment = AdjustmentMode(args.adjustment or ("raw" if russian else "auto_adjusted"))

    if args.provider == "offline":
        adapter = OfflineAdapter()
    elif args.provider == "yfinance":
        adapter = YFinanceAdapter()
    elif args.provider == "moex":
        adapter = MoexAdapter()
    else:
        token = settings.FINAM_API_TOKEN.get_secret_value() if settings.FINAM_API_TOKEN else None
        adapter = FinamAdapter(api_token=token)

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
        close = getattr(adapter, "aclose", None)
        if close is not None:
            await close()

    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    return 0 if report.status.value != "FAILED" else 1


async def _run_export(args: argparse.Namespace) -> int:
    settings = WorkerSettings()
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    engine = create_engine(settings)
    try:
        manifest = await export_quotes(
            engine,
            provider=args.provider,
            adjustment_mode=args.adjustment,
            symbols=symbols,
            start=args.start,
            end=args.end,
            output_dir=args.output_dir,
        )
    except ExportError as exc:
        print(json.dumps({"status": "FAILED", "code": exc.code, "message": exc.message}, ensure_ascii=False))
        return 2
    finally:
        await engine.dispose()
    print(json.dumps({"status": "complete", "dataset_id": manifest["dataset_id"], "row_count": manifest["row_count"]}, ensure_ascii=False))
    return 0


async def _run_finam_daily() -> int:
    settings = WorkerSettings()
    token = settings.FINAM_API_TOKEN.get_secret_value() if settings.FINAM_API_TOKEN else ""
    if not token.strip():
        print("FINAM_NOT_CONFIGURED: FINAM_API_TOKEN is required.", file=sys.stderr)
        return 2
    engine = create_engine(settings)
    try:
        await run_finam_schedule(MarketDataPipeline(FinamAdapter(token), QuoteRepository(engine), settings))
    finally:
        await engine.dispose()
    return 0


async def _run_finam_monitor(args: argparse.Namespace) -> int:
    settings = WorkerSettings()
    token = settings.FINAM_API_TOKEN.get_secret_value() if settings.FINAM_API_TOKEN else ""
    if not token.strip():
        print("FINAM_NOT_CONFIGURED: FINAM_API_TOKEN is required.", file=sys.stderr)
        return 2
    try:
        await run_monitor(FinamAdapter(token), status_file=args.status_file, news_interval=args.news_interval)
    except PermanentIOError as exc:
        print(f"{exc.code}: Finam monitor stopped.", file=sys.stderr)
        return 1
    return 0


async def _run_ipo(args: argparse.Namespace) -> int:
    report = await search_ipos(args.market, args.month, args.query)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "ok" else 1


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "collect":
        return asyncio.run(_run_collect(args))
    if args.command == "export":
        return asyncio.run(_run_export(args))
    if args.command == "portfolio":
        print(json.dumps(research_portfolio(), ensure_ascii=False, indent=2))
        return 0
    if args.command == "ipo":
        return asyncio.run(_run_ipo(args))
    if args.command == "finam-daily":
        try:
            return asyncio.run(_run_finam_daily())
        except KeyboardInterrupt:
            return 0
    if args.command == "finam-monitor":
        try:
            return asyncio.run(_run_finam_monitor(args))
        except KeyboardInterrupt:
            return 0
    parser.error(f"unknown command {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
