"""Export one stored quote series to CSV plus a manifest.

The snapshot is the current research database, not a vendor as-of history.
It does not call Yahoo. A finished directory is never replaced in place.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import subprocess
from datetime import date, datetime, timezone
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from worker.session_calendar import SESSION_TZ, session_days
from worker.types import SESSION_TIMEZONE, stored_price

FORMAT_VERSION = "1"
CSV_NAME = "quotes.csv"
MANIFEST_NAME = "manifest.json"
CSV_COLUMNS = (
    "provider",
    "symbol",
    "exchange",
    "interval",
    "quote_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "adjustment_mode",
    "session_timezone",
    "currency",
    "run_id",
)
LIMITATIONS = (
    "Это снимок текущей исследовательской базы, а не историческое состояние источника на дату сессии.",
    "Цены могли измениться из-за более поздних корректировок поставщика.",
    "Разовые закрытия биржи не входят в календарь и выглядят как пропуски.",
    "quote_time — официальное закрытие сессии в America/New_York.",
)


class ExportError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


def render_csv(rows: list[dict]) -> str:
    lines: list[str] = []
    writer_buffer = _CsvBuffer(lines)
    writer = csv.DictWriter(
        writer_buffer,
        fieldnames=CSV_COLUMNS,
        lineterminator="\n",
        extrasaction="ignore",
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return "".join(lines)


def _price(value) -> str:
    return f"{stored_price(value):.8f}"


def _quote_time(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(SESSION_TZ).isoformat(timespec="seconds")


def _session_date(value: datetime) -> date:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(ZoneInfo(str(SESSION_TZ))).date()


async def export_quotes(
    engine: AsyncEngine,
    *,
    provider: str,
    adjustment_mode: str,
    symbols: list[str],
    start: date,
    end: date,
    output_dir: Path,
) -> dict:
    if end <= start:
        raise ExportError("INVALID_WINDOW", "end is exclusive and must be later than start")
    symbols = [symbol.strip().upper() for symbol in symbols if symbol.strip()]
    if not symbols or not provider or adjustment_mode not in {"raw", "auto_adjusted"}:
        raise ExportError("INVALID_SELECTION", "provider, adjustment_mode and symbols are required")
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise ExportError("EXPORT_EXISTS", f"{output_dir} already exists")

    rows = await _read_rows(
        engine,
        provider=provider,
        adjustment_mode=adjustment_mode,
        symbols=symbols,
        start=start,
        end=end,
    )
    if not rows:
        raise ExportError("EMPTY_SELECTION", "no stored quotes match this selection")

    csv_text = render_csv(rows)
    digest = hashlib.sha256(csv_text.encode("utf-8")).hexdigest()
    manifest = _manifest(
        rows=rows,
        provider=provider,
        adjustment_mode=adjustment_mode,
        symbols=symbols,
        start=start,
        end=end,
        digest=digest,
    )
    _publish(output_dir, csv_text, manifest)
    return manifest


async def _read_rows(engine, *, provider, adjustment_mode, symbols, start, end) -> list[dict]:
    placeholders = ", ".join(f":symbol_{index}" for index in range(len(symbols)))
    query = text(
        f"""
        SELECT provider, symbol, exchange, interval, quote_time,
               open, high, low, close, volume, adjustment_mode,
               session_timezone, currency, run_id::text AS run_id
        FROM market_quotes
        WHERE provider = :provider
          AND adjustment_mode = :adjustment_mode
          AND interval = '1d'
          AND symbol IN ({placeholders})
          AND CAST(quote_time AT TIME ZONE 'America/New_York' AS date) >= :start
          AND CAST(quote_time AT TIME ZONE 'America/New_York' AS date) < :end
        ORDER BY symbol, quote_time, exchange
        """
    )
    params = {
        "provider": provider,
        "adjustment_mode": adjustment_mode,
        "start": start,
        "end": end,
    }
    params.update({f"symbol_{index}": symbol for index, symbol in enumerate(symbols)})
    async with engine.connect() as conn:
        conn = await conn.execution_options(isolation_level="REPEATABLE READ")
        async with conn.begin():
            result = await conn.execute(query, params)
            stored = result.mappings().all()
    rendered = []
    for row in stored:
        rendered.append(
            {
                "provider": row["provider"],
                "symbol": row["symbol"],
                "exchange": row["exchange"],
                "interval": row["interval"],
                "quote_time": _quote_time(row["quote_time"]),
                "open": _price(row["open"]),
                "high": _price(row["high"]),
                "low": _price(row["low"]),
                "close": _price(row["close"]),
                "volume": str(int(row["volume"])),
                "adjustment_mode": row["adjustment_mode"],
                "session_timezone": row["session_timezone"],
                "currency": row["currency"],
                "run_id": row["run_id"] or "",
                "_session_date": _session_date(row["quote_time"]),
            }
        )
    return rendered


def _manifest(*, rows, provider, adjustment_mode, symbols, start, end, digest) -> dict:
    expected = session_days(start, end)
    by_symbol: dict[str, list[date]] = {symbol: [] for symbol in symbols}
    run_ids = sorted({row["run_id"] for row in rows if row["run_id"]})
    for row in rows:
        by_symbol.setdefault(row["symbol"], []).append(row["_session_date"])
    coverage = {}
    gaps = []
    for symbol in symbols:
        present = by_symbol.get(symbol, [])
        found = set(present)
        coverage[symbol] = {
            "rows": len(present),
            "first": None if not present else present[0].isoformat(),
            "last": None if not present else present[-1].isoformat(),
        }
        for day in expected:
            if day not in found:
                gaps.append({"symbol": symbol, "session_date": day.isoformat()})
    return {
        "format_version": FORMAT_VERSION,
        "status": "complete",
        "dataset_id": f"sha256:{digest}",
        "csv_sha256": digest,
        "csv_filename": CSV_NAME,
        "exported_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "app_commit": _app_commit(),
        "yfinance_version": version("yfinance"),
        "selection": {
            "provider": provider,
            "adjustment_mode": adjustment_mode,
            "symbols": symbols,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "interval": "1d",
            "quote_time_timezone": SESSION_TIMEZONE,
        },
        "row_count": len(rows),
        "coverage": coverage,
        "run_ids": run_ids,
        "gaps": gaps,
        "limitations": list(LIMITATIONS),
    }


def _publish(output_dir: Path, csv_text: str, manifest: dict) -> None:
    partial = output_dir.with_name(output_dir.name + ".partial")
    if partial.exists():
        shutil.rmtree(partial)
    partial.mkdir(parents=True)
    try:
        (partial / CSV_NAME).write_text(csv_text, encoding="utf-8", newline="\n")
        body = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        (partial / MANIFEST_NAME).write_text(body, encoding="utf-8", newline="\n")
        if output_dir.exists():
            raise ExportError("EXPORT_EXISTS", f"{output_dir} already exists")
        os.replace(partial, output_dir)
    except Exception:
        shutil.rmtree(partial, ignore_errors=True)
        if output_dir.exists() and not (output_dir / MANIFEST_NAME).exists():
            shutil.rmtree(output_dir, ignore_errors=True)
        raise


def _app_commit() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parents[1],
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return completed.stdout.strip() or "unknown"


class _CsvBuffer:
    def __init__(self, lines: list[str]):
        self._lines = lines

    def write(self, value: str) -> None:
        self._lines.append(value)
