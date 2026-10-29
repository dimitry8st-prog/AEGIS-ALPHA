"""Export one stored quote series to CSV plus a manifest.

The snapshot is the current research database, not a vendor as-of history.
It reads PostgreSQL only and does not call any data provider.

Publication never touches an existing destination, including an empty one.
Each run writes into its own temporary directory, claims the destination
with an atomic ``mkdir`` and moves ``manifest.json`` in last. A directory
without ``manifest.json`` is not a finished snapshot.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from worker.russian_market import (
    RU_CURRENCY,
    RU_QUOTE_TIME_RULE,
    RU_SESSION_TIMEZONE,
    SUPPORTED_PROVIDERS,
)
from worker.session_calendar import session_days
from worker.types import SESSION_TIMEZONE, stored_price

FORMAT_VERSION = "2"
CSV_NAME = "quotes.csv"
MANIFEST_NAME = "manifest.json"
CSV_COLUMNS = (
    "provider",
    "symbol",
    "exchange",
    "interval",
    "session_date",
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
COMMON_LIMITATIONS = (
    "Это снимок текущей исследовательской базы, а не историческое состояние источника на дату сессии.",
    "Цены могли измениться из-за более поздних корректировок поставщика.",
)


@dataclass(frozen=True)
class MarketProfile:
    timezone: str
    currency: str
    quote_time_rule: str
    calendar: str | None
    limitations: tuple[str, ...]


US_PROFILE = MarketProfile(
    timezone=SESSION_TIMEZONE,
    currency="USD",
    quote_time_rule="official cash-session close: 16:00 America/New_York, 13:00 on scheduled early closes",
    calendar="us-equities-regular-sessions",
    limitations=(
        "Разовые закрытия биржи не входят в календарь и выглядят как пропуски.",
        "quote_time — официальное закрытие сессии в America/New_York.",
    ),
)
RU_PROFILE = MarketProfile(
    timezone=RU_SESSION_TIMEZONE,
    currency=RU_CURRENCY,
    quote_time_rule=RU_QUOTE_TIME_RULE,
    calendar=None,
    limitations=(
        "quote_time 23:59:59 Europe/Moscow — метка даты свечи, а не фактическое закрытие биржи.",
        "Проверенного календаря торгов Мосбиржи нет: пропуски не вычислялись, полнота покрытия неизвестна.",
    ),
)


def market_profile(provider: str) -> MarketProfile:
    if provider not in SUPPORTED_PROVIDERS:
        raise ExportError("INVALID_SELECTION", f"unsupported provider {provider!r}")
    return RU_PROFILE if provider in {"moex", "finam"} else US_PROFILE


class ExportError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


def render_csv(rows: list[dict]) -> str:
    lines: list[str] = []
    writer = csv.DictWriter(
        _CsvBuffer(lines),
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


def _local(value: datetime, zone: ZoneInfo) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(zone)


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
    profile = market_profile(provider)
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
        profile=profile,
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
        profile=profile,
    )
    publish(output_dir, csv_text, manifest)
    return manifest


async def _read_rows(engine, *, provider, adjustment_mode, symbols, start, end, profile) -> list[dict]:
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
          AND CAST(quote_time AT TIME ZONE :zone AS date) >= :start
          AND CAST(quote_time AT TIME ZONE :zone AS date) < :end
        ORDER BY symbol, quote_time, exchange
        """
    )
    params = {
        "provider": provider,
        "adjustment_mode": adjustment_mode,
        "zone": profile.timezone,
        "start": start,
        "end": end,
    }
    params.update({f"symbol_{index}": symbol for index, symbol in enumerate(symbols)})
    async with engine.connect() as conn:
        conn = await conn.execution_options(isolation_level="REPEATABLE READ")
        async with conn.begin():
            result = await conn.execute(query, params)
            stored = result.mappings().all()

    zone = ZoneInfo(profile.timezone)
    rendered = []
    for row in stored:
        if row["session_timezone"] != profile.timezone or row["currency"] != profile.currency:
            raise ExportError(
                "INCONSISTENT_ROWS",
                f"{provider} rows must use {profile.timezone} and {profile.currency}",
            )
        local = _local(row["quote_time"], zone)
        rendered.append(
            {
                "provider": row["provider"],
                "symbol": row["symbol"],
                "exchange": row["exchange"],
                "interval": row["interval"],
                "session_date": local.date().isoformat(),
                "quote_time": local.isoformat(timespec="seconds"),
                "open": _price(row["open"]),
                "high": _price(row["high"]),
                "low": _price(row["low"]),
                "close": _price(row["close"]),
                "volume": str(int(row["volume"])),
                "adjustment_mode": row["adjustment_mode"],
                "session_timezone": row["session_timezone"],
                "currency": row["currency"],
                "run_id": row["run_id"] or "",
            }
        )
    return rendered


def _manifest(*, rows, provider, adjustment_mode, symbols, start, end, digest, profile) -> dict:
    by_symbol: dict[str, list[str]] = {symbol: [] for symbol in symbols}
    for row in rows:
        by_symbol.setdefault(row["symbol"], []).append(row["session_date"])
    coverage = {
        symbol: {
            "rows": len(by_symbol[symbol]),
            "first": by_symbol[symbol][0] if by_symbol[symbol] else None,
            "last": by_symbol[symbol][-1] if by_symbol[symbol] else None,
        }
        for symbol in symbols
    }
    if profile.calendar is None:
        coverage_status = "UNKNOWN"
        gaps = None
    else:
        coverage_status = "CHECKED_AGAINST_CALENDAR"
        expected = [day.isoformat() for day in session_days(start, end)]
        gaps = [
            {"symbol": symbol, "session_date": day}
            for symbol in symbols
            for day in expected
            if day not in set(by_symbol[symbol])
        ]
    return {
        "format_version": FORMAT_VERSION,
        "status": "complete",
        "dataset_id": f"sha256:{digest}",
        "csv_sha256": digest,
        "csv_filename": CSV_NAME,
        "exported_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "app_commit": _app_commit(),
        "yfinance_version": _package_version("yfinance"),
        "selection": {
            "provider": provider,
            "adjustment_mode": adjustment_mode,
            "symbols": symbols,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "interval": "1d",
            "session_date_timezone": profile.timezone,
        },
        "time": {
            "timezone": profile.timezone,
            "session_date_rule": f"session_date is the calendar date of quote_time in {profile.timezone}",
            "quote_time_rule": profile.quote_time_rule,
            "calendar": profile.calendar,
        },
        "row_count": len(rows),
        "coverage": coverage,
        "coverage_status": coverage_status,
        "run_ids": sorted({row["run_id"] for row in rows if row["run_id"]}),
        "gaps": gaps,
        "limitations": [*COMMON_LIMITATIONS, *profile.limitations],
    }


def publish(output_dir: Path, csv_text: str, manifest: dict) -> None:
    output_dir = Path(output_dir)
    parent = output_dir.parent
    parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", suffix=".partial", dir=parent))
    claimed = False
    moved: list[Path] = []
    try:
        staged_csv = staging / CSV_NAME
        _write_synced(staged_csv, csv_text.encode("utf-8"))
        if hashlib.sha256(staged_csv.read_bytes()).hexdigest() != manifest["csv_sha256"]:
            raise ExportError("WRITE_MISMATCH", "written CSV does not match its digest")
        body = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        staged_manifest = staging / MANIFEST_NAME
        _write_synced(staged_manifest, body.encode("utf-8"))
        try:
            os.mkdir(output_dir)
        except FileExistsError as exc:
            raise ExportError("EXPORT_EXISTS", f"{output_dir} already exists") from exc
        claimed = True
        for name in (CSV_NAME, MANIFEST_NAME):
            os.replace(staging / name, output_dir / name)
            moved.append(output_dir / name)
    except BaseException:
        if claimed:
            for path in reversed(moved):
                path.unlink(missing_ok=True)
            try:
                os.rmdir(output_dir)
            except OSError:
                pass
        raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _write_synced(path: Path, data: bytes) -> None:
    with open(path, "xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def _package_version(name: str) -> str | None:
    try:
        return version(name)
    except PackageNotFoundError:
        return None


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
