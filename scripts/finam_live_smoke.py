"""Strict Finam → PostgreSQL → re-collect → CSV/manifest acceptance.

Real broker calls require ``--live``. Without that flag the CLI does not
contact Finam and does not change the database. Mock verification is driven by
pytest with ``httpx.MockTransport`` and an explicitly supplied test database.

The API secret and the acceptance database URL come from the process
environment (``FINAM_API_TOKEN``, ``FINAM_ACCEPTANCE_DATABASE_URL``). They are
not CLI arguments. ``DATABASE_URL`` is not a fallback. This script does not
read ``.env``.

Exit 0 is allowed only after every acceptance stage passes. Ordinary
``python -m worker collect`` may still exit 0 on PARTIAL; this scenario does not.
Successful mock runs report ``MOCK_PASSED``, never ``LIVE_VERIFIED``.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable
from urllib.parse import unquote, urlparse

import httpx
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncEngine

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from worker.adapters.base import MarketDataAdapter
from worker.adapters.finam_adapter import FinamAdapter
from worker.config import WorkerSettings
from worker.export import CSV_NAME, MANIFEST_NAME, ExportError, export_quotes
from worker.pipeline import MarketDataPipeline
from worker.repository import QuoteRepository, create_engine
from worker.types import AdjustmentMode, FetchResult, QuoteBar, RunReport, RunStatus

DEFAULT_SYMBOLS = ("GAZP", "GMKN")
PROVIDER = "finam"
ADJUSTMENT = "raw"

STATUS_MOCK_PASSED = "MOCK_PASSED"
STATUS_LIVE_VERIFIED = "LIVE_VERIFIED"
STATUS_NOT_LIVE = "NOT_LIVE"
STATUS_NOT_CONFIGURED = "NOT_CONFIGURED"
STATUS_FAILED = "ACCEPTANCE_FAILED"

_SECRET_KEYS = re.compile(
    r"(authorization|api[_-]?token|password|secret|bearer|database_url|dsn)",
    re.IGNORECASE,
)
_URL_WITH_CREDS = re.compile(r"://[^/\s:@]+:[^/\s@]+@")
_BEARER = re.compile(r"Bearer\s+\S+", re.IGNORECASE)
ACCEPTANCE_DATABASE_ENV = "FINAM_ACCEPTANCE_DATABASE_URL"


@dataclass(frozen=True)
class StageResult:
    name: str
    ok: bool
    detail: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "ok": self.ok, "detail": self.detail}


def redact(value: Any, secrets: tuple[str, ...] = ()) -> Any:
    """Remove secrets, auth headers and password-bearing URLs from diagnostics."""
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            if _SECRET_KEYS.search(str(key)):
                out[str(key)] = "[redacted]"
            else:
                out[str(key)] = redact(item, secrets)
        return out
    if isinstance(value, list):
        return [redact(item, secrets) for item in value]
    if isinstance(value, tuple):
        return tuple(redact(item, secrets) for item in value)
    if isinstance(value, str):
        text_value = value
        for secret in secrets:
            if secret and secret in text_value:
                text_value = text_value.replace(secret, "[redacted]")
        text_value = _URL_WITH_CREDS.sub("://[redacted]@", text_value)
        text_value = _BEARER.sub("Bearer [redacted]", text_value)
        return text_value
    return value


def acceptance_exit_code(report: dict[str, Any]) -> int:
    """0 only when every acceptance stage passed."""
    if report.get("status") in {STATUS_MOCK_PASSED, STATUS_LIVE_VERIFIED}:
        return 0
    return 2


class _SecretLogFilter(logging.Filter):
    def __init__(self, secrets: tuple[str, ...]):
        super().__init__()
        self._secrets = tuple(item for item in secrets if item)

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            message = str(record.msg)
        record.msg = redact(message, self._secrets)
        record.args = ()
        return True


class _LogGuard:
    """Scrub secrets from library logs for the duration of one CLI run.

    A filter on the root logger does not see records emitted by child loggers.
    The same filter is therefore attached to handlers that already exist
    (including the test capture handler) before any broker or database call.
    """

    def __init__(self, secrets: tuple[str, ...]):
        self._filter = _SecretLogFilter(secrets)
        self._previous: list[tuple[logging.Logger, int]] = []
        self._handlers: list[logging.Handler] = []

    def install(self) -> "_LogGuard":
        names = ("", "httpx", "httpcore", "sqlalchemy", "sqlalchemy.engine", "asyncpg")
        seen_handlers: set[int] = set()
        for name in names:
            logger = logging.getLogger() if name == "" else logging.getLogger(name)
            self._previous.append((logger, logger.level))
            logger.addFilter(self._filter)
            for handler in logger.handlers:
                if id(handler) in seen_handlers:
                    continue
                handler.addFilter(self._filter)
                self._handlers.append(handler)
                seen_handlers.add(id(handler))
            if name:
                logger.setLevel(logging.WARNING)
        return self

    def close(self) -> None:
        for logger, level in self._previous:
            logger.removeFilter(self._filter)
            logger.setLevel(level)
        for handler in self._handlers:
            handler.removeFilter(self._filter)
        self._previous.clear()
        self._handlers.clear()


def safe_db_label(database_url: str) -> str:
    """Host/db label without userinfo."""
    parsed = urlparse(database_url)
    host = parsed.hostname or "unknown"
    port = f":{parsed.port}" if parsed.port else ""
    path = parsed.path or ""
    return f"{host}{port}{path}"


def _url_secrets(database_url: str | None) -> tuple[str, ...]:
    if not database_url:
        return ()
    found = [database_url]
    password = urlparse(database_url).password
    if password and len(password) >= 8:
        found.append(password)
        decoded = unquote(password)
        if decoded != password and len(decoded) >= 8:
            found.append(decoded)
    return tuple(found)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_symbols(value: str | None) -> list[str]:
    raw = value or ",".join(DEFAULT_SYMBOLS)
    symbols = [item.strip().upper() for item in raw.split(",") if item.strip()]
    if symbols != list(DEFAULT_SYMBOLS):
        raise ValueError(
            f"acceptance symbols must be exactly {','.join(DEFAULT_SYMBOLS)}"
        )
    return symbols


async def count_quotes(
    engine: AsyncEngine,
    *,
    symbols: list[str],
    start: date,
    end: date,
) -> dict[str, int]:
    placeholders = ", ".join(f":symbol_{index}" for index in range(len(symbols)))
    params: dict[str, Any] = {
        "provider": PROVIDER,
        "adjustment": ADJUSTMENT,
        "start": start,
        "end": end,
        "zone": "Europe/Moscow",
    }
    params.update({f"symbol_{index}": symbol for index, symbol in enumerate(symbols)})
    query = text(
        f"""
        SELECT symbol, COUNT(*)::int AS n
        FROM market_quotes
        WHERE provider = :provider
          AND adjustment_mode = :adjustment
          AND interval = '1d'
          AND symbol IN ({placeholders})
          AND CAST(quote_time AT TIME ZONE :zone AS date) >= :start
          AND CAST(quote_time AT TIME ZONE :zone AS date) < :end
        GROUP BY symbol
        """
    )
    async with engine.connect() as conn:
        rows = (await conn.execute(query, params)).mappings().all()
    counts = {symbol: 0 for symbol in symbols}
    for row in rows:
        counts[str(row["symbol"])] = int(row["n"])
    return counts


async def count_revisions(engine: AsyncEngine) -> int:
    async with engine.connect() as conn:
        value = await conn.scalar(text("SELECT COUNT(*)::int FROM market_quote_revisions"))
    return int(value or 0)


def _counters_consistent(report: RunReport) -> bool:
    counters = report.counters
    confirmed = counters.inserted + counters.updated + counters.duplicates
    accounted = confirmed + counters.quarantined + counters.failed
    return counters.received == accounted


def _summarize_collect(report: RunReport) -> dict[str, Any]:
    return {
        "status": report.status.value,
        "run_id": str(report.run_id),
        "counters": {
            "received": report.counters.received,
            "inserted": report.counters.inserted,
            "updated": report.counters.updated,
            "duplicates": report.counters.duplicates,
            "quarantined": report.counters.quarantined,
            "failed": report.counters.failed,
        },
        "per_symbol": report.per_symbol,
        "errors": report.errors,
        "notes": report.notes,
        "counters_consistent": _counters_consistent(report),
    }


def verify_export_artifacts(output_dir: Path) -> dict[str, Any]:
    csv_path = output_dir / CSV_NAME
    manifest_path = output_dir / MANIFEST_NAME
    if not csv_path.is_file() or not manifest_path.is_file():
        return {
            "ok": False,
            "reason": "CSV_OR_MANIFEST_MISSING",
            "csv_exists": csv_path.is_file(),
            "manifest_exists": manifest_path.is_file(),
        }
    csv_bytes = csv_path.read_bytes()
    digest = hashlib.sha256(csv_bytes).hexdigest()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    row_count = max(len(csv_bytes.decode("utf-8").splitlines()) - 1, 0)
    symbols_in_csv = {
        line.split(",", 2)[1]
        for line in csv_bytes.decode("utf-8").splitlines()[1:]
        if line.strip()
    }
    selection = manifest.get("selection") or {}
    checks = {
        "sha256_match": digest == manifest.get("csv_sha256"),
        "row_count_match": row_count == manifest.get("row_count"),
        "provider": selection.get("provider") == PROVIDER,
        "adjustment_mode": selection.get("adjustment_mode") == ADJUSTMENT,
        "symbols_both": set(DEFAULT_SYMBOLS).issubset(symbols_in_csv),
        "dataset_id": manifest.get("dataset_id") == f"sha256:{digest}",
        "coverage_status_unknown": manifest.get("coverage_status") == "UNKNOWN",
    }
    return {
        "ok": all(checks.values()),
        "checks": checks,
        "csv_sha256": digest,
        "manifest_csv_sha256": manifest.get("csv_sha256"),
        "row_count": row_count,
        "manifest_row_count": manifest.get("row_count"),
        "symbols_in_csv": sorted(symbols_in_csv),
        "coverage_status": manifest.get("coverage_status"),
        "calendar_completeness_proven": False,
        "note": (
            "Both tickers have rows; Russian session-calendar completeness "
            "is not proven (coverage_status=UNKNOWN)."
        ),
    }


class _RecordingAdapter(MarketDataAdapter):
    """Keep the actual source bars for each pass independently of stored rows."""

    def __init__(self, adapter: MarketDataAdapter) -> None:
        self.adapter = adapter
        self.provider_name = adapter.provider_name
        self.bars: dict[str, frozenset[QuoteBar]] = {}

    async def fetch_daily(
        self, symbol: str, start: date, end: date, adjustment_mode: AdjustmentMode
    ) -> FetchResult:
        result = await self.adapter.fetch_daily(symbol, start, end, adjustment_mode)
        self.bars[symbol] = frozenset(result.bars)
        return result


async def run_acceptance_chain(
    *,
    adapter: FinamAdapter,
    engine: AsyncEngine,
    symbols: list[str],
    start: date,
    end: date,
    output_dir: Path,
    mode: str,
    settings: WorkerSettings | None = None,
    secrets: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Run the strict acceptance chain using an injected adapter and engine.

    Does not wipe the database. ``mode`` is ``mock`` or ``live`` and only
    affects the final status label on success.
    """
    if symbols != list(DEFAULT_SYMBOLS):
        raise ValueError(f"acceptance symbols must be exactly {','.join(DEFAULT_SYMBOLS)}")
    if end <= start:
        raise ValueError("end must be after start (exclusive end)")

    worker_settings = settings or WorkerSettings(
        DATABASE_URL="postgresql://127.0.0.1:1/unused",
        IO_RETRY_WAIT_SECONDS=0.01,
        IO_MAX_ATTEMPTS=3,
        RUN_DEADLINE_SECONDS=120,
        FINAM_API_TOKEN=None,
    )
    recorded = _RecordingAdapter(adapter)
    pipeline = MarketDataPipeline(recorded, QuoteRepository(engine), worker_settings)
    stages: list[StageResult] = []
    failure_reason: str | None = None
    collect_options = dict(
        symbols=symbols,
        start=start,
        end=end,
        adjustment_mode=AdjustmentMode.RAW,
        interval="1d",
    )

    quotes_before = await count_quotes(engine, symbols=symbols, start=start, end=end)
    revisions_before = await count_revisions(engine)

    first = await pipeline.run(**collect_options)
    first_summary = _summarize_collect(first)
    quotes_after_first = await count_quotes(engine, symbols=symbols, start=start, end=end)
    both_present = all(quotes_after_first.get(symbol, 0) > 0 for symbol in symbols)
    first_ok = (
        first.status is RunStatus.READY
        and first.provider == PROVIDER
        and first.adjustment_mode == ADJUSTMENT
        and not first.errors
        and first.counters.quarantined == 0
        and first.counters.failed == 0
        and _counters_consistent(first)
        and all(first.per_symbol.get(symbol, {}).get("status") == "OK" for symbol in symbols)
        and all(int(first.per_symbol.get(symbol, {}).get("bars") or 0) > 0 for symbol in symbols)
        and both_present
    )
    if first.status is RunStatus.PARTIAL:
        failure_reason = failure_reason or "FIRST_COLLECT_PARTIAL"
    elif first.status is RunStatus.FAILED:
        failure_reason = failure_reason or "FIRST_COLLECT_FAILED"
    elif not both_present:
        missing = [symbol for symbol in symbols if quotes_after_first.get(symbol, 0) == 0]
        failure_reason = failure_reason or f"MISSING_TICKER_DATA:{','.join(missing)}"
    elif not first_ok:
        failure_reason = failure_reason or "FIRST_COLLECT_NOT_READY"
    stages.append(
        StageResult(
            "first_collect_ready",
            first_ok,
            {
                **first_summary,
                "quotes_before": quotes_before,
                "quotes_after": quotes_after_first,
            },
        )
    )

    per_ticker_ok = first_ok
    stages.append(
        StageResult(
            "both_tickers_saved",
            per_ticker_ok,
            {
                "quotes": quotes_after_first,
                "required_symbols": list(symbols),
                "calendar_completeness_proven": False,
            },
        )
    )

    clean_adapters = first_ok
    stages.append(
        StageResult(
            "no_adapter_errors_or_rejects",
            clean_adapters,
            {
                "errors": first.errors,
                "quarantined": first.counters.quarantined,
                "failed": first.counters.failed,
                "counters_consistent": _counters_consistent(first),
            },
        )
    )

    revisions_mid = await count_revisions(engine)
    first_source_bars = recorded.bars.copy()
    recorded.bars.clear()
    second = await pipeline.run(**collect_options)
    source_bars_match = first_source_bars == recorded.bars
    second_summary = _summarize_collect(second)
    quotes_after_second = await count_quotes(engine, symbols=symbols, start=start, end=end)
    revisions_after = await count_revisions(engine)
    repeat_ok = (
        first_ok
        and second.status is RunStatus.READY
        and not second.errors
        and second.counters.quarantined == 0
        and second.counters.failed == 0
        and second.counters.inserted == 0
        and second.counters.updated == 0
        and second.counters.duplicates == second.counters.received
        and second.counters.received > 0
        and source_bars_match
        and _counters_consistent(second)
        and quotes_after_second == quotes_after_first
        and revisions_after == revisions_mid
    )
    if first_ok and not repeat_ok:
        failure_reason = failure_reason or (
            "REPEAT_SOURCE_BARS_CHANGED"
            if second.status is RunStatus.READY and not source_bars_match
            else "REPEAT_COLLECT_NOT_IDEMPOTENT"
        )
    stages.append(
        StageResult(
            "repeat_collect_duplicates_only",
            repeat_ok,
            {
                **second_summary,
                "source_bars_match": source_bars_match,
                "quotes_after": quotes_after_second,
                "revisions_before_repeat": revisions_mid,
                "revisions_after_repeat": revisions_after,
                "revisions_before_first": revisions_before,
            },
        )
    )

    export_one: dict[str, Any] | None = None
    export_two: dict[str, Any] | None = None
    verify_one: dict[str, Any] = {"ok": False, "reason": "SKIPPED"}
    verify_two: dict[str, Any] = {"ok": False, "reason": "SKIPPED"}
    export_dirs_ok = False
    identical_exports = False
    export_exists_ok = False
    first_dir = Path(output_dir)
    second_dir = Path(str(output_dir) + "-repeat")
    first_csv_before_conflict: bytes | None = None
    first_manifest_before_conflict: bytes | None = None

    if repeat_ok:
        try:
            export_one = await export_quotes(
                engine,
                provider=PROVIDER,
                adjustment_mode=ADJUSTMENT,
                symbols=symbols,
                start=start,
                end=end,
                output_dir=first_dir,
            )
            verify_one = verify_export_artifacts(first_dir)
            export_two = await export_quotes(
                engine,
                provider=PROVIDER,
                adjustment_mode=ADJUSTMENT,
                symbols=symbols,
                start=start,
                end=end,
                output_dir=second_dir,
            )
            verify_two = verify_export_artifacts(second_dir)
            csv_one = (first_dir / CSV_NAME).read_bytes()
            csv_two = (second_dir / CSV_NAME).read_bytes()
            identical_exports = (
                csv_one == csv_two
                and export_one["csv_sha256"] == export_two["csv_sha256"]
                and verify_one.get("csv_sha256") == verify_two.get("csv_sha256")
            )
            export_dirs_ok = bool(verify_one.get("ok") and verify_two.get("ok"))
            first_csv_before_conflict = csv_one
            first_manifest_before_conflict = (first_dir / MANIFEST_NAME).read_bytes()
            try:
                await export_quotes(
                    engine,
                    provider=PROVIDER,
                    adjustment_mode=ADJUSTMENT,
                    symbols=symbols,
                    start=start,
                    end=end,
                    output_dir=first_dir,
                )
                export_exists_ok = False
                failure_reason = failure_reason or "EXPORT_EXISTS_NOT_RAISED"
            except ExportError as exc:
                export_exists_ok = (
                    exc.code == "EXPORT_EXISTS"
                    and (first_dir / CSV_NAME).read_bytes() == first_csv_before_conflict
                    and (first_dir / MANIFEST_NAME).read_bytes()
                    == first_manifest_before_conflict
                )
                if not export_exists_ok:
                    failure_reason = failure_reason or "EXPORT_EXISTS_MUTATED_TARGET"
        except ExportError as exc:
            failure_reason = failure_reason or f"EXPORT_ERROR:{exc.code}"
            export_one = {"code": exc.code, "message": exc.message}
        except Exception as exc:  # noqa: BLE001 — surfaced in report, redacted below
            failure_reason = failure_reason or f"EXPORT_EXCEPTION:{type(exc).__name__}"
            export_one = {"error_type": type(exc).__name__, "message": str(exc)[:300]}
    else:
        failure_reason = failure_reason or "EXPORT_SKIPPED_AFTER_COLLECT_FAILURE"

    stages.append(
        StageResult(
            "export_selection",
            bool(export_dirs_ok and export_one and export_two),
            {
                "first": {
                    "dataset_id": None if export_one is None else export_one.get("dataset_id"),
                    "row_count": None if export_one is None else export_one.get("row_count"),
                    "csv_sha256": None if export_one is None else export_one.get("csv_sha256"),
                    "selection": None if export_one is None else export_one.get("selection"),
                },
                "second": {
                    "dataset_id": None if export_two is None else export_two.get("dataset_id"),
                    "row_count": None if export_two is None else export_two.get("row_count"),
                    "csv_sha256": None if export_two is None else export_two.get("csv_sha256"),
                },
            },
        )
    )
    stages.append(
        StageResult(
            "csv_matches_manifest",
            bool(verify_one.get("ok") and verify_two.get("ok")),
            {"first": verify_one, "second": verify_two},
        )
    )
    stages.append(
        StageResult(
            "identical_exports",
            identical_exports,
            {
                "sha256_equal": (
                    None
                    if export_one is None or export_two is None
                    else export_one.get("csv_sha256") == export_two.get("csv_sha256")
                ),
                "first_dir": str(first_dir),
                "second_dir": str(second_dir),
            },
        )
    )
    stages.append(
        StageResult(
            "export_exists_preserves_target",
            export_exists_ok,
            {
                "code": "EXPORT_EXISTS" if export_exists_ok else None,
                "target": str(first_dir),
            },
        )
    )

    all_ok = all(stage.ok for stage in stages)
    if all_ok:
        status = STATUS_LIVE_VERIFIED if mode == "live" else STATUS_MOCK_PASSED
        failure_reason = None
    else:
        status = STATUS_FAILED
        failure_reason = failure_reason or "STAGE_FAILED"

    report = {
        "mode": mode,
        "status": status,
        "provider": PROVIDER,
        "symbols": list(symbols),
        "adjustment_mode": ADJUSTMENT,
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "stages": [stage.to_dict() for stage in stages],
        "ticker_counts": quotes_after_second if repeat_ok else quotes_after_first,
        "repeat_collect": second_summary,
        "csv_manifest_checks": {"first": verify_one, "second": verify_two},
        "failure_reason": failure_reason,
        "limitations": [
            "Russian trading-calendar completeness is not proven.",
            "coverage_status remains UNKNOWN when a calendar is absent.",
            "Presence of both tickers is not proof of no missing session minutes or days.",
        ],
    }
    return redact(report, secrets)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Strict Finam acceptance: collect → repeat → export CSV/manifest. "
            "Real broker access requires --live. "
            f"FINAM_API_TOKEN and {ACCEPTANCE_DATABASE_ENV} belong in the process "
            "environment, never in arguments. DATABASE_URL is not read."
        )
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help=(
            "Contact Finam and write to the database named by "
            f"{ACCEPTANCE_DATABASE_ENV}. Do not pass the token or URL as arguments."
        ),
    )
    parser.add_argument("--start", type=_parse_date, required=True, help="Inclusive start YYYY-MM-DD")
    parser.add_argument("--end", type=_parse_date, required=True, help="Exclusive end YYYY-MM-DD")
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Destination directory for the first export (must not already exist)",
    )
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Must be GAZP,GMKN",
    )
    return parser


def _env_text(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _token_from_env() -> str | None:
    return _env_text("FINAM_API_TOKEN")


def _acceptance_database_url() -> str | None:
    """Separate test database. Never falls back to DATABASE_URL or .env."""
    return _env_text(ACCEPTANCE_DATABASE_ENV)


def _database_target(database_url: str) -> tuple[str, int, str]:
    url = make_url(database_url)
    if url.get_backend_name() != "postgresql" or not url.database:
        raise ValueError("An explicit PostgreSQL database is required")
    host = (url.host or "localhost").lower().rstrip(".")
    if host in {"localhost", "127.0.0.1", "::1"}:
        host = "localhost"
    return host, url.port or 5432, url.database


def _known_secrets() -> tuple[str, ...]:
    parts: list[str] = []
    token = _token_from_env()
    if token:
        parts.append(token)
    parts.extend(_url_secrets(_acceptance_database_url()))
    # Scrub the working URL if a library logs it, without using it as a connection.
    parts.extend(_url_secrets(_env_text("DATABASE_URL")))
    unique: list[str] = []
    for item in parts:
        if item and item not in unique:
            unique.append(item)
    return tuple(unique)


async def async_main(argv: list[str] | None = None) -> tuple[int, dict[str, Any]]:
    guard = _LogGuard(_known_secrets()).install()
    try:
        return await _async_main(argv)
    finally:
        guard.close()


async def _async_main(argv: list[str] | None) -> tuple[int, dict[str, Any]]:
    parser = build_parser()
    args = parser.parse_args(argv)
    window = {"start": args.start.isoformat(), "end": args.end.isoformat()}

    if not args.live:
        report = {
            "mode": "dry",
            "status": STATUS_NOT_LIVE,
            "provider": PROVIDER,
            "symbols": args.symbols,
            "adjustment_mode": ADJUSTMENT,
            "window": window,
            "output_dir": str(args.output_dir),
            "stages": [],
            "failure_reason": (
                "Real Finam calls require --live. Without it this scenario does "
                "not contact the broker and does not change the database."
            ),
        }
        return acceptance_exit_code(report), redact(report, _known_secrets())

    try:
        symbols = _parse_symbols(args.symbols)
    except ValueError as exc:
        report = {
            "mode": "live",
            "status": STATUS_FAILED,
            "provider": PROVIDER,
            "symbols": args.symbols,
            "adjustment_mode": ADJUSTMENT,
            "window": window,
            "stages": [],
            "failure_reason": str(exc),
        }
        return acceptance_exit_code(report), redact(report, _known_secrets())

    base = {
        "provider": PROVIDER,
        "symbols": symbols,
        "adjustment_mode": ADJUSTMENT,
        "window": window,
        "output_dir": str(args.output_dir),
        "stages": [],
        "ticker_counts": {},
        "repeat_collect": None,
        "csv_manifest_checks": {},
        "limitations": [
            "Russian trading-calendar completeness is not proven.",
            "Successful mock tests never set LIVE_VERIFIED.",
        ],
    }

    if args.end <= args.start:
        report = {
            **base,
            "mode": "live",
            "status": STATUS_FAILED,
            "failure_reason": "end must be after start (exclusive end)",
        }
        return acceptance_exit_code(report), redact(report, _known_secrets())

    token = _token_from_env()
    if not token:
        report = {
            **base,
            "mode": "live",
            "status": STATUS_NOT_CONFIGURED,
            "failure_reason": "FINAM_API_TOKEN is missing or empty in the process environment",
        }
        return acceptance_exit_code(report), redact(report, _known_secrets())

    database_url = _acceptance_database_url()
    if not database_url:
        report = {
            **base,
            "mode": "live",
            "status": STATUS_NOT_CONFIGURED,
            "failure_reason": (
                f"{ACCEPTANCE_DATABASE_ENV} is required for a separate test database; "
                "DATABASE_URL is not used as an implicit fallback"
            ),
        }
        return acceptance_exit_code(report), redact(report, _known_secrets())

    working_url = _env_text("DATABASE_URL")
    try:
        acceptance_target = _database_target(database_url)
        working_target = _database_target(working_url) if working_url else None
    except (ArgumentError, ValueError):
        report = {
            **base,
            "mode": "live",
            "status": STATUS_NOT_CONFIGURED,
            "failure_reason": "Invalid PostgreSQL database configuration",
        }
        return acceptance_exit_code(report), redact(report, _known_secrets())
    if acceptance_target == working_target:
        report = {
            **base,
            "mode": "live",
            "status": STATUS_NOT_CONFIGURED,
            "failure_reason": "ACCEPTANCE_DATABASE_MATCHES_WORKING_DATABASE",
        }
        return acceptance_exit_code(report), redact(report, _known_secrets())

    # Init arguments win over the process environment, including DATABASE_URL.
    settings = WorkerSettings(
        _env_file=None,
        DATABASE_URL=database_url,
        IO_RETRY_WAIT_SECONDS=0.5,
        FINAM_API_TOKEN=None,
    )
    if settings.DATABASE_URL != database_url:
        report = {
            **base,
            "mode": "live",
            "status": STATUS_FAILED,
            "failure_reason": f"{ACCEPTANCE_DATABASE_ENV} was not applied to the worker settings",
        }
        return acceptance_exit_code(report), redact(report, _known_secrets())

    secrets = _known_secrets()
    engine = create_engine(settings)
    try:
        adapter = FinamAdapter(api_token=token)
        try:
            report = await run_acceptance_chain(
                adapter=adapter,
                engine=engine,
                symbols=symbols,
                start=args.start,
                end=args.end,
                output_dir=args.output_dir,
                mode="live",
                settings=settings,
                secrets=secrets,
            )
            report["database"] = safe_db_label(database_url)
            report = redact(report, secrets)
        finally:
            close = getattr(adapter, "aclose", None)
            if close is not None:
                await close()
    finally:
        await engine.dispose()

    return acceptance_exit_code(report), report


def main(argv: list[str] | None = None) -> int:
    try:
        code, report = asyncio.run(async_main(argv))
    except Exception as exc:  # noqa: BLE001 — CLI must never leak secrets
        failure = redact(
            {
                "mode": "live" if (argv and "--live" in argv) else "dry",
                "status": STATUS_FAILED,
                "provider": PROVIDER,
                "failure_reason": f"{type(exc).__name__}: {exc}"[:500],
            },
            secrets=_known_secrets(),
        )
        print(json.dumps(failure, ensure_ascii=False, indent=2))
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return code


# Optional hook used by tests to build a MockTransport-backed adapter without
# duplicating Finam HTTP contract details in the CLI.
def mock_finam_client(
    handler: Callable[[httpx.Request], httpx.Response],
) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


if __name__ == "__main__":
    raise SystemExit(main())
