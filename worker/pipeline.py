"""Orchestrate fetch → validate → persist → report for one manual collection run."""

from __future__ import annotations

import time
from datetime import date, datetime, timezone
from typing import Any
from uuid import UUID

from worker.adapters.base import MarketDataAdapter
from worker.config import WorkerSettings
from worker.io_retry import PermanentIOError, TransientIOError, with_retries
from worker.repository import QuoteRepository
from worker.types import (
    AdjustmentMode,
    FetchStatus,
    PersistCounters,
    QuarantineRow,
    RunReport,
    RunStatus,
)
from worker.validation import validate_bar


class MarketDataPipeline:
    def __init__(
        self,
        adapter: MarketDataAdapter,
        repo: QuoteRepository,
        settings: WorkerSettings,
    ) -> None:
        self.adapter = adapter
        self.repo = repo
        self.settings = settings

    async def run(
        self,
        *,
        symbols: list[str],
        start: date,
        end: date,
        adjustment_mode: AdjustmentMode = AdjustmentMode.AUTO_ADJUSTED,
        interval: str = "1d",
    ) -> RunReport:
        started = datetime.now(timezone.utc)
        deadline = time.monotonic() + self.settings.RUN_DEADLINE_SECONDS
        symbols = [s.strip().upper() for s in symbols if s.strip()]
        errors: list[dict[str, Any]] = []
        notes: list[str] = []
        per_symbol: dict[str, dict[str, Any]] = {}
        counters = PersistCounters()

        if not symbols:
            return self._failed_local(
                started,
                symbols,
                start,
                end,
                adjustment_mode,
                interval,
                "INVALID_PARAMS",
                "symbols must not be empty",
            )
        if end <= start:
            return self._failed_local(
                started,
                symbols,
                start,
                end,
                adjustment_mode,
                interval,
                "INVALID_PARAMS",
                "window_end must be after window_start (end is exclusive)",
            )
        if interval != "1d":
            return self._failed_local(
                started,
                symbols,
                start,
                end,
                adjustment_mode,
                interval,
                "INVALID_PARAMS",
                "only interval=1d is supported in this stage",
            )

        try:
            run_id = await self.repo.create_run(
                provider=self.adapter.provider_name,
                symbols=symbols,
                interval=interval,
                adjustment_mode=adjustment_mode.value,
                window_start=start,
                window_end=end,
                params={
                    "provider": self.adapter.provider_name,
                    "symbols": symbols,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "interval": interval,
                    "adjustment_mode": adjustment_mode.value,
                    "session_timezone": "America/New_York",
                    "quote_time_rule": "session date at 16:00 America/New_York",
                },
            )
        except Exception as exc:
            return self._failed_local(
                started,
                symbols,
                start,
                end,
                adjustment_mode,
                interval,
                "DB_UNAVAILABLE",
                f"cannot create collection run: {exc}"[:500],
            )

        for symbol in symbols:
            try:
                fetch = await with_retries(
                    lambda s=symbol: self.adapter.fetch_daily(
                        s, start, end, adjustment_mode
                    ),
                    max_attempts=self.settings.IO_MAX_ATTEMPTS,
                    wait_seconds=self.settings.IO_RETRY_WAIT_SECONDS,
                    deadline_monotonic=deadline,
                    operation_timeout=self.settings.IO_OPERATION_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                code = "SOURCE_UNAVAILABLE"
                if isinstance(exc, PermanentIOError):
                    code = exc.code
                elif isinstance(exc, TransientIOError):
                    code = exc.code
                elif isinstance(exc, TimeoutError) and str(exc) == "DEADLINE_EXCEEDED":
                    code = "DEADLINE_EXCEEDED"
                per_symbol[symbol] = {
                    "status": FetchStatus.UNAVAILABLE.value,
                    "error": str(exc)[:300],
                }
                errors.append(
                    {
                        "symbol": symbol,
                        "code": code,
                        "message": str(exc)[:300],
                    }
                )
                continue

            row_count = len(fetch.bars) + len(fetch.parse_failures)
            per_symbol[symbol] = {
                "status": fetch.status.value,
                "bars": row_count,
                "parse_failures": len(fetch.parse_failures),
                "error_code": fetch.error_code,
            }

            if fetch.status is FetchStatus.UNAVAILABLE:
                errors.append(
                    {
                        "symbol": symbol,
                        "code": fetch.error_code or "SOURCE_UNAVAILABLE",
                        "message": fetch.error_message or "",
                    }
                )
                continue

            if fetch.status is FetchStatus.EMPTY:
                notes.append(
                    f"{symbol}: source returned 0 bars for "
                    f"[{start.isoformat()}, {end.isoformat()})"
                )
                continue

            for failure in fetch.parse_failures:
                await self._persist_quarantine(
                    failure, run_id, symbol, counters, errors, deadline
                )

            for bar in fetch.bars:
                counters.received += 1
                bad = validate_bar(bar)
                if bad is not None:
                    await self._persist_quarantine(
                        bad, run_id, symbol, counters, errors, deadline
                    )
                    continue

                try:
                    outcome = await with_retries(
                        lambda b=bar: self._upsert_confirmed(b, run_id),
                        max_attempts=self.settings.IO_MAX_ATTEMPTS,
                        wait_seconds=self.settings.IO_RETRY_WAIT_SECONDS,
                        deadline_monotonic=deadline,
                        operation_timeout=self.settings.IO_OPERATION_TIMEOUT_SECONDS,
                    )
                except Exception as exc:
                    counters.failed += 1
                    errors.append(
                        {
                            "symbol": symbol,
                            "code": "RECORD_WRITE_FAILED",
                            "message": str(exc)[:300],
                        }
                    )
                    continue

                if outcome == "inserted":
                    counters.inserted += 1
                elif outcome == "updated":
                    counters.updated += 1
                else:
                    counters.duplicates += 1

        finished = datetime.now(timezone.utc)
        status = self._decide_status(
            counters=counters,
            errors=errors,
            symbols=symbols,
            per_symbol=per_symbol,
        )
        report = RunReport(
            run_id=run_id,
            status=status,
            provider=self.adapter.provider_name,
            symbols=symbols,
            interval=interval,
            adjustment_mode=adjustment_mode.value,
            window_start=start,
            window_end=end,
            started_at=started,
            finished_at=finished,
            counters=counters,
            per_symbol=per_symbol,
            errors=errors,
            notes=notes,
        )
        try:
            await self.repo.finish_run(report)
        except Exception as exc:
            report.status = RunStatus.FAILED
            report.errors.append(
                {
                    "code": "REPORT_PERSIST_FAILED",
                    "message": str(exc)[:300],
                }
            )
        return report

    async def _persist_quarantine(
        self,
        row: QuarantineRow,
        run_id: UUID,
        symbol: str,
        counters: PersistCounters,
        errors: list[dict[str, Any]],
        deadline: float,
    ) -> None:
        # Parse failures increment received here; validation failures already did.
        if row.reason_code == "PARSE_ERROR":
            counters.received += 1
        try:
            await with_retries(
                lambda r=row: self.repo.quarantine(r, run_id),
                max_attempts=self.settings.IO_MAX_ATTEMPTS,
                wait_seconds=self.settings.IO_RETRY_WAIT_SECONDS,
                deadline_monotonic=deadline,
                operation_timeout=self.settings.IO_OPERATION_TIMEOUT_SECONDS,
            )
            counters.quarantined += 1
        except Exception as exc:
            counters.failed += 1
            errors.append(
                {
                    "symbol": symbol,
                    "code": "QUARANTINE_WRITE_FAILED",
                    "message": str(exc)[:300],
                }
            )

    async def _upsert_confirmed(self, bar, run_id: UUID) -> str:
        try:
            return await self.repo.upsert_bar(bar, run_id)
        except Exception as exc:
            # Uncertain outcome → inspect DB by natural key before failing.
            existing = await self.repo.get_existing(bar)
            if existing is None:
                raise
            _id, o, h, l, c, v = existing
            if (o, h, l, c, v) == bar.ohlcv_tuple():
                return "duplicate"
            # Data present but different — treat as needing update retry
            raise exc

    @staticmethod
    def _decide_status(
        *,
        counters: PersistCounters,
        errors: list[dict[str, Any]],
        symbols: list[str],
        per_symbol: dict[str, dict[str, Any]],
    ) -> RunStatus:
        confirmed = counters.inserted + counters.updated + counters.duplicates
        statuses = [per_symbol.get(s, {}).get("status") for s in symbols]
        all_unavailable = bool(symbols) and all(
            st == FetchStatus.UNAVAILABLE.value for st in statuses
        )
        if all_unavailable:
            return RunStatus.FAILED

        # Received rows but nothing confirmed in quotes or quarantine.
        if (
            counters.received > 0
            and confirmed == 0
            and counters.quarantined == 0
        ):
            return RunStatus.FAILED

        all_empty = bool(symbols) and all(
            st == FetchStatus.EMPTY.value for st in statuses
        )
        if all_empty:
            return RunStatus.PARTIAL

        has_unavailable = any(
            st == FetchStatus.UNAVAILABLE.value for st in statuses
        )
        has_empty = any(st == FetchStatus.EMPTY.value for st in statuses)

        if (
            counters.quarantined
            or counters.failed
            or has_unavailable
            or has_empty
        ):
            return RunStatus.PARTIAL

        if confirmed > 0 and confirmed == counters.received:
            return RunStatus.READY

        if confirmed > 0 and counters.quarantined == 0 and counters.failed == 0:
            return RunStatus.READY

        return RunStatus.PARTIAL

    def _failed_local(
        self,
        started: datetime,
        symbols: list[str],
        start: date,
        end: date,
        adjustment_mode: AdjustmentMode,
        interval: str,
        code: str,
        message: str,
    ) -> RunReport:
        from uuid import uuid4

        finished = datetime.now(timezone.utc)
        return RunReport(
            run_id=uuid4(),
            status=RunStatus.FAILED,
            provider=self.adapter.provider_name,
            symbols=symbols,
            interval=interval,
            adjustment_mode=adjustment_mode.value,
            window_start=start,
            window_end=end,
            started_at=started,
            finished_at=finished,
            counters=PersistCounters(),
            errors=[{"code": code, "message": message}],
            notes=[],
        )
