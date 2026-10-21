"""PostgreSQL persistence for quotes, quarantine, revisions and collection runs."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from zoneinfo import ZoneInfo

from worker.config import WorkerSettings, sqlalchemy_async_url
from worker.session_calendar import legacy_sixteen_close
from worker.types import SESSION_TIMEZONE, QuarantineRow, QuoteBar, RunReport, stored_ohlcv


def create_engine(settings: WorkerSettings) -> AsyncEngine:
    return create_async_engine(
        sqlalchemy_async_url(settings.DATABASE_URL),
        pool_pre_ping=True,
    )


class QuoteRepository:
    def __init__(self, engine: AsyncEngine):
        self.engine = engine

    async def create_run(
        self,
        *,
        provider: str,
        symbols: list[str],
        interval: str,
        adjustment_mode: str,
        window_start,
        window_end,
        params: dict[str, Any],
    ) -> UUID:
        run_id = uuid4()
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    INSERT INTO market_collection_runs (
                        id, provider, symbols, interval, adjustment_mode,
                        window_start, window_end, status, params, started_at
                    ) VALUES (
                        :id, :provider, CAST(:symbols AS jsonb), :interval, :adjustment_mode,
                        :window_start, :window_end, 'RUNNING', CAST(:params AS jsonb), :started_at
                    )
                    """
                ),
                {
                    "id": run_id,
                    "provider": provider,
                    "symbols": json.dumps(symbols),
                    "interval": interval,
                    "adjustment_mode": adjustment_mode,
                    "window_start": window_start,
                    "window_end": window_end,
                    "params": json.dumps(params),
                    "started_at": datetime.now(timezone.utc),
                },
            )
        return run_id

    async def finish_run(self, report: RunReport) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    UPDATE market_collection_runs SET
                        status = :status,
                        finished_at = :finished_at,
                        received_count = :received,
                        inserted_count = :inserted,
                        updated_count = :updated,
                        duplicate_count = :duplicates,
                        quarantined_count = :quarantined,
                        failed_count = :failed,
                        report = CAST(:report AS jsonb),
                        errors = CAST(:errors AS jsonb)
                    WHERE id = :id
                    """
                ),
                {
                    "id": report.run_id,
                    "status": report.status.value,
                    "finished_at": report.finished_at,
                    "received": report.counters.received,
                    "inserted": report.counters.inserted,
                    "updated": report.counters.updated,
                    "duplicates": report.counters.duplicates,
                    "quarantined": report.counters.quarantined,
                    "failed": report.counters.failed,
                    "report": json.dumps(report.to_dict()),
                    "errors": json.dumps(report.errors),
                },
            )

    async def get_existing(
        self, bar: QuoteBar
    ) -> Optional[tuple[UUID, Decimal, Decimal, Decimal, Decimal, int]]:
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        """
                        SELECT id, open, high, low, close, volume
                        FROM market_quotes
                        WHERE provider = :provider
                          AND symbol = :symbol
                          AND exchange = :exchange
                          AND interval = :interval
                          AND quote_time = :quote_time
                          AND adjustment_mode = :adjustment_mode
                        """
                    ),
                    {
                        "provider": bar.provider,
                        "symbol": bar.symbol,
                        "exchange": bar.exchange,
                        "interval": bar.interval,
                        "quote_time": bar.quote_time.astimezone(timezone.utc),
                        "adjustment_mode": bar.adjustment_mode.value,
                    },
                )
            ).first()
        if row is None:
            return None
        open_, high, low, close, volume = stored_ohlcv(
            row.open, row.high, row.low, row.close, row.volume
        )
        return (row.id, open_, high, low, close, volume)

    async def upsert_bar(self, bar: QuoteBar, run_id: UUID) -> str:
        """Insert or update a bar. Returns inserted|updated|duplicate.

        An early-close bar at 13:00 retargets a legacy 16:00 row for the same
        session instead of inserting a second natural key. The old timestamp
        is stored in market_quote_revisions first. If both timestamps already
        exist, the 16:00 row is copied into that history and removed in the
        same transaction.
        """
        bar = bar.at_storage_scale()
        received_at = datetime.now(timezone.utc)
        quote_time = bar.quote_time.astimezone(timezone.utc)
        existing = await self.get_existing(bar)
        legacy = await self._legacy_early_close(bar)

        async with self.engine.begin() as conn:
            if existing is None and legacy is not None:
                await _retarget_legacy(
                    conn, legacy, bar, quote_time, run_id, received_at
                )
                return "updated"

            if existing is not None and legacy is not None:
                await _archive_legacy_conflict(
                    conn,
                    existing[0],
                    existing[1:],
                    legacy,
                    bar,
                    quote_time,
                    run_id,
                    received_at,
                )

            if existing is None:
                await conn.execute(
                    text(
                        """
                        INSERT INTO market_quotes (
                            id, provider, symbol, exchange, interval, quote_time,
                            open, high, low, close, volume, adjustment_mode,
                            session_timezone, currency, received_at, run_id, payload_hash
                        ) VALUES (
                            :id, :provider, :symbol, :exchange, :interval, :quote_time,
                            :open, :high, :low, :close, :volume, :adjustment_mode,
                            :session_timezone, :currency, :received_at, :run_id, :payload_hash
                        )
                        """
                    ),
                    {
                        "id": uuid4(),
                        "provider": bar.provider,
                        "symbol": bar.symbol,
                        "exchange": bar.exchange,
                        "interval": bar.interval,
                        "quote_time": quote_time,
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                        "adjustment_mode": bar.adjustment_mode.value,
                        "session_timezone": bar.session_timezone,
                        "currency": bar.currency,
                        "received_at": received_at,
                        "run_id": run_id,
                        "payload_hash": _payload_hash(bar),
                    },
                )
                return "inserted"

            quote_id, o, h, l, c, v = existing
            if (o, h, l, c, v) == bar.ohlcv_tuple():
                return "updated" if legacy is not None else "duplicate"

            await _insert_revision(
                conn,
                quote_id=quote_id,
                bar=bar,
                quote_time=quote_time,
                previous_quote_time=None,
                previous=(o, h, l, c, v),
                run_id=run_id,
                changed_at=received_at,
            )
            await conn.execute(
                text(
                    """
                    UPDATE market_quotes SET
                        open = :open,
                        high = :high,
                        low = :low,
                        close = :close,
                        volume = :volume,
                        received_at = :received_at,
                        run_id = :run_id,
                        payload_hash = :payload_hash,
                        updated_at = :received_at
                    WHERE id = :id
                    """
                ),
                {
                    "id": quote_id,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "received_at": received_at,
                    "run_id": run_id,
                    "payload_hash": _payload_hash(bar),
                },
            )
            return "updated"

    async def _legacy_early_close(self, bar: QuoteBar):
        local = bar.quote_time.astimezone(ZoneInfo(SESSION_TIMEZONE))
        if (local.hour, local.minute, local.second) != (13, 0, 0):
            return None
        legacy_time = legacy_sixteen_close(local.date())
        if legacy_time is None:
            return None
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        """
                        SELECT id, quote_time, open, high, low, close, volume
                        FROM market_quotes
                        WHERE provider = :provider
                          AND symbol = :symbol
                          AND exchange = :exchange
                          AND interval = :interval
                          AND quote_time = :quote_time
                          AND adjustment_mode = :adjustment_mode
                        """
                    ),
                    {
                        "provider": bar.provider,
                        "symbol": bar.symbol,
                        "exchange": bar.exchange,
                        "interval": bar.interval,
                        "quote_time": legacy_time.astimezone(timezone.utc),
                        "adjustment_mode": bar.adjustment_mode.value,
                    },
                )
            ).first()
        if row is None:
            return None
        return row

    async def quarantine(self, row: QuarantineRow, run_id: UUID) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    INSERT INTO market_quote_quarantine (
                        id, provider, symbol, exchange, interval, quote_time,
                        adjustment_mode, reason_code, reason_detail, payload, run_id, received_at
                    ) VALUES (
                        :id, :provider, :symbol, :exchange, :interval, :quote_time,
                        :adjustment_mode, :reason_code, :reason_detail, CAST(:payload AS jsonb),
                        :run_id, :received_at
                    )
                    """
                ),
                {
                    "id": uuid4(),
                    "provider": row.provider,
                    "symbol": row.symbol,
                    "exchange": row.exchange,
                    "interval": row.interval,
                    "quote_time": row.quote_time.astimezone(timezone.utc)
                    if row.quote_time
                    else None,
                    "adjustment_mode": row.adjustment_mode,
                    "reason_code": row.reason_code,
                    "reason_detail": row.reason_detail,
                    "payload": json.dumps(row.payload),
                    "run_id": run_id,
                    "received_at": datetime.now(timezone.utc),
                },
            )


def _ohlcv(row) -> tuple:
    return (
        Decimal(str(row.open)),
        Decimal(str(row.high)),
        Decimal(str(row.low)),
        Decimal(str(row.close)),
        int(row.volume),
    )


async def _insert_revision(
    conn,
    *,
    quote_id: UUID,
    bar: QuoteBar,
    quote_time: datetime,
    previous_quote_time: datetime | None,
    previous: tuple,
    run_id: UUID,
    changed_at: datetime,
    current: tuple | None = None,
) -> None:
    new_open, new_high, new_low, new_close, new_volume = (
        bar.ohlcv_tuple() if current is None else current
    )
    o, h, l, c, v = previous
    await conn.execute(
        text(
            """
            INSERT INTO market_quote_revisions (
                id, quote_id, provider, symbol, exchange, interval, quote_time,
                previous_quote_time, adjustment_mode, open, high, low, close, volume,
                previous_open, previous_high, previous_low, previous_close, previous_volume,
                run_id, changed_at
            ) VALUES (
                :id, :quote_id, :provider, :symbol, :exchange, :interval, :quote_time,
                :previous_quote_time, :adjustment_mode, :open, :high, :low, :close, :volume,
                :previous_open, :previous_high, :previous_low, :previous_close, :previous_volume,
                :run_id, :changed_at
            )
            """
        ),
        {
            "id": uuid4(),
            "quote_id": quote_id,
            "provider": bar.provider,
            "symbol": bar.symbol,
            "exchange": bar.exchange,
            "interval": bar.interval,
            "quote_time": quote_time,
            "previous_quote_time": previous_quote_time,
            "adjustment_mode": bar.adjustment_mode.value,
            "open": new_open,
            "high": new_high,
            "low": new_low,
            "close": new_close,
            "volume": new_volume,
            "previous_open": o,
            "previous_high": h,
            "previous_low": l,
            "previous_close": c,
            "previous_volume": v,
            "run_id": run_id,
            "changed_at": changed_at,
        },
    )


async def _retarget_legacy(conn, legacy, bar: QuoteBar, quote_time, run_id, received_at) -> None:
    """Move the 16:00 row onto 13:00. History keeps the old timestamp."""
    previous_quote_time = legacy.quote_time
    if previous_quote_time.tzinfo is None:
        previous_quote_time = previous_quote_time.replace(tzinfo=timezone.utc)
    await _insert_revision(
        conn,
        quote_id=legacy.id,
        bar=bar,
        quote_time=quote_time,
        previous_quote_time=previous_quote_time.astimezone(timezone.utc),
        previous=_ohlcv(legacy),
        run_id=run_id,
        changed_at=received_at,
    )
    await conn.execute(
        text(
            """
            UPDATE market_quotes SET
                quote_time = :quote_time,
                open = :open,
                high = :high,
                low = :low,
                close = :close,
                volume = :volume,
                received_at = :received_at,
                run_id = :run_id,
                payload_hash = :payload_hash,
                updated_at = :received_at
            WHERE id = :id
            """
        ),
        {
            "id": legacy.id,
            "quote_time": quote_time,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "received_at": received_at,
            "run_id": run_id,
            "payload_hash": _payload_hash(bar),
        },
    )


async def _archive_legacy_conflict(
    conn,
    survivor_id,
    survivor_ohlcv,
    legacy,
    bar: QuoteBar,
    quote_time,
    run_id,
    received_at,
) -> None:
    """Both 13:00 and 16:00 exist. Keep the 13:00 row and its history.

    Existing revisions of the 16:00 row move onto the survivor before that
    row is removed. The delete runs in this same transaction, after the move
    and after the 16:00 values themselves are copied into a revision.
    """
    await conn.execute(
        text(
            """
            UPDATE market_quote_revisions
            SET quote_id = :survivor
            WHERE quote_id = :legacy
            """
        ),
        {"survivor": survivor_id, "legacy": legacy.id},
    )
    previous_quote_time = legacy.quote_time
    if previous_quote_time.tzinfo is None:
        previous_quote_time = previous_quote_time.replace(tzinfo=timezone.utc)
    await _insert_revision(
        conn,
        quote_id=survivor_id,
        bar=bar,
        quote_time=quote_time,
        previous_quote_time=previous_quote_time.astimezone(timezone.utc),
        previous=_ohlcv(legacy),
        current=tuple(survivor_ohlcv),
        run_id=run_id,
        changed_at=received_at,
    )
    await conn.execute(
        text("DELETE FROM market_quotes WHERE id = :id"),
        {"id": legacy.id},
    )


def _payload_hash(bar: QuoteBar) -> str:
    import hashlib

    raw = "|".join(
        [
            bar.provider,
            bar.symbol,
            bar.exchange,
            bar.interval,
            bar.quote_time.astimezone(timezone.utc).isoformat(),
            bar.adjustment_mode.value,
            str(bar.open),
            str(bar.high),
            str(bar.low),
            str(bar.close),
            str(bar.volume),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
