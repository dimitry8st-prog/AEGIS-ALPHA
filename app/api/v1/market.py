"""Read-only access to collection reports and stored quotes.

This module does not start a collection.
"""

from __future__ import annotations

from datetime import date, timedelta
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.dependencies import get_current_active_user
from app.core.error_handler import AppException, NotFoundError
from app.models.user import User
from app.schemas.market import QuotePage, QuoteRow, RunDetail, RunPage, RunSummary

router = APIRouter()

MAX_PAGE = 200
MAX_WINDOW_DAYS = 366
SESSION_TIMEZONE = "America/New_York"


def _window(start: date, end: date) -> None:
    if end <= start:
        raise AppException(
            code="INVALID_WINDOW",
            message="end is exclusive and must be later than start",
            status_code=422,
        )
    if (end - start) > timedelta(days=MAX_WINDOW_DAYS):
        raise AppException(
            code="INVALID_WINDOW",
            message=f"period cannot exceed {MAX_WINDOW_DAYS} days",
            status_code=422,
        )


def _migration_error(exc: Exception) -> None:
    if isinstance(exc, ProgrammingError):
        raise AppException(
            code="MIGRATION_REQUIRED",
            message="Market tables are missing. Run: alembic upgrade head",
            status_code=503,
        ) from exc
    raise exc


@router.get("/runs", response_model=RunPage)
async def list_runs(
    limit: int = Query(50, ge=1, le=MAX_PAGE),
    offset: int = Query(0, ge=0, le=100_000),
    status: str | None = Query(None, pattern="^(READY|PARTIAL|FAILED|RUNNING)$"),
    provider: str | None = Query(None, pattern="^(offline|yfinance)$"),
    adjustment_mode: str | None = Query(None, pattern="^(raw|auto_adjusted)$"),
    start: date | None = None,
    end: date | None = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """List collection runs. A period matches runs whose window overlaps [start, end).

    Dates are calendar dates. The stored window uses the same inclusive start
    and exclusive end as the worker. One run has one provider and one
    adjustment_mode; filters keep those series apart.
    """
    del current_user
    if (start is None) ^ (end is None):
        raise AppException(
            code="INVALID_WINDOW",
            message="start and end must be set together",
            status_code=422,
        )
    if start is not None and end is not None:
        _window(start, end)

    filters: list[str] = []
    params: dict = {}
    if status:
        filters.append("status = :status")
        params["status"] = status
    if provider:
        filters.append("provider = :provider")
        params["provider"] = provider
    if adjustment_mode:
        filters.append("adjustment_mode = :adjustment_mode")
        params["adjustment_mode"] = adjustment_mode
    if start is not None and end is not None:
        filters.append("window_start < :end AND window_end > :start")
        params["start"] = start
        params["end"] = end
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    try:
        total = (
            await db.execute(
                text(f"SELECT COUNT(*) FROM market_collection_runs {where}"),
                params,
            )
        ).scalar_one()
        rows = (
            await db.execute(
                text(
                    f"""
                    SELECT id, provider, symbols, interval, adjustment_mode,
                           window_start, window_end, status, started_at, finished_at,
                           received_count, inserted_count, updated_count, duplicate_count,
                           quarantined_count, failed_count
                    FROM market_collection_runs
                    {where}
                    ORDER BY started_at DESC, id ASC
                    LIMIT :limit OFFSET :offset
                    """
                ),
                {**params, "limit": limit, "offset": offset},
            )
        ).mappings().all()
    except ProgrammingError as exc:
        _migration_error(exc)
    return RunPage(
        limit=limit,
        offset=offset,
        total=int(total),
        items=[RunSummary.model_validate(dict(row)) for row in rows],
    )


@router.get("/runs/{run_id}", response_model=RunDetail)
async def get_run(
    run_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """One collection report. Unknown run_id is 404."""
    del current_user
    try:
        row = (
            await db.execute(
                text(
                    """
                    SELECT id, provider, symbols, interval, adjustment_mode,
                           window_start, window_end, status, params, started_at, finished_at,
                           received_count, inserted_count, updated_count, duplicate_count,
                           quarantined_count, failed_count, report, errors
                    FROM market_collection_runs
                    WHERE id = :run_id
                    """
                ),
                {"run_id": run_id},
            )
        ).mappings().first()
    except ProgrammingError as exc:
        _migration_error(exc)
    if row is None:
        raise NotFoundError("Run", str(run_id))
    return RunDetail.model_validate(dict(row))


@router.get("/quotes", response_model=QuotePage)
async def list_quotes(
    provider: str = Query(..., pattern="^(offline|yfinance)$"),
    adjustment_mode: str = Query(..., pattern="^(raw|auto_adjusted)$"),
    start: date = Query(...),
    end: date = Query(...),
    symbol: str | None = Query(None, min_length=1, max_length=32, pattern=r"^[A-Za-z0-9.\-]+$"),
    limit: int = Query(100, ge=1, le=MAX_PAGE),
    offset: int = Query(0, ge=0, le=100_000),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """Quotes for one provider and one adjustment mode.

    `start` is an inclusive session date and `end` is exclusive. Both are
    America/New_York calendar dates, not UTC midnights. The span cannot
    exceed 366 days. offline and yfinance are never returned together, and
    raw is never mixed with auto_adjusted.
    """
    del current_user
    _window(start, end)
    filters = [
        "provider = :provider",
        "adjustment_mode = :adjustment_mode",
        "CAST((quote_time AT TIME ZONE 'America/New_York') AS date) >= :start",
        "CAST((quote_time AT TIME ZONE 'America/New_York') AS date) < :end",
    ]
    params: dict = {
        "provider": provider,
        "adjustment_mode": adjustment_mode,
        "start": start,
        "end": end,
    }
    if symbol:
        filters.append("symbol = :symbol")
        params["symbol"] = symbol.strip().upper()
    where = "WHERE " + " AND ".join(filters)
    try:
        total = (
            await db.execute(text(f"SELECT COUNT(*) FROM market_quotes {where}"), params)
        ).scalar_one()
        rows = (
            await db.execute(
                text(
                    f"""
                    SELECT id, provider, symbol, exchange, interval, quote_time,
                           CAST(open AS text) AS open, CAST(high AS text) AS high,
                           CAST(low AS text) AS low, CAST(close AS text) AS close,
                           volume, adjustment_mode,
                           session_timezone, currency, received_at, run_id
                    FROM market_quotes
                    {where}
                    ORDER BY quote_time ASC, symbol ASC, id ASC
                    LIMIT :limit OFFSET :offset
                    """
                ),
                {**params, "limit": limit, "offset": offset},
            )
        ).mappings().all()
    except ProgrammingError as exc:
        _migration_error(exc)
    return QuotePage(
        limit=limit,
        offset=offset,
        total=int(total),
        timezone=SESSION_TIMEZONE,
        items=[QuoteRow.model_validate(dict(row)) for row in rows],
    )
