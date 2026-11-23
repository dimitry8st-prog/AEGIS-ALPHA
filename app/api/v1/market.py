"""Read-only access to collection reports, quotes, Finam minutes and IPO calendars.

This module does not start a collection.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import aclosing
from datetime import date, datetime, timedelta, timezone
from typing import Literal
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import AsyncSessionLocal, get_db
from app.core.dependencies import get_current_active_user, get_current_user, security
from app.core.error_handler import AppException, NotFoundError
from app.models.user import User
from app.schemas.market import FinamMinute, FinamMinutes, IpoReport, QuotePage, QuoteRow, RunDetail, RunPage, RunSummary
from app.core.config import settings
from worker.adapters.finam_adapter import FinamAdapter
from worker.adapters.finam_stream import stream_finam_market
from worker.finam_live_status import live_failure_stage
from worker.io_retry import PermanentIOError, TransientIOError
from worker.ipo import search_ipos
from worker.russian_market import provider_timezone
from worker.portfolio import research_portfolio

logger = structlog.get_logger()

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
    provider: str | None = Query(None, pattern="^(offline|yfinance|moex|finam)$"),
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
    provider: str = Query(..., pattern="^(offline|yfinance|moex|finam)$"),
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
    local session dates (Moscow for moex/finam, New York for US providers). The span cannot
    exceed 366 days. offline and yfinance are never returned together, and
    raw is never mixed with auto_adjusted.
    """
    del current_user
    _window(start, end)
    session_timezone = provider_timezone(provider)
    filters = [
        "provider = :provider",
        "adjustment_mode = :adjustment_mode",
        "CAST((quote_time AT TIME ZONE :session_timezone) AS date) >= :start",
        "CAST((quote_time AT TIME ZONE :session_timezone) AS date) < :end",
    ]
    params: dict = {
        "provider": provider,
        "adjustment_mode": adjustment_mode,
        "start": start,
        "end": end,
        "session_timezone": session_timezone,
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
        timezone=session_timezone,
        items=[QuoteRow.model_validate(dict(row)) for row in rows],
    )


@router.get("/portfolios/russian-research")
async def russian_research_portfolio(
    current_user: User = Depends(get_current_active_user),
):
    """Research watchlist selected by the owner; it does not read brokerage positions."""
    del current_user
    portfolio = research_portfolio()
    configured = bool(settings.FINAM_API_TOKEN and settings.FINAM_API_TOKEN.get_secret_value())
    portfolio["broker_connection"] = {
        "provider": "finam",
        "status": "CONFIGURED_NOT_VERIFIED" if configured else "NOT_CONFIGURED",
        "required_environment_variable": "FINAM_API_TOKEN",
    }
    return portfolio


@router.get("/ipos", response_model=IpoReport)
async def list_ipos(
    market: Literal["all", "moex", "us"] = "all",
    month: str | None = Query(None, pattern=r"^[0-9]{4}-(0[1-9]|1[0-2])$"),
    query: str | None = Query(None, max_length=120),
    current_user: User = Depends(get_current_active_user),
):
    """Find IPO companies and return separate source statuses and date types."""
    del current_user
    if month is not None and month.startswith("0000-"):
        raise AppException(code="INVALID_MONTH", message="month must use a positive year", status_code=422)
    return await search_ipos(market, month, query)


@router.get("/finam/minutes", response_model=FinamMinutes)
async def read_finam_minutes(
    symbol: Literal["GAZP", "GMKN"],
    start: datetime,
    end: datetime,
    current_user: User = Depends(get_current_active_user),
):
    """Read a closed M1 window directly from Finam, with an exclusive end."""
    del current_user
    token = settings.FINAM_API_TOKEN.get_secret_value() if settings.FINAM_API_TOKEN else None
    try:
        history = await FinamAdapter(token).fetch_minute_history(symbol, start, end)
    except TransientIOError as exc:
        raise AppException(code=exc.code, message=str(exc), status_code=503) from exc
    except PermanentIOError as exc:
        status = {"INVALID_WINDOW": 422, "FINAM_NOT_CONFIGURED": 503}.get(exc.code, 502)
        raise AppException(code=exc.code, message=str(exc), status_code=status) from exc

    start, end = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
    expected = (end - start) // timedelta(minutes=1)
    bars = history.bars
    if history.rejected:
        logger.warning("Finam minute history rejected rows", symbol=symbol, reasons=history.rejected)
    if not bars:
        status = "INVALID" if history.rejected else "EMPTY"
    else:
        status = "READY" if len(bars) == expected and not history.rejected else "PARTIAL"
    return FinamMinutes(
        symbol=symbol,
        start=start,
        end=end,
        expected_minutes=expected,
        status=status,
        rejected=history.rejected,
        items=[FinamMinute(
            quote_time=bar.quote_time, open=str(bar.open), high=str(bar.high),
            low=str(bar.low), close=str(bar.close), volume=bar.volume,
        ) for bar in bars],
    )


async def _authenticate_stream(credentials: HTTPAuthorizationCredentials = Depends(security)) -> None:
    # Close the DB session before the potentially long response starts.
    async with AsyncSessionLocal() as db:
        await get_current_user(credentials, db)


def _log_live_failure(code: str) -> str:
    stage = live_failure_stage(code)
    # Keep broker tokens and response bodies out of the journal.
    logger.warning("Finam live stream failed", code=code, stage=stage)
    return stage


async def _live_events(jwt: str):
    try:
        # Periodic reconnection renews the upstream session and validates the AEGIS login.
        async with asyncio.timeout(300):
            async with aclosing(stream_finam_market(jwt)) as events:
                async for event in events:
                    yield json.dumps(event, separators=(",", ":")) + "\n"
    except TimeoutError:
        return
    except (PermanentIOError, TransientIOError) as exc:
        stage = _log_live_failure(exc.code)
        yield json.dumps({"type": "error", "code": exc.code, "stage": stage}, separators=(",", ":")) + "\n"


@router.get(
    "/finam/stream",
    dependencies=[Depends(_authenticate_stream)],
    response_class=StreamingResponse,
    responses={200: {"content": {"application/x-ndjson": {}}}},
)
async def live_finam_market():
    """Stream NDJSON quotes and M1 candle updates for GAZP and GMKN."""
    token = settings.FINAM_API_TOKEN.get_secret_value() if settings.FINAM_API_TOKEN else None
    try:
        jwt = await FinamAdapter(token).open_market_session()
    except TransientIOError as exc:
        _log_live_failure(exc.code)
        raise AppException(code=exc.code, message=str(exc), status_code=503) from exc
    except PermanentIOError as exc:
        _log_live_failure(exc.code)
        status = 503 if exc.code == "FINAM_NOT_CONFIGURED" else 502
        raise AppException(code=exc.code, message=str(exc), status_code=status) from exc
    return StreamingResponse(
        _live_events(jwt), media_type="application/x-ndjson",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )
