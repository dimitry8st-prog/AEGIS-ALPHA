"""Read-only response models for collection runs and stored quotes."""

from datetime import date, datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class RunSummary(BaseModel):
    id: UUID
    provider: str
    symbols: Any
    interval: str
    adjustment_mode: str
    window_start: date
    window_end: date
    status: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    received_count: int
    inserted_count: int
    updated_count: int
    duplicate_count: int
    quarantined_count: int
    failed_count: int


class RunDetail(RunSummary):
    params: Any = None
    report: Any = None
    errors: Any = None


class QuoteRow(BaseModel):
    id: UUID
    provider: str
    symbol: str
    exchange: str
    interval: str
    quote_time: datetime
    open: str
    high: str
    low: str
    close: str
    volume: int
    adjustment_mode: str
    session_timezone: str
    currency: str
    received_at: datetime
    run_id: Optional[UUID] = None


class RunPage(BaseModel):
    limit: int
    offset: int
    total: int
    items: list[RunSummary]


class QuotePage(BaseModel):
    limit: int
    offset: int
    total: int
    timezone: str = Field(
        default="America/New_York",
        description="start/end are session dates in this timezone. start is inclusive, end is exclusive.",
    )
    items: list[QuoteRow]
