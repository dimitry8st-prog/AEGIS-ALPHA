"""Read-only response models for collection runs and stored quotes."""

from datetime import date, datetime
from typing import Any, Literal, Optional
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
        description="Provider's local session-date timezone: New York for US, Moscow for moex/finam. End exclusive.",
    )
    items: list[QuoteRow]


class FinamMinute(BaseModel):
    quote_time: datetime
    open: str
    high: str
    low: str
    close: str
    volume: int


class FinamMinutes(BaseModel):
    provider: Literal["finam"] = "finam"
    interval: Literal["1m"] = "1m"
    currency: Literal["RUB"] = "RUB"
    symbol: str
    start: datetime
    end: datetime
    status: Literal["READY", "PARTIAL", "EMPTY", "INVALID"]
    expected_minutes: int
    rejected: dict[str, int] = Field(default_factory=dict)
    items: list[FinamMinute]


class IpoEntry(BaseModel):
    id: str
    company: str
    symbol: str | None
    exchange: str | None
    status: str
    date: date | None
    date_kind: str
    offer_price: str | None
    price_kind: str
    currency: str
    source_url: str


class IpoSource(BaseModel):
    market: Literal["moex", "us"]
    url: str
    scope: str
    status: Literal["ok", "unavailable"]
    error: str | None = None
    entries: list[IpoEntry]


class IpoReport(BaseModel):
    observed_at: datetime
    status: Literal["ok", "partial", "unavailable"]
    sources: list[IpoSource]
