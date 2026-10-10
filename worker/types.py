"""Shared types and run-status contracts for the market-data worker."""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Optional
from uuid import UUID


SESSION_TIMEZONE = "America/New_York"
# Daily bars are anchored to the regular US equity session calendar date.
# quote_time is stored as that session date at 16:00 America/New_York
# (regular session close), then persisted as TIMESTAMPTZ (UTC).
DAILY_SESSION_CLOSE_HOUR = 16


class AdjustmentMode(str, Enum):
    AUTO_ADJUSTED = "auto_adjusted"
    RAW = "raw"


class RunStatus(str, Enum):
    """Terminal statuses for a collection run.

    READY — every requested symbol was fetched successfully; every valid bar
    was confirmed written (inserted/updated/duplicate); no quarantine rows
    for required symbols; run report persisted.
    PARTIAL — at least one symbol unavailable/empty-unexpected, or some bars
    quarantined, or some temporary errors after retries; run report persisted.
    FAILED — invalid parameters, or the run report / quotes could not be
    confirmed in the database.
    """

    READY = "READY"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class FetchStatus(str, Enum):
    OK = "OK"
    EMPTY = "EMPTY"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class QuoteBar:
    provider: str
    symbol: str
    exchange: str
    interval: str
    quote_time: datetime  # timezone-aware instant of the bar
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    adjustment_mode: AdjustmentMode
    session_timezone: str = SESSION_TIMEZONE
    currency: str = "USD"

    def natural_key(self) -> tuple:
        return (
            self.provider,
            self.symbol,
            self.exchange,
            self.interval,
            self.quote_time.astimezone(timezone.utc),
            self.adjustment_mode.value,
        )

    def ohlcv_tuple(self) -> tuple:
        return (self.open, self.high, self.low, self.close, self.volume)


@dataclass
class FetchResult:
    status: FetchStatus
    bars: list[QuoteBar] = field(default_factory=list)
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    symbol: Optional[str] = None


@dataclass
class QuarantineRow:
    provider: str
    symbol: str
    exchange: str
    interval: str
    quote_time: Optional[datetime]
    adjustment_mode: Optional[str]
    reason_code: str
    reason_detail: str
    payload: dict[str, Any]


@dataclass
class PersistCounters:
    received: int = 0
    inserted: int = 0
    updated: int = 0
    duplicates: int = 0
    quarantined: int = 0
    failed: int = 0


@dataclass
class RunReport:
    run_id: UUID
    status: RunStatus
    provider: str
    symbols: list[str]
    interval: str
    adjustment_mode: str
    window_start: date
    window_end: date  # exclusive calendar end (same convention as yfinance)
    started_at: datetime
    finished_at: datetime
    counters: PersistCounters
    per_symbol: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["run_id"] = str(self.run_id)
        data["status"] = self.status.value
        data["started_at"] = self.started_at.isoformat()
        data["finished_at"] = self.finished_at.isoformat()
        data["window_start"] = self.window_start.isoformat()
        data["window_end"] = self.window_end.isoformat()
        return data
