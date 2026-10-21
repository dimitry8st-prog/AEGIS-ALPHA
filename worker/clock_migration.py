"""Move early-close quotes from the old 16:00 stamp to 13:00.

The previous natural key included 16:00, so a later 13:00 insert would be a
second row. This keeps one row per session. The old timestamp and OHLCV are
written to market_quote_revisions before the quote changes. A 16:00 row is
removed only when a 13:00 row already exists, and only after its history has
been copied onto that surviving row in the same transaction.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import text

from worker.repository import _payload_hash
from worker.session_calendar import SESSION_TZ, legacy_sixteen_close, quote_time_for_session
from worker.types import AdjustmentMode, QuoteBar


def migrate_legacy_early_closes(connection) -> int:
    rows = connection.execute(
        text(
            """
            SELECT id, provider, symbol, exchange, interval, quote_time,
                   open, high, low, close, volume, adjustment_mode,
                   session_timezone, currency, run_id
            FROM market_quotes
            """
        )
    ).mappings().all()
    changed = 0
    for row in rows:
        local = _as_local(row["quote_time"])
        if local is None or (local.hour, local.minute, local.second) != (16, 0, 0):
            continue
        if legacy_sixteen_close(local.date()) is None:
            continue
        if connection.execute(
            text("SELECT 1 FROM market_quotes WHERE id = :id"),
            {"id": row["id"]},
        ).first() is None:
            continue
        new_time = quote_time_for_session(local.date())
        if new_time is None:
            continue
        new_utc = new_time.astimezone(timezone.utc)
        survivor = connection.execute(
            text(
                """
                SELECT id, open, high, low, close, volume
                FROM market_quotes
                WHERE provider = :provider
                  AND symbol = :symbol
                  AND exchange = :exchange
                  AND interval = :interval
                  AND adjustment_mode = :adjustment_mode
                  AND quote_time = :quote_time
                  AND id <> :id
                """
            ),
            {
                "provider": row["provider"],
                "symbol": row["symbol"],
                "exchange": row["exchange"],
                "interval": row["interval"],
                "adjustment_mode": row["adjustment_mode"],
                "quote_time": new_utc,
                "id": row["id"],
            },
        ).mappings().first()
        if survivor is None:
            _revise(connection, row, row, new_utc, row["quote_time"], row["id"])
            bar = _bar(row, new_time)
            connection.execute(
                text(
                    """
                    UPDATE market_quotes SET
                        quote_time = :quote_time,
                        payload_hash = :payload_hash,
                        updated_at = :updated_at
                    WHERE id = :id
                    """
                ),
                {
                    "id": row["id"],
                    "quote_time": new_utc,
                    "payload_hash": _payload_hash(bar),
                    "updated_at": datetime.now(timezone.utc),
                },
            )
        else:
            connection.execute(
                text(
                    """
                    UPDATE market_quote_revisions
                    SET quote_id = :survivor
                    WHERE quote_id = :legacy
                    """
                ),
                {"survivor": survivor["id"], "legacy": row["id"]},
            )
            _revise(connection, row, survivor, new_utc, row["quote_time"], survivor["id"])
            connection.execute(
                text("DELETE FROM market_quotes WHERE id = :id"),
                {"id": row["id"]},
            )
        changed += 1
    return changed


def _revise(connection, legacy, current, new_utc, previous_quote_time, quote_id) -> None:
    if previous_quote_time.tzinfo is None:
        previous_quote_time = previous_quote_time.replace(tzinfo=timezone.utc)
    connection.execute(
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
            "provider": legacy["provider"],
            "symbol": legacy["symbol"],
            "exchange": legacy["exchange"],
            "interval": legacy["interval"],
            "quote_time": new_utc,
            "previous_quote_time": previous_quote_time.astimezone(timezone.utc),
            "adjustment_mode": legacy["adjustment_mode"],
            "open": current["open"],
            "high": current["high"],
            "low": current["low"],
            "close": current["close"],
            "volume": current["volume"],
            "previous_open": legacy["open"],
            "previous_high": legacy["high"],
            "previous_low": legacy["low"],
            "previous_close": legacy["close"],
            "previous_volume": legacy["volume"],
            "run_id": legacy["run_id"],
            "changed_at": datetime.now(timezone.utc),
        },
    )


def _bar(row, quote_time: datetime) -> QuoteBar:
    return QuoteBar(
        provider=row["provider"],
        symbol=row["symbol"],
        exchange=row["exchange"],
        interval=row["interval"],
        quote_time=quote_time,
        open=Decimal(str(row["open"])),
        high=Decimal(str(row["high"])),
        low=Decimal(str(row["low"])),
        close=Decimal(str(row["close"])),
        volume=int(row["volume"]),
        adjustment_mode=AdjustmentMode(row["adjustment_mode"]),
        session_timezone=row["session_timezone"] or "America/New_York",
        currency=row["currency"] or "USD",
    )


def _as_local(value) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(ZoneInfo(str(SESSION_TZ)))
