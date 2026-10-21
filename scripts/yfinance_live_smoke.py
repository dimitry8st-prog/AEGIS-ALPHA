"""One live yfinance request. Not used by CI and not replaced by offline data.

Prints VERIFIED or NOT_VERIFIED. Does not write to PostgreSQL.
"""

from __future__ import annotations

import asyncio
import json
import sys
import traceback
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from worker.adapters.yfinance_adapter import YFinanceAdapter
from worker.types import AdjustmentMode


async def _one(mode: AdjustmentMode) -> dict:
    adapter = YFinanceAdapter()
    result = await adapter.fetch_daily(
        "AAPL",
        date(2024, 1, 2),
        date(2024, 1, 3),
        mode,
    )
    bar = result.bars[0] if result.bars else None
    return {
        "adjustment_mode": mode.value,
        "status": result.status.value,
        "bars": len(result.bars),
        "parse_failures": len(result.parse_failures),
        "quote_time": None if bar is None else bar.quote_time.isoformat(),
        "exchange": None if bar is None else bar.exchange,
        "close": None if bar is None else str(bar.close),
    }


async def main() -> int:
    try:
        raw = await _one(AdjustmentMode.RAW)
        adjusted = await _one(AdjustmentMode.AUTO_ADJUSTED)
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "NOT_VERIFIED",
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:500],
                    "traceback": traceback.format_exc()[-1000:],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 2
    ok = (
        raw["status"] == "OK"
        and adjusted["status"] == "OK"
        and raw["bars"] == 1
        and adjusted["bars"] == 1
        and raw["exchange"] == "XNAS"
        and raw["quote_time"] is not None
        and raw["quote_time"].startswith("2024-01-02T16:00:00")
    )
    print(
        json.dumps(
            {
                "status": "VERIFIED" if ok else "NOT_VERIFIED",
                "raw": raw,
                "auto_adjusted": adjusted,
                "closes_differ": raw["close"] != adjusted["close"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
