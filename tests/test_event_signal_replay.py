import json

import pytest

from scripts.replay_event_signal import EXAMPLE_DIR, replay, window_start_ms


def test_saved_real_candles_replay_in_both_directions():
    spec = json.loads((EXAMPLE_DIR / "events.json").read_text(encoding="utf-8"))
    rows = json.loads((EXAMPLE_DIR / "candles.json").read_text(encoding="utf-8"))
    symbol, (first, second) = spec["symbol"], spec["cases"]
    short = replay(symbol, first, rows)["candidate"]
    long = replay(symbol, second, rows)["candidate"]

    assert (short["direction"], short["available_at"]) == ("SHORT", "2026-11-19T17:00:00+00:00")
    assert (long["direction"], long["available_at"]) == ("LONG", "2026-11-20T14:47:00+00:00")

    available = window_start_ms(short["available_at"])
    changed = [
        {**row, "h": "999", "c": "999", "v": "1000000"} if row["t"] >= available else row
        for row in rows
    ]
    late_fact = {**first["public_context"][0], "available_by": "2026-11-19T16:50:00+00:00"}
    replay_with_late_fact = replay(
        symbol, {**first, "public_context": [*first["public_context"], late_fact]}, changed
    )
    assert replay_with_late_fact["candidate"] == short
    assert replay_with_late_fact["public_context"] == first["public_context"]


def test_missing_candle_blocks_replay():
    spec = json.loads((EXAMPLE_DIR / "events.json").read_text(encoding="utf-8"))
    rows = json.loads((EXAMPLE_DIR / "candles.json").read_text(encoding="utf-8"))
    with pytest.raises(ValueError, match="missing 1 M1 candle"):
        replay(spec["symbol"], spec["cases"][0], rows[1:])
