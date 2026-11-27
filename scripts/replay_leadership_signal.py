"""Replay a leadership event from timestamped, normalized public evidence."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from scripts.replay_event_signal import parse_time


EXAMPLE = Path(__file__).resolve().parents[1] / "docs/examples/nvidia-ceo-return/event.json"
OFFICIAL_SOURCES = {"issuer", "regulator"}
EARLY_SOURCES = {"public_authority", "editorial"}


def replay(case: dict, as_of: datetime) -> dict:
    if as_of.utcoffset() is None:
        raise ValueError("as_of must include a timezone")
    expires_at = parse_time(case["expires_at"])
    evidence = []
    for item in case["evidence"]:
        published_at = parse_time(item["published_at"])
        observed_at = parse_time(item["observed_at"]) if item.get("observed_at") else None
        if observed_at is not None and observed_at < published_at:
            raise ValueError("observed_at cannot precede published_at")
        available_at = observed_at or published_at
        if available_at <= as_of and available_at < expires_at:
            evidence.append((available_at, item))

    status = "NO_SIGNAL"
    transitions = []
    for available_at, item in sorted(evidence, key=lambda record: record[0]):
        kind = item["kind"]
        source = item["source"]
        next_status = status
        if kind == "public_support" and source["kind"] in EARLY_SOURCES and status == "NO_SIGNAL":
            next_status = "WATCH"
        elif source["kind"] in OFFICIAL_SOURCES:
            next_status = {
                "reinstatement": "CONFIRMED",
                "rejection": "CANCELLED",
            }.get(kind, status)
        if next_status != status:
            status = next_status
            transitions.append({
                "status": status,
                "available_at": available_at.isoformat(),
                "published_at": item["published_at"],
                "observed_at": item.get("observed_at"),
                "occurred_date": item.get("occurred_date"),
                "source": source,
            })

    if status == "WATCH" and as_of >= expires_at:
        status = "EXPIRED"
        transitions.append({"status": status, "available_at": expires_at.isoformat()})

    return {
        "case_id": case["id"],
        "symbol": case["symbol"],
        "event_type": case["event_type"],
        "as_of": as_of.astimezone(timezone.utc).isoformat(),
        "status": status,
        "transitions": transitions,
        "entry_candidate": None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", type=Path, default=EXAMPLE)
    parser.add_argument("--as-of", required=True, help="ISO-8601 timestamp with timezone")
    args = parser.parse_args()
    try:
        case = json.loads(args.case.read_text(encoding="utf-8"))
        report = replay(case, parse_time(args.as_of))
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.error(str(exc))
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
