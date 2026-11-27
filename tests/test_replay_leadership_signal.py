import json

import pytest

from scripts.replay_leadership_signal import EXAMPLE, parse_time, replay


@pytest.fixture
def nvidia_case():
    return json.loads(EXAMPLE.read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    ("as_of", "status", "transitions"),
    [
        ("2026-11-20T21:30:59+03:00", "NO_SIGNAL", []),
        ("2026-11-20T21:31:00+03:00", "WATCH", ["WATCH"]),
        ("2026-11-20T23:36:00+03:00", "WATCH", ["WATCH"]),
        ("2026-11-22T23:59:00-05:00", "WATCH", ["WATCH"]),
        ("2026-11-23T16:54:59+03:00", "WATCH", ["WATCH"]),
        ("2026-11-23T16:55:00+03:00", "CONFIRMED", ["WATCH", "CONFIRMED"]),
        ("2026-11-23T17:31:00+03:00", "CONFIRMED", ["WATCH", "CONFIRMED"]),
    ],
)
def test_nvidia_replay_uses_only_available_evidence(nvidia_case, as_of, status, transitions):
    result = replay(nvidia_case, parse_time(as_of))
    assert result["status"] == status
    assert [item["status"] for item in result["transitions"]] == transitions
    assert result["entry_candidate"] is None


def test_media_report_of_reinstatement_cannot_confirm_fact(nvidia_case):
    nvidia_case["evidence"][1]["source"]["kind"] = "editorial"
    result = replay(nvidia_case, parse_time("2026-11-23T17:00:00+03:00"))
    assert result["status"] == "WATCH"


def test_recognition_waits_for_observed_evidence(nvidia_case):
    nvidia_case["evidence"][1]["observed_at"] = "2026-11-23T17:05:00+03:00"
    waiting = replay(nvidia_case, parse_time("2026-11-23T17:00:00+03:00"))
    assert waiting["status"] == "WATCH"
    confirmed = replay(nvidia_case, parse_time("2026-11-23T17:05:00+03:00"))
    assert confirmed["transitions"][-1]["available_at"] == "2026-11-23T14:05:00+00:00"
    assert confirmed["status"] == "CONFIRMED"


def test_watch_expires_if_source_is_discovered_after_market_open(nvidia_case):
    nvidia_case["evidence"][1]["observed_at"] = "2026-11-23T17:31:00+03:00"
    result = replay(nvidia_case, parse_time("2026-11-23T17:31:00+03:00"))
    assert result["status"] == "EXPIRED"
    assert result["transitions"][-1]["available_at"] == "2026-11-23T14:30:00+00:00"


def test_official_rejection_cancels_watch(nvidia_case):
    nvidia_case["evidence"][1]["kind"] = "rejection"
    result = replay(nvidia_case, parse_time("2026-11-23T17:00:00+03:00"))
    assert result["status"] == "CANCELLED"


def test_repeated_support_does_not_repeat_alert(nvidia_case):
    nvidia_case["evidence"].append(nvidia_case["evidence"][0].copy())
    result = replay(nvidia_case, parse_time("2026-11-20T23:36:00+03:00"))
    assert [item["status"] for item in result["transitions"]] == ["WATCH"]
