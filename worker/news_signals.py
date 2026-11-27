"""Point-in-time event status from Finam news, following the PR #12 rules.

A public message (any source not listed as official) moves an event to WATCH.
An official disclosure confirms the fact: CONFIRMED. Only versions available
to AEGIS by `as_of` are used, and a deleted latest version withdraws its story.

Dividend stages are separate events, so a board recommendation never
confirms a shareholder decision or a record date. For X5 the registry asks
for this split explicitly; the same patterns apply to every ticker.
"""

from __future__ import annotations

import json
import re
from datetime import datetime

from worker.finam_news import NewsItem, NewsLedger
from worker.russian_market import REGISTRY_FILE

NO_SIGNAL = "NO_SIGNAL"
WATCH = "WATCH"
CONFIRMED = "CONFIRMED"

DIVIDEND_RECOMMENDATION = "DIVIDEND_RECOMMENDATION"
SHAREHOLDER_DECISION = "SHAREHOLDER_DECISION"
RECORD_DATE = "RECORD_DATE"
COMPANY_NEWS = "COMPANY_NEWS"

_DIVIDEND = re.compile(r"дивиденд|dividend", re.IGNORECASE)
_RECOMMENDATION = re.compile(r"рекомендова|recommend", re.IGNORECASE)
_SHAREHOLDERS = re.compile(
    r"собрани\w*\s+акционер|\bгоса\b|\bвоса\b|акционеры\s+(?:одобрил|утвердил)|"
    r"shareholders?\s+(?:approv|meeting)|\bagm\b|\begm\b",
    re.IGNORECASE,
)
_RECORD_DATE = re.compile(
    r"закрыти\w*\s+реестр|отсечк|дат\w*\s+(?:составления\s+списка|фиксации)|record\s+date",
    re.IGNORECASE,
)


def official_sources() -> frozenset[str]:
    registry = json.loads(REGISTRY_FILE.read_text(encoding="utf-8"))
    return frozenset(registry.get("news_sources", {}).get("official", []))


def event_types(item: NewsItem) -> list[str]:
    text = item.title
    if not _DIVIDEND.search(text):
        return [COMPANY_NEWS]
    kinds = []
    if _RECOMMENDATION.search(text):
        kinds.append(DIVIDEND_RECOMMENDATION)
    if _SHAREHOLDERS.search(text):
        kinds.append(SHAREHOLDER_DECISION)
    if _RECORD_DATE.search(text):
        kinds.append(RECORD_DATE)
    return kinds or [COMPANY_NEWS]


def event_status(
    ledger: NewsLedger,
    symbol: str,
    event_type: str,
    as_of: datetime,
    official: frozenset[str] | None = None,
) -> dict:
    if as_of.utcoffset() is None:
        raise ValueError("as_of must include a timezone")
    official = official_sources() if official is None else official
    evidence = [
        item for item in ledger.known_at(as_of)
        if symbol in item.symbols and not item.deleted and event_type in event_types(item)
    ]
    status = NO_SIGNAL
    transitions = []
    for item in sorted(evidence, key=lambda row: (row.available_at, row.story)):
        kind = "official" if item.source in official else "public"
        next_status = CONFIRMED if kind == "official" else (WATCH if status == NO_SIGNAL else status)
        if next_status != status:
            status = next_status
            transitions.append({
                "status": status,
                "available_at": item.available_at.isoformat(),
                "published_at": item.published_at.isoformat(),
                "provider_received_at": item.provider_received_at.isoformat() if item.provider_received_at else None,
                "source": item.source,
                "source_kind": kind,
                "news_id": item.news_id,
                "version": item.version,
                "title": item.title,
            })
    return {
        "symbol": symbol,
        "event_type": event_type,
        "as_of": as_of.isoformat(),
        "status": status,
        "transitions": transitions,
        "entry_candidate": None,
    }


def active_events(ledger: NewsLedger, symbols, as_of: datetime, official: frozenset[str] | None = None) -> list[dict]:
    official = official_sources() if official is None else official
    pairs = sorted({
        (symbol, kind)
        for item in ledger.known_at(as_of) if not item.deleted
        for symbol in item.symbols if symbol in symbols
        for kind in event_types(item)
    })
    return [event_status(ledger, symbol, kind, as_of, official) for symbol, kind in pairs]
