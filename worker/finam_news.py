"""Normalize Finam news history and keep every version once.

A news item is identified by (source, news_id, version). Finam documents that
history pages may repeat an event and that the client removes duplicates. A
new version of the same (source, news_id) is a correction; earlier versions
are kept so that a replay can see what was known at the time.

`observed_at` is when AEGIS first received the version. It is never earlier
than `published_at`, so a publication cannot become available before it exists.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from worker.russian_market import RU_CONTEXT_SYMBOLS, RU_WATCHLIST, finam_symbol

_TICKER_BY_FINAM = {finam_symbol(symbol): symbol for symbol in (*RU_WATCHLIST, *RU_CONTEXT_SYMBOLS)}


@dataclass(frozen=True)
class NewsItem:
    source: str
    news_id: str
    version: str
    title: str
    url: str | None
    published_at: datetime
    updated_at: datetime | None
    provider_received_at: datetime | None
    observed_at: datetime
    symbols: tuple[str, ...]
    deleted: bool
    source_version: str | None
    language: str | None

    @property
    def key(self) -> tuple[str, str, str]:
        return self.source, self.news_id, self.version

    @property
    def story(self) -> tuple[str, str]:
        return self.source, self.news_id

    @property
    def available_at(self) -> datetime:
        return max(self.observed_at, self.published_at)

    def to_dict(self) -> dict:
        row = asdict(self)
        for name in ("published_at", "updated_at", "provider_received_at", "observed_at"):
            row[name] = row[name].isoformat() if row[name] else None
        row["symbols"] = list(self.symbols)
        row["available_at"] = self.available_at.isoformat()
        return row


def _time(value, *, required: bool = False) -> datetime | None:
    if value in (None, ""):
        if required:
            raise ValueError("missing timestamp")
        return None
    if not isinstance(value, str):
        raise ValueError("timestamp must be RFC3339")
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    return stamp.astimezone(timezone.utc)


def parse_news(row: dict, observed_at: datetime) -> NewsItem:
    if observed_at.utcoffset() is None:
        raise ValueError("observed_at must include a timezone")
    source, news_id = row.get("source"), row.get("id")
    if not isinstance(source, str) or not source or not isinstance(news_id, str) or not news_id:
        raise ValueError("news source and id are required")
    version = row.get("version")
    symbols = row.get("symbols") or []
    if not isinstance(symbols, list):
        raise ValueError("symbols must be a list")
    return NewsItem(
        source=source,
        news_id=news_id,
        version=str(version) if version not in (None, "") else "0",
        title=str(row.get("title") or ""),
        url=row.get("url") or None,
        published_at=_time(row.get("published_at"), required=True),
        updated_at=_time(row.get("updated_at")),
        provider_received_at=_time(row.get("received_at")),
        observed_at=observed_at.astimezone(timezone.utc),
        symbols=tuple(sorted({_TICKER_BY_FINAM[s] for s in symbols if s in _TICKER_BY_FINAM})),
        deleted=bool(row.get("deleted")),
        source_version=row.get("source_version") or None,
        language=row.get("language") or None,
    )


def _version_order(item: NewsItem) -> tuple:
    numeric = item.version.isdigit()
    return (
        item.updated_at or item.published_at,
        numeric,
        int(item.version) if numeric else 0,
        item.version,
    )


class NewsLedger:
    """All observed versions, keyed by (source, news_id, version)."""

    def __init__(self):
        self.items: dict[tuple[str, str, str], NewsItem] = {}

    def add(self, items) -> dict:
        counts = {"new": 0, "duplicates": 0, "corrections": 0}
        stories = {item.story for item in self.items.values()}
        for item in items:
            if item.key in self.items:
                # The first observation time is the one that matters for latency.
                counts["duplicates"] += 1
                continue
            counts["corrections" if item.story in stories else "new"] += 1
            self.items[item.key] = item
            stories.add(item.story)
        return counts

    def known_at(self, as_of: datetime) -> list[NewsItem]:
        """The latest version of each story that AEGIS had seen by `as_of`."""
        latest: dict[tuple[str, str], NewsItem] = {}
        for item in self.items.values():
            if item.available_at > as_of:
                continue
            current = latest.get(item.story)
            if current is None or _version_order(item) > _version_order(current):
                latest[item.story] = item
        return sorted(latest.values(), key=lambda item: (item.published_at, item.story), reverse=True)


def ingest_history(ledger: NewsLedger, rows: list[dict], observed_at: datetime) -> dict:
    parsed, rejected = [], 0
    for row in rows:
        try:
            parsed.append(parse_news(row, observed_at))
        except (ValueError, TypeError, AttributeError):
            rejected += 1
    counts = ledger.add(parsed)
    counts["rejected"] = rejected
    counts["received"] = len(rows)
    return counts
