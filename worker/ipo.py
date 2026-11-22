"""Read company IPO calendars for research."""

from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from worker.russian_market import RU_SESSION_TIMEZONE

MOEX_IPO_URL = "https://www.moex.com/ipo"
NASDAQ_IPO_URL = "https://www.nasdaq.com/market-activity/ipos"
NASDAQ_API_URL = "https://api.nasdaq.com/api/ipo/calendar"
MOEX_STAGES = {"planned": "planned", "inProgress": "in_progress", "completed": "completed"}
NASDAQ_DATE_FIELDS = {
    "upcoming": ("expectedPriceDate", "expected_ipo_date"),
    "priced": ("pricedDate", "calendar_priced_date"),
    "filed": ("filedDate", "filing_date"),
    "withdrawn": ("withdrawDate", "withdrawal_date"),
}


class _NextPageData(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reading = False
        self.chunks: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "script":
            self.reading = dict(attrs).get("id") == "__NEXT_DATA__"

    def handle_endtag(self, tag):
        if tag == "script":
            self.reading = False

    def handle_data(self, data):
        if self.reading:
            self.chunks.append(data)


def _optional_text(row: dict, field: str) -> str | None:
    value = row.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"invalid {field}")
    return value.strip() or None


def parse_moex_calendar(page: str) -> list[dict]:
    parser = _NextPageData()
    parser.feed(page)
    document = json.loads("".join(parser.chunks))
    components = document["props"]["pageProps"]["recursiveComponentContentProps"]
    tables = [
        data for component in components.values()
        if isinstance(component, dict)
        and isinstance(data := component.get("initData"), dict)
        and MOEX_STAGES.keys() <= data.keys()
    ]
    if len(tables) != 1:
        raise ValueError("MOEX IPO table missing or ambiguous")

    entries = []
    for stage, status in MOEX_STAGES.items():
        rows = tables[0][stage]
        if not isinstance(rows, list):
            raise ValueError("invalid MOEX IPO rows")
        for row in rows:
            event_date = _optional_text(row, "tradingStartDate")
            entries.append({
                "id": row["contentItemId"], "company": row["emitterFullName"],
                "symbol": _optional_text(row, "secId"), "exchange": "MOEX",
                "status": status, "date": date.fromisoformat(event_date).isoformat() if event_date else None,
                "date_kind": "exchange_trading_start" if status == "completed" else "scheduled_trading_start",
                "offer_price": _optional_text(row, "placementSharePrice"),
                "price_kind": "offer_price" if status == "completed" else "proposed_offer_price",
                "currency": "RUB", "source_url": MOEX_IPO_URL,
            })
    return entries


def parse_nasdaq_calendar(document: dict) -> list[dict]:
    if document["status"]["rCode"] != 200:
        raise ValueError("Nasdaq IPO calendar request rejected")
    tables = document["data"]
    entries = []
    for stage, (field, date_kind) in NASDAQ_DATE_FIELDS.items():
        table = tables[stage]
        if stage == "upcoming":
            table = table["upcomingTable"]
        # The calendar returns null rows when the month has no announcements.
        rows = table["rows"]
        if rows is None:
            continue
        if not isinstance(rows, list):
            raise ValueError("invalid Nasdaq IPO rows")
        for row in rows:
            event_date = _optional_text(row, field)
            entries.append({
                "id": row["dealID"], "company": row["companyName"],
                "symbol": _optional_text(row, "proposedTickerSymbol"),
                "exchange": _optional_text(row, "proposedExchange"),
                "status": "expected" if stage == "upcoming" else stage,
                "date": datetime.strptime(event_date, "%m/%d/%Y").date().isoformat() if event_date else None,
                "date_kind": date_kind, "offer_price": _optional_text(row, "proposedSharePrice"),
                "price_kind": "offer_price" if stage == "priced" else "proposed_offer_price",
                "currency": "USD", "source_url": NASDAQ_IPO_URL,
            })
    return entries


def _read_source(market: str, month: str) -> dict:
    source = {
        "market": market,
        "url": MOEX_IPO_URL if market == "moex" else NASDAQ_IPO_URL,
        "scope": "published placements on the MOEX IPO page" if market == "moex" else month,
    }
    url = MOEX_IPO_URL if market == "moex" else f"{NASDAQ_API_URL}?{urlencode({'date': month})}"
    media_type = "text/html" if market == "moex" else "application/json"
    request = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": media_type})
    try:
        with urlopen(request, timeout=12) as response:
            body = response.read(1_000_001)
        if len(body) > 1_000_000:
            raise ValueError("calendar response exceeds size limit")
        if market == "moex":
            entries = parse_moex_calendar(body.decode("utf-8"))
        else:
            entries = parse_nasdaq_calendar(json.loads(body))
        if any(not isinstance(entry["company"], str) or not entry["company"].strip() for entry in entries):
            raise ValueError("calendar returned a missing or invalid company name")
        return {**source, "status": "ok", "entries": entries}
    except HTTPError as exc:
        reason = f"HTTP {exc.code}"
    except OSError:
        reason = "connection failed or timed out"
    except (AttributeError, KeyError, TypeError, ValueError):
        reason = "unexpected calendar format"
    return {**source, "status": "unavailable", "error": reason, "entries": []}


async def search_ipos(market: str, month: str | None, query: str | None) -> dict:
    today = datetime.now(ZoneInfo(RU_SESSION_TIMEZONE)).date()
    if month is None:
        next_month = date(today.year + (today.month == 12), today.month % 12 + 1, 1)
        months = [today.strftime("%Y-%m"), next_month.strftime("%Y-%m")]
    else:
        months = [month]
    lookups = [("moex", months[0])] if market in ("all", "moex") else []
    if market in ("all", "us"):
        lookups.extend(("us", selected) for selected in months)
    sources = await asyncio.gather(*(asyncio.to_thread(_read_source, name, selected) for name, selected in lookups))
    if query:
        needle = query.strip().casefold()
        for source in sources:
            source["entries"] = [
                entry for entry in source["entries"]
                if needle in entry["company"].casefold() or needle in (entry["symbol"] or "").casefold()
            ]
    statuses = {source["status"] for source in sources}
    return {
        "observed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "status": next(iter(statuses)) if len(statuses) == 1 else "partial",
        "sources": sources,
    }
