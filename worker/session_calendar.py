"""Regular NASDAQ/NYSE cash-session clock for daily bars.

Recurring full holidays follow the weekday rules (Saturday observed on Friday,
Sunday observed on Monday). One-off closures, including days of mourning, are
not in this calendar and are not invented.

A regular session closes at 16:00 America/New_York. A scheduled early close
is 13:00 America/New_York. A date that is not a session returns None so the
caller can reject it instead of storing 16:00.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from worker.types import DAILY_SESSION_CLOSE_HOUR, SESSION_TIMEZONE

EARLY_CLOSE = time(13, 0)
REGULAR_CLOSE = time(DAILY_SESSION_CLOSE_HOUR, 0)
SESSION_TZ = ZoneInfo(SESSION_TIMEZONE)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    shift = (weekday - first.weekday()) % 7
    return first + timedelta(days=shift + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        cursor = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        cursor = date(year, month + 1, 1) - timedelta(days=1)
    return cursor - timedelta(days=(cursor.weekday() - weekday) % 7)


def _easter(year: int) -> date:
    """Anonymous Gregorian algorithm. Good Friday is two days earlier."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    ell = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ell) // 451
    month = (h + ell - 7 * m + 114) // 31
    day = ((h + ell - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _observed(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def full_holidays(year: int) -> set[date]:
    days = {
        _observed(date(year, 1, 1)),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _easter(year) - timedelta(days=2),
        _last_weekday(year, 5, 0),
        _observed(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed(date(year, 12, 25)),
    }
    if year >= 2022:
        days.add(_observed(date(year, 6, 19)))
    # Jan 1 of the next year can be observed on Dec 31 of this year.
    observed_next_new_year = _observed(date(year + 1, 1, 1))
    days.add(observed_next_new_year)
    return {day for day in days if day.year == year}


def is_early_close(day: date) -> bool:
    if day in full_holidays(day.year) or day.weekday() >= 5:
        return False
    thanksgiving = _nth_weekday(day.year, 11, 3, 4)
    if day == thanksgiving + timedelta(days=1):
        return True
    christmas_eve = date(day.year, 12, 24)
    if day == christmas_eve and christmas_eve.weekday() < 5:
        return True
    july_third = date(day.year, 7, 3)
    independence = date(day.year, 7, 4)
    if day == july_third and july_third.weekday() < 5 and independence.weekday() != 5:
        return True
    return False


def session_close(day: date) -> time | None:
    if day.weekday() >= 5 or day in full_holidays(day.year):
        return None
    if is_early_close(day):
        return EARLY_CLOSE
    return REGULAR_CLOSE


def quote_time_for_session(day: date) -> datetime | None:
    close = session_close(day)
    if close is None:
        return None
    return datetime.combine(day, close, tzinfo=SESSION_TZ)
