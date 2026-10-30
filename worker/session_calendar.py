"""Regular NASDAQ/NYSE cash-session clock for daily bars.

Recurring full holidays follow the weekday rules: a Saturday holiday is
observed on Friday, and a Sunday holiday is observed on Monday. One-off
closures, including days of mourning, are not in this calendar.

New Year's Day is the Nasdaq exception in Release 34-93675, footnote 9.
When January 1 falls on Saturday, the holiday is not moved to the preceding
Friday. That Friday stays a full session and closes at 16:00 America/New_York.
December 31, 2021 is the published example. A Sunday New Year's Day is still
observed on the following Monday.

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
    # A Saturday New Year's Day is not observed on December 31.
    # _observed() would return that Friday, so drop any date outside this year.
    return {day for day in days if day.year == year}


def legacy_sixteen_close(session_day: date) -> datetime | None:
    """Prior contract stamped every session at 16:00, including early closes.

    Returns that old instant when ``session_day`` is an early close. Callers
    use it to retarget an existing row instead of inserting a second key.
    """
    if session_close(session_day) != EARLY_CLOSE:
        return None
    return datetime.combine(session_day, REGULAR_CLOSE, tzinfo=SESSION_TZ)


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


def session_days(start: date, end: date) -> list[date]:
    """Cash sessions in ``[start, end)``. Weekends and full holidays are omitted."""
    if end <= start:
        return []
    days: list[date] = []
    cursor = start
    while cursor < end:
        if session_close(cursor) is not None:
            days.append(cursor)
        cursor += timedelta(days=1)
    return days


def quote_time_for_session(day: date) -> datetime | None:
    close = session_close(day)
    if close is None:
        return None
    return datetime.combine(day, close, tzinfo=SESSION_TZ)
