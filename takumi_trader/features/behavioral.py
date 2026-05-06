"""Behavioral / time-pattern features.

Pre-news anticipation, post-news drift, day-after-FOMC, Friday profit-taking,
session-open momentum, lunch-hour drift.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

UTC = timezone.utc
JST = timezone(timedelta(hours=9))


def is_pre_news_window(ts_utc: int, next_event_ts: int, pre_window_minutes: int = 30) -> bool:
    """True if within `pre_window_minutes` of next high-impact event."""
    if next_event_ts <= 0:
        return False
    delta_sec = next_event_ts - ts_utc
    return 0 < delta_sec <= pre_window_minutes * 60


def is_post_news_window(ts_utc: int, last_event_ts: int, post_window_minutes: int = 60) -> bool:
    """True if within `post_window_minutes` of last high-impact event."""
    if last_event_ts <= 0:
        return False
    delta_sec = ts_utc - last_event_ts
    return 0 <= delta_sec <= post_window_minutes * 60


def time_to_next_event_minutes(ts_utc: int, next_event_ts: int) -> float:
    """Minutes until next high-impact event."""
    if next_event_ts <= 0:
        return 9999.0
    return float(max(0, (next_event_ts - ts_utc) / 60.0))


def day_after_fomc(ts_utc: int, fomc_dates: list[int]) -> bool:
    """True if the calendar day after a FOMC announcement (UTC).

    Statistical: FOMC moves often have continuation 18-36 hours later.
    """
    if not fomc_dates:
        return False
    dt = datetime.fromtimestamp(ts_utc, tz=UTC).date()
    for fomc_ts in fomc_dates:
        fomc_date = datetime.fromtimestamp(fomc_ts, tz=UTC).date()
        if (dt - fomc_date).days == 1:
            return True
    return False


def is_friday_late(ts_utc: int) -> bool:
    """Friday after 18:00 UTC — typical profit-taking window."""
    dt = datetime.fromtimestamp(ts_utc, tz=UTC)
    return dt.weekday() == 4 and dt.hour >= 18


def is_sunday_open(ts_utc: int) -> bool:
    """Sunday 22:00 UTC onwards — Sydney/Tokyo Monday open with typical gap."""
    dt = datetime.fromtimestamp(ts_utc, tz=UTC)
    return dt.weekday() == 6 and dt.hour >= 22


def london_open_window(ts_utc: int, dst_active: bool = True) -> bool:
    """First 90 minutes of London session.

    DST: 07:00-08:30 UTC; non-DST: 08:00-09:30 UTC
    """
    dt = datetime.fromtimestamp(ts_utc, tz=UTC)
    hm = dt.hour * 60 + dt.minute
    if dst_active:
        return 420 <= hm < 510  # 07:00-08:30 UTC
    return 480 <= hm < 570  # 08:00-09:30 UTC


def ny_open_window(ts_utc: int, dst_active: bool = True) -> bool:
    """First 90 minutes of NY session."""
    dt = datetime.fromtimestamp(ts_utc, tz=UTC)
    hm = dt.hour * 60 + dt.minute
    if dst_active:
        return 780 <= hm < 870  # 13:00-14:30 UTC
    return 840 <= hm < 930  # 14:00-15:30 UTC


def lunch_hour_drift(ts_utc: int) -> str:
    """Identify which 'lunch hour' the timestamp falls in."""
    dt = datetime.fromtimestamp(ts_utc, tz=UTC)
    hm = dt.hour * 60 + dt.minute
    # Tokyo lunch: 03:00-04:00 UTC
    if 180 <= hm < 240:
        return "tokyo_lunch"
    # London lunch: 11:00-13:00 UTC
    if 660 <= hm < 780:
        return "london_lunch"
    # NY lunch: 16:00-17:30 UTC
    if 960 <= hm < 1050:
        return "ny_lunch"
    return ""


def days_until_next_fomc(ts_utc: int, fomc_schedule_utc: list[int]) -> int:
    """Days until next FOMC meeting (-1 if past or no schedule)."""
    if not fomc_schedule_utc:
        return -1
    upcoming = [t for t in fomc_schedule_utc if t > ts_utc]
    if not upcoming:
        return -1
    next_fomc = min(upcoming)
    return int((next_fomc - ts_utc) / 86400)


def days_into_quarter(ts_utc: int) -> int:
    """Days elapsed since start of current quarter."""
    dt = datetime.fromtimestamp(ts_utc, tz=UTC)
    quarter_month = ((dt.month - 1) // 3) * 3 + 1
    quarter_start = dt.replace(month=quarter_month, day=1, hour=0, minute=0, second=0, microsecond=0)
    return (dt - quarter_start).days


def is_holiday_thin_market(ts_utc: int, half_day_dates: list = None) -> bool:
    """True if known half-day or holiday-impacted session."""
    if half_day_dates is None:
        half_day_dates = []
    dt = datetime.fromtimestamp(ts_utc, tz=UTC).date()
    return dt in half_day_dates
