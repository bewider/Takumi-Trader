"""Economic calendar fetcher — ForexFactory unofficial scraping.

ForexFactory does NOT publish an official API but exposes a JSON feed at
https://nfs.faireconomy.media/ff_calendar_thisweek.json

This is FREE (no monthly fee) but unofficial — fragile to layout changes.
Cache aggressively to be a polite scraper.

Alternative: Investing.com's calendar (also free, requires more parsing).
"""
from __future__ import annotations

import json
import urllib.request
from pathlib import Path
from datetime import datetime, timezone, timedelta

UTC = timezone.utc

CACHE_DIR = Path(".feature_cache")
CACHE_DIR.mkdir(exist_ok=True)

# Mapping from ForexFactory's currency codes to ISO codes
FF_CURRENCY_MAP = {
    "USD": "USD", "EUR": "EUR", "GBP": "GBP", "JPY": "JPY",
    "CAD": "CAD", "AUD": "AUD", "NZD": "NZD", "CHF": "CHF",
    "CNY": "CNY",  # China — relevant for AUD
}


def fetch_forex_factory_thisweek(cache_hours: int = 4) -> list[dict]:
    """Fetch this week's events from ForexFactory.

    Returns list of dicts:
        {date_utc, currency, impact, title, forecast, previous}
    """
    cache_path = CACHE_DIR / "ff_calendar_thisweek.json"
    if cache_path.exists():
        mtime = cache_path.stat().st_mtime
        if (datetime.now().timestamp() - mtime) / 3600 < cache_hours:
            try:
                return json.loads(cache_path.read_text())
            except Exception:
                pass
    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0 TAKUMI-features"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode())
    except Exception:
        return []

    events = []
    for item in payload:
        try:
            # FF format: "2026-04-29T08:30:00-04:00"
            dt_str = item.get("date", "")
            if not dt_str:
                continue
            # Parse and convert to UTC
            dt = datetime.fromisoformat(dt_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            ts_utc = int(dt.astimezone(UTC).timestamp())
            events.append({
                "ts_utc": ts_utc,
                "currency": item.get("country", ""),
                "impact": item.get("impact", "Low"),
                "title": item.get("title", ""),
                "forecast": item.get("forecast", ""),
                "previous": item.get("previous", ""),
            })
        except Exception:
            continue
    cache_path.write_text(json.dumps(events))
    return events


def fetch_next_week(cache_hours: int = 24) -> list[dict]:
    """Fetch next week's events. Same structure."""
    cache_path = CACHE_DIR / "ff_calendar_nextweek.json"
    if cache_path.exists():
        mtime = cache_path.stat().st_mtime
        if (datetime.now().timestamp() - mtime) / 3600 < cache_hours:
            try:
                return json.loads(cache_path.read_text())
            except Exception:
                pass
    url = "https://nfs.faireconomy.media/ff_calendar_nextweek.json"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode())
    except Exception:
        return []
    events = []
    for item in payload:
        try:
            dt = datetime.fromisoformat(item.get("date", ""))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            events.append({
                "ts_utc": int(dt.astimezone(UTC).timestamp()),
                "currency": item.get("country", ""),
                "impact": item.get("impact", "Low"),
                "title": item.get("title", ""),
                "forecast": item.get("forecast", ""),
                "previous": item.get("previous", ""),
            })
        except Exception:
            continue
    cache_path.write_text(json.dumps(events))
    return events


def get_all_events() -> list[dict]:
    """Get this week + next week events."""
    return fetch_forex_factory_thisweek() + fetch_next_week()


def time_to_next_event(ts_utc: int, currencies: list[str], min_impact: str = "High") -> tuple:
    """Return (seconds_to_next_event, event_dict) for next event affecting any
    of the given currencies with at least `min_impact`.

    impact ranking: High > Medium > Low > Holiday
    """
    impact_order = {"High": 3, "Medium": 2, "Low": 1, "Holiday": 0}
    min_rank = impact_order.get(min_impact, 0)
    events = get_all_events()
    upcoming = [
        e for e in events
        if e["ts_utc"] > ts_utc
        and e["currency"] in currencies
        and impact_order.get(e["impact"], 0) >= min_rank
    ]
    if not upcoming:
        return (-1, None)
    nearest = min(upcoming, key=lambda e: e["ts_utc"])
    return (nearest["ts_utc"] - ts_utc, nearest)


def time_since_last_event(ts_utc: int, currencies: list[str], min_impact: str = "High") -> tuple:
    """Return (seconds_since_last_event, event_dict) for nearest past event."""
    impact_order = {"High": 3, "Medium": 2, "Low": 1, "Holiday": 0}
    min_rank = impact_order.get(min_impact, 0)
    events = get_all_events()
    past = [
        e for e in events
        if e["ts_utc"] <= ts_utc
        and e["currency"] in currencies
        and impact_order.get(e["impact"], 0) >= min_rank
    ]
    if not past:
        return (-1, None)
    nearest = max(past, key=lambda e: e["ts_utc"])
    return (ts_utc - nearest["ts_utc"], nearest)


def is_news_blackout(ts_utc: int, currencies: list[str],
                      pre_minutes: int = 5, post_minutes: int = 5,
                      min_impact: str = "High") -> tuple[bool, dict]:
    """True if within ±N min of high-impact event for any currency in list.

    Returns (in_blackout, event_or_None).
    """
    sec_to_next, next_ev = time_to_next_event(ts_utc, currencies, min_impact)
    sec_since_last, last_ev = time_since_last_event(ts_utc, currencies, min_impact)
    if sec_to_next > 0 and sec_to_next <= pre_minutes * 60:
        return (True, next_ev)
    if sec_since_last >= 0 and sec_since_last <= post_minutes * 60:
        return (True, last_ev)
    return (False, None)


def events_today(ts_utc: int, currencies: list[str] = None) -> list[dict]:
    """All events on the same calendar day (UTC) affecting the currencies."""
    events = get_all_events()
    dt = datetime.fromtimestamp(ts_utc, tz=UTC).date()
    out = []
    for e in events:
        ev_date = datetime.fromtimestamp(e["ts_utc"], tz=UTC).date()
        if ev_date != dt:
            continue
        if currencies and e["currency"] not in currencies:
            continue
        out.append(e)
    return sorted(out, key=lambda x: x["ts_utc"])


# ──────────────────────────────────────────────────────────────────
# Static central bank meeting schedule (for years where API may miss)
# ──────────────────────────────────────────────────────────────────
# Update annually. Sample for 2026.

CB_MEETINGS_2026 = {
    "FOMC": [
        # Approximate — verify on Fed's site
        ("2026-01-28", "FOMC Rate Decision"),
        ("2026-03-18", "FOMC Rate Decision + SEP"),
        ("2026-04-29", "FOMC Rate Decision"),
        ("2026-06-17", "FOMC Rate Decision + SEP"),
        ("2026-07-29", "FOMC Rate Decision"),
        ("2026-09-16", "FOMC Rate Decision + SEP"),
        ("2026-11-04", "FOMC Rate Decision"),
        ("2026-12-16", "FOMC Rate Decision + SEP"),
    ],
}
