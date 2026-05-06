"""News Filter — block trades around HIGH-impact economic news releases.

Downloads economic calendar data from the free Forex Factory API,
caches locally, and provides O(log n) blackout window lookups.

Usage:
    nf = NewsFilter(cache_dir)
    nf.download_current_week()   # fetch this week's RED events
    nf.save_cache()              # persist to disk

    # Check if a pair is in a news blackout window
    if nf.is_blackout("EURUSD", time.time()):
        skip_trade()

Currency-aware: USD news only blocks USD-containing pairs (EURUSD, GBPUSD, etc.)
but NOT pairs like EURGBP.
"""

from __future__ import annotations

import json
import logging
import time
from bisect import bisect_right
from datetime import datetime, timezone, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

# Forex Factory free calendar API (faireconomy.media mirror)
_FF_API_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

# Currencies we track — must match strength.py CURRENCIES
_TRACKED_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CAD", "AUD", "NZD", "CHF"}

# Default blackout windows (minutes)
DEFAULT_PRE_NEWS_MIN = 30
DEFAULT_POST_NEWS_MIN = 60

_CACHE_FILENAME = "economic_calendar.json"


class NewsFilter:
    """Manages economic calendar data and blackout window queries.

    Downloads HIGH-impact events from Forex Factory, caches them locally,
    and provides fast currency-aware blackout checks for both live trading
    and backtesting.
    """

    def __init__(self, cache_dir: Path | None = None) -> None:
        import sys
        if cache_dir:
            self._cache_dir = cache_dir
        elif getattr(sys, "frozen", False):
            self._cache_dir = Path(sys.executable).parent / "data"
        else:
            self._cache_dir = (
                Path(__file__).resolve().parent.parent.parent / "data"
            )
        self._cache_file = self._cache_dir / _CACHE_FILENAME

        # All cached events: list of {time, currency, title, impact}
        self._events: list[dict] = []

        # Pre-computed blackout intervals per currency for fast lookups
        # {currency: sorted list of (blackout_start_ts, blackout_end_ts)}
        self._intervals: dict[str, list[tuple[float, float]]] = {}
        # Sorted event timestamps per currency (for bisect)
        self._event_times: dict[str, list[float]] = {}

        self._loaded = False

    # ── Download ──────────────────────────────────────────────────

    def download_current_week(self) -> int:
        """Fetch this week's HIGH-impact events from Forex Factory API.

        Merges new events into the existing cache (deduplicates by time+currency).
        Returns number of new RED events found.
        """
        import urllib.request
        import urllib.error

        try:
            req = urllib.request.Request(
                _FF_API_URL,
                headers={"User-Agent": "TAKUMI-Trader/1.0"},
            )
            resp = urllib.request.urlopen(req, timeout=15)
            raw = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
            logger.warning("Failed to download calendar from API: %s", e)
            return 0

        if not isinstance(raw, list):
            logger.warning("Unexpected API response format")
            return 0

        # Filter to HIGH impact events on tracked currencies
        new_events: list[dict] = []
        for ev in raw:
            if ev.get("impact") != "High":
                continue
            ccy = ev.get("country", "").upper()
            if ccy not in _TRACKED_CURRENCIES:
                continue

            date_str = ev.get("date", "")
            if not date_str:
                continue

            # Parse ISO 8601 date → unix timestamp
            try:
                dt = datetime.fromisoformat(date_str)
                ts = dt.timestamp()
            except (ValueError, TypeError):
                continue

            new_events.append({
                "time": ts,
                "currency": ccy,
                "title": ev.get("title", ""),
                "impact": "High",
            })

        if not new_events:
            logger.info("No new RED events found in current week")
            return 0

        # Merge with existing (deduplicate by time + currency)
        existing_keys = {
            (e["time"], e["currency"]) for e in self._events
        }
        added = 0
        for ev in new_events:
            key = (ev["time"], ev["currency"])
            if key not in existing_keys:
                self._events.append(ev)
                existing_keys.add(key)
                added += 1

        if added > 0:
            self._build_intervals()
            logger.info(
                "Calendar updated: %d new RED events (total: %d)",
                added, len(self._events),
            )

        return added

    # ── Cache Persistence ─────────────────────────────────────────

    def load_cache(self) -> bool:
        """Load cached events from disk. Returns True if cache exists."""
        if not self._cache_file.exists():
            logger.info("No news calendar cache at %s", self._cache_file)
            return False

        try:
            data = json.loads(
                self._cache_file.read_text(encoding="utf-8")
            )
            if isinstance(data, list):
                self._events = data
                self._build_intervals()
                self._loaded = True
                logger.info(
                    "Loaded %d cached RED news events", len(self._events)
                )
                return True
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to load news cache: %s", e)

        return False

    def save_cache(self) -> None:
        """Persist events to disk."""
        try:
            self._cache_dir.mkdir(parents=True, exist_ok=True)
            # Sort by time for readability
            sorted_events = sorted(self._events, key=lambda e: e["time"])
            self._cache_file.write_text(
                json.dumps(sorted_events, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            logger.info(
                "Saved %d events to %s", len(sorted_events), self._cache_file
            )
        except OSError as e:
            logger.warning("Failed to save news cache: %s", e)

    @property
    def loaded(self) -> bool:
        """Whether any FRESH events are loaded.

        Returns False if all cached events are more than 24 hours old
        (stale calendar = no protection). A stale calendar is treated as
        "not loaded" to prevent silent loss of news protection.
        """
        if not (self._loaded or len(self._events) > 0):
            return False
        if not self._events:
            return False
        # Check if we have any events within the last or next 7 days.
        # NOTE: events use key "time" (not "timestamp")
        import time as _t
        now = _t.time()
        week = 7 * 86400
        has_fresh = any(
            abs(ev.get("time", 0) - now) < week
            for ev in self._events
        )
        if not has_fresh:
            logger.warning("News calendar is stale — all events >7d old")
            return False
        return True

    @property
    def event_count(self) -> int:
        """Total cached events."""
        return len(self._events)

    # ── Blackout Queries ──────────────────────────────────────────

    def is_blackout(
        self,
        pair: str,
        unix_ts: float,
        pre_min: int = DEFAULT_PRE_NEWS_MIN,
        post_min: int = DEFAULT_POST_NEWS_MIN,
    ) -> bool:
        """Check if a pair is inside a news blackout window.

        A blackout window = [event_time - pre_min, event_time + post_min].
        Returns True if base_ccy OR quote_ccy has a RED event in window.

        Uses pre-computed sorted intervals with binary search for O(log n).
        """
        if not self._events:
            return False

        base_ccy = pair[:3].upper()
        quote_ccy = pair[3:6].upper()

        for ccy in (base_ccy, quote_ccy):
            if ccy not in self._event_times:
                continue

            times = self._event_times[ccy]
            if not times:
                continue

            # Find the insertion point for unix_ts
            idx = bisect_right(times, unix_ts)

            # Check the event BEFORE this point (already started?)
            # and the event AT/AFTER this point (upcoming?)
            for i in range(max(0, idx - 1), min(len(times), idx + 1)):
                event_ts = times[i]
                window_start = event_ts - pre_min * 60
                window_end = event_ts + post_min * 60
                if window_start <= unix_ts <= window_end:
                    return True

        return False

    def get_blocking_event(
        self,
        pair: str,
        unix_ts: float,
        pre_min: int = DEFAULT_PRE_NEWS_MIN,
        post_min: int = DEFAULT_POST_NEWS_MIN,
    ) -> dict | None:
        """Return the event causing a blackout, or None if not blocked."""
        if not self._events:
            return None

        base_ccy = pair[:3].upper()
        quote_ccy = pair[3:6].upper()

        for ccy in (base_ccy, quote_ccy):
            if ccy not in self._event_times:
                continue

            times = self._event_times[ccy]
            events = self._events_by_ccy.get(ccy, [])
            if not times:
                continue

            idx = bisect_right(times, unix_ts)
            for i in range(max(0, idx - 1), min(len(times), idx + 1)):
                event_ts = times[i]
                if (event_ts - pre_min * 60) <= unix_ts <= (event_ts + post_min * 60):
                    # Find matching event
                    for ev in events:
                        if ev["time"] == event_ts:
                            return ev
        return None

    def get_upcoming_events(
        self,
        pair: str,
        unix_ts: float,
        horizon_hours: float = 4.0,
    ) -> list[dict]:
        """Get upcoming RED events for a pair within the horizon window."""
        if not self._events:
            return []

        base_ccy = pair[:3].upper()
        quote_ccy = pair[3:6].upper()
        horizon_end = unix_ts + horizon_hours * 3600

        upcoming: list[dict] = []
        for ccy in (base_ccy, quote_ccy):
            for ev in self._events_by_ccy.get(ccy, []):
                if unix_ts <= ev["time"] <= horizon_end:
                    upcoming.append(ev)

        upcoming.sort(key=lambda e: e["time"])
        return upcoming

    # ── Internal ──────────────────────────────────────────────────

    def _build_intervals(self) -> None:
        """Pre-sort events by currency for fast bisect lookups."""
        self._event_times = {}
        self._events_by_ccy: dict[str, list[dict]] = {}

        for ev in self._events:
            ccy = ev.get("currency", "")
            if ccy not in _TRACKED_CURRENCIES:
                continue

            if ccy not in self._event_times:
                self._event_times[ccy] = []
                self._events_by_ccy[ccy] = []

            self._event_times[ccy].append(ev["time"])
            self._events_by_ccy[ccy].append(ev)

        # Sort each currency's event times for binary search
        for ccy in self._event_times:
            paired = sorted(
                zip(self._event_times[ccy], self._events_by_ccy[ccy]),
                key=lambda x: x[0],
            )
            if paired:
                self._event_times[ccy] = [p[0] for p in paired]
                self._events_by_ccy[ccy] = [p[1] for p in paired]

        self._loaded = True

    def get_date_range(self) -> tuple[str, str] | None:
        """Return (earliest_date, latest_date) of cached events, or None."""
        if not self._events:
            return None
        earliest = min(e["time"] for e in self._events)
        latest = max(e["time"] for e in self._events)
        return (
            datetime.fromtimestamp(earliest, tz=timezone.utc).strftime("%Y-%m-%d"),
            datetime.fromtimestamp(latest, tz=timezone.utc).strftime("%Y-%m-%d"),
        )
