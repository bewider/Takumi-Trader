"""Average Daily Range (ADR) Calculator.

Calculates the full 24-hour average daily range over the past 2 weeks,
then tracks today's range to show how much of the ADR has been consumed.

Trading day boundary: 07:00 JST (= 22:00 UTC previous day).
This aligns with the standard forex daily candle rollover and avoids
splitting the NY session across two days.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_JST = ZoneInfo("Asia/Tokyo")

# Trading day starts at 07:00 JST (22:00 UTC) — aligns with forex daily rollover
_DAY_START_HOUR_JST = 7

# Minimum trading days needed for a valid average
_MIN_DAYS = 5

# Number of calendar days to look back (2 weeks ≈ 10 trading days)
_LOOKBACK_DAYS = 14


def _trading_day(dt_jst: datetime) -> str:
    """Return the trading day key for a JST datetime.

    Hours before 07:00 JST belong to the previous trading day.
    """
    if dt_jst.hour < _DAY_START_HOUR_JST:
        return (dt_jst - timedelta(days=1)).strftime("%Y-%m-%d")
    return dt_jst.strftime("%Y-%m-%d")


def _current_trading_day() -> str:
    """Return today's trading day key."""
    return _trading_day(datetime.now(_JST))


def compute_session_adr(
    h1_candles: Any,
    pair: str,
) -> float:
    """Compute the full 24h average daily range over the last 2 weeks.

    Args:
        h1_candles: Numpy structured array of H1 candles (need ~336 bars for 2 weeks).
        pair: Pair symbol (kept for API compatibility).

    Returns:
        Average daily range in price units, or 0.0 if insufficient data.
    """
    if h1_candles is None or len(h1_candles) < 24:
        return 0.0

    # Group candles by trading day (07:00 JST boundaries)
    daily_highs: dict[str, float] = defaultdict(lambda: -1e10)
    daily_lows: dict[str, float] = defaultdict(lambda: 1e10)

    today_key = _current_trading_day()
    now_jst = datetime.now(_JST)

    for candle in h1_candles:
        ts = int(candle["time"])
        dt_jst = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(_JST)

        day_key = _trading_day(dt_jst)

        # Skip current trading day (we track it separately)
        if day_key == today_key:
            continue

        # Only include bars within 2-week lookback
        days_ago = (now_jst.date() - dt_jst.date()).days
        if days_ago > _LOOKBACK_DAYS + 1 or days_ago < 0:
            continue

        h = float(candle["high"])
        l = float(candle["low"])

        if h > daily_highs[day_key]:
            daily_highs[day_key] = h
        if l < daily_lows[day_key]:
            daily_lows[day_key] = l

    # Calculate range for each complete day
    ranges: list[float] = []
    for day_key in daily_highs:
        if day_key in daily_lows:
            hi = daily_highs[day_key]
            lo = daily_lows[day_key]
            if hi > lo and hi > 0:
                ranges.append(hi - lo)

    if len(ranges) < _MIN_DAYS:
        return 0.0

    return float(np.mean(ranges))


def compute_today_session_range(
    h1_candles: Any,
    m1_candles: Any = None,
) -> tuple[float, float]:
    """Get today's high and low since 07:00 JST.

    Uses H1 candles for completed hours, plus M1 candles for the
    current forming hour.

    Args:
        h1_candles: Numpy structured array of H1 candles.
        m1_candles: Optional M1 candles for current price.

    Returns:
        (today_high, today_low) in price units.
    """
    today_key = _current_trading_day()

    session_high = -1e10
    session_low = 1e10
    found = False

    if h1_candles is not None:
        for candle in h1_candles:
            ts = int(candle["time"])
            dt_jst = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(_JST)

            if _trading_day(dt_jst) != today_key:
                continue

            h = float(candle["high"])
            l = float(candle["low"])
            if h > session_high:
                session_high = h
            if l < session_low:
                session_low = l
            found = True

    # Include M1 candles for current hour precision
    if m1_candles is not None and len(m1_candles) > 0:
        for candle in m1_candles:
            ts = int(candle["time"])
            dt_jst = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(_JST)

            if _trading_day(dt_jst) != today_key:
                continue

            h = float(candle["high"])
            l = float(candle["low"])
            if h > session_high:
                session_high = h
            if l < session_low:
                session_low = l
            found = True

    if not found:
        return 0.0, 0.0

    return session_high, session_low


class SessionRangeTracker:
    """Tracks full 24h ADR and today's consumed range per pair."""

    def __init__(self) -> None:
        # pair -> average daily range (price units)
        self._session_adr: dict[str, float] = {}
        # pair -> (today_high, today_low)
        self._today_range: dict[str, tuple[float, float]] = {}

    def update_adr(self, pair: str, h1_candles: Any) -> None:
        """Compute and cache the full 24h ADR for a pair.

        Call during warmup and periodically (e.g., on H1 close).
        Needs ~336 H1 bars (2 weeks).
        """
        adr = compute_session_adr(h1_candles, pair)
        if adr > 0:
            self._session_adr[pair] = adr

    def update_today(
        self, pair: str, h1_candles: Any, m1_candles: Any = None
    ) -> None:
        """Update today's high/low for a pair."""
        hi, lo = compute_today_session_range(h1_candles, m1_candles)
        if hi > lo:
            self._today_range[pair] = (hi, lo)

    def get_consumed_pct(self, pair: str) -> float:
        """Get how much of the full 24h ADR has been consumed today (0-100+)."""
        adr = self._session_adr.get(pair, 0.0)
        if adr <= 0:
            return 0.0

        today = self._today_range.get(pair)
        if today is None:
            return 0.0

        hi, lo = today
        if hi <= lo:
            return 0.0

        return ((hi - lo) / adr) * 100.0

    def get_all_consumed_pct(self) -> dict[str, float]:
        """Get consumed percentage for all tracked pairs."""
        return {pair: self.get_consumed_pct(pair) for pair in self._session_adr}

    def get_session_adr_pips(self, pair: str) -> float:
        """Get the full 24h ADR in pips."""
        adr = self._session_adr.get(pair, 0.0)
        if adr <= 0:
            return 0.0
        pip = 0.01 if "JPY" in pair else 0.0001
        return adr / pip
