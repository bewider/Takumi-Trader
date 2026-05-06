"""FX-specific structural features.

Carry, fix windows, month-end, triangular arbitrage, holiday awareness.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional

JST = timezone(timedelta(hours=9))
UTC = timezone.utc

# Hardcoded recent central-bank policy rates (update periodically)
# These are SAMPLE values — in production fetch from FRED/BoE/ECB
DEFAULT_POLICY_RATES = {
    "USD": 5.50,   # Fed Funds upper bound
    "EUR": 4.50,   # ECB deposit rate
    "GBP": 5.25,   # BoE bank rate
    "JPY": 0.10,   # BoJ short rate
    "CAD": 5.00,   # BoC overnight
    "AUD": 4.35,   # RBA cash rate
    "NZD": 5.50,   # RBNZ OCR
    "CHF": 1.75,   # SNB policy rate
}


def carry_differential(pair: str, rates: dict = None) -> float:
    """Daily carry (interest rate differential) for the pair.

    For BUY EURUSD: receives EUR rate, pays USD rate. Positive = receive carry.
    """
    if rates is None:
        rates = DEFAULT_POLICY_RATES
    if len(pair) != 6:
        return 0.0
    base = pair[:3]
    quote = pair[3:]
    return float(rates.get(base, 0) - rates.get(quote, 0))


def is_in_fix_window(ts_utc: int, window: str = "london_4pm") -> bool:
    """Is the given UTC timestamp inside a known FX fixing window?

    Windows tracked:
        london_4pm:   15:55-16:05 GMT (WM/Refinitiv 4pm fix)
        ecb_2pm:      13:10-13:20 GMT (ECB Frankfurt fix)
        tokyo_11am:   01:55-02:05 GMT (Tokyo fixing — 11am JST)
    """
    dt = datetime.fromtimestamp(ts_utc, tz=UTC)
    hm = dt.hour * 60 + dt.minute
    if window == "london_4pm":
        return 955 <= hm <= 965  # 15:55-16:05 UTC
    if window == "ecb_2pm":
        return 790 <= hm <= 800  # 13:10-13:20 UTC
    if window == "tokyo_11am":
        return 115 <= hm <= 125  # 01:55-02:05 UTC
    return False


def all_fix_windows(ts_utc: int) -> dict:
    """Returns booleans for each known fix window."""
    return {
        "in_london_fix": is_in_fix_window(ts_utc, "london_4pm"),
        "in_ecb_fix": is_in_fix_window(ts_utc, "ecb_2pm"),
        "in_tokyo_fix": is_in_fix_window(ts_utc, "tokyo_11am"),
    }


def is_month_end(ts_utc: int, days_threshold: int = 2) -> bool:
    """True if within the last N business days of the month (institutional rebalancing)."""
    dt = datetime.fromtimestamp(ts_utc, tz=UTC)
    # Find month end
    if dt.month == 12:
        month_end = dt.replace(year=dt.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        month_end = dt.replace(month=dt.month + 1, day=1) - timedelta(days=1)
    while month_end.weekday() > 4:  # back up to Friday
        month_end -= timedelta(days=1)
    days_remaining = (month_end - dt).days
    return 0 <= days_remaining <= days_threshold


def is_quarter_end(ts_utc: int, days_threshold: int = 3) -> bool:
    """True if within the last N business days of a quarter."""
    dt = datetime.fromtimestamp(ts_utc, tz=UTC)
    quarter_ends = {3, 6, 9, 12}
    if dt.month not in quarter_ends:
        return False
    return is_month_end(ts_utc, days_threshold)


def triangular_arb_drift(eurusd: float, usdjpy: float, eurjpy: float, pip_size: float = 0.01) -> float:
    """Deviation from triangular arb parity (in pips of EURJPY).

    EURJPY should equal EURUSD × USDJPY exactly. Real markets have small drift
    due to spreads + latency.
    """
    if eurusd <= 0 or usdjpy <= 0 or eurjpy <= 0:
        return 0.0
    implied = eurusd * usdjpy
    return float((eurjpy - implied) / pip_size)


def is_dst_active(country: str = "UK", ref_dt: Optional[datetime] = None) -> bool:
    """Is DST currently active for the given region?

    UK/EU: last Sunday March → last Sunday October
    US: 2nd Sunday March → 1st Sunday November
    """
    if ref_dt is None:
        ref_dt = datetime.now(UTC)
    year = ref_dt.year
    if country in ("UK", "EU"):
        # Last Sunday of March
        mar_last_sun = datetime(year, 3, 31, tzinfo=UTC)
        while mar_last_sun.weekday() != 6:
            mar_last_sun -= timedelta(days=1)
        # Last Sunday of October
        oct_last_sun = datetime(year, 10, 31, tzinfo=UTC)
        while oct_last_sun.weekday() != 6:
            oct_last_sun -= timedelta(days=1)
        return mar_last_sun <= ref_dt < oct_last_sun
    if country == "US":
        # 2nd Sunday March
        mar_2nd_sun = datetime(year, 3, 1, tzinfo=UTC)
        sundays = 0
        while sundays < 2:
            if mar_2nd_sun.weekday() == 6:
                sundays += 1
                if sundays == 2:
                    break
            mar_2nd_sun += timedelta(days=1)
        # 1st Sunday November
        nov_1st_sun = datetime(year, 11, 1, tzinfo=UTC)
        while nov_1st_sun.weekday() != 6:
            nov_1st_sun += timedelta(days=1)
        return mar_2nd_sun <= ref_dt < nov_1st_sun
    return False


def major_holiday(ts_utc: int) -> Optional[str]:
    """Detect known major FX holidays. Returns label or None.

    Sample list — extend with full holiday calendar in production.
    """
    dt = datetime.fromtimestamp(ts_utc, tz=UTC)
    md = (dt.month, dt.day)
    holidays = {
        (1, 1): "New Year",
        (12, 25): "Christmas",
        (12, 24): "Christmas Eve (half-day)",
        (12, 26): "Boxing Day",
        (7, 4): "US Independence Day",
        (5, 1): "May Day (UK/EU)",
    }
    return holidays.get(md)


def session_label_jst(ts_utc: int) -> str:
    """JST session label (matches TAKUMI's session_manager)."""
    jst = datetime.fromtimestamp(ts_utc, tz=UTC).astimezone(JST)
    hm = jst.hour * 60 + jst.minute
    if 480 <= hm < 525:
        return "Australia"
    if 525 <= hm < 575:
        return "Tokyo_open"
    if 575 <= hm < 728:
        return "Morning"
    if 728 <= hm < 884:
        return "Afternoon"
    if 884 <= hm < 945:
        return "Frankfurt_open"
    if 945 <= hm < 996:
        return "London_open"
    if 996 <= hm < 1244:
        return "London"
    if 1244 <= hm < 1295:
        return "US_open"
    if hm >= 1295 or hm < 300:
        return "US"
    if 300 <= hm <= 477:
        return "NO_TRADE"
    return "off_hours"
