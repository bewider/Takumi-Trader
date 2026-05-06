"""Price levels & technical anchors.

Round numbers, prior period OHLC, pivot-point systems, VWAP variants,
volume profile (POC, VAH, VAL, naked POCs), session ranges.
"""
from __future__ import annotations

import numpy as np
from datetime import datetime, timezone, timedelta

JST = timezone(timedelta(hours=9))


# ──────────────────────────────────────────────────────────────────
# 1. ROUND NUMBERS
# ──────────────────────────────────────────────────────────────────

def round_number_distance_pips(price: float, pip_size: float = 0.0001) -> dict:
    """Returns distances (in pips) from `price` to nearest:
        00, 25, 50, 75 levels (micro)
        big_figure (e.g., 1.1700, 1.1800)
        century (e.g., 100.00, 110.00 for JPY pairs)
    """
    # 50-pip granularity (00 / 50)
    nearest_50 = round(price / (50 * pip_size)) * (50 * pip_size)
    # 25-pip (25 / 75)
    nearest_25 = round(price / (25 * pip_size)) * (25 * pip_size)
    # 100-pip (big figure)
    nearest_big = round(price / (100 * pip_size)) * (100 * pip_size)
    # 1000-pip (century, e.g., 1.1000, 1.2000)
    nearest_century = round(price / (1000 * pip_size)) * (1000 * pip_size)
    return {
        "dist_to_nearest_50_pips": abs(price - nearest_50) / pip_size,
        "dist_to_nearest_25_pips": abs(price - nearest_25) / pip_size,
        "dist_to_big_figure_pips": abs(price - nearest_big) / pip_size,
        "dist_to_century_pips": abs(price - nearest_century) / pip_size,
        "nearest_50_level": nearest_50,
        "nearest_25_level": nearest_25,
        "nearest_big_figure": nearest_big,
        "nearest_century": nearest_century,
    }


# ──────────────────────────────────────────────────────────────────
# 2. PRIOR PERIOD OHLC (day, week, month)
# ──────────────────────────────────────────────────────────────────

def prior_day_ohlc(times_utc: np.ndarray, opens, highs, lows, closes, ref_time_utc: int) -> dict:
    """Get OHLC of the calendar day BEFORE ref_time (in UTC)."""
    t = np.asarray(times_utc, dtype=np.int64)
    ref_dt = datetime.fromtimestamp(ref_time_utc, tz=timezone.utc)
    today_start = int(ref_dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    yesterday_start = today_start - 86400
    mask = (t >= yesterday_start) & (t < today_start)
    if not mask.any():
        return _empty_ohlc()
    return {
        "prev_day_open": float(opens[mask][0]),
        "prev_day_high": float(np.max(highs[mask])),
        "prev_day_low": float(np.min(lows[mask])),
        "prev_day_close": float(closes[mask][-1]),
    }


def prior_week_ohlc(times_utc: np.ndarray, opens, highs, lows, closes, ref_time_utc: int) -> dict:
    """OHLC of the calendar week before ref_time. Week = Mon 00:00 UTC → Sun 23:59 UTC."""
    t = np.asarray(times_utc, dtype=np.int64)
    ref_dt = datetime.fromtimestamp(ref_time_utc, tz=timezone.utc)
    # Start of THIS week (Monday)
    weekday = ref_dt.weekday()  # Mon=0
    this_week_start = ref_dt.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=weekday)
    last_week_start = this_week_start - timedelta(days=7)
    mask = (t >= int(last_week_start.timestamp())) & (t < int(this_week_start.timestamp()))
    if not mask.any():
        return _empty_ohlc(prefix="prev_week")
    return {
        "prev_week_open": float(opens[mask][0]),
        "prev_week_high": float(np.max(highs[mask])),
        "prev_week_low": float(np.min(lows[mask])),
        "prev_week_close": float(closes[mask][-1]),
    }


def prior_month_ohlc(times_utc: np.ndarray, opens, highs, lows, closes, ref_time_utc: int) -> dict:
    """OHLC of the calendar month before ref_time."""
    t = np.asarray(times_utc, dtype=np.int64)
    ref_dt = datetime.fromtimestamp(ref_time_utc, tz=timezone.utc)
    this_month_start = ref_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # Previous month start
    if this_month_start.month == 1:
        prev_month_start = this_month_start.replace(year=this_month_start.year - 1, month=12)
    else:
        prev_month_start = this_month_start.replace(month=this_month_start.month - 1)
    mask = (t >= int(prev_month_start.timestamp())) & (t < int(this_month_start.timestamp()))
    if not mask.any():
        return _empty_ohlc(prefix="prev_month")
    return {
        "prev_month_open": float(opens[mask][0]),
        "prev_month_high": float(np.max(highs[mask])),
        "prev_month_low": float(np.min(lows[mask])),
        "prev_month_close": float(closes[mask][-1]),
    }


def year_high_low(highs: np.ndarray, lows: np.ndarray) -> dict:
    """Just max/min over the entire array — caller controls window."""
    if len(highs) == 0:
        return {"year_high": 0.0, "year_low": 0.0}
    return {"year_high": float(np.max(highs)), "year_low": float(np.min(lows))}


def _empty_ohlc(prefix: str = "prev_day") -> dict:
    return {
        f"{prefix}_open": 0.0, f"{prefix}_high": 0.0,
        f"{prefix}_low": 0.0, f"{prefix}_close": 0.0,
    }


# ──────────────────────────────────────────────────────────────────
# 3. SESSION RANGES (Asian, London, NY)
# ──────────────────────────────────────────────────────────────────

def session_range(times_utc, highs, lows, ref_time_utc: int, session_jst_start: int, session_jst_end: int) -> dict:
    """Compute high/low of a JST session for the date containing ref_time.

    session_jst_start, _end: minutes-of-day in JST (e.g., 480, 900 for 8:00-15:00).
    """
    t = np.asarray(times_utc, dtype=np.int64)
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    ref_jst = datetime.fromtimestamp(ref_time_utc, tz=timezone.utc).astimezone(JST)
    today_start_jst = ref_jst.replace(hour=0, minute=0, second=0, microsecond=0)

    sess_start_dt = today_start_jst + timedelta(minutes=session_jst_start)
    sess_end_dt = today_start_jst + timedelta(minutes=session_jst_end)
    sess_start = int(sess_start_dt.timestamp())
    sess_end = int(sess_end_dt.timestamp())

    mask = (t >= sess_start) & (t < sess_end)
    if not mask.any():
        return {"session_high": 0.0, "session_low": 0.0, "session_range_pips": 0.0}
    sh = float(np.max(h[mask]))
    sl = float(np.min(lo[mask]))
    return {"session_high": sh, "session_low": sl, "session_range_pips": (sh - sl)}


def asian_session_range(times_utc, highs, lows, ref_time_utc: int) -> dict:
    """Asian session: 08:00-15:00 JST."""
    return session_range(times_utc, highs, lows, ref_time_utc, 480, 900)


def london_session_range(times_utc, highs, lows, ref_time_utc: int) -> dict:
    """London session: 15:45-20:45 JST (summer DST). For winter shift +60 min externally."""
    return session_range(times_utc, highs, lows, ref_time_utc, 945, 1245)


def ny_session_range(times_utc, highs, lows, ref_time_utc: int) -> dict:
    """NY session: 22:30-01:30 JST (summer EDT). Wraps midnight; uses single-day approx."""
    return session_range(times_utc, highs, lows, ref_time_utc, 1350, 1440)


# ──────────────────────────────────────────────────────────────────
# 4. PIVOT POINTS (Classic, Fibonacci, Camarilla, Woodie, DeMark)
# ──────────────────────────────────────────────────────────────────

def classic_pivots(prev_high, prev_low, prev_close) -> dict:
    """Classic floor-trader pivot points."""
    p = (prev_high + prev_low + prev_close) / 3.0
    r1 = 2 * p - prev_low
    s1 = 2 * p - prev_high
    r2 = p + (prev_high - prev_low)
    s2 = p - (prev_high - prev_low)
    r3 = prev_high + 2 * (p - prev_low)
    s3 = prev_low - 2 * (prev_high - p)
    return {
        "pivot_pp": p, "pivot_r1": r1, "pivot_r2": r2, "pivot_r3": r3,
        "pivot_s1": s1, "pivot_s2": s2, "pivot_s3": s3,
    }


def fibonacci_pivots(prev_high, prev_low, prev_close) -> dict:
    """Fibonacci pivot points."""
    p = (prev_high + prev_low + prev_close) / 3.0
    rng = prev_high - prev_low
    return {
        "fib_pp": p,
        "fib_r1": p + 0.382 * rng, "fib_r2": p + 0.618 * rng, "fib_r3": p + 1.000 * rng,
        "fib_s1": p - 0.382 * rng, "fib_s2": p - 0.618 * rng, "fib_s3": p - 1.000 * rng,
    }


def camarilla_pivots(prev_high, prev_low, prev_close) -> dict:
    """Camarilla pivots (closer to price than classic)."""
    rng = prev_high - prev_low
    return {
        "cam_r4": prev_close + rng * 1.1 / 2,
        "cam_r3": prev_close + rng * 1.1 / 4,
        "cam_r2": prev_close + rng * 1.1 / 6,
        "cam_r1": prev_close + rng * 1.1 / 12,
        "cam_s1": prev_close - rng * 1.1 / 12,
        "cam_s2": prev_close - rng * 1.1 / 6,
        "cam_s3": prev_close - rng * 1.1 / 4,
        "cam_s4": prev_close - rng * 1.1 / 2,
    }


def woodie_pivots(prev_high, prev_low, prev_close, today_open) -> dict:
    """Woodie pivots — weighted toward today's open."""
    p = (prev_high + prev_low + 2 * today_open) / 4.0
    return {
        "wood_pp": p,
        "wood_r1": 2 * p - prev_low, "wood_r2": p + (prev_high - prev_low),
        "wood_s1": 2 * p - prev_high, "wood_s2": p - (prev_high - prev_low),
    }


def demark_pivots(prev_open, prev_high, prev_low, prev_close) -> dict:
    """DeMark pivots."""
    if prev_close < prev_open:
        x = prev_high + 2 * prev_low + prev_close
    elif prev_close > prev_open:
        x = 2 * prev_high + prev_low + prev_close
    else:
        x = prev_high + prev_low + 2 * prev_close
    p = x / 4.0
    r1 = x / 2 - prev_low
    s1 = x / 2 - prev_high
    return {"dm_pp": p, "dm_r1": r1, "dm_s1": s1}


# ──────────────────────────────────────────────────────────────────
# 5. VWAP (Volume-Weighted Average Price)
# ──────────────────────────────────────────────────────────────────

def vwap(closes: np.ndarray, volumes: np.ndarray, highs=None, lows=None) -> float:
    """Standard VWAP.

    If highs+lows provided, uses (H+L+C)/3 typical price; else uses close.
    """
    c = np.asarray(closes, dtype=np.float64)
    v = np.asarray(volumes, dtype=np.float64)
    if highs is not None and lows is not None:
        h = np.asarray(highs, dtype=np.float64)
        lo = np.asarray(lows, dtype=np.float64)
        typ = (h + lo + c) / 3.0
    else:
        typ = c
    if v.sum() <= 0:
        return float(c.mean()) if len(c) else 0.0
    return float(np.sum(typ * v) / np.sum(v))


def session_vwap(
    times_utc: np.ndarray,
    closes: np.ndarray,
    volumes: np.ndarray,
    ref_time_utc: int,
    session_jst_start: int = 480,
    session_jst_end: int = 1440,
    highs=None, lows=None,
) -> float:
    """VWAP for the current JST session up to ref_time."""
    t = np.asarray(times_utc, dtype=np.int64)
    ref_jst = datetime.fromtimestamp(ref_time_utc, tz=timezone.utc).astimezone(JST)
    today_start_jst = ref_jst.replace(hour=0, minute=0, second=0, microsecond=0)
    sess_start = int((today_start_jst + timedelta(minutes=session_jst_start)).timestamp())
    sess_end = int((today_start_jst + timedelta(minutes=session_jst_end)).timestamp())
    mask = (t >= sess_start) & (t < min(sess_end, ref_time_utc))
    if not mask.any():
        return 0.0
    if highs is not None and lows is not None:
        return vwap(closes[mask], volumes[mask], highs=highs[mask], lows=lows[mask])
    return vwap(closes[mask], volumes[mask])


def anchored_vwap(
    times_utc: np.ndarray,
    closes: np.ndarray,
    volumes: np.ndarray,
    anchor_time_utc: int,
    highs=None, lows=None,
) -> float:
    """VWAP from an anchor time (e.g., FOMC announcement, swing high)."""
    t = np.asarray(times_utc, dtype=np.int64)
    mask = t >= anchor_time_utc
    if not mask.any():
        return 0.0
    if highs is not None and lows is not None:
        return vwap(closes[mask], volumes[mask], highs=highs[mask], lows=lows[mask])
    return vwap(closes[mask], volumes[mask])


# ──────────────────────────────────────────────────────────────────
# 6. VOLUME PROFILE — POC, VAH, VAL
# ──────────────────────────────────────────────────────────────────

def volume_profile(
    closes: np.ndarray,
    volumes: np.ndarray,
    highs: np.ndarray = None,
    lows: np.ndarray = None,
    pip_size: float = 0.0001,
    bin_pips: float = 1.0,
    value_area_pct: float = 0.70,
) -> dict:
    """Compute volume profile: POC, VAH, VAL.

    Distributes each bar's volume across price levels using H-L range and
    bin granularity (default 1 pip per bin).

    Returns:
        poc:        Point of Control (most-traded price)
        vah:        Value Area High
        val:        Value Area Low
        levels:     dict of price → volume
    """
    c = np.asarray(closes, dtype=np.float64)
    v = np.asarray(volumes, dtype=np.float64)
    if len(c) == 0 or v.sum() <= 0:
        return {"poc": 0.0, "vah": 0.0, "val": 0.0, "levels": {}}

    if highs is not None and lows is not None:
        h = np.asarray(highs, dtype=np.float64)
        lo = np.asarray(lows, dtype=np.float64)
    else:
        h = lo = c

    bin_size = bin_pips * pip_size
    profile: dict[float, float] = {}
    for i in range(len(c)):
        bar_low = lo[i]
        bar_high = h[i]
        bar_vol = v[i]
        if bar_high <= bar_low or bar_vol <= 0:
            continue
        n_bins = max(1, int((bar_high - bar_low) / bin_size))
        vol_per_bin = bar_vol / n_bins
        for b in range(n_bins):
            level = round((bar_low + b * bin_size) / bin_size) * bin_size
            profile[level] = profile.get(level, 0.0) + vol_per_bin

    if not profile:
        return {"poc": 0.0, "vah": 0.0, "val": 0.0, "levels": {}}

    sorted_levels = sorted(profile.items(), key=lambda x: -x[1])
    poc = sorted_levels[0][0]
    total_v = sum(profile.values())
    target = total_v * value_area_pct
    accum = 0.0
    in_va: list[float] = [poc]
    sorted_by_dist = sorted(profile.items(), key=lambda x: abs(x[0] - poc))
    for level, vol in sorted_by_dist:
        if accum >= target:
            break
        in_va.append(level)
        accum += vol
    vah = max(in_va)
    val = min(in_va)
    return {"poc": poc, "vah": vah, "val": val, "levels": profile}


def naked_pocs(daily_pocs: list[tuple[int, float]], current_price: float, pip_size: float = 0.0001) -> list:
    """Return daily POCs that have NOT been touched since they formed.

    daily_pocs: list of (epoch_utc, poc_price) for prior days, ordered oldest-first.
    Returns list of (date, poc) for naked ones, with pip distance from current price.

    Naked logic: a POC is naked if no subsequent day's high/low passed through it.
    Caller must check this — here we just return all + distances.
    """
    if not daily_pocs:
        return []
    out = []
    for ts, p in daily_pocs:
        out.append({
            "date_utc": ts,
            "poc_price": p,
            "dist_pips": abs(current_price - p) / pip_size,
            "above_current": p > current_price,
        })
    return out
