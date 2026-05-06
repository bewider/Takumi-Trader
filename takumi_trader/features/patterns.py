"""Technical pattern detection.

Liquidity sweeps, FVG (Fair Value Gaps), order blocks, equal highs/lows,
double tops/bottoms, head-and-shoulders, classical patterns.
"""
from __future__ import annotations

import numpy as np


def fair_value_gaps(
    opens: np.ndarray, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
    pip_size: float = 0.0001, min_gap_pips: float = 1.0,
) -> list[dict]:
    """Detect Fair Value Gaps (3-bar patterns).

    Bullish FVG: bar1 high < bar3 low (gap up).
    Bearish FVG: bar1 low > bar3 high (gap down).
    """
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    if len(h) < 3:
        return []
    gaps = []
    for i in range(2, len(h)):
        # Bullish: bar i-2 high < bar i low
        if h[i - 2] < lo[i]:
            gap_size = (lo[i] - h[i - 2]) / pip_size
            if gap_size >= min_gap_pips:
                gaps.append({
                    "type": "bullish_fvg",
                    "bar_idx": i,
                    "gap_low": float(h[i - 2]),
                    "gap_high": float(lo[i]),
                    "gap_pips": float(gap_size),
                })
        # Bearish: bar i-2 low > bar i high
        if lo[i - 2] > h[i]:
            gap_size = (lo[i - 2] - h[i]) / pip_size
            if gap_size >= min_gap_pips:
                gaps.append({
                    "type": "bearish_fvg",
                    "bar_idx": i,
                    "gap_low": float(h[i]),
                    "gap_high": float(lo[i - 2]),
                    "gap_pips": float(gap_size),
                })
    return gaps


def order_blocks(
    opens: np.ndarray, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
    move_threshold_pct: float = 0.005,
) -> list[dict]:
    """Detect ICT-style order blocks: last opposing candle before strong move.

    Bullish OB: last bearish candle before a strong rally.
    Bearish OB: last bullish candle before a strong selloff.
    """
    o = np.asarray(opens, dtype=np.float64)
    c = np.asarray(closes, dtype=np.float64)
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    if len(c) < 4:
        return []
    blocks = []
    for i in range(2, len(c) - 1):
        next_move = (c[i + 1] - c[i]) / c[i]
        # Bullish OB: i is bearish (close < open), next move is strongly bullish
        if c[i] < o[i] and next_move > move_threshold_pct:
            blocks.append({
                "type": "bullish_ob",
                "bar_idx": i,
                "ob_high": float(h[i]),
                "ob_low": float(lo[i]),
                "ob_open": float(o[i]),
                "ob_close": float(c[i]),
            })
        # Bearish OB: i is bullish, next is strongly bearish
        if c[i] > o[i] and next_move < -move_threshold_pct:
            blocks.append({
                "type": "bearish_ob",
                "bar_idx": i,
                "ob_high": float(h[i]),
                "ob_low": float(lo[i]),
                "ob_open": float(o[i]),
                "ob_close": float(c[i]),
            })
    return blocks


def liquidity_pools(
    highs: np.ndarray, lows: np.ndarray, swing_lookback: int = 5, tolerance_pips: float = 2.0,
    pip_size: float = 0.0001,
) -> dict:
    """Identify swing-high / swing-low clusters as liquidity pools."""
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    n = len(h)
    if n < swing_lookback * 2 + 1:
        return {"buy_side_pools": [], "sell_side_pools": []}
    swing_highs, swing_lows = [], []
    for i in range(swing_lookback, n - swing_lookback):
        if h[i] == max(h[i - swing_lookback: i + swing_lookback + 1]):
            swing_highs.append((i, h[i]))
        if lo[i] == min(lo[i - swing_lookback: i + swing_lookback + 1]):
            swing_lows.append((i, lo[i]))
    # Cluster swings by tolerance
    tol = tolerance_pips * pip_size
    sell_pools = _cluster_levels(swing_highs, tol)
    buy_pools = _cluster_levels(swing_lows, tol)
    return {"sell_side_pools": sell_pools, "buy_side_pools": buy_pools}


def _cluster_levels(levels: list, tol: float) -> list:
    """Group levels within tolerance into clusters."""
    if not levels:
        return []
    levels = sorted(levels, key=lambda x: x[1])
    clusters = [[levels[0]]]
    for idx, lvl in levels[1:]:
        if abs(lvl - clusters[-1][-1][1]) <= tol:
            clusters[-1].append((idx, lvl))
        else:
            clusters.append([(idx, lvl)])
    return [
        {"price": float(np.mean([l for _, l in c])), "touches": len(c)}
        for c in clusters if len(c) >= 2
    ]


def equal_highs_lows(highs: np.ndarray, lows: np.ndarray, tolerance_pips: float = 2.0,
                     pip_size: float = 0.0001, lookback: int = 50) -> dict:
    """Detect double-top / double-bottom patterns within tolerance."""
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    if len(h) < lookback:
        return {"equal_highs": False, "equal_lows": False}
    recent_h = h[-lookback:]
    recent_l = lo[-lookback:]
    tol = tolerance_pips * pip_size
    h_sorted = np.sort(recent_h)[::-1]
    l_sorted = np.sort(recent_l)
    eq_highs = abs(h_sorted[0] - h_sorted[1]) < tol if len(h_sorted) > 1 else False
    eq_lows = abs(l_sorted[0] - l_sorted[1]) < tol if len(l_sorted) > 1 else False
    return {
        "equal_highs": bool(eq_highs),
        "equal_lows": bool(eq_lows),
        "high_level": float(h_sorted[0]) if eq_highs else 0.0,
        "low_level": float(l_sorted[0]) if eq_lows else 0.0,
    }


def trendline_break(closes: np.ndarray, swing_lookback: int = 10) -> dict:
    """Detect trendline break via linear regression on swing points."""
    c = np.asarray(closes, dtype=np.float64)
    n = len(c)
    if n < swing_lookback * 4:
        return {"break_type": "none", "slope": 0.0}
    # Find recent swing highs/lows
    swing_h_idx = []
    swing_l_idx = []
    for i in range(swing_lookback, n - swing_lookback):
        if c[i] == max(c[i - swing_lookback: i + swing_lookback + 1]):
            swing_h_idx.append(i)
        if c[i] == min(c[i - swing_lookback: i + swing_lookback + 1]):
            swing_l_idx.append(i)
    # Fit upper trendline through last 3 swing highs, lower through swing lows
    res = {"break_type": "none", "slope": 0.0}
    if len(swing_h_idx) >= 3:
        idx = swing_h_idx[-3:]
        prices = c[idx]
        slope_h, intercept_h = np.polyfit(idx, prices, 1)
        projected = slope_h * (n - 1) + intercept_h
        if c[-1] > projected:
            res = {"break_type": "upper_break_up", "slope": float(slope_h)}
    if len(swing_l_idx) >= 3:
        idx = swing_l_idx[-3:]
        prices = c[idx]
        slope_l, intercept_l = np.polyfit(idx, prices, 1)
        projected = slope_l * (n - 1) + intercept_l
        if c[-1] < projected:
            res = {"break_type": "lower_break_down", "slope": float(slope_l)}
    return res


def head_and_shoulders(highs: np.ndarray, lows: np.ndarray, lookback: int = 50) -> dict:
    """Detect H&S / inverse H&S patterns. Returns pattern type if detected."""
    h = np.asarray(highs, dtype=np.float64)
    if len(h) < lookback:
        return {"pattern": "none"}
    sub = h[-lookback:]
    n = len(sub)
    # Find 3 local maxima
    peaks = []
    for i in range(2, n - 2):
        if sub[i] > sub[i - 1] and sub[i] > sub[i - 2] and sub[i] > sub[i + 1] and sub[i] > sub[i + 2]:
            peaks.append((i, sub[i]))
    if len(peaks) < 3:
        return {"pattern": "none"}
    # Last 3 peaks
    p1, p2, p3 = peaks[-3:]
    # H&S: middle peak highest, two shoulders roughly equal
    if p2[1] > p1[1] and p2[1] > p3[1] and abs(p1[1] - p3[1]) / p2[1] < 0.02:
        return {"pattern": "head_and_shoulders", "head": float(p2[1]),
                "shoulders": (float(p1[1]), float(p3[1]))}
    return {"pattern": "none"}


def candlestick_pattern(opens: np.ndarray, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> str:
    """Identify last bar's candlestick pattern (single-bar)."""
    if len(opens) < 1:
        return "none"
    o = float(opens[-1])
    h = float(highs[-1])
    lo = float(lows[-1])
    c = float(closes[-1])
    body = abs(c - o)
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - lo
    rng = h - lo
    if rng == 0:
        return "none"
    body_ratio = body / rng
    if body_ratio < 0.10:
        return "doji"
    if lower_wick > 2 * body and upper_wick < body:
        return "hammer" if c > o else "hanging_man"
    if upper_wick > 2 * body and lower_wick < body:
        return "shooting_star" if c < o else "inverted_hammer"
    if body_ratio > 0.85:
        return "marubozu_bull" if c > o else "marubozu_bear"
    return "normal"
