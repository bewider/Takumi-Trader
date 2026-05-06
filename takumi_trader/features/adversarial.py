"""Adversarial / game-theoretic features.

Stop-hunt detection, liquidity sweeps, quote stuffing, round-number
magnetism — patterns associated with predatory market behavior that
TAKUMI's strategies should be aware of.
"""
from __future__ import annotations

import numpy as np


def stop_hunt_score(
    highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
    level: float, pip_size: float = 0.0001, beyond_pips: float = 3.0,
    return_within_bars: int = 2,
) -> dict:
    """Detect stop-hunt: wick beyond `level` then close back inside within N bars.

    Returns score (1.0 = full stop hunt, 0.0 = none) and direction.
    """
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < return_within_bars + 1:
        return {"hunt_score": 0.0, "direction": "none"}
    # Look at last bar for the wick + return-within-bars closes
    threshold = beyond_pips * pip_size
    last_idx = -1
    # Up-side hunt: high pierced level + threshold, then close came back below level
    if h[last_idx] > level + threshold:
        recent_close = c[-1]
        if recent_close < level:
            penetration = (h[last_idx] - level) / pip_size
            return {"hunt_score": min(1.0, penetration / 10), "direction": "up_hunt", "penetration_pips": float(penetration)}
    # Down-side hunt
    if lo[last_idx] < level - threshold:
        recent_close = c[-1]
        if recent_close > level:
            penetration = (level - lo[last_idx]) / pip_size
            return {"hunt_score": min(1.0, penetration / 10), "direction": "down_hunt", "penetration_pips": float(penetration)}
    return {"hunt_score": 0.0, "direction": "none"}


def liquidity_sweep_pattern(
    highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
    lookback_swings: int = 20, sweep_threshold_pips: float = 2.0,
    pip_size: float = 0.0001,
) -> dict:
    """Detect ICT-style liquidity sweep: price breaks recent swing high/low,
    fails to sustain, reverses.

    Returns sweep direction + the swing level taken.
    """
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    c = np.asarray(closes, dtype=np.float64)
    n = len(c)
    if n < lookback_swings + 5:
        return {"sweep_type": "none"}
    recent_h = h[-lookback_swings - 5: -5]
    recent_l = lo[-lookback_swings - 5: -5]
    swing_high = float(recent_h.max()) if len(recent_h) else 0
    swing_low = float(recent_l.min()) if len(recent_l) else 0
    last_h = float(h[-1])
    last_l = float(lo[-1])
    last_c = float(c[-1])
    sweep_thresh = sweep_threshold_pips * pip_size

    # Bullish sweep: low broke below swing low, close above swing low
    if last_l < swing_low - sweep_thresh and last_c > swing_low:
        return {
            "sweep_type": "bullish_sweep",
            "swing_taken": swing_low,
            "depth_pips": float((swing_low - last_l) / pip_size),
        }
    # Bearish sweep: high broke above swing high, close below
    if last_h > swing_high + sweep_thresh and last_c < swing_high:
        return {
            "sweep_type": "bearish_sweep",
            "swing_taken": swing_high,
            "depth_pips": float((last_h - swing_high) / pip_size),
        }
    return {"sweep_type": "none"}


def round_number_magnetism(
    highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
    pip_size: float = 0.0001, level_pips: float = 50.0, lookback: int = 100,
) -> dict:
    """Quantify how often price approaches and retreats from nearest big round numbers.

    Returns magnet score (0-1) — high means price is being pulled toward level.
    """
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < lookback:
        return {"magnet_score": 0.0, "nearest_level": 0.0}
    sub = c[-lookback:]
    bin_size = level_pips * pip_size
    nearest = round(sub[-1] / bin_size) * bin_size
    distances = np.abs(sub - nearest) / pip_size
    # If many bars are within 5 pips, magnet is active
    close_count = (distances < 5).sum()
    score = close_count / lookback
    return {"magnet_score": float(score), "nearest_level": float(nearest)}


def round_number_rejection_history(
    highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
    test_level: float, pip_size: float = 0.0001, lookback: int = 200,
    rejection_pips: float = 5.0,
) -> dict:
    """Count how many times the given level has been REJECTED (touched then reversed)
    in the lookback window.
    """
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    c = np.asarray(closes, dtype=np.float64)
    n = len(c)
    if n < lookback:
        lookback = n
    sub_h = h[-lookback:]
    sub_l = lo[-lookback:]
    sub_c = c[-lookback:]
    threshold = rejection_pips * pip_size
    rejections_up = 0  # touched from below, rejected
    rejections_down = 0
    for i in range(1, len(sub_c)):
        if sub_h[i] >= test_level - 0.5 * pip_size and sub_c[i] < test_level - threshold:
            rejections_up += 1
        if sub_l[i] <= test_level + 0.5 * pip_size and sub_c[i] > test_level + threshold:
            rejections_down += 1
    return {"rejections_up": rejections_up, "rejections_down": rejections_down}


def liquidity_void_detection(
    highs: np.ndarray, lows: np.ndarray, closes: np.ndarray,
    volumes: np.ndarray = None, gap_pips_threshold: float = 5.0, pip_size: float = 0.0001,
) -> dict:
    """Detect price gaps where volume was thin (likely fill targets)."""
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    if len(h) < 2:
        return {"voids": []}
    voids = []
    for i in range(1, len(h)):
        gap_up = lo[i] - h[i - 1]
        gap_down = lo[i - 1] - h[i]
        if gap_up > gap_pips_threshold * pip_size:
            voids.append({
                "type": "up_void", "bar_idx": i,
                "void_low": float(h[i - 1]), "void_high": float(lo[i]),
                "size_pips": float(gap_up / pip_size),
            })
        if gap_down > gap_pips_threshold * pip_size:
            voids.append({
                "type": "down_void", "bar_idx": i,
                "void_low": float(h[i]), "void_high": float(lo[i - 1]),
                "size_pips": float(gap_down / pip_size),
            })
    return {"voids": voids}


def tick_burst_detection(
    tick_volumes: np.ndarray, lookback: int = 30, burst_z_threshold: float = 3.0,
) -> dict:
    """Detect volume bursts (potential informed flow / news reaction)."""
    v = np.asarray(tick_volumes, dtype=np.float64)
    if len(v) < lookback + 1:
        return {"burst_detected": False, "z_score": 0.0}
    history = v[-lookback - 1: -1]
    mean = history.mean()
    std = history.std(ddof=1)
    if std < 1e-12:
        return {"burst_detected": False, "z_score": 0.0}
    z = (v[-1] - mean) / std
    return {"burst_detected": bool(z > burst_z_threshold), "z_score": float(z)}
