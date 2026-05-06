"""Microstructure & order-flow features.

All functions are pure-numpy and accept arrays from MT5 bars or ticks.
No external monthly fees — derives signals from data already obtainable
via mt5.copy_rates_* and mt5.copy_ticks_range.

Implemented:
    cumulative_volume_delta_m1   — CVD via tick rule on M1 bars
    tick_rule_classify           — sign-classify each bar's volume
    cvd_divergence               — price vs CVD divergence flag
    lee_ready_aggressor          — buyer/seller-initiated tick classification
    aggressor_imbalance          — running buy-vs-sell aggressor ratio
    vpin                         — volume-synchronized prob. of informed trading
    kyle_lambda                  — price impact per signed volume unit
    amihud_illiquidity           — |return| / volume daily measure
    quote_to_trade_ratio         — quote updates per executed trade
    effective_spread             — actual cost vs quoted spread
    realized_spread              — post-trade reversion measure
    iceberg_pattern_score        — repeated trades at same level

Conventions:
    All "bars" inputs are dict-like with keys: time, open, high, low, close,
    tick_volume (optional). MT5 returns numpy structured arrays with these
    fields — pass directly.
    All "ticks" inputs are dict-like with keys: time, bid, ask, last, volume,
    flags. From mt5.copy_ticks_range with COPY_TICKS_ALL.
"""
from __future__ import annotations

from typing import Optional

import numpy as np


# ──────────────────────────────────────────────────────────────────
# 1. CUMULATIVE VOLUME DELTA (CVD) — tick-rule based
# ──────────────────────────────────────────────────────────────────

def tick_rule_classify(closes: np.ndarray) -> np.ndarray:
    """Classify each bar's flow direction using the tick rule.

    Returns +1 if close > prev_close (buyer-initiated), -1 if <,
    0 if equal. The first bar returns 0 by convention.
    """
    closes = np.asarray(closes, dtype=np.float64)
    out = np.zeros(len(closes), dtype=np.int8)
    out[1:] = np.sign(closes[1:] - closes[:-1])
    return out


def cumulative_volume_delta_m1(closes: np.ndarray, tick_volumes: np.ndarray) -> np.ndarray:
    """Running cumulative volume delta from M1 bars.

    CVD[i] = Σ_{j≤i} sign(close_j - close_{j-1}) * tick_volume_j
    """
    closes = np.asarray(closes, dtype=np.float64)
    vols = np.asarray(tick_volumes, dtype=np.float64)
    direction = tick_rule_classify(closes)
    delta = direction * vols
    return np.cumsum(delta)


def cvd_divergence(
    closes: np.ndarray,
    tick_volumes: np.ndarray,
    window: int = 30,
    price_move_threshold_pips: float = 5.0,
    pip_size: float = 0.0001,
) -> tuple[bool, float, float]:
    """Detect price-vs-CVD divergence over the last `window` bars.

    Returns (is_divergent, price_move_pips, cvd_change).
    Divergent: price moved one way by > threshold while CVD moved the other.
    This signals an exhausted move with weak follow-through.
    """
    closes = np.asarray(closes, dtype=np.float64)
    if len(closes) < window + 1:
        return (False, 0.0, 0.0)
    cvd = cumulative_volume_delta_m1(closes, tick_volumes)
    price_move = (closes[-1] - closes[-window]) / pip_size
    cvd_change = cvd[-1] - cvd[-window]
    is_div = (
        (price_move > price_move_threshold_pips and cvd_change <= 0)
        or (price_move < -price_move_threshold_pips and cvd_change >= 0)
    )
    return (bool(is_div), float(price_move), float(cvd_change))


# ──────────────────────────────────────────────────────────────────
# 2. LEE-READY AGGRESSOR CLASSIFICATION (tick-level)
# ──────────────────────────────────────────────────────────────────

def lee_ready_aggressor(
    last_prices: np.ndarray,
    bids: np.ndarray,
    asks: np.ndarray,
) -> np.ndarray:
    """Classify each tick as buyer (+1) or seller (-1) initiated.

    Lee-Ready (1991) algorithm:
        last >= ask  → buyer-aggressor
        last <= bid  → seller-aggressor
        midpoint     → fall back to tick rule on last_prices

    Inputs must be aligned numpy arrays from mt5.copy_ticks_range.
    """
    last = np.asarray(last_prices, dtype=np.float64)
    b = np.asarray(bids, dtype=np.float64)
    a = np.asarray(asks, dtype=np.float64)

    out = np.zeros(len(last), dtype=np.int8)
    out[last >= a] = 1
    out[last <= b] = -1
    # Midpoint ticks: fall back to tick-rule
    mid_mask = out == 0
    if mid_mask.any():
        # Use change vs previous last for mid-quote ticks
        diffs = np.zeros(len(last))
        diffs[1:] = np.sign(last[1:] - last[:-1])
        out[mid_mask] = diffs[mid_mask].astype(np.int8)
    return out


def aggressor_imbalance(aggressors: np.ndarray, volumes: Optional[np.ndarray] = None) -> float:
    """(buyers - sellers) / (buyers + sellers) ratio. Volume-weighted if volumes provided."""
    a = np.asarray(aggressors)
    if volumes is not None:
        v = np.asarray(volumes, dtype=np.float64)
        buy = float(v[a > 0].sum())
        sell = float(v[a < 0].sum())
    else:
        buy = float((a > 0).sum())
        sell = float((a < 0).sum())
    total = buy + sell
    return (buy - sell) / total if total > 0 else 0.0


# ──────────────────────────────────────────────────────────────────
# 3. VPIN — Volume-Synchronized PIN (Easley, López de Prado)
# ──────────────────────────────────────────────────────────────────

def vpin(
    aggressors: np.ndarray,
    volumes: np.ndarray,
    bucket_size: int = 50,
    window_buckets: int = 50,
) -> float:
    """Compute VPIN — running average of |buy_vol - sell_vol| / total_vol over
    equal-volume buckets.

    High VPIN (>0.5) = informed-trader pressure / order-flow toxicity.
    """
    a = np.asarray(aggressors)
    v = np.asarray(volumes, dtype=np.float64)
    if len(a) == 0 or v.sum() <= 0:
        return 0.0

    cum_v = np.cumsum(v)
    bucket_thresholds = np.arange(bucket_size, cum_v[-1] + 1, bucket_size)
    if len(bucket_thresholds) < window_buckets:
        return 0.0

    imbalances = []
    last_pos = 0
    for thresh in bucket_thresholds:
        pos = int(np.searchsorted(cum_v, thresh))
        if pos <= last_pos:
            continue
        seg_a = a[last_pos: pos + 1]
        seg_v = v[last_pos: pos + 1]
        buy = float(seg_v[seg_a > 0].sum())
        sell = float(seg_v[seg_a < 0].sum())
        tot = buy + sell
        if tot > 0:
            imbalances.append(abs(buy - sell) / tot)
        last_pos = pos + 1
    if len(imbalances) < window_buckets:
        return 0.0
    return float(np.mean(imbalances[-window_buckets:]))


# ──────────────────────────────────────────────────────────────────
# 4. KYLE'S LAMBDA — price impact per signed volume
# ──────────────────────────────────────────────────────────────────

def kyle_lambda(
    closes: np.ndarray,
    signed_volumes: np.ndarray,
) -> float:
    """Kyle's λ via OLS regression of price changes on signed volumes.

    Δprice = λ × signed_volume + ε

    Higher λ = more price impact per traded unit = less liquidity.
    """
    c = np.asarray(closes, dtype=np.float64)
    sv = np.asarray(signed_volumes, dtype=np.float64)
    if len(c) < 3:
        return 0.0
    dp = np.diff(c)
    sv_aligned = sv[1:]
    if len(dp) != len(sv_aligned) or sv_aligned.std() < 1e-12:
        return 0.0
    cov = np.cov(dp, sv_aligned, ddof=1)[0, 1]
    var = np.var(sv_aligned, ddof=1)
    return float(cov / var) if var > 1e-12 else 0.0


# ──────────────────────────────────────────────────────────────────
# 5. AMIHUD ILLIQUIDITY — |return| / volume
# ──────────────────────────────────────────────────────────────────

def amihud_illiquidity(closes: np.ndarray, volumes: np.ndarray) -> float:
    """Daily/window Amihud illiquidity: mean(|return| / volume) × 1e6.

    Scaled so values are interpretable. Higher = less liquid.
    """
    c = np.asarray(closes, dtype=np.float64)
    v = np.asarray(volumes, dtype=np.float64)
    if len(c) < 2:
        return 0.0
    rets = np.abs(np.diff(c) / c[:-1])
    vols = v[1:]
    valid = vols > 0
    if not valid.any():
        return 0.0
    ratio = rets[valid] / vols[valid]
    return float(ratio.mean() * 1e6)


# ──────────────────────────────────────────────────────────────────
# 6. QUOTE-TO-TRADE RATIO
# ──────────────────────────────────────────────────────────────────

def quote_to_trade_ratio(quote_count: int, trade_count: int) -> float:
    """Quote updates per executed trade. High = MM hesitation."""
    if trade_count <= 0:
        return float("inf")
    return quote_count / trade_count


# ──────────────────────────────────────────────────────────────────
# 7. EFFECTIVE / REALIZED SPREAD (tick-level)
# ──────────────────────────────────────────────────────────────────

def effective_spread(
    last_prices: np.ndarray,
    bids: np.ndarray,
    asks: np.ndarray,
    aggressors: np.ndarray,
) -> float:
    """Effective spread = 2 × Q × (last - midpoint), averaged.
    Q = +1 buyer-initiated, -1 seller-initiated.
    """
    last = np.asarray(last_prices, dtype=np.float64)
    b = np.asarray(bids, dtype=np.float64)
    a = np.asarray(asks, dtype=np.float64)
    q = np.asarray(aggressors, dtype=np.float64)
    midpoint = (a + b) / 2.0
    es = 2.0 * q * (last - midpoint)
    return float(np.mean(es)) if len(es) else 0.0


def realized_spread(
    last_prices: np.ndarray,
    bids: np.ndarray,
    asks: np.ndarray,
    aggressors: np.ndarray,
    horizon_ticks: int = 30,
) -> float:
    """Realized spread = 2 × Q × (last - mid_{t+h}). Measures price reversion
    after trade — the "true" spread market makers earn after adverse selection.
    """
    last = np.asarray(last_prices, dtype=np.float64)
    b = np.asarray(bids, dtype=np.float64)
    a = np.asarray(asks, dtype=np.float64)
    q = np.asarray(aggressors, dtype=np.float64)
    midpoint = (a + b) / 2.0
    n = len(last)
    if n < horizon_ticks + 1:
        return 0.0
    rs = 2.0 * q[: n - horizon_ticks] * (last[: n - horizon_ticks] - midpoint[horizon_ticks:])
    return float(np.mean(rs))


# ──────────────────────────────────────────────────────────────────
# 8. ICEBERG ORDER PATTERN SCORE
# ──────────────────────────────────────────────────────────────────

def iceberg_pattern_score(
    last_prices: np.ndarray,
    volumes: np.ndarray,
    price_tolerance_pips: float = 0.5,
    pip_size: float = 0.0001,
    min_repeats: int = 5,
) -> tuple[float, list]:
    """Detect iceberg-like patterns: many trades at the same price level.

    Returns (max_concentration_score, list_of_iceberg_levels).
    score = volume at most-traded level / total volume in window.
    Score > 0.3 with ≥ min_repeats hits suggests a real iceberg.
    """
    last = np.asarray(last_prices, dtype=np.float64)
    v = np.asarray(volumes, dtype=np.float64)
    if len(last) == 0:
        return (0.0, [])
    rounded = np.round(last / (price_tolerance_pips * pip_size)) * (price_tolerance_pips * pip_size)
    levels, counts = np.unique(rounded, return_counts=True)
    if len(levels) == 0:
        return (0.0, [])
    total_v = v.sum()
    if total_v <= 0:
        return (0.0, [])
    iceberg_levels = []
    max_conc = 0.0
    for lvl, cnt in zip(levels, counts):
        if cnt < min_repeats:
            continue
        lvl_v = float(v[rounded == lvl].sum())
        conc = lvl_v / total_v
        if conc > max_conc:
            max_conc = conc
        if conc > 0.30:
            iceberg_levels.append((float(lvl), int(cnt), conc))
    return (max_conc, iceberg_levels)
