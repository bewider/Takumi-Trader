"""Regime classifiers and trend/range indicators.

Implemented:
    adx                   — Average Directional Index (Wilder)
    choppiness_index      — pure trend/range distinguisher
    aroon                 — Aroon Up/Down (time-since-high/low)
    vortex                — VI+ / VI-
    kama                  — Kaufman Adaptive Moving Average
    supertrend            — ATR-based trend signal
    ichimoku              — full 5-component cloud
    donchian_position     — % position in 20-bar Donchian range
    linear_regression     — slope + R²
    hurst_exponent        — fractal trend/range classifier
    detrended_fluctuation — DFA scaling exponent (Hurst alternative)
    kaufman_efficiency    — efficiency ratio (price net move / total move)
    trend_persistence     — % of bars in trend direction
    mean_reversion_zscore — z-score from MA
    regime_classify       — composite regime label (TREND/RANGE/CHOP)
"""
from __future__ import annotations

import numpy as np


def adx(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> tuple[float, float, float]:
    """Wilder's ADX. Returns (ADX, +DI, -DI). ADX > 25 = strong trend."""
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    c = np.asarray(closes, dtype=np.float64)
    n = len(c)
    if n < period * 2:
        return (0.0, 0.0, 0.0)
    plus_dm = np.zeros(n)
    minus_dm = np.zeros(n)
    tr = np.zeros(n)
    for i in range(1, n):
        up_move = h[i] - h[i - 1]
        down_move = lo[i - 1] - lo[i]
        if up_move > down_move and up_move > 0:
            plus_dm[i] = up_move
        if down_move > up_move and down_move > 0:
            minus_dm[i] = down_move
        tr[i] = max(h[i] - lo[i], abs(h[i] - c[i - 1]), abs(lo[i] - c[i - 1]))

    # Wilder smoothing
    smooth_plus = np.zeros(n)
    smooth_minus = np.zeros(n)
    smooth_tr = np.zeros(n)
    smooth_plus[period] = plus_dm[1: period + 1].sum()
    smooth_minus[period] = minus_dm[1: period + 1].sum()
    smooth_tr[period] = tr[1: period + 1].sum()
    for i in range(period + 1, n):
        smooth_plus[i] = smooth_plus[i - 1] - smooth_plus[i - 1] / period + plus_dm[i]
        smooth_minus[i] = smooth_minus[i - 1] - smooth_minus[i - 1] / period + minus_dm[i]
        smooth_tr[i] = smooth_tr[i - 1] - smooth_tr[i - 1] / period + tr[i]

    plus_di = 100 * smooth_plus / np.where(smooth_tr > 0, smooth_tr, 1)
    minus_di = 100 * smooth_minus / np.where(smooth_tr > 0, smooth_tr, 1)
    dx = 100 * np.abs(plus_di - minus_di) / np.where(plus_di + minus_di > 0, plus_di + minus_di, 1)
    adx_val = np.zeros(n)
    if n > period * 2:
        adx_val[period * 2] = dx[period + 1: period * 2 + 1].mean()
        for i in range(period * 2 + 1, n):
            adx_val[i] = (adx_val[i - 1] * (period - 1) + dx[i]) / period
    return (float(adx_val[-1]), float(plus_di[-1]), float(minus_di[-1]))


def choppiness_index(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> float:
    """Choppiness Index — 0 to 100. >61.8 = choppy/range, <38.2 = trending.

    CI = 100 × log10(Σ ATR / (Hmax - Lmin)) / log10(period)
    """
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < period + 1:
        return 50.0
    tr = np.zeros(period)
    for i in range(period):
        idx = len(c) - period + i
        tr[i] = max(h[idx] - lo[idx], abs(h[idx] - c[idx - 1]), abs(lo[idx] - c[idx - 1]))
    sum_tr = tr.sum()
    h_max = h[-period:].max()
    l_min = lo[-period:].min()
    if h_max == l_min or sum_tr <= 0:
        return 50.0
    return float(100.0 * np.log10(sum_tr / (h_max - l_min)) / np.log10(period))


def aroon(highs: np.ndarray, lows: np.ndarray, period: int = 14) -> tuple[float, float, float]:
    """Aroon Up / Down / Oscillator (-100 to +100)."""
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    if len(h) < period + 1:
        return (50.0, 50.0, 0.0)
    sub_h = h[-period - 1:]
    sub_l = lo[-period - 1:]
    bars_since_high = period - int(np.argmax(sub_h))
    bars_since_low = period - int(np.argmin(sub_l))
    aroon_up = 100.0 * (period - bars_since_high) / period
    aroon_down = 100.0 * (period - bars_since_low) / period
    return (aroon_up, aroon_down, aroon_up - aroon_down)


def vortex(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> tuple[float, float]:
    """Vortex Indicator (VI+ and VI-)."""
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    c = np.asarray(closes, dtype=np.float64)
    n = len(c)
    if n < period + 1:
        return (1.0, 1.0)
    vm_plus = np.abs(h[1:] - lo[:-1])
    vm_minus = np.abs(lo[1:] - h[:-1])
    tr = np.maximum.reduce([
        h[1:] - lo[1:],
        np.abs(h[1:] - c[:-1]),
        np.abs(lo[1:] - c[:-1]),
    ])
    if len(vm_plus) < period:
        return (1.0, 1.0)
    sum_tr = tr[-period:].sum()
    if sum_tr <= 0:
        return (1.0, 1.0)
    return (
        float(vm_plus[-period:].sum() / sum_tr),
        float(vm_minus[-period:].sum() / sum_tr),
    )


def kama(closes: np.ndarray, period: int = 10, fast: int = 2, slow: int = 30) -> float:
    """Kaufman Adaptive Moving Average — adjusts speed based on efficiency ratio."""
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < period + 1:
        return float(c[-1]) if len(c) else 0.0
    direction = abs(c[-1] - c[-period - 1])
    volatility = np.sum(np.abs(np.diff(c[-period - 1:])))
    er = direction / volatility if volatility > 0 else 0.0
    fast_sc = 2.0 / (fast + 1)
    slow_sc = 2.0 / (slow + 1)
    sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2
    kama_val = float(c[-period - 1])
    for i in range(period, len(c)):
        kama_val = kama_val + sc * (c[i] - kama_val)
    return kama_val


def supertrend(highs, lows, closes, atr_period: int = 10, multiplier: float = 3.0) -> tuple[float, int]:
    """SuperTrend indicator. Returns (band_value, direction).

    direction = +1 (up trend), -1 (down trend).
    """
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    c = np.asarray(closes, dtype=np.float64)
    n = len(c)
    if n < atr_period + 1:
        return (0.0, 0)
    tr = np.zeros(n)
    tr[0] = h[0] - lo[0]
    for i in range(1, n):
        tr[i] = max(h[i] - lo[i], abs(h[i] - c[i - 1]), abs(lo[i] - c[i - 1]))
    atr_val = tr[:atr_period].mean()
    for i in range(atr_period, n):
        atr_val = (atr_val * (atr_period - 1) + tr[i]) / atr_period
    hl2 = (h[-1] + lo[-1]) / 2
    upper = hl2 + multiplier * atr_val
    lower = hl2 - multiplier * atr_val
    if c[-1] > (upper + lower) / 2:
        return (lower, 1)
    return (upper, -1)


def ichimoku(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> dict:
    """Ichimoku Cloud — all 5 components.

    Tenkan (9):  conversion line
    Kijun (26):  base line
    Senkou A:    midpoint of Tenkan/Kijun, plotted 26 periods ahead
    Senkou B:    52-period midpoint, plotted 26 ahead
    Chikou:      close, plotted 26 periods behind
    """
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < 52:
        return {"tenkan": 0, "kijun": 0, "senkou_a": 0, "senkou_b": 0, "chikou": 0,
                "above_cloud": False, "in_cloud": False, "below_cloud": False}
    tenkan = (h[-9:].max() + lo[-9:].min()) / 2
    kijun = (h[-26:].max() + lo[-26:].min()) / 2
    senkou_a = (tenkan + kijun) / 2
    senkou_b = (h[-52:].max() + lo[-52:].min()) / 2
    chikou = c[-1]
    cloud_top = max(senkou_a, senkou_b)
    cloud_bot = min(senkou_a, senkou_b)
    return {
        "tenkan": float(tenkan),
        "kijun": float(kijun),
        "senkou_a": float(senkou_a),
        "senkou_b": float(senkou_b),
        "chikou": float(chikou),
        "above_cloud": bool(c[-1] > cloud_top),
        "in_cloud": bool(cloud_bot <= c[-1] <= cloud_top),
        "below_cloud": bool(c[-1] < cloud_bot),
    }


def donchian_position(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 20) -> float:
    """Position within Donchian range. 0 = at low, 1 = at high."""
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < period:
        return 0.5
    h_max = h[-period:].max()
    l_min = lo[-period:].min()
    if h_max == l_min:
        return 0.5
    return float((c[-1] - l_min) / (h_max - l_min))


def linear_regression(closes: np.ndarray, period: int = 20) -> tuple[float, float]:
    """Linear regression slope + R² over last `period` bars."""
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < period:
        return (0.0, 0.0)
    y = c[-period:]
    x = np.arange(period)
    slope, intercept = np.polyfit(x, y, 1)
    y_pred = slope * x + intercept
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 1e-12 else 0.0
    return (float(slope), float(r2))


def hurst_exponent(closes: np.ndarray, max_lag: int = 50) -> float:
    """Hurst exponent via R/S analysis.

    H > 0.5 → trending (persistent)
    H = 0.5 → random walk
    H < 0.5 → mean-reverting (anti-persistent)
    """
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < max_lag * 2:
        return 0.5
    log_rets = np.diff(np.log(c))
    lags = range(2, max_lag)
    rs_vals = []
    for lag in lags:
        chunks = len(log_rets) // lag
        if chunks < 1:
            continue
        rs_chunk = []
        for i in range(chunks):
            chunk = log_rets[i * lag: (i + 1) * lag]
            if chunk.std() < 1e-12:
                continue
            mean = chunk.mean()
            cum_dev = np.cumsum(chunk - mean)
            r = cum_dev.max() - cum_dev.min()
            s = chunk.std()
            rs_chunk.append(r / s if s > 0 else 0)
        if rs_chunk:
            rs_vals.append((lag, np.mean(rs_chunk)))
    if len(rs_vals) < 5:
        return 0.5
    log_lags = np.log([x[0] for x in rs_vals])
    log_rs = np.log([x[1] for x in rs_vals if x[1] > 0])
    if len(log_rs) < 5 or len(log_lags) != len(log_rs):
        return 0.5
    h, _ = np.polyfit(log_lags[: len(log_rs)], log_rs, 1)
    return float(h)


def detrended_fluctuation(closes: np.ndarray, max_window: int = 50) -> float:
    """DFA scaling exponent — alternative to Hurst, robust to non-stationarity."""
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < max_window * 2:
        return 0.5
    log_rets = np.diff(np.log(c))
    y = np.cumsum(log_rets - log_rets.mean())
    windows = np.unique(np.logspace(0.5, np.log10(max_window), 10).astype(int))
    fluct = []
    for w in windows:
        if len(y) < w * 2:
            continue
        n_segs = len(y) // w
        rms = []
        for i in range(n_segs):
            seg = y[i * w: (i + 1) * w]
            x_seg = np.arange(w)
            slope, intercept = np.polyfit(x_seg, seg, 1)
            detrended = seg - (slope * x_seg + intercept)
            rms.append(np.sqrt(np.mean(detrended ** 2)))
        if rms:
            fluct.append((w, np.mean(rms)))
    if len(fluct) < 4:
        return 0.5
    log_w = np.log([x[0] for x in fluct])
    log_f = np.log([x[1] for x in fluct if x[1] > 0])
    if len(log_f) < 4:
        return 0.5
    alpha, _ = np.polyfit(log_w[: len(log_f)], log_f, 1)
    return float(alpha)


def kaufman_efficiency_ratio(closes: np.ndarray, period: int = 20) -> float:
    """Kaufman ER: |net price change| / sum of absolute changes."""
    c = np.asarray(closes, dtype=np.float64)
    if len(c) <= period:
        return 0.0
    direction = abs(c[-1] - c[-period - 1])
    volatility = np.sum(np.abs(np.diff(c[-period - 1:])))
    return float(direction / volatility) if volatility > 0 else 0.0


def trend_persistence(closes: np.ndarray, ma_period: int = 50) -> float:
    """% of bars in same direction as moving average slope, last `ma_period` bars."""
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < ma_period * 2:
        return 0.5
    sma = np.convolve(c, np.ones(ma_period) / ma_period, mode="valid")
    if len(sma) < 2:
        return 0.5
    sma_slope = np.sign(np.diff(sma))
    rets = np.sign(np.diff(c[-len(sma_slope) - 1:]))
    aligned = (sma_slope == rets).sum()
    return float(aligned / len(sma_slope))


def mean_reversion_zscore(closes: np.ndarray, ma_period: int = 20) -> float:
    """Z-score of current price from MA. Large |z| = mean-reversion candidate."""
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < ma_period:
        return 0.0
    sub = c[-ma_period:]
    mean = sub.mean()
    std = sub.std(ddof=1)
    if std < 1e-12:
        return 0.0
    return float((c[-1] - mean) / std)


def regime_classify(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> str:
    """Composite regime classifier using ADX + Choppiness + Hurst.

    Returns "TREND_UP" | "TREND_DOWN" | "RANGE" | "CHOP" | "MIXED".
    """
    adx_val, plus_di, minus_di = adx(highs, lows, closes)
    chop = choppiness_index(highs, lows, closes)
    h = hurst_exponent(closes, max_lag=20)
    if adx_val > 25 and chop < 50:
        if plus_di > minus_di and h > 0.5:
            return "TREND_UP"
        if minus_di > plus_di and h > 0.5:
            return "TREND_DOWN"
    if chop > 61.8:
        return "CHOP"
    if h < 0.45 and adx_val < 20:
        return "RANGE"
    return "MIXED"
