"""Volatility estimators — model-free + parametric forecasts.

All operate on numpy OHLC arrays. No external deps beyond numpy.

Estimators implemented:
    realized_variance        — sum of squared M1 returns
    parkinson                — high-low based (5x more efficient than C-C)
    garman_klass             — OHLC-based (7.4x more efficient)
    rogers_satchell          — drift-robust OHLC
    yang_zhang               — most efficient OHLC estimator (overnight + intraday)
    bipower_variation        — jump-robust
    realized_skew            — third moment of returns
    realized_kurtosis        — fourth moment
    lee_mykland_jump         — statistical test for jumps
    volatility_ratio         — short vs long term ratio
    vol_of_vol               — stddev of recent vol estimates
    atr                      — Wilder ATR
    atr_percentile_rank      — current ATR rank in trailing distribution
    har_rv_forecast          — HAR-RV daily/weekly/monthly forecast (Corsi 2009)
    bollinger_bands          — classic indicator
    keltner_channels         — ATR-based channel
"""
from __future__ import annotations

import numpy as np


def realized_variance(closes: np.ndarray) -> float:
    """Sum of squared log returns (model-free vol)."""
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < 2:
        return 0.0
    log_rets = np.diff(np.log(c))
    return float(np.sum(log_rets ** 2))


def parkinson(highs: np.ndarray, lows: np.ndarray) -> float:
    """Parkinson 1980 estimator — vol from H-L range only.

    σ² = (1 / 4 ln 2) × mean(ln(H/L)²)
    """
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    if len(h) == 0:
        return 0.0
    log_hl = np.log(h / lo)
    var = (1.0 / (4.0 * np.log(2.0))) * np.mean(log_hl ** 2)
    return float(np.sqrt(var))


def garman_klass(opens: np.ndarray, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> float:
    """Garman-Klass 1980 estimator — uses all four OHLC values."""
    o = np.asarray(opens, dtype=np.float64)
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    c = np.asarray(closes, dtype=np.float64)
    if len(o) == 0:
        return 0.0
    log_hl = np.log(h / lo)
    log_co = np.log(c / o)
    var = np.mean(0.5 * log_hl ** 2 - (2 * np.log(2) - 1) * log_co ** 2)
    return float(np.sqrt(max(var, 0)))


def rogers_satchell(opens: np.ndarray, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> float:
    """Rogers-Satchell — drift-robust OHLC vol."""
    o = np.asarray(opens, dtype=np.float64)
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    c = np.asarray(closes, dtype=np.float64)
    if len(o) == 0:
        return 0.0
    var = np.mean(
        np.log(h / c) * np.log(h / o) + np.log(lo / c) * np.log(lo / o)
    )
    return float(np.sqrt(max(var, 0)))


def yang_zhang(opens: np.ndarray, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> float:
    """Yang-Zhang 2000 — combines overnight + intraday, most efficient OHLC est."""
    o = np.asarray(opens, dtype=np.float64)
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    c = np.asarray(closes, dtype=np.float64)
    n = len(o)
    if n < 2:
        return 0.0
    overnight = np.log(o[1:] / c[:-1])
    open_close = np.log(c / o)
    var_overnight = np.var(overnight, ddof=1) if len(overnight) > 1 else 0.0
    var_oc = np.var(open_close, ddof=1) if len(open_close) > 1 else 0.0
    rs_var = np.mean(
        np.log(h / c) * np.log(h / o) + np.log(lo / c) * np.log(lo / o)
    )
    k = 0.34 / (1.34 + (n + 1) / (n - 1))
    var_yz = var_overnight + k * var_oc + (1 - k) * rs_var
    return float(np.sqrt(max(var_yz, 0)))


def bipower_variation(closes: np.ndarray) -> float:
    """Barndorff-Nielsen jump-robust vol estimate.

    BV = (π/2) × Σ |r_t| × |r_{t-1}|
    """
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < 3:
        return 0.0
    log_rets = np.abs(np.diff(np.log(c)))
    return float((np.pi / 2.0) * np.sum(log_rets[1:] * log_rets[:-1]))


def realized_skew(closes: np.ndarray) -> float:
    """Skewness of returns."""
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < 3:
        return 0.0
    rets = np.diff(c) / c[:-1]
    if rets.std() < 1e-12:
        return 0.0
    return float(np.mean(((rets - rets.mean()) / rets.std()) ** 3))


def realized_kurtosis(closes: np.ndarray) -> float:
    """Excess kurtosis of returns."""
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < 4:
        return 0.0
    rets = np.diff(c) / c[:-1]
    if rets.std() < 1e-12:
        return 0.0
    return float(np.mean(((rets - rets.mean()) / rets.std()) ** 4) - 3.0)


def lee_mykland_jump(
    closes: np.ndarray,
    bv_window: int = 270,
    threshold: float = 4.6,
) -> tuple[bool, float]:
    """Lee-Mykland 2008 jump detection test.

    Returns (is_jump, test_statistic). |stat| > threshold (default 4.6 ≈ 99% CI)
    indicates a jump occurred at the latest bar.
    """
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < bv_window + 1:
        return (False, 0.0)
    log_rets = np.diff(np.log(c))
    if len(log_rets) < bv_window:
        return (False, 0.0)
    # BV in the trailing window (excludes latest)
    window = log_rets[-bv_window - 1: -1]
    bv = (np.pi / 2.0) * np.sum(np.abs(window[1:]) * np.abs(window[:-1]))
    sigma_hat = np.sqrt(bv / bv_window) if bv > 0 else 1e-12
    stat = log_rets[-1] / sigma_hat if sigma_hat > 0 else 0.0
    return (abs(stat) > threshold, float(stat))


def volatility_ratio(closes: np.ndarray, short_window: int = 5, long_window: int = 60) -> float:
    """Ratio of short-term to long-term realized vol. >1 = expanding."""
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < long_window + 1:
        return 1.0
    rets = np.diff(c) / c[:-1]
    if len(rets) < long_window:
        return 1.0
    short_vol = float(np.std(rets[-short_window:], ddof=1))
    long_vol = float(np.std(rets[-long_window:], ddof=1))
    return short_vol / long_vol if long_vol > 1e-12 else 1.0


def vol_of_vol(closes: np.ndarray, vol_window: int = 20, lookback_bars: int = 30) -> float:
    """Stddev of recent realized-vol estimates (vol-of-vol)."""
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < vol_window + lookback_bars:
        return 0.0
    rets = np.diff(c) / c[:-1]
    if len(rets) < vol_window + lookback_bars:
        return 0.0
    vols = []
    for i in range(lookback_bars):
        end = len(rets) - i
        start = end - vol_window
        if start < 0:
            break
        vols.append(np.std(rets[start:end], ddof=1))
    return float(np.std(vols, ddof=1)) if len(vols) > 1 else 0.0


def atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> float:
    """Wilder ATR (smoothed True Range)."""
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    c = np.asarray(closes, dtype=np.float64)
    n = len(c)
    if n < period + 1:
        return 0.0
    tr = np.empty(n)
    tr[0] = h[0] - lo[0]
    for i in range(1, n):
        tr[i] = max(h[i] - lo[i], abs(h[i] - c[i - 1]), abs(lo[i] - c[i - 1]))
    atr_val = tr[:period].mean()
    for i in range(period, n):
        atr_val = (atr_val * (period - 1) + tr[i]) / period
    return float(atr_val)


def atr_percentile_rank(
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    period: int = 14,
    lookback_bars: int = 1440,  # ~60 days of H1
) -> float:
    """Current ATR's percentile rank in the trailing `lookback_bars` window."""
    h = np.asarray(highs, dtype=np.float64)
    lo = np.asarray(lows, dtype=np.float64)
    c = np.asarray(closes, dtype=np.float64)
    n = len(c)
    if n < period + lookback_bars:
        return 50.0
    # Compute rolling ATR for the lookback window
    atrs = []
    for end in range(period, n):
        sub_h = h[end - period: end]
        sub_l = lo[end - period: end]
        sub_c = c[end - period: end]
        a = atr(sub_h, sub_l, sub_c, period=period)
        atrs.append(a)
    if len(atrs) < lookback_bars:
        return 50.0
    cur = atrs[-1]
    sample = atrs[-lookback_bars:]
    rank = (np.array(sample) < cur).sum() / len(sample) * 100
    return float(rank)


def har_rv_forecast(rv_daily: np.ndarray) -> float:
    """HAR-RV (Corsi 2009) — heterogeneous autoregressive realized vol forecast.

    RV_t+1 = β_d × RV_d + β_w × RV_5d + β_m × RV_22d
    Returns 1-step-ahead forecast.

    Note: this is the FORECAST given coefficients. To estimate coefficients,
    run OLS on past data; here we use simple equal-weighted as fallback.
    """
    rv = np.asarray(rv_daily, dtype=np.float64)
    if len(rv) < 22:
        return float(rv[-1]) if len(rv) > 0 else 0.0
    rv_d = rv[-1]
    rv_w = float(np.mean(rv[-5:]))
    rv_m = float(np.mean(rv[-22:]))
    # Equal-weighted fallback (true coefficients require fit)
    return (rv_d + rv_w + rv_m) / 3.0


def bollinger_bands(
    closes: np.ndarray,
    period: int = 20,
    num_std: float = 2.0,
) -> tuple[float, float, float]:
    """Returns (upper, middle/SMA, lower)."""
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < period:
        return (0.0, 0.0, 0.0)
    sub = c[-period:]
    sma = float(sub.mean())
    std = float(sub.std(ddof=1))
    return (sma + num_std * std, sma, sma - num_std * std)


def keltner_channels(
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    period: int = 20,
    atr_mult: float = 1.5,
) -> tuple[float, float, float]:
    """Keltner channel (EMA midline ± ATR multiplier)."""
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < period + 1:
        return (0.0, 0.0, 0.0)
    a = atr(highs, lows, closes, period=10)
    alpha = 2.0 / (period + 1)
    ema = float(c[0])
    for v in c[1:]:
        ema = alpha * v + (1 - alpha) * ema
    return (ema + atr_mult * a, ema, ema - atr_mult * a)
