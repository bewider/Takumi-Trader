"""Statistical features — autocorrelation, spectral, cointegration, drift.

All numpy-based, no statsmodels dependency for portability.
"""
from __future__ import annotations

import numpy as np


def autocorrelation(closes: np.ndarray, lags: list[int] = None) -> dict:
    """Compute return autocorrelation at given lags."""
    if lags is None:
        lags = [1, 5, 15]
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < max(lags) + 2:
        return {f"acf_lag_{k}": 0.0 for k in lags}
    rets = np.diff(c) / c[:-1]
    out = {}
    for k in lags:
        if len(rets) <= k:
            out[f"acf_lag_{k}"] = 0.0
            continue
        a = rets[:-k]
        b = rets[k:]
        if a.std() < 1e-12 or b.std() < 1e-12:
            out[f"acf_lag_{k}"] = 0.0
        else:
            out[f"acf_lag_{k}"] = float(np.corrcoef(a, b)[0, 1])
    return out


def partial_autocorrelation(closes: np.ndarray, lags: list[int] = None) -> dict:
    """Approximate PACF via Durbin-Levinson recursion."""
    if lags is None:
        lags = [1, 5, 15]
    c = np.asarray(closes, dtype=np.float64)
    max_lag = max(lags)
    if len(c) < max_lag + 2:
        return {f"pacf_lag_{k}": 0.0 for k in lags}
    rets = np.diff(c) / c[:-1]
    if rets.std() < 1e-12:
        return {f"pacf_lag_{k}": 0.0 for k in lags}
    rets = rets - rets.mean()
    # ACF first
    n = len(rets)
    var = np.var(rets, ddof=0)
    acf = []
    for k in range(max_lag + 1):
        if k == 0:
            acf.append(1.0)
        else:
            acf.append(np.sum(rets[:-k] * rets[k:]) / (n * var))
    # Durbin-Levinson
    pacf = [1.0]
    phi = [[0.0] * (max_lag + 1) for _ in range(max_lag + 1)]
    for k in range(1, max_lag + 1):
        if k == 1:
            phi[1][1] = acf[1]
        else:
            num = acf[k] - sum(phi[k - 1][j] * acf[k - j] for j in range(1, k))
            den = 1 - sum(phi[k - 1][j] * acf[j] for j in range(1, k))
            phi[k][k] = num / den if abs(den) > 1e-12 else 0.0
            for j in range(1, k):
                phi[k][j] = phi[k - 1][j] - phi[k][k] * phi[k - 1][k - j]
        pacf.append(phi[k][k])
    return {f"pacf_lag_{k}": float(pacf[k]) for k in lags}


def rolling_skew(closes: np.ndarray, window: int = 60) -> float:
    """Rolling skewness over `window` returns."""
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < window + 1:
        return 0.0
    rets = np.diff(c[-window - 1:]) / c[-window - 1: -1]
    if rets.std() < 1e-12:
        return 0.0
    return float(np.mean(((rets - rets.mean()) / rets.std()) ** 3))


def rolling_kurtosis(closes: np.ndarray, window: int = 60) -> float:
    """Rolling excess kurtosis."""
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < window + 1:
        return 0.0
    rets = np.diff(c[-window - 1:]) / c[-window - 1: -1]
    if rets.std() < 1e-12:
        return 0.0
    return float(np.mean(((rets - rets.mean()) / rets.std()) ** 4) - 3.0)


def fft_dominant_frequency(closes: np.ndarray, sample_rate_minutes: float = 1.0) -> dict:
    """Dominant cycle frequency via FFT.

    Returns dominant period (in minutes) and amplitude ratio.
    """
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < 32:
        return {"dominant_period_min": 0.0, "amplitude_ratio": 0.0}
    detrended = c - np.mean(c)
    fft = np.fft.rfft(detrended)
    freqs = np.fft.rfftfreq(len(detrended), d=sample_rate_minutes)
    magnitudes = np.abs(fft)
    if len(magnitudes) < 2:
        return {"dominant_period_min": 0.0, "amplitude_ratio": 0.0}
    # Skip DC
    dom_idx = int(np.argmax(magnitudes[1:])) + 1
    dom_freq = freqs[dom_idx]
    dom_period = 1.0 / dom_freq if dom_freq > 0 else 0.0
    amp_ratio = float(magnitudes[dom_idx] / magnitudes[1:].sum()) if magnitudes[1:].sum() > 0 else 0.0
    return {"dominant_period_min": float(dom_period), "amplitude_ratio": amp_ratio}


def granger_causality_pairwise(x: np.ndarray, y: np.ndarray, lag: int = 5) -> dict:
    """Approximate Granger causality test: does x[t-lag] predict y[t]?

    Compares unrestricted (with x lags) vs restricted (only y lags) regression.
    Returns F-statistic approx and p-value flag.
    """
    x_arr = np.asarray(x, dtype=np.float64)
    y_arr = np.asarray(y, dtype=np.float64)
    n = min(len(x_arr), len(y_arr))
    if n < lag * 4:
        return {"f_stat": 0.0, "x_predicts_y": False}
    x_arr = x_arr[-n:]
    y_arr = y_arr[-n:]

    # Build restricted regression: y[t] = a + b*y[t-1] + ... + b_lag*y[t-lag] + e
    Y = y_arr[lag:]
    X_restricted = np.column_stack([np.ones(len(Y))] + [y_arr[lag - k: -k or None] for k in range(1, lag + 1)])
    # Unrestricted: add x lags
    X_unrestricted = np.column_stack([X_restricted] + [x_arr[lag - k: -k or None] for k in range(1, lag + 1)])

    # OLS
    def rss(X, Y):
        coef, _, _, _ = np.linalg.lstsq(X, Y, rcond=None)
        residuals = Y - X @ coef
        return float(np.sum(residuals ** 2))

    rss_r = rss(X_restricted, Y)
    rss_u = rss(X_unrestricted, Y)
    if rss_u <= 0 or rss_r <= rss_u:
        return {"f_stat": 0.0, "x_predicts_y": False}
    n_obs = len(Y)
    df_num = lag
    df_den = n_obs - 2 * lag - 1
    if df_den <= 0:
        return {"f_stat": 0.0, "x_predicts_y": False}
    f_stat = ((rss_r - rss_u) / df_num) / (rss_u / df_den)
    return {"f_stat": float(f_stat), "x_predicts_y": bool(f_stat > 3.0)}


def cointegration_engle_granger(x: np.ndarray, y: np.ndarray) -> dict:
    """Engle-Granger 2-step cointegration test.

    1. Regress y = a + b*x + ε
    2. Test residuals for stationarity via ADF
    Returns spread half-life if cointegrated.
    """
    x_arr = np.asarray(x, dtype=np.float64)
    y_arr = np.asarray(y, dtype=np.float64)
    n = min(len(x_arr), len(y_arr))
    if n < 30:
        return {"is_cointegrated": False, "beta": 0.0, "half_life": 0.0, "spread": 0.0}
    x_arr = x_arr[-n:]
    y_arr = y_arr[-n:]
    # Step 1: regression
    X = np.column_stack([np.ones(n), x_arr])
    coef, _, _, _ = np.linalg.lstsq(X, y_arr, rcond=None)
    a, b = coef[0], coef[1]
    spread = y_arr - (a + b * x_arr)
    # Step 2: ADF test approximation via lag-1 regression
    if len(spread) < 5:
        return {"is_cointegrated": False, "beta": 0.0, "half_life": 0.0, "spread": float(spread[-1])}
    d_spread = np.diff(spread)
    spread_lag = spread[:-1]
    if spread_lag.std() < 1e-12:
        return {"is_cointegrated": False, "beta": float(b), "half_life": 0.0, "spread": float(spread[-1])}
    X2 = np.column_stack([np.ones(len(d_spread)), spread_lag])
    coef2, _, _, _ = np.linalg.lstsq(X2, d_spread, rcond=None)
    rho = coef2[1]
    # If rho < 0 strongly, spread mean-reverts
    is_coint = rho < -0.05
    half_life = -np.log(2) / rho if rho < 0 else float("inf")
    return {
        "is_cointegrated": bool(is_coint),
        "beta": float(b),
        "half_life": float(half_life),
        "spread": float(spread[-1]),
        "rho": float(rho),
    }


def half_life_mean_reversion(closes: np.ndarray) -> float:
    """Half-life of mean reversion via Ornstein-Uhlenbeck fit on log-returns."""
    c = np.asarray(closes, dtype=np.float64)
    if len(c) < 30:
        return float("inf")
    log_c = np.log(c)
    d_log = np.diff(log_c)
    log_lag = log_c[:-1]
    if log_lag.std() < 1e-12:
        return float("inf")
    X = np.column_stack([np.ones(len(d_log)), log_lag])
    coef, _, _, _ = np.linalg.lstsq(X, d_log, rcond=None)
    rho = coef[1]
    if rho >= 0:
        return float("inf")
    return float(-np.log(2) / rho)


def population_stability_index(reference: np.ndarray, current: np.ndarray, n_bins: int = 10) -> float:
    """Drift detection via PSI between reference and current distributions.

    PSI < 0.1: no drift; 0.1-0.25: moderate; > 0.25: significant drift.
    """
    ref = np.asarray(reference, dtype=np.float64)
    cur = np.asarray(current, dtype=np.float64)
    if len(ref) < 10 or len(cur) < 10:
        return 0.0
    edges = np.percentile(ref, np.linspace(0, 100, n_bins + 1))
    edges[0] = -np.inf
    edges[-1] = np.inf
    ref_hist, _ = np.histogram(ref, bins=edges)
    cur_hist, _ = np.histogram(cur, bins=edges)
    ref_p = ref_hist / max(ref_hist.sum(), 1)
    cur_p = cur_hist / max(cur_hist.sum(), 1)
    # Avoid log(0)
    ref_p = np.clip(ref_p, 1e-8, 1.0)
    cur_p = np.clip(cur_p, 1e-8, 1.0)
    psi = np.sum((cur_p - ref_p) * np.log(cur_p / ref_p))
    return float(psi)


def kolmogorov_smirnov_two_sample(a: np.ndarray, b: np.ndarray) -> dict:
    """KS test statistic + critical-value comparison (95% CI)."""
    a = np.sort(np.asarray(a, dtype=np.float64))
    b = np.sort(np.asarray(b, dtype=np.float64))
    n1, n2 = len(a), len(b)
    if n1 < 5 or n2 < 5:
        return {"ks_stat": 0.0, "different_dist": False}
    all_v = np.concatenate([a, b])
    all_sorted = np.sort(all_v)
    cdf_a = np.searchsorted(a, all_sorted, side="right") / n1
    cdf_b = np.searchsorted(b, all_sorted, side="right") / n2
    ks = float(np.max(np.abs(cdf_a - cdf_b)))
    # 95% critical
    crit_95 = 1.36 * np.sqrt((n1 + n2) / (n1 * n2))
    return {"ks_stat": ks, "critical_95": float(crit_95), "different_dist": bool(ks > crit_95)}


def pca_factor_loadings(returns_matrix: np.ndarray, n_components: int = 3) -> dict:
    """PCA on multi-pair returns. Returns explained variance ratios + first-component weights.

    returns_matrix: shape (n_obs, n_pairs)
    """
    M = np.asarray(returns_matrix, dtype=np.float64)
    if M.shape[0] < 10 or M.shape[1] < 2:
        return {"explained_variance_ratio": [], "pc1_weights": []}
    M_centered = M - M.mean(axis=0)
    cov = np.cov(M_centered.T)
    eigenvalues, eigenvectors = np.linalg.eigh(cov)
    # Sort descending
    idx = np.argsort(eigenvalues)[::-1]
    eigenvalues = eigenvalues[idx]
    eigenvectors = eigenvectors[:, idx]
    total = eigenvalues.sum()
    if total <= 0:
        return {"explained_variance_ratio": [], "pc1_weights": []}
    n = min(n_components, len(eigenvalues))
    return {
        "explained_variance_ratio": [float(e / total) for e in eigenvalues[:n]],
        "pc1_weights": [float(w) for w in eigenvectors[:, 0]],
    }
