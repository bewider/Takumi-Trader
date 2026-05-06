"""Cross-currency strength derived metrics.

Operates on TAKUMI's existing composite_scores dict (8 currencies, 0-10 scale).
"""
from __future__ import annotations

import numpy as np

CURRENCIES = ("USD", "EUR", "GBP", "JPY", "CAD", "AUD", "NZD", "CHF")


def csi_dispersion(scores: dict) -> float:
    """max(scores) - min(scores). Wide = strong macro forces; narrow = chop."""
    vals = [scores.get(c, 5.0) for c in CURRENCIES if c in scores]
    if len(vals) < 2:
        return 0.0
    return float(max(vals) - min(vals))


def csi_breadth(scores: dict, strong_thresh: float = 7.0, weak_thresh: float = 3.0) -> dict:
    """Number of strong (>=7) and weak (<=3) currencies."""
    strong = sum(1 for c in CURRENCIES if scores.get(c, 5.0) >= strong_thresh)
    weak = sum(1 for c in CURRENCIES if scores.get(c, 5.0) <= weak_thresh)
    return {
        "csi_strong_count": strong,
        "csi_weak_count": weak,
        "csi_polarized": strong + weak,
    }


def csi_rate_of_change(scores_now: dict, scores_prev: dict) -> dict:
    """Rate of change per currency. Returns Δ (now - prev) per currency."""
    out = {}
    for c in CURRENCIES:
        out[f"d{c}"] = float(scores_now.get(c, 5.0) - scores_prev.get(c, 5.0))
    return out


def csi_clustering(scores: dict, threshold: float = 0.5) -> list:
    """Cluster currencies by closeness in score. Returns list of clusters."""
    sorted_ccy = sorted(scores.items(), key=lambda x: x[1])
    clusters: list[list] = []
    for ccy, score in sorted_ccy:
        if not clusters or abs(clusters[-1][-1][1] - score) > threshold:
            clusters.append([(ccy, score)])
        else:
            clusters[-1].append((ccy, score))
    return clusters


def csi_correlation_matrix(scores_history: list[dict]) -> dict:
    """Rolling correlation between currency strength time series."""
    if not scores_history:
        return {}
    series = {c: [] for c in CURRENCIES}
    for snapshot in scores_history:
        for c in CURRENCIES:
            series[c].append(snapshot.get(c, 5.0))
    arrays = {c: np.array(v) for c, v in series.items()}
    out = {}
    for i, c1 in enumerate(CURRENCIES):
        for c2 in CURRENCIES[i + 1:]:
            a, b = arrays[c1], arrays[c2]
            if len(a) > 5 and a.std() > 1e-12 and b.std() > 1e-12:
                out[f"{c1}_{c2}_corr"] = float(np.corrcoef(a, b)[0, 1])
    return out


def beta_vs_index(pair_returns: np.ndarray, index_returns: np.ndarray) -> dict:
    """Rolling beta of pair vs reference index (DXY, gold, VIX, S&P)."""
    p = np.asarray(pair_returns, dtype=np.float64)
    i = np.asarray(index_returns, dtype=np.float64)
    n = min(len(p), len(i))
    if n < 10:
        return {"beta": 0.0, "r_squared": 0.0}
    p = p[-n:]
    i = i[-n:]
    if i.std() < 1e-12:
        return {"beta": 0.0, "r_squared": 0.0}
    cov = np.cov(p, i, ddof=1)[0, 1]
    var = np.var(i, ddof=1)
    beta = cov / var
    pred = beta * (i - i.mean()) + p.mean()
    ss_res = np.sum((p - pred) ** 2)
    ss_tot = np.sum((p - p.mean()) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 1e-12 else 0.0
    return {"beta": float(beta), "r_squared": float(r2)}
