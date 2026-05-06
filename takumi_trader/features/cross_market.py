"""Cross-market indicators — DXY, equity correlations, currency baskets.

Computes synthetic indices from existing pair data. No external fetch.
"""
from __future__ import annotations

import numpy as np


def dxy_from_pairs(eurusd: float, usdjpy: float, gbpusd: float,
                    usdcad: float, usdsek: float, usdchf: float) -> float:
    """Compute DXY using ICE formula:
    DXY = 50.14348112 × EUR^-0.576 × JPY^0.136 × GBP^-0.119 × CAD^0.091 × SEK^0.042 × CHF^0.036
    """
    if any(p <= 0 for p in (eurusd, usdjpy, gbpusd, usdcad, usdsek, usdchf)):
        return 0.0
    return float(
        50.14348112
        * (eurusd ** -0.576)
        * (usdjpy ** 0.136)
        * (gbpusd ** -0.119)
        * (usdcad ** 0.091)
        * (usdsek ** 0.042)
        * (usdchf ** 0.036)
    )


def dxy_approximate(eurusd: float, usdjpy: float, gbpusd: float, usdcad: float, usdchf: float) -> float:
    """Simplified DXY using only majors TAKUMI tracks (excludes SEK).
    Re-normalizes the 4 weights present so they sum to ~1.
    """
    if any(p <= 0 for p in (eurusd, usdjpy, gbpusd, usdcad, usdchf)):
        return 0.0
    # Original weights without SEK: 0.576+0.136+0.119+0.091+0.036 = 0.958
    # Re-normalize to 1.0 by dividing each by 0.958
    return float(
        50.14348112
        * (eurusd ** (-0.576 / 0.958))
        * (usdjpy ** (0.136 / 0.958))
        * (gbpusd ** (-0.119 / 0.958))
        * (usdcad ** (0.091 / 0.958))
        * (usdchf ** (0.036 / 0.958))
    )


def eur_index(eurusd: float, eurjpy: float, eurgbp: float, euraud: float, eurcad: float, eurchf: float) -> float:
    """Synthetic EUR Index — geometric mean of EUR vs majors."""
    pairs = [eurusd, eurjpy, eurgbp, euraud, eurcad, eurchf]
    if any(p <= 0 for p in pairs):
        return 0.0
    # Normalize to be ~100 on average
    return float(100.0 * np.prod([p / p for p in pairs]) ** (1 / 6) * np.exp(np.mean(np.log(pairs)) / 10))


def jpy_index(usdjpy: float, eurjpy: float, gbpjpy: float, audjpy: float, cadjpy: float, nzdjpy: float, chfjpy: float) -> float:
    """Synthetic JPY Index — inverse since JPY is quoted (USDJPY etc.)."""
    pairs = [usdjpy, eurjpy, gbpjpy, audjpy, cadjpy, nzdjpy, chfjpy]
    if any(p <= 0 for p in pairs):
        return 0.0
    return float(100.0 / (np.exp(np.mean(np.log(pairs))) / 100))


def gbp_index(gbpusd: float, eurgbp: float, gbpjpy: float, gbpaud: float, gbpcad: float, gbpchf: float, gbpnzd: float) -> float:
    """Synthetic GBP Index."""
    # EURGBP is inverted (EUR is base); use 1/EURGBP for GBP-base
    pairs = [gbpusd, 1.0 / eurgbp if eurgbp > 0 else 0, gbpjpy, gbpaud, gbpcad, gbpchf, gbpnzd]
    if any(p <= 0 for p in pairs):
        return 0.0
    return float(100.0 * np.exp(np.mean(np.log(pairs)) / 10))


def aud_index(audusd: float, audjpy: float, euraud: float, gbpaud: float,
                audcad: float, audnzd: float, audchf: float) -> float:
    """Synthetic AUD Index (inverting EURAUD/GBPAUD which have AUD as quote)."""
    pairs = [audusd, audjpy,
             1.0 / euraud if euraud > 0 else 0,
             1.0 / gbpaud if gbpaud > 0 else 0,
             audcad, audnzd, audchf]
    if any(p <= 0 for p in pairs):
        return 0.0
    return float(100.0 * np.exp(np.mean(np.log(pairs)) / 10))


def carry_basket_value(rates: dict, weights: dict = None) -> float:
    """Long-high-yielders / short-low-yielders carry basket cumulative carry."""
    if weights is None:
        # Default G10 carry basket weights
        weights = {"AUD": 0.25, "NZD": 0.25, "USD": 0.0,
                    "EUR": -0.10, "GBP": -0.10, "JPY": -0.30, "CHF": -0.10, "CAD": 0.10}
    total = 0.0
    for ccy, w in weights.items():
        total += w * rates.get(ccy, 0)
    return float(total)


def cross_correlation_window(
    pair_returns: np.ndarray, ref_returns: np.ndarray, max_lag: int = 5,
) -> dict:
    """Cross-correlation function — finds lead/lag relationship."""
    p = np.asarray(pair_returns, dtype=np.float64)
    r = np.asarray(ref_returns, dtype=np.float64)
    n = min(len(p), len(r))
    if n < max_lag * 2 + 1:
        return {}
    p = p[-n:]
    r = r[-n:]
    if p.std() < 1e-12 or r.std() < 1e-12:
        return {}
    out = {}
    for lag in range(-max_lag, max_lag + 1):
        if lag < 0:
            x = p[:lag] if lag != 0 else p
            y = r[-lag:] if lag != 0 else r
        elif lag > 0:
            x = p[lag:]
            y = r[:-lag]
        else:
            x = p
            y = r
        if len(x) > 5 and x.std() > 1e-12 and y.std() > 1e-12:
            out[f"xcorr_lag_{lag}"] = float(np.corrcoef(x, y)[0, 1])
    return out
