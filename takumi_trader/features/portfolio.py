"""Portfolio-level risk metrics.

Operates on lists of active TrackedTrade-like dicts.
"""
from __future__ import annotations

import numpy as np
from collections import defaultdict


def portfolio_delta_pips(trades: list) -> float:
    """Sum of current pnl_pips across all open trades — net portfolio delta."""
    return float(sum(t.get("pnl_pips", 0) for t in trades))


def currency_exposure(trades: list) -> dict:
    """Net long/short exposure per currency.

    For BUY EURUSD: +1 EUR, -1 USD.
    For SELL EURUSD: -1 EUR, +1 USD.
    """
    expo: dict[str, float] = defaultdict(float)
    for t in trades:
        pair = t["pair"]
        direction = t.get("direction", "BUY")
        size = t.get("size", 1.0)
        sign = +1 if direction == "BUY" else -1
        if len(pair) == 6:
            base = pair[:3]
            quote = pair[3:]
            expo[base] += sign * size
            expo[quote] -= sign * size
    return dict(expo)


def concentration_risk(trades: list) -> dict:
    """% of total exposure in single currency."""
    expo = currency_exposure(trades)
    total = sum(abs(v) for v in expo.values())
    if total == 0:
        return {"max_currency_share": 0.0, "max_currency": ""}
    abs_exp = {c: abs(v) / total for c, v in expo.items()}
    if not abs_exp:
        return {"max_currency_share": 0.0, "max_currency": ""}
    max_c = max(abs_exp, key=abs_exp.get)
    return {"max_currency_share": float(abs_exp[max_c]), "max_currency": max_c}


def correlation_matrix_active(trade_pairs: list[str], pair_returns: dict) -> dict:
    """Correlation matrix between currently-traded pairs."""
    if len(trade_pairs) < 2:
        return {}
    out = {}
    for i, p1 in enumerate(trade_pairs):
        for p2 in trade_pairs[i + 1:]:
            r1 = pair_returns.get(p1)
            r2 = pair_returns.get(p2)
            if r1 is None or r2 is None or len(r1) < 10 or len(r2) < 10:
                continue
            n = min(len(r1), len(r2))
            a = np.array(r1[-n:])
            b = np.array(r2[-n:])
            if a.std() > 1e-12 and b.std() > 1e-12:
                out[f"{p1}_{p2}_corr"] = float(np.corrcoef(a, b)[0, 1])
    return out


def value_at_risk(equity_curve: list[float], confidence: float = 0.95) -> float:
    """Historical-simulation VaR at given confidence (default 95%)."""
    if len(equity_curve) < 30:
        return 0.0
    eq = np.asarray(equity_curve, dtype=np.float64)
    rets = np.diff(eq) / eq[:-1]
    return float(-np.percentile(rets, (1 - confidence) * 100))


def expected_shortfall(equity_curve: list[float], confidence: float = 0.95) -> float:
    """CVaR — expected return given VaR breach."""
    if len(equity_curve) < 30:
        return 0.0
    eq = np.asarray(equity_curve, dtype=np.float64)
    rets = np.diff(eq) / eq[:-1]
    var_threshold = np.percentile(rets, (1 - confidence) * 100)
    tail = rets[rets <= var_threshold]
    return float(-tail.mean()) if len(tail) > 0 else 0.0


def sharpe_ratio(returns: np.ndarray, periods_per_year: int = 252 * 24) -> float:
    """Annualized Sharpe (assumes returns are per H1 bar by default)."""
    r = np.asarray(returns, dtype=np.float64)
    if len(r) < 10 or r.std() < 1e-12:
        return 0.0
    return float(np.sqrt(periods_per_year) * r.mean() / r.std(ddof=1))


def sortino_ratio(returns: np.ndarray, periods_per_year: int = 252 * 24) -> float:
    """Sortino — like Sharpe but only penalizes downside."""
    r = np.asarray(returns, dtype=np.float64)
    if len(r) < 10:
        return 0.0
    downside = r[r < 0]
    if len(downside) == 0 or downside.std() < 1e-12:
        return float("inf") if r.mean() > 0 else 0.0
    return float(np.sqrt(periods_per_year) * r.mean() / downside.std(ddof=1))


def calmar_ratio(equity_curve: list[float]) -> float:
    """Annualized return / max drawdown."""
    eq = np.asarray(equity_curve, dtype=np.float64)
    if len(eq) < 30:
        return 0.0
    running_max = np.maximum.accumulate(eq)
    dd = (eq - running_max) / running_max
    max_dd = abs(float(dd.min())) if len(dd) > 0 else 0.0
    if max_dd < 1e-12:
        return 0.0
    total_ret = (eq[-1] - eq[0]) / eq[0]
    n_periods = len(eq)
    annualized = (1 + total_ret) ** (252 * 24 / n_periods) - 1
    return float(annualized / max_dd)


def ulcer_index(equity_curve: list[float], window: int = 100) -> float:
    """Ulcer Index — measure of DD intensity over time."""
    eq = np.asarray(equity_curve, dtype=np.float64)
    if len(eq) < window:
        return 0.0
    sub = eq[-window:]
    running_max = np.maximum.accumulate(sub)
    pct_dd = ((sub - running_max) / running_max) * 100
    return float(np.sqrt(np.mean(pct_dd ** 2)))


def drawdown_duration(equity_curve: list[float]) -> dict:
    """Current drawdown depth and duration."""
    eq = np.asarray(equity_curve, dtype=np.float64)
    if len(eq) < 2:
        return {"current_dd_pct": 0.0, "dd_duration_bars": 0, "max_dd_pct": 0.0}
    running_max = np.maximum.accumulate(eq)
    dd_pct = (eq - running_max) / running_max * 100
    cur_dd = float(dd_pct[-1])
    # Duration: how many bars since last peak
    last_peak_idx = int(np.argmax(running_max == running_max[-1]))
    duration = len(eq) - last_peak_idx - 1
    return {
        "current_dd_pct": cur_dd,
        "dd_duration_bars": int(duration),
        "max_dd_pct": float(dd_pct.min()),
    }


def win_loss_streak(pnls: list[float]) -> dict:
    """Current win/loss streak length."""
    if not pnls:
        return {"current_streak": 0, "streak_type": ""}
    streak = 1
    last_sign = 1 if pnls[-1] > 0 else -1 if pnls[-1] < 0 else 0
    for i in range(len(pnls) - 2, -1, -1):
        cur_sign = 1 if pnls[i] > 0 else -1 if pnls[i] < 0 else 0
        if cur_sign == last_sign and cur_sign != 0:
            streak += 1
        else:
            break
    return {"current_streak": streak, "streak_type": "win" if last_sign > 0 else "loss" if last_sign < 0 else "flat"}


def profit_factor_rolling(pnls: list[float], window: int = 50) -> float:
    """Rolling profit factor over last `window` trades."""
    if len(pnls) < window:
        return 0.0
    sub = pnls[-window:]
    wins = sum(p for p in sub if p > 0)
    losses = abs(sum(p for p in sub if p <= 0))
    return float(wins / losses) if losses > 0 else float("inf")
