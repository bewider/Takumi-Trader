"""TAKUMI features library — comprehensive feature engineering for FX trading.

Free-tier only — no monthly subscriptions. All modules use existing TAKUMI
data (MT5 bars/ticks, composite scores) plus optional FREE external sources
(FRED, Yahoo Finance, ForexFactory RSS, CFTC, Reddit, Wikipedia).

Modules:
    microstructure   — CVD, Lee-Ready, VPIN, Kyle's Lambda, Amihud, iceberg
    volatility       — Parkinson, GK, YZ, jump detection, ATR percentile, GARCH
    levels           — round numbers, OHLC, pivots (5 systems), VWAP, volume profile
    regimes          — ADX, Choppiness, Hurst, DFA, Aroon, Vortex, KAMA, Ichimoku
    statistics       — autocorr, FFT, Granger, cointegration, half-life, drift
    csi_metrics      — currency strength dispersion, breadth, RoC, beta
    portfolio        — delta, exposure, VaR, Sharpe, Sortino, Calmar, Ulcer
    fx_specific      — carry, fix windows, month-end, triangular arb, DST
    patterns         — FVG, order blocks, liquidity pools, equal highs/lows
    adversarial      — stop-hunt, liquidity sweep, round-number magnetism
    behavioral       — pre/post-news, day-after-FOMC, lunch hour, Friday late
    cross_market     — DXY synthetic from existing pairs, currency baskets
    yields           — FRED API for Treasury yields, real yields, credit spreads
    market_data      — Yahoo Finance for VIX, gold, oil, equity indices
    calendar         — ForexFactory weekly events, news blackout detection
    positioning      — CFTC COT data, speculator z-scores
    sentiment        — RSS scraping, keyword sentiment, Reddit, Wikipedia views
    feature_engine   — master aggregator producing 100+ feature columns
"""
from .feature_engine import FeatureEngine

# Re-export commonly used module-level helpers
from . import (
    microstructure,
    volatility,
    levels,
    regimes,
    statistics,
    csi_metrics,
    portfolio,
    fx_specific,
    patterns,
    adversarial,
    behavioral,
    cross_market,
    yields,
    market_data,
    calendar,
    positioning,
    sentiment,
)

__all__ = [
    "FeatureEngine",
    "microstructure", "volatility", "levels", "regimes", "statistics",
    "csi_metrics", "portfolio", "fx_specific", "patterns", "adversarial",
    "behavioral", "cross_market", "yields", "market_data", "calendar",
    "positioning", "sentiment",
]
