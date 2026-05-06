"""Higher Timeframe Trend Regime — H4/D1 computation and caching (Phase 8.2).

Computes currency strength on H4 and D1 timeframes using the same
EMA displacement + weighted ROC methodology as the main engine.
Classifies each currency as BULLISH / BEARISH / NEUTRAL on each TF.

Regime data is cached and refreshed:
  - H4: on every H4 candle close (~6 times per day)
  - D1: on every D1 candle close (~once per day)
  - On startup: from warmup data (50 H4, 20 D1 candles)
"""

from __future__ import annotations

import logging
import math
from collections import deque
from typing import Any

import numpy as np

from takumi_trader.core.strength import (
    ALL_28_PAIRS,
    CURRENCIES,
    _CCY_PAIR_MAP,
    compute_atr,
    compute_ema,
)

logger = logging.getLogger(__name__)

# Default threshold for regime classification (± this value)
DEFAULT_REGIME_THRESHOLD = 3.0

# ROC lookback bars per HTF
_HTF_ROC_LOOKBACK = {"H4": 5, "D1": 3}

# Z-score window for HTF (smaller — these TFs update slowly)
_HTF_ZSCORE_WINDOW = 60

# Weights: (displacement, weighted_roc, tick_velocity)
_HTF_WEIGHTS = {"H4": (0.50, 0.50, 0.00), "D1": (0.50, 0.50, 0.00)}


class HTFRegimeTracker:
    """Computes and caches H4/D1 trend regime for all 8 currencies."""

    def __init__(self, threshold: float = DEFAULT_REGIME_THRESHOLD) -> None:
        self.threshold = threshold

        # Cached regime: ccy -> {tf -> (regime_str, strength_value)}
        self._regimes: dict[str, dict[str, tuple[str, float]]] = {
            ccy: {"H4": ("NEUTRAL", 0.0), "D1": ("NEUTRAL", 0.0)}
            for ccy in CURRENCIES
        }

        # Z-score buffers for HTFs
        self._zscore_buffers: dict[str, dict[str, deque]] = {
            ccy: {tf: deque(maxlen=_HTF_ZSCORE_WINDOW) for tf in ("H4", "D1")}
            for ccy in CURRENCIES
        }

        # Velocity tracking: composite strength history per ccy (last 10 M1 readings)
        self._strength_history: dict[str, deque] = {
            ccy: deque(maxlen=15) for ccy in CURRENCIES
        }

    # ── Bootstrap ─────────────────────────────────────────────────

    def bootstrap(self, warmup_data: dict[str, dict[str, Any]]) -> None:
        """Compute regime from warmup data.

        Args:
            warmup_data: warmup_data[pair][tf_label] = numpy structured array.
                         Must include 'H4' and 'D1' keys.
        """
        for tf in ("H4", "D1"):
            pair_series: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
            min_len = 999_999

            for pair in ALL_28_PAIRS:
                candles = warmup_data.get(pair, {}).get(tf)
                if candles is None or len(candles) < 15:
                    continue
                closes = candles["close"].astype(np.float64)
                highs = candles["high"].astype(np.float64)
                lows = candles["low"].astype(np.float64)
                ema8 = compute_ema(closes, 8)
                atr14 = compute_atr(highs, lows, closes, 14)
                pair_series[pair] = (closes, ema8, atr14)
                min_len = min(min_len, len(closes))

            if not pair_series or min_len < 15:
                continue

            start_idx = max(15, min_len - _HTF_ZSCORE_WINDOW)
            for idx in range(start_idx, min_len):
                pair_scores: dict[str, float] = {}
                for pair, (closes, ema8, atr14) in pair_series.items():
                    if idx >= len(closes):
                        continue
                    atr_val = float(atr14[idx])
                    if atr_val <= 1e-10:
                        continue
                    displacement = (float(closes[idx]) - float(ema8[idx])) / atr_val
                    n = _HTF_ROC_LOOKBACK.get(tf, 5)
                    total_w = 0.0
                    weighted_sum = 0.0
                    for i in range(min(n, idx)):
                        w = math.exp(-0.3 * i)
                        roc = (float(closes[idx]) - float(closes[idx - i - 1])) / atr_val
                        weighted_sum += w * roc
                        total_w += w
                    weighted_roc = weighted_sum / total_w if total_w > 0 else 0.0
                    w_d, w_r, _ = _HTF_WEIGHTS.get(tf, (0.50, 0.50, 0.00))
                    eff = w_d + w_r
                    score = (w_d / eff) * displacement + (w_r / eff) * weighted_roc
                    pair_scores[pair] = score

                if pair_scores:
                    raw = self._aggregate_raw(pair_scores)
                    for ccy in CURRENCIES:
                        self._zscore_buffers[ccy][tf].append(raw.get(ccy, 0.0))

            # Compute final regime from last value
            self._update_regimes(tf)

        logger.info(
            "HTF regime bootstrap: H4 bufs=%d, D1 bufs=%d",
            len(self._zscore_buffers["USD"]["H4"]),
            len(self._zscore_buffers["USD"]["D1"]),
        )

    # ── Live Update ───────────────────────────────────────────────

    def update(
        self, candle_data: dict[str, Any], tf: str
    ) -> dict[str, tuple[str, float]]:
        """Recompute regime for a timeframe from live candle data.

        Args:
            candle_data: {pair: numpy array} for this TF.
            tf: "H4" or "D1".

        Returns:
            Dict of ccy -> (regime, strength).
        """
        pair_scores: dict[str, float] = {}

        for pair in ALL_28_PAIRS:
            candles = candle_data.get(pair)
            if candles is None or len(candles) < 15:
                continue
            closes = candles["close"].astype(np.float64)
            highs = candles["high"].astype(np.float64)
            lows = candles["low"].astype(np.float64)
            ema8 = compute_ema(closes, 8)
            atr14 = compute_atr(highs, lows, closes, 14)
            atr_val = float(atr14[-1])
            if atr_val <= 1e-10:
                continue
            displacement = (float(closes[-1]) - float(ema8[-1])) / atr_val
            n = _HTF_ROC_LOOKBACK.get(tf, 5)
            total_w = 0.0
            weighted_sum = 0.0
            for i in range(min(n, len(closes) - 1)):
                w = math.exp(-0.3 * i)
                roc = (float(closes[-1]) - float(closes[-(i + 2)])) / atr_val
                weighted_sum += w * roc
                total_w += w
            weighted_roc = weighted_sum / total_w if total_w > 0 else 0.0
            w_d, w_r, _ = _HTF_WEIGHTS.get(tf, (0.50, 0.50, 0.00))
            eff = w_d + w_r
            score = (w_d / eff) * displacement + (w_r / eff) * weighted_roc
            pair_scores[pair] = score

        if pair_scores:
            raw = self._aggregate_raw(pair_scores)
            for ccy in CURRENCIES:
                self._zscore_buffers[ccy][tf].append(raw.get(ccy, 0.0))

        self._update_regimes(tf)
        return {ccy: self._regimes[ccy][tf] for ccy in CURRENCIES}

    # ── Velocity ──────────────────────────────────────────────────

    def update_velocity(self, composite_scores: dict[str, float]) -> None:
        """Record composite strength for velocity calculation.

        Call once per M1 close with the latest composite scores.
        """
        for ccy in CURRENCIES:
            if ccy in composite_scores:
                self._strength_history[ccy].append(composite_scores[ccy])

    def get_velocity(self, ccy: str, lookback: int = 5) -> tuple[float, bool]:
        """Get strength velocity for a currency.

        Returns:
            (velocity_per_minute, is_fast)
        """
        hist = self._strength_history.get(ccy)
        if not hist or len(hist) < lookback + 1:
            return 0.0, False
        current = hist[-1]
        past = hist[-(lookback + 1)]
        vel = (current - past) / lookback
        return vel, abs(vel) >= 0.6  # default velocity threshold

    # ── Query ─────────────────────────────────────────────────────

    def get_regime(self, ccy: str, tf: str) -> tuple[str, float]:
        """Get cached regime for a currency on a timeframe."""
        return self._regimes.get(ccy, {}).get(tf, ("NEUTRAL", 0.0))

    def get_all_regimes(self) -> dict[str, dict[str, tuple[str, float]]]:
        """Get all cached regimes."""
        return dict(self._regimes)

    # ── Internals ─────────────────────────────────────────────────

    def _update_regimes(self, tf: str) -> None:
        """Recompute regime classification from Z-score buffers."""
        for ccy in CURRENCIES:
            buf = self._zscore_buffers[ccy][tf]
            if len(buf) < 5:
                continue
            arr = np.array(buf)
            mean = float(np.mean(arr))
            std = float(np.std(arr))
            raw = float(buf[-1])

            if std < 1e-10:
                strength = 0.0
            else:
                z = (raw - mean) / std
                strength = 10.0 * math.tanh(z * 1.0)

            if strength > self.threshold:
                regime = "BULLISH"
            elif strength < -self.threshold:
                regime = "BEARISH"
            else:
                regime = "NEUTRAL"

            self._regimes[ccy][tf] = (regime, round(strength, 1))

    def _aggregate_raw(self, pair_scores: dict[str, float]) -> dict[str, float]:
        """Average signed pair scores per currency."""
        raw: dict[str, float] = {}
        for ccy, pairs_info in _CCY_PAIR_MAP.items():
            contributions: list[float] = []
            for pair, is_base in pairs_info:
                score = pair_scores.get(pair)
                if score is None:
                    continue
                contributions.append(score if is_base else -score)
            raw[ccy] = sum(contributions) / len(contributions) if contributions else 0.0
        return raw
