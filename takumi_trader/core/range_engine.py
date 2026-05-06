"""Range Accumulation Detector & Breakout Alert Engine.

Detects pairs compressing into tight ranges on M1, cross-references with
currency strength to predict breakout direction, and fires tiered alerts.

Tiers:
  RANGE     — range detected (visual only, blue)
  LOADED    — range + quality ≥ 60 + directional strength building (purple)
  BREAKOUT  — price broke range boundary in predicted direction (cyan)
  PRIME     — strength FULL ALERT + range LOADED aligned (gold, highest conviction)

Tuned for Asian session + first 2 hours of London session.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

import numpy as np

from takumi_trader.core.strength import ALL_28_PAIRS, CURRENCIES, TIMEFRAME_LABELS

logger = logging.getLogger(__name__)

# Composite strength weights per TF
_COMPOSITE_W = {"M1": 0.40, "M5": 0.25, "M15": 0.20, "H1": 0.15}


@dataclass
class RangeState:
    """State of range detection for a single pair."""

    pair: str
    tier: str = ""  # RANGE / LOADED / BREAKOUT / PRIME / STALE / ""
    range_pct: float = 0.0
    quality_score: float = 0.0
    window_high: float = 0.0
    window_low: float = 0.0
    predicted_direction: str = ""  # BUY / SELL / ""
    strength_delta: float = 0.0
    strength_building: bool = False
    building_rate: float = 0.0
    adr_consumed_pct: float = 0.0
    adr_pips: float = 0.0
    timestamp: float = 0.0


class RangeEngine:
    """Detects range accumulation and predicts breakout direction."""

    def __init__(self) -> None:
        # ── Config (tuned for Asian + early London) ──
        self.window_size: int = 10  # M1 candles
        self.range_threshold_pct: float = 5.0  # max range as % of ADR
        self.min_quality: int = 60  # for LOADED tier
        self.strength_delta_min: float = 3.0  # for direction prediction
        self.breakout_cooldown_s: int = 300  # 5 minutes
        self.max_adr_consumed: float = 85.0  # suppress if daily range spent
        self.enable_bb: bool = True
        self.enable_volume: bool = True

        # ── State ──
        self._adr: dict[str, float] = {}  # pair → ADR (price units)
        self._today_high: dict[str, float] = {}
        self._today_low: dict[str, float] = {}
        self._active: dict[str, RangeState] = {}
        self._range_start_delta: dict[str, float] = {}
        self._cooldown_until: dict[str, float] = {}
        self._bb_width_history: dict[str, deque] = {
            p: deque(maxlen=150) for p in ALL_28_PAIRS
        }

    # ── ADR ──────────────────────────────────────────────────────────

    def update_adr(self, pair: str, daily_candles: Any) -> None:
        """Compute and cache ADR from D1 candle data.

        daily_candles should include ~15 bars; the last one is today's
        forming candle, previous ones are completed days.
        """
        if daily_candles is None or len(daily_candles) < 6:
            return

        # Today's forming candle (last in array)
        today = daily_candles[-1]
        self._today_high[pair] = float(today["high"])
        self._today_low[pair] = float(today["low"])

        # ADR from completed days (exclude today)
        completed = [
            c for c in daily_candles[:-1] if float(c["tick_volume"]) > 0
        ]
        if len(completed) < 5:
            return
        recent = completed[-10:]
        ranges = [float(c["high"]) - float(c["low"]) for c in recent]
        self._adr[pair] = float(np.mean(ranges))

    def get_adr_pips(self, pair: str) -> float:
        adr = self._adr.get(pair, 0.0)
        if adr <= 0:
            return 0.0
        return adr / (0.01 if "JPY" in pair else 0.0001)

    # ── Main Detection ───────────────────────────────────────────────

    def detect_all(
        self,
        m1_candle_data: dict[str, Any],
        ccy_scores_per_tf: dict[str, dict[str, float]],
        tick_prices: dict[str, float] | None = None,
    ) -> list[RangeState]:
        """Run range detection for all pairs on M1 candle close.

        Args:
            m1_candle_data: pair → numpy structured array of M1 candles.
            ccy_scores_per_tf: tf → {ccy: score} from strength engine.
            tick_prices: pair → current bid (for breakout detection).

        Returns:
            List of RangeState for pairs with detected ranges.
        """
        results: list[RangeState] = []
        now = time.time()

        for pair in ALL_28_PAIRS:
            candles = m1_candle_data.get(pair)
            if candles is None or len(candles) < max(self.window_size + 10, 60):
                continue
            adr = self._adr.get(pair, 0.0)
            if adr <= 1e-10:
                continue
            if now < self._cooldown_until.get(pair, 0):
                continue

            state = self._detect_pair(
                pair, candles, adr, ccy_scores_per_tf, tick_prices, now
            )
            if state:
                results.append(state)

        return results

    # ── Per-pair detection ───────────────────────────────────────────

    def _detect_pair(
        self,
        pair: str,
        candles: Any,
        adr: float,
        ccy_tf: dict[str, dict[str, float]],
        tick_prices: dict[str, float] | None,
        now: float,
    ) -> RangeState | None:
        closes = candles["close"].astype(np.float64)
        highs = candles["high"].astype(np.float64)
        lows = candles["low"].astype(np.float64)
        volumes = candles["tick_volume"].astype(np.float64)
        win = self.window_size

        # ── Range ──
        w_high = float(np.max(highs[-win:]))
        w_low = float(np.min(lows[-win:]))
        w_range = w_high - w_low
        range_pct = (w_range / adr) * 100.0

        # ── ADR consumed ──
        t_high = self._today_high.get(pair, w_high)
        t_low = self._today_low.get(pair, w_low)
        adr_consumed = ((t_high - t_low) / adr) * 100.0
        if adr_consumed > self.max_adr_consumed:
            return None

        # Not in range → clear tracking
        if range_pct > self.range_threshold_pct:
            self._active.pop(pair, None)
            self._range_start_delta.pop(pair, None)
            return None

        # ── BB Squeeze ──
        bb_rank = 50.0
        if self.enable_bb and len(closes) >= 40:
            bb_rank = self._bb_squeeze(closes, pair)

        # ── Volume Compression ──
        vol_ratio = 1.0
        if self.enable_volume and len(volumes) >= 60:
            recent_vol = float(np.mean(volumes[-win:]))
            baseline_vol = float(np.mean(volumes[-60:-win]))
            vol_ratio = recent_vol / baseline_vol if baseline_vol > 0 else 1.0

        # ── Quality ──
        quality = self._quality_score(range_pct, bb_rank, vol_ratio)

        # ── Strength delta & direction ──
        base, quote = pair[:3], pair[3:]
        comp_base = self._composite(base, ccy_tf)
        comp_quote = self._composite(quote, ccy_tf)
        delta = comp_base - comp_quote

        if pair not in self._range_start_delta:
            self._range_start_delta[pair] = delta
        start_delta = self._range_start_delta[pair]
        building_rate = delta - start_delta
        building = abs(delta) > abs(start_delta) and abs(delta) > self.strength_delta_min

        if delta > self.strength_delta_min:
            direction = "BUY"
        elif delta < -self.strength_delta_min:
            direction = "SELL"
        else:
            direction = ""

        # ── Breakout check ──
        price = (
            tick_prices.get(pair) if tick_prices else None
        ) or float(closes[-1])
        pip = 0.01 if "JPY" in pair else 0.0001
        breakout_dir = ""
        if price > w_high + pip:
            breakout_dir = "BUY"
        elif price < w_low - pip:
            breakout_dir = "SELL"

        # ── Tier ──
        tier = "RANGE"
        if quality >= self.min_quality and direction and building:
            tier = "LOADED"

        if breakout_dir:
            if breakout_dir == direction:
                tier = "BREAKOUT"
                self._cooldown_until[pair] = now + self.breakout_cooldown_s
            else:
                # Wrong direction — invalidate
                self._active.pop(pair, None)
                self._range_start_delta.pop(pair, None)
                return None

        # Stale check (range persisted > 2× window without breakout)
        if pair in self._active and tier == "RANGE":
            prev = self._active[pair]
            if now - prev.timestamp > self.window_size * 2 * 60:
                tier = "STALE"

        ts = self._active[pair].timestamp if pair in self._active else now
        state = RangeState(
            pair=pair,
            tier=tier,
            range_pct=round(range_pct, 1),
            quality_score=round(quality, 0),
            window_high=w_high,
            window_low=w_low,
            predicted_direction=direction,
            strength_delta=round(delta, 1),
            strength_building=building,
            building_rate=round(building_rate, 1),
            adr_consumed_pct=round(adr_consumed, 0),
            adr_pips=round(self.get_adr_pips(pair), 0),
            timestamp=ts,
        )
        self._active[pair] = state
        return state

    # ── Helpers ───────────────────────────────────────────────────────

    def _bb_squeeze(self, closes: np.ndarray, pair: str) -> float:
        """Compute BB width and return percentile rank over recent history."""
        period = 20
        sma = float(np.mean(closes[-period:]))
        std = float(np.std(closes[-period:]))
        if sma <= 0:
            return 50.0
        width = (4.0 * std) / sma * 100.0
        self._bb_width_history[pair].append(width)
        buf = self._bb_width_history[pair]
        if len(buf) < 20:
            return 50.0
        arr = np.array(buf)
        return float(np.sum(arr <= width)) / len(arr) * 100.0

    def _quality_score(
        self, range_pct: float, bb_rank: float, vol_ratio: float
    ) -> float:
        """Combine metrics into 0-100 quality score."""
        r = max(0.0, (1.0 - range_pct / self.range_threshold_pct)) * 40.0
        b = max(0.0, (1.0 - bb_rank / 30.0)) * 35.0 if self.enable_bb else 17.5
        v = max(0.0, (1.0 - vol_ratio / 0.8)) * 25.0 if self.enable_volume else 12.5
        return min(100.0, r + b + v)

    def _composite(
        self, ccy: str, scores_per_tf: dict[str, dict[str, float]]
    ) -> float:
        """Weighted composite strength for a currency."""
        total = 0.0
        total_w = 0.0
        for tf, w in _COMPOSITE_W.items():
            s = scores_per_tf.get(tf, {}).get(ccy)
            if s is not None:
                total += w * s
                total_w += w
        return total / total_w if total_w > 0 else 0.0
