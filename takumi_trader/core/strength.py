"""Currency strength calculation engine.

Uses ATR-normalized indicators with Z-score + tanh normalization:
  1. EMA-8 Displacement: (close - EMA8) / ATR14 — directional pressure
  2. Weighted Micro-ROC: exponential-decay weighted rate of change / ATR14
  3. Tick Velocity (M1 only): (current_close - candle_open) / ATR14

Per-currency raw strength = average of signed pair scores across 7 pairs.
Final strength = 10 * tanh(z_score * sensitivity), bounded to ±10.

This approach:
  • ATR-normalizes everything so all pairs are comparable.
  • Uses Z-score to contextualize "how extreme vs. recent history."
  • tanh compresses to bounded ±10 range.
  • 200-bar warmup bootstraps the Z-score buffers on startup.
"""

from __future__ import annotations

import logging
import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any

import numpy as np

try:
    from numba import njit
except ImportError:
    # Graceful fallback — app works without numba, just slower
    def njit(*args, **kwargs):  # type: ignore[misc]
        def decorator(func):
            return func
        if len(args) == 1 and callable(args[0]):
            return args[0]
        return decorator

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────

CURRENCIES: list[str] = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "NZD", "CHF"]

ALL_28_PAIRS: list[str] = [
    "EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
    "EURGBP", "EURAUD", "EURNZD", "EURCAD", "EURCHF", "EURJPY",
    "GBPAUD", "GBPNZD", "GBPCAD", "GBPCHF", "GBPJPY",
    "AUDNZD", "AUDCAD", "AUDCHF", "AUDJPY",
    "NZDCAD", "NZDCHF", "NZDJPY",
    "CADCHF", "CADJPY",
    "CHFJPY",
]

DISPLAY_PAIRS: list[str] = [
    "GBPJPY", "AUDJPY", "EURJPY", "NZDJPY", "CADJPY", "USDJPY", "CHFJPY",
    "AUDUSD", "NZDUSD", "EURUSD", "GBPUSD", "USDCAD",
    "GBPAUD", "GBPNZD", "GBPCAD", "GBPCHF",
    "EURAUD", "EURNZD", "EURCAD", "EURCHF", "USDCHF",
    "AUDCAD", "NZDCAD", "EURGBP", "AUDCHF", "NZDCHF", "CADCHF",
]

TIMEFRAME_LABELS: list[str] = ["M1", "M5", "M15", "H1"]

# Lookback bars for weighted Micro-ROC per timeframe
ROC_LOOKBACK: dict[str, int] = {"M1": 10, "M5": 8, "M15": 6, "H1": 5}

# Z-score rolling window size (in candle closes per timeframe)
# Reduced from 120 to 50 for faster reaction to regime changes (A+B+C combo)
ZSCORE_WINDOW: int = 50

# Bars to fetch on live updates per timeframe.
# M1 needs more bars for range engine (BB squeeze history + volume baseline).
LIVE_FETCH_BARS: dict[str, int] = {
    "M1": 150,
    "M5": 50,
    "M15": 50,
    "H1": 50,
}

# Bars to fetch for startup warmup
WARMUP_BARS: int = 200

# Composite score weights: (displacement, weighted_roc, tick_velocity)
_WEIGHTS: dict[str, tuple[float, float, float]] = {
    "M1": (0.35, 0.35, 0.30),
    "M5": (0.50, 0.50, 0.00),
    "M15": (0.50, 0.50, 0.00),
    "H1": (0.50, 0.50, 0.00),
}

# Currency → list of (pair, is_base) for quick lookup
_CCY_PAIR_MAP: dict[str, list[tuple[str, bool]]] = {c: [] for c in CURRENCIES}
for _p in ALL_28_PAIRS:
    _b, _q = _p[:3], _p[3:]
    _CCY_PAIR_MAP[_b].append((_p, True))
    _CCY_PAIR_MAP[_q].append((_p, False))


# ── Data Classes ─────────────────────────────────────────────────────

def pair_currencies(pair: str) -> tuple[str, str]:
    """Split a 6-char pair symbol into (base, quote) currencies."""
    return pair[:3], pair[3:]


@dataclass
class TimeframeResult:
    """Results for a single timeframe."""
    currency_scores: dict[str, float] = field(default_factory=dict)
    pair_scores: dict[str, float] = field(default_factory=dict)


@dataclass
class MomentumPhase:
    """Momentum acceleration/deceleration phase for a currency."""
    phase: str = "neutral"      # "accelerating" / "decelerating" / "neutral"
    acceleration: float = 0.0   # rate of change of strength (2nd derivative)
    streak: int = 0             # consecutive readings in same direction
    velocity: float = 0.0       # 1st derivative: how fast score is changing
    accel_magnitude: str = "none"  # "none" / "gentle" / "strong" / "explosive"
    tf_velocities: dict[str, float] = field(default_factory=dict)  # per-TF velocity


@dataclass
class CalculationResult:
    """Complete calculation results for all timeframes."""
    timeframes: dict[str, TimeframeResult] = field(default_factory=dict)
    range_states: list = field(default_factory=list)  # list[RangeState]
    momentum_phases: dict[str, MomentumPhase] = field(default_factory=dict)  # ccy -> phase
    flow_states: dict = field(default_factory=dict)  # pair -> TickFlowState
    close_prices: dict[str, float] = field(default_factory=dict)  # pair -> latest close
    high_prices: dict[str, float] = field(default_factory=dict)   # pair -> latest M1 high
    low_prices: dict[str, float] = field(default_factory=dict)    # pair -> latest M1 low
    tick_volumes: dict[str, int] = field(default_factory=dict)    # pair -> M1 tick volume
    m1_bar_time: int = 0  # Unix timestamp of current M1 bar open (for bar-close detection)
    h1_atr: dict[str, float] = field(default_factory=dict)        # pair -> H1 ATR(14) raw price
    session_range_pct: dict[str, float] = field(default_factory=dict)  # pair -> consumed %
    htf_regimes: dict = field(default_factory=dict)  # ccy -> {tf: (regime, strength)}
    velocity_data: dict = field(default_factory=dict)  # ccy -> (velocity, is_fast)
    composite_scores: dict[str, float] = field(default_factory=dict)  # ccy -> composite (all TFs)
    htf_composite_scores: dict[str, float] = field(default_factory=dict)  # ccy -> composite (M5+M15+H1 only)
    # Acceleration entry candidates: {pair: (direction, reason_str)}
    accel_candidates: dict[str, tuple[str, str]] = field(default_factory=dict)
    # Structural filter data: {pair: {prev_day_high, prev_day_low, prev_week_high, prev_week_low, pip}}
    structural_levels: dict[str, dict] = field(default_factory=dict)
    session_label: str = ""     # current session name
    # Stoch engine scores (0-10 scale, QM4-style)
    stoch_scores: dict[str, dict[str, float]] = field(default_factory=dict)  # {tf: {ccy: score}}
    stoch_entry_candidates: dict[str, tuple[str, str]] = field(default_factory=dict)  # {pair: (dir, reason)}
    stoch_entry_candidates_tuned: dict[str, tuple[str, str]] = field(default_factory=dict)  # tuned thresholds
    stoch_velocities: dict[str, float] = field(default_factory=dict)  # {ccy: velocity}
    # ── Live-candle engine outputs (2026-04-21) ──
    # Same shape as the candle-close versions above, but computed EVERY cycle
    # using bars that INCLUDE the forming candle. Drives the 5 "-live" paper
    # systems (Sv2-live / Sv2-Tun-live / Sv2+SS-live / Sv2+SS-Tun-live / Sv2+ATR-live).
    stoch_scores_live: dict[str, dict[str, float]] = field(default_factory=dict)
    stoch_entry_candidates_live: dict[str, tuple[str, str]] = field(default_factory=dict)
    stoch_entry_candidates_tuned_live: dict[str, tuple[str, str]] = field(default_factory=dict)
    # ── AU Gold suite data channel (2026-04-24) ──
    # Completely separate from ALL_28_PAIRS / CURRENCIES / _CCY_PAIRS so the
    # forex strength engine remains untouched. mt5_worker fetches XAUUSD bars
    # on a parallel loop and stores them here keyed by TF label
    # ('M1','M5','M15','H1','H4','D1'). AU1-5 strategies read exclusively
    # from this dict + from composite_scores['USD'] (read-only).
    #   xau_candles[tf]       -> np.ndarray of OHLCV bars (MT5 shape)
    #   xau_price            -> latest XAUUSD BID close (float)
    #   xau_high / xau_low   -> current M1 high/low (like fx high_prices)
    #   xau_spread_points    -> current bid-ask spread in pips
    #   xau_symbol           -> resolved broker symbol ("XAUUSD", "XAUUSDm", etc.)
    xau_candles: dict[str, Any] = field(default_factory=dict)
    xau_price: float = 0.0
    xau_high: float = 0.0
    xau_low: float = 0.0
    xau_spread_points: float = 0.0
    xau_symbol: str = ""
    # ── Shadow capture transport (Phase B, 2026-05-03) ──
    # Populated by mt5_worker._capture_sv2_shadow at M5 close: maps each
    # pair where Sv2's strength gate passed to the shadow_id of its
    # full-form journal record. main_window uses these IDs to call
    # mark_decision (at downstream block sites) and mark_executed (at
    # paper_trader.open_paper_trade success). Empty dict on cycles
    # that aren't M5 close, or when shadow logging is disabled.
    sv2_shadow_ids: dict[str, int] = field(default_factory=dict)
    connected: bool = True
    error_message: str = ""


# ── Technical Indicator Functions (Numba JIT-compiled) ─────────────

@njit(cache=True)
def _ema_jit(data: np.ndarray, alpha: float) -> np.ndarray:
    """JIT-compiled EMA core loop — ~50-100× faster than pure Python."""
    n = len(data)
    out = np.empty(n, dtype=np.float64)
    out[0] = data[0]
    for i in range(1, n):
        out[i] = alpha * data[i] + (1.0 - alpha) * out[i - 1]
    return out


def compute_ema(data: np.ndarray, period: int) -> np.ndarray:
    """Compute Exponential Moving Average over a 1D array."""
    alpha = 2.0 / (period + 1)
    return _ema_jit(np.ascontiguousarray(data, dtype=np.float64), alpha)


@njit(cache=True)
def _atr_jit(high: np.ndarray, low: np.ndarray, close: np.ndarray, alpha: float) -> np.ndarray:
    """JIT-compiled ATR: True Range + EMA smoothing in a single pass."""
    n = len(close)
    out = np.empty(n, dtype=np.float64)
    # First bar: TR = high - low
    out[0] = high[0] - low[0]
    for i in range(1, n):
        tr1 = high[i] - low[i]
        tr2 = abs(high[i] - close[i - 1])
        tr3 = abs(low[i] - close[i - 1])
        tr = max(tr1, max(tr2, tr3))
        out[i] = alpha * tr + (1.0 - alpha) * out[i - 1]
    return out


def compute_atr(
    high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14
) -> np.ndarray:
    """Compute Average True Range using EMA smoothing."""
    alpha = 2.0 / (period + 1)
    return _atr_jit(
        np.ascontiguousarray(high, dtype=np.float64),
        np.ascontiguousarray(low, dtype=np.float64),
        np.ascontiguousarray(close, dtype=np.float64),
        alpha,
    )


@njit(cache=True)
def _weighted_roc_jit(closes: np.ndarray, atr_val: float, n: int, decay: float) -> float:
    """JIT-compiled weighted micro-ROC computation."""
    total_w = 0.0
    weighted_sum = 0.0
    last = len(closes) - 1
    limit = min(n, last)
    for i in range(limit):
        w = math.exp(-decay * i)
        roc = (closes[last] - closes[last - i - 1]) / atr_val
        weighted_sum += w * roc
        total_w += w
    return weighted_sum / total_w if total_w > 0.0 else 0.0


# ── Calculation Engine ───────────────────────────────────────────────

class CalculationEngine:
    """Stateful engine that maintains Z-score buffers and computes strengths."""

    def __init__(
        self,
        sensitivity: float = 1.0,
        ema_period: int = 8,
        atr_period: int = 14,
        roc_decay: float = 0.3,
        roc_lookbacks: dict[str, int] | None = None,
        weights_m1: tuple[float, float, float] | None = None,
    ) -> None:
        self.sensitivity = sensitivity
        self._ema_period = ema_period
        self._atr_period = atr_period
        self._roc_decay = roc_decay
        self._roc_lookbacks = roc_lookbacks or dict(ROC_LOOKBACK)
        self._weights: dict[str, tuple[float, float, float]] = dict(_WEIGHTS)
        if weights_m1 is not None:
            self._weights["M1"] = weights_m1

        # Z-score rolling buffers: updated only on candle close
        self._zscore_buffers: dict[str, dict[str, deque]] = {
            ccy: {tf: deque(maxlen=ZSCORE_WINDOW) for tf in TIMEFRAME_LABELS}
            for ccy in CURRENCIES
        }

        # Momentum tracking: last N composite scores per currency for acceleration
        self._momentum_history: dict[str, deque] = {
            ccy: deque(maxlen=10) for ccy in CURRENCIES
        }
        self._momentum_phases: dict[str, MomentumPhase] = {
            ccy: MomentumPhase() for ccy in CURRENCIES
        }

        # Per-TF score history for acceleration detection (velocity per TF)
        self._tf_score_history: dict[str, dict[str, deque]] = {
            ccy: {tf: deque(maxlen=10) for tf in TIMEFRAME_LABELS}
            for ccy in CURRENCIES
        }

    # ── Bootstrap ────────────────────────────────────────────────

    def bootstrap(self, warmup_data: dict[str, dict[str, Any]]) -> None:
        """Fill Z-score buffers from 200-bar warmup data.

        Args:
            warmup_data: warmup_data[pair][tf_label] = numpy structured array.
        """
        for tf in TIMEFRAME_LABELS:
            # Pre-compute full indicator series per pair
            pair_series: dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] = {}
            min_len = 999_999

            for pair in ALL_28_PAIRS:
                candles = warmup_data.get(pair, {}).get(tf)
                if candles is None or len(candles) < 30:
                    continue

                closes = candles["close"].astype(np.float64)
                highs = candles["high"].astype(np.float64)
                lows = candles["low"].astype(np.float64)

                ema8 = compute_ema(closes, self._ema_period)
                atr14 = compute_atr(highs, lows, closes, self._atr_period)

                pair_series[pair] = (closes, ema8, atr14)
                min_len = min(min_len, len(closes))

            if not pair_series or min_len < 30:
                continue

            # Walk through last ZSCORE_WINDOW bars (with 20-bar warmup margin)
            start_idx = max(20, min_len - ZSCORE_WINDOW)

            for idx in range(start_idx, min_len):
                pair_scores: dict[str, float] = {}

                for pair, (closes, ema8, atr14) in pair_series.items():
                    if idx >= len(closes):
                        continue
                    atr_val = float(atr14[idx])
                    if atr_val <= 1e-10:
                        continue

                    # EMA Displacement
                    displacement = (float(closes[idx]) - float(ema8[idx])) / atr_val

                    # Weighted Micro-ROC (use slice up to idx+1 for JIT function)
                    n = self._roc_lookbacks.get(tf, 8)
                    weighted_roc = _weighted_roc_jit(closes[:idx + 1], atr_val, n, self._roc_decay)

                    # Composite (no tick velocity for historical data)
                    w_d, w_r, w_t = self._weights.get(tf, (0.50, 0.50, 0.00))
                    effective = w_d + w_r
                    score = (w_d / effective) * displacement + (w_r / effective) * weighted_roc
                    pair_scores[pair] = score

                if pair_scores:
                    raw = self._aggregate_raw(pair_scores)
                    for ccy in CURRENCIES:
                        self._zscore_buffers[ccy][tf].append(raw.get(ccy, 0.0))

        logger.info(
            "Z-score bootstrap complete: %s",
            {tf: len(self._zscore_buffers["USD"][tf]) for tf in TIMEFRAME_LABELS},
        )

    # ── Live Computation ─────────────────────────────────────────

    def compute(
        self,
        candle_data: dict[str, Any],
        tf: str,
        update_zscore: bool = False,
    ) -> TimeframeResult:
        """Compute currency and pair scores for one timeframe.

        Args:
            candle_data: candle_data[pair] = numpy structured array (latest N bars).
            tf: Timeframe label (e.g. "M1").
            update_zscore: If True, append raw values to Z-score buffer
                           (should be True on candle close only).

        Returns:
            TimeframeResult with normalized currency_scores and pair_scores.
        """
        pair_scores: dict[str, float] = {}

        for pair in ALL_28_PAIRS:
            candles = candle_data.get(pair)
            if candles is None or len(candles) < 20:
                continue

            closes = candles["close"].astype(np.float64)
            highs = candles["high"].astype(np.float64)
            lows = candles["low"].astype(np.float64)

            ema8 = compute_ema(closes, self._ema_period)
            atr14 = compute_atr(highs, lows, closes, self._atr_period)

            atr_val = float(atr14[-1])
            if atr_val <= 1e-10:
                continue

            # EMA Displacement
            displacement = (float(closes[-1]) - float(ema8[-1])) / atr_val

            # Weighted Micro-ROC (JIT-compiled)
            n = self._roc_lookbacks.get(tf, 8)
            weighted_roc = _weighted_roc_jit(closes, atr_val, n, self._roc_decay)

            # Composite score
            w_d, w_r, w_t = self._weights.get(tf, (0.50, 0.50, 0.00))

            if w_t > 0 and tf == "M1":
                # Tick velocity: how far price moved within the current forming candle
                tick_vel = (float(closes[-1]) - float(candles[-1]["open"])) / atr_val
                score = w_d * displacement + w_r * weighted_roc + w_t * tick_vel
            else:
                # No tick velocity — redistribute weight to displacement + ROC
                effective = w_d + w_r
                score = (w_d / effective) * displacement + (w_r / effective) * weighted_roc

            pair_scores[pair] = score

        if not pair_scores:
            return TimeframeResult()

        # Aggregate to raw per-currency strengths
        raw = self._aggregate_raw(pair_scores)

        # Update Z-score buffers on candle close
        if update_zscore:
            for ccy in CURRENCIES:
                self._zscore_buffers[ccy][tf].append(raw.get(ccy, 0.0))

        # Normalize with Z-score + tanh → ±10
        normalized: dict[str, float] = {}
        for ccy in CURRENCIES:
            r = raw.get(ccy, 0.0)
            buf = self._zscore_buffers[ccy][tf]

            if len(buf) < 5:
                # Not enough history — use raw value scaled simply
                normalized[ccy] = max(-10.0, min(10.0, r * 5.0))
                continue

            arr = np.array(buf)
            mean = float(np.mean(arr))
            std = float(np.std(arr))

            if std < 1e-10:
                normalized[ccy] = 0.0
            else:
                z = (r - mean) / std
                normalized[ccy] = 10.0 * math.tanh(z * self.sensitivity)

        # Pair display scores from normalized currency scores
        display_scores: dict[str, float] = {}
        for pair in DISPLAY_PAIRS:
            base_s = normalized.get(pair[:3], 0.0)
            quote_s = normalized.get(pair[3:], 0.0)
            display_scores[pair] = (base_s - quote_s) / 2.0

        return TimeframeResult(
            currency_scores=normalized,
            pair_scores=display_scores,
        )

    # ── Momentum Tracking ────────────────────────────────────────

    def update_momentum(
        self,
        composite_scores: dict[str, float],
        tf_scores: dict[str, dict[str, float]] | None = None,
    ) -> dict[str, MomentumPhase]:
        """Update momentum acceleration tracking from composite currency scores.

        Should be called once per cycle with the latest composite scores.
        Optionally accepts per-TF scores for per-TF velocity tracking.

        Args:
            composite_scores: {ccy: composite_score} across all TFs.
            tf_scores: {tf: {ccy: score}} per-timeframe scores (optional).

        Returns:
            Dict of ccy -> MomentumPhase.
        """
        _LOOKBACK = 3  # bars to compute velocity over

        for ccy in CURRENCIES:
            score = composite_scores.get(ccy, 0.0)
            history = self._momentum_history[ccy]
            history.append(score)

            phase = self._momentum_phases[ccy]

            # ── Per-TF velocity tracking ──
            if tf_scores:
                tf_vels: dict[str, float] = {}
                for tf in TIMEFRAME_LABELS:
                    tf_ccy_scores = tf_scores.get(tf)
                    if tf_ccy_scores and ccy in tf_ccy_scores:
                        tf_hist = self._tf_score_history[ccy][tf]
                        tf_hist.append(tf_ccy_scores[ccy])
                        if len(tf_hist) >= _LOOKBACK + 1:
                            tf_vels[tf] = tf_hist[-1] - tf_hist[-_LOOKBACK - 1]
                        else:
                            tf_vels[tf] = 0.0
                phase.tf_velocities = tf_vels

            # ── Composite velocity (1st derivative) ──
            if len(history) >= _LOOKBACK + 1:
                velocity = history[-1] - history[-_LOOKBACK - 1]
            else:
                velocity = 0.0
            phase.velocity = round(velocity, 3)

            if len(history) < 3:
                continue

            # ── Acceleration (2nd derivative) ──
            delta_now = history[-1] - history[-2]
            delta_prev = history[-2] - history[-3]
            acceleration = delta_now - delta_prev
            phase.acceleration = round(acceleration, 3)

            # ── Classify acceleration magnitude (strict) ──
            abs_vel = abs(velocity)
            if abs_vel > 5.0:
                phase.accel_magnitude = "explosive"
            elif abs_vel > 3.5:
                phase.accel_magnitude = "strong"
            elif abs_vel > 2.0:
                phase.accel_magnitude = "gentle"
            else:
                phase.accel_magnitude = "none"

            # ── Phase tracking ──
            if acceleration > 0.1:
                if phase.phase == "accelerating":
                    phase.streak += 1
                else:
                    phase.phase = "accelerating"
                    phase.streak = 1
            elif acceleration < -0.1:
                if phase.phase == "decelerating":
                    phase.streak += 1
                else:
                    phase.phase = "decelerating"
                    phase.streak = 1
            else:
                phase.phase = "neutral"
                phase.streak = 0

        return dict(self._momentum_phases)

    def check_acceleration_entry(
        self,
        base_ccy: str,
        quote_ccy: str,
        direction: str,
        min_velocity: float = 1.5,
        min_spread: float = 6.0,
        min_htf_agree: int = 3,  # ALL 3 HTFs (M5+M15+H1) must agree
    ) -> tuple[bool, str]:
        """Check if acceleration conditions are met for an early entry.

        Returns:
            (should_enter, reason_str)
        """
        base_phase = self._momentum_phases.get(base_ccy)
        quote_phase = self._momentum_phases.get(quote_ccy)
        if not base_phase or not quote_phase:
            return False, ""

        # Determine expected velocity directions
        if direction == "BUY":
            # Base should be getting STRONGER (positive velocity)
            # Quote should be getting WEAKER (negative velocity)
            base_ok = base_phase.velocity >= min_velocity
            quote_ok = quote_phase.velocity <= -min_velocity
        else:  # SELL
            base_ok = base_phase.velocity <= -min_velocity
            quote_ok = quote_phase.velocity >= min_velocity

        if not (base_ok and quote_ok):
            return False, ""

        # Check BOTH currencies have "strong" or "explosive" magnitude
        _OK_MAGS = ("strong", "explosive")
        if base_phase.accel_magnitude not in _OK_MAGS or quote_phase.accel_magnitude not in _OK_MAGS:
            return False, ""

        # Check HTF agreement: how many of M5/M15/H1 have velocity in the right direction?
        htf_tfs = ("M5", "M15", "H1")
        htf_agree = 0
        for tf in htf_tfs:
            base_tf_vel = base_phase.tf_velocities.get(tf, 0.0)
            quote_tf_vel = quote_phase.tf_velocities.get(tf, 0.0)
            if direction == "BUY":
                if base_tf_vel > 0.5 and quote_tf_vel < -0.5:
                    htf_agree += 1
            else:
                if base_tf_vel < -0.5 and quote_tf_vel > 0.5:
                    htf_agree += 1

        if htf_agree < min_htf_agree:
            return False, ""

        # Ensure HTF composite spread is already positive in trade direction
        # (prevents entering when spread is already flipped — would trigger
        # spread-collapse exit immediately).
        # Use the latest z-score buffer values as proxy for current HTF scores.
        base_htf_score = 0.0
        quote_htf_score = 0.0
        htf_score_count = 0
        for tf in htf_tfs:
            base_buf = self._zscore_buffers.get(base_ccy, {}).get(tf)
            quote_buf = self._zscore_buffers.get(quote_ccy, {}).get(tf)
            if base_buf and len(base_buf) > 0 and quote_buf and len(quote_buf) > 0:
                base_htf_score += base_buf[-1]
                quote_htf_score += quote_buf[-1]
                htf_score_count += 1
        if htf_score_count > 0:
            base_htf_avg = base_htf_score / htf_score_count
            quote_htf_avg = quote_htf_score / htf_score_count
            if direction == "BUY" and (base_htf_avg - quote_htf_avg) < 0:
                return False, ""  # HTF spread already flipped against BUY
            if direction == "SELL" and (quote_htf_avg - base_htf_avg) < 0:
                return False, ""  # HTF spread already flipped against SELL

        # Build reason string
        reason = (
            f"accel: {base_ccy} vel={base_phase.velocity:+.1f} "
            f"({base_phase.accel_magnitude}) | "
            f"{quote_ccy} vel={quote_phase.velocity:+.1f} "
            f"({quote_phase.accel_magnitude}) | "
            f"HTF agree={htf_agree}/3"
        )
        return True, reason

    def get_momentum_phases(self) -> dict[str, MomentumPhase]:
        """Get current momentum phases for all currencies."""
        return dict(self._momentum_phases)

    # ── Internal helpers ─────────────────────────────────────────

    def _aggregate_raw(self, pair_scores: dict[str, float]) -> dict[str, float]:
        """Average signed pair scores per currency across its 7 pairs."""
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
