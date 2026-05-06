"""Stochastic Currency Strength Engine — TAKUMI Trader Core v2

Replaces the EMA + ROC + Z-score engine with a simpler, more transparent
Stochastic-based approach proven against QM4 FSM.

For each timeframe:
  1. Compute Stochastic %K for all 28 pairs
  2. Adjust for base/quote (invert for quote currencies)
  3. Average across 7 pairs per currency
  4. Apply power scaling for clearer extremes
  5. Track velocity (speed of score change) for momentum detection

Entry: both currencies in extremes + velocity confirms
Exit: counter-momentum (velocity reverses strongly)
"""

import logging
import time
from dataclasses import dataclass, field

import numpy as np

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "NZD", "CHF"]

ALL_28_PAIRS = [
    "EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
    "EURGBP", "EURAUD", "EURNZD", "EURCAD", "EURCHF", "EURJPY",
    "GBPAUD", "GBPNZD", "GBPCAD", "GBPCHF", "GBPJPY",
    "AUDNZD", "AUDCAD", "AUDCHF", "AUDJPY",
    "NZDCAD", "NZDCHF", "NZDJPY",
    "CADCHF", "CADJPY",
    "CHFJPY",
]

DISPLAY_PAIRS = [
    "GBPJPY", "AUDJPY", "EURJPY", "NZDJPY", "CADJPY", "USDJPY", "CHFJPY",
    "AUDUSD", "NZDUSD", "EURUSD", "GBPUSD", "USDCAD", "GBPAUD", "GBPNZD",
    "GBPCAD", "GBPCHF", "EURAUD", "EURNZD", "EURCAD", "EURCHF", "USDCHF",
    "NZDCAD", "AUDCAD", "EURGBP", "AUDCHF", "NZDCHF", "CADCHF",
]

# Map each currency to its pairs and base/quote role
_CCY_PAIRS: dict[str, list[tuple[str, bool]]] = {ccy: [] for ccy in CURRENCIES}
for _p in ALL_28_PAIRS:
    _CCY_PAIRS[_p[:3]].append((_p, True))
    _CCY_PAIRS[_p[3:]].append((_p, False))

# Per-TF configuration: (period, power)
# Shorter periods = more reactive, higher power = more extreme values
TF_CONFIG: dict[str, tuple[int, float]] = {
    "M5":  (2, 0.7),   # Very reactive — entry detection
    "M15": (2, 0.7),   # Fast — proven QM4 match
    "H1":  (5, 0.5),   # Moderate — trend confirmation
    "H4":  (7, 0.5),   # Smooth — swing direction
    "D1":  (5, 0.5),   # Daily bias
    "W1":  (5, 0.5),   # Weekly bias
}

# Velocity tracking window (number of readings to keep)
_VELOCITY_WINDOW = 5

# Entry thresholds (relaxed from 7.5/2.5 — M5 Stoch(2) fluctuates rapidly)
ENTRY_STRONG = 7.0      # Currency must be above this to be "strong"
ENTRY_WEAK = 3.0        # Currency must be below this to be "weak"
ENTRY_MIN_VELOCITY = 1.5  # Minimum velocity (speed of change) for entry
ENTRY_MIN_TF_AGREE = 1   # Minimum TFs that must agree (out of M5+M15) — 1 is enough, M5 Stoch(2) is very reactive

# Exit thresholds
EXIT_COUNTER_VELOCITY = 2.0  # Velocity threshold for counter-momentum exit
EXIT_CONFIRM_BARS = 3        # Consecutive bars of counter-momentum before exit


# ── Data Classes ──────────────────────────────────────────────────────

@dataclass
class CurrencyScore:
    """Score for one currency across all timeframes."""
    scores: dict[str, float] = field(default_factory=dict)   # tf -> score (0-10)
    velocity: float = 0.0          # speed of change (positive = strengthening)
    velocity_magnitude: str = "none"  # none, gentle, strong, explosive
    prev_scores: list[float] = field(default_factory=list)  # history for velocity


@dataclass
class StochResult:
    """Complete calculation result for one cycle."""
    currency_scores: dict[str, CurrencyScore] = field(default_factory=dict)
    pair_spreads: dict[str, float] = field(default_factory=dict)  # pair -> spread (base-quote)
    entry_candidates: dict[str, tuple[str, str]] = field(default_factory=dict)  # pair -> (direction, reason)
    timestamp: float = 0.0


# ── Engine ────────────────────────────────────────────────────────────

class StochStrengthEngine:
    """Stochastic-based currency strength engine.

    Computes currency strength scores on a 0-10 scale using per-pair
    Stochastic %K averaged across all pairs for each currency.

    Tracks velocity (speed of score change) for momentum detection.
    """

    def __init__(self) -> None:
        # Per-currency score history for velocity tracking
        self._score_history: dict[str, list[float]] = {
            ccy: [] for ccy in CURRENCIES
        }
        # Counter-momentum exit confirmation counters
        self._exit_counters: dict[str, int] = {}
        # Last computed scores per TF
        self._cached_scores: dict[str, dict[str, float]] = {}
        # Candle close detection
        self._last_candle_times: dict[str, int] = {}

    def compute_tf(
        self,
        pair_data: dict[str, np.ndarray],
        tf_label: str,
    ) -> dict[str, float]:
        """Compute currency strength scores for one timeframe.

        Args:
            pair_data: {pair: structured_numpy_array} with close/high/low fields
            tf_label: timeframe label (M5, M15, H1, etc.)

        Returns:
            {currency: score_0_to_10}
        """
        config = TF_CONFIG.get(tf_label)
        if config is None:
            return {ccy: 5.0 for ccy in CURRENCIES}

        period, power = config

        # Step 1: Compute Stochastic per pair
        pair_scores: dict[str, float] = {}
        for pair in ALL_28_PAIRS:
            if pair not in pair_data:
                continue
            rates = pair_data[pair]
            if len(rates) < period + 1:
                continue

            closes = rates["close"].astype(np.float64)
            highs = rates["high"].astype(np.float64)
            lows = rates["low"].astype(np.float64)

            h = np.max(highs[-period:])
            l = np.min(lows[-period:])
            if h != l:
                pair_scores[pair] = ((closes[-1] - l) / (h - l)) * 100.0
            else:
                pair_scores[pair] = 50.0

        # Step 2+3: Average per currency (base/quote adjusted)
        scores: dict[str, float] = {}
        for ccy in CURRENCIES:
            vals: list[float] = []
            for pair, is_base in _CCY_PAIRS[ccy]:
                if pair in pair_scores:
                    v = pair_scores[pair]
                    vals.append(v if is_base else 100.0 - v)

            if vals:
                avg = float(np.mean(vals))

                # Step 4: Power scaling
                # ── THRESHOLD CALIBRATION NOTE ──
                # power<1.0 compresses the middle and pushes values toward
                # extremes. With power=0.7:
                #   underlying stoch avg 50   → score 5.0 (neutral)
                #   underlying stoch avg 54   → score ~5.8
                #   underlying stoch avg 70   → score ~7.6
                #   underlying stoch avg 90   → score ~9.1
                # So ENTRY_STRONG=7.0 does NOT mean "underlying stoch >=70%".
                # It corresponds to an underlying average of ~62%. If you
                # tune ENTRY_STRONG/ENTRY_WEAK, remember to consider this
                # non-linear mapping — these thresholds live on the power-
                # scaled 0..10 range, NOT the raw stochastic 0..100 scale.
                if power != 1.0:
                    centered = (avg - 50.0) / 50.0
                    scaled = np.sign(centered) * abs(centered) ** power
                    scores[ccy] = round(max(0.0, min(10.0, (scaled + 1.0) * 5.0)), 1)
                else:
                    scores[ccy] = round(max(0.0, min(10.0, avg / 10.0)), 1)
            # BUG #4 FIX (2026-04-21): if NO pairs contributed for this currency
            # (all pair fetches failed), do NOT insert a fake "neutral 5.0".
            # Downstream filters now treat missing ccy as "data unavailable →
            # block the trade" rather than silently passing as if neutral.
            # Previous behaviour: scores[ccy] = 5.0 (silently bypassed HTF
            # filters that use thresholds like < 3.5 / > 6.5).

        self._cached_scores[tf_label] = scores
        return scores

    def update_velocity(self, composite_scores: dict[str, float]) -> None:
        """Track velocity (speed of change) for each currency.

        Call this once per M5 candle close with the M5+M15 composite scores.
        Velocity = how fast the score is changing over recent readings.
        """
        for ccy in CURRENCIES:
            score = composite_scores.get(ccy, 5.0)
            history = self._score_history[ccy]
            history.append(score)

            # Keep only recent history
            if len(history) > _VELOCITY_WINDOW + 1:
                history.pop(0)

    def get_velocity(self, ccy: str) -> float:
        """Get current velocity for a currency.

        Returns positive for strengthening, negative for weakening.
        """
        history = self._score_history.get(ccy, [])
        if len(history) < 2:
            return 0.0

        # Velocity = weighted average of recent deltas (more recent = higher weight)
        deltas = [history[i] - history[i - 1] for i in range(1, len(history))]
        if not deltas:
            return 0.0

        weights = [i + 1 for i in range(len(deltas))]  # 1, 2, 3, ...
        return sum(d * w for d, w in zip(deltas, weights)) / sum(weights)

    def get_velocity_magnitude(self, velocity: float) -> str:
        """Classify velocity into magnitude categories."""
        av = abs(velocity)
        if av >= 3.0:
            return "explosive"
        elif av >= 2.0:
            return "strong"
        elif av >= 1.0:
            return "gentle"
        return "none"

    def get_composite(self, tfs: list[str] | None = None) -> dict[str, float]:
        """Get composite score averaged across specified TFs.

        Args:
            tfs: list of TF labels to average. Default: M5+M15.
        """
        if tfs is None:
            tfs = ["M5", "M15"]

        composite: dict[str, float] = {}
        for ccy in CURRENCIES:
            total = 0.0
            count = 0
            for tf in tfs:
                tf_scores = self._cached_scores.get(tf)
                if tf_scores and ccy in tf_scores:
                    total += tf_scores[ccy]
                    count += 1
            if count > 0:
                composite[ccy] = round(total / count, 1)
            else:
                composite[ccy] = 5.0

        return composite

    def check_entry(
        self,
        base_ccy: str,
        quote_ccy: str,
        direction: str,
        min_strong: float = ENTRY_STRONG,
        min_weak: float = ENTRY_WEAK,
        min_velocity: float = ENTRY_MIN_VELOCITY,
        min_tf_agree: int = ENTRY_MIN_TF_AGREE,
        h1_block_strong: float = 7.0,
        h1_block_weak: float = 3.0,
        h4_block_strong: float = 6.5,
        h4_block_weak: float = 3.5,
        d1_block_strong: float = 7.0,
        d1_block_weak: float = 3.0,
    ) -> tuple[bool, str]:
        """Check if entry conditions are met for a pair.

        Args:
            base_ccy: base currency (e.g., "AUD")
            quote_ccy: quote currency (e.g., "JPY")
            direction: "BUY" or "SELL"

        Returns:
            (pass, reason_string)
        """
        # Check which fast TFs agree
        fast_tfs = ["M5", "M15"]
        agree_count = 0
        details = []

        for tf in fast_tfs:
            tf_scores = self._cached_scores.get(tf, {})
            base_score = tf_scores.get(base_ccy, 5.0)
            quote_score = tf_scores.get(quote_ccy, 5.0)

            if direction == "BUY":
                if base_score >= min_strong and quote_score <= min_weak:
                    agree_count += 1
                    details.append(f"{tf}:{base_ccy}={base_score}/{quote_ccy}={quote_score}")
            else:  # SELL
                if base_score <= min_weak and quote_score >= min_strong:
                    agree_count += 1
                    details.append(f"{tf}:{base_ccy}={base_score}/{quote_ccy}={quote_score}")

        if agree_count < min_tf_agree:
            return False, f"only {agree_count}/{min_tf_agree} fast TFs agree"

        # Check H1 not strongly against (soft filter — just block counter-trend)
        # BUG #4 FIX (2026-04-21): block when H1 data is missing rather than
        # assuming "neutral" (which would silently pass the filter).
        h1_scores = self._cached_scores.get("H1", {})
        if not h1_scores:
            return False, "H1 scores not yet computed — blocking"
        h1_base = h1_scores.get(base_ccy)
        h1_quote = h1_scores.get(quote_ccy)
        if h1_base is None or h1_quote is None:
            return False, f"H1 data missing for {base_ccy}/{quote_ccy} — blocking"

        if direction == "BUY":
            if h1_base < h1_block_weak or h1_quote > h1_block_strong:
                return False, f"H1 against: {base_ccy}={h1_base}/{quote_ccy}={h1_quote}"
        else:
            if h1_base > h1_block_strong or h1_quote < h1_block_weak:
                return False, f"H1 against: {base_ccy}={h1_base}/{quote_ccy}={h1_quote}"

        # Check velocity (soft filter — skip if no history yet)
        base_vel = self.get_velocity(base_ccy)
        quote_vel = self.get_velocity(quote_ccy)
        has_velocity_data = len(self._score_history.get(base_ccy, [])) >= 3

        if has_velocity_data:
            # Only block if velocity is strongly AGAINST the trade
            if direction == "BUY":
                if base_vel < -min_velocity:
                    return False, f"{base_ccy} velocity strongly against: {base_vel:.1f}"
                if quote_vel > min_velocity:
                    return False, f"{quote_ccy} velocity strongly against: {quote_vel:.1f}"
            else:
                if base_vel > min_velocity:
                    return False, f"{base_ccy} velocity strongly against: {base_vel:.1f}"
                if quote_vel < -min_velocity:
                    return False, f"{quote_ccy} velocity strongly against: {quote_vel:.1f}"

            # ── Velocity-AND veto (added 2026-04-30) ──────────────────
            # The single-side check above only blocks LOUD counter-momentum
            # (|vel| ≥ min_velocity = 1.5). It misses the fading-move trap
            # where BOTH currencies are mildly but consistently signed
            # against the trade — the textbook "small upbar pullback inside
            # a downtrend" pattern.
            #
            # Real example (2026-04-30 16:57 GBPJPY BUY in a falling market):
            #   base_vel(GBP)  = -1.002  (single-side gate is -1.5 → passes)
            #   quote_vel(JPY) = +0.977  (single-side gate is +1.5 → passes)
            #   Both signed AGAINST the BUY → would have been killed here.
            #
            # A small dead-zone (0.3) prevents false vetoes from numerical
            # noise when both velocities are essentially zero.
            _DEAD = 0.3
            if direction == "BUY":
                if base_vel < -_DEAD and quote_vel > _DEAD:
                    return False, (
                        f"both velocities against BUY: "
                        f"{base_ccy}={base_vel:+.2f} {quote_ccy}={quote_vel:+.2f}"
                    )
            else:
                if base_vel > _DEAD and quote_vel < -_DEAD:
                    return False, (
                        f"both velocities against SELL: "
                        f"{base_ccy}={base_vel:+.2f} {quote_ccy}={quote_vel:+.2f}"
                    )

        # Check H4 not against trade direction (HARD RULE)
        # BUG #4 FIX (2026-04-21): use .get(ccy) without default instead of
        # .get(ccy, 5.0). A missing score (partial data during MT5 transient)
        # now BLOCKS the entry rather than silently passing the filter as
        # if the currency were neutral.
        h4_scores = self._cached_scores.get("H4", {})
        if h4_scores:
            h4_base = h4_scores.get(base_ccy)
            h4_quote = h4_scores.get(quote_ccy)
            if h4_base is None or h4_quote is None:
                return False, f"H4 data missing for {base_ccy}/{quote_ccy} — blocking"
            if direction == "BUY" and (h4_base < h4_block_weak or h4_quote > h4_block_strong):
                return False, f"H4 against: {base_ccy}={h4_base}/{quote_ccy}={h4_quote}"
            if direction == "SELL" and (h4_base > h4_block_strong or h4_quote < h4_block_weak):
                return False, f"H4 against: {base_ccy}={h4_base}/{quote_ccy}={h4_quote}"

        # Check D1 not against trade direction (HARD RULE)
        # BUG #1 FIX (2026-04-21): d1_block_* is now a parameter, not hard-coded.
        # Previously D1 used literal 3.0/7.0 regardless of what the caller
        # passed for h1/h4 — making tuned entries (System D/E) subject to
        # strict D1 even when the rest of the filter chain was loosened.
        # BUG #4 FIX: use .get(ccy) without default — missing scores = block.
        d1_scores = self._cached_scores.get("D1", {})
        if d1_scores:
            d1_base = d1_scores.get(base_ccy)
            d1_quote = d1_scores.get(quote_ccy)
            if d1_base is None or d1_quote is None:
                return False, f"D1 data missing for {base_ccy}/{quote_ccy} — blocking"
            if direction == "BUY" and (d1_base < d1_block_weak or d1_quote > d1_block_strong):
                return False, f"D1 against: {base_ccy}={d1_base}/{quote_ccy}={d1_quote}"
            if direction == "SELL" and (d1_base > d1_block_strong or d1_quote < d1_block_weak):
                return False, f"D1 against: {base_ccy}={d1_base}/{quote_ccy}={d1_quote}"

        reason = f"M5+M15 agree ({agree_count}), H1 confirms, vel {base_ccy}={base_vel:+.1f}/{quote_ccy}={quote_vel:+.1f}"
        return True, reason

    def check_exit(
        self,
        base_ccy: str,
        quote_ccy: str,
        direction: str,
        min_counter_vel: float = EXIT_COUNTER_VELOCITY,
    ) -> tuple[bool, str]:
        """Check if counter-momentum exit conditions are met.

        Only exits when strong/explosive velocity reversal detected
        on BOTH currencies, or persistent one-sided reversal.

        Returns:
            (should_exit, reason_string)
        """
        base_vel = self.get_velocity(base_ccy)
        quote_vel = self.get_velocity(quote_ccy)
        base_mag = self.get_velocity_magnitude(base_vel)
        quote_mag = self.get_velocity_magnitude(quote_vel)

        counter_mags = ("strong", "explosive")
        pair_key = base_ccy + quote_ccy

        if direction == "BUY":
            base_reversing = base_vel < -min_counter_vel and base_mag in counter_mags
            quote_reversing = quote_vel > min_counter_vel and quote_mag in counter_mags
        else:
            base_reversing = base_vel > min_counter_vel and base_mag in counter_mags
            quote_reversing = quote_vel < -min_counter_vel and quote_mag in counter_mags

        # Both reversing = immediate exit
        if base_reversing and quote_reversing:
            self._exit_counters.pop(pair_key, None)
            return True, f"counter_momentum: {base_ccy} vel={base_vel:+.1f} {quote_ccy} vel={quote_vel:+.1f}"

        # One reversing = count confirmations
        if base_reversing or quote_reversing:
            self._exit_counters[pair_key] = self._exit_counters.get(pair_key, 0) + 1
            if self._exit_counters[pair_key] >= EXIT_CONFIRM_BARS:
                self._exit_counters.pop(pair_key, None)
                reversing_ccy = base_ccy if base_reversing else quote_ccy
                rev_vel = base_vel if base_reversing else quote_vel
                return True, f"momentum_fading: {reversing_ccy} vel={rev_vel:+.1f} ({EXIT_CONFIRM_BARS} bars)"
        else:
            self._exit_counters[pair_key] = 0

        return False, ""

    def get_pair_spread(self, pair: str) -> float:
        """Get the M5+M15 composite spread for a pair.

        For BUY pairs: base_score - quote_score (positive = base stronger)
        """
        composite = self.get_composite(["M5", "M15"])
        base_ccy = pair[:3]
        quote_ccy = pair[3:]
        return composite.get(base_ccy, 5.0) - composite.get(quote_ccy, 5.0)

    def get_all_scores(self) -> dict[str, CurrencyScore]:
        """Get full score data for all currencies (for UI display)."""
        result: dict[str, CurrencyScore] = {}
        for ccy in CURRENCIES:
            cs = CurrencyScore()
            for tf, scores in self._cached_scores.items():
                if ccy in scores:
                    cs.scores[tf] = scores[ccy]

            vel = self.get_velocity(ccy)
            cs.velocity = vel
            cs.velocity_magnitude = self.get_velocity_magnitude(vel)
            cs.prev_scores = list(self._score_history.get(ccy, []))
            result[ccy] = cs
        return result
