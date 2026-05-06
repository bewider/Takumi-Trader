"""Alert Quality Filter Engine — 4 filters + conviction scoring (Phase 8).

Filters:
  1. HTF Trend Regime    (30 pts) — H4/D1 alignment
  2. Strength Velocity   (20 pts) — fast snap vs slow drift
  3. Isolation Score     (20 pts) — currency outlier detection
  4. Structural Filter   (15 pts) — key level proximity + TP clearance

Total raw: 85 pts → normalized to 0–100 conviction score.
Disabled filters contribute full points (neutral).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from takumi_trader.core.strength import CURRENCIES

logger = logging.getLogger(__name__)


@dataclass
class FilterResult:
    """Result from a single filter."""
    score: int = 0
    max_pts: int = 0
    passed: bool = True
    reason: str = ""
    details: dict = field(default_factory=dict)


@dataclass
class ConvictionResult:
    """Full conviction evaluation result."""
    conviction: int = 100           # 0–100
    tier: str = "FULL"              # FULL / DIMMED / SUPPRESSED
    components: dict[str, FilterResult] = field(default_factory=dict)
    strong_ccy: str = ""
    weak_ccy: str = ""


# ── Filter Settings ──────────────────────────────────────────────

@dataclass
class FilterSettings:
    """User-configurable filter settings."""
    # Toggle ON/OFF
    trend_regime_enabled: bool = True
    strength_velocity_enabled: bool = True
    isolation_enabled: bool = True
    structural_enabled: bool = True        # Key Level + TP Clearance

    # Thresholds
    regime_threshold: float = 3.0        # H4/D1 strength for BULL/BEAR
    velocity_threshold: float = 0.6      # pts/min for "fast"
    velocity_max_scale: float = 1.2      # velocity at which full points are awarded
    isolation_min_gap: float = 1.0       # min gap for isolation points
    structural_proximity_pips: float = 10.0  # min distance from key level

    # Conviction thresholds
    conviction_full_threshold: int = 70
    conviction_dimmed_threshold: int = 45


# ── Main Engine ──────────────────────────────────────────────────

class FilterEngine:
    """Evaluates alert quality through 4 independent filters."""

    def __init__(self, settings: FilterSettings | None = None) -> None:
        self.settings = settings or FilterSettings()

    def evaluate(
        self,
        strong_ccy: str,
        weak_ccy: str,
        pair: str,
        direction: str,
        htf_regimes: dict[str, dict[str, tuple[str, float]]] | None = None,
        velocity_data: dict[str, tuple[float, bool]] | None = None,
        composite_scores: dict[str, float] | None = None,
        structural_data: dict[str, Any] | None = None,
        entry_price: float = 0.0,
        tp_pips: float = 0.0,
    ) -> ConvictionResult:
        """Run all enabled filters and compute conviction score.

        Args:
            strong_ccy: The strong currency in the pair.
            weak_ccy: The weak currency in the pair.
            pair: Currency pair symbol.
            direction: "BUY" or "SELL".
            htf_regimes: {ccy: {tf: (regime, strength)}} from HTFRegimeTracker.
            velocity_data: {ccy: (velocity, is_fast)} from HTFRegimeTracker.
            composite_scores: {ccy: composite_score} for isolation calc.
            structural_data: Key level data for structural filter.
            entry_price: Current entry price for structural checks.
            tp_pips: Take-profit in pips for TP clearance check.

        Returns:
            ConvictionResult with score, tier, and component breakdown.
        """
        s = self.settings
        components: dict[str, FilterResult] = {}

        # ── Filter 1: HTF Trend Regime (30 pts) ──
        if s.trend_regime_enabled and htf_regimes:
            components["trend_regime"] = self._check_trend_regime(
                strong_ccy, weak_ccy, direction, htf_regimes
            )
        else:
            components["trend_regime"] = FilterResult(
                score=30, max_pts=30, passed=True,
                reason="Filter disabled" if not s.trend_regime_enabled else "No HTF data"
            )

        # ── Filter 2: Strength Velocity (20 pts) ──
        if s.strength_velocity_enabled and velocity_data:
            components["strength_velocity"] = self._check_velocity(
                strong_ccy, weak_ccy, direction, velocity_data
            )
        else:
            components["strength_velocity"] = FilterResult(
                score=20, max_pts=20, passed=True,
                reason="Filter disabled" if not s.strength_velocity_enabled else "No velocity data"
            )

        # ── Filter 3: Isolation Score (20 pts) ──
        if s.isolation_enabled and composite_scores:
            components["isolation"] = self._check_isolation(
                strong_ccy, weak_ccy, composite_scores
            )
        else:
            components["isolation"] = FilterResult(
                score=20, max_pts=20, passed=True,
                reason="Filter disabled" if not s.isolation_enabled else "No score data"
            )

        # ── Filter 4: Structural — Key Level Proximity + TP Clearance (15 pts) ──
        if s.structural_enabled and structural_data and entry_price > 0:
            components["structural"] = self._check_structural(
                pair, direction, entry_price, tp_pips, structural_data
            )
        else:
            components["structural"] = FilterResult(
                score=15, max_pts=15, passed=True,
                reason="Filter disabled" if not s.structural_enabled else "No level data"
            )

        # ── Conviction aggregation ──
        total_score = sum(c.score for c in components.values())
        max_possible = sum(c.max_pts for c in components.values())
        conviction = round((total_score / max_possible) * 100) if max_possible > 0 else 100

        # Classify tier
        if conviction >= s.conviction_full_threshold:
            tier = "FULL"
        elif conviction >= s.conviction_dimmed_threshold:
            tier = "DIMMED"
        else:
            tier = "SUPPRESSED"

        return ConvictionResult(
            conviction=conviction,
            tier=tier,
            components=components,
            strong_ccy=strong_ccy,
            weak_ccy=weak_ccy,
        )

    # ── Filter 1: HTF Trend Regime ────────────────────────────────

    def _check_trend_regime(
        self,
        strong_ccy: str,
        weak_ccy: str,
        direction: str,
        regimes: dict[str, dict[str, tuple[str, float]]],
    ) -> FilterResult:
        score = 0
        reasons: list[str] = []

        expected_strong = "BULLISH" if direction == "BUY" else "BEARISH"
        expected_weak = "BEARISH" if direction == "BUY" else "BULLISH"

        # H4
        h4_strong = regimes.get(strong_ccy, {}).get("H4", ("NEUTRAL", 0.0))
        h4_weak = regimes.get(weak_ccy, {}).get("H4", ("NEUTRAL", 0.0))
        h4s = h4_strong[0] == expected_strong
        h4w = h4_weak[0] == expected_weak

        if h4s:
            score += 8
        if h4w:
            score += 8
        if h4s and h4w:
            score += 4
            reasons.append("H4 fully aligned")
        elif h4s or h4w:
            reasons.append("H4 partial")
        else:
            reasons.append("H4 counter-trend")

        # D1
        d1_strong = regimes.get(strong_ccy, {}).get("D1", ("NEUTRAL", 0.0))
        d1_weak = regimes.get(weak_ccy, {}).get("D1", ("NEUTRAL", 0.0))
        d1s = d1_strong[0] == expected_strong
        d1w = d1_weak[0] == expected_weak

        if d1s:
            score += 4
        if d1w:
            score += 4
        if d1s and d1w:
            score += 2
            reasons.append("D1 fully aligned")
        elif d1s or d1w:
            reasons.append("D1 partial")

        return FilterResult(
            score=min(30, score),
            max_pts=30,
            passed=score >= 12,
            reason=" · ".join(reasons) if reasons else "No HTF alignment",
            details={
                "h4_strong": h4_strong[0],
                "h4_weak": h4_weak[0],
                "d1_strong": d1_strong[0],
                "d1_weak": d1_weak[0],
            },
        )

    # ── Filter 2: Strength Velocity ──────────────────────────────

    def _check_velocity(
        self,
        strong_ccy: str,
        weak_ccy: str,
        direction: str,
        velocity_data: dict[str, tuple[float, bool]],
    ) -> FilterResult:
        strong_vel, strong_fast = velocity_data.get(strong_ccy, (0.0, False))
        weak_vel, weak_fast = velocity_data.get(weak_ccy, (0.0, False))

        max_vel = self.settings.velocity_max_scale

        # Strong ccy should be moving in the alert direction
        strong_score = min(10, max(0, int((abs(strong_vel) / max_vel) * 10)))
        weak_score = min(10, max(0, int((abs(weak_vel) / max_vel) * 10)))
        total = strong_score + weak_score

        reasons = []
        if strong_fast:
            reasons.append(f"Strong {strong_ccy} snap {strong_vel:+.1f}/min")
        else:
            reasons.append(f"Strong {strong_ccy} drift {strong_vel:+.1f}/min")
        if weak_fast:
            reasons.append(f"Weak {weak_ccy} snap {weak_vel:+.1f}/min")
        else:
            reasons.append(f"Weak {weak_ccy} drift {weak_vel:+.1f}/min")

        return FilterResult(
            score=total,
            max_pts=20,
            passed=total >= 8,
            reason=" · ".join(reasons),
            details={"strong_vel": strong_vel, "weak_vel": weak_vel},
        )

    # ── Filter 3: Isolation Score ────────────────────────────────

    def _check_isolation(
        self,
        strong_ccy: str,
        weak_ccy: str,
        composite_scores: dict[str, float],
    ) -> FilterResult:
        # Sort currencies by composite strength
        sorted_ccys = sorted(
            composite_scores.items(), key=lambda x: x[1], reverse=True
        )
        ranking = {ccy: i + 1 for i, (ccy, _) in enumerate(sorted_ccys)}

        top_gap = sorted_ccys[0][1] - sorted_ccys[1][1] if len(sorted_ccys) >= 2 else 0.0
        bottom_gap = sorted_ccys[-2][1] - sorted_ccys[-1][1] if len(sorted_ccys) >= 2 else 0.0

        strong_rank = ranking.get(strong_ccy, 4)
        weak_rank = ranking.get(weak_ccy, 5)

        score = 0
        reasons = []
        min_gap = self.settings.isolation_min_gap

        # Strong currency isolation
        if strong_rank == 1:
            if top_gap >= min_gap * 2:
                score += 8
                reasons.append(f"Strong #{strong_rank} isolated (gap {top_gap:.1f})")
            elif top_gap >= min_gap:
                score += 5
                reasons.append(f"Strong #{strong_rank} (gap {top_gap:.1f})")
            else:
                score += 2
                reasons.append(f"Strong #{strong_rank} bunched (gap {top_gap:.1f})")
        else:
            reasons.append(f"Strong #{strong_rank}")

        # Weak currency isolation
        n = len(sorted_ccys)
        if weak_rank == n:
            if bottom_gap >= min_gap * 2:
                score += 8
                reasons.append(f"Weak #{weak_rank} isolated (gap {bottom_gap:.1f})")
            elif bottom_gap >= min_gap:
                score += 5
                reasons.append(f"Weak #{weak_rank} (gap {bottom_gap:.1f})")
            else:
                score += 2
                reasons.append(f"Weak #{weak_rank} bunched (gap {bottom_gap:.1f})")
        else:
            reasons.append(f"Weak #{weak_rank}")

        # Bonus: #1 vs #8
        if strong_rank == 1 and weak_rank == n:
            score += 4
            reasons.append("#1 vs #8 pairing")

        return FilterResult(
            score=min(20, score),
            max_pts=20,
            passed=score >= 8,
            reason=" · ".join(reasons),
            details={
                "strong_rank": strong_rank,
                "weak_rank": weak_rank,
                "top_gap": round(top_gap, 1),
                "bottom_gap": round(bottom_gap, 1),
            },
        )

    # ── Filter 4: Structural — Key Level Proximity + TP Clearance ──

    def _check_structural(
        self,
        pair: str,
        direction: str,
        entry_price: float,
        tp_pips: float,
        levels: dict[str, Any],
    ) -> FilterResult:
        """Check if entry is too close to key levels or TP is blocked.

        Structural data keys:
            prev_day_high, prev_day_low: Previous 24h high/low.
            prev_week_high, prev_week_low: Previous 5-day high/low.
            pip: Pip value for this pair.
        """
        pip = levels.get("pip", 0.0001)
        proximity = self.settings.structural_proximity_pips
        score = 0
        reasons: list[str] = []

        day_high = levels.get("prev_day_high", 0.0)
        day_low = levels.get("prev_day_low", 0.0)
        week_high = levels.get("prev_week_high", 0.0)
        week_low = levels.get("prev_week_low", 0.0)

        # ── Key Level Proximity (8 pts) ──
        level_ok = True
        if direction == "BUY":
            dist_day = (day_high - entry_price) / pip if day_high > 0 else 999
            dist_week = (week_high - entry_price) / pip if week_high > 0 else 999
            if 0 < dist_day < proximity:
                level_ok = False
                reasons.append(f"Near day high ({dist_day:.0f}p)")
            elif 0 < dist_week < proximity:
                level_ok = False
                reasons.append(f"Near week high ({dist_week:.0f}p)")
        else:
            dist_day = (entry_price - day_low) / pip if day_low > 0 else 999
            dist_week = (entry_price - week_low) / pip if week_low > 0 else 999
            if 0 < dist_day < proximity:
                level_ok = False
                reasons.append(f"Near day low ({dist_day:.0f}p)")
            elif 0 < dist_week < proximity:
                level_ok = False
                reasons.append(f"Near week low ({dist_week:.0f}p)")

        if level_ok:
            score += 8
            reasons.append("Clear of key levels")

        # ── TP Clearance (7 pts) ──
        tp_ok = True
        if tp_pips > 0:
            if direction == "BUY":
                tp_price = entry_price + tp_pips * pip
                if tp_price > day_high > 0:
                    tp_ok = False
                    over = (tp_price - day_high) / pip
                    reasons.append(f"TP {over:.0f}p above day high")
            else:
                tp_price = entry_price - tp_pips * pip
                if tp_price < day_low and day_low > 0:
                    tp_ok = False
                    over = (day_low - tp_price) / pip
                    reasons.append(f"TP {over:.0f}p below day low")

        if tp_ok:
            score += 7
            reasons.append("TP has clearance")

        return FilterResult(
            score=score,
            max_pts=15,
            passed=score >= 7,
            reason=" · ".join(reasons),
            details={
                "level_ok": level_ok,
                "tp_ok": tp_ok,
                "day_high": day_high,
                "day_low": day_low,
                "week_high": week_high,
                "week_low": week_low,
            },
        )
