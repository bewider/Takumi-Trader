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
    # Min distance from a key level (prev day/week/month high or low). A
    # BUY within this many pips of ANY prev high is blocked; likewise SELL
    # near any prev low. 2026-04-21: raised from 10→20 after CADJPY 18:05
    # BUY fired at 12.4p above prev_week_high (within 20 but past old 10),
    # triggering a user bug report. The check is STILL overridden dynamically
    # by max(proximity, sl_pips + 10) inside _check_structural so a pair
    # with larger-than-normal SL gets proportional protection.
    structural_proximity_pips: float = 20.0  # min distance from key level

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
        sl_pips: float = 0.0,
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
                pair, direction, entry_price, tp_pips, structural_data,
                sl_pips=sl_pips,
            )
        elif not s.structural_enabled:
            components["structural"] = FilterResult(
                score=15, max_pts=15, passed=True,
                reason="Filter disabled"
            )
        else:
            # No structural data available — block trade (fail-safe)
            components["structural"] = FilterResult(
                score=0, max_pts=15, passed=False,
                reason="No structural level data — blocked"
            )

        # ── Conviction aggregation ──
        total_score = sum(c.score for c in components.values())
        max_possible = sum(c.max_pts for c in components.values())
        conviction = round((total_score / max_possible) * 100) if max_possible > 0 else 100

        # Structural filter is a HARD BLOCK — if entry is near a key level,
        # suppress regardless of conviction score
        struct = components.get("structural")
        if struct and not struct.passed and s.structural_enabled:
            tier = "SUPPRESSED"
            # User-visible log so the blocks are traceable (the check
            # is silent otherwise — and the user reported on 2026-04-21
            # that they couldn't tell why a CADJPY BUY wasn't blocked).
            logger.info(
                "[LEVELS] %s %s %s @ %.5f BLOCKED — %s",
                direction, pair, strong_ccy + "/" + weak_ccy,
                entry_price, struct.reason,
            )
        elif conviction >= s.conviction_full_threshold:
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
        sl_pips: float = 0.0,
    ) -> FilterResult:
        """Check if entry is too close to key levels or TP is blocked.

        Structural data keys:
            prev_day_high, prev_day_low: Previous 24h high/low.
            prev_week_high, prev_week_low: Previous 5-day high/low.
            pip: Pip value for this pair.

        The proximity check is SL-aware (2026-04-21): the effective
        clearance requirement is max(structural_proximity_pips, sl_pips+10).
        This guarantees that a normal pullback to a broken level can't
        trigger our stop — the level stays outside the SL buffer + 10p
        margin — regardless of the pair's volatility. User rule: "no
        trading against previous day, week, month HIGH/LOW".
        """
        pip = levels.get("pip", 0.0001)
        # Dynamic proximity: at least `structural_proximity_pips`, and
        # always at least SL + 10p so a pullback to the broken level
        # can't run through our stop. For CADJPY BUY at 12.4p above the
        # prev_week_high with SL=8.4p, effective proximity is
        # max(20, 8.4+10)=20 — blocks the trade. (Previous fixed 10p
        # missed it; user reported the bug on 18:05 CADJPY.)
        _base_proximity = self.settings.structural_proximity_pips
        _sl_aware = sl_pips + 10.0 if sl_pips > 0 else 0.0
        proximity = max(_base_proximity, _sl_aware)
        score = 0
        reasons: list[str] = []

        day_high = levels.get("prev_day_high", 0.0)
        day_low = levels.get("prev_day_low", 0.0)
        week_high = levels.get("prev_week_high", 0.0)
        week_low = levels.get("prev_week_low", 0.0)
        month_high = levels.get("prev_month_high", 0.0)
        month_low = levels.get("prev_month_low", 0.0)

        # Fail-safe: if all key levels are zero, data is invalid → block
        if day_high == 0 and day_low == 0 and week_high == 0 and week_low == 0:
            return FilterResult(
                score=0, max_pts=15, passed=False,
                reason="Invalid structural data (all zeros)",
            )

        # ── Key Level Proximity (8 pts) ──
        # Block if price is WITHIN `proximity` pips of a level, either above or
        # below. abs(dist) catches both:
        #   - approaching the level (positive dist, hasn't crossed yet)
        #   - just broke through (negative dist, sitting on the level)
        # A true breakout requires clearing the level by at least `proximity` pips.
        level_ok = True
        if direction == "BUY":
            dist_day = (day_high - entry_price) / pip if day_high > 0 else 999
            dist_week = (week_high - entry_price) / pip if week_high > 0 else 999
            dist_month = (month_high - entry_price) / pip if month_high > 0 else 999
            if abs(dist_day) < proximity:
                level_ok = False
                reasons.append(f"At day high ({dist_day:.1f}p)")
            elif abs(dist_week) < proximity:
                level_ok = False
                reasons.append(f"At week high ({dist_week:.1f}p)")
            elif abs(dist_month) < proximity:
                level_ok = False
                reasons.append(f"At month high ({dist_month:.1f}p)")
        else:
            dist_day = (entry_price - day_low) / pip if day_low > 0 else 999
            dist_week = (entry_price - week_low) / pip if week_low > 0 else 999
            dist_month = (entry_price - month_low) / pip if month_low > 0 else 999
            if abs(dist_day) < proximity:
                level_ok = False
                reasons.append(f"At day low ({dist_day:.1f}p)")
            elif abs(dist_week) < proximity:
                level_ok = False
                reasons.append(f"At week low ({dist_week:.1f}p)")
            elif abs(dist_month) < proximity:
                level_ok = False
                reasons.append(f"At month low ({dist_month:.1f}p)")

        # ── Directional level check: is there a major level in the way? ──
        # For SELL: block if a major low is within (SL + TP + buffer) pips below entry
        # For BUY: block if a major high is within (SL + TP + buffer) pips above entry
        # This catches trades that are heading TOWARD a support/resistance
        _DIR_BUFFER = 10.0  # extra buffer pips
        if tp_pips > 0:
            _required_room = tp_pips + _DIR_BUFFER
            if direction == "BUY":
                for lvl_name, lvl in [("day high", day_high), ("week high", week_high), ("month high", month_high)]:
                    if lvl > 0:
                        room = (lvl - entry_price) / pip
                        if 0 < room < _required_room:
                            level_ok = False
                            reasons.append(f"Only {room:.0f}p room to {lvl_name} (need {_required_room:.0f}p)")
                            break
            else:
                for lvl_name, lvl in [("day low", day_low), ("week low", week_low), ("month low", month_low)]:
                    if lvl > 0:
                        room = (entry_price - lvl) / pip
                        if 0 < room < _required_room:
                            level_ok = False
                            reasons.append(f"Only {room:.0f}p room to {lvl_name} (need {_required_room:.0f}p)")
                            break

        if level_ok:
            score += 8
            reasons.append("Clear of key levels")

        # ── TP Clearance (7 pts) ──
        # The projected TP price must be at least 10 pips BEFORE a major level.
        # E.g. BUY with TP=20p: if weekly high is 25p above entry, TP lands
        # at 20p → only 5p clearance from level → BLOCKED (need 10p).
        # Entry must be at least (TP + 10) pips from the level.
        _TP_CLEARANCE = 10.0  # pips of clearance required beyond TP
        tp_ok = True
        if tp_pips > 0:
            if direction == "BUY":
                tp_price = entry_price + tp_pips * pip
                for lvl_name, lvl in [("day high", day_high), ("week high", week_high), ("month high", month_high)]:
                    if lvl > 0 and lvl > entry_price:
                        # Only check levels AHEAD of entry (above for BUY)
                        clearance = (lvl - tp_price) / pip
                        if clearance < _TP_CLEARANCE:
                            tp_ok = False
                            reasons.append(f"TP {clearance:.0f}p clearance vs {lvl_name} (need {_TP_CLEARANCE:.0f}p)")
                            break
            else:
                tp_price = entry_price - tp_pips * pip
                for lvl_name, lvl in [("day low", day_low), ("week low", week_low), ("month low", month_low)]:
                    if lvl > 0 and lvl < entry_price:
                        # Only check levels AHEAD of entry (below for SELL)
                        clearance = (tp_price - lvl) / pip
                        if clearance < _TP_CLEARANCE:
                            tp_ok = False
                            reasons.append(f"TP {clearance:.0f}p clearance vs {lvl_name} (need {_TP_CLEARANCE:.0f}p)")
                            break

        if tp_ok:
            score += 7
            reasons.append("TP has clearance")

        return FilterResult(
            score=score,
            max_pts=15,
            passed=level_ok and tp_ok,
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
