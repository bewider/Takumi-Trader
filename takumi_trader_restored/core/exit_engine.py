"""Exit Engine — 5 exit detectors + vote aggregation + trailing strength stop (Phase 7.9-7.10, 7.12).

Five independent exit signal detectors, each casting a boolean vote:
  1. Strength Reversal  — base currency weakening OR quote strengthening
  2. Momentum Stall     — M1 momentum has flattened or reversed
  3. Range Exhaustion   — price approaching ADR limit
  4. Time Decay         — trade has been open too long for a scalp
  5. Adverse Flow       — tick flow turning against the trade

Vote escalation:
  0-1 votes: no action
  2 votes:   WATCH (tighten stop mentally)
  3 votes:   CLOSE (consider taking profit / closing)
  4-5 votes: URGENT (close immediately)

Trailing Strength Stop:
  If the currency strength delta drops by more than 40% from its peak
  since trade entry, escalate to CLOSE regardless of vote count.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from takumi_trader.core.trade_tracker import TrackedTrade

logger = logging.getLogger(__name__)

# ── Detector Thresholds ──────────────────────────────────────────────

# 1. Strength Reversal
_STRENGTH_REVERSAL_DROP = 3.0     # base dropped by this much from entry
_STRENGTH_QUOTE_RISE = 3.0        # quote rose by this much from entry

# 2. Momentum Stall
_MOMENTUM_FLAT_THRESHOLD = 0.3    # M1 score change < this = flat
_MOMENTUM_REVERSE_THRESHOLD = -1.0  # M1 score moved against trade

# 3. Range Exhaustion
_ADR_EXHAUSTION_PCT = 80.0        # ADR consumed > this %

# 4. Time Decay
_MAX_SCALP_MINUTES = 15.0         # scalp should be done within this
_EXTENDED_SCALP_MINUTES = 30.0    # definitely too long

# 5. Adverse Flow
_FLOW_AGAINST_THRESHOLD = -0.15   # flow bias against trade direction

# Trailing Strength Stop
_TRAILING_STRENGTH_DROP_PCT = 40.0  # delta dropped 40% from peak


@dataclass
class ExitVoteResult:
    """Result from the exit engine for one trade."""
    pair: str
    votes: dict[str, bool]        # detector_name -> voted_exit
    vote_count: int
    urgency: str                  # "" / "WATCH" / "CLOSE" / "URGENT"
    suggested_action: str         # "" / "TIGHTEN" / "PARTIAL" / "EXIT"
    reasons: list[str]            # human-readable reasons
    trailing_stop_triggered: bool


class ExitEngine:
    """Evaluates exit signals for tracked trades."""

    def __init__(self) -> None:
        # Track peak strength delta per trade for trailing stop
        self._peak_delta: dict[str, float] = {}

    def evaluate(
        self,
        trade: TrackedTrade,
        current_ccy_scores: dict[str, float],
        m1_pair_score: float | None = None,
        m1_pair_score_prev: float | None = None,
        adr_consumed_pct: float = 0.0,
        flow_bias: float | None = None,
    ) -> ExitVoteResult:
        """Run all 5 exit detectors on a tracked trade.

        Args:
            trade: The active tracked trade.
            current_ccy_scores: Current composite currency scores {ccy: score}.
            m1_pair_score: Current M1 pair score for this pair.
            m1_pair_score_prev: Previous M1 pair score (for momentum detection).
            adr_consumed_pct: How much of ADR has been consumed today.
            flow_bias: Current tick flow bias for this pair (-1 to +1).

        Returns:
            ExitVoteResult with all detector votes and recommendation.
        """
        base, quote = trade.pair[:3], trade.pair[3:]
        base_score = current_ccy_scores.get(base, 0.0)
        quote_score = current_ccy_scores.get(quote, 0.0)
        current_delta = base_score - quote_score

        # Update peak delta tracking
        if trade.direction == "SELL":
            current_delta = -current_delta  # flip for sell trades

        peak_key = trade.pair
        if peak_key not in self._peak_delta:
            self._peak_delta[peak_key] = abs(current_delta)
        else:
            self._peak_delta[peak_key] = max(
                self._peak_delta[peak_key], abs(current_delta)
            )

        votes: dict[str, bool] = {}
        reasons: list[str] = []

        # ── Detector 1: Strength Reversal ──
        vote1 = self._detect_strength_reversal(
            trade, base_score, quote_score
        )
        votes["strength_reversal"] = vote1
        if vote1:
            reasons.append("Strength reversing against trade")

        # ── Detector 2: Momentum Stall ──
        vote2 = self._detect_momentum_stall(
            trade, m1_pair_score, m1_pair_score_prev
        )
        votes["momentum_stall"] = vote2
        if vote2:
            reasons.append("M1 momentum stalled/reversed")

        # ── Detector 3: Range Exhaustion ──
        vote3 = self._detect_range_exhaustion(adr_consumed_pct)
        votes["range_exhaustion"] = vote3
        if vote3:
            reasons.append(f"ADR consumed {adr_consumed_pct:.0f}%")

        # ── Detector 4: Time Decay ──
        vote4 = self._detect_time_decay(trade)
        votes["time_decay"] = vote4
        if vote4:
            reasons.append(f"Open {trade.duration_minutes:.0f}min (scalp limit)")

        # ── Detector 5: Adverse Flow ──
        vote5 = self._detect_adverse_flow(trade, flow_bias)
        votes["adverse_flow"] = vote5
        if vote5:
            reasons.append("Tick flow against trade direction")

        # ── Trailing Strength Stop ──
        trailing_triggered = self._check_trailing_strength(
            trade.pair, current_delta
        )
        if trailing_triggered:
            reasons.append("Strength delta dropped >40% from peak")

        # ── Vote Aggregation ──
        vote_count = sum(votes.values())

        if trailing_triggered and vote_count < 3:
            vote_count = 3  # Force CLOSE level

        if vote_count >= 4:
            urgency = "URGENT"
            action = "EXIT"
        elif vote_count == 3:
            urgency = "CLOSE"
            action = "EXIT"
        elif vote_count == 2:
            urgency = "WATCH"
            action = "TIGHTEN"
        else:
            urgency = ""
            action = ""

        # Check for partial target
        if (trade.pnl_pips >= trade.partial_target_pips
                and not trade.partial_taken
                and vote_count >= 1):
            action = "PARTIAL"

        result = ExitVoteResult(
            pair=trade.pair,
            votes=votes,
            vote_count=vote_count,
            urgency=urgency,
            suggested_action=action,
            reasons=reasons,
            trailing_stop_triggered=trailing_triggered,
        )

        # Update trade with exit info
        trade.exit_votes = votes
        trade.exit_vote_count = vote_count
        trade.exit_urgency = urgency
        trade.suggested_action = action

        return result

    def clear_trade(self, pair: str) -> None:
        """Clean up state when a trade is closed."""
        self._peak_delta.pop(pair, None)

    # ── Individual Detectors ──────────────────────────────────────────

    def _detect_strength_reversal(
        self,
        trade: TrackedTrade,
        current_base: float,
        current_quote: float,
    ) -> bool:
        """Check if currency strength is reversing against the trade."""
        if trade.direction == "BUY":
            base_dropped = trade.entry_base_score - current_base >= _STRENGTH_REVERSAL_DROP
            quote_rose = current_quote - trade.entry_quote_score >= _STRENGTH_QUOTE_RISE
            return base_dropped or quote_rose
        else:  # SELL
            base_rose = current_base - trade.entry_base_score >= _STRENGTH_REVERSAL_DROP
            quote_dropped = trade.entry_quote_score - current_quote >= _STRENGTH_QUOTE_RISE
            return base_rose or quote_dropped

    def _detect_momentum_stall(
        self,
        trade: TrackedTrade,
        current_score: float | None,
        prev_score: float | None,
    ) -> bool:
        """Check if M1 momentum has stalled or reversed."""
        if current_score is None or prev_score is None:
            return False

        delta = current_score - prev_score

        if trade.direction == "BUY":
            # For a BUY, we want positive pair score; stall = score dropping
            return delta < _MOMENTUM_REVERSE_THRESHOLD
        else:
            # For a SELL, we want negative pair score; stall = score rising
            return delta > abs(_MOMENTUM_REVERSE_THRESHOLD)

    def _detect_range_exhaustion(self, adr_consumed_pct: float) -> bool:
        """Check if daily range is nearly exhausted."""
        return adr_consumed_pct >= _ADR_EXHAUSTION_PCT

    def _detect_time_decay(self, trade: TrackedTrade) -> bool:
        """Check if trade has been open too long for a scalp.

        Conviction-adjusted (Phase 8.9):
          High conviction (≥80): 1.3× time limit (more patience)
          Low conviction (<60):  0.7× time limit (prove yourself fast)
        """
        conv = trade.entry_conviction
        if conv >= 80:
            mult = 1.3
        elif conv < 60:
            mult = 0.7
        else:
            mult = 1.0
        return trade.duration_minutes >= _MAX_SCALP_MINUTES * mult

    def _detect_adverse_flow(
        self, trade: TrackedTrade, flow_bias: float | None
    ) -> bool:
        """Check if tick flow is moving against trade direction."""
        if flow_bias is None:
            return False

        if trade.direction == "BUY":
            return flow_bias < _FLOW_AGAINST_THRESHOLD
        else:
            return flow_bias > abs(_FLOW_AGAINST_THRESHOLD)

    def _check_trailing_strength(
        self, pair: str, current_delta: float
    ) -> bool:
        """Check if strength delta has dropped significantly from peak."""
        peak = self._peak_delta.get(pair, 0.0)
        if peak < 3.0:  # Not enough peak strength to be meaningful
            return False

        current_abs = abs(current_delta)
        drop_pct = ((peak - current_abs) / peak) * 100.0
        return drop_pct >= _TRAILING_STRENGTH_DROP_PCT
