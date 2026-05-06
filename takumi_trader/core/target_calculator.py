"""Dynamic Pip Target Calculator (Phase 7.11).

Calculates adaptive pip targets based on:
  - ATR of the traded pair (volatility context)
  - ADR consumed percentage (remaining daily range)
  - Session context (high vs low volatility sessions)
  - Currency strength delta (stronger divergence = more room to run)

Also handles partial target logic (take-half at 50% of target).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Session-based target multipliers
_SESSION_MULTIPLIER = {
    "tokyo": 0.7,        # lower volatility
    "london": 1.0,       # high volatility
    "frankfurt": 0.9,    # moderate
    "overlap": 1.2,      # highest volatility
    "ny_pm": 0.8,        # declining volatility
    "off_hours": 0.5,    # minimal volatility
}

# Base target ranges (pips) by pair type
_BASE_TARGET = {
    "JPY": 12.0,         # JPY pairs move more pips
    "default": 8.0,      # non-JPY pairs
}


def calculate_dynamic_target(
    pair: str,
    atr_pips: float,
    adr_consumed_pct: float,
    strength_delta: float,
    session: str = "off_hours",
    conviction: int = 100,
) -> tuple[float, float]:
    """Calculate dynamic pip target and partial target.

    Args:
        pair: Currency pair symbol.
        atr_pips: Current ATR in pips (from M5 or M15).
        adr_consumed_pct: How much of the daily range has been used (0-100).
        strength_delta: Absolute strength divergence between base and quote.
        session: Current trading session key.
        conviction: Entry conviction score 0-100 (Phase 8.9).

    Returns:
        (full_target_pips, partial_target_pips)
    """
    # Base target
    is_jpy = "JPY" in pair
    base = _BASE_TARGET["JPY"] if is_jpy else _BASE_TARGET["default"]

    # ATR adjustment: scale target relative to current volatility
    # If ATR > base, increase target; if ATR < base, decrease
    if atr_pips > 0:
        atr_factor = min(2.0, max(0.5, atr_pips / base))
    else:
        atr_factor = 1.0

    # ADR remaining adjustment: less remaining range = smaller target
    adr_remaining = max(0.0, 100.0 - adr_consumed_pct)
    if adr_remaining < 20:
        adr_factor = 0.5  # very little room left
    elif adr_remaining < 40:
        adr_factor = 0.7
    elif adr_remaining < 60:
        adr_factor = 0.85
    else:
        adr_factor = 1.0

    # Session multiplier
    session_factor = _SESSION_MULTIPLIER.get(session, 0.8)

    # Strength delta bonus: stronger divergence = more room to run
    if strength_delta >= 15.0:
        strength_factor = 1.3
    elif strength_delta >= 12.0:
        strength_factor = 1.15
    elif strength_delta >= 8.0:
        strength_factor = 1.0
    else:
        strength_factor = 0.85

    # Conviction multiplier (Phase 8.9): range 0.7–1.3
    conviction_mult = 0.7 + (conviction / 100.0) * 0.6

    # Combined target
    target = base * atr_factor * adr_factor * session_factor * strength_factor * conviction_mult

    # Clamp to reasonable range
    min_target = 3.0 if not is_jpy else 5.0
    max_target = 25.0 if not is_jpy else 35.0
    target = max(min_target, min(max_target, target))

    # Partial target at 50%
    partial = target * 0.5

    return round(target, 1), round(partial, 1)


def calculate_atr_pips(atr_value: float, pair: str) -> float:
    """Convert raw ATR value to pips.

    Args:
        atr_value: Raw ATR value from the indicator.
        pair: Currency pair symbol.

    Returns:
        ATR in pips.
    """
    pip = 0.01 if "JPY" in pair else 0.0001
    return atr_value / pip
