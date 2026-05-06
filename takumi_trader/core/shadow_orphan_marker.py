"""Orphan-PENDING fix — extracted from main_window._on_data for unit testability.

When `alert_mgr.check_and_fire(fire_candidates, open_pairs=open_pairs)` removes
pairs that were in `full_candidates` but are already represented by an open
trade (or are within an alert cooldown window), those pairs never reach the
per-pair trade-decision loop where the per-gate `mark_decision` hooks live.
Without this sweep, their shadow records would stay in STATUS_PENDING forever
— the orphan-PENDING bug from Phase B's day-1 validation (12 records).

This function runs immediately after `fired` is computed and marks every
excluded pair with the appropriate gate:

    pair in open_pairs AND existing trade present  →  GATE_DUPLICATE
                                                       block_source = "alert_mgr_dedup"
    excluded for any other reason (cooldown etc.)  →  GATE_INTERNAL
                                                       block_source = "alert_mgr_nondup"

The two paths are categorically distinct in Edge Miner: GATE_DUPLICATE answers
"would the second signal have been profitable?", GATE_INTERNAL is the
catch-all for cases the gate vocabulary doesn't promote to first-class.

Pure function — takes all dependencies as args, returns nothing, mutates only
through `shadow_logger.mark_decision`. Designed to be exception-safe at the
per-pair level so a single misbehaving record can't break the sweep for the
others. Failures are logged at WARNING.
"""
from __future__ import annotations

import logging
from typing import Any, Iterable

from takumi_trader.core.shadow_logger import (
    STATUS_BLOCKED, GATE_DUPLICATE, GATE_INTERNAL,
)

logger = logging.getLogger(__name__)


def mark_alert_mgr_orphans(
    shadow_logger,           # ShadowLogger instance, or None to no-op
    result,                  # CalculationResult — must have .sv2_shadow_ids
    full_candidates: dict,   # pair -> (direction, scores) tuple
    fired: Iterable[str],    # pairs that survived alert_mgr
    open_pairs: set[str],    # pairs with currently-open trades
    trade_tracker,           # exposes get_trade(pair) -> TrackedTrade | None
) -> dict[str, str]:
    """Mark shadow records for pairs that alert_mgr filtered out.

    Returns: dict of {pair: gate_chosen} for the pairs marked. Used by tests
    to verify the right gate fired; production code can ignore the return.
    """
    if shadow_logger is None:
        return {}

    excluded = set(full_candidates) - set(fired)
    if not excluded:
        return {}

    ids = getattr(result, "sv2_shadow_ids", None) or {}
    results: dict[str, str] = {}

    for pair in excluded:
        sid = ids.get(pair)
        if sid is None:
            # No shadow_id captured this cycle — pair wasn't a strength-pass
            # at the worker, so there's nothing to mark. Skip silently.
            continue

        candidate = full_candidates.get(pair)
        direction = candidate[0] if candidate else ""
        existing = None
        try:
            existing = trade_tracker.get_trade(pair)
        except Exception as exc:
            logger.warning(
                "[SHADOW] trade_tracker.get_trade(%s) raised: %s",
                pair, exc,
            )

        if pair in open_pairs and existing is not None:
            # Primary case: dedup against open trade for this pair
            try:
                shadow_logger.mark_decision(
                    sid, status=STATUS_BLOCKED, gate=GATE_DUPLICATE,
                    reason="alert_mgr filtered (pair already in open_pairs)",
                    metadata={
                        "existing_direction": existing.direction,
                        "existing_trade_age_minutes": round(
                            existing.duration_minutes, 1
                        ),
                        "existing_entry_price": existing.entry_price,
                        "blocked_direction": direction,
                        "already_open": True,
                        "block_source": "alert_mgr_dedup",
                    },
                )
                results[pair] = GATE_DUPLICATE
            except Exception as exc:
                logger.warning(
                    "[SHADOW] orphan-mark mark_decision failed pair=%s gate=%s: %s",
                    pair, GATE_DUPLICATE, exc,
                )
        else:
            # Secondary: alert_mgr filtered for a non-duplicate reason
            # (cooldown, debounce). Rare; categorize as INTERNAL so Edge
            # Miner can still slice it via the block_source field.
            try:
                shadow_logger.mark_decision(
                    sid, status=STATUS_BLOCKED, gate=GATE_INTERNAL,
                    reason="alert_mgr filtered (non-duplicate reason)",
                    metadata={
                        "blocked_direction": direction,
                        "in_open_pairs": pair in open_pairs,
                        "existing_present": existing is not None,
                        "block_source": "alert_mgr_nondup",
                    },
                )
                results[pair] = GATE_INTERNAL
            except Exception as exc:
                logger.warning(
                    "[SHADOW] orphan-mark mark_decision failed pair=%s gate=%s: %s",
                    pair, GATE_INTERNAL, exc,
                )

    return results
