"""Order Flow Proxy via tick direction analysis (Phase 7.4).

Tracks tick-by-tick price direction using MT5 tick data to infer
buying vs selling pressure. Provides a flow_bias score per pair
that can be used as a supplementary signal to the strength engine.

Flow bias = exponential-weighted sum of tick directions over a
rolling window. Positive = net buying, negative = net selling.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# Rolling window for tick direction tracking (number of ticks)
_TICK_WINDOW = 200
# Decay factor for exponential weighting (higher = more recent bias)
_DECAY = 0.02


@dataclass
class TickFlowState:
    """Current flow state for a pair."""

    pair: str
    flow_bias: float = 0.0        # net direction: +ve = buying, -ve = selling
    tick_count: int = 0           # ticks in current window
    buy_ratio: float = 0.5       # fraction of upticks
    intensity: float = 0.0       # absolute flow strength (0-1)


class TickFlowTracker:
    """Tracks tick direction to approximate order flow."""

    def __init__(self, window: int = _TICK_WINDOW) -> None:
        self._window = window
        # pair -> deque of tick directions (+1 for uptick, -1 for downtick)
        self._directions: dict[str, deque] = {}
        # pair -> last known price for direction detection
        self._last_price: dict[str, float] = {}

    def update_from_candles(
        self, pair: str, candles: Any
    ) -> TickFlowState | None:
        """Approximate flow from candle data when tick data isn't available.

        Uses close vs open of recent candles as a proxy for tick direction.
        This is less granular than actual ticks but still useful.

        Args:
            pair: Currency pair symbol.
            candles: Numpy structured array with 'open', 'close', 'tick_volume'.

        Returns:
            TickFlowState or None if insufficient data.
        """
        if candles is None or len(candles) < 10:
            return None

        if pair not in self._directions:
            self._directions[pair] = deque(maxlen=self._window)

        buf = self._directions[pair]

        # Use last N candles to approximate direction
        n = min(50, len(candles))
        for i in range(-n, 0):
            c = candles[i]
            direction = 1 if float(c["close"]) >= float(c["open"]) else -1
            # Weight by relative volume
            vol = max(1, int(c["tick_volume"]))
            # Add multiple entries proportional to volume (capped)
            entries = min(vol // 50 + 1, 5)
            for _ in range(entries):
                buf.append(direction)

        return self._compute_state(pair)

    def update_from_ticks(
        self, pair: str, ticks: Any
    ) -> TickFlowState | None:
        """Update from actual MT5 tick data.

        Args:
            pair: Currency pair symbol.
            ticks: Numpy structured array with 'bid' or 'last' field.

        Returns:
            TickFlowState or None if insufficient data.
        """
        if ticks is None or len(ticks) < 2:
            return None

        if pair not in self._directions:
            self._directions[pair] = deque(maxlen=self._window)

        buf = self._directions[pair]
        prices = ticks["bid"] if "bid" in ticks.dtype.names else ticks["last"]

        last_p = self._last_price.get(pair, float(prices[0]))
        for p in prices:
            p_f = float(p)
            if p_f > last_p:
                buf.append(1)
            elif p_f < last_p:
                buf.append(-1)
            last_p = p_f
        self._last_price[pair] = last_p

        return self._compute_state(pair)

    def get_state(self, pair: str) -> TickFlowState | None:
        """Get current flow state for a pair without updating."""
        if pair not in self._directions or len(self._directions[pair]) < 5:
            return None
        return self._compute_state(pair)

    def get_all_states(self) -> dict[str, TickFlowState]:
        """Get flow states for all tracked pairs."""
        states = {}
        for pair in self._directions:
            state = self.get_state(pair)
            if state:
                states[pair] = state
        return states

    def _compute_state(self, pair: str) -> TickFlowState:
        """Compute flow bias from direction buffer."""
        buf = self._directions[pair]
        if not buf:
            return TickFlowState(pair=pair)

        # Exponential-weighted sum (most recent ticks weighted more)
        total = 0.0
        weight_sum = 0.0
        n = len(buf)
        for i, d in enumerate(buf):
            w = 1.0 + _DECAY * (i - n)  # newer entries have higher index
            w = max(0.01, w)
            total += d * w
            weight_sum += w

        flow_bias = total / weight_sum if weight_sum > 0 else 0.0

        # Buy ratio
        ups = sum(1 for d in buf if d > 0)
        buy_ratio = ups / n if n > 0 else 0.5

        # Intensity: how strong is the imbalance (0-1)
        intensity = min(1.0, abs(flow_bias) * 2.0)

        return TickFlowState(
            pair=pair,
            flow_bias=round(flow_bias, 4),
            tick_count=n,
            buy_ratio=round(buy_ratio, 3),
            intensity=round(intensity, 3),
        )
