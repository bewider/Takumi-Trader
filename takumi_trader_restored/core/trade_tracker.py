"""Active Trade Tracker — entry recording, P/L calculation (Phase 7.8).

Tracks manually initiated trades (user clicks TRACK on a pair alert).
Records entry price, direction, currency scores at entry, and computes
live P/L in pips. Feeds into the exit engine for vote-based exit signals.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Pip values
_PIP = {"JPY": 0.01}
_DEFAULT_PIP = 0.0001


def pip_value(pair: str) -> float:
    """Get pip value for a pair."""
    return _PIP.get(pair[3:6], _DEFAULT_PIP)


@dataclass
class TrackedTrade:
    """Represents a single tracked trade."""

    pair: str
    direction: str               # "BUY" or "SELL"
    entry_price: float
    entry_time: float            # time.time()
    entry_scores: dict[str, float] = field(default_factory=dict)  # {ccy: score}
    entry_base_score: float = 0.0
    entry_quote_score: float = 0.0

    # Live state (updated each cycle)
    current_price: float = 0.0
    pnl_pips: float = 0.0
    duration_minutes: float = 0.0
    peak_pnl_pips: float = 0.0   # best P/L achieved (for trailing)
    worst_pnl_pips: float = 0.0  # worst P/L (for stop tracking)

    # Exit signals
    exit_votes: dict[str, bool] = field(default_factory=dict)  # detector -> vote
    exit_vote_count: int = 0
    exit_vote_total: int = 5     # total detectors
    exit_urgency: str = ""       # "" / "WATCH" / "CLOSE" / "URGENT"
    suggested_action: str = ""   # "" / "TIGHTEN" / "PARTIAL" / "EXIT"

    # Target
    target_pips: float = 10.0
    partial_target_pips: float = 5.0
    partial_taken: bool = False

    # Conviction at entry (Phase 8.9)
    entry_conviction: int = 100   # 0–100 conviction score at trade entry

    # Paper trade fields
    is_paper: bool = False           # True for auto-opened paper trades
    sl_pips: float = 0.0            # Stop loss distance in pips
    tp_pips: float = 0.0            # Take profit distance in pips
    sl_price: float = 0.0           # Computed SL price level
    tp_price: float = 0.0           # Computed TP price level
    close_reason: str = ""          # "sl_hit", "tp_hit", "signal_exit", "manual"
    entry_type: str = "standard"    # "standard" or "acceleration"
    close_time: float = 0.0         # time.time() at close
    close_price: float = 0.0        # Price at close

    # Active flag
    active: bool = True


class TradeTracker:
    """Manages active tracked trades."""

    def __init__(self, max_trades: int = 5) -> None:
        self._trades: dict[str, TrackedTrade] = {}  # pair -> trade
        self._max_trades = max_trades
        self._closed_history: list[TrackedTrade] = []

    @property
    def active_trades(self) -> dict[str, TrackedTrade]:
        """Return all active trades."""
        return {p: t for p, t in self._trades.items() if t.active}

    @property
    def trade_count(self) -> int:
        return len(self.active_trades)

    def has_trade(self, pair: str) -> bool:
        return pair in self._trades and self._trades[pair].active

    def get_trade(self, pair: str) -> TrackedTrade | None:
        t = self._trades.get(pair)
        return t if t and t.active else None

    def open_trade(
        self,
        pair: str,
        direction: str,
        entry_price: float,
        currency_scores: dict[str, float] | None = None,
        target_pips: float = 10.0,
        is_paper: bool = False,
        sl_pips: float = 0.0,
        tp_pips: float = 0.0,
        sl_price: float = 0.0,
        tp_price: float = 0.0,
    ) -> TrackedTrade | None:
        """Record a new tracked trade.

        Args:
            pair: Currency pair symbol.
            direction: "BUY" or "SELL".
            entry_price: Price at trade entry.
            currency_scores: Current composite currency scores.
            target_pips: Initial pip target.
            is_paper: True for paper trades (bypass max_trades limit).
            sl_pips: Stop loss distance in pips.
            tp_pips: Take profit distance in pips.
            sl_price: Computed SL price level.
            tp_price: Computed TP price level.

        Returns:
            TrackedTrade or None if max trades exceeded.
        """
        # Paper trades bypass max_trades limit
        if not is_paper and self.trade_count >= self._max_trades:
            logger.warning("Max trades (%d) reached, cannot track %s", self._max_trades, pair)
            return None

        if self.has_trade(pair):
            logger.warning("Already tracking %s", pair)
            return self._trades[pair]

        scores = currency_scores or {}
        base, quote = pair[:3], pair[3:]

        trade = TrackedTrade(
            pair=pair,
            direction=direction,
            entry_price=entry_price,
            entry_time=time.time(),
            entry_scores=dict(scores),
            entry_base_score=scores.get(base, 0.0),
            entry_quote_score=scores.get(quote, 0.0),
            current_price=entry_price,
            target_pips=target_pips,
            partial_target_pips=target_pips * 0.5,
            is_paper=is_paper,
            sl_pips=sl_pips,
            tp_pips=tp_pips,
            sl_price=sl_price,
            tp_price=tp_price,
        )
        self._trades[pair] = trade
        tag = "[PAPER] " if is_paper else ""
        logger.info("%sTrade opened: %s %s @ %.5f target=%.1f pips SL=%.5f TP=%.5f",
                     tag, direction, pair, entry_price, target_pips, sl_price, tp_price)
        return trade

    def update_price(self, pair: str, current_price: float) -> TrackedTrade | None:
        """Update current price and P/L for a tracked trade.

        Args:
            pair: Currency pair symbol.
            current_price: Current market price.

        Returns:
            Updated TrackedTrade or None if not tracked.
        """
        trade = self.get_trade(pair)
        if not trade:
            return None

        trade.current_price = current_price
        pip = pip_value(pair)

        if trade.direction == "BUY":
            trade.pnl_pips = (current_price - trade.entry_price) / pip
        else:
            trade.pnl_pips = (trade.entry_price - current_price) / pip

        trade.duration_minutes = (time.time() - trade.entry_time) / 60.0

        # Update peak/worst
        if trade.pnl_pips > trade.peak_pnl_pips:
            trade.peak_pnl_pips = trade.pnl_pips
        if trade.pnl_pips < trade.worst_pnl_pips:
            trade.worst_pnl_pips = trade.pnl_pips

        return trade

    def close_trade(
        self, pair: str, reason: str = "manual", close_price: float = 0.0
    ) -> TrackedTrade | None:
        """Close a tracked trade.

        Args:
            pair: Currency pair symbol.
            reason: Reason for closing ("manual", "sl_hit", "tp_hit", "signal_exit").
            close_price: Price at close (0 = use current_price).

        Returns:
            Closed TrackedTrade or None if not tracked.
        """
        trade = self.get_trade(pair)
        if not trade:
            return None

        trade.active = False
        trade.close_reason = reason
        trade.close_time = time.time()
        trade.close_price = close_price if close_price > 0 else trade.current_price
        self._closed_history.append(trade)
        tag = "[PAPER] " if trade.is_paper else ""
        logger.info("%sTrade closed: %s %s P/L=%.1f pips reason=%s",
                     tag, trade.direction, pair, trade.pnl_pips, reason)
        return trade

    def mark_partial(self, pair: str) -> None:
        """Mark partial target as taken."""
        trade = self.get_trade(pair)
        if trade:
            trade.partial_taken = True

    def get_closed_history(self) -> list[TrackedTrade]:
        """Return closed trade history."""
        return list(self._closed_history)

    def save_to_file(self, path: str | Path) -> None:
        """Persist active trades to a JSON file."""
        active = self.active_trades
        data = []
        for pair, trade in active.items():
            d = asdict(trade)
            data.append(d)
        try:
            Path(path).write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception:
            logger.exception("Failed to save trades to %s", path)

    def load_from_file(self, path: str | Path) -> int:
        """Restore active trades from a JSON file.

        Returns:
            Number of trades restored.
        """
        p = Path(path)
        if not p.exists():
            return 0
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            count = 0
            for d in data:
                if not d.get("active", False):
                    continue
                trade = TrackedTrade(
                    pair=d["pair"],
                    direction=d["direction"],
                    entry_price=d["entry_price"],
                    entry_time=d["entry_time"],
                    entry_scores=d.get("entry_scores", {}),
                    entry_base_score=d.get("entry_base_score", 0.0),
                    entry_quote_score=d.get("entry_quote_score", 0.0),
                    current_price=d.get("current_price", d["entry_price"]),
                    pnl_pips=d.get("pnl_pips", 0.0),
                    peak_pnl_pips=d.get("peak_pnl_pips", 0.0),
                    worst_pnl_pips=d.get("worst_pnl_pips", 0.0),
                    target_pips=d.get("target_pips", 10.0),
                    partial_target_pips=d.get("partial_target_pips", 5.0),
                    partial_taken=d.get("partial_taken", False),
                    entry_conviction=d.get("entry_conviction", 100),
                    is_paper=d.get("is_paper", False),
                    sl_pips=d.get("sl_pips", 0.0),
                    tp_pips=d.get("tp_pips", 0.0),
                    sl_price=d.get("sl_price", 0.0),
                    tp_price=d.get("tp_price", 0.0),
                    active=True,
                )
                self._trades[trade.pair] = trade
                count += 1
            logger.info("Restored %d trades from %s", count, path)
            return count
        except Exception:
            logger.exception("Failed to load trades from %s", path)
            return 0
