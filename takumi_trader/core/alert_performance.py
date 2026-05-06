"""Alert Performance Tracker — MAE/MFE tracking for all fired alerts (Stage 1).

Automatically records every trend alert and tracks its price outcome.

Two-phase tracking per alert:
  Phase 1: Entry → Exit signal (strength reversal detected)
  Phase 2: Exit signal → +4 hours (post-exit observation)

Tracks each pair individually with:
  - MFE (Maximum Favorable Excursion): best move in alert direction
  - MAE (Maximum Adverse Excursion): worst move against alert direction
  - Exit signal timing and P/L at exit
  - Post-exit price behavior (did price continue or reverse?)

Duplicate prevention: one trade per pair+direction per session.
Gap filling: on restart, fetches M1 history from MT5 to fill gaps.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from datetime import timezone, timedelta

_JST_FALLBACK = timezone(timedelta(hours=9))
_JST_CACHE = None


def _jst():
    """Lazy JST timezone — uses zoneinfo if available, UTC+9 fallback."""
    global _JST_CACHE
    if _JST_CACHE is None:
        try:
            from zoneinfo import ZoneInfo
            _JST_CACHE = ZoneInfo("Asia/Tokyo")
        except Exception:
            _JST_CACHE = _JST_FALLBACK
    return _JST_CACHE

from takumi_trader.core.trade_tracker import pip_value

logger = logging.getLogger(__name__)

# Exit detection: strength spread must drop below this to trigger exit signal
_EXIT_SPREAD_THRESHOLD = 4.0

# Hours to continue tracking after exit signal
_POST_EXIT_HOURS = 4.0

# Maximum tracking hours if no exit signal fires (safety cap)
_MAX_TRACKING_HOURS = 8.0


@dataclass
class AlertOutcome:
    """Record of a single alert's price outcome."""

    # Identity
    alert_id: str = ""
    pair: str = ""
    direction: str = ""  # "BUY" or "SELL"
    entry_price: float = 0.0
    entry_time: float = 0.0
    entry_time_str: str = ""

    # Context at entry
    conviction_score: int = 0
    conviction_tier: str = ""  # "FULL" / "DIMMED"
    session: str = ""
    base_score: float = 0.0
    quote_score: float = 0.0
    strength_spread: float = 0.0

    # Phase 1: Entry → Exit signal
    mfe_pips: float = 0.0
    mae_pips: float = 0.0  # stored as positive value
    time_to_mfe_minutes: float = 0.0
    time_to_mae_minutes: float = 0.0
    best_price: float = 0.0
    worst_price: float = 0.0

    # Exit signal
    exit_signal_fired: bool = False
    exit_signal_time: float = 0.0
    exit_signal_time_str: str = ""
    exit_signal_pnl_pips: float = 0.0
    exit_signal_price: float = 0.0
    time_to_exit_minutes: float = 0.0
    exit_reason: str = ""  # "spread_collapsed" / "direction_flipped" / "timeout"

    # Phase 2: Post-exit (+4 hours after exit signal)
    post_exit_mfe_pips: float = 0.0  # best move AFTER exit (in original direction)
    post_exit_mae_pips: float = 0.0  # worst move AFTER exit
    post_exit_final_pnl_pips: float = 0.0  # P/L at end of tracking vs entry

    # Overall (entire observation: entry through exit + 4h)
    max_mfe_pips: float = 0.0  # best favorable move across ENTIRE tracking window
    max_mae_pips: float = 0.0  # worst adverse move across ENTIRE tracking window
    final_pnl_pips: float = 0.0  # P/L at very end of all tracking
    total_tracking_minutes: float = 0.0
    completed: bool = False
    completion_time: float = 0.0

    # ATR at entry (H1 ATR(14) converted to pips)
    entry_atr_pips: float = 0.0

    # Bar-by-bar running MFE/MAE history (Phase 1 only: entry → signal exit)
    # Used by optimizer for accurate SL/TP simulation (determines hit order)
    bar_running_mfe: list[float] = field(default_factory=list)  # cumulative max MFE in pips per M1 bar
    bar_running_mae: list[float] = field(default_factory=list)  # cumulative max MAE in pips per M1 bar

    # SL/TP levels (set at entry, used for simulation in backtester)
    sl_pips: float = 0.0
    tp_pips: float = 0.0
    sl_price: float = 0.0
    tp_price: float = 0.0
    close_reason: str = ""  # "sl_hit", "tp_hit", "counter_momentum", "timeout", "backtest_end"

    # Entry type: "standard" (all 4 TFs extreme) or "acceleration" (momentum-driven early entry)
    entry_type: str = "stoch_v2"

    # Last update timestamp (for gap detection on restart)
    last_update_time: float = 0.0


class AlertPerformanceTracker:
    """Tracks price outcomes for all fired alerts automatically."""

    def __init__(self, post_exit_hours: float = _POST_EXIT_HOURS) -> None:
        self.post_exit_hours = post_exit_hours
        self._active: list[AlertOutcome] = []
        self._completed: list[AlertOutcome] = []
        # Track completed pair+direction+session keys to prevent duplicates
        self._session_keys: set[str] = set()

    def register_alert(
        self,
        pair: str,
        direction: str,
        entry_price: float,
        conviction_score: int,
        conviction_tier: str,
        session: str,
        base_score: float,
        quote_score: float,
    ) -> AlertOutcome | None:
        """Register a new alert for tracking.

        One trade per pair+direction per session. Skips duplicates.
        Returns the AlertOutcome if registered, None if duplicate.
        """
        now = time.time()

        # Unique key: pair + direction + session
        session_key = f"{pair}_{direction}_{session}"

        # Skip if already active with same pair+direction
        active_key = f"{pair}_{direction}"
        if any(f"{a.pair}_{a.direction}" == active_key for a in self._active):
            return None

        # Skip if already completed this pair+direction in this session
        if session_key in self._session_keys:
            return None

        alert_id = f"{pair}_{int(now * 1000)}"
        outcome = AlertOutcome(
            alert_id=alert_id,
            pair=pair,
            direction=direction,
            entry_price=entry_price,
            entry_time=now,
            entry_time_str=datetime.now(_jst()).strftime("%Y-%m-%d %H:%M:%S"),
            conviction_score=conviction_score,
            conviction_tier=conviction_tier,
            session=session,
            base_score=round(base_score, 2),
            quote_score=round(quote_score, 2),
            strength_spread=round(abs(base_score - quote_score), 2),
            best_price=entry_price,
            worst_price=entry_price,
            last_update_time=now,
        )

        self._active.append(outcome)
        self._session_keys.add(session_key)
        logger.info(
            "Perf tracker: registered %s %s @ %.5f (conviction=%d, session=%s)",
            direction, pair, entry_price, conviction_score, session,
        )
        return outcome

    def fill_gaps_from_mt5(self) -> None:
        """Fill price gaps for active alerts using MT5 M1 history.

        Call after MT5 is connected and active alerts are loaded.
        Fetches M1 candles covering the time the app was offline
        and retroactively updates MFE/MAE.
        """
        if not self._active:
            return

        try:
            import MetaTrader5 as mt5
        except ImportError:
            logger.warning("MetaTrader5 not available for gap filling")
            return

        if not mt5.terminal_info():
            logger.warning("MT5 not connected, skipping gap fill")
            return

        now = time.time()

        for alert in self._active:
            if alert.completed:
                continue

            # Determine gap: from last_update_time (or entry_time) to now
            last_t = alert.last_update_time or alert.entry_time
            gap_seconds = now - last_t
            if gap_seconds < 120:  # less than 2 min gap, skip
                continue

            gap_bars = int(gap_seconds / 60) + 5  # M1 bars + buffer
            gap_bars = min(gap_bars, 1500)  # cap at ~25 hours

            candles = mt5.copy_rates_from_pos(
                alert.pair, mt5.TIMEFRAME_M1, 0, gap_bars
            )
            if candles is None or len(candles) < 2:
                continue

            pip = pip_value(alert.pair)
            filled_count = 0

            for candle in candles:
                candle_time = float(candle["time"])
                # Only process candles in the gap period
                if candle_time <= last_t:
                    continue

                elapsed_min = (candle_time - alert.entry_time) / 60.0
                high = float(candle["high"])
                low = float(candle["low"])
                close = float(candle["close"])

                # Check both high and low for MFE/MAE
                for price in (high, low, close):
                    if alert.direction == "BUY":
                        pnl = (price - alert.entry_price) / pip
                    else:
                        pnl = (alert.entry_price - price) / pip

                    if not alert.exit_signal_fired:
                        if pnl > alert.mfe_pips:
                            alert.mfe_pips = round(pnl, 1)
                            alert.best_price = price
                            alert.time_to_mfe_minutes = round(elapsed_min, 1)
                        if pnl < 0 and abs(pnl) > alert.mae_pips:
                            alert.mae_pips = round(abs(pnl), 1)
                            alert.worst_price = price
                            alert.time_to_mae_minutes = round(elapsed_min, 1)
                    else:
                        # Post-exit phase gap fill
                        post_gain = pnl - alert.exit_signal_pnl_pips
                        if post_gain > alert.post_exit_mfe_pips:
                            alert.post_exit_mfe_pips = round(post_gain, 1)
                        adverse = alert.exit_signal_pnl_pips - pnl
                        if adverse > alert.post_exit_mae_pips:
                            alert.post_exit_mae_pips = round(adverse, 1)

                filled_count += 1

            alert.last_update_time = now

            if filled_count > 0:
                logger.info(
                    "Perf tracker: gap-filled %s %s with %d M1 bars (gap=%.0f min)",
                    alert.direction, alert.pair, filled_count, gap_seconds / 60,
                )

    def update(
        self,
        close_prices: dict[str, float],
        composite_scores: dict[str, float],
        htf_composite_scores: dict[str, float] | None = None,
    ) -> list[AlertOutcome]:
        """Update all active alerts with current prices and strength scores.

        Args:
            close_prices: {pair: latest_close_price}
            composite_scores: {currency: composite_strength} (all TFs)
            htf_composite_scores: {currency: composite_strength} (M5+M15+H1 only)
                                  Used for exit detection to filter M1 noise.

        Returns list of newly completed alerts.
        """
        if not self._active:
            return []

        now = time.time()
        newly_completed: list[AlertOutcome] = []

        for alert in self._active:
            price = close_prices.get(alert.pair)
            if price is None:
                continue

            pip = pip_value(alert.pair)
            elapsed_min = (now - alert.entry_time) / 60.0
            elapsed_hours = elapsed_min / 60.0

            if alert.direction == "BUY":
                current_pnl = (price - alert.entry_price) / pip
            else:
                current_pnl = (alert.entry_price - price) / pip

            # ── Track MAX-MFE / MAX-MAE across entire observation window ──
            if current_pnl > alert.max_mfe_pips:
                alert.max_mfe_pips = round(current_pnl, 1)
            if current_pnl < 0 and abs(current_pnl) > alert.max_mae_pips:
                alert.max_mae_pips = round(abs(current_pnl), 1)

            if not alert.exit_signal_fired:
                # ── Phase 1: Entry → Exit signal ──
                if current_pnl > alert.mfe_pips:
                    alert.mfe_pips = round(current_pnl, 1)
                    alert.best_price = price
                    alert.time_to_mfe_minutes = round(elapsed_min, 1)

                if current_pnl < 0 and abs(current_pnl) > alert.mae_pips:
                    alert.mae_pips = round(abs(current_pnl), 1)
                    alert.worst_price = price
                    alert.time_to_mae_minutes = round(elapsed_min, 1)

                # Check for exit signal — prefer HTF scores to filter M1 noise
                exit_scores = htf_composite_scores if htf_composite_scores else composite_scores
                exit_reason = self._check_exit_signal(alert, exit_scores)
                if exit_reason:
                    alert.exit_signal_fired = True
                    alert.exit_signal_time = now
                    alert.exit_signal_time_str = datetime.now(_jst()).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    alert.exit_signal_pnl_pips = round(current_pnl, 1)
                    alert.exit_signal_price = price
                    alert.time_to_exit_minutes = round(elapsed_min, 1)
                    alert.exit_reason = exit_reason
                    logger.info(
                        "Perf tracker: exit signal for %s %s — reason=%s, P/L=%.1f pips @ %.1f min",
                        alert.direction, alert.pair, exit_reason,
                        current_pnl, elapsed_min,
                    )

                # Safety cap
                if not alert.exit_signal_fired and elapsed_hours >= _MAX_TRACKING_HOURS:
                    alert.exit_signal_fired = True
                    alert.exit_signal_time = now
                    alert.exit_signal_time_str = datetime.now(_jst()).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    alert.exit_signal_pnl_pips = round(current_pnl, 1)
                    alert.exit_signal_price = price
                    alert.time_to_exit_minutes = round(elapsed_min, 1)
                    alert.exit_reason = "timeout"

            else:
                # ── Phase 2: Post-exit observation (+4 hours) ──
                post_exit_elapsed_hours = (now - alert.exit_signal_time) / 3600.0

                post_gain = current_pnl - alert.exit_signal_pnl_pips
                if post_gain > alert.post_exit_mfe_pips:
                    alert.post_exit_mfe_pips = round(post_gain, 1)
                adverse = alert.exit_signal_pnl_pips - current_pnl
                if adverse > alert.post_exit_mae_pips:
                    alert.post_exit_mae_pips = round(adverse, 1)

                # Check if post-exit window expired
                if post_exit_elapsed_hours >= self.post_exit_hours:
                    alert.post_exit_final_pnl_pips = round(current_pnl, 1)
                    alert.final_pnl_pips = round(current_pnl, 1)
                    alert.total_tracking_minutes = round(elapsed_min, 1)
                    alert.completed = True
                    alert.completion_time = now
                    newly_completed.append(alert)

            alert.last_update_time = now

        # Move completed from active list
        if newly_completed:
            self._active = [a for a in self._active if not a.completed]
            self._completed.extend(newly_completed)
            for a in newly_completed:
                logger.info(
                    "Perf tracker: completed %s %s — MFE=%.1f MAE=%.1f "
                    "Exit@%.1fmin(%.1fpips) PostMFE=%.1f PostMAE=%.1f Final=%.1f",
                    a.direction, a.pair, a.mfe_pips, a.mae_pips,
                    a.time_to_exit_minutes, a.exit_signal_pnl_pips,
                    a.post_exit_mfe_pips, a.post_exit_mae_pips, a.final_pnl_pips,
                )

        return newly_completed

    def _check_exit_signal(
        self, alert: AlertOutcome, composite_scores: dict[str, float]
    ) -> str:
        """Check if exit conditions are met for an alert."""
        base_ccy = alert.pair[:3]
        quote_ccy = alert.pair[3:]
        base_score = composite_scores.get(base_ccy)
        quote_score = composite_scores.get(quote_ccy)

        if base_score is None or quote_score is None:
            return ""

        # Stoch v2: no spread-collapse exit — perf tracker only monitors
        # SL/TP and counter-momentum exits (handled by paper_trader).
        # Perf tracker just records outcomes without triggering exits.
        return ""

    def get_active_count(self) -> int:
        """Get number of alerts currently being tracked."""
        return len(self._active)

    def get_active_alerts(self) -> list[AlertOutcome]:
        """Get all currently active alerts."""
        return list(self._active)

    # ── Persistence ───────────────────────────────────────────────

    def save_completed(self, path: Path) -> None:
        """Append newly completed alerts to the JSON file."""
        if not self._completed:
            return

        path.parent.mkdir(parents=True, exist_ok=True)

        existing: list[dict] = []
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                existing = []

        for outcome in self._completed:
            d = asdict(outcome)
            d.pop("bar_running_mfe", None)  # Exclude bar history (backtest only)
            d.pop("bar_running_mae", None)
            existing.append(d)

        path.write_text(
            json.dumps(existing, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info(
            "Perf tracker: saved %d outcomes (total %d)",
            len(self._completed), len(existing),
        )
        self._completed.clear()

    def save_active(self, path: Path) -> None:
        """Save all active (in-progress) alerts to disk for crash recovery."""
        path.parent.mkdir(parents=True, exist_ok=True)
        data = []
        for a in self._active:
            d = asdict(a)
            d.pop("bar_running_mfe", None)
            d.pop("bar_running_mae", None)
            data.append(d)
        try:
            path.write_text(
                json.dumps(data, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except OSError as e:
            logger.warning("Failed to save active alerts: %s", e)

    def load_active(self, path: Path) -> None:
        """Restore active alerts from disk on startup."""
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            for d in data:
                o = AlertOutcome()
                for k, v in d.items():
                    if hasattr(o, k):
                        setattr(o, k, v)
                if not o.completed:
                    self._active.append(o)
                    # Re-populate session key to prevent duplicates
                    self._session_keys.add(f"{o.pair}_{o.direction}_{o.session}")
            if self._active:
                logger.info("Perf tracker: restored %d active alerts from disk", len(self._active))
            path.unlink(missing_ok=True)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to load active alerts: %s", e)

    @staticmethod
    def load_history(path: Path) -> list[AlertOutcome]:
        """Load all completed alert outcomes from disk."""
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            outcomes = []
            for d in data:
                o = AlertOutcome()
                for k, v in d.items():
                    if hasattr(o, k):
                        setattr(o, k, v)
                outcomes.append(o)
            return outcomes
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to load alert outcomes: %s", e)
            return []
