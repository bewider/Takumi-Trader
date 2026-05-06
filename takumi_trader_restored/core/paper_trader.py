"""Paper Trader — local trade simulation with SL/TP management.

Automatically opens paper trades on fired FULL alerts, monitors them
against SL/TP price levels using **M1 bar-close confirmation**, and closes on:
  - SL hit (checked at M1 bar close — matches backtest logic exactly)
  - TP hit (checked at M1 bar close)
  - Spread-collapse signal exit (from main_window)

SL/TP is ONLY checked when the M1 bar closes (every 60 seconds), using the
completed bar's high/low. This exactly matches the backtester's logic.

Uses per-pair optimized SL/TP from pair_algo_settings.json.
Journals all completed trades to data/paper_trades.json.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from takumi_trader.core.pair_algo_settings import get_pair_settings
from takumi_trader.core.trade_tracker import TrackedTrade, TradeTracker, pip_value

logger = logging.getLogger(__name__)

_JST_FALLBACK = timezone(timedelta(hours=9))
_JST_CACHE = None


def _jst():
    global _JST_CACHE
    if _JST_CACHE is None:
        try:
            from zoneinfo import ZoneInfo
            _JST_CACHE = ZoneInfo("Asia/Tokyo")
        except Exception:
            _JST_CACHE = _JST_FALLBACK
    return _JST_CACHE


# Default SL/TP if pair has no optimized settings
_DEFAULT_SL_PIPS = 10.0
_DEFAULT_TP_PIPS = 20.0


@dataclass
class PaperTradeRecord:
    """Completed paper trade record for the journal."""

    pair: str = ""
    direction: str = ""
    entry_price: float = 0.0
    entry_time: float = 0.0
    entry_time_str: str = ""
    close_price: float = 0.0
    close_time: float = 0.0
    close_time_str: str = ""
    close_reason: str = ""         # "sl_hit", "tp_hit", "signal_exit"
    sl_pips: float = 0.0
    tp_pips: float = 0.0
    sl_price: float = 0.0
    tp_price: float = 0.0
    pnl_pips: float = 0.0
    peak_pnl_pips: float = 0.0
    worst_pnl_pips: float = 0.0
    duration_minutes: float = 0.0
    entry_conviction: int = 0
    session: str = ""
    is_win: bool = False
    entry_type: str = "standard"   # "standard" or "acceleration"

    # Post-close observation (4h window after trade closes)
    post_close_max_mfe_pips: float = 0.0   # Best the trade could have done after close
    post_close_max_mae_pips: float = 0.0   # Worst excursion after close (confirms exit quality)
    post_close_final_pips: float = 0.0     # P/L at end of 4h window (from entry)
    post_close_minutes: float = 0.0        # How long we observed after close
    post_close_complete: bool = False       # True when 4h observation is done


class PaperTrader:
    """Manages paper trades with SL/TP monitoring."""

    # Post-close observation window (matches backtest post_exit_hours)
    POST_CLOSE_HOURS = 4.0

    def __init__(
        self,
        trade_tracker: TradeTracker,
        journal_path: Path,
    ) -> None:
        self._tracker = trade_tracker
        self._journal_path = journal_path
        self._journal: list[PaperTradeRecord] = []
        # Records still being observed post-close (journal_index -> True)
        # Using index-based tracking so multiple trades per pair are all observed
        self._post_close_watching: dict[int, bool] = {}
        self._last_journal_save: float = 0.0  # throttle saves to every 30s
        # ── M1 bar-close confirmation state ──
        # Track the current M1 bar timestamp so we detect bar changes
        self._last_m1_bar_time: int = 0
        # Accumulate each bar's high/low per pair for the completed bar
        self._bar_highs: dict[str, float] = {}  # pair -> running high within current bar
        self._bar_lows: dict[str, float] = {}   # pair -> running low within current bar

    # ── Open ────────────────────────────────────────────────────────

    def open_paper_trade(
        self,
        pair: str,
        direction: str,
        entry_price: float,
        composite_scores: dict[str, float] | None = None,
        conviction: int = 0,
        session: str = "",
        h1_atr: float = 0.0,
        entry_type: str = "standard",
    ) -> TrackedTrade | None:
        """Open a paper trade using dynamic ATR-based SL/TP.

        SL/TP is computed as: sl_atr × H1_ATR / pip (same as backtester).
        Falls back to static sl_pips/tp_pips if H1 ATR is unavailable.

        Returns the TrackedTrade or None if already tracking this pair.
        """
        if self._tracker.has_trade(pair):
            return None

        # Look up optimized ATR multipliers from pair settings
        settings = get_pair_settings(pair)
        pip = pip_value(pair)

        if settings and h1_atr > 0:
            # Dynamic: same formula as backtester
            sl_atr_mult = settings.get("sl_atr", 0.3)
            tp_atr_mult = settings.get("tp_atr", 1.0)
            sl_pips = round(sl_atr_mult * h1_atr / pip, 1)
            tp_pips = round(tp_atr_mult * h1_atr / pip, 1)
            logger.info(
                "[PAPER] %s ATR-based SL/TP: H1_ATR=%.5f  "
                "SL=%.1f×ATR=%.1fp  TP=%.1f×ATR=%.1fp",
                pair, h1_atr, sl_atr_mult, sl_pips, tp_atr_mult, tp_pips,
            )
        elif settings:
            # Fallback to static averages if no ATR available
            sl_pips = settings.get("sl_pips", _DEFAULT_SL_PIPS)
            tp_pips = settings.get("tp_pips", _DEFAULT_TP_PIPS)
            logger.warning(
                "[PAPER] %s no H1 ATR available, using static SL=%.1fp TP=%.1fp",
                pair, sl_pips, tp_pips,
            )
        else:
            sl_pips = _DEFAULT_SL_PIPS
            tp_pips = _DEFAULT_TP_PIPS

        # Compute SL/TP price levels
        if direction == "BUY":
            sl_price = entry_price - (sl_pips * pip)
            tp_price = entry_price + (tp_pips * pip)
        else:  # SELL
            sl_price = entry_price + (sl_pips * pip)
            tp_price = entry_price - (tp_pips * pip)

        trade = self._tracker.open_trade(
            pair=pair,
            direction=direction,
            entry_price=entry_price,
            currency_scores=composite_scores,
            target_pips=tp_pips,
            is_paper=True,
            sl_pips=sl_pips,
            tp_pips=tp_pips,
            sl_price=sl_price,
            tp_price=tp_price,
        )
        if trade:
            trade.entry_conviction = conviction
            trade._session = session
            trade.entry_type = entry_type
            logger.info(
                "[PAPER] Opened %s %s @ %.5f  SL=%.5f (%.1fp)  TP=%.5f (%.1fp)  conv=%d  session=%s",
                direction, pair, entry_price, sl_price, sl_pips, tp_price, tp_pips, conviction, session,
            )
        return trade

    # ── SL/TP Monitoring ────────────────────────────────────────────

    def _check_sl_tp(
        self, trade: TrackedTrade, high: float, low: float
    ) -> str | None:
        """Check if SL or TP was hit using M1 candle high/low.

        Returns "sl_hit", "tp_hit", or None.
        TP is checked first (optimistic fill assumption).
        """
        if trade.sl_price <= 0 or trade.tp_price <= 0:
            return None

        if trade.direction == "BUY":
            # TP hit if high reaches TP level
            if high >= trade.tp_price:
                return "tp_hit"
            # SL hit if low reaches SL level
            if low <= trade.sl_price:
                return "sl_hit"
        else:  # SELL
            # TP hit if low reaches TP level
            if low <= trade.tp_price:
                return "tp_hit"
            # SL hit if high reaches SL level
            if high >= trade.sl_price:
                return "sl_hit"

        return None

    def update_cycle(
        self,
        high_prices: dict[str, float],
        low_prices: dict[str, float],
        close_prices: dict[str, float],
        m1_bar_time: int = 0,
    ) -> list[PaperTradeRecord]:
        """Check all active paper trades for SL/TP hits.

        Uses **M1 bar-close confirmation** to match backtest logic exactly:
        - SL/TP is only checked when a new M1 bar opens (= previous bar closed)
        - Uses the completed bar's accumulated high/low for the check
        - No intra-bar SL/TP — pure statistical SL/TP from optimization

        Called every cycle (~1 second) from main_window.
        Returns list of newly closed paper trade records.
        """
        closed: list[PaperTradeRecord] = []
        active = self._tracker.active_trades
        if not active:
            return closed

        # Detect M1 bar close: when bar timestamp changes, previous bar is complete
        bar_just_closed = False
        if m1_bar_time > 0 and m1_bar_time != self._last_m1_bar_time:
            if self._last_m1_bar_time > 0:
                bar_just_closed = True
            self._last_m1_bar_time = m1_bar_time

        # Update running high/low for the current bar (accumulate intra-bar)
        for pair in list(active.keys()):
            h = high_prices.get(pair)
            l = low_prices.get(pair)
            if h is None or l is None:
                continue
            # Expand the running bar range
            if pair in self._bar_highs:
                self._bar_highs[pair] = max(self._bar_highs[pair], h)
                self._bar_lows[pair] = min(self._bar_lows[pair], l)
            else:
                self._bar_highs[pair] = h
                self._bar_lows[pair] = l

        for pair, trade in list(active.items()):
            if not trade.is_paper:
                continue

            # ── SL/TP: only checked at M1 bar close (matches backtest exactly) ──
            if bar_just_closed:
                bar_h = self._bar_highs.get(pair)
                bar_l = self._bar_lows.get(pair)
                if bar_h is not None and bar_l is not None:
                    hit = self._check_sl_tp(trade, bar_h, bar_l)
                    if hit:
                        if hit == "tp_hit":
                            close_price = trade.tp_price
                        else:
                            close_price = trade.sl_price
                        logger.info(
                            "[PAPER] %s %s %s at M1 bar close — price %.5f",
                            trade.direction, pair, hit, close_price,
                        )
                        record = self._close_and_journal(pair, hit, close_price)
                        if record:
                            closed.append(record)

        # Reset bar accumulators when a new bar starts
        if bar_just_closed:
            self._bar_highs.clear()
            self._bar_lows.clear()

        return closed

    # ── Post-Close Observation ─────────────────────────────────────

    def post_close_cycle(
        self,
        high_prices: dict[str, float],
        low_prices: dict[str, float],
        close_prices: dict[str, float],
    ) -> None:
        """Track MAX-MFE / MAX-MAE for 4h after each closed paper trade.

        Called every cycle from main_window (same cadence as update_cycle).
        Uses M1 high/low for realistic extremes, matching backtester behavior.
        """
        if not self._post_close_watching:
            return

        now = time.time()
        completed_indices: list[int] = []

        for idx in list(self._post_close_watching.keys()):
            record = self._journal[idx]
            pair = record.pair

            # Check if 4h observation window has elapsed
            elapsed_min = (now - record.close_time) / 60.0
            record.post_close_minutes = round(elapsed_min, 1)

            high = high_prices.get(pair)
            low = low_prices.get(pair)
            close = close_prices.get(pair)
            if high is None or low is None:
                continue

            pip = pip_value(pair)

            # Compute excursion from entry price using M1 high/low
            if record.direction == "BUY":
                best_pnl = (high - record.entry_price) / pip
                worst_pnl = (low - record.entry_price) / pip
            else:  # SELL
                best_pnl = (record.entry_price - low) / pip
                worst_pnl = (record.entry_price - high) / pip

            # Update MAX-MFE (best possible from entry)
            if best_pnl > record.post_close_max_mfe_pips:
                record.post_close_max_mfe_pips = round(best_pnl, 1)

            # Update MAX-MAE (worst excursion from entry)
            if worst_pnl < -record.post_close_max_mae_pips:
                record.post_close_max_mae_pips = round(abs(worst_pnl), 1)

            # Final P/L at current close
            if close is not None:
                if record.direction == "BUY":
                    record.post_close_final_pips = round(
                        (close - record.entry_price) / pip, 1
                    )
                else:
                    record.post_close_final_pips = round(
                        (record.entry_price - close) / pip, 1
                    )

            # Check if observation window is complete
            if elapsed_min >= self.POST_CLOSE_HOURS * 60:
                record.post_close_complete = True
                completed_indices.append(idx)
                logger.info(
                    "[PAPER] Post-close complete %s %s — "
                    "MAX-MFE=%.1fp  MAX-MAE=%.1fp  Final=%.1fp  "
                    "(closed at %.1fp)",
                    record.direction, pair,
                    record.post_close_max_mfe_pips,
                    record.post_close_max_mae_pips,
                    record.post_close_final_pips,
                    record.pnl_pips,
                )

        # Remove completed observations
        for idx in completed_indices:
            del self._post_close_watching[idx]

        # Save journal — immediately on completion, throttled otherwise
        if completed_indices:
            self.save_journal()
            self._last_journal_save = now
        elif self._post_close_watching and (now - self._last_journal_save) >= 30.0:
            self.save_journal()
            self._last_journal_save = now

    @property
    def post_close_count(self) -> int:
        """Number of trades currently being observed post-close."""
        return len(self._post_close_watching)

    # ── Signal Exit ─────────────────────────────────────────────────

    def handle_exit_signal(
        self, pair: str, urgency: str, close_price: float
    ) -> PaperTradeRecord | None:
        """Close a paper trade on exit engine URGENT signal.

        Returns PaperTradeRecord if closed, None otherwise.
        """
        trade = self._tracker.get_trade(pair)
        if not trade or not trade.is_paper:
            return None

        if urgency not in ("URGENT", "CLOSE"):
            return None

        return self._close_and_journal(pair, "signal_exit", close_price)

    # ── Close & Journal ─────────────────────────────────────────────

    def _close_and_journal(
        self, pair: str, reason: str, close_price: float
    ) -> PaperTradeRecord | None:
        """Close a paper trade and create a journal record."""
        trade = self._tracker.get_trade(pair)
        if not trade or not trade.is_paper:
            return None

        # Compute final P/L at the actual close price
        pip = pip_value(pair)
        if trade.direction == "BUY":
            pnl = (close_price - trade.entry_price) / pip
        else:
            pnl = (trade.entry_price - close_price) / pip

        duration = (time.time() - trade.entry_time) / 60.0
        now_str = datetime.now(_jst()).strftime("%Y-%m-%d %H:%M:%S")
        entry_str = datetime.fromtimestamp(
            trade.entry_time, tz=_jst()
        ).strftime("%Y-%m-%d %H:%M:%S")

        record = PaperTradeRecord(
            pair=pair,
            direction=trade.direction,
            entry_price=trade.entry_price,
            entry_time=trade.entry_time,
            entry_time_str=entry_str,
            close_price=close_price,
            close_time=time.time(),
            close_time_str=now_str,
            close_reason=reason,
            sl_pips=trade.sl_pips,
            tp_pips=trade.tp_pips,
            sl_price=trade.sl_price,
            tp_price=trade.tp_price,
            pnl_pips=round(pnl, 1),
            peak_pnl_pips=round(trade.peak_pnl_pips, 1),
            worst_pnl_pips=round(trade.worst_pnl_pips, 1),
            duration_minutes=round(duration, 1),
            entry_conviction=trade.entry_conviction,
            session=getattr(trade, "_session", ""),
            is_win=pnl > 0,
            entry_type=getattr(trade, "entry_type", "standard"),
        )

        self._journal.append(record)
        journal_idx = len(self._journal) - 1

        # Close the trade in the tracker
        self._tracker.close_trade(pair, reason=reason, close_price=close_price)

        # Start post-close observation (4h window)
        self._post_close_watching[journal_idx] = True

        logger.info(
            "[PAPER] Closed %s %s — reason=%s  P/L=%.1f pips  duration=%.0f min  "
            "peak=%.1f  worst=%.1f  (watching 4h post-close)",
            trade.direction, pair, reason, pnl, duration,
            trade.peak_pnl_pips, trade.worst_pnl_pips,
        )
        return record

    # ── Active Paper Trades ─────────────────────────────────────────

    def get_active_paper_trades(self) -> dict[str, TrackedTrade]:
        """Return all active paper trades."""
        return {
            p: t for p, t in self._tracker.active_trades.items()
            if t.is_paper
        }

    @property
    def active_count(self) -> int:
        return len(self.get_active_paper_trades())

    # ── Stats ───────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """Compute summary statistics from journal."""
        if not self._journal:
            return {
                "total": 0, "wins": 0, "losses": 0,
                "wr": 0.0, "total_pnl": 0.0, "avg_pnl": 0.0,
                "avg_win": 0.0, "avg_loss": 0.0,
                "avg_duration": 0.0,
                "sl_hits": 0, "tp_hits": 0, "signal_exits": 0,
            }

        wins = [r for r in self._journal if r.is_win]
        losses = [r for r in self._journal if not r.is_win]
        total = len(self._journal)

        return {
            "total": total,
            "wins": len(wins),
            "losses": len(losses),
            "wr": round(len(wins) / total * 100, 1) if total else 0.0,
            "total_pnl": round(sum(r.pnl_pips for r in self._journal), 1),
            "avg_pnl": round(sum(r.pnl_pips for r in self._journal) / total, 1),
            "avg_win": round(sum(r.pnl_pips for r in wins) / len(wins), 1) if wins else 0.0,
            "avg_loss": round(sum(r.pnl_pips for r in losses) / len(losses), 1) if losses else 0.0,
            "avg_duration": round(
                sum(r.duration_minutes for r in self._journal) / total, 1
            ),
            "sl_hits": sum(1 for r in self._journal if r.close_reason == "sl_hit"),
            "tp_hits": sum(1 for r in self._journal if r.close_reason == "tp_hit"),
            "signal_exits": sum(1 for r in self._journal if r.close_reason == "signal_exit"),
        }

    # ── Persistence ─────────────────────────────────────────────────

    def save_journal(self) -> None:
        """Save the complete journal to disk."""
        self._journal_path.parent.mkdir(parents=True, exist_ok=True)
        data = [asdict(r) for r in self._journal]
        try:
            self._journal_path.write_text(
                json.dumps(data, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            logger.info("Paper journal saved: %d trades", len(data))
        except OSError as e:
            logger.warning("Failed to save paper journal: %s", e)

    def load_journal(self) -> int:
        """Load journal from disk on startup.

        Returns number of records loaded.
        """
        if not self._journal_path.exists():
            return 0
        try:
            data = json.loads(
                self._journal_path.read_text(encoding="utf-8")
            )
            for d in data:
                r = PaperTradeRecord()
                for k, v in d.items():
                    if hasattr(r, k):
                        setattr(r, k, v)
                self._journal.append(r)

            # Restore post-close watching for incomplete observations
            now = time.time()
            for idx, r in enumerate(self._journal):
                if not r.post_close_complete and r.close_time > 0:
                    elapsed_h = (now - r.close_time) / 3600.0
                    if elapsed_h < self.POST_CLOSE_HOURS:
                        # Still within 4h window — keep watching
                        self._post_close_watching[idx] = True
                    else:
                        # Past 4h window — mark complete with whatever we have
                        r.post_close_complete = True
                        r.post_close_minutes = round(elapsed_h * 60, 1)
                        logger.info(
                            "[PAPER] Post-close expired on reload: %s %s "
                            "(closed %.1fh ago, marking complete)",
                            r.direction, r.pair, elapsed_h,
                        )

            watching = len(self._post_close_watching)
            logger.info(
                "Paper journal loaded: %d trades (%d still observing post-close)",
                len(self._journal), watching,
            )
            return len(self._journal)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to load paper journal: %s", e)
            return 0

    @property
    def journal(self) -> list[PaperTradeRecord]:
        return list(self._journal)
