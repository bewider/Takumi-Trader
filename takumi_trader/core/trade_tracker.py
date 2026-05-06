"""Active Trade Tracker — entry recording, P/L calculation (Phase 7.8).

Tracks manually initiated trades (user clicks TRACK on a pair alert).
Records entry price, direction, currency scores at entry, and computes
live P/L in pips. Feeds into the exit engine for vote-based exit signals.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Pip values
_PIP = {"JPY": 0.01}
_DEFAULT_PIP = 0.0001
# 2026-04-24: Gold (XAUUSD) is NOT a currency pair — it's a commodity quoted
# in USD/oz at 0.01 precision on IC Markets. Using pip_value = 0.01 means
# "1 pip = $0.01 per oz" which matches how brokers display gold P/L (a
# 20-pip SL = $0.20/oz move ≈ typical noise floor). The strength engine
# does NOT see XAUUSD — gold data flows on a separate channel
# (result.xau_candles) so CSI calculations are untouched.
_GOLD_SYMBOLS = {"XAUUSD", "XAUUSD.raw", "XAUUSDm", "XAUUSD.cash",
                 "XAUUSD.s", "XAUUSD.z"}


def pip_value(pair: str) -> float:
    """Get pip value for a pair.

    For forex: 0.01 if JPY quote else 0.0001.
    For gold (any XAUUSD variant): 0.01 — matches how IC Markets quotes gold
    (0.01/oz precision) and how most brokers display gold P/L in "points".
    """
    if pair in _GOLD_SYMBOLS or pair.startswith("XAU"):
        return 0.01
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
    close_reason: str = ""          # "sl_hit", "tp_hit", "signal_exit", "manual", "weekend_close"
    entry_type: str = "stoch_v2"    # "stoch_v2", "sv2_ss", or "sv2_atr"
    close_time: float = 0.0         # time.time() at close
    close_price: float = 0.0        # Price at close

    # Metadata (persisted across restarts)
    session: str = ""               # Trading session at entry (London, US, etc.)
    qm4_alert_type: str = ""        # For QM4 system: "MTF", "CUM", "PAIR/MTF", etc.
    adr_consumed_pct: float = 0.0   # ADR consumed % at entry
    # For DTC-combo trades: which source system signaled this trade
    # ("sv2_ss", "sv2_atr", or "sv2_b_tuned"). Empty for non-DTC trades.
    dtc_source_system: str = ""

    # Entry signal data (captured at trade open for diagnostics)
    entry_m5_base: float = 0.0
    entry_m5_quote: float = 0.0
    entry_m15_base: float = 0.0
    entry_m15_quote: float = 0.0
    entry_h1_base: float = 0.0
    entry_h1_quote: float = 0.0
    entry_h4_base: float = 0.0
    entry_h4_quote: float = 0.0
    # QM4-specific high-timeframe scores (also useful diagnostically for Sv2)
    entry_d1_base: float = 0.0
    entry_d1_quote: float = 0.0
    entry_w1_base: float = 0.0
    entry_w1_quote: float = 0.0
    entry_mn_base: float = 0.0
    entry_mn_quote: float = 0.0
    entry_alignment_count: int = 0  # # of 6 TFs (M15..MN) at extreme (≤2 or ≥8)
    entry_div_spread: float = 0.0   # composite base - quote
    entry_spread_std: float = 0.0   # spread stability StdDev
    entry_h1_atr_pips: float = 0.0  # H1 ATR in pips
    entry_structural: str = ""      # "OK" or block reason
    entry_tier: str = ""            # "FULL", "DIMMED", "SUPPRESSED"

    # Deep analytics context (captured at entry)
    entry_tick_volume_ratio: float = 0.0    # current M1 volume / 15-bar avg
    entry_momentum_buildup_sec: int = 0     # seconds since signal first qualified
    entry_dist_day_high_pips: float = 0.0   # signed pips (positive = above price)
    entry_dist_day_low_pips: float = 0.0
    entry_dist_week_high_pips: float = 0.0
    entry_dist_week_low_pips: float = 0.0
    entry_dist_month_high_pips: float = 0.0
    entry_dist_month_low_pips: float = 0.0
    entry_cluster_count: int = 0            # same base/quote pairs also signaling
    entry_dist_00_pips: float = 0.0         # signed pips to nearest 100-pip round (e.g., 1.2100)
    entry_dist_000_pips: float = 0.0        # signed pips to nearest 1000-pip round (e.g., 1.2000)
    entry_session_minutes_in: int = 0       # minutes since session started
    entry_day_of_week: int = 0              # 0=Mon, 6=Sun (JST)
    entry_prev_trade_result: str = ""       # "win"/"loss"/""
    entry_concurrent_trades: int = 0        # active trades across all systems
    entry_m1_body_pct: float = 0.0          # M1 candle body % of range
    entry_m1_direction: str = ""            # "bull"/"bear"/"doji"
    entry_atr_ratio: float = 0.0            # H1 ATR / 20-bar avg

    # ── Conviction breakdown (4 sub-scores out of 30/20/20/15) ──
    entry_conv_trend: int = 0          # trend_regime score (0-30)
    entry_conv_velocity: int = 0       # velocity score (0-20)
    entry_conv_isolation: int = 0      # isolation score (0-20)
    entry_conv_structural: int = 0     # structural score (0-15)

    # ── Pair-specific SL/TP ATR multipliers ──
    entry_sl_atr_mult: float = 0.0     # SL = this × H1_ATR
    entry_tp_atr_mult: float = 0.0     # TP = this × H1_ATR

    # ── Currency-specific context ──
    entry_strong_ccy: str = ""         # "USD", "EUR", etc.
    entry_weak_ccy: str = ""
    entry_strong_rank: int = 0         # 1-8 (1=strongest of 8)
    entry_weak_rank: int = 0           # 1-8 (8=weakest of 8)
    entry_strong_top_gap: float = 0.0  # gap between #1 and #2
    entry_weak_bottom_gap: float = 0.0 # gap between #7 and #8
    entry_strong_velocity: float = 0.0 # points/min
    entry_weak_velocity: float = 0.0   # points/min

    # ── ATR slope (Sv2+ATR signal, computed for all) ──
    entry_m5_tr_slope_ratio: float = 0.0  # last-3-bar TR avg / prev-3-bar TR avg

    # ── News timing ──
    entry_minutes_since_news: float = -1.0  # minutes since last RED news on either ccy

    # ── Alt system signal data (BREAKOUT / SQUEEZE / DIVERGENCE) ──
    entry_alt_signal_1: float = 0.0  # BRK: range_pips, SQZ: squeeze_bars, DIV: z_score
    entry_alt_signal_2: float = 0.0  # BRK: breakout_dist, SQZ: bb_width/kc_width ratio, DIV: ratio_vs_mean
    entry_alt_signal_3: str = ""     # BRK: n/a, SQZ: n/a, DIV: pair_group ("AUDUSD/NZDUSD")
    entry_alt_signal_4: float = 0.0  # BRK: asian_high, SQZ: sma_distance_pips, DIV: std_of_ratio

    # ── Extended Squeeze-specific context (added 2026-04-20) ──
    # Populated only when entry_type=="squeeze"; other systems leave at defaults.
    # Purpose: future hypothesis-driven filter tuning. Ignore in analysis
    # queries if entry_type != "squeeze".
    sqz_bb_kc_ratio_min: float = 0.0         # tightest BB/KC ratio during squeeze
    sqz_bb_width_pips_release: float = 0.0   # BB width in pips at release moment
    sqz_bb_width_min_pips: float = 0.0       # tightest BB width (pips) during squeeze
    sqz_real_age_bars: int = 0               # M15 bars from squeeze start to release
    sqz_dist_to_upper_bb_pips: float = 0.0   # signed; > 0 = upper BB above close
    sqz_dist_to_lower_bb_pips: float = 0.0   # signed; > 0 = close above lower BB
    sqz_close_pos_in_kc: float = -1.0        # 0..1 = measured (0=at lower KC, 1=at upper KC); -1 = not measured (non-squeeze trade). (Fix BUG #9)
    sqz_atr_ratio_during: float = 0.0        # ATR at release / ATR at squeeze start
    sqz_touches_count: int = 0               # M15 bars during squeeze with ratio >= 0.9 (BB near KC edge, shallow)
    sqz_concurrent_count: int = 0            # # OTHER pairs in squeeze at release moment

    # ── Broker spread cost (paper trades only) ──
    entry_spread_price: float = 0.0  # ask - bid at entry, in price units

    # ── NEW momentum / trend-start signals (added 2026-04-20) ──
    # Captured ONLY for analysis — never used as a trade gate.
    # Default values mean "not measured" (history too shallow, MT5 lookup
    # missed, or record imported from before this field existed).
    entry_m1_consec_aligned: int = 0           # +N = N M1 bars aligned with trade dir, -N = counter
    entry_composite_vel_90s: float = 0.0       # rate-of-change of div spread (pts/min) over last ~90s
    entry_m5_higher_highs: bool = False        # last 3 M5 bars form HH (for BUY) or LH (for SELL)
    entry_m5_higher_lows: bool = False         # last 3 M5 bars form HL (for BUY) or LL (for SELL)
    entry_vwap_dist_pips: float = 0.0          # signed; >0 = above session VWAP
    entry_adx_h1: float = 0.0                  # ADX(14) on H1 — trend strength (>25 trending)
    entry_bb_position_m15: float = 0.5         # 0=lower band, 0.5=middle, 1=upper band (20-bar, 2σ)
    entry_bb_width_ratio_m15: float = 0.0      # current BB width / BB width 5 bars ago
    entry_tick_flow_bias: float = 0.0          # -1..+1 from TickFlowTracker (sell flow vs buy flow)
    entry_volume_ramp_5m: float = 0.0          # sum(last 5 M1 vol) / sum(prev 10 M1 vol); >1.5 = ramping
    entry_range_compression: float = 0.0       # std(last 10 M1 closes) / std(last 30); <0.5 = compression
    entry_cross_pair_confirm: int = 0          # # of other pairs sharing a ccy that also fired this cycle
    entry_session_vol_pct: float = 0.0         # percentile of current session's volume vs historical avg
    entry_m5_close_strength: float = 0.5       # M5 last-bar close position in its range, in TRADE direction
                                               # (0=bar closed against us, 1=closed at extreme in our favor)

    # Trade journey (tracked during update_cycle)
    time_to_5p_profit_min: float = -1.0     # minutes to first +5p (-1 if never)
    went_profit_first: bool = False         # reached +5p before -5p
    near_sl_count: int = 0                  # times within 2 pips of SL
    near_tp_count: int = 0                  # times within 2 pips of TP
    bars_to_close: int = 0                  # M1 bars from entry to close

    # 2026-04-23: BE-stop state for Sv2-upgraded system. When the trade's peak
    # favorable excursion reaches +7 pips, SL is moved to break-even and this
    # flag is set so the move only happens once. Preserved across restarts via
    # the generic load_from_file setattr loop.
    be_moved: bool = False

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

        BUG FIX (2026-04-21): this method previously listed every field by
        hand in the TrackedTrade(...) constructor call. Any new field added
        to TrackedTrade was silently lost on restart — notably
        `dtc_source_system` (which caused 25 of 32 DTC trades to lose their
        source tag). Now uses generic dataclass-field introspection so ALL
        fields are restored automatically, and new fields added to
        TrackedTrade in future will persist across restart without needing
        to touch this method.
        """
        p = Path(path)
        if not p.exists():
            return 0
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            count = 0
            # Enumerate TrackedTrade's fields ONCE (cheap: just names)
            _valid_fields = {f.name for f in fields(TrackedTrade)}
            for d in data:
                if not d.get("active", False):
                    continue
                # Required fields (no defaults on TrackedTrade): pair,
                # direction, entry_price, entry_time. Construct with those
                # four, then overlay every other persisted field via
                # setattr. Anything in the JSON that isn't a TrackedTrade
                # field is silently ignored (forward-compat: old files
                # with now-removed fields still load).
                try:
                    trade = TrackedTrade(
                        pair=d["pair"],
                        direction=d["direction"],
                        entry_price=d["entry_price"],
                        entry_time=d["entry_time"],
                    )
                except KeyError as _k:
                    logger.warning(
                        "Skipping trade with missing required field %s in %s",
                        _k, path,
                    )
                    continue
                _skip = {"pair", "direction", "entry_price", "entry_time"}
                for k, v in d.items():
                    if k in _valid_fields and k not in _skip:
                        try:
                            setattr(trade, k, v)
                        except Exception as _sf_exc:
                            logger.debug(
                                "Could not restore field %s on %s: %s",
                                k, trade.pair, _sf_exc,
                            )
                # Legacy compat: some older dumps prefixed a few fields with
                # an underscore. Map them through if the clean name wasn't set.
                for legacy, modern in (
                    ("_session", "session"),
                    ("_qm4_alert_type", "qm4_alert_type"),
                    ("_adr_consumed_pct", "adr_consumed_pct"),
                ):
                    if legacy in d and modern in _valid_fields and not getattr(trade, modern, None):
                        try:
                            setattr(trade, modern, d[legacy])
                        except Exception:
                            pass
                trade.active = True
                # If current_price wasn't in the saved dict, default to entry
                if trade.current_price == 0.0:
                    trade.current_price = trade.entry_price
                self._trades[trade.pair] = trade
                count += 1
            logger.info("Restored %d trades from %s", count, path)
            return count
        except Exception:
            logger.exception("Failed to load trades from %s", path)
            return 0
