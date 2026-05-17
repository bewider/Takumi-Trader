"""Main application window: table, alert list, compact toggle, status bar."""

from __future__ import annotations

import json
import logging
import math
import time
from collections import deque
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

from PyQt6.QtCore import QByteArray, QPoint, QSettings, Qt
from PyQt6.QtGui import QAction, QColor, QFont, QIcon
from PyQt6.QtWidgets import (
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMainWindow,
    QMenu,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

from takumi_trader.core.alert_performance import AlertPerformanceTracker
from takumi_trader.core.alerts import AlertManager

try:
    from takumi_trader.core.ctrader_position_manager import CTraderPositionManager
    from takumi_trader.core.ctrader_worker import CTraderBridge
    _CTRADER_AVAILABLE = True
except Exception:
    _CTRADER_AVAILABLE = False
from takumi_trader.core.exit_engine import ExitEngine
from takumi_trader.core.paper_trader import PaperTrader
from takumi_trader.core.filter_engine import ConvictionResult, FilterEngine
from takumi_trader.core.mt5_worker import MT5Worker
from takumi_trader.core.range_engine import RangeState
from takumi_trader.core.session_manager import get_current_session
from takumi_trader.core.strength import (
    CURRENCIES,
    DISPLAY_PAIRS,
    TIMEFRAME_LABELS,
    CalculationResult,
)
from takumi_trader.core.target_calculator import calculate_dynamic_target
from takumi_trader.core.trade_tracker import TradeTracker
from takumi_trader.ui.filter_toolbar import FilterToolbar
from takumi_trader.ui.health_alerts import HealthAlerts, StalenessWatchdog
from takumi_trader.ui.settings_dialog import SettingsDialog, load_settings

logger = logging.getLogger(__name__)

# Stoch v2 display timeframes (no M1 — too noisy)
_DISPLAY_TFS = ["M5", "M15", "H1", "H4", "D1"]
_NUM_TF = len(_DISPLAY_TFS)       # 5: M5, M15, H1, H4, D1
_TOTAL_COLS = 2 + _NUM_TF         # 7: Pair | Range | M5 | M15 | H1 | H4 | D1
_RANGE_COL = 1                    # Column index for the Range% column
_TF_COL_OFFSET = 2                # Timeframe columns start at index 2

# Color thresholds for cell backgrounds (light theme)
_CELL_STYLES: list[tuple[float, str, str]] = [
    # (min_score, bg_color, text_color)
    (9.0, "#00c853", "#ffffff"),
    (6.0, "#43a047", "#ffffff"),
    (3.0, "#c8e6c9", "#1b5e20"),
    (-2.9, "#f0f0f0", "#555555"),
    (-5.9, "#ffcdd2", "#b71c1c"),
    (-8.9, "#e53935", "#ffffff"),
]
_EXTREME_NEG = ("#d50000", "#ffffff")

# Arrow colors
_ARROW_UP = ("\u2191", "#2e7d32")
_ARROW_DOWN = ("\u2193", "#c62828")
_ARROW_FLAT = ("\u2192", "#9e9e9e")

# Max alert history entries visible
_MAX_ALERT_HISTORY = 30

# Persistence file paths — next to the exe (frozen) or project root (dev)
import sys as _sys
if getattr(_sys, "frozen", False):
    _DATA_DIR = Path(_sys.executable).parent / "data"
else:
    _DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
_TRADES_FILE = _DATA_DIR / "tracked_trades.json"
_ALERTS_FILE = _DATA_DIR / "alert_history.json"
_OUTCOMES_FILE = _DATA_DIR / "alert_outcomes.json"
_ACTIVE_PERF_FILE = _DATA_DIR / "active_perf_alerts.json"
_CTRADER_POS_FILE = _DATA_DIR / "ctrader_positions.json"
_PAPER_TRADES_FILE = _DATA_DIR / "paper_trades.json"
_CSI_LOG_FILE = _DATA_DIR / "csi_alert_log.json"

# ── System D (QM4) kill switch (added 2026-05-14) ──
# QM4 inputs (CSI scores) are temporarily unavailable per operator.
# Setting this False prevents NEW QM4 paper trades from opening but
# leaves existing QM4 paper-trader state intact:
#   - update_cycle() still runs on QM4 paper trader to close any
#     existing open positions via SL/TP monitoring
#   - QM4 alert engine (_qm4_engine) still receives CSI scores (when
#     they arrive) and emits alerts to the alerts panel for operator
#     visibility — only the trade-OPENING branch is gated
#   - PerformanceDialog still shows historical QM4 stats correctly
# To re-enable: flip this back to True. No other code changes needed.
_QM4_TRADING_ENABLED = False

# ── cTrader non-retryable error codes (added 2026-05-14) ──
# Server-side refusals where retrying within seconds is wasteful and
# produces alert cascades. When the order-error handler sees one of
# these codes, it SKIPS clearing the pair's position lock — so the
# strategy can't immediately re-fire on the next cycle. Position lock
# releases naturally when (a) the paper trade closes, or (b) the next
# legitimate signal cycle for that pair triggers (M5 close + fresh
# entry conditions).
#
# Codes intentionally left out: timeouts, auth-races, network glitches
# — those genuinely benefit from quick retry on the next cycle.
_CT_NON_RETRYABLE_CODES: tuple[str, ...] = (
    "CANT_ROUTE_REQUEST",      # cTrader can't route the order (symbol
                                # disabled, broker routing blocked, etc.)
    "SYMBOL_NOT_FOUND",         # account doesn't have this symbol
    "SYMBOL_NOT_ENABLED",       # symbol present but trading disabled
    "MARKET_CLOSED",            # session closed for this symbol
    "INVALID_VOLUME",           # lot size rejected by broker
    "BLOCKED_PAYLOAD_TYPE",     # rate limited (also handled upstream
                                # in ctrader_worker; defensive)
    "ORDER_BLOCKED",            # broker-level risk control
)


def _ct_is_non_retryable_error(error: str) -> bool:
    """Classify a cTrader error message as non-retryable (server-refusal).

    Substring match — error strings arrive in multiple shapes:
      * "CANT_ROUTE_REQUEST: Cannot route request" (generic handler)
      * "Order rejected: CANT_ROUTE_REQUEST: ..." (order-callback path)
    Returns True iff one of _CT_NON_RETRYABLE_CODES appears anywhere
    in the message.
    """
    return any(code in error for code in _CT_NON_RETRYABLE_CODES)
_TRADES_FILE_SS = _DATA_DIR / "tracked_trades_ss.json"
_TRADES_FILE_ATR = _DATA_DIR / "tracked_trades_atr.json"
_TRADES_FILE_QM4 = _DATA_DIR / "tracked_trades_qm4.json"
_TRADES_FILE_A_TUNED = _DATA_DIR / "tracked_trades_a_tuned.json"
_TRADES_FILE_B_TUNED = _DATA_DIR / "tracked_trades_b_tuned.json"
_TRADES_FILE_BREAKOUT = _DATA_DIR / "tracked_trades_breakout.json"
_TRADES_FILE_SQUEEZE = _DATA_DIR / "tracked_trades_squeeze.json"
_TRADES_FILE_SQUEEZE_REV = _DATA_DIR / "tracked_trades_squeeze_rev.json"
_TRADES_FILE_DIVERGENCE = _DATA_DIR / "tracked_trades_divergence.json"
_TRADES_FILE_DTC_COMBO = _DATA_DIR / "tracked_trades_dtc_combo.json"
# Live-candle system tracker files (2026-04-21)
_TRADES_FILE_SV2_LIVE = _DATA_DIR / "tracked_trades_sv2_live.json"
_TRADES_FILE_A_TUNED_LIVE = _DATA_DIR / "tracked_trades_sv2_a_tuned_live.json"
_TRADES_FILE_SS_LIVE = _DATA_DIR / "tracked_trades_sv2_ss_live.json"
_TRADES_FILE_B_TUNED_LIVE = _DATA_DIR / "tracked_trades_sv2_b_tuned_live.json"
_TRADES_FILE_ATR_LIVE = _DATA_DIR / "tracked_trades_sv2_atr_live.json"
# Sv2-upgraded (2026-04-23): live-candle engine + conv≥65 + revenge cooldown
# + BE-stop at +7p peak (opt-outs EURUSD/GBPUSD/NZDUSD/GBPCAD).
_TRADES_FILE_SV2_UPGRADED = _DATA_DIR / "tracked_trades_sv2_upgraded.json"
# AU Gold suite (2026-04-24) — 5 XAUUSD strategies, paper-only, completely
# isolated from the forex CSI pipeline (see core/au_gold_systems.py).
_TRADES_FILE_AU1 = _DATA_DIR / "tracked_trades_au1_london.json"
_TRADES_FILE_AU2 = _DATA_DIR / "tracked_trades_au2_ny_orb.json"
_TRADES_FILE_AU3 = _DATA_DIR / "tracked_trades_au3_pullback.json"
_TRADES_FILE_AU4 = _DATA_DIR / "tracked_trades_au4_divergence.json"
_TRADES_FILE_AU5 = _DATA_DIR / "tracked_trades_au5_mean_rev.json"

# ── Quality filter: ADR consumption ceiling ──
# Trades with adr_consumed_pct ABOVE this threshold are blocked at entry
# for systems where it has been validated to improve results.
# Validated improvements (raw history, 3% compound, ICMarkets commission):
#   Sv2 (A):     $863  → $1,101  (+28%)
#   A-tuned (D): $1,189 → $1,480  (+24%)
# NOT applied to SS / ATR / B-tuned / DTC because those use the more
# precise per-pair blacklist that captures the same trades better.
# Set to None to disable. Set to a float <= 100 to block above that %.
_ADR_QUALITY_MAX_PCT: float | None = 70.0
_ADR_QUALITY_SYSTEMS: tuple = ("Sv2", "A-tuned")  # which systems gate on ADR

# Paper trade exit: same spread threshold as backtester._check_exit()
# Exit when base/quote composite spread drops below this value.
_PAPER_EXIT_SPREAD_THRESHOLD = 4.0
# Spread-collapse confirmation: require N consecutive readings below threshold
# before actually closing. Prevents premature exits on momentary dips.
_SPREAD_COLLAPSE_CONFIRMATIONS = 3  # consecutive below-threshold checks needed

# Per-TF alert thresholds for currency strength
# Stricter: +1.0 across all TFs for higher quality entries
_ALERT_THRESHOLDS: dict[str, float] = {
    "M1": 6.5,
    "M5": 6.0,
    "M15": 5.5,
    "H1": 5.0,
}

# Minimum composite divergence spread to trigger alert (base_ccy - quote_ccy)
_MIN_DIVERGENCE_SPREAD = 12.0


def _cell_colors(score: float) -> tuple[str, str]:
    """Return (bg_color, text_color) for a given pair score (-10 to +10 scale)."""
    for threshold, bg, fg in _CELL_STYLES:
        if score >= threshold:
            return bg, fg
    return _EXTREME_NEG


def _cell_colors_stoch(score: float) -> tuple[str, str]:
    """Return (bg_color, text_color) for Stoch v2 currency score (0-10 scale).

    Only highlight clear extremes: 8.5-9.9 green, 0.1-1.5 red.
    Everything in between stays neutral.
    """
    if score >= 8.5:
        return "#1a8a1a", "#ffffff"   # green (extreme strong)
    elif score <= 1.5:
        return "#c62828", "#ffffff"   # red (extreme weak)
    else:
        return "#f0f0f0", "#555555"   # neutral


class MainWindow(QMainWindow):
    """Primary application window with data table and alert list."""

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("TAKUMI Trader")
        self.resize(820, 1060)

        # Set window icon (works for taskbar + title bar)
        import os, sys
        if getattr(sys, 'frozen', False):
            base = sys._MEIPASS
        else:
            base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        icon_path = os.path.join(base, "resources", "app_icon.ico")
        if os.path.isfile(icon_path):
            self.setWindowIcon(QIcon(icon_path))

        # State
        self._compact = False
        self._drag_pos: QPoint | None = None

        # Score history for pairs (momentum arrows)
        self._score_history: dict[str, dict[str, deque]] = {}
        for pair in DISPLAY_PAIRS:
            self._score_history[pair] = {
                tf: deque(maxlen=5) for tf in _DISPLAY_TFS
            }

        # Score history for individual currencies (momentum arrows)
        _CCY_TFS = list(_DISPLAY_TFS)
        self._ccy_score_history: dict[str, dict[str, deque]] = {}
        for ccy in CURRENCIES:
            self._ccy_score_history[ccy] = {
                tf: deque(maxlen=5) for tf in _CCY_TFS
            }

        # Alert history (most recent first, capped at _MAX_ALERT_HISTORY)
        self._alert_history: deque[str] = deque(maxlen=_MAX_ALERT_HISTORY)
        self._load_alert_history()

        # Session key tracking — prevents re-entry on same pair+direction
        # within the same session. Only used for QM4 (data shows re-entries
        # are profitable for Sv2-based systems A/B/C but hurt QM4).
        self._session_keys_qm4: set[str] = set()  # System D (QM4) only
        self._last_session: str = ""
        self._prev_tuned_pairs: set[str] = set()  # transition detection for tuned systems

        # Alert manager
        settings = load_settings()
        self._alert_mgr = AlertManager(cooldown_seconds=settings["cooldown_seconds"])
        self._alert_mgr.sound_enabled = settings["sound_enabled"]
        self._alert_mgr.sound_file = settings["sound_file"]
        self._font_size = settings["font_size"]

        # Filter engine
        self._filter_engine = FilterEngine()

        # News filter (RED news hard block for paper trading)
        from takumi_trader.core.news_filter import NewsFilter
        self._news_filter = NewsFilter()
        self._news_filter.load_cache()  # load from disk (may be empty)

        # Diagnostic recorder (per-minute algo state logging)
        from takumi_trader.core.diagnostic_recorder import DiagnosticRecorder
        self._diagnostic_recorder = DiagnosticRecorder()

        # Trade tracking + exit engine
        self._trade_tracker = TradeTracker(max_trades=28)
        self._trade_tracker.load_from_file(_TRADES_FILE)
        self._exit_engine = ExitEngine()
        self._alert_perf = AlertPerformanceTracker()
        self._alert_perf.load_active(_ACTIVE_PERF_FILE)
        self._paper_trader = PaperTrader(
            trade_tracker=self._trade_tracker,
            journal_path=_PAPER_TRADES_FILE,
        )
        self._paper_trader.load_journal()

        # ── Parallel system B: Sv2 + Spread Stability filter ──
        from takumi_trader.core.trade_tracker import TradeTracker as _TT2
        self._trade_tracker_ss = _TT2(max_trades=28)
        _SS_JOURNAL = _DATA_DIR / "paper_trades_ss.json"
        self._paper_trader_ss = PaperTrader(
            trade_tracker=self._trade_tracker_ss,
            journal_path=_SS_JOURNAL,
        )
        self._trade_tracker_ss.load_from_file(_TRADES_FILE_SS)
        self._paper_trader_ss.load_journal()

        # ── Parallel system C: Sv2 + ATR Expansion filter ──
        from takumi_trader.core.trade_tracker import TradeTracker as _TT3
        self._trade_tracker_atr = _TT3(max_trades=28)
        _ATR_JOURNAL = _DATA_DIR / "paper_trades_atr.json"
        self._paper_trader_atr = PaperTrader(
            trade_tracker=self._trade_tracker_atr,
            journal_path=_ATR_JOURNAL,
        )
        self._trade_tracker_atr.load_from_file(_TRADES_FILE_ATR)
        self._paper_trader_atr.load_journal()

        # ── System D: QM4 CSI-based trading ──
        from takumi_trader.core.trade_tracker import TradeTracker as _TT4
        self._trade_tracker_qm4 = _TT4(max_trades=28)
        _QM4_JOURNAL = _DATA_DIR / "paper_trades_qm4.json"
        self._paper_trader_qm4 = PaperTrader(
            trade_tracker=self._trade_tracker_qm4,
            journal_path=_QM4_JOURNAL,
        )
        self._trade_tracker_qm4.load_from_file(_TRADES_FILE_QM4)
        self._paper_trader_qm4.load_journal()

        # ── System A-tuned: Sv2 (tuned variant) ──
        from takumi_trader.core.trade_tracker import TradeTracker as _TT5
        self._trade_tracker_a_tuned = _TT5(max_trades=28)
        _A_TUNED_JOURNAL = _DATA_DIR / "paper_trades_a_tuned.json"
        self._paper_trader_a_tuned = PaperTrader(
            trade_tracker=self._trade_tracker_a_tuned,
            journal_path=_A_TUNED_JOURNAL,
        )
        self._trade_tracker_a_tuned.load_from_file(_TRADES_FILE_A_TUNED)
        self._paper_trader_a_tuned.load_journal()

        # ── System B-tuned: Sv2+SS (tuned variant) ──
        from takumi_trader.core.trade_tracker import TradeTracker as _TT6
        self._trade_tracker_b_tuned = _TT6(max_trades=28)
        _B_TUNED_JOURNAL = _DATA_DIR / "paper_trades_b_tuned.json"
        self._paper_trader_b_tuned = PaperTrader(
            trade_tracker=self._trade_tracker_b_tuned,
            journal_path=_B_TUNED_JOURNAL,
        )
        self._trade_tracker_b_tuned.load_from_file(_TRADES_FILE_B_TUNED)
        self._paper_trader_b_tuned.load_journal()

        # ── System E: BREAKOUT (Session Range Breakout) ──
        from takumi_trader.core.trade_tracker import TradeTracker as _TT7
        self._trade_tracker_breakout = _TT7(max_trades=28)
        _BREAKOUT_JOURNAL = _DATA_DIR / "paper_trades_breakout.json"
        self._paper_trader_breakout = PaperTrader(
            trade_tracker=self._trade_tracker_breakout,
            journal_path=_BREAKOUT_JOURNAL,
        )
        self._trade_tracker_breakout.load_from_file(_TRADES_FILE_BREAKOUT)
        self._paper_trader_breakout.load_journal()

        # ── System F: SQUEEZE (Bollinger + Keltner) ──
        from takumi_trader.core.trade_tracker import TradeTracker as _TT8
        self._trade_tracker_squeeze = _TT8(max_trades=28)
        _SQUEEZE_JOURNAL = _DATA_DIR / "paper_trades_squeeze.json"
        self._paper_trader_squeeze = PaperTrader(
            trade_tracker=self._trade_tracker_squeeze,
            journal_path=_SQUEEZE_JOURNAL,
        )
        self._trade_tracker_squeeze.load_from_file(_TRADES_FILE_SQUEEZE)
        self._paper_trader_squeeze.load_journal()

        # ── System F-REV: SQUEEZE-REV (inverse-direction mirror of Squeeze) ──
        # For every Squeeze signal that opens a paper trade, this system opens
        # an OPPOSITE-direction paper trade with the same SL/TP distances. Same
        # entry timing, same metadata, mirrored direction. Net P/L should be
        # roughly -original - (2 x spread_cost). Paper-only.
        from takumi_trader.core.trade_tracker import TradeTracker as _TT8R
        self._trade_tracker_squeeze_rev = _TT8R(max_trades=28)
        _SQUEEZE_REV_JOURNAL = _DATA_DIR / "paper_trades_squeeze_rev.json"
        self._paper_trader_squeeze_rev = PaperTrader(
            trade_tracker=self._trade_tracker_squeeze_rev,
            journal_path=_SQUEEZE_REV_JOURNAL,
        )
        self._trade_tracker_squeeze_rev.load_from_file(_TRADES_FILE_SQUEEZE_REV)
        self._paper_trader_squeeze_rev.load_journal()

        # ── System G: DIVERGENCE (Correlation Mean Reversion) ──
        from takumi_trader.core.trade_tracker import TradeTracker as _TT9
        self._trade_tracker_divergence = _TT9(max_trades=28)
        _DIVERGENCE_JOURNAL = _DATA_DIR / "paper_trades_divergence.json"
        self._paper_trader_divergence = PaperTrader(
            trade_tracker=self._trade_tracker_divergence,
            journal_path=_DIVERGENCE_JOURNAL,
        )
        self._trade_tracker_divergence.load_from_file(_TRADES_FILE_DIVERGENCE)
        self._paper_trader_divergence.load_journal()

        # ── Health-alert popup manager ──
        # Centralised non-blocking popups for any subsystem failure
        # (MT5 disconnect, cTrader disconnect, order rejection, save fail,
        # config load error, stale data feed). Per-key cooldown so flapping
        # connections don't spam. Created BEFORE DTC config + workers so
        # any startup error has somewhere to be reported.
        self._health_alerts = HealthAlerts(self)

        # ── DTC-combo: filtered aggregate of SS/ATR/B-tuned ──
        # Takes signals from the 3 currency-strength systems, applies each
        # system's optimised per-pair + per-time blacklist, then same-pair
        # dedup within a configurable window. Rules live in
        # data/dtc_combo_config.json so they can be tuned weekly without
        # code changes.
        from takumi_trader.core.trade_tracker import TradeTracker as _TT10
        self._trade_tracker_dtc_combo = _TT10(max_trades=28)
        _DTC_COMBO_JOURNAL = _DATA_DIR / "paper_trades_dtc_combo.json"
        self._paper_trader_dtc_combo = PaperTrader(
            trade_tracker=self._trade_tracker_dtc_combo,
            journal_path=_DTC_COMBO_JOURNAL,
        )
        self._trade_tracker_dtc_combo.load_from_file(_TRADES_FILE_DTC_COMBO)
        self._paper_trader_dtc_combo.load_journal()

        # ── Live-candle paper-only systems (2026-04-21) ──
        # Mirror A/B/C/D/E but use the live-candle engine (stoch_scores_live +
        # stoch_entry_candidates_live fields on the worker result). These let
        # us A/B-compare "compute on candle close" (existing systems) vs
        # "compute every cycle with forming bars" (new live systems). Same
        # signal filter chain as the originals. Paper-only, no cTrader.
        from takumi_trader.core.trade_tracker import TradeTracker as _TT_LIVE
        self._trade_tracker_sv2_live = _TT_LIVE(max_trades=28)
        self._paper_trader_sv2_live = PaperTrader(
            trade_tracker=self._trade_tracker_sv2_live,
            journal_path=_DATA_DIR / "paper_trades_sv2_live.json",
        )
        self._trade_tracker_sv2_live.load_from_file(_TRADES_FILE_SV2_LIVE)
        self._paper_trader_sv2_live.load_journal()

        self._trade_tracker_sv2_a_tuned_live = _TT_LIVE(max_trades=28)
        self._paper_trader_sv2_a_tuned_live = PaperTrader(
            trade_tracker=self._trade_tracker_sv2_a_tuned_live,
            journal_path=_DATA_DIR / "paper_trades_sv2_a_tuned_live.json",
        )
        self._trade_tracker_sv2_a_tuned_live.load_from_file(_TRADES_FILE_A_TUNED_LIVE)
        self._paper_trader_sv2_a_tuned_live.load_journal()

        self._trade_tracker_sv2_ss_live = _TT_LIVE(max_trades=28)
        self._paper_trader_sv2_ss_live = PaperTrader(
            trade_tracker=self._trade_tracker_sv2_ss_live,
            journal_path=_DATA_DIR / "paper_trades_sv2_ss_live.json",
        )
        self._trade_tracker_sv2_ss_live.load_from_file(_TRADES_FILE_SS_LIVE)
        self._paper_trader_sv2_ss_live.load_journal()

        self._trade_tracker_sv2_b_tuned_live = _TT_LIVE(max_trades=28)
        self._paper_trader_sv2_b_tuned_live = PaperTrader(
            trade_tracker=self._trade_tracker_sv2_b_tuned_live,
            journal_path=_DATA_DIR / "paper_trades_sv2_b_tuned_live.json",
        )
        self._trade_tracker_sv2_b_tuned_live.load_from_file(_TRADES_FILE_B_TUNED_LIVE)
        self._paper_trader_sv2_b_tuned_live.load_journal()

        self._trade_tracker_sv2_atr_live = _TT_LIVE(max_trades=28)
        self._paper_trader_sv2_atr_live = PaperTrader(
            trade_tracker=self._trade_tracker_sv2_atr_live,
            journal_path=_DATA_DIR / "paper_trades_sv2_atr_live.json",
        )
        self._trade_tracker_sv2_atr_live.load_from_file(_TRADES_FILE_ATR_LIVE)
        self._paper_trader_sv2_atr_live.load_journal()

        # ── Sv2-upgraded (2026-04-23) ──
        # Fires off the LIVE-candle engine's stoch_entry_candidates_live (same
        # signal source as Sv2-live), but with three extra rules:
        #   1. Conviction ≥ 65 (vs default FULL-tier threshold of 50)
        #   2. Revenge cooldown: skip same-pair re-entry within 60 min of a loss
        #   3. BE-stop move at +7p peak (opt-outs: EURUSD/GBPUSD/NZDUSD/GBPCAD)
        # Paper-only, parallel to Sv2 — no cTrader, no MT5 mirror.
        self._trade_tracker_sv2_upgraded = _TT_LIVE(max_trades=28)
        self._paper_trader_sv2_upgraded = PaperTrader(
            trade_tracker=self._trade_tracker_sv2_upgraded,
            journal_path=_DATA_DIR / "paper_trades_sv2_upgraded.json",
        )
        self._trade_tracker_sv2_upgraded.load_from_file(_TRADES_FILE_SV2_UPGRADED)
        self._paper_trader_sv2_upgraded.load_journal()
        # Revenge cooldown state: pair -> (close_time_ts, was_win).
        # Populated when a Sv2-upgraded trade closes. Entry path consults this
        # to enforce the 60-min cooldown after a loss on the same pair.
        self._sv2_upgraded_last_close: dict[str, tuple[float, bool]] = {}

        # ── AU Gold suite: 5 XAUUSD strategies (2026-04-24) ──
        # Paper-only, trade only on the XAUUSD gold symbol. Completely
        # isolated from the forex CSI pipeline — see core/au_gold_systems.py.
        # Each strategy gets its own tracker + paper_trader + journal file
        # so they can be analysed independently in the Performance / LiveCan
        # tabs alongside the existing forex systems.
        self._trade_tracker_au1 = _TT_LIVE(max_trades=28)
        self._paper_trader_au1 = PaperTrader(
            trade_tracker=self._trade_tracker_au1,
            journal_path=_DATA_DIR / "paper_trades_au1_london.json",
        )
        self._trade_tracker_au1.load_from_file(_TRADES_FILE_AU1)
        self._paper_trader_au1.load_journal()

        self._trade_tracker_au2 = _TT_LIVE(max_trades=28)
        self._paper_trader_au2 = PaperTrader(
            trade_tracker=self._trade_tracker_au2,
            journal_path=_DATA_DIR / "paper_trades_au2_ny_orb.json",
        )
        self._trade_tracker_au2.load_from_file(_TRADES_FILE_AU2)
        self._paper_trader_au2.load_journal()

        self._trade_tracker_au3 = _TT_LIVE(max_trades=28)
        self._paper_trader_au3 = PaperTrader(
            trade_tracker=self._trade_tracker_au3,
            journal_path=_DATA_DIR / "paper_trades_au3_pullback.json",
        )
        self._trade_tracker_au3.load_from_file(_TRADES_FILE_AU3)
        self._paper_trader_au3.load_journal()

        self._trade_tracker_au4 = _TT_LIVE(max_trades=28)
        self._paper_trader_au4 = PaperTrader(
            trade_tracker=self._trade_tracker_au4,
            journal_path=_DATA_DIR / "paper_trades_au4_divergence.json",
        )
        self._trade_tracker_au4.load_from_file(_TRADES_FILE_AU4)
        self._paper_trader_au4.load_journal()

        self._trade_tracker_au5 = _TT_LIVE(max_trades=28)
        self._paper_trader_au5 = PaperTrader(
            trade_tracker=self._trade_tracker_au5,
            journal_path=_DATA_DIR / "paper_trades_au5_mean_rev.json",
        )
        self._trade_tracker_au5.load_from_file(_TRADES_FILE_AU5)
        self._paper_trader_au5.load_journal()

        # AU Gold signal engine (holds state for all 5 strategies).
        # Engine bodies are skeletons in Phase A — returns [] until Phase B
        # implements the actual signal logic.
        from takumi_trader.core.au_gold_systems import AuGoldSystemEngine
        self._au_gold_engine = AuGoldSystemEngine()

        # Cross-system dedup tracker: pair → last DTC entry epoch-seconds.
        # Used to skip a DTC trade if another source system already fired a
        # DTC trade on the same pair within dedup_window_seconds.
        self._dtc_combo_last_open_ts: dict[str, float] = {}
        # Load DTC-combo config (filters, dedup window)
        self._dtc_combo_cfg = self._load_dtc_combo_cfg()

        # Alt systems signal engine
        from takumi_trader.core.alt_systems import AltSystemEngine
        self._alt_engine = AltSystemEngine()

        # Retroactive SL/TP check — close trades hit while app was offline
        # Delayed 15s to let MT5 worker initialize
        from PyQt6.QtCore import QTimer
        QTimer.singleShot(15_000, self._retroactive_sl_tp_check)

        self._prev_m1_pair_scores: dict[str, float] = {}  # for momentum stall detection
        self._last_composite_scores: dict[str, float] = {}  # latest composite ccy scores
        # Spread stability: rolling history of base-quote spread per pair
        self._spread_history: dict[str, deque] = {}
        _SPREAD_HISTORY_LEN = 30  # ~30 seconds of readings at 1s polling
        # ATR expansion: track M5 true range per pair
        # Compare latest TR to rolling average over 12 M5 candles (~1 hour)
        # Detects when a pair starts moving faster than its recent pace
        self._tr_history: dict[str, deque] = {}  # true range per M5 bar
        self._tr_last_bar: dict[str, int] = {}
        self._atr_history: dict[str, deque] = {}  # kept for compatibility
        self._atr_last_bar: dict[str, int] = {}
        self._atr_frontloaded = False
        # Deep analytics: per-pair rolling histories for context
        self._m1_tick_volume_history: dict[str, deque] = {}  # last 15 M1 tick volumes (one per completed bar)
        self._m1_tv_last_bar: dict[str, int] = {}            # per-bar guard for tick volume (fix BUG #2)
        self._m1_tv_last_seen: dict[str, int] = {}           # latest in-progress tv; appended on rollover
        self._h1_atr_history: dict[str, deque] = {}          # last 20 H1 ATR readings
        self._h1_atr_last_bar: dict[str, int] = {}
        # ── NEW histories for momentum / trend-start fields (2026-04-20) ──
        # Each deque appended at most once per bar rollover (see _on_data).
        # Front-loaded from MT5 on first _on_data cycle so fields populate
        # immediately instead of needing 6 hours of runtime warm-up.
        self._m1_close_history: dict[str, deque] = {}        # last 30 M1 closes
        self._m1_close_last_bar: dict[str, int] = {}
        self._m1_direction_history: dict[str, deque] = {}    # last 10 M1 directions (+1/-1/0)
        self._m5_bar_history: dict[str, deque] = {}          # last 6 M5 (high, low, close) tuples
        self._m5_bar_last_key: dict[str, int] = {}
        self._m15_close_history: dict[str, deque] = {}       # last 25 M15 closes (for BB 20 + lookback)
        self._m15_close_last_bar: dict[str, int] = {}
        self._h1_bar_history: dict[str, deque] = {}          # last 30 H1 (high, low, close) tuples — for ADX
        self._h1_bar_last_key: dict[str, int] = {}
        # Composite-spread velocity: list of (timestamp, div_spread) tuples per pair
        # maxlen 120 @ ~1s per _on_data cycle = ~120 seconds of samples, enough for
        # a real 90s slope window with safety margin. (Fixes BUG #5.)
        self._composite_spread_history: dict[str, deque] = {}
        # Session VWAP: per-pair (date, sum_typical_x_vol, sum_vol, last_m1_bar).
        # last_m1_bar prevents re-accumulating the SAME in-progress M1 bar each
        # _on_data cycle. (Fixes BUG #1.)
        self._session_vwap: dict[str, dict] = {}
        self._entry_ctx_frontloaded = False
        # Track when each pair first entered the qualifying state (for momentum buildup)
        self._pair_first_qualify_time: dict[str, float] = {}
        # Cache latest alert candidates so TRACK links can access direction info
        self._latest_alert_candidates: dict[str, tuple[str, dict[str, float]]] = {}
        # Cache latest close prices per pair from M1 data
        self._latest_close_prices: dict[str, float] = {}
        # Cache latest conviction results
        self._latest_conviction: dict[str, ConvictionResult] = {}
        # Track last exit alert time per pair to avoid spamming
        self._last_exit_alert_time: dict[str, float] = {}
        self._exit_alert_cooldown = 60  # seconds between exit notifications for same pair
        # Spread-collapse confirmation counters per pair
        self._sc_confirm_count: dict[str, int] = {}
        self._gap_fill_done = False  # one-time gap fill on first data cycle
        # ── Strength engine health/diagnostic state (added 2026-04-21) ──
        # Debugs the "grid frozen but P/L moving" symptom — logs stoch_scores
        # coverage to data/strength_debug.log every 10s, and fires a CRITICAL
        # alert 10s after first data if the strength engine produced empty
        # scores (indicates MT5 bar cache desync — needs MT5-first restart).
        self._strength_debug_last_log: float = 0.0
        self._strength_health_check_done: bool = False
        self._strength_first_data_time: float = 0.0
        self._closed_trades_suppressed = False  # True after user clicks CLEAR
        # ── Weekend close-all state (2026-05-02) ──
        # JST ordinal-day on which the Sat 04:00 weekend close-all routine
        # last fired. -1 means it hasn't fired yet this run. The routine
        # closes paper + cTrader + MT5 positions; entry blocking is handled
        # automatically by is_weekend() now returning True from Sat 04:00 JST.
        self._weekend_close_done_for_ord: int = -1

        self._setup_ui()
        self._apply_light_theme()

        # Default: always on top
        self.setWindowFlags(self.windowFlags() | Qt.WindowType.WindowStaysOnTopHint)

        # Restore saved window position, size, and monitor
        self._restore_geometry()

        # Active trade inline HTML (rendered at top of alert panel)
        self._active_trade_html_parts: list[str] = []

        # Render restored alert history on startup
        if self._alert_history:
            self._refresh_alert_panel()
        if self._trade_tracker.active_trades:
            # Trades will be rendered inline once first data cycle arrives
            pairs = ", ".join(self._trade_tracker.active_trades.keys())
            logger.info(
                "Restored %d trade(s): %s — waiting for data...",
                self._trade_tracker.trade_count, pairs,
            )

        # Render closed trades on startup
        self._update_closed_trades_panel()

        # Tracks last data tick so the watchdog can detect feed staleness
        self._last_data_ts: float = 0.0

        # ── Shadow logger (Phase B vertical slice — Sv2 only) ──────────
        # Captures the unfiltered signal universe for Edge Miner. Built
        # once here, passed to the worker (which writes strength-rejects
        # and full strength-passes) and consulted from main_window's
        # gate sites (which call mark_decision/mark_executed). One file
        # per system; this is Sv2 only until Phase F validation passes
        # and we fan out to the other 21 systems.
        from takumi_trader.core.shadow_logger import ShadowLogger
        self._shadow_logger_sv2 = ShadowLogger(
            "Sv2", _DATA_DIR / "shadow_trades_Sv2.json",
        )

        # Worker — accepts the shadow logger so the M5-close shadow
        # capture (mt5_worker._capture_sv2_shadow) can write strength-
        # rejects and full strength-passes inline. Passing None disables
        # shadow capture; the worker behaves byte-identically to before.
        self._worker = MT5Worker(
            poll_interval=1.0,
            shadow_logger_sv2=self._shadow_logger_sv2,
        )
        self._worker.data_ready.connect(self._on_data)
        self._worker.connection_status.connect(self._on_connection_status)
        self._worker.start()

        # ── Shadow-sim worker (Phase D.4) ──────────────────────────────
        # Drains pending_simulation/pending_calibration on a 5-min cycle:
        # permanent-FAILED fast-path -> real sims -> calibration writes.
        # Built once here, runs in its own QThread (mirrors MT5Worker /
        # CsiWorker pattern). Failure-isolated: bad cycles log + retry,
        # fatal exceptions emit fatal_error for the operator alert path.
        try:
            from takumi_trader.core.m1_cache import M1Cache
            from takumi_trader.core.broker_spread_model import BrokerSpreadModel
            from takumi_trader.core.shadow_simulator import (
                ShadowSimulator, ShadowSimulatorConfig,
            )
            from takumi_trader.core.shadow_logger import ShadowCalibrationLog
            from takumi_trader.core.shadow_sim_worker import (
                ShadowSimWorker, make_paper_trade_lookup,
            )
            from takumi_trader.features.feature_engine import FeatureEngine
            import MetaTrader5 as _mt5_mod

            _sim_config = ShadowSimulatorConfig()
            _m1_cache = M1Cache(
                cache_dir=_DATA_DIR / "m1_cache",
                mt5_module=_mt5_mod,
            )
            _spread_model = BrokerSpreadModel(_sim_config)
            # Network features OFF for the simulator's engine — historical
            # recompute can't use live Yahoo/FRED/calendar snapshots, and
            # firing network refreshes from the worker thread would race
            # with paper_trader's main-thread engine. Price-derived features
            # (the bulk of feat_*) compute fine without network.
            _shadow_feature_engine = FeatureEngine(enable_network=False)
            self._shadow_calibration_log = ShadowCalibrationLog(
                _DATA_DIR / "shadow_calibration_Sv2.json",
            )
            self._shadow_simulator = ShadowSimulator(
                m1_cache=_m1_cache,
                spread_model=_spread_model,
                feature_engine=_shadow_feature_engine,
                calibration_log=self._shadow_calibration_log,
                config=_sim_config,
            )
            # Closure resolves EXECUTED records' journal_idx to closed
            # paper-trade outcomes via on-disk read (thread-safe, mtime-cached).
            _real_trade_lookup = make_paper_trade_lookup(_PAPER_TRADES_FILE)
            self._shadow_sim_worker = ShadowSimWorker(
                shadow_logger=self._shadow_logger_sv2,
                simulator=self._shadow_simulator,
                poll_interval=300.0,
                max_per_cycle=50,
                max_permanent_per_cycle=1000,
                real_trade_lookup=_real_trade_lookup,
            )
            self._shadow_sim_worker.cycle_complete.connect(self._on_sim_cycle_stats)
            self._shadow_sim_worker.drift_warning.connect(self._on_sim_drift_warning)
            self._shadow_sim_worker.fatal_error.connect(self._on_sim_fatal)
            self._shadow_sim_worker.start()
            logger.info(
                "[SHADOW] sim worker started (poll=300s, real-trade lookup wired)"
            )
        except Exception as exc:
            # Shadow-sim is observability infrastructure — its failure must
            # NOT take down trading. Log loudly, surface to health alerts,
            # leave self._shadow_sim_worker unset (closeEvent guard handles).
            logger.error(
                "[SHADOW] sim worker failed to start: %s", exc, exc_info=True,
            )
            if hasattr(self, "_health_alerts"):
                self._health_alerts.notify(
                    "warning", "Shadow",
                    f"Shadow-sim worker failed to start:\n\n{exc}\n\n"
                    "Trading continues normally. Calibration data won't "
                    "accumulate until this is fixed and TAKUMI restarted.",
                )
            self._shadow_sim_worker = None

        # Watchdog: pop a critical alert if no MT5 ticks for >90s
        # (uses _health_alerts which was created earlier in __init__)
        self._data_watchdog = StalenessWatchdog(
            parent=self, health=self._health_alerts,
            threshold_sec=90.0, check_interval_ms=15000,
        )

        # ── cTrader auto-trading bridge ──
        self._ctrader_config = settings  # reuse already-loaded settings dict
        self._ctrader_bridge: CTraderBridge | None = None
        self._ctrader_pos_mgr: CTraderPositionManager | None = None
        # ═══ cTrader PAUSED until Monday ═══
        # Rate-limited by server from earlier flapping. Paper trades still work,
        # stats still tracked. To re-enable: remove this guard and restart.
        _CTRADER_PAUSED_UNTIL_MONDAY = False

        if _CTRADER_AVAILABLE and not _CTRADER_PAUSED_UNTIL_MONDAY:
            try:
                self._ctrader_bridge = CTraderBridge(self)
                self._ctrader_pos_mgr = CTraderPositionManager()
                self._ctrader_pos_mgr.load(_CTRADER_POS_FILE)
                self._ctrader_bridge.connected.connect(self._on_ctrader_status)
                self._ctrader_bridge.order_opened.connect(self._on_ctrader_order_opened)
                self._ctrader_bridge.order_closed.connect(self._on_ctrader_order_closed)
                self._ctrader_bridge.order_error.connect(self._on_ctrader_order_error)
                self._ctrader_bridge.positions_synced.connect(self._on_ctrader_positions_synced)
                self._ctrader_bridge.balance_updated.connect(self._on_ctrader_balance)
                if settings.get("ctrader_enabled"):
                    self._ctrader_status_label.setText("cT: Connecting...")
                    self._ctrader_status_label.setStyleSheet("color: #e07020; padding: 0 4px;")
                    self._ctrader_bridge.start(settings)
                else:
                    self._ctrader_status_label.setText("cT: Disabled")
                    self._ctrader_status_label.setStyleSheet("color: #999; padding: 0 4px;")
            except Exception as exc:
                logger.error("Failed to init cTrader bridge: %s", exc)
                self._ctrader_status_label.setText(f"cT: Error — {exc}")
                self._ctrader_status_label.setStyleSheet("color: #c62828; padding: 0 4px;")
                self._ctrader_bridge = None
                self._ctrader_pos_mgr = None
        elif _CTRADER_PAUSED_UNTIL_MONDAY:
            self._ctrader_bridge = None
            self._ctrader_pos_mgr = None
            self._ctrader_status_label.setText("cT: PAUSED")
            self._ctrader_status_label.setStyleSheet(
                "color: #9c27b0; padding: 0 4px; font-weight: bold;"
            )
            logger.warning("cTrader PAUSED — reconnection disabled until manual restart")
        else:
            self._ctrader_status_label.setText("cT: Not available")
            self._ctrader_status_label.setStyleSheet("color: #999; padding: 0 4px;")

        # ── MT5 auto-trading ──
        from takumi_trader.core.mt5_trader import MT5Trader, MT5PositionManager
        import sys as _sys
        if getattr(_sys, "frozen", False):
            _mt5_pos_path = Path(_sys.executable).parent / "data" / "mt5_positions.json"
        else:
            _mt5_pos_path = Path(__file__).resolve().parent.parent.parent / "data" / "mt5_positions.json"
        self._mt5_pos_mgr = MT5PositionManager(_mt5_pos_path)
        self._mt5_trader = MT5Trader(self._mt5_pos_mgr)
        self._mt5_trader.order_opened.connect(self._on_mt5_order_opened)
        self._mt5_trader.order_closed.connect(self._on_mt5_order_closed)
        self._mt5_trader.order_error.connect(self._on_mt5_order_error)
        self._mt5_config = settings  # reuse settings dict
        # Sync with broker on startup
        if settings.get("mt5_trading_enabled", False):
            self._mt5_pos_mgr.sync_with_broker()

        # ── MT5 mirror config (cherry-pick which paper trades get mirrored) ──
        self._mt5_mirror_cfg: dict = {
            "enabled": False,
            "risk_pct": 3.0,
            "max_positions": 10,
            "mirror_pairs": [],
            "mirror_systems": [],
            "mirror_combos": [],
            "minute_blacklist_per_pair": {},
        }
        try:
            _mirror_cfg_path = _DATA_DIR / "mt5_mirror_config.json"
            if _mirror_cfg_path.exists():
                import json as _json
                _loaded = _json.loads(_mirror_cfg_path.read_text(encoding="utf-8"))
                if isinstance(_loaded, dict):
                    # Strip documentation-only keys
                    _loaded.pop("_note", None)
                    self._mt5_mirror_cfg.update(_loaded)
                # ── Validate minute_blacklist_per_pair structure ──
                _bl_raw = self._mt5_mirror_cfg.get("minute_blacklist_per_pair", {})
                _bl_clean: dict[str, list] = {}
                if not isinstance(_bl_raw, dict):
                    logger.error("[MT5 MIRROR] minute_blacklist_per_pair must be dict, got %s — using empty",
                                 type(_bl_raw).__name__)
                    _bl_raw = {}
                import re as _re
                _time_re = _re.compile(r"^\d{1,2}:\d{2}$")
                for _p, _wins in _bl_raw.items():
                    if not isinstance(_wins, list):
                        logger.error("[MT5 MIRROR] blacklist[%s] must be a list of [start,end] pairs, got %s — skipping",
                                     _p, type(_wins).__name__)
                        continue
                    _clean_wins = []
                    for _w in _wins:
                        if (isinstance(_w, (list, tuple)) and len(_w) == 2
                                and isinstance(_w[0], str) and isinstance(_w[1], str)
                                and _time_re.match(_w[0]) and _time_re.match(_w[1])):
                            # Validate hours/minutes in range
                            try:
                                sh, sm = [int(x) for x in _w[0].split(":")]
                                eh, em = [int(x) for x in _w[1].split(":")]
                                if 0 <= sh <= 24 and 0 <= sm <= 59 and 0 <= eh <= 24 and 0 <= em <= 59:
                                    _clean_wins.append([_w[0], _w[1]])
                                else:
                                    logger.error("[MT5 MIRROR] blacklist[%s] bad time values %s — skipping", _p, _w)
                            except (ValueError, AttributeError):
                                logger.error("[MT5 MIRROR] blacklist[%s] bad entry %s — skipping", _p, _w)
                        else:
                            logger.error("[MT5 MIRROR] blacklist[%s] entry must be [\"HH:MM\",\"HH:MM\"], got %s — skipping",
                                         _p, _w)
                    if _clean_wins:
                        _bl_clean[_p] = _clean_wins
                self._mt5_mirror_cfg["minute_blacklist_per_pair"] = _bl_clean

                logger.info(
                    "[MT5 MIRROR] Loaded config: enabled=%s pairs=%s systems=%s combos=%s risk=%.1f%% blacklist=%s",
                    self._mt5_mirror_cfg.get("enabled"),
                    self._mt5_mirror_cfg.get("mirror_pairs"),
                    self._mt5_mirror_cfg.get("mirror_systems"),
                    self._mt5_mirror_cfg.get("mirror_combos"),
                    self._mt5_mirror_cfg.get("risk_pct", 3.0),
                    self._mt5_mirror_cfg.get("minute_blacklist_per_pair"),
                )
        except Exception as _mcfg_ex:
            logger.warning("[MT5 MIRROR] Failed to load config: %s", _mcfg_ex)

        # Startup notice in alert panel — verify MT5 connectivity + config
        if self._mt5_mirror_cfg.get("enabled"):
            _mcfg = self._mt5_mirror_cfg
            _startup_now = datetime.now(_jst()).strftime("%H:%M:%S")
            # Probe MT5 state so the user sees actual status, not just "ready"
            try:
                import MetaTrader5 as _mt5_probe
                _ti = _mt5_probe.terminal_info()
                _ai = _mt5_probe.account_info()
                if _ti is None or _ai is None:
                    self._alert_history.appendleft(
                        f'<span style="font-size:9pt; color:#666;">[{_startup_now}]</span> '
                        f'<span style="font-size:10pt; color:#c62828; font-weight:bold;">'
                        f'\u26a0 MT5 MIRROR — NOT READY</span> '
                        f'<span style="font-size:9pt; color:#888;">MT5 terminal not responding. Mirror config loaded but cannot execute trades.</span>'
                    )
                elif not _ti.trade_allowed:
                    self._alert_history.appendleft(
                        f'<span style="font-size:9pt; color:#666;">[{_startup_now}]</span> '
                        f'<span style="font-size:10pt; color:#c62828; font-weight:bold;">'
                        f'\u26a0 MT5 MIRROR — ALGO DISABLED</span> '
                        f'<span style="font-size:9pt; color:#888;">Enable in MT5: Tools \u2192 Options \u2192 Expert Advisors \u2192 Allow algorithmic trading</span>'
                    )
                else:
                    _pair_str = ",".join(_mcfg.get("mirror_pairs") or []) or "any"
                    _sys_str = ",".join(_mcfg.get("mirror_systems") or []) or "any"
                    _combo_str = ",".join(_mcfg.get("mirror_combos") or [])
                    _target = _combo_str if _combo_str else f"{_pair_str} / {_sys_str}"
                    _min_bl = _mcfg.get("minute_blacklist_per_pair") or {}
                    _hour_detail = ""
                    if _min_bl:
                        _hour_detail = "  blocked windows: " + ", ".join(
                            f"{p}[{','.join(f'{w[0]}-{w[1]}' for w in wins)}]"
                            for p, wins in _min_bl.items() if wins
                        )
                    self._alert_history.appendleft(
                        f'<span style="font-size:9pt; color:#666;">[{_startup_now}]</span> '
                        f'<span style="font-size:10pt; color:#0288d1; font-weight:bold;">'
                        f'\U0001f501 MT5 MIRROR READY</span> '
                        f'<span style="font-size:9pt; color:#888;">'
                        f'acct#{_ai.login} equity={_ai.equity:,.0f} {_ai.currency}  '
                        f'target: {_target}  risk: {_mcfg.get("risk_pct",3.0):.1f}%{_hour_detail}</span>'
                    )
                    logger.info(
                        "[MT5 MIRROR] Startup check PASSED — account=%s equity=%s %s algo=%s pairs=%s",
                        _ai.login, _ai.equity, _ai.currency, _ti.trade_allowed,
                        _mcfg.get("mirror_pairs"),
                    )
            except Exception as _probe_ex:
                logger.error("[MT5 MIRROR] Startup probe failed: %s", _probe_ex)
                self._alert_history.appendleft(
                    f'<span style="font-size:9pt; color:#666;">[{_startup_now}]</span> '
                    f'<span style="font-size:10pt; color:#c62828; font-weight:bold;">'
                    f'\u26a0 MT5 MIRROR PROBE FAILED</span> '
                    f'<span style="font-size:9pt; color:#888;">{_probe_ex}</span>'
                )
            # Force a panel refresh via QTimer so the message appears immediately
            # once the UI is built (appendleft alone doesn't trigger a redraw)
            from PyQt6.QtCore import QTimer as _QT
            _QT.singleShot(500, self._refresh_alert_panel)

        # ── CSI / QM4 alert engine ────────────────────────────────
        from takumi_trader.core.csi_worker import CsiWorker
        from takumi_trader.core.qm4_alerts import QM4AlertEngine
        from takumi_trader.core.qm4_log import QM4AlertLog
        csi_cooldown = settings.get("csi_cooldown_minutes", 5) * 60
        self._qm4_engine = QM4AlertEngine(cooldown_seconds=csi_cooldown)
        self._csi_log = QM4AlertLog(_CSI_LOG_FILE)
        self._csi_sound_file: str = settings.get("csi_sound_file", "")
        self._csi_sound_enabled: bool = settings.get("csi_sound_enabled", True)
        # ── OCR forced OFF (2026-05-14) ──
        # External QM4 software (which OCR scrapes from) is no longer
        # available. TAKUMI relies solely on its internal CSI meter
        # (computed mode) until/unless QM4 software comes back online.
        # The settings.get("ocr_enabled") value is intentionally IGNORED
        # — the Settings dialog toggle still exists but is a no-op until
        # this hardcode is reverted. To re-enable when QM4 returns:
        # restore `ocr_enabled = settings.get("ocr_enabled", False)`
        # here AND in _on_settings_changed below.
        ocr_enabled = False
        self._csi_worker = CsiWorker(self, ocr_mode=ocr_enabled)
        self._csi_worker.scores_ready.connect(self._on_csi_scores)
        self._csi_worker.start()

        # Apply compact mode if previously enabled
        if settings.get("compact_mode", False):
            self._toggle_compact()

        # Auto-open Performance and Backtest windows on startup (after event loop starts)
        from PyQt6.QtCore import QTimer
        QTimer.singleShot(500, self._open_performance)

        # Auto-update Dukascopy data on startup (background, last 7 days)
        QTimer.singleShot(15_000, self._auto_update_dukascopy)

        # Refresh news calendar: first load 10s after start, then every 6 hours
        QTimer.singleShot(10_000, self._refresh_news_calendar)
        self._news_timer = QTimer()
        self._news_timer.timeout.connect(self._refresh_news_calendar)
        self._news_timer.start(6 * 3600 * 1000)  # 6 hours

    # ── UI Setup ──────────────────────────────────────────────────────

    def _setup_ui(self) -> None:
        """Build toolbar, table, currency table, alerts, and status bar."""
        # Toolbar
        self._toolbar = QToolBar()
        self._toolbar.setMovable(False)
        self._toolbar.setStyleSheet(
            "QToolBar { spacing: 1px; padding: 0px; }"
            " QToolBar::separator { width: 2px; }"
            " QPushButton { padding: 2px 5px; }"
        )
        self.addToolBar(self._toolbar)

        self._btn_pin = QPushButton("\U0001f4cc Pinned")
        self._btn_pin.setCheckable(True)
        self._btn_pin.setChecked(True)
        self._btn_pin.clicked.connect(self._toggle_always_on_top)
        self._toolbar.addWidget(self._btn_pin)

        self._toolbar.addSeparator()
        btn_settings = QPushButton("\u2699 Settings")
        btn_settings.clicked.connect(self._open_settings)
        self._toolbar.addWidget(btn_settings)

        self._toolbar.addSeparator()
        btn_perf = QPushButton("\U0001f4ca Perf")
        btn_perf.setToolTip("Alert performance statistics (MAE/MFE)")
        btn_perf.clicked.connect(self._open_performance)
        self._toolbar.addWidget(btn_perf)

        self._toolbar.addSeparator()
        btn_backtest = QPushButton("\U0001f504 BackT")
        btn_backtest.setToolTip("Run historical backtest simulation")
        btn_backtest.clicked.connect(self._open_backtest)
        self._toolbar.addWidget(btn_backtest)

        # ── LiveCan: live-candle-engine paper-system comparison dialog ──
        # Opens a side-by-side view of 5 "-live" systems that use the
        # live-candle engine (forming bars, updates every cycle) vs the
        # candle-close engine used by A/B/C/D/E.
        btn_livecan = QPushButton("\u26A1 LiveCan")
        btn_livecan.setToolTip(
            "Live Candle CSI systems — performance of the 5 live-engine "
            "counterparts to A/B/C/D/E"
        )
        btn_livecan.clicked.connect(self._open_livecan)
        self._toolbar.addWidget(btn_livecan)

        # Central widget
        central = QWidget()
        self.setCentralWidget(central)
        self._main_layout = QVBoxLayout(central)
        self._main_layout.setContentsMargins(4, 4, 4, 4)
        self._main_layout.setSpacing(4)

        # ── Filter Toolbar ──
        self._filter_toolbar = FilterToolbar()
        self._filter_toolbar.filters_changed.connect(self._on_filters_changed)
        self._main_layout.addWidget(self._filter_toolbar)

        # ── Pairs Table: Pair | M1 | M5 | M15 | H1 ──
        self._table = QTableWidget(len(DISPLAY_PAIRS), _TOTAL_COLS)
        self._table.setHorizontalHeaderLabels(["Pair", "Range"] + _DISPLAY_TFS)
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        self._table.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self._table.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        header = self._table.horizontalHeader()
        for col in range(_TOTAL_COLS):
            header.setSectionResizeMode(col, QHeaderView.ResizeMode.Stretch)

        # Populate pair names and placeholder values
        mono_font = QFont("Consolas", self._font_size)
        for row, pair in enumerate(DISPLAY_PAIRS):
            item = QTableWidgetItem(pair)
            item.setFont(mono_font)
            item.setTextAlignment(
                Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter
            )
            self._table.setItem(row, 0, item)
            # Range% column
            range_cell = QTableWidgetItem("\u2014")
            range_cell.setFont(mono_font)
            range_cell.setTextAlignment(
                Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter
            )
            self._table.setItem(row, _RANGE_COL, range_cell)
            # Timeframe columns
            for col in range(_TF_COL_OFFSET, _TOTAL_COLS):
                cell = QTableWidgetItem("\u2014")
                cell.setFont(mono_font)
                cell.setTextAlignment(
                    Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter
                )
                self._table.setItem(row, col, cell)

        # ── Currency Strength Table: Currency | M5 | M15 | H1 | H4 | D1 ──
        _CCY_COLS = 1 + len(_DISPLAY_TFS)  # Currency + 5 TFs
        _CCY_TF_LABELS = list(_DISPLAY_TFS)
        self._ccy_tf_labels = _CCY_TF_LABELS
        self._ccy_cols = _CCY_COLS

        self._ccy_table = QTableWidget(len(CURRENCIES), _CCY_COLS)
        self._ccy_table.setHorizontalHeaderLabels(["Currency"] + _CCY_TF_LABELS)
        self._ccy_table.verticalHeader().setVisible(False)
        self._ccy_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._ccy_table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        self._ccy_table.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self._ccy_table.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._ccy_table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        ccy_header = self._ccy_table.horizontalHeader()
        for col in range(_CCY_COLS):
            ccy_header.setSectionResizeMode(col, QHeaderView.ResizeMode.Stretch)

        for row, ccy in enumerate(CURRENCIES):
            item = QTableWidgetItem(ccy)
            item.setFont(QFont("Consolas", self._font_size, QFont.Weight.Bold))
            item.setTextAlignment(
                Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter
            )
            self._ccy_table.setItem(row, 0, item)
            # Timeframe columns (M1, M5, M15, H1, H4) — start at col 1
            for col in range(1, _CCY_COLS):
                cell = QTableWidgetItem("\u2014")
                cell.setFont(mono_font)
                cell.setTextAlignment(
                    Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter
                )
                self._ccy_table.setItem(row, col, cell)

        # Apply row heights and table fixed heights based on font size
        self._apply_table_sizes()

        # Add both tables to layout (fixed heights, flush, no gap)
        self._main_layout.setSpacing(0)
        self._main_layout.addWidget(self._table)
        self._main_layout.addWidget(self._ccy_table)

        # ── Vertical splitter for 3 resizable panels ──
        self._panels_splitter = QSplitter(Qt.Orientation.Vertical)
        self._panels_splitter.setHandleWidth(5)
        self._panels_splitter.setStyleSheet(
            "QSplitter::handle { background: #c0c0c0; }"
            "QSplitter::handle:hover { background: #4a6fa5; }"
        )

        # ── Panel 1: OPEN TRADES ──
        open_trades_widget = QWidget()
        open_trades_layout = QVBoxLayout(open_trades_widget)
        open_trades_layout.setContentsMargins(0, 0, 0, 0)
        open_trades_layout.setSpacing(0)

        open_header_row = QHBoxLayout()
        open_header_row.setContentsMargins(0, 0, 0, 0)
        self._open_header = QLabel("\U0001f4c8 OPEN TRADES")
        self._open_header.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
        self._open_header.setStyleSheet("color: #222222; padding: 4px 0;")
        open_header_row.addWidget(self._open_header)
        open_header_row.addStretch()
        self._open_stats_label = QLabel("")
        self._open_stats_label.setFont(QFont("Segoe UI", 9))
        self._open_stats_label.setStyleSheet("color: #4a6fa5; font-weight: bold; padding: 0 8px;")
        open_header_row.addWidget(self._open_stats_label)
        open_trades_layout.addLayout(open_header_row)

        self._open_scroll = QScrollArea()
        self._open_scroll.setWidgetResizable(True)
        self._open_scroll.setStyleSheet(
            "QScrollArea { border: 1px solid #d0d0d0; background: #ffffff; }"
        )
        self._open_panel = QLabel("No open trades.")
        self._open_panel.setWordWrap(True)
        self._open_panel.setFont(QFont("Consolas", 10))
        self._open_panel.setStyleSheet(
            "color: #888888; padding: 4px 6px; background: #ffffff;"
        )
        self._open_panel.setAlignment(
            Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft
        )
        self._open_panel.setTextFormat(Qt.TextFormat.RichText)
        self._open_panel.setOpenExternalLinks(False)
        self._open_panel.linkActivated.connect(self._on_alert_link_clicked)
        self._open_scroll.setWidget(self._open_panel)
        open_trades_layout.addWidget(self._open_scroll, stretch=1)
        self._panels_splitter.addWidget(open_trades_widget)

        # ── Panel 2: CLOSED TRADES ──
        closed_trades_widget = QWidget()
        closed_trades_layout = QVBoxLayout(closed_trades_widget)
        closed_trades_layout.setContentsMargins(0, 0, 0, 0)
        closed_trades_layout.setSpacing(0)

        closed_header_row = QHBoxLayout()
        closed_header_row.setContentsMargins(0, 0, 0, 0)
        self._closed_header = QLabel("\u2705 CLOSED TRADES")
        self._closed_header.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
        self._closed_header.setStyleSheet("color: #222222; padding: 4px 0;")
        closed_header_row.addWidget(self._closed_header)
        closed_header_row.addStretch()

        self._closed_stats_label = QLabel("")
        self._closed_stats_label.setFont(QFont("Segoe UI", 9))
        self._closed_stats_label.setStyleSheet("color: #4a6fa5; font-weight: bold; padding: 0 8px;")
        closed_header_row.addWidget(self._closed_stats_label)

        btn_clear_closed = QPushButton("CLEAR")
        btn_clear_closed.setFixedHeight(22)
        btn_clear_closed.setStyleSheet(
            "QPushButton { background: #e8e8e8; color: #555555;"
            " border: 1px solid #cccccc; border-radius: 3px;"
            " padding: 1px 8px; font-size: 10px; font-weight: bold; }"
            " QPushButton:hover { background: #d0d0d0; }"
        )
        btn_clear_closed.clicked.connect(self._clear_closed_trades)
        closed_header_row.addWidget(btn_clear_closed)
        closed_trades_layout.addLayout(closed_header_row)

        self._closed_scroll = QScrollArea()
        self._closed_scroll.setWidgetResizable(True)
        self._closed_scroll.setStyleSheet(
            "QScrollArea { border: 1px solid #d0d0d0; background: #ffffff; }"
        )

        self._closed_panel = QLabel("No closed trades yet.")
        self._closed_panel.setWordWrap(True)
        self._closed_panel.setFont(QFont("Consolas", 10))
        self._closed_panel.setStyleSheet(
            "color: #888888; padding: 4px 6px; background: #ffffff;"
        )
        self._closed_panel.setAlignment(
            Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft
        )
        self._closed_panel.setTextFormat(Qt.TextFormat.RichText)
        self._closed_scroll.setWidget(self._closed_panel)
        closed_trades_layout.addWidget(self._closed_scroll, stretch=1)
        self._panels_splitter.addWidget(closed_trades_widget)

        # ── Panel 3: TREND ALERTS ──
        alerts_widget = QWidget()
        alerts_layout = QVBoxLayout(alerts_widget)
        alerts_layout.setContentsMargins(0, 0, 0, 0)
        alerts_layout.setSpacing(0)

        alert_header_row = QHBoxLayout()
        alert_header_row.setContentsMargins(0, 0, 0, 0)
        self._alert_header = QLabel("\u26a1 TREND ALERTS")
        self._alert_header.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
        self._alert_header.setStyleSheet("color: #222222; padding: 4px 0;")
        alert_header_row.addWidget(self._alert_header)
        alert_header_row.addStretch()
        btn_clear_alerts = QPushButton("CLEAR")
        btn_clear_alerts.setFixedHeight(22)
        btn_clear_alerts.setStyleSheet(
            "QPushButton { background: #e8e8e8; color: #555555;"
            " border: 1px solid #cccccc; border-radius: 3px;"
            " padding: 1px 8px; font-size: 10px; font-weight: bold; }"
            " QPushButton:hover { background: #d0d0d0; }"
        )
        btn_clear_alerts.clicked.connect(self._clear_alerts)
        alert_header_row.addWidget(btn_clear_alerts)
        alerts_layout.addLayout(alert_header_row)

        self._alert_scroll = QScrollArea()
        self._alert_scroll.setWidgetResizable(True)
        self._alert_scroll.setStyleSheet(
            "QScrollArea { border: 1px solid #d0d0d0; background: #ffffff; }"
        )

        self._alert_label = QLabel("No 4-timeframe alignments detected.")
        self._alert_label.setWordWrap(True)
        self._alert_label.setFont(QFont("Consolas", 10))
        self._alert_label.setStyleSheet(
            "color: #888888; padding: 4px 6px; background: #ffffff;"
            " a { color: #4a6fa5; text-decoration: none; font-weight: bold; }"
        )
        self._alert_label.setAlignment(
            Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft
        )
        self._alert_label.setTextFormat(Qt.TextFormat.RichText)
        self._alert_label.setOpenExternalLinks(False)
        self._alert_label.linkActivated.connect(self._on_alert_link_clicked)
        self._alert_scroll.setWidget(self._alert_label)
        alerts_layout.addWidget(self._alert_scroll, stretch=1)
        self._panels_splitter.addWidget(alerts_widget)

        # Set initial sizes: Open 150, Closed 200, Alerts stretches
        self._panels_splitter.setSizes([150, 200, 400])
        self._panels_splitter.setStretchFactor(0, 1)  # open trades
        self._panels_splitter.setStretchFactor(1, 1)  # closed trades
        self._panels_splitter.setStretchFactor(2, 2)  # alerts (gets more space)
        self._main_layout.addWidget(self._panels_splitter, stretch=1)

        # Status bar
        self._status_label = QLabel("Connecting to MT5\u2026")
        self._status_label.setFont(QFont("Segoe UI", 9))
        self.statusBar().addPermanentWidget(self._status_label, 1)
        self._session_label = QLabel("")
        self._session_label.setFont(QFont("Segoe UI", 9, QFont.Weight.Bold))
        self._session_label.setStyleSheet("color: #4a6fa5; padding: 0 8px;")
        self.statusBar().addPermanentWidget(self._session_label)
        self._status_dot = QLabel("\u25cf")
        self._status_dot.setFont(QFont("Segoe UI", 12))
        self._status_dot.setStyleSheet("color: #ff5252;")
        self.statusBar().addWidget(self._status_dot)

        # cTrader connection status in status bar
        self._ctrader_status_label = QLabel("")
        self._ctrader_status_label.setFont(QFont("Segoe UI", 8))
        self._ctrader_status_label.setStyleSheet("color: #999; padding: 0 4px;")
        self.statusBar().addPermanentWidget(self._ctrader_status_label)

    def _apply_table_sizes(self) -> None:
        """Set row heights and table fixed heights based on current font size."""
        # Row height scales with font: base 20px at size 10, +2px per point
        row_h = max(18, 20 + (self._font_size - 10) * 2)

        self._table.verticalHeader().setDefaultSectionSize(row_h)
        pairs_total_h = len(DISPLAY_PAIRS) * row_h + 40
        self._table.setFixedHeight(pairs_total_h)

        self._ccy_table.verticalHeader().setDefaultSectionSize(row_h)
        ccy_total_h = len(CURRENCIES) * row_h + 40  # +40 for header row
        self._ccy_table.setFixedHeight(ccy_total_h)

    def _apply_font_size(self) -> None:
        """Apply the current font size to all table cells and recalculate heights."""
        mono_font = QFont("Consolas", self._font_size)
        bold_font = QFont("Consolas", self._font_size, QFont.Weight.Bold)
        for row in range(self._table.rowCount()):
            for col in range(self._table.columnCount()):
                item = self._table.item(row, col)
                if item:
                    item.setFont(mono_font)
        for row in range(self._ccy_table.rowCount()):
            item0 = self._ccy_table.item(row, 0)
            if item0:
                item0.setFont(bold_font)
            for col in range(1, self._ccy_table.columnCount()):
                item = self._ccy_table.item(row, col)
                if item:
                    item.setFont(mono_font)
        self._apply_table_sizes()

    def _apply_light_theme(self) -> None:
        """Apply the light grey/white theme stylesheet."""
        self.setStyleSheet(
            """
            QMainWindow { background: #f5f5f5; }
            QWidget { background: #f5f5f5; color: #222222; }
            QToolBar { background: #e8e8e8; border-bottom: 1px solid #d0d0d0;
                       spacing: 6px; padding: 2px; }
            QPushButton { background: #4a6fa5; color: #ffffff; border: none;
                          padding: 5px 14px; border-radius: 3px; font-size: 12px; }
            QPushButton:hover { background: #5a83bf; }
            QTableWidget { background: #ffffff; gridline-color: #dcdcdc;
                           border: 1px solid #d0d0d0; color: #222222; }
            QTableWidget::item { padding: 1px 2px; }
            QHeaderView::section { background: #e0e0e0; color: #333333;
                                   border: 1px solid #d0d0d0; padding: 1px 2px;
                                   font-weight: bold; }
            QStatusBar { background: #e8e8e8; color: #555555; }
            """
        )

    # ── Data Handling ─────────────────────────────────────────────────

    def _shadow_mark_sv2(
        self,
        result,
        pair: str,
        status: str,
        gate: str,
        reason: str,
        metadata: dict | None = None,
    ) -> None:
        """Tiny wrapper: look up shadow_id for `pair` from result.sv2_shadow_ids
        and call mark_decision. No-op if shadow logging is off, the pair
        wasn't captured this cycle, or the dict isn't on the result yet
        (older worker versions or non-M5-close cycles).

        Pulled out into a helper because every gate-site call would
        otherwise duplicate the lookup + None-check + try/except
        boilerplate. Single point of failure if mark_decision ever
        raises; otherwise the gate sites stay readable."""
        if self._shadow_logger_sv2 is None:
            return
        ids = getattr(result, "sv2_shadow_ids", None) or {}
        sid = ids.get(pair)
        if sid is None:
            return  # pair wasn't captured (not a strength-pass this cycle)
        try:
            self._shadow_logger_sv2.mark_decision(
                sid, status=status, gate=gate, reason=reason,
                metadata=metadata,
            )
        except Exception as _exc:
            logging.getLogger(__name__).warning(
                "[SHADOW] mark_decision failed pair=%s gate=%s: %s",
                pair, gate, _exc,
            )

    def _shadow_structural_metadata(self, conv) -> tuple[str, str, dict]:
        """Decode a structural HARD BLOCK ConvictionResult into the
        three Site-5 metadata fields the design review specified:
        structural_reason_type ('proximity' | 'tp_clearance'),
        offending_side ('above' | 'below'), and
        offending_level_pips (closest violated level distance).

        Returns (gate, reason, metadata_dict). If the structural
        component passed, returns the GATE_CONVICTION metadata path
        instead — caller doesn't need to switch on tier first.
        """
        from takumi_trader.core.shadow_logger import (
            GATE_STRUCTURAL, GATE_CONVICTION,
        )
        struct = conv.components.get("structural") if conv else None
        if struct and not struct.passed:
            details = struct.details or {}
            level_ok = bool(details.get("level_ok", True))
            tp_ok = bool(details.get("tp_ok", True))
            # If both fail, level proximity fires first in filter_engine's
            # check order — call it the primary reason.
            reason_type = "proximity" if not level_ok else "tp_clearance"
            return (
                GATE_STRUCTURAL,
                struct.reason or "structural HARD BLOCK",
                {
                    "conv_score": int(getattr(conv, "conviction", 0)),
                    "tier": getattr(conv, "tier", ""),
                    "structural_reason_type": reason_type,
                    "level_ok": level_ok,
                    "tp_ok": tp_ok,
                    # offending_side: BUYs are blocked by levels ABOVE
                    # entry, SELLs by levels BELOW entry — derivable from
                    # direction at the call site, so we expose neither
                    # here; caller stamps it.
                    "components": {
                        k: getattr(v, "score", 0)
                        for k, v in conv.components.items()
                    },
                },
            )
        # Score-based DIMMED/SUPPRESSED — pure conviction
        return (
            GATE_CONVICTION,
            f"tier={getattr(conv, 'tier', '?')}, "
            f"conv={getattr(conv, 'conviction', 0)}",
            {
                "conv_score": int(getattr(conv, "conviction", 0)),
                "tier": getattr(conv, "tier", ""),
                "components": {
                    k: getattr(v, "score", 0)
                    for k, v in (conv.components if conv else {}).items()
                },
            },
        )

    def _all_paper_traders(self) -> list:
        """Return the 22 PaperTrader instances managed by this window.

        Hard-coded list rather than introspection because (a) the names
        are stable and (b) explicit-is-better-than-magic when the routine
        below force-closes real exposure. Ordering doesn't matter — every
        trader is independent.
        """
        return [
            self._paper_trader,                       # Sv2
            self._paper_trader_ss,                    # Sv2+SS
            self._paper_trader_atr,                   # Sv2+ATR
            self._paper_trader_qm4,                   # QM4
            self._paper_trader_a_tuned,               # Sv2-tuned
            self._paper_trader_b_tuned,               # Sv2+SS-tuned
            self._paper_trader_breakout,              # Breakout
            self._paper_trader_squeeze,               # Squeeze
            self._paper_trader_squeeze_rev,           # Squeeze-REV
            self._paper_trader_divergence,            # Divergence
            self._paper_trader_dtc_combo,             # DTC-combo
            self._paper_trader_sv2_live,              # Sv2 live mirror
            self._paper_trader_sv2_a_tuned_live,
            self._paper_trader_sv2_ss_live,
            self._paper_trader_sv2_b_tuned_live,
            self._paper_trader_sv2_atr_live,
            self._paper_trader_sv2_upgraded,          # Sv2-upgraded
            self._paper_trader_au1,                   # AU Gold suite
            self._paper_trader_au2,
            self._paper_trader_au3,
            self._paper_trader_au4,
            self._paper_trader_au5,
        ]

    def _close_all_for_weekend(self, result: CalculationResult) -> bool:
        """Force-close every open trade across paper + cTrader + MT5.

        Fires once at Sat 04:00 JST (3 hours before the real broker close)
        from the per-cycle hook in _on_data. Failures in any one lane are
        logged but don't abort the others — defensive on purpose since
        we'd rather have partial progress than no progress.

        Paper close prices come from result.close_prices (the M1 BID).
        XAUUSD price for AU paper trades comes from result.xau_price.

        Returns True if every paper trader is now flat AND every live
        position-manager reports zero open. False means the caller should
        retry on the next data cycle (typical reason: a paper trade got
        skipped because its M1 close price was missing this cycle).
        """
        log = logging.getLogger(__name__)
        log.info(
            "[WEEKEND-CLOSE] Sat 04:00 JST trigger — flattening all "
            "exposure (paper + cTrader + MT5)."
        )

        # ── Lane 1: paper traders (22 systems) ────────────────────────
        def _get_close_price(pair: str) -> float:
            # XAUUSD lives on its own data channel for the AU suite;
            # everything else uses the M1 close grid.
            if pair == result.xau_symbol or pair.upper().startswith("XAUUSD"):
                return float(result.xau_price or 0.0)
            return float(result.close_prices.get(pair, 0.0) or 0.0)

        paper_total = 0
        for pt in self._all_paper_traders():
            try:
                closed = pt.close_all_open("weekend_close", _get_close_price)
                if closed:
                    log.info(
                        "[WEEKEND-CLOSE] %s closed %d paper trades",
                        type(pt).__name__, len(closed),
                    )
                paper_total += len(closed)
            except Exception as exc:
                log.warning(
                    "[WEEKEND-CLOSE] paper trader close failed: %s",
                    exc, exc_info=True,
                )
        log.info("[WEEKEND-CLOSE] paper total closed: %d", paper_total)

        # ── Lane 2: cTrader live positions ────────────────────────────
        ct_total = 0
        if self._ctrader_bridge is not None and self._ctrader_pos_mgr is not None:
            try:
                positions = self._ctrader_pos_mgr.all_positions()
                for pair, pos in positions.items():
                    try:
                        self._ctrader_bridge.close_position(
                            pos.position_id, pos.volume,
                        )
                        ct_total += 1
                        log.info(
                            "[WEEKEND-CLOSE] cTrader close requested: "
                            "%s pos=%d vol=%.2f",
                            pair, pos.position_id, pos.volume,
                        )
                    except Exception as exc:
                        log.warning(
                            "[WEEKEND-CLOSE] cTrader %s close failed: %s",
                            pair, exc,
                        )
            except Exception as exc:
                log.warning(
                    "[WEEKEND-CLOSE] cTrader iteration failed: %s", exc,
                )
        log.info("[WEEKEND-CLOSE] cTrader close requests sent: %d", ct_total)

        # ── Lane 3: MT5 live positions ────────────────────────────────
        mt5_total = 0
        try:
            mt5_positions = self._mt5_pos_mgr.all_positions()
            for pair in list(mt5_positions.keys()):
                try:
                    if self._mt5_trader.close_position(pair):
                        mt5_total += 1
                except Exception as exc:
                    log.warning(
                        "[WEEKEND-CLOSE] MT5 %s close failed: %s",
                        pair, exc,
                    )
        except Exception as exc:
            log.warning(
                "[WEEKEND-CLOSE] MT5 iteration failed: %s", exc,
            )
        log.info("[WEEKEND-CLOSE] MT5 closed: %d", mt5_total)

        log.info(
            "[WEEKEND-CLOSE] DONE — paper=%d cTrader=%d MT5=%d. "
            "Trading blocked until Mon ~07:00 JST (is_weekend()=True).",
            paper_total, ct_total, mt5_total,
        )

        # All-clear check: any leftover open trades anywhere?
        # Paper: re-poll each tracker. Live: the cTrader close request is
        # async (Twisted) so we accept "request sent" as success — the
        # bridge will reconcile in due course; the position_manager state
        # will lag by a few seconds. MT5 close is synchronous so it's
        # already reflected in _mt5_pos_mgr.
        paper_remaining = sum(
            len(pt._tracker.active_trades)
            for pt in self._all_paper_traders()
        )
        mt5_remaining = len(self._mt5_pos_mgr.all_positions())
        if paper_remaining or mt5_remaining:
            log.warning(
                "[WEEKEND-CLOSE] retry next cycle — leftover paper=%d mt5=%d",
                paper_remaining, mt5_remaining,
            )
            return False
        return True

    def _on_data(self, result: CalculationResult) -> None:
        """Handle new data from the worker thread."""
        self._last_result = result  # cache for cTrader order params
        # Stamp the staleness watchdog — see StalenessWatchdog._tick
        self._last_data_ts = time.time()

        # ── Weekend close-all (2026-05-02) ──────────────────────────────
        # At 04:00 JST every Saturday, flatten ALL open exposure (paper +
        # cTrader + MT5) before the Friday US-session close. Idempotent:
        # the per-day ordinal flag stops it re-firing on subsequent cycles.
        # Entry blocking from this point until Mon ~07:00 JST is handled
        # by is_weekend() (extended in session_manager.py to fire from
        # Fri 19:00 UTC = Sat 04:00 JST). The two work together —
        # is_weekend() prevents new opens; this routine flattens existing.
        try:
            _now_jst = datetime.now(_jst())
            _jst_hm = _now_jst.hour * 60 + _now_jst.minute
            _jst_ord = _now_jst.toordinal()
            # Sat = weekday 5 in Python (Mon=0). Fire ONCE per Saturday,
            # any time at-or-after 04:00 JST. Re-running TAKUMI mid-window
            # picks up the close-all on the next data cycle.
            if (_now_jst.weekday() == 5
                    and _jst_hm >= 4 * 60
                    and self._weekend_close_done_for_ord != _jst_ord):
                # Only stamp "done" if the routine reports zero remaining
                # open trades — protects against retrying being locked out
                # when a single paper trade got skipped this cycle for a
                # missing close price (data races in the M1 grid).
                _all_clear = self._close_all_for_weekend(result)
                if _all_clear:
                    self._weekend_close_done_for_ord = _jst_ord
        except Exception as _wkc_exc:
            # Never let this crash the data pipeline — losing one weekend
            # close is recoverable, losing the data feed is not.
            logging.getLogger(__name__).warning(
                "Weekend close-all failed: %s", _wkc_exc, exc_info=True,
            )

        # ── Strength-engine diagnostic logger (2026-04-21, fix #1) ──
        # Writes one line every 10s to data/strength_debug.log showing stoch_scores
        # coverage per TF. If the grid freezes while P/L moves, this file shows
        # whether stoch_scores went empty while close_prices stayed populated
        # (confirms the hypothesis that the strength engine desynced from MT5
        # bar cache after a reconnect).
        try:
            _now_log = time.time()
            if _now_log - self._strength_debug_last_log >= 10.0:
                self._strength_debug_last_log = _now_log
                _counts = {
                    tf: len(result.stoch_scores.get(tf, {}))
                    for tf in ("M5", "M15", "H1", "H4", "D1", "W1", "MN")
                }
                _line = (
                    f"{datetime.now(_jst()).strftime('%H:%M:%S')}  "
                    f"connected={result.connected}  "
                    f"stoch_per_tf={_counts}  "
                    f"close_prices={len(result.close_prices)}  "
                    f"h1_atr={len(result.h1_atr)}  "
                    f"composite={len(result.composite_scores)}\n"
                )
                with open(_DATA_DIR / "strength_debug.log", "a", encoding="utf-8") as _df:
                    _df.write(_line)
        except Exception:
            pass  # diagnostic must never break _on_data

        # ── Schedule strength-engine startup health check (2026-04-21, fix #3) ──
        # On the FIRST data cycle, start a 10s timer that verifies all 8
        # currencies × 4 TFs (M5/M15/H1/H4) produced scores. If not → CRITICAL
        # alert telling the operator to close TAKUMI + MT5, then start MT5
        # first, then TAKUMI. Runs exactly once per TAKUMI session.
        if self._strength_first_data_time == 0.0:
            self._strength_first_data_time = time.time()
            try:
                from PyQt6.QtCore import QTimer as _QTimer
                _QTimer.singleShot(10000, self._check_strength_engine_health)
            except Exception:
                pass

        # One-time gap fill for active perf alerts after MT5 connects
        if not self._gap_fill_done and result.connected:
            self._gap_fill_done = True
            if self._alert_perf.get_active_count() > 0:
                self._alert_perf.fill_gaps_from_mt5()

        alert_candidates: dict[str, tuple[str, dict[str, float]]] = {}

        # ── Diagnostic recording (per-minute algo state) ──
        try:
            open_pairs = set(self._trade_tracker.active_trades.keys())
            self._diagnostic_recorder.record(
                result, open_pairs=open_pairs, session=result.session_label
            )
        except Exception as _diag_err:
            logging.getLogger(__name__).warning("Diagnostic error: %s", _diag_err)

        # ── Update pair scores table (Stoch v2: 0-10 scale) ──
        for row, pair in enumerate(DISPLAY_PAIRS):
            base, quote = pair[:3], pair[3:]
            tf_scores: dict[str, float] = {}
            for col_idx, tf in enumerate(_DISPLAY_TFS):
                col = col_idx + _TF_COL_OFFSET
                # Use Stoch v2 scores (0-10 per currency)
                stoch_tf = result.stoch_scores.get(tf, {})
                b_sc = stoch_tf.get(base)
                q_sc = stoch_tf.get(quote)

                if b_sc is None or q_sc is None:
                    # Fallback to old engine
                    tf_result = result.timeframes.get(tf)
                    if tf_result and pair in tf_result.pair_scores:
                        score = tf_result.pair_scores[pair]
                    else:
                        continue
                else:
                    # Pair spread on 0-10 scale: positive = base stronger
                    score = b_sc - q_sc  # range: -10 to +10

                tf_scores[tf] = score

                # Update history
                self._score_history[pair][tf].append(score)

                # Arrow
                history = self._score_history[pair][tf]
                if len(history) >= 5:
                    delta = history[-1] - history[0]
                    if delta > 0.3:
                        arrow, arrow_color = _ARROW_UP
                    elif delta < -0.3:
                        arrow, arrow_color = _ARROW_DOWN
                    else:
                        arrow, arrow_color = _ARROW_FLAT
                else:
                    arrow, arrow_color = _ARROW_FLAT

                # Format cell
                bg, fg = _cell_colors(score)
                text = f"{score:+.1f} {arrow}"

                item = self._table.item(row, col)
                if item:
                    item.setText(text)
                    item.setBackground(QColor(bg))
                    item.setForeground(QColor(fg))

        # ── Update Range% column ──
        for row, pair in enumerate(DISPLAY_PAIRS):
            pct = result.session_range_pct.get(pair, 0.0)
            item = self._table.item(row, _RANGE_COL)
            if item:
                if pct > 0:
                    item.setText(f"{pct:.0f}%")
                    # Color code: green (low) → yellow (mid) → red (high)
                    if pct >= 90:
                        item.setBackground(QColor("#ef5350"))
                        item.setForeground(QColor("#ffffff"))
                    elif pct >= 75:
                        item.setBackground(QColor("#ff9800"))
                        item.setForeground(QColor("#ffffff"))
                    elif pct >= 50:
                        item.setBackground(QColor("#ffeb3b"))
                        item.setForeground(QColor("#333333"))
                    elif pct >= 25:
                        item.setBackground(QColor("#c8e6c9"))
                        item.setForeground(QColor("#1b5e20"))
                    else:
                        item.setBackground(QColor("#e8f5e9"))
                        item.setForeground(QColor("#2e7d32"))
                else:
                    item.setText("\u2014")
                    item.setBackground(QColor("#ffffff"))
                    item.setForeground(QColor("#999999"))

        # ── Update currency strength table (Stoch v2: 0-10 scale) ──
        for row, ccy in enumerate(CURRENCIES):
            for col_idx, tf in enumerate(_DISPLAY_TFS):
                col = col_idx + 1  # TFs start at column 1 in currency table

                # Use Stoch v2 scores (0-10 scale)
                stoch_tf = result.stoch_scores.get(tf, {})
                score = stoch_tf.get(ccy)

                if score is None:
                    # Fallback to old engine for M5/M15/H1
                    tf_result = result.timeframes.get(tf)
                    if tf_result and ccy in tf_result.currency_scores:
                        score = tf_result.currency_scores[ccy]
                    elif tf in ("H4", "D1"):
                        htf_data = result.htf_regimes.get(ccy, {}).get(tf)
                        if htf_data:
                            _, score = htf_data
                    if score is None:
                        continue

                # Update history
                self._ccy_score_history[ccy][tf].append(score)

                # Arrow
                history = self._ccy_score_history[ccy][tf]
                if len(history) >= 5:
                    delta = history[-1] - history[0]
                    if delta > 0.3:
                        arrow, _ = _ARROW_UP
                    elif delta < -0.3:
                        arrow, _ = _ARROW_DOWN
                    else:
                        arrow, _ = _ARROW_FLAT
                else:
                    arrow, _ = _ARROW_FLAT

                # Format: Stoch scores are 0-10, display without +/- sign
                bg, fg = _cell_colors_stoch(score)
                text = f"{score:.1f} {arrow}"

                item = self._ccy_table.item(row, col)
                if item:
                    item.setText(text)
                    item.setBackground(QColor(bg))
                    item.setForeground(QColor(fg))

        # ── Force repaint of both tables (2026-04-21 safety net) ──
        # Defensive fix for the "grid frozen but data flowing" symptom.
        # item.setText/setBackground normally triggers a paint event via
        # Qt's dataChanged signal, but after certain state transitions
        # (tab switches, minimize/restore, spurious QAbstractItemModel
        # glitches) Qt can silently stop delivering paint events. An
        # explicit viewport().update() is cheap and forces the redraw.
        try:
            self._table.viewport().update()
            self._ccy_table.viewport().update()
        except Exception:
            pass

        # ── Currency-based divergence alerts ──
        # Collect per-TF currency scores
        ccy_per_tf: dict[str, dict[str, float]] = {}  # ccy -> {tf: score}
        for ccy in CURRENCIES:
            ccy_per_tf[ccy] = {}
            for tf in TIMEFRAME_LABELS:
                tf_result = result.timeframes.get(tf)
                if tf_result and ccy in tf_result.currency_scores:
                    ccy_per_tf[ccy][tf] = tf_result.currency_scores[ccy]

        # For each display pair, check if base is strong + quote is weak
        for pair in DISPLAY_PAIRS:
            base, quote = pair[:3], pair[3:]
            base_scores = ccy_per_tf.get(base, {})
            quote_scores = ccy_per_tf.get(quote, {})

            if len(base_scores) < _NUM_TF or len(quote_scores) < _NUM_TF:
                continue

            # Check composite spread
            base_composite = sum(base_scores.values()) / _NUM_TF
            quote_composite = sum(quote_scores.values()) / _NUM_TF
            spread = base_composite - quote_composite

        # ── Frontload M5 true range history on first data cycle ──
        if not self._atr_frontloaded and result.connected:
            self._atr_frontloaded = True
            try:
                import MetaTrader5 as _mt5
                import numpy as _np_fl
                for pair in DISPLAY_PAIRS:
                    m5 = _mt5.copy_rates_from_pos(pair, _mt5.TIMEFRAME_M5, 0, 20)
                    if m5 is not None and len(m5) >= 12:
                        for i in range(len(m5)):
                            h = float(m5[i]["high"])
                            l = float(m5[i]["low"])
                            c_prev = float(m5[i - 1]["close"]) if i > 0 else h
                            tr = max(h - l, abs(h - c_prev), abs(l - c_prev))
                            if pair not in self._tr_history:
                                self._tr_history[pair] = deque(maxlen=12)
                            self._tr_history[pair].append(tr)
                            self._tr_last_bar[pair] = int(m5[i]["time"]) // 300
                logger.info("M5 TR history frontloaded for %d pairs", len(self._tr_history))
            except Exception as e:
                logger.warning("M5 TR frontload failed: %s", e)

        # ── Frontload entry-context histories on first data cycle (2026-04-20) ──
        # Populates M1/M5/M15/H1 bar histories + today's VWAP so the 14 new
        # entry-context fields can be computed IMMEDIATELY instead of waiting
        # 6 hours of runtime for the deques to fill organically.
        if not self._entry_ctx_frontloaded and result.connected:
            self._entry_ctx_frontloaded = True
            try:
                import MetaTrader5 as _mt5fl
                pair_count = {"m1": 0, "m5": 0, "m15": 0, "h1": 0, "vwap": 0}
                _jst_today = datetime.now(_jst()).date()
                for pair in DISPLAY_PAIRS:
                    _pip = 0.01 if "JPY" in pair else 0.0001

                    # M1 bars: 30 for close history + 15 for tick volume ramp
                    m1 = _mt5fl.copy_rates_from_pos(pair, _mt5fl.TIMEFRAME_M1, 0, 30)
                    if m1 is not None and len(m1) >= 2:
                        closes = [float(b["close"]) for b in m1]
                        self._m1_close_history[pair] = deque(closes, maxlen=30)
                        # Derive trailing directions from consecutive closes
                        dirs = []
                        for i in range(1, len(closes)):
                            delta = closes[i] - closes[i - 1]
                            if abs(delta) < 0.5 * _pip:
                                dirs.append(0)
                            else:
                                dirs.append(1 if delta > 0 else -1)
                        self._m1_direction_history[pair] = deque(dirs[-10:], maxlen=10)
                        # Use the TIME field (Unix seconds) as bar key — matches
                        # the check in the per-cycle update block
                        self._m1_close_last_bar[pair] = int(m1[-1]["time"])
                        # Tick volume history (reuse existing deque)
                        tvs = [int(b["tick_volume"]) for b in m1[-15:]]
                        if pair not in self._m1_tick_volume_history:
                            self._m1_tick_volume_history[pair] = deque(maxlen=15)
                        self._m1_tick_volume_history[pair].clear()
                        self._m1_tick_volume_history[pair].extend(tvs)
                        pair_count["m1"] += 1

                    # M5 bars: 6 (high, low, close) tuples
                    m5 = _mt5fl.copy_rates_from_pos(pair, _mt5fl.TIMEFRAME_M5, 0, 6)
                    if m5 is not None and len(m5) >= 1:
                        bars = [(float(b["high"]), float(b["low"]), float(b["close"]))
                                for b in m5]
                        self._m5_bar_history[pair] = deque(bars, maxlen=6)
                        self._m5_bar_last_key[pair] = int(m5[-1]["time"]) // 300
                        pair_count["m5"] += 1

                    # M15 bars: 25 closes (for BB 20 + 5-bar lookback)
                    m15 = _mt5fl.copy_rates_from_pos(pair, _mt5fl.TIMEFRAME_M15, 0, 25)
                    if m15 is not None and len(m15) >= 1:
                        closes15 = [float(b["close"]) for b in m15]
                        self._m15_close_history[pair] = deque(closes15, maxlen=25)
                        self._m15_close_last_bar[pair] = int(m15[-1]["time"]) // 900
                        pair_count["m15"] += 1

                    # H1 bars: 30 (high, low, close) tuples — for ADX(14)
                    h1 = _mt5fl.copy_rates_from_pos(pair, _mt5fl.TIMEFRAME_H1, 0, 30)
                    if h1 is not None and len(h1) >= 1:
                        h1bars = [(float(b["high"]), float(b["low"]), float(b["close"]))
                                  for b in h1]
                        self._h1_bar_history[pair] = deque(h1bars, maxlen=30)
                        self._h1_bar_last_key[pair] = int(h1[-1]["time"]) // 3600
                        pair_count["h1"] += 1

                    # Session VWAP: replay today's M1 bars to accumulate sum(tpv), sum(vol)
                    # Fetch 1440 M1 bars (24h max) and filter to today's JST date.
                    today_m1 = _mt5fl.copy_rates_from_pos(pair, _mt5fl.TIMEFRAME_M1, 0, 1440)
                    if today_m1 is not None and len(today_m1) >= 1:
                        sum_tpv = 0.0
                        sum_vol = 0.0
                        for b in today_m1:
                            bar_dt = datetime.fromtimestamp(int(b["time"]), tz=_jst())
                            if bar_dt.date() != _jst_today:
                                continue
                            h = float(b["high"]); l = float(b["low"]); c = float(b["close"])
                            v = float(int(b["tick_volume"]) or 0)
                            if v > 0 and h > 0 and l > 0 and c > 0:
                                sum_tpv += ((h + l + c) / 3.0) * v
                                sum_vol += v
                        self._session_vwap[pair] = {
                            "date": _jst_today,
                            "sum_tpv": sum_tpv,
                            "sum_vol": sum_vol,
                        }
                        if sum_vol > 0:
                            pair_count["vwap"] += 1

                logger.info(
                    "Entry-context histories frontloaded — "
                    "M1:%d pairs, M5:%d, M15:%d, H1:%d, VWAP:%d",
                    pair_count["m1"], pair_count["m5"], pair_count["m15"],
                    pair_count["h1"], pair_count["vwap"],
                )
            except Exception as e:
                logger.warning("Entry-context frontload failed: %s", e)
                # Don't block — deques will fill organically as bars roll over

        # ── Track spread stability + ATR expansion per pair ──
        for pair in DISPLAY_PAIRS:
            base, quote = pair[:3], pair[3:]
            b = result.composite_scores.get(base, 5.0)
            q = result.composite_scores.get(quote, 5.0)
            sp = b - q
            if pair not in self._spread_history:
                self._spread_history[pair] = deque(maxlen=30)
            self._spread_history[pair].append(sp)
            # Track M5 true range — record once per M5 bar
            m5_key = result.m1_bar_time // 300  # 5-minute bucket
            if pair not in self._tr_history:
                self._tr_history[pair] = deque(maxlen=12)  # last hour
                self._tr_last_bar[pair] = 0
            if m5_key != self._tr_last_bar.get(pair, 0):
                self._tr_last_bar[pair] = m5_key
                # Compute true range from high/low/close
                h = result.high_prices.get(pair, 0)
                l = result.low_prices.get(pair, 0)
                if h > 0 and l > 0:
                    tr = h - l  # simplified TR for M5 bar
                    self._tr_history[pair].append(tr)

            # ── Deep analytics: M1 tick volume rolling history ──
            # One reading per M1 bar — the COMPLETED bar's final volume.
            #
            # BUG #2 FIX (2026-04-20): the old guard `hist[-1] != tv` was broken
            # — `result.tick_volumes[pair]` is the RUNNING total of the in-progress
            # M1 bar (monotonically increasing as ticks arrive), so every cycle saw
            # a different value and appended. The deque filled with mid-bar snapshots
            # of one single bar, making `entry_tick_volume_ratio` + `entry_volume_ramp_5m`
            # useless. New logic: track last-seen in-progress volume per pair; on M1
            # rollover, append the PREVIOUS bar's peak volume (which was its final).
            m1_key = result.m1_bar_time
            if pair not in self._m1_tick_volume_history:
                self._m1_tick_volume_history[pair] = deque(maxlen=15)
            tv = result.tick_volumes.get(pair, 0)
            last_bar_key = self._m1_tv_last_bar.get(pair, 0)
            if m1_key != last_bar_key:
                # Rollover detected. Append the completed bar's final volume.
                completed_tv = self._m1_tv_last_seen.get(pair, 0)
                if completed_tv > 0 and last_bar_key > 0:
                    self._m1_tick_volume_history[pair].append(completed_tv)
                self._m1_tv_last_bar[pair] = m1_key
            # Always update last-seen volume for the currently-forming bar
            if tv > 0:
                self._m1_tv_last_seen[pair] = tv

            # ── Deep analytics: H1 ATR rolling history ──
            # One reading per H1 bar
            h1_atr_val = result.h1_atr.get(pair, 0.0)
            h1_key = result.m1_bar_time // 3600
            if pair not in self._h1_atr_history:
                self._h1_atr_history[pair] = deque(maxlen=20)
                self._h1_atr_last_bar[pair] = 0
            if h1_atr_val > 0 and h1_key != self._h1_atr_last_bar.get(pair, 0):
                self._h1_atr_last_bar[pair] = h1_key
                self._h1_atr_history[pair].append(h1_atr_val)

            # ── NEW 2026-04-20: H1 (H, L, C) bar history for ADX computation ──
            # BUG #3 FIX: the old code used `h1fresh[0]` (just-completed) and stored
            # its hour key as `_h1_bar_last_key`. But frontload ALSO stored the
            # FORMING bar's key as `_h1_bar_last_key`. Post-rollover the two keys
            # matched, so the "latest_key != stored_key" guard was always False and
            # the deque never advanced — ADX stayed frozen.
            #
            # Correct semantics:
            #   `_h1_bar_last_key` = hour key of the CURRENTLY-FORMING bar
            #   rollover detected via `result.m1_bar_time // 3600 > last_key`
            #   on rollover: frontload stored an incomplete forming bar as the
            #     deque's tail. Replace it with the now-finalized bar, then update
            #     `_h1_bar_last_key` to the new forming hour.
            if pair in self._h1_bar_history and h1_key > self._h1_bar_last_key.get(pair, 0):
                try:
                    import MetaTrader5 as _mt5h1
                    # Fetch 2 bars: [just-completed, new-forming]. MT5 returns
                    # time-ASCENDING so index [-1] = newest = currently forming.
                    h1fresh = _mt5h1.copy_rates_from_pos(pair, _mt5h1.TIMEFRAME_H1, 0, 2)
                    if h1fresh is not None and len(h1fresh) >= 2:
                        completed = h1fresh[0]
                        forming = h1fresh[-1]
                        completed_key = int(completed["time"]) // 3600
                        forming_key = int(forming["time"]) // 3600
                        stored_key = self._h1_bar_last_key.get(pair, 0)
                        dq = self._h1_bar_history[pair]
                        if completed_key == stored_key and len(dq):
                            # Our deque's tail holds the then-forming (now-completed)
                            # bar as captured at frontload time — replace with its
                            # finalized H/L/C.
                            dq.pop()
                            dq.append((
                                float(completed["high"]),
                                float(completed["low"]),
                                float(completed["close"]),
                            ))
                        elif completed_key > stored_key:
                            # Missed one or more hours — just append the latest completed.
                            dq.append((
                                float(completed["high"]),
                                float(completed["low"]),
                                float(completed["close"]),
                            ))
                        # Advance stored key to point at the new forming bar
                        self._h1_bar_last_key[pair] = forming_key
                except Exception:
                    pass  # ADX stays on last known bars

            # ── NEW 2026-04-20: M1 close + direction history ──
            # Records one point per M1 bar. Used for:
            #   range_compression (std 10 / std 30)
            #   m1_consec_aligned (trailing bar directions)
            close_now = result.close_prices.get(pair, 0.0)
            if pair not in self._m1_close_history:
                self._m1_close_history[pair] = deque(maxlen=30)
                self._m1_direction_history[pair] = deque(maxlen=10)
                self._m1_close_last_bar[pair] = 0
            if close_now > 0 and m1_key != self._m1_close_last_bar.get(pair, 0):
                # New M1 bar: append current close + derive direction from previous close
                prev_hist = self._m1_close_history[pair]
                if prev_hist:
                    delta = close_now - prev_hist[-1]
                    pip = 0.01 if "JPY" in pair else 0.0001
                    # Treat moves < 0.5 pip as doji (0), else +1/-1
                    if abs(delta) < 0.5 * pip:
                        direction = 0
                    else:
                        direction = 1 if delta > 0 else -1
                    self._m1_direction_history[pair].append(direction)
                prev_hist.append(close_now)
                self._m1_close_last_bar[pair] = m1_key

            # ── NEW 2026-04-20: M5 bar (high, low, close) history ──
            # Used for: m5_higher_highs/lows, m5_close_strength (close position in bar range)
            if pair not in self._m5_bar_history:
                self._m5_bar_history[pair] = deque(maxlen=6)
                self._m5_bar_last_key[pair] = 0
            if m5_key != self._m5_bar_last_key.get(pair, 0):
                h = result.high_prices.get(pair, 0.0)
                l = result.low_prices.get(pair, 0.0)
                c = close_now
                if h > 0 and l > 0 and c > 0:
                    self._m5_bar_history[pair].append((h, l, c))
                    self._m5_bar_last_key[pair] = m5_key

            # ── NEW 2026-04-20: M15 close history (for Bollinger Bands) ──
            m15_key = result.m1_bar_time // 900
            if pair not in self._m15_close_history:
                self._m15_close_history[pair] = deque(maxlen=25)
                self._m15_close_last_bar[pair] = 0
            if close_now > 0 and m15_key != self._m15_close_last_bar.get(pair, 0):
                self._m15_close_history[pair].append(close_now)
                self._m15_close_last_bar[pair] = m15_key

            # ── NEW 2026-04-20: composite-spread history for velocity calc ──
            # One sample per _on_data cycle (NOT per bar) — for sub-second slope.
            # Tuple (timestamp, spread) — spread already computed above as `sp`.
            if pair not in self._composite_spread_history:
                # maxlen 120 @ ~1s per _on_data cycle → covers ~120s of samples,
                # enough to compute a real 90-second slope for entry_composite_vel_90s.
                # (Fix BUG #5: was maxlen=20 which only held ~20s, making the "90s"
                # velocity field effectively a 20s slope with misleading name.)
                self._composite_spread_history[pair] = deque(maxlen=120)
            self._composite_spread_history[pair].append((time.time(), sp))

            # ── NEW 2026-04-20: session VWAP accumulator ──
            # Resets at JST midnight (simple daily VWAP). Accumulates ONE entry
            # per completed M1 bar (on rollover) using the previous bar's snapshot
            # of typical*vol captured from result — at rollover, `result` describes
            # the NEW bar's first tick, but `_vwap_prev_sample[pair]` holds the
            # previous bar's last-seen snapshot, which is the most-accurate
            # close/typical/volume we ever saw for that completed bar.
            #
            # BUG #1 FIX (2026-04-20): the old code accumulated sum_tpv += typical*vol
            # every _on_data cycle (~1s), using the same in-progress M1 bar's running
            # volume. Since vol increases monotonically within a bar, the sum became
            # a heavily-overweighted sample of recent prices instead of a proper
            # volume-weighted average.
            if not hasattr(self, "_vwap_prev_sample"):
                self._vwap_prev_sample: dict[str, tuple] = {}
            today_date = datetime.now(_jst()).date()
            vwap_state = self._session_vwap.get(pair)
            if vwap_state is None or vwap_state.get("date") != today_date:
                self._session_vwap[pair] = {
                    "date": today_date, "sum_tpv": 0.0, "sum_vol": 0.0,
                    "last_m1_bar": 0,
                }
                vwap_state = self._session_vwap[pair]
                # Reset prev-sample too (don't carry yesterday's snapshot across midnight)
                self._vwap_prev_sample.pop(pair, None)

            h = result.high_prices.get(pair, 0.0)
            l = result.low_prices.get(pair, 0.0)
            c = close_now
            vol = float(result.tick_volumes.get(pair, 0) or 0)

            # On M1 rollover, the PREVIOUS bar's last-seen sample is final. Accumulate it.
            last_m1 = vwap_state.get("last_m1_bar", 0)
            if m1_key != last_m1:
                prev = self._vwap_prev_sample.get(pair)
                if prev is not None and last_m1 > 0:
                    prev_typical, prev_vol = prev
                    if prev_vol > 0:
                        vwap_state["sum_tpv"] += prev_typical * prev_vol
                        vwap_state["sum_vol"] += prev_vol
                vwap_state["last_m1_bar"] = m1_key

            # Always keep the current bar's latest snapshot (overwriting each cycle).
            if h > 0 and l > 0 and c > 0 and vol > 0:
                typical = (h + l + c) / 3.0
                self._vwap_prev_sample[pair] = (typical, vol)

        # ── STND and ACCEL entries DISABLED — using Stoch v2 only ──
        _accel_entry_types: dict[str, str] = {}

        # ── STOCH v2 entries: QM4-style currency strength ──
        if hasattr(result, 'stoch_entry_candidates') and result.stoch_entry_candidates:
            for pair, (stoch_dir, stoch_reason) in result.stoch_entry_candidates.items():
                base, quote = pair[:3], pair[3:]
                # Enforce minimum divergence spread
                base_comp = result.composite_scores.get(base, 5.0)
                quote_comp = result.composite_scores.get(quote, 5.0)
                spread = abs(base_comp - quote_comp)
                if spread < _MIN_DIVERGENCE_SPREAD:
                    # ── Site 2: GATE_DIVERGENCE_SPREAD (Phase B shadow) ──
                    from takumi_trader.core.shadow_logger import (
                        STATUS_BLOCKED as _SHB, GATE_DIVERGENCE_SPREAD as _GDS,
                    )
                    self._shadow_mark_sv2(
                        result, pair, status=_SHB, gate=_GDS,
                        reason=f"composite spread {spread:.1f} < min {_MIN_DIVERGENCE_SPREAD:.1f}",
                        metadata={
                            "composite_spread": round(spread, 2),
                            "threshold": _MIN_DIVERGENCE_SPREAD,
                            "base_composite": round(base_comp, 2),
                            "quote_composite": round(quote_comp, 2),
                        },
                    )
                    continue
                # Build tf_display from stoch scores
                tf_display = {}
                for tf, scores in result.stoch_scores.items():
                    b_sc = scores.get(base, 5.0)
                    q_sc = scores.get(quote, 5.0)
                    tf_display[tf] = b_sc - q_sc
                # Always use stoch_v2 (overrides standard if both qualify)
                alert_candidates[pair] = (stoch_dir, tf_display)
                _accel_entry_types[pair] = "stoch_v2"

        # ── TUNED entries: looser thresholds for earlier entry ──
        _MIN_DIVERGENCE_SPREAD_TUNED = 10.0
        _tuned_candidates: dict[str, tuple[str, dict[str, float]]] = {}
        if hasattr(result, 'stoch_entry_candidates_tuned') and result.stoch_entry_candidates_tuned:
            for pair, (stoch_dir, stoch_reason) in result.stoch_entry_candidates_tuned.items():
                base, quote = pair[:3], pair[3:]
                base_comp = result.composite_scores.get(base, 5.0)
                quote_comp = result.composite_scores.get(quote, 5.0)
                spread = abs(base_comp - quote_comp)
                if spread < _MIN_DIVERGENCE_SPREAD_TUNED:
                    continue
                tf_display = {}
                for tf, scores in result.stoch_scores.items():
                    b_sc = scores.get(base, 5.0)
                    q_sc = scores.get(quote, 5.0)
                    tf_display[tf] = b_sc - q_sc
                _tuned_candidates[pair] = (stoch_dir, tf_display)

        # ── Track momentum build-up: when did each pair first qualify? ──
        # BUG FIX 2026-04-21: also include LIVE-engine candidates so that a
        # pair the live engine caught mid-bar (before candle-close qualifies
        # it) still gets an entry_momentum_buildup_sec stamp on its trade.
        # Previously, live-only-qualified pairs never entered
        # _pair_first_qualify_time so the stamp stayed at 0.
        _now_ts = time.time()
        _live_std_keys = set(getattr(result, "stoch_entry_candidates_live", {}) or {})
        _live_tuned_keys = set(getattr(result, "stoch_entry_candidates_tuned_live", {}) or {})
        _qualifying_now = (
            set(alert_candidates.keys())
            | set(_tuned_candidates.keys())
            | _live_std_keys
            | _live_tuned_keys
        )
        # New entries: record timestamp
        for _p in _qualifying_now:
            if _p not in self._pair_first_qualify_time:
                self._pair_first_qualify_time[_p] = _now_ts
        # Removed entries: clear timestamp (so next qualification restarts)
        for _p in list(self._pair_first_qualify_time.keys()):
            if _p not in _qualifying_now:
                del self._pair_first_qualify_time[_p]

        # ── Run conviction filter on each candidate ──
        self._filter_engine.settings = self._filter_toolbar.filter_settings
        conviction_results: dict[str, ConvictionResult] = {}
        for pair, (direction, _scores) in alert_candidates.items():
            base, quote = pair[:3], pair[3:]
            strong_ccy = base if direction == "BUY" else quote
            weak_ccy = quote if direction == "BUY" else base
            adr_pct = result.session_range_pct.get(pair, 0.0)

            # Structural data from MT5 worker (key levels)
            _struct_data = result.structural_levels.get(pair)
            _entry_px = result.close_prices.get(pair, 0.0)
            _tp_pips_filter = 0.0
            _sl_pips_filter = 0.0
            if _struct_data:
                _pip = _struct_data.get("pip", 0.0001)
                from takumi_trader.core.pair_algo_settings import get_pair_settings as _gps
                _ps = _gps(pair)
                if _ps and result.h1_atr.get(pair, 0) > 0:
                    _tp_pips_filter = _ps.get("tp_atr", 0.5) * result.h1_atr[pair] / _pip
                    _sl_pips_filter = _ps.get("sl_atr", 0.3) * result.h1_atr[pair] / _pip

            conv = self._filter_engine.evaluate(
                strong_ccy=strong_ccy,
                weak_ccy=weak_ccy,
                pair=pair,
                direction=direction,
                htf_regimes=result.htf_regimes,
                velocity_data=result.velocity_data,
                composite_scores=result.composite_scores,
                structural_data=_struct_data,
                entry_price=_entry_px,
                tp_pips=_tp_pips_filter,
                sl_pips=_sl_pips_filter,
            )
            conviction_results[pair] = conv

        # Run conviction filter on tuned candidates too
        _tuned_conviction: dict[str, ConvictionResult] = {}
        for pair, (direction, _scores) in _tuned_candidates.items():
            if pair in conviction_results:
                _tuned_conviction[pair] = conviction_results[pair]
                continue
            base, quote = pair[:3], pair[3:]
            strong_ccy = base if direction == "BUY" else quote
            weak_ccy = quote if direction == "BUY" else base
            _struct_data = result.structural_levels.get(pair)
            _entry_px = result.close_prices.get(pair, 0.0)
            _tp_pips_filter = 0.0
            _sl_pips_filter = 0.0
            if _struct_data:
                _pip = _struct_data.get("pip", 0.0001)
                from takumi_trader.core.pair_algo_settings import get_pair_settings as _gps2
                _ps2 = _gps2(pair)
                if _ps2 and result.h1_atr.get(pair, 0) > 0:
                    _tp_pips_filter = _ps2.get("tp_atr", 0.5) * result.h1_atr[pair] / _pip
                    _sl_pips_filter = _ps2.get("sl_atr", 0.3) * result.h1_atr[pair] / _pip
            conv = self._filter_engine.evaluate(
                strong_ccy=strong_ccy, weak_ccy=weak_ccy,
                pair=pair, direction=direction,
                htf_regimes=result.htf_regimes,
                velocity_data=result.velocity_data,
                composite_scores=result.composite_scores,
                structural_data=_struct_data,
                entry_price=_entry_px,
                tp_pips=_tp_pips_filter,
                sl_pips=_sl_pips_filter,
            )
            _tuned_conviction[pair] = conv

        # ─────────────────────────────────────────────────────────────
        # LIVE-ENGINE candidate conviction (BUG FIX 2026-04-21)
        # ─────────────────────────────────────────────────────────────
        # Previously, conviction_results was only populated for pairs the
        # CANDLE-CLOSE engine qualified (alert_candidates / _tuned_candidates).
        # The live systems gated on `conviction_results.get(pair)` — so a
        # pair the LIVE engine caught MID-BAR (before the M5 close) was
        # silently skipped because there was no conviction entry for it.
        # Net result: live and candle-close always fired together at M5
        # close, defeating the entire purpose of a live-candle engine.
        # Fix: compute conviction for LIVE-only pairs too. `result.composite_scores`
        # is already LIVE (built from cached_results_live in mt5_worker), so we
        # use the same composite as the candle-close conviction path. The only
        # difference is the candidate SET: live-engine pairs that candle-close
        # didn't qualify also get a conviction entry now.
        _live_pairs_dirs: dict[str, str] = {}
        _live_candidates_std = getattr(result, "stoch_entry_candidates_live", None) or {}
        _live_candidates_tuned = getattr(result, "stoch_entry_candidates_tuned_live", None) or {}
        for _pair_l, (_dir_l, _) in _live_candidates_std.items():
            _live_pairs_dirs[_pair_l] = _dir_l
        for _pair_l, (_dir_l, _) in _live_candidates_tuned.items():
            _live_pairs_dirs.setdefault(_pair_l, _dir_l)

        for _lpair, _ldir in _live_pairs_dirs.items():
            if _lpair in conviction_results:
                continue  # already computed from candle-close path
            base, quote = _lpair[:3], _lpair[3:]
            strong_ccy = base if _ldir == "BUY" else quote
            weak_ccy = quote if _ldir == "BUY" else base
            _struct_data_l = result.structural_levels.get(_lpair)
            _entry_px_l = result.close_prices.get(_lpair, 0.0)
            _tp_pips_l = 0.0
            _sl_pips_l = 0.0
            if _struct_data_l:
                _pip_l = _struct_data_l.get("pip", 0.0001)
                from takumi_trader.core.pair_algo_settings import get_pair_settings as _gps_l
                _ps_l = _gps_l(_lpair)
                if _ps_l and result.h1_atr.get(_lpair, 0) > 0:
                    _tp_pips_l = _ps_l.get("tp_atr", 0.5) * result.h1_atr[_lpair] / _pip_l
                    _sl_pips_l = _ps_l.get("sl_atr", 0.3) * result.h1_atr[_lpair] / _pip_l
            conv = self._filter_engine.evaluate(
                strong_ccy=strong_ccy,
                weak_ccy=weak_ccy,
                pair=_lpair,
                direction=_ldir,
                htf_regimes=result.htf_regimes,
                velocity_data=result.velocity_data,
                composite_scores=result.composite_scores,
                structural_data=_struct_data_l,
                entry_price=_entry_px_l,
                tp_pips=_tp_pips_l,
                sl_pips=_sl_pips_l,
            )
            conviction_results[_lpair] = conv

        # Build full tuned candidates (FULL conviction only)
        # Only include NEWLY entered pairs (transition detection)
        _tuned_current = set(_tuned_candidates.keys())
        _tuned_newly = _tuned_current - self._prev_tuned_pairs
        self._prev_tuned_pairs = _tuned_current

        _tuned_full: dict[str, tuple[str, dict[str, float]]] = {}
        for pair in _tuned_newly:
            if pair not in _tuned_candidates:
                continue
            direction, scores = _tuned_candidates[pair]
            conv = _tuned_conviction.get(pair)
            if conv and conv.tier == "FULL":
                _tuned_full[pair] = (direction, scores)

        # Only fire FULL alerts with sound; DIMMED show quietly
        # Skip standard entries — only stoch_v2 alerts
        full_candidates: dict[str, tuple[str, dict[str, float]]] = {}
        for pair, (direction, scores) in alert_candidates.items():
            if _accel_entry_types.get(pair, "standard") == "standard":
                # Site 7 in the gate map: not shadow-instrumented —
                # _accel_entry_types is always "stoch_v2" for Sv2
                # candidates by construction (set ~line 2002). Defensive
                # only; this branch is logically unreachable for Sv2.
                continue
            conv = conviction_results.get(pair)
            if conv and conv.tier == "FULL":
                full_candidates[pair] = (direction, scores)
            else:
                # ── Site 5: GATE_STRUCTURAL or GATE_CONVICTION (split) ──
                # tier=SUPPRESSED can mean (a) structural HARD BLOCK or
                # (b) low-score DIMMED/SUPPRESSED. _shadow_structural_metadata
                # picks the right gate by checking
                # conv.components["structural"].passed.
                from takumi_trader.core.shadow_logger import STATUS_BLOCKED as _SHB
                gate, reason, meta = self._shadow_structural_metadata(conv)
                meta["offending_side"] = (
                    "above" if direction == "BUY" else "below"
                )
                self._shadow_mark_sv2(
                    result, pair, status=_SHB, gate=gate,
                    reason=reason, metadata=meta,
                )

        # Cache for TRACK button usage
        self._latest_alert_candidates = dict(alert_candidates)
        self._latest_conviction = conviction_results
        if result.close_prices:
            self._latest_close_prices.update(result.close_prices)

        # Share latest close_prices snapshot with all paper traders so the
        # FeatureEngine can compute synthetic baskets (EUR/JPY/GBP/AUD index)
        # and triangular-arb drift at trade-open time. Sharing avoids
        # passing it through every open_paper_trade() call site.
        try:
            _cp_snapshot = dict(result.close_prices)
            for _pt in (
                self._paper_trader, self._paper_trader_ss, self._paper_trader_atr,
                self._paper_trader_a_tuned, self._paper_trader_b_tuned,
                self._paper_trader_qm4, self._paper_trader_breakout,
                self._paper_trader_squeeze, self._paper_trader_squeeze_rev,
                self._paper_trader_divergence, self._paper_trader_dtc_combo,
                self._paper_trader_sv2_live, self._paper_trader_sv2_a_tuned_live,
                self._paper_trader_sv2_ss_live, self._paper_trader_sv2_b_tuned_live,
                self._paper_trader_sv2_atr_live, self._paper_trader_sv2_upgraded,
                self._paper_trader_au1, self._paper_trader_au2, self._paper_trader_au3,
                self._paper_trader_au4, self._paper_trader_au5,
            ):
                _pt._cross_pair_close_cache = _cp_snapshot
        except Exception:
            pass

        # Block all alerts during NO_TRADE session (5:01-7:57 JST)
        try:
            _jst_now_alert = datetime.now(_jst())
            _jst_hm_alert = _jst_now_alert.hour * 60 + _jst_now_alert.minute
            _alerts_blocked = _jst_hm_alert >= 1320 or _jst_hm_alert < 477  # 22:00-7:57 blocked
        except Exception:
            _alerts_blocked = False

        # Fire notifications only for FULL tier alerts (skip during NO_TRADE)
        if _alerts_blocked:
            fire_candidates: dict[str, tuple[str, dict[str, float]]] = {}
        else:
            fire_candidates = {
                p: v for p, v in full_candidates.items()
                if conviction_results.get(p, ConvictionResult()).tier == "FULL"
            }
        open_pairs = set(self._trade_tracker.active_trades.keys())
        fired = self._alert_mgr.check_and_fire(fire_candidates, open_pairs=open_pairs)
        self._update_alert_display(full_candidates, fired, conviction_results)

        # ── Phase B closeout: orphan-PENDING fix (2026-05-05) ──────────
        # alert_mgr.check_and_fire filters fire_candidates against
        # open_pairs BEFORE the per-pair trade-decision loop below runs.
        # Pairs in full_candidates but not fired would otherwise leave
        # their shadow records stuck at STATUS_PENDING. The extracted
        # mark_alert_mgr_orphans() function handles the sweep — see
        # takumi_trader/core/shadow_orphan_marker.py for the full
        # rationale + the test suite at scripts/test_orphan_fix.py.
        from takumi_trader.core.shadow_orphan_marker import (
            mark_alert_mgr_orphans,
        )
        mark_alert_mgr_orphans(
            shadow_logger=self._shadow_logger_sv2,
            result=result,
            full_candidates=full_candidates,
            fired=fired,
            open_pairs=open_pairs,
            trade_tracker=self._trade_tracker,
        )

        # ── Register fired alerts for performance tracking ──
        for pair in fired:
            if pair not in full_candidates:
                continue
            direction, _scores = full_candidates[pair]
            entry_price = result.close_prices.get(pair, 0.0)
            if entry_price <= 0:
                continue
            conv = conviction_results.get(pair)
            base_ccy, quote_ccy = pair[:3], pair[3:]
            self._alert_perf.register_alert(
                pair=pair,
                direction=direction,
                entry_price=entry_price,
                conviction_score=conv.conviction if conv else 0,
                conviction_tier=conv.tier if conv else "FULL",
                session=get_current_session(),
                base_score=result.composite_scores.get(base_ccy, 0.0),
                quote_score=result.composite_scores.get(quote_ccy, 0.0),
            )

        # ── Paper trade auto-open on fired alerts ──
        # NO-TRADE window: 22:00 → 7:57 JST (inclusive both ends).
        # TRADE window:    7:58 → 21:59 JST (inclusive both ends).
        # Gates Sv2 (A), A-tuned (D), SS (B), B-tuned (E), ATR (C) and
        # therefore DTC-combo (which only fires from those source paths).
        try:
            _jst_now_pt = datetime.now(_jst())
            _jst_hm_pt = _jst_now_pt.hour * 60 + _jst_now_pt.minute
            # 478 = 7:58 JST (first allowed minute), 1320 = 22:00 JST (first blocked)
            _paper_trade_allowed = 478 <= _jst_hm_pt < 1320
        except Exception:
            _paper_trade_allowed = True

        # Cache current session once per cycle to avoid mid-loop boundary flips
        _cycle_session = get_current_session()
        if _cycle_session != self._last_session:
            self._session_keys_qm4.clear()
            self._last_session = _cycle_session

        # Helper: stamp entry signal data onto a trade for diagnostics
        def _stamp_entry_signals(trade, pair, conv, spread_std=0.0, direction=""):
            if trade is None:
                return
            base, quote = pair[:3], pair[3:]
            pip = 0.01 if "JPY" in pair else 0.0001
            # Stoch scores per TF — for "-live" systems, stamp from the LIVE
            # engine (which is what actually fired the entry). For candle-close
            # systems, stamp from the candle-close engine. This keeps the
            # entry-time snapshot consistent with the engine that fired.
            _is_live_sys = trade.entry_type.endswith("_live") if trade.entry_type else False
            _score_src = result.stoch_scores_live if _is_live_sys else result.stoch_scores
            # Fallback to candle-close scores if live scores are empty (startup
            # warmup before the live engine has populated all TFs)
            if _is_live_sys and not _score_src:
                _score_src = result.stoch_scores
            for tf, attr_b, attr_q in [
                ("M5", "entry_m5_base", "entry_m5_quote"),
                ("M15", "entry_m15_base", "entry_m15_quote"),
                ("H1", "entry_h1_base", "entry_h1_quote"),
                ("H4", "entry_h4_base", "entry_h4_quote"),
            ]:
                tf_scores = _score_src.get(tf, {})
                # Per-TF fallback: if live engine hasn't cached this TF yet,
                # use the candle-close value rather than leaving 0.0
                if _is_live_sys and not tf_scores:
                    tf_scores = result.stoch_scores.get(tf, {})
                setattr(trade, attr_b, round(tf_scores.get(base, 0.0), 1))
                setattr(trade, attr_q, round(tf_scores.get(quote, 0.0), 1))
            base_comp = result.composite_scores.get(base, 5.0)
            quote_comp = result.composite_scores.get(quote, 5.0)
            trade.entry_div_spread = round(base_comp - quote_comp, 1)
            trade.entry_spread_std = round(spread_std, 2)
            h1_atr = result.h1_atr.get(pair, 0.0)
            trade.entry_h1_atr_pips = round(h1_atr / pip, 1) if h1_atr > 0 else 0.0
            if conv:
                trade.entry_tier = conv.tier
                struct = conv.components.get("structural")
                trade.entry_structural = struct.reason[:40] if struct and not struct.passed else "OK"
            else:
                trade.entry_tier = "?"
                trade.entry_structural = "?"

            # ── Deep analytics: market/portfolio context ──
            import numpy as _np_dx
            # Tick volume ratio (current M1 vol / avg of last 15)
            _tv_hist = self._m1_tick_volume_history.get(pair)
            _cur_tv = result.tick_volumes.get(pair, 0)
            if _tv_hist and len(_tv_hist) >= 3 and _cur_tv > 0:
                _avg_tv = float(_np_dx.mean(list(_tv_hist)))
                trade.entry_tick_volume_ratio = round(_cur_tv / _avg_tv, 2) if _avg_tv > 0 else 0.0

            # Momentum build-up seconds
            _qt = self._pair_first_qualify_time.get(pair)
            if _qt:
                trade.entry_momentum_buildup_sec = int(time.time() - _qt)

            # Distance to key levels (signed pips: positive = level above price)
            _entry_px = trade.entry_price
            _struct = result.structural_levels.get(pair, {})
            for _fld, _key in [
                ("entry_dist_day_high_pips", "prev_day_high"),
                ("entry_dist_day_low_pips", "prev_day_low"),
                ("entry_dist_week_high_pips", "prev_week_high"),
                ("entry_dist_week_low_pips", "prev_week_low"),
                ("entry_dist_month_high_pips", "prev_month_high"),
                ("entry_dist_month_low_pips", "prev_month_low"),
            ]:
                _lvl = _struct.get(_key, 0.0)
                if _lvl > 0 and _entry_px > 0:
                    setattr(trade, _fld, round((_lvl - _entry_px) / pip, 1))

            # Currency cluster count (other pairs with same base or quote
            # signaling same direction). BUG FIX 2026-04-21: merge candle-close
            # candidates with LIVE-engine candidates so the cluster also
            # reflects pairs the live engine identified mid-bar.
            _cluster = 0
            _direction = direction or trade.direction
            _cluster_src: dict[str, tuple[str, dict]] = {}
            for _cp, _cv in alert_candidates.items():
                _cluster_src[_cp] = _cv
            for _cp, _cv in (getattr(result, "stoch_entry_candidates_live", {}) or {}).items():
                _cluster_src.setdefault(_cp, (_cv[0], {}))
            for _cp, _cv in (getattr(result, "stoch_entry_candidates_tuned_live", {}) or {}).items():
                _cluster_src.setdefault(_cp, (_cv[0], {}))
            for _cp, (_cd, _) in _cluster_src.items():
                if _cp == pair:
                    continue
                _cb, _cq = _cp[:3], _cp[3:]
                # Same base currency, same direction means: if GBPUSD BUY (GBP strong),
                # we look for other GBP-base BUYs (GBP strong elsewhere)
                if _cb == base and _cd == _direction:
                    _cluster += 1
                # Or GBP-quote SELLs (GBP strong)
                elif _cq == base and _cd != _direction:
                    _cluster += 1
                # Same weak currency logic
                elif _cb == quote and _cd != _direction:
                    _cluster += 1
                elif _cq == quote and _cd == _direction:
                    _cluster += 1
            trade.entry_cluster_count = _cluster

            # Distance to nearest round numbers (100-pip and 1000-pip levels)
            # 100 pips = 0.01 (non-JPY) or 1.00 (JPY)
            # 1000 pips = 0.1 (non-JPY) or 10.0 (JPY)
            if _entry_px > 0:
                _step_100 = 100 * pip  # 100 pips in price units
                _step_1000 = 1000 * pip
                _nearest_100 = round(_entry_px / _step_100) * _step_100
                _nearest_1000 = round(_entry_px / _step_1000) * _step_1000
                trade.entry_dist_00_pips = round((_nearest_100 - _entry_px) / pip, 1)
                trade.entry_dist_000_pips = round((_nearest_1000 - _entry_px) / pip, 1)

            # Session minutes + day of week
            from takumi_trader.core.session_manager import minutes_since_session_start as _mins_sess
            trade.entry_session_minutes_in = _mins_sess(trade.entry_time)
            trade.entry_day_of_week = datetime.fromtimestamp(trade.entry_time, tz=_jst()).weekday()

            # Concurrent trades across all systems (10 candle-close +
            # 5 live-candle paper trackers = 15 total)
            _conc = (
                len(self._trade_tracker.active_trades) +
                len(self._trade_tracker_ss.active_trades) +
                len(self._trade_tracker_atr.active_trades) +
                len(self._trade_tracker_qm4.active_trades) +
                len(self._trade_tracker_a_tuned.active_trades) +
                len(self._trade_tracker_b_tuned.active_trades) +
                len(self._trade_tracker_breakout.active_trades) +
                len(self._trade_tracker_squeeze.active_trades) +
                len(self._trade_tracker_squeeze_rev.active_trades) +
                len(self._trade_tracker_divergence.active_trades) +
                len(self._trade_tracker_dtc_combo.active_trades) +
                len(self._trade_tracker_sv2_live.active_trades) +
                len(self._trade_tracker_sv2_a_tuned_live.active_trades) +
                len(self._trade_tracker_sv2_ss_live.active_trades) +
                len(self._trade_tracker_sv2_b_tuned_live.active_trades) +
                len(self._trade_tracker_sv2_atr_live.active_trades) +
                len(self._trade_tracker_sv2_upgraded.active_trades) +
                # AU Gold suite (2026-04-24)
                len(self._trade_tracker_au1.active_trades) +
                len(self._trade_tracker_au2.active_trades) +
                len(self._trade_tracker_au3.active_trades) +
                len(self._trade_tracker_au4.active_trades) +
                len(self._trade_tracker_au5.active_trades)
            )
            trade.entry_concurrent_trades = _conc

            # Previous trade result on same pair (same system's journal)
            _sys_pt_map = {
                "stoch_v2": self._paper_trader,
                "sv2_ss": self._paper_trader_ss,
                "sv2_atr": self._paper_trader_atr,
                "sv2_qm4": self._paper_trader_qm4,
                "sv2_a_tuned": self._paper_trader_a_tuned,
                "sv2_b_tuned": self._paper_trader_b_tuned,
                "breakout": self._paper_trader_breakout,
                "squeeze": self._paper_trader_squeeze,
                "squeeze_rev": self._paper_trader_squeeze_rev,
                "divergence": self._paper_trader_divergence,
                "dtc_combo": self._paper_trader_dtc_combo,
                # Live-candle systems (2026-04-21)
                "sv2_live":         self._paper_trader_sv2_live,
                "sv2_a_tuned_live": self._paper_trader_sv2_a_tuned_live,
                "sv2_ss_live":      self._paper_trader_sv2_ss_live,
                "sv2_b_tuned_live": self._paper_trader_sv2_b_tuned_live,
                "sv2_atr_live":     self._paper_trader_sv2_atr_live,
                # Sv2-upgraded (2026-04-23)
                "sv2_upgraded":     self._paper_trader_sv2_upgraded,
                # AU Gold suite (2026-04-24)
                "au1_london_breakout": self._paper_trader_au1,
                "au2_ny_orb":          self._paper_trader_au2,
                "au3_trend_pullback":  self._paper_trader_au3,
                "au4_usd_divergence":  self._paper_trader_au4,
                "au5_asian_mean_rev":  self._paper_trader_au5,
            }
            _sys_pt = _sys_pt_map.get(trade.entry_type)
            if _sys_pt and _sys_pt._journal:
                for _r in reversed(_sys_pt._journal):
                    if _r.pair == pair and _r.close_reason:
                        trade.entry_prev_trade_result = "win" if _r.is_win else "loss"
                        break

            # M1 candle body/direction
            _m1_h = result.high_prices.get(pair, 0.0)
            _m1_l = result.low_prices.get(pair, 0.0)
            _m1_c = result.close_prices.get(pair, 0.0)
            _m1_o = _entry_px  # approximation: entry price ~ open of current M1
            # Actually we don't have M1 open stored. Use high-low as range; body = |close - open|
            # Without open, we use: bull if close > previous close. As proxy, use close vs entry.
            if _m1_h > 0 and _m1_l > 0 and _m1_h > _m1_l:
                _range = _m1_h - _m1_l
                # Body approximation: distance of close from midpoint × 2
                _mid = (_m1_h + _m1_l) / 2
                _body = abs(_m1_c - _mid) * 2
                trade.entry_m1_body_pct = round(min(100.0, _body / _range * 100), 1)
                if _m1_c > _mid + _range * 0.1:
                    trade.entry_m1_direction = "bull"
                elif _m1_c < _mid - _range * 0.1:
                    trade.entry_m1_direction = "bear"
                else:
                    trade.entry_m1_direction = "doji"

            # ATR ratio: current H1 ATR / avg of last 20 H1 ATRs
            _atr_hist = self._h1_atr_history.get(pair)
            if _atr_hist and len(_atr_hist) >= 3 and h1_atr > 0:
                _avg_atr = float(_np_dx.mean(list(_atr_hist)))
                trade.entry_atr_ratio = round(h1_atr / _avg_atr, 2) if _avg_atr > 0 else 0.0

            # ── Conviction component breakdown (4 sub-scores) ──
            try:
                if conv and hasattr(conv, "components") and conv.components:
                    _comp_t = conv.components.get("trend_regime")
                    _comp_v = conv.components.get("strength_velocity")
                    _comp_i = conv.components.get("isolation")
                    _comp_s = conv.components.get("structural")
                    trade.entry_conv_trend = int(getattr(_comp_t, "score", 0) or 0)
                    trade.entry_conv_velocity = int(getattr(_comp_v, "score", 0) or 0)
                    trade.entry_conv_isolation = int(getattr(_comp_i, "score", 0) or 0)
                    trade.entry_conv_structural = int(getattr(_comp_s, "score", 0) or 0)
                    # Strong/weak ccy from conv
                    trade.entry_strong_ccy = getattr(conv, "strong_ccy", "") or ""
                    trade.entry_weak_ccy = getattr(conv, "weak_ccy", "") or ""
                    # Isolation details: ranks + gaps
                    if _comp_i and getattr(_comp_i, "details", None):
                        _id = _comp_i.details
                        trade.entry_strong_rank = int(_id.get("strong_rank", 0) or 0)
                        trade.entry_weak_rank = int(_id.get("weak_rank", 0) or 0)
                        trade.entry_strong_top_gap = float(_id.get("top_gap", 0.0) or 0.0)
                        trade.entry_weak_bottom_gap = float(_id.get("bottom_gap", 0.0) or 0.0)
                    # Velocity details: per-ccy velocity in points/min
                    if _comp_v and getattr(_comp_v, "details", None):
                        _vd = _comp_v.details
                        trade.entry_strong_velocity = round(float(_vd.get("strong_vel", 0.0) or 0.0), 3)
                        trade.entry_weak_velocity = round(float(_vd.get("weak_vel", 0.0) or 0.0), 3)
            except Exception as _conv_ex:
                logger.debug("[STAMP] Conviction breakdown error %s: %s", pair, _conv_ex)

            # ── Pair-specific SL/TP ATR multipliers ──
            try:
                from takumi_trader.core.pair_algo_settings import get_pair_settings as _gps
                _ps = _gps(pair) or {}
                trade.entry_sl_atr_mult = float(_ps.get("sl_atr", 0.0) or 0.0)
                trade.entry_tp_atr_mult = float(_ps.get("tp_atr", 0.0) or 0.0)
            except Exception:
                pass

            # ── M5 True Range slope ratio (recent 3-bar avg / prev 3-bar avg) ──
            try:
                _tr_hist = getattr(self, "_tr_history", {}).get(pair)
                if _tr_hist and len(_tr_hist) >= 6:
                    _trl = list(_tr_hist)
                    _r_avg = float(_np_dx.mean(_trl[-3:]))
                    _o_avg = float(_np_dx.mean(_trl[-6:-3]))
                    if _o_avg > 0:
                        trade.entry_m5_tr_slope_ratio = round(_r_avg / _o_avg, 3)
            except Exception:
                pass

            # ── Minutes since last RED news on either currency ──
            try:
                if self._news_filter.loaded:
                    _now_ts = time.time()
                    _events = getattr(self._news_filter, "_events", []) or []
                    _candidates = [
                        float(e["time"]) for e in _events
                        if e.get("time", 0) <= _now_ts
                        and e.get("currency") in (base, quote)
                    ]
                    if _candidates:
                        trade.entry_minutes_since_news = round(
                            (_now_ts - max(_candidates)) / 60.0, 1
                        )
            except Exception:
                pass

            # ═══════════════════════════════════════════════════════════
            # NEW momentum / trend-start signals (added 2026-04-20).
            # Stats-only. NOT used as filters yet. Defaults = "not measured"
            # when history is shallow or data is missing; safe to ignore in
            # analysis queries by checking the default sentinel.
            # ═══════════════════════════════════════════════════════════
            _td = trade.direction  # "BUY" or "SELL"
            _is_buy = (_td == "BUY")

            # ── (1) M1 consecutive bars aligned with trade direction ──
            try:
                hist_dir = self._m1_direction_history.get(pair)
                if hist_dir:
                    want = 1 if _is_buy else -1
                    count = 0
                    for d in reversed(hist_dir):
                        if d == want:
                            count += 1
                        else:
                            break
                    # If leading bars are AGAINST us, report as negative
                    if count == 0 and hist_dir and hist_dir[-1] == -want:
                        against = 0
                        for d in reversed(hist_dir):
                            if d == -want:
                                against += 1
                            else:
                                break
                        count = -against
                    trade.entry_m1_consec_aligned = count
            except Exception:
                pass

            # ── (2) Composite-spread velocity (pts/min) over last ~90s ──
            try:
                hist_sp = self._composite_spread_history.get(pair)
                if hist_sp and len(hist_sp) >= 3:
                    now_ts = time.time()
                    window = [(ts, v) for ts, v in hist_sp if now_ts - ts <= 90.0]
                    if len(window) >= 2:
                        t0, v0 = window[0]
                        t1, v1 = window[-1]
                        dt_min = (t1 - t0) / 60.0
                        if dt_min > 0:
                            # Sign so that +velocity = expanding in trade direction.
                            # div_spread = base - quote; BUY wants spread widening.
                            slope = (v1 - v0) / dt_min
                            trade.entry_composite_vel_90s = round(
                                slope if _is_buy else -slope, 3
                            )
            except Exception:
                pass

            # ── (3) M5 higher-highs / higher-lows (last 3 M5 bars) ──
            try:
                m5_hist = self._m5_bar_history.get(pair)
                if m5_hist and len(m5_hist) >= 3:
                    last3 = list(m5_hist)[-3:]
                    highs = [b[0] for b in last3]
                    lows = [b[1] for b in last3]
                    if _is_buy:
                        trade.entry_m5_higher_highs = (highs[0] < highs[1] < highs[2])
                        trade.entry_m5_higher_lows  = (lows[0]  < lows[1]  < lows[2])
                    else:
                        # For SELL: "Lower Highs / Lower Lows" stored in same fields
                        trade.entry_m5_higher_highs = (highs[0] > highs[1] > highs[2])
                        trade.entry_m5_higher_lows  = (lows[0]  > lows[1]  > lows[2])
            except Exception:
                pass

            # ── (4) Distance to session VWAP, signed, in pips ──
            try:
                vwap_state = self._session_vwap.get(pair)
                if vwap_state and vwap_state.get("sum_vol", 0) > 0:
                    vwap_price = vwap_state["sum_tpv"] / vwap_state["sum_vol"]
                    current_price = result.close_prices.get(pair, 0.0)
                    if current_price > 0:
                        raw = current_price - vwap_price
                        # Sign so that +value = price is on the "trade-direction" side of VWAP
                        trade.entry_vwap_dist_pips = round(
                            (raw if _is_buy else -raw) / pip, 1
                        )
            except Exception:
                pass

            # ── (5) ADX on H1 — standard Wilder 14-period ──
            # Implemented 2026-04-20 now that _h1_bar_history is populated.
            # Needs at least 2*period+1 = 29 bars to produce a valid reading.
            # Output range is 0..100; > 25 = trending, < 20 = ranging.
            try:
                h1bars = self._h1_bar_history.get(pair)
                if h1bars and len(h1bars) >= 29:
                    import numpy as _np_adx
                    bars = list(h1bars)
                    highs = _np_adx.array([b[0] for b in bars], dtype=float)
                    lows = _np_adx.array([b[1] for b in bars], dtype=float)
                    closes = _np_adx.array([b[2] for b in bars], dtype=float)
                    period = 14
                    # True Range + Directional Movement (per-bar values)
                    n = len(bars)
                    tr = _np_adx.zeros(n - 1)
                    plus_dm = _np_adx.zeros(n - 1)
                    minus_dm = _np_adx.zeros(n - 1)
                    for i in range(1, n):
                        tr[i - 1] = max(
                            highs[i] - lows[i],
                            abs(highs[i] - closes[i - 1]),
                            abs(lows[i] - closes[i - 1]),
                        )
                        up = highs[i] - highs[i - 1]
                        down = lows[i - 1] - lows[i]
                        plus_dm[i - 1] = up if (up > down and up > 0) else 0.0
                        minus_dm[i - 1] = down if (down > up and down > 0) else 0.0
                    # Wilder smoothing — AVERAGE form (keeps values on 0..100 scale).
                    # smoothed[0] = avg(vals[0:p])
                    # smoothed[i] = smoothed[i-1] - smoothed[i-1]/p + vals[i]/p
                    def _wilder_avg(vals, p):
                        out = [sum(vals[:p]) / p]
                        for i in range(p, len(vals)):
                            out.append(out[-1] - out[-1] / p + vals[i] / p)
                        return out
                    tr_s = _wilder_avg(tr.tolist(), period)
                    pdm_s = _wilder_avg(plus_dm.tolist(), period)
                    mdm_s = _wilder_avg(minus_dm.tolist(), period)
                    # DI+ / DI- (100 * ratio)
                    plus_di = [100 * pdm_s[i] / tr_s[i] if tr_s[i] > 0 else 0.0
                               for i in range(len(tr_s))]
                    minus_di = [100 * mdm_s[i] / tr_s[i] if tr_s[i] > 0 else 0.0
                                for i in range(len(tr_s))]
                    # DX = 100 * |DI+ - DI-| / (DI+ + DI-)  (0..100 already)
                    dx = []
                    for i in range(len(plus_di)):
                        s = plus_di[i] + minus_di[i]
                        dx.append(100 * abs(plus_di[i] - minus_di[i]) / s if s > 0 else 0.0)
                    # ADX = Wilder smoothing of DX (still on 0..100)
                    if len(dx) >= period:
                        adx_series = _wilder_avg(dx, period)
                        trade.entry_adx_h1 = round(adx_series[-1], 2)
            except Exception:
                pass

            # ── (6) Bollinger Band position on M15 (0..1) ──
            # ── (7) BB width expansion ratio (current / 5-bar-ago width) ──
            try:
                m15_hist = self._m15_close_history.get(pair)
                if m15_hist and len(m15_hist) >= 20:
                    import numpy as _np_bb
                    closes = list(m15_hist)
                    # Current BB(20, 2σ)
                    last20 = closes[-20:]
                    sma = float(_np_bb.mean(last20))
                    std = float(_np_bb.std(last20))
                    upper = sma + 2 * std
                    lower = sma - 2 * std
                    current_price = result.close_prices.get(pair, closes[-1])
                    if upper > lower:
                        pos = (current_price - lower) / (upper - lower)
                        trade.entry_bb_position_m15 = round(max(0.0, min(1.0, pos)), 3)
                    # BB width expansion vs 5 bars ago
                    if len(closes) >= 25:
                        prior20 = closes[-25:-5]
                        prior_std = float(_np_bb.std(prior20))
                        prior_width = 4 * prior_std  # 2σ + 2σ
                        current_width = 4 * std
                        if prior_width > 0:
                            trade.entry_bb_width_ratio_m15 = round(
                                current_width / prior_width, 3
                            )
            except Exception:
                pass

            # ── (8) Tick flow imbalance (from TickFlowTracker via result.flow_states) ──
            try:
                flow_states = getattr(result, "flow_states", None) or {}
                state = flow_states.get(pair)
                if state is not None:
                    bias = getattr(state, "flow_bias", 0.0)
                    # Sign so +value = flow in trade direction
                    trade.entry_tick_flow_bias = round(
                        bias if _is_buy else -bias, 3
                    )
            except Exception:
                pass

            # ── (9) Volume ramp 5min (last 5 M1 vols / previous 10 M1 vols) ──
            try:
                tv_hist = self._m1_tick_volume_history.get(pair)
                if tv_hist and len(tv_hist) >= 15:
                    tv_list = list(tv_hist)
                    last5 = sum(tv_list[-5:])
                    prev10 = sum(tv_list[-15:-5])
                    if prev10 > 0:
                        # Normalize: last5 is 5 bars, prev10 is 10 bars.
                        # Compare AVERAGE per bar so a ratio of 1.0 = flat.
                        trade.entry_volume_ramp_5m = round(
                            (last5 / 5.0) / (prev10 / 10.0), 2
                        )
            except Exception:
                pass

            # ── (10) Range compression (std last 10 closes / std last 30) ──
            try:
                close_hist = self._m1_close_history.get(pair)
                if close_hist and len(close_hist) >= 30:
                    import numpy as _np_rc
                    closes = list(close_hist)
                    std_10 = float(_np_rc.std(closes[-10:]))
                    std_30 = float(_np_rc.std(closes[-30:]))
                    if std_30 > 0:
                        trade.entry_range_compression = round(std_10 / std_30, 3)
            except Exception:
                pass

            # ── (11) Cross-pair momentum confirmation ──
            # Count how many OTHER pairs sharing a currency with `pair` ALSO
            # fired an SV2-FAMILY alert in this cycle (proxy for currency-wide
            # momentum). NOTE (BUG #10 clarification): `fired` is the stoch-v2
            # fired-pair list from the earlier Sv2 processing block — it does
            # NOT include alt-system (squeeze/breakout/divergence) firings.
            # So for a squeeze/alt trade, this field reads as "# of Sv2 pairs
            # on the same currency that also had an SV2 alert this cycle",
            # which is still a meaningful cluster-confirmation proxy but not
            # the same as "alt-system cluster".
            try:
                other_firing = [
                    fp for fp in fired
                    if fp != pair and (base in fp or quote in fp)
                ]
                trade.entry_cross_pair_confirm = len(other_firing)
            except Exception:
                pass

            # ── (12) Session volume percentile — STUB (needs historical baseline) ──
            # Requires aggregating 90+ days of per-(pair, session, dow) volume
            # data and computing percentile at entry. Field stays at 0.0 until
            # we load such a baseline. Default means "not measured."
            pass

            # ── (13) M5 bar close-strength (close position in bar range) ──
            # For BUY:  1.0 = closed at bar high  /  0.0 = closed at bar low
            # For SELL: 1.0 = closed at bar low   /  0.0 = closed at bar high
            try:
                m5_hist = self._m5_bar_history.get(pair)
                if m5_hist:
                    h, l, c = m5_hist[-1]
                    if h > l:
                        raw = (c - l) / (h - l)  # 0..1 with 1 = close at high
                        trade.entry_m5_close_strength = round(
                            raw if _is_buy else (1.0 - raw), 3
                        )
            except Exception:
                pass

            # ── Propagate all stamped fields to journal record + tracker ──
            # The journal record was created BEFORE stamping (in open_paper_trade),
            # so it has zero values for everything stamped here. Use the generic
            # dataclass-field sync on PaperTrader to transfer every shared field
            # from trade → record and persist BOTH the journal AND the
            # tracked_trades file. This is the RESTART-SAFETY guarantee: any
            # field stamped onto the trade is on disk before this stamp returns.
            # (2026-04-21 refactor: previously this was a ~40-field hand-
            # maintained list that silently dropped any new field added to
            # PaperTradeRecord until someone remembered to add it here.)
            try:
                _pt_for_sync = _sys_pt_map.get(trade.entry_type)
                if _pt_for_sync is not None:
                    _pt_for_sync.sync_trade_to_journal(trade, save=True)
            except Exception as _sync_ex:
                logger.warning("[STAMP] Journal sync failed for %s: %s", pair, _sync_ex)
            # Also persist the TrackedTrade itself so mid-trade crashes don't
            # lose in-memory stamped context. _save_trades writes all 15
            # tracker files (small JSON, no perf concern per-cycle).
            try:
                self._save_trades()
            except Exception as _st_ex:
                logger.debug("[STAMP] _save_trades failed for %s: %s", pair, _st_ex)

        if _paper_trade_allowed:
            for pair in fired:
                if pair not in full_candidates:
                    continue
                # RED news hard block (currency-aware)
                if self._news_filter.loaded and self._news_filter.is_blackout(
                    pair, time.time()
                ):
                    ev = self._news_filter.get_blocking_event(pair, time.time())
                    ev_title = ev["title"] if ev else "RED news"
                    ev_ccy = ev["currency"] if ev else "?"
                    now_news = datetime.now(_jst()).strftime("%H:%M:%S")
                    self._alert_history.appendleft(
                        f'<span style="color:#ff6600;">[{now_news}] ⚠ {pair} '
                        f'BLOCKED — {ev_ccy} {ev_title}</span>'
                    )
                    # ── Site 4: GATE_NEWS (Phase B shadow) ──
                    from takumi_trader.core.shadow_logger import (
                        STATUS_BLOCKED as _SHB, GATE_NEWS as _GN,
                    )
                    self._shadow_mark_sv2(
                        result, pair, status=_SHB, gate=_GN,
                        reason=f"{ev_ccy} {ev_title}",
                        metadata={
                            "event_title": ev_title,
                            "event_currency": ev_ccy,
                            "event_time_iso": (ev.get("time_iso") if ev else ""),
                            "minutes_to_event": (ev.get("minutes_to_event") if ev else 0.0),
                        },
                    )
                    continue
                direction, _ = full_candidates[pair]

                # Only FULL conviction opens paper trades (block DIMMED)
                conv = conviction_results.get(pair)
                if not conv or conv.tier != "FULL":
                    # Site 5 already marked these upstream at the
                    # alert_candidates -> full_candidates filter; this
                    # branch is defensive (full_candidates was built
                    # only from FULL-tier pairs). No shadow re-mark.
                    continue

                entry_price = result.close_prices.get(pair, 0.0)
                if not math.isfinite(entry_price) or entry_price <= 0:
                    # ── Site 6: GATE_INTERNAL (bad entry price) ──
                    from takumi_trader.core.shadow_logger import (
                        STATUS_BLOCKED as _SHB, GATE_INTERNAL as _GI,
                    )
                    self._shadow_mark_sv2(
                        result, pair, status=_SHB, gate=_GI,
                        reason=f"bad entry price: {entry_price!r}",
                        metadata={
                            "entry_price": entry_price,
                            "close_prices_size": len(result.close_prices),
                        },
                    )
                    continue
                conv = conviction_results.get(pair)
                _etype = _accel_entry_types.get(pair, "standard")
                # Skip STND entries — only open paper trades for stoch_v2
                if _etype != "stoch_v2":
                    # Site 7 in the gate map: defensive only — not
                    # shadow-instrumented because _accel_entry_types is
                    # always "stoch_v2" for Sv2 candidates by construction.
                    logger.warning("BLOCKED non-sv2 paper trade: %s %s etype=%s", pair, direction, _etype)
                    continue
                # ── ADR quality gate (Sv2: validated +28% improvement) ──
                _adr_pct = result.session_range_pct.get(pair, 0.0)
                if (_ADR_QUALITY_MAX_PCT is not None
                        and "Sv2" in _ADR_QUALITY_SYSTEMS
                        and _adr_pct > _ADR_QUALITY_MAX_PCT):
                    logger.info(
                        "[Sv2] %s %s skipped — ADR exhausted (%.0f%% > %.0f%%)",
                        direction, pair, _adr_pct, _ADR_QUALITY_MAX_PCT,
                    )
                    # ── Site 8: GATE_ADR (Phase B shadow) ──
                    from takumi_trader.core.shadow_logger import (
                        STATUS_BLOCKED as _SHB, GATE_ADR as _GA,
                    )
                    self._shadow_mark_sv2(
                        result, pair, status=_SHB, gate=_GA,
                        reason=f"ADR exhausted: {_adr_pct:.0f}% > {_ADR_QUALITY_MAX_PCT:.0f}%",
                        metadata={
                            "adr_pct": round(_adr_pct, 2),
                            "threshold": _ADR_QUALITY_MAX_PCT,
                            "system": "Sv2",
                        },
                    )
                    continue
                pt = self._paper_trader.open_paper_trade(
                    pair=pair,
                    direction=direction,
                    entry_price=entry_price,
                    composite_scores=result.composite_scores,
                    conviction=conv.conviction if conv else 0,
                    session=get_current_session(),
                    h1_atr=result.h1_atr.get(pair, 0.0),
                    entry_type=_etype,
                    adr_consumed_pct=result.session_range_pct.get(pair, 0.0),
                )
                if pt is None:
                    # ── Site 9: GATE_DUPLICATE (Phase B shadow) ──
                    # open_paper_trade returns None for either:
                    #   (a) tracker.has_trade(pair) — duplicate position
                    #   (b) entry_type validation fail — already filtered
                    #       at Site 7 above, so unreachable here.
                    # Therefore None ⇒ duplicate. Mark accordingly.
                    from takumi_trader.core.shadow_logger import (
                        STATUS_BLOCKED as _SHB, GATE_DUPLICATE as _GD,
                    )
                    _existing = self._trade_tracker.get_trade(pair)
                    self._shadow_mark_sv2(
                        result, pair, status=_SHB, gate=_GD,
                        reason="duplicate trade for pair (has_trade=True)",
                        metadata={
                            "has_trade": True,
                            "existing_direction": (
                                _existing.direction if _existing else ""
                            ),
                            "existing_trade_age_minutes": (
                                _existing.duration_minutes if _existing else 0.0
                            ),
                        },
                    )
                    continue
                # ── Phase B mark_executed (Sv2 paper lane) ──
                # Trade opened successfully; bind shadow_id to the real
                # trade journal index for Edge Miner cross-table joins.
                if (self._shadow_logger_sv2 is not None
                        and getattr(result, "sv2_shadow_ids", None)):
                    _sid = result.sv2_shadow_ids.get(pair)
                    if _sid is not None:
                        try:
                            from takumi_trader.core.shadow_logger import LANE_PAPER
                            self._shadow_logger_sv2.mark_executed(
                                _sid,
                                lane=LANE_PAPER,
                                ref={
                                    "system": "Sv2",
                                    "journal_idx": getattr(pt, "_journal_idx", -1),
                                },
                            )
                        except Exception as _exc:
                            logger.warning(
                                "[SHADOW] mark_executed failed pair=%s: %s",
                                pair, _exc,
                            )
                _stamp_entry_signals(pt, pair, conv)
                # Log every Sv2 attempt for cross-system comparison
                try:
                    with open(_DATA_DIR / "trade_attempts.log", "a") as _f:
                        _has = self._trade_tracker.has_trade(pair)
                        _f.write(f"{datetime.now(_jst()):%m-%d %H:%M:%S} [Sv2] {direction} {pair} "
                                 f"result={'OPEN' if pt else 'BLOCKED'} "
                                 f"has_trade={_has} conv={conv.conviction if conv else '?'} "
                                 f"tier={conv.tier if conv else '?'} etype={_etype}\n")
                except Exception:
                    pass
                # Debug ADR
                _adr_dbg = result.session_range_pct.get(pair, -1)
                _adr_keys = len(result.session_range_pct)
                try:
                    with open(_DATA_DIR / "adr_debug.log", "a") as _f:
                        _f.write(f"{pair}: adr={_adr_dbg:.1f} keys={_adr_keys} h1_atr={result.h1_atr.get(pair,0):.5f}\n")
                except Exception:
                    pass
                if pt:
                    open_now = datetime.now(_jst()).strftime("%H:%M:%S")
                    dir_c = "#1b8a2a" if direction == "BUY" else "#c62828"
                    _etype_tag = ' <span style="color:#ff9800;font-weight:bold;">⚡ACCEL</span>' if _etype == "acceleration" else ""
                    open_html = (
                        f'<span style="font-size:9pt; color:#666;">[{open_now}]</span> '
                        f'<span style="font-size:10pt; color:#4a6fa5; font-weight:bold;">'
                        f'\U0001f4dd PAPER OPEN</span>{_etype_tag} '
                        f'<span style="font-size:10pt; color:{dir_c}; font-weight:bold;">'
                        f'{direction} {pair}</span> '
                        f'<span style="font-size:9pt; color:#888;">'
                        f'@ {entry_price:.5f}  SL:{pt.sl_pips:.1f}p  TP:{pt.tp_pips:.1f}p  '
                        f'Conv:{conv.conviction if conv else 0}</span>'
                    )
                    self._alert_history.appendleft(open_html)
                    self._mirror_to_mt5(pt, "stoch_v2")
        else:
            # ── Site 3: GATE_NO_TRADE_WINDOW (Phase B shadow) ──
            #
            # NOTE on scoping: NO_TRADE is marked ONLY for pairs in
            # full_candidates (i.e., that survived strength + divergence
            # + conviction). Pairs blocked upstream are already captured
            # with their correct gate. Marking them again as NO_TRADE
            # would corrupt the gate distribution and pollute the journal
            # with ~15K duplicate-reason records/day. The Edge Miner
            # question this scope answers is: "for pairs that SURVIVED
            # all upstream filters, does the NO_TRADE window block
            # trades that would have been profitable?"
            if self._shadow_logger_sv2 is not None and full_candidates:
                from takumi_trader.core.shadow_logger import (
                    STATUS_BLOCKED as _SHB, GATE_NO_TRADE_WINDOW as _GNTW,
                )
                _now_jst_ntw = datetime.now(_jst())
                _hm = _now_jst_ntw.hour * 60 + _now_jst_ntw.minute
                for pair in full_candidates:
                    self._shadow_mark_sv2(
                        result, pair, status=_SHB, gate=_GNTW,
                        reason=(
                            f"Outside Sv2 trade window "
                            f"(JST {_now_jst_ntw:%H:%M}, allowed 07:58-21:59)"
                        ),
                        metadata={
                            "jst_hm": _hm,
                            "allowed_start_min": 478,
                            "allowed_end_min": 1320,
                            "current_session": result.session_label or "",
                        },
                    )

        # ── System A-tuned: Sv2 with looser entry thresholds (tuned signals only) ──
        if _paper_trade_allowed:
            for pair, (direction, _) in _tuned_full.items():
                if self._news_filter.loaded and self._news_filter.is_blackout(pair, time.time()):
                    continue
                entry_price = result.close_prices.get(pair, 0.0)
                if not math.isfinite(entry_price) or entry_price <= 0:
                    continue
                conv = _tuned_conviction.get(pair) or conviction_results.get(pair)
                # ── ADR quality gate (A-tuned: validated +24% improvement) ──
                _adr_pct_at = result.session_range_pct.get(pair, 0.0)
                if (_ADR_QUALITY_MAX_PCT is not None
                        and "A-tuned" in _ADR_QUALITY_SYSTEMS
                        and _adr_pct_at > _ADR_QUALITY_MAX_PCT):
                    logger.info(
                        "[A-tuned] %s %s skipped — ADR exhausted (%.0f%% > %.0f%%)",
                        direction, pair, _adr_pct_at, _ADR_QUALITY_MAX_PCT,
                    )
                    continue
                pt_at = self._paper_trader_a_tuned.open_paper_trade(
                    pair=pair, direction=direction, entry_price=entry_price,
                    composite_scores=result.composite_scores,
                    conviction=conv.conviction if conv else 0,
                    session=get_current_session(),
                    h1_atr=result.h1_atr.get(pair, 0.0),
                    entry_type="sv2_a_tuned",
                    adr_consumed_pct=result.session_range_pct.get(pair, 0.0),
                )
                _stamp_entry_signals(pt_at, pair, conv)
                if pt_at:
                    # cTrader mirror removed — DTC-combo is now the sole
                    # source of cTrader orders. A-tuned remains paper-only.
                    self._mirror_to_mt5(pt_at, "sv2_a_tuned")

        # ── System B: Sv2 + Spread Stability (parallel paper trades) ──
        if _paper_trade_allowed:
            import numpy as _np
            _SPREAD_STD_MAX = 3.0
            for pair in fired:
                if pair not in full_candidates:
                    continue
                # RED news hard block
                if self._news_filter.loaded and self._news_filter.is_blackout(pair, time.time()):
                    continue
                direction, _ = full_candidates[pair]
                conv = conviction_results.get(pair)
                if not conv or conv.tier != "FULL":
                    continue
                _etype = _accel_entry_types.get(pair, "standard")
                if _etype != "stoch_v2":
                    continue
                # Spread stability check — THE key difference from System A
                _spread_hist = self._spread_history.get(pair)
                if _spread_hist and len(_spread_hist) >= 10:
                    _std = float(_np.std(list(_spread_hist)))
                    if _std > _SPREAD_STD_MAX:
                        continue  # too choppy — System B skips
                entry_price = result.close_prices.get(pair, 0.0)
                if not math.isfinite(entry_price) or entry_price <= 0:
                    continue
                _ss_std = float(_np.std(list(_spread_hist))) if _spread_hist and len(_spread_hist) >= 3 else 0.0
                pt_ss = self._paper_trader_ss.open_paper_trade(
                    pair=pair,
                    direction=direction,
                    entry_price=entry_price,
                    composite_scores=result.composite_scores,
                    conviction=conv.conviction if conv else 0,
                    session=get_current_session(),
                    h1_atr=result.h1_atr.get(pair, 0.0),
                    entry_type="sv2_ss",
                    adr_consumed_pct=result.session_range_pct.get(pair, 0.0),
                )
                _stamp_entry_signals(pt_ss, pair, conv, spread_std=_ss_std)
                # Log every SS attempt for cross-system comparison
                try:
                    with open(_DATA_DIR / "trade_attempts.log", "a") as _f:
                        _has = self._trade_tracker_ss.has_trade(pair)
                        _std_val = float(_np.std(list(self._spread_history.get(pair, [0])))) if self._spread_history.get(pair) else -1
                        _f.write(f"{datetime.now(_jst()):%m-%d %H:%M:%S} [SS]  {direction} {pair} "
                                 f"result={'OPEN' if pt_ss else 'BLOCKED'} "
                                 f"has_trade={_has} conv={conv.conviction if conv else '?'} "
                                 f"tier={conv.tier if conv else '?'} std={_std_val:.2f}\n")
                except Exception:
                    pass
                if pt_ss:
                    # cTrader mirror removed — DTC-combo routes to cTrader now.
                    open_now = datetime.now(_jst()).strftime("%H:%M:%S")
                    dir_c = "#1b8a2a" if direction == "BUY" else "#c62828"
                    self._alert_history.appendleft(
                        f'<span style="font-size:9pt; color:#666;">[{open_now}]</span> '
                        f'<span style="font-size:10pt; color:#9c27b0; font-weight:bold;">'
                        f'\U0001f9ea SS OPEN</span> '
                        f'<span style="font-size:10pt; color:{dir_c}; font-weight:bold;">'
                        f'{direction} {pair}</span> '
                        f'<span style="font-size:9pt; color:#888;">'
                        f'@ {entry_price:.5f} StdDev OK</span>'
                    )
                    self._mirror_to_mt5(pt_ss, "sv2_ss")
                    pt_dtc = self._maybe_open_dtc_combo(pt_ss, "sv2_ss", result, conv)
                    if pt_dtc is not None:
                        _stamp_entry_signals(pt_dtc, pair, conv, spread_std=_ss_std)

        # ── System B-tuned: Sv2+SS with looser entry + StdDev=1.5 (tuned signals only) ──
        # StdDev 3.0/3.5 was too lenient (composite spread barely moves over 30s).
        # 1.5 provides real filtering: only enters when the signal is very stable.
        if _paper_trade_allowed:
            import numpy as _np_bt
            _SPREAD_STD_MAX_BT = 1.5
            for pair, (direction, _) in _tuned_full.items():
                if self._news_filter.loaded and self._news_filter.is_blackout(pair, time.time()):
                    continue
                # Spread stability check (same as B but with 3.5 threshold)
                _spread_hist = self._spread_history.get(pair)
                if _spread_hist and len(_spread_hist) >= 10:
                    _std = float(_np_bt.std(list(_spread_hist)))
                    if _std > _SPREAD_STD_MAX_BT:
                        continue
                entry_price = result.close_prices.get(pair, 0.0)
                if not math.isfinite(entry_price) or entry_price <= 0:
                    continue
                conv = _tuned_conviction.get(pair)
                _bt_std = float(_np_bt.std(list(_spread_hist))) if _spread_hist and len(_spread_hist) >= 3 else 0.0
                pt_bt = self._paper_trader_b_tuned.open_paper_trade(
                    pair=pair, direction=direction, entry_price=entry_price,
                    composite_scores=result.composite_scores,
                    conviction=conv.conviction if conv else 0,
                    session=get_current_session(),
                    h1_atr=result.h1_atr.get(pair, 0.0),
                    entry_type="sv2_b_tuned",
                    adr_consumed_pct=result.session_range_pct.get(pair, 0.0),
                )
                _stamp_entry_signals(pt_bt, pair, conv, spread_std=_bt_std)
                if pt_bt:
                    # cTrader mirror removed — DTC-combo routes to cTrader.
                    self._mirror_to_mt5(pt_bt, "sv2_b_tuned")
                    pt_dtc = self._maybe_open_dtc_combo(pt_bt, "sv2_b_tuned", result, conv)
                    if pt_dtc is not None:
                        _stamp_entry_signals(pt_dtc, pair, conv, spread_std=_bt_std)

        # ── System C: Sv2 + ATR Expansion (parallel paper trades) ──
        if _paper_trade_allowed:
            import numpy as _np_c
            # ATR slope: check if M5 true range is TRENDING UP over recent bars.
            # Compare the average of the last 3 bars vs the average of the 3 bars
            # before that. If recent > older, momentum is building — enter.
            # This catches sustained momentum, not just one-bar spikes.
            _ATR_SLOPE_MIN = 1.0  # recent avg must be >= older avg (flat or rising)
            for pair in fired:
                if pair not in full_candidates:
                    continue
                # RED news hard block
                if self._news_filter.loaded and self._news_filter.is_blackout(pair, time.time()):
                    continue
                direction, _ = full_candidates[pair]
                conv = conviction_results.get(pair)
                if not conv or conv.tier != "FULL":
                    continue
                _etype = _accel_entry_types.get(pair, "stoch_v2")
                if _etype != "stoch_v2":
                    continue
                # ATR slope — is M5 true range trending up?
                _tr_hist = self._tr_history.get(pair)
                if _tr_hist and len(_tr_hist) >= 6:
                    _tr_list = list(_tr_hist)
                    _recent_avg = float(_np_c.mean(_tr_list[-3:]))  # last 3 bars (15 min)
                    _older_avg = float(_np_c.mean(_tr_list[-6:-3]))  # 3 bars before (15 min)
                    if _older_avg > 0 and _recent_avg / _older_avg < _ATR_SLOPE_MIN:
                        continue  # M5 TR is contracting or flat — System C skips
                entry_price = result.close_prices.get(pair, 0.0)
                if not math.isfinite(entry_price) or entry_price <= 0:
                    continue
                pt_atr = self._paper_trader_atr.open_paper_trade(
                    pair=pair,
                    direction=direction,
                    entry_price=entry_price,
                    composite_scores=result.composite_scores,
                    conviction=conv.conviction if conv else 0,
                    session=get_current_session(),
                    h1_atr=result.h1_atr.get(pair, 0.0),
                    entry_type="sv2_atr",
                    adr_consumed_pct=result.session_range_pct.get(pair, 0.0),
                )
                _stamp_entry_signals(pt_atr, pair, conv)
                if pt_atr:
                    # System C: paper only (no cTrader)
                    open_now = datetime.now(_jst()).strftime("%H:%M:%S")
                    dir_c = "#1b8a2a" if direction == "BUY" else "#c62828"
                    self._alert_history.appendleft(
                        f'<span style="font-size:9pt; color:#666;">[{open_now}]</span> '
                        f'<span style="font-size:10pt; color:#e65100; font-weight:bold;">'
                        f'\U0001f4c8 ATR OPEN</span> '
                        f'<span style="font-size:10pt; color:{dir_c}; font-weight:bold;">'
                        f'{direction} {pair}</span> '
                        f'<span style="font-size:9pt; color:#888;">'
                        f'@ {entry_price:.5f} ATR expanding</span>'
                    )
                    self._mirror_to_mt5(pt_atr, "sv2_atr")
                    pt_dtc = self._maybe_open_dtc_combo(pt_atr, "sv2_atr", result, conv)
                    if pt_dtc is not None:
                        _stamp_entry_signals(pt_dtc, pair, conv)

        # ═══════════════════════════════════════════════════════════════
        # 5 LIVE-CANDLE SYSTEMS (2026-04-21) — paper-only, no cTrader
        # ═══════════════════════════════════════════════════════════════
        # Mirror A/B/C/D/E filter chains but use the live engine's firing
        # decisions (stoch_entry_candidates_live / _tuned_live) instead of
        # the candle-close engine. Accessible via the LiveCan UI button.
        # Same SL/TP, same conviction gate, same news/ADR/spread filters —
        # the ONLY difference vs A-E is which engine's signal started the
        # entry chain.
        if _paper_trade_allowed:
            import numpy as _np_live

            def _try_open_live_system(
                _candidates: dict,
                _paper_trader,
                _entry_type: str,
                _spread_std_max: float = 0.0,         # 0 = no spread filter; 3.0 = SS; 1.5 = B-tuned
                _require_atr_slope: bool = False,      # for ATR variant
                _adr_gate_system: str | None = None,   # "Sv2" or "A-tuned" — gate on _ADR_QUALITY_MAX_PCT if in _ADR_QUALITY_SYSTEMS
            ) -> None:
                for _lpair, (_ldir, _lreason) in (_candidates or {}).items():
                    # News blackout
                    if self._news_filter.loaded and self._news_filter.is_blackout(_lpair, time.time()):
                        continue
                    # Conviction FULL gate (shared with A-E)
                    _lconv = conviction_results.get(_lpair)
                    if not _lconv or _lconv.tier != "FULL":
                        continue
                    # Spread stability filter (SS / B-tuned variants)
                    _lstd = 0.0
                    if _spread_std_max > 0:
                        _lsh = self._spread_history.get(_lpair)
                        if not _lsh or len(_lsh) < 10:
                            continue
                        _lstd = float(_np_live.std(list(_lsh)))
                        if _lstd > _spread_std_max:
                            continue
                    # ATR slope filter (ATR variant)
                    if _require_atr_slope:
                        _lth = self._tr_history.get(_lpair)
                        if not _lth or len(_lth) < 6:
                            continue
                        _ltl = list(_lth)
                        _lrec = float(_np_live.mean(_ltl[-3:]))
                        _lold = float(_np_live.mean(_ltl[-6:-3]))
                        if _lold <= 0 or _lrec / _lold < 1.0:
                            continue
                    # ADR quality gate — mirrors the candle-close Sv2/A-tuned
                    # check so the live systems track the same constant if
                    # _ADR_QUALITY_MAX_PCT or _ADR_QUALITY_SYSTEMS is retuned.
                    if (_adr_gate_system is not None
                            and _ADR_QUALITY_MAX_PCT is not None
                            and _adr_gate_system in _ADR_QUALITY_SYSTEMS):
                        _ladr = result.session_range_pct.get(_lpair, 0.0)
                        if _ladr > _ADR_QUALITY_MAX_PCT:
                            continue
                    # Entry price sanity
                    _lentry = result.close_prices.get(_lpair, 0.0)
                    if not math.isfinite(_lentry) or _lentry <= 0:
                        continue
                    # Open paper trade
                    _lpt = _paper_trader.open_paper_trade(
                        pair=_lpair, direction=_ldir, entry_price=_lentry,
                        composite_scores=result.composite_scores,
                        conviction=_lconv.conviction if _lconv else 0,
                        session=get_current_session(),
                        h1_atr=result.h1_atr.get(_lpair, 0.0),
                        entry_type=_entry_type,
                        adr_consumed_pct=result.session_range_pct.get(_lpair, 0.0),
                    )
                    if _lpt:
                        _stamp_entry_signals(_lpt, _lpair, _lconv, spread_std=_lstd)

            # System A mirror — Sv2-live (standard thresholds + ADR gate)
            _try_open_live_system(
                result.stoch_entry_candidates_live,
                self._paper_trader_sv2_live,
                "sv2_live",
                _adr_gate_system="Sv2",
            )
            # System D mirror — Sv2-Tun-live (tuned thresholds + ADR gate)
            _try_open_live_system(
                result.stoch_entry_candidates_tuned_live,
                self._paper_trader_sv2_a_tuned_live,
                "sv2_a_tuned_live",
                _adr_gate_system="A-tuned",
            )
            # System B mirror — Sv2+SS-live (standard + spread_std ≤ 3.0)
            _try_open_live_system(
                result.stoch_entry_candidates_live,
                self._paper_trader_sv2_ss_live,
                "sv2_ss_live",
                _spread_std_max=3.0,
            )
            # System E mirror — Sv2+SS-Tun-live (tuned + spread_std ≤ 1.5)
            _try_open_live_system(
                result.stoch_entry_candidates_tuned_live,
                self._paper_trader_sv2_b_tuned_live,
                "sv2_b_tuned_live",
                _spread_std_max=1.5,
            )
            # System C mirror — Sv2+ATR-live (standard + ATR slope ≥ 1.0)
            _try_open_live_system(
                result.stoch_entry_candidates_live,
                self._paper_trader_sv2_atr_live,
                "sv2_atr_live",
                _require_atr_slope=True,
            )

            # ═══════════════════════════════════════════════════════════════
            # Sv2-UPGRADED (2026-04-23) — live-candle engine + three extras
            # ═══════════════════════════════════════════════════════════════
            # Signal source: stoch_entry_candidates_live (same as Sv2-live).
            # Extra gates vs Sv2-live:
            #   A. conviction score ≥ 65 (vs default FULL-tier threshold of 50)
            #   B. revenge cooldown: skip same-pair re-entry within 60 min
            #      after a loss on that pair
            # Exit: BE-stop move at +7p peak for all pairs except
            #   {EURUSD, GBPUSD, NZDUSD, GBPCAD} — handled inside
            #   paper_trader.update_cycle() based on entry_type == "sv2_upgraded".
            # ADR gate: match Sv2-live (uses _adr_gate_system="Sv2")
            _upgraded_candidates = result.stoch_entry_candidates_live or {}
            for _upair, (_udir, _ureason) in _upgraded_candidates.items():
                # ── Inherited shared protections (same as Sv2 / Sv2-live) ──
                # - 07:58-22:00 JST trade window:  via outer `if _paper_trade_allowed:` block
                # - HTF H1/H4/D1 against-trend block:  inside stoch_engine.check_entry()
                # - Velocity against-trade block:  inside stoch_engine.check_entry()
                # (the live engine's check_entry produces stoch_entry_candidates_live
                #  AFTER applying these — pairs that fail are not in the dict)

                # 1. RED news blackout (shared with A-E)
                if self._news_filter.loaded and self._news_filter.is_blackout(_upair, time.time()):
                    continue

                # 2. Conviction gate — TWO requirements:
                #    (a) tier == "FULL" → guarantees structural filter PASSED
                #        (no trading against prev day/week/month H/L) AND all
                #        conviction components (trend/velocity/isolation/structural)
                #        scored sufficient.  This is the "major levels" protection.
                #    (b) conviction >= 65 → tighter than Sv2's default 50,
                #        per the upgrade spec.
                _uconv = conviction_results.get(_upair)
                if not _uconv or _uconv.tier != "FULL" or _uconv.conviction < 65:
                    continue

                # 3. Revenge cooldown: skip same-pair re-entry within 60 min of a loss
                _last = self._sv2_upgraded_last_close.get(_upair)
                if _last is not None:
                    _last_ts, _last_was_win = _last
                    if not _last_was_win and (time.time() - _last_ts) < 60 * 60:
                        logger.info(
                            "[sv2_upgraded] %s %s skipped — revenge cooldown "
                            "(prev loss %.0fm ago)",
                            _udir, _upair, (time.time() - _last_ts) / 60,
                        )
                        continue

                # 4. ADR quality gate (mirrors Sv2-live's _adr_gate_system="Sv2")
                if (_ADR_QUALITY_MAX_PCT is not None
                        and "Sv2" in _ADR_QUALITY_SYSTEMS):
                    _uadr = result.session_range_pct.get(_upair, 0.0)
                    if _uadr > _ADR_QUALITY_MAX_PCT:
                        continue

                # 5. Entry price sanity
                _uentry = result.close_prices.get(_upair, 0.0)
                if not math.isfinite(_uentry) or _uentry <= 0:
                    continue
                # Open paper trade
                _upt = self._paper_trader_sv2_upgraded.open_paper_trade(
                    pair=_upair, direction=_udir, entry_price=_uentry,
                    composite_scores=result.composite_scores,
                    conviction=_uconv.conviction if _uconv else 0,
                    session=get_current_session(),
                    h1_atr=result.h1_atr.get(_upair, 0.0),
                    entry_type="sv2_upgraded",
                    adr_consumed_pct=result.session_range_pct.get(_upair, 0.0),
                )
                if _upt:
                    _stamp_entry_signals(_upt, _upair, _uconv)

            # ═══════════════════════════════════════════════════════════════
            # AU Gold suite (2026-04-24) — 5 XAUUSD strategies, paper-only
            # ═══════════════════════════════════════════════════════════════
            # Isolated from forex CSI calculations. Engine reads result.xau_*
            # fields + result.composite_scores['USD'] (read-only). All 5
            # strategy bodies are SKELETONS in Phase A — returns [] until
            # Phase B fills in the logic. This call is wired now so that
            # Phase B implementations light up automatically.
            try:
                _au_signals = self._au_gold_engine.update(result)
            except Exception as _au_err:
                # NEVER let a gold strategy exception break the forex systems
                logger.warning("[AU GOLD] engine.update() raised: %s", _au_err)
                _au_signals = []

            # Periodic engine-status snapshot (every ~30 minutes).
            # Lets the operator verify what each strategy is "thinking"
            # without having to attach a debugger or scroll through
            # debug-level traffic.
            try:
                _now_t = time.time()
                if _now_t - getattr(self, "_au_last_status_log", 0.0) > 1800:
                    self._au_last_status_log = _now_t
                    _st = self._au_gold_engine.get_status()
                    logger.info(
                        "[AU GOLD STATUS] AU1 range=%s/%s fired=%s | AU2 orb=%s/%s fired=%s | "
                        "AU3 regime=%s pb=%d | AU4 samples=%d/30 | AU5 rsi=%.1f samples=%d/15",
                        _st["au1"]["asian_high"], _st["au1"]["asian_low"], _st["au1"]["fired_today"],
                        _st["au2"]["orb_high"], _st["au2"]["orb_low"], _st["au2"]["fired_today"],
                        _st["au3"]["regime"], _st["au3"]["pullback_bars"],
                        _st["au4"]["samples"],
                        _st["au5"]["last_rsi"], _st["au5"]["samples"],
                    )
            except Exception:
                pass  # Status log is purely advisory — never let it break trading

            # Periodic MT5 broker reconciliation (Fix A for the 10013 bug).
            # The broker can close positions server-side via SL/TP without
            # notifying us. If we never reconcile, _mt5_pos_mgr accumulates
            # stale tickets and the next attempted close on those pairs
            # returns 10013 TRADE_RETCODE_INVALID. Run sync_with_broker
            # every 30 seconds to keep local state aligned with reality.
            try:
                _mt5_now = time.time()
                if (self._mt5_pos_mgr is not None
                        and _mt5_now - getattr(self, "_mt5_last_sync", 0.0) > 30.0):
                    self._mt5_last_sync = _mt5_now
                    self._mt5_pos_mgr.sync_with_broker(magic=self._mt5_trader._magic)
            except Exception as _sync_exc:
                # Non-fatal — sync failure just means we may see a 10013
                # later that Fix B in mt5_trader.close_position will catch.
                logger.debug("[MT5] periodic sync_with_broker failed: %s", _sync_exc)

            if _au_signals:
                # NO_TRADE window: 05:00-07:57 JST (system-wide blackout for
                # forex AND gold). AU1/AU2/AU5 already idle in this window
                # by their session-time gates, but AU3 (H1 pullback) and AU4
                # (H1 divergence) trigger on every H1 close — including
                # 05:00, 06:00, 07:00 — so the guard must be enforced here.
                # Same threshold values used by alt_systems (BRK/SQZ/DIV).
                _jst_au = datetime.now(_jst())
                _au_hm = _jst_au.hour * 60 + _jst_au.minute
                _au_no_trade = (300 <= _au_hm <= 477)  # 5*60=300, 7*60+57=477
                if _au_no_trade:
                    logger.info(
                        "[AU GOLD] NO_TRADE window (%02d:%02d JST) — "
                        "suppressing %d signal(s): %s",
                        _jst_au.hour, _jst_au.minute, len(_au_signals),
                        [s.strategy_id for s in _au_signals],
                    )
                    _au_signals = []  # consumed/discarded for this cycle

            if _au_signals:
                _AU_PT_MAP = {
                    "au1_london_breakout":  self._paper_trader_au1,
                    "au2_ny_orb":           self._paper_trader_au2,
                    "au3_trend_pullback":   self._paper_trader_au3,
                    "au4_usd_divergence":   self._paper_trader_au4,
                    "au5_asian_mean_rev":   self._paper_trader_au5,
                }
                _xau_symbol = result.xau_symbol or "XAUUSD"
                for _sig in _au_signals:
                    _au_pt = _AU_PT_MAP.get(_sig.strategy_id)
                    if _au_pt is None:
                        logger.warning("[AU GOLD] unknown strategy_id %r", _sig.strategy_id)
                        continue
                    # Use a canonical symbol "XAUUSD" for the trade record's
                    # pair field (makes cross-broker analysis consistent even
                    # if the broker's resolved symbol is XAUUSDm/.raw/etc.).
                    _au_pt_trade = _au_pt.open_paper_trade(
                        pair="XAUUSD",
                        direction=_sig.direction,
                        entry_price=_sig.entry_price,
                        composite_scores=result.composite_scores,
                        conviction=0,  # AU systems don't use the FX conviction tier
                        session=get_current_session(),
                        h1_atr=0.0,  # Gold ATR is in xau-space, not result.h1_atr
                        entry_type=_sig.strategy_id,
                        adr_consumed_pct=0.0,  # No ADR concept for gold
                        sl_pips_override=_sig.sl_pips,
                        tp_pips_override=_sig.tp_pips,
                    )
                    if _au_pt_trade is not None:
                        # Stamp AU-specific fields on the TrackedTrade so the
                        # journal record gets them via the generic trade→record
                        # sync at close time.
                        try:
                            _au_pt_trade.au_entry_reason = _sig.entry_reason
                            _au_pt_trade.au_metadata_json = json.dumps(_sig.metadata) if _sig.metadata else ""
                            _au_pt_trade.au_usd_strength_at_entry = result.composite_scores.get("USD", 0.0) or 0.0
                            _au_pt_trade.au_spread_points_at_entry = result.xau_spread_points or 0.0
                            _au_pt_trade.au_asian_range_high = _sig.asian_range_high
                            _au_pt_trade.au_asian_range_low = _sig.asian_range_low
                            _au_pt_trade.au_correlation_xau_usd = _sig.correlation_xau_usd
                            _au_pt_trade.au_rsi_at_entry = _sig.rsi_at_entry
                            # Persist immediately (restart-safety)
                            _au_pt.sync_trade_to_journal(_au_pt_trade, save=True)
                        except Exception as _stamp_err:
                            logger.warning(
                                "[AU GOLD] stamp failed for %s %s: %s",
                                _sig.strategy_id, _sig.direction, _stamp_err,
                            )
                        # Log open to alert panel
                        _now_au = datetime.now(_jst()).strftime("%H:%M:%S")
                        _dir_c = "#1b8a2a" if _sig.direction == "BUY" else "#c62828"
                        self._alert_history.appendleft(
                            f'<span style="font-size:9pt; color:#666;">[{_now_au}]</span> '
                            f'<span style="font-size:10pt; color:#ffb300; font-weight:bold;">'
                            f'\U0001f947 {_sig.strategy_id.upper()} OPEN</span> '
                            f'<span style="font-size:10pt; color:{_dir_c}; font-weight:bold;">'
                            f'{_sig.direction} XAUUSD</span> '
                            f'<span style="font-size:9pt; color:#888;">'
                            f'@ {_sig.entry_price:.2f}  SL:{_sig.sl_pips:.1f}p  '
                            f'TP:{_sig.tp_pips:.1f}p  {_sig.entry_reason[:50]}</span>'
                        )

        # ── cTrader debug: log every time alerts fire ──
        if fired:
            try:
                with open(_DATA_DIR / "ctrader_debug.log", "a") as _f:
                    from datetime import datetime as _dtx
                    _has_bridge = self._ctrader_bridge is not None
                    _has_mgr = self._ctrader_pos_mgr is not None
                    _is_conn = self._ctrader_bridge.is_connected if _has_bridge else False
                    _f.write(f"{_dtx.now().strftime('%H:%M:%S')} "
                             f"bridge={_has_bridge} mgr={_has_mgr} "
                             f"connected={_is_conn} fired={len(fired)} "
                             f"pairs={','.join(fired[:3])}\n")
            except Exception:
                pass

        # ── cTrader auto-open on fired alerts ──
        if self._ctrader_bridge and self._ctrader_pos_mgr:
            # No new trades 5:01–6:59 JST — only tracking/recording
            try:
                _jst_now = datetime.now(_jst())
                _jst_hm = _jst_now.hour * 60 + _jst_now.minute
                _ctrader_trade_allowed = 480 <= _jst_hm < 1320  # 8:00-21:59 JST (3 min warm-up)
            except Exception:
                _ctrader_trade_allowed = True  # fallback: allow trades

            # Update cTrader status label + health check
            if self._ctrader_bridge.is_connected:
                if "Connect" in self._ctrader_status_label.text():
                    self._ctrader_status_label.setText("cT: \u25cf Connected")
                    self._ctrader_status_label.setStyleSheet(
                        "color: #2e7d32; padding: 0 4px; font-weight: bold;"
                    )
                self._ct_last_connected = time.time()
                # Query balance every 5 minutes
                if not hasattr(self, '_ct_last_balance_query'):
                    self._ct_last_balance_query = 0
                if time.time() - self._ct_last_balance_query > 300:
                    self._ct_last_balance_query = time.time()
                    self._ctrader_bridge.query_balance()
            else:
                self._ctrader_status_label.setText("cT: \u25cb Disconnected")
                self._ctrader_status_label.setStyleSheet("color: #c62828; padding: 0 4px;")
                # NOTE: Do NOT force-restart the bridge here. ClientService
                # (Twisted) handles automatic reconnection with exponential
                # backoff. Manual stop()/start() would fight against it and
                # cause the rapid reconnect cycling we had before.

            _ct_enabled = self._ctrader_config.get("ctrader_enabled")
            _ct_auto = self._ctrader_config.get("ctrader_auto_open")
            _ct_connected = self._ctrader_bridge.is_connected

            # Retry any orders queued while cTrader was disconnected
            self._ct_flush_pending_orders()

            # ── OLD Sv2 cTrader auto-open loop DELETED ──
            # Previously this block opened a cTrader order for every fired
            # stoch_v2 alert (System A). That bypassed the DTC-combo filter
            # pipeline, producing unfiltered live trades. Removed. The only
            # remaining cTrader entry point is _maybe_open_dtc_combo(), which
            # routes filtered DTC-combo trades into cTrader. All other paper
            # systems (Sv2, SS, ATR, A-tuned, B-tuned) stay paper-only.
            _ = (_ctrader_trade_allowed, _ct_enabled, _ct_auto, _ct_connected)  # silence unused-var warnings

        # ── OLD MT5 auto-open block DELETED ──
        # The dormant MT5 auto-trading feature (controlled by the "Enable MT5
        # auto-trading" checkbox + mt5_auto_open) bypassed the mirror's
        # pair/time filters. Removed to avoid conflict. MT5 execution is now
        # exclusively handled by _mirror_to_mt5() which respects
        # data/mt5_mirror_config.json.
        # (The "mt5_trading_enabled" checkbox still controls the startup
        # sync_with_broker() call in __init__ — that is harmless.)

        # NOTE: Paper trade SL/TP monitoring is done AFTER _update_tracked_trades
        # so that spread-collapse (signal exit) gets first priority, matching
        # the backtester's Hybrid mode.  See "Paper trade SL/TP" block below.

        # ── Update performance tracker with latest prices + scores ──
        if result.close_prices:
            newly_done = self._alert_perf.update(
                result.close_prices,
                result.composite_scores,
                result.htf_composite_scores,
            )
            if newly_done:
                self._alert_perf.save_completed(_OUTCOMES_FILE)
            # Persist active alerts for crash/sleep recovery
            if self._alert_perf.get_active_count() > 0:
                self._alert_perf.save_active(_ACTIVE_PERF_FILE)

        # ── Range alerts ──
        strength_alert_pairs = set(alert_candidates.keys())
        self._process_range_states(result.range_states, strength_alert_pairs)

        # ── Update tracked trades (includes spread-collapse exit for paper) ──
        alert_count_before = len(self._alert_history)
        self._update_tracked_trades(result)
        # If exit engine added notifications, refresh alert panel
        if len(self._alert_history) > alert_count_before:
            self._refresh_alert_panel()

        # ── Paper trade SL/TP monitoring ──
        # Runs AFTER _update_tracked_trades so that spread-collapse (signal exit)
        # gets first priority, matching the backtester's Hybrid mode.
        # Trades already closed by spread-collapse won't be in active list.
        if result.high_prices and result.low_prices:
            closed_papers = self._paper_trader.update_cycle(
                result.high_prices, result.low_prices, result.close_prices,
                m1_bar_time=result.m1_bar_time,
            )
            if closed_papers:
                self._paper_trader.save_journal()
                self._save_trades()
                self._refresh_perf_paper_tab()
                # Log to alert panel
                for rec in closed_papers:
                    pnl_c = "#1b8a2a" if rec.is_win else "#c62828"
                    reason_label = {
                        "sl_hit": "SL HIT", "tp_hit": "TP HIT",
                        "signal_exit": "SIGNAL EXIT",
                        "spread_collapsed": "SIGNAL EXIT",
                        "direction_flipped": "SIGNAL EXIT",
                    }.get(rec.close_reason, rec.close_reason)
                    now_str2 = datetime.now(_jst()).strftime("%H:%M:%S")
                    rrr = abs(rec.pnl_pips) / rec.sl_pips if rec.sl_pips > 0 else 0
                    rrr_str = f"+{rrr:.1f}R" if rec.is_win else f"-{rrr:.1f}R"
                    paper_html = (
                        f'<span style="font-size:9pt; color:#666;">[{now_str2}]</span> '
                        f'<span style="font-size:10pt; color:#4a6fa5; font-weight:bold;">'
                        f'\U0001f4dd PAPER {reason_label}</span> '
                        f'<span style="font-size:10pt; font-weight:bold;">'
                        f'{rec.direction} {rec.pair}</span> '
                        f'<span style="font-size:10pt; color:{pnl_c}; font-weight:bold;">'
                        f'{rec.pnl_pips:+.1f}p ({rrr_str})</span> '
                        f'<span style="font-size:9pt; color:#888;">'
                        f'{rec.duration_minutes:.0f}min</span>'
                    )
                    self._alert_history.appendleft(paper_html)
                self._refresh_alert_panel()

            # System A-tuned: update cycle
            self._paper_trader_a_tuned.update_cycle(
                result.high_prices, result.low_prices, result.close_prices,
                m1_bar_time=result.m1_bar_time,
            )

            # System B: Sv2+SS update cycle
            closed_ss = self._paper_trader_ss.update_cycle(
                result.high_prices, result.low_prices, result.close_prices,
                m1_bar_time=result.m1_bar_time,
            )
            if closed_ss:
                for rec in closed_ss:
                    pnl_c = "#1b8a2a" if rec.is_win else "#c62828"
                    now_str_ss = datetime.now(_jst()).strftime("%H:%M:%S")
                    self._alert_history.appendleft(
                        f'<span style="font-size:9pt; color:#666;">[{now_str_ss}]</span> '
                        f'<span style="font-size:10pt; color:#9c27b0; font-weight:bold;">'
                        f'\U0001f9ea SS CLOSE</span> '
                        f'<span style="font-size:10pt; font-weight:bold;">'
                        f'{rec.direction} {rec.pair}</span> '
                        f'<span style="font-size:10pt; color:{pnl_c}; font-weight:bold;">'
                        f'{rec.pnl_pips:+.1f}p</span>'
                    )
                self._refresh_alert_panel()

            # System B-tuned: update cycle
            self._paper_trader_b_tuned.update_cycle(
                result.high_prices, result.low_prices, result.close_prices,
                m1_bar_time=result.m1_bar_time,
            )

            # System C: Sv2+ATR update cycle
            closed_atr = self._paper_trader_atr.update_cycle(
                result.high_prices, result.low_prices, result.close_prices,
                m1_bar_time=result.m1_bar_time,
            )
            if closed_atr:
                for rec in closed_atr:
                    pnl_c = "#1b8a2a" if rec.is_win else "#c62828"
                    now_str_atr = datetime.now(_jst()).strftime("%H:%M:%S")
                    self._alert_history.appendleft(
                        f'<span style="font-size:9pt; color:#666;">[{now_str_atr}]</span> '
                        f'<span style="font-size:10pt; color:#e65100; font-weight:bold;">'
                        f'\U0001f4c8 ATR CLOSE</span> '
                        f'<span style="font-size:10pt; font-weight:bold;">'
                        f'{rec.direction} {rec.pair}</span> '
                        f'<span style="font-size:10pt; color:{pnl_c}; font-weight:bold;">'
                        f'{rec.pnl_pips:+.1f}p</span>'
                    )
                self._refresh_alert_panel()

            # Live-candle systems: update cycle (peak/worst/post-close observation)
            for _live_pt in (
                self._paper_trader_sv2_live,
                self._paper_trader_sv2_a_tuned_live,
                self._paper_trader_sv2_ss_live,
                self._paper_trader_sv2_b_tuned_live,
                self._paper_trader_sv2_atr_live,
            ):
                _live_pt.update_cycle(
                    result.high_prices, result.low_prices, result.close_prices,
                    m1_bar_time=result.m1_bar_time,
                )

            # Sv2-upgraded: update cycle (this is where the BE-stop move
            # logic runs inside paper_trader.update_cycle — see the
            # "sv2_upgraded" branch in that method).
            closed_up = self._paper_trader_sv2_upgraded.update_cycle(
                result.high_prices, result.low_prices, result.close_prices,
                m1_bar_time=result.m1_bar_time,
            )
            if closed_up:
                # Record close timestamps for revenge-cooldown enforcement
                _now_up = datetime.now(_jst()).strftime("%H:%M:%S")
                for rec in closed_up:
                    self._sv2_upgraded_last_close[rec.pair] = (time.time(), rec.is_win)
                    pnl_c_up = "#1b8a2a" if rec.is_win else "#c62828"
                    self._alert_history.appendleft(
                        f'<span style="font-size:9pt; color:#666;">[{_now_up}]</span> '
                        f'<span style="font-size:10pt; color:#e91e63; font-weight:bold;">'
                        f'\u26a1 Sv2-up CLOSE</span> '
                        f'<span style="font-size:10pt; font-weight:bold;">'
                        f'{rec.direction} {rec.pair}</span> '
                        f'<span style="font-size:10pt; color:{pnl_c_up}; font-weight:bold;">'
                        f'{rec.pnl_pips:+.1f}p</span>'
                    )
                self._refresh_alert_panel()

            # ── AU Gold suite: update cycle for all 5 strategies ──
            # Gold trades on a SEPARATE price channel (xau_* fields). We pass
            # xau-space highs/lows keyed by "XAUUSD" so paper_trader's SL/TP
            # checks use the correct prices. Forex high/low dict is ignored
            # for gold — gold trades only care about XAUUSD bars.
            if result.xau_high > 0 and result.xau_low > 0 and result.xau_price > 0:
                _xau_h = {"XAUUSD": result.xau_high}
                _xau_l = {"XAUUSD": result.xau_low}
                _xau_c = {"XAUUSD": result.xau_price}
                for _au_pt in (self._paper_trader_au1, self._paper_trader_au2,
                                self._paper_trader_au3, self._paper_trader_au4,
                                self._paper_trader_au5):
                    _closed_au = _au_pt.update_cycle(
                        _xau_h, _xau_l, _xau_c,
                        m1_bar_time=result.m1_bar_time,
                    )
                    if _closed_au:
                        _now_au = datetime.now(_jst()).strftime("%H:%M:%S")
                        for rec in _closed_au:
                            _pnl_c_au = "#1b8a2a" if rec.is_win else "#c62828"
                            self._alert_history.appendleft(
                                f'<span style="font-size:9pt; color:#666;">[{_now_au}]</span> '
                                f'<span style="font-size:10pt; color:#ffb300; font-weight:bold;">'
                                f'\U0001f947 {rec.entry_type.upper()} CLOSE</span> '
                                f'<span style="font-size:10pt; font-weight:bold;">'
                                f'{rec.direction} {rec.pair}</span> '
                                f'<span style="font-size:10pt; color:{_pnl_c_au}; font-weight:bold;">'
                                f'{rec.pnl_pips:+.1f}p</span>'
                            )
                        self._refresh_alert_panel()

            # DTC-combo: update cycle (filtered aggregate of SS/ATR/B-tuned)
            closed_dtc = self._paper_trader_dtc_combo.update_cycle(
                result.high_prices, result.low_prices, result.close_prices,
                m1_bar_time=result.m1_bar_time,
            )
            if closed_dtc:
                for rec in closed_dtc:
                    pnl_c = "#1b8a2a" if rec.is_win else "#c62828"
                    now_str_dtc = datetime.now(_jst()).strftime("%H:%M:%S")
                    self._alert_history.appendleft(
                        f'<span style="font-size:9pt; color:#666;">[{now_str_dtc}]</span> '
                        f'<span style="font-size:10pt; color:#0277bd; font-weight:bold;">'
                        f'\U0001f3af DTC CLOSE</span> '
                        f'<span style="font-size:10pt; font-weight:bold;">'
                        f'{rec.direction} {rec.pair}</span> '
                        f'<span style="font-size:10pt; color:{pnl_c}; font-weight:bold;">'
                        f'{rec.pnl_pips:+.1f}p</span>'
                    )
                self._refresh_alert_panel()

            # System D: QM4 CSI update cycle
            closed_qm4 = self._paper_trader_qm4.update_cycle(
                result.high_prices, result.low_prices, result.close_prices,
                m1_bar_time=result.m1_bar_time,
            )
            if closed_qm4:
                for rec in closed_qm4:
                    pnl_c = "#1b8a2a" if rec.is_win else "#c62828"
                    now_str_qm4 = datetime.now(_jst()).strftime("%H:%M:%S")
                    atype = getattr(rec, 'qm4_alert_type', '')
                    self._alert_history.appendleft(
                        f'<span style="font-size:9pt; color:#666;">[{now_str_qm4}]</span> '
                        f'<span style="font-size:10pt; color:#ff6f00; font-weight:bold;">'
                        f'\U0001f3af QM4 CLOSE</span> '
                        f'<span style="font-size:10pt; font-weight:bold;">'
                        f'{rec.direction} {rec.pair}</span> '
                        f'<span style="font-size:10pt; color:{pnl_c}; font-weight:bold;">'
                        f'{rec.pnl_pips:+.1f}p</span>'
                        f'<span style="font-size:9pt; color:#888;"> [{atype}]</span>'
                    )
                self._refresh_alert_panel()

            # ── Systems E/F/G + Squeeze-REV: Alt systems (BREAKOUT, SQUEEZE, DIVERGENCE, SQUEEZE-REV) ──
            # Update cycle for SL/TP monitoring on existing trades
            for _alt_pt in (self._paper_trader_breakout, self._paper_trader_squeeze,
                            self._paper_trader_divergence, self._paper_trader_squeeze_rev):
                _alt_pt.update_cycle(
                    result.high_prices, result.low_prices, result.close_prices,
                    m1_bar_time=result.m1_bar_time,
                )

            # Check for new alt-system signals
            try:
                _alt_signals = self._alt_engine.update(
                    result.high_prices, result.low_prices, result.close_prices,
                    h1_atr=result.h1_atr,
                    m1_bar_time=result.m1_bar_time,
                )
                if _alt_signals:
                    # NO_TRADE filter: 05:00-07:57 JST
                    _jst_alt = datetime.now(_jst())
                    _alt_hm = _jst_alt.hour * 60 + _jst_alt.minute
                    _alt_allowed = not (300 <= _alt_hm <= 477)  # 5*60=300, 7*60+57=477

                    if _alt_allowed:
                        _ALT_PT_MAP = {
                            "breakout": self._paper_trader_breakout,
                            "squeeze": self._paper_trader_squeeze,
                            "divergence": self._paper_trader_divergence,
                        }
                        _ALT_LABEL = {
                            "breakout": ("\U0001f4ca BRK", "#0288d1"),
                            "squeeze": ("\U0001f504 SQZ", "#7b1fa2"),
                            "divergence": ("\U0001f4c8 DIV", "#00796b"),
                        }
                        for sig in _alt_signals:
                            _apt = _ALT_PT_MAP.get(sig.system_type)
                            if _apt is None:
                                continue
                            # News filter: block if RED news blackout for this pair
                            if self._news_filter.loaded and self._news_filter.is_blackout(
                                sig.pair, time.time()
                            ):
                                logger.info(
                                    "[ALT] %s %s %s blocked by news filter",
                                    sig.system_type.upper(), sig.direction, sig.pair,
                                )
                                continue
                            # Fix BUG #4: pass the alt-engine's own computed SL/TP
                            # (squeeze/breakout/divergence each compute strategy-specific
                            # values; without these overrides, stoch_v2's per-pair
                            # sl_atr/tp_atr settings were silently taking over and the
                            # engine's SL/TP computation was dead code).
                            pt = _apt.open_paper_trade(
                                pair=sig.pair,
                                direction=sig.direction,
                                entry_price=sig.entry_price,
                                composite_scores=self._last_composite_scores,
                                conviction=0,
                                session=get_current_session(),
                                h1_atr=result.h1_atr.get(sig.pair, 0.0),
                                entry_type=sig.system_type,
                                adr_consumed_pct=result.session_range_pct.get(sig.pair, 0.0),
                                sl_pips_override=sig.sl_pips,
                                tp_pips_override=sig.tp_pips,
                            )
                            if pt:
                                # Store system-specific signal data
                                pt.entry_alt_signal_1 = sig.alt_signal_1
                                pt.entry_alt_signal_2 = sig.alt_signal_2
                                pt.entry_alt_signal_3 = sig.alt_signal_3
                                pt.entry_alt_signal_4 = sig.alt_signal_4
                                # Extended Squeeze-specific context (populated only for squeeze signals)
                                if sig.system_type == "squeeze":
                                    pt.sqz_bb_kc_ratio_min = sig.sqz_bb_kc_ratio_min
                                    pt.sqz_bb_width_pips_release = sig.sqz_bb_width_pips_release
                                    pt.sqz_bb_width_min_pips = sig.sqz_bb_width_min_pips
                                    pt.sqz_real_age_bars = sig.sqz_real_age_bars
                                    pt.sqz_dist_to_upper_bb_pips = sig.sqz_dist_to_upper_bb_pips
                                    pt.sqz_dist_to_lower_bb_pips = sig.sqz_dist_to_lower_bb_pips
                                    pt.sqz_close_pos_in_kc = sig.sqz_close_pos_in_kc
                                    pt.sqz_atr_ratio_during = sig.sqz_atr_ratio_during
                                    pt.sqz_touches_count = sig.sqz_touches_count
                                    pt.sqz_concurrent_count = sig.sqz_concurrent_count

                                    # ── Squeeze-REV mirror (2026-04-29) ──
                                    # Open a paper trade in the OPPOSITE direction
                                    # AND with SWAPPED SL/TP distances so each
                                    # outcome perfectly mirrors the original:
                                    #   original BUY hits SL@E-10  → REV SELL hits TP@E-10  ✓
                                    #   original BUY hits TP@E+20  → REV SELL hits SL@E+20  ✓
                                    # If we kept the same sl/tp distances, both
                                    # sides would lose on most trades — that's
                                    # NOT a true mirror.
                                    _rev_dir = "SELL" if sig.direction == "BUY" else "BUY"
                                    _rev_pt = self._paper_trader_squeeze_rev.open_paper_trade(
                                        pair=sig.pair,
                                        direction=_rev_dir,
                                        entry_price=sig.entry_price,
                                        composite_scores=self._last_composite_scores,
                                        conviction=0,
                                        session=get_current_session(),
                                        h1_atr=result.h1_atr.get(sig.pair, 0.0),
                                        entry_type="squeeze_rev",
                                        adr_consumed_pct=result.session_range_pct.get(sig.pair, 0.0),
                                        # SWAPPED — original SL distance becomes REV TP distance,
                                        # original TP distance becomes REV SL distance
                                        sl_pips_override=sig.tp_pips,
                                        tp_pips_override=sig.sl_pips,
                                    )
                                    if _rev_pt:
                                        _rev_pt.entry_alt_signal_1 = sig.alt_signal_1
                                        _rev_pt.entry_alt_signal_2 = sig.alt_signal_2
                                        _rev_pt.entry_alt_signal_3 = sig.alt_signal_3
                                        _rev_pt.entry_alt_signal_4 = sig.alt_signal_4
                                        _rev_pt.sqz_bb_kc_ratio_min = sig.sqz_bb_kc_ratio_min
                                        _rev_pt.sqz_bb_width_pips_release = sig.sqz_bb_width_pips_release
                                        _rev_pt.sqz_bb_width_min_pips = sig.sqz_bb_width_min_pips
                                        _rev_pt.sqz_real_age_bars = sig.sqz_real_age_bars
                                        _rev_pt.sqz_dist_to_upper_bb_pips = sig.sqz_dist_to_upper_bb_pips
                                        _rev_pt.sqz_dist_to_lower_bb_pips = sig.sqz_dist_to_lower_bb_pips
                                        _rev_pt.sqz_close_pos_in_kc = sig.sqz_close_pos_in_kc
                                        _rev_pt.sqz_atr_ratio_during = sig.sqz_atr_ratio_during
                                        _rev_pt.sqz_touches_count = sig.sqz_touches_count
                                        _rev_pt.sqz_concurrent_count = sig.sqz_concurrent_count
                                        _conv_rev = conviction_results.get(sig.pair) if conviction_results else None
                                        _stamp_entry_signals(_rev_pt, sig.pair, _conv_rev, direction=_rev_dir)
                                        _rev_dir_c = "#1b8a2a" if _rev_dir == "BUY" else "#c62828"
                                        _now_rev = datetime.now(_jst()).strftime("%H:%M:%S")
                                        self._alert_history.appendleft(
                                            f'<span style="font-size:9pt; color:#666;">[{_now_rev}]</span> '
                                            f'<span style="font-size:10pt; color:#ad1457; font-weight:bold;">'
                                            f'\U0001f504 SQZ-REV OPEN</span> '
                                            f'<span style="font-size:10pt; color:{_rev_dir_c}; font-weight:bold;">'
                                            f'{_rev_dir} {sig.pair}</span> '
                                            f'<span style="font-size:9pt; color:#888;">'
                                            f'SL={sig.tp_pips:.1f} TP={sig.sl_pips:.1f}</span>'
                                        )
                                # Stamp generic market context (stoch scores, ATR, news, etc.)
                                conv = conviction_results.get(sig.pair) if conviction_results else None
                                _stamp_entry_signals(pt, sig.pair, conv, direction=sig.direction)
                                _lbl, _clr = _ALT_LABEL.get(sig.system_type, ("ALT", "#888"))
                                _dir_c = "#1b8a2a" if sig.direction == "BUY" else "#c62828"
                                _now_alt = datetime.now(_jst()).strftime("%H:%M:%S")
                                self._alert_history.appendleft(
                                    f'<span style="font-size:9pt; color:#666;">[{_now_alt}]</span> '
                                    f'<span style="font-size:10pt; color:{_clr}; font-weight:bold;">'
                                    f'{_lbl} OPEN</span> '
                                    f'<span style="font-size:10pt; color:{_dir_c}; font-weight:bold;">'
                                    f'{sig.direction} {sig.pair}</span> '
                                    f'<span style="font-size:9pt; color:#888;">'
                                    f'SL={sig.sl_pips:.1f} TP={sig.tp_pips:.1f}</span>'
                                )
                                self._refresh_alert_panel()
            except Exception as _alt_ex:
                logger.warning("[ALT] Signal check error: %s", _alt_ex)

        # Update status timestamp + session
        now = datetime.now(_jst()).strftime("%H:%M:%S")
        self._status_label.setText(f"Last update: {now}")
        if result.session_label:
            if result.session_label == "WEEKEND":
                self._session_label.setText("\U0001f534 TRADING OFF")
                self._session_label.setStyleSheet("color: #cc3333; padding: 0 8px; font-weight: bold;")
            elif result.session_label == "NO_TRADE":
                self._session_label.setText("\U0001f534 NO_TRADE")
                self._session_label.setStyleSheet("color: #cc3333; padding: 0 8px; font-weight: bold;")
            else:
                self._session_label.setText(f"\U0001f7e2 {result.session_label}")
                self._session_label.setStyleSheet("color: #4a6fa5; padding: 0 8px; font-weight: bold;")

    def _check_strength_engine_health(self) -> None:
        """Verify the currency-strength engine initialized properly.

        Called exactly ONCE per TAKUMI session, 10 seconds after the first
        data cycle arrives. If stoch_scores are missing/partial for any of
        the 4 primary timeframes (M5/M15/H1/H4) across all 8 currencies →
        fire a CRITICAL popup telling the operator to restart MT5 + TAKUMI
        in the correct order.

        Motivated by the "grid frozen but P/L still moving" symptom observed
        after an MT5 restart — close_prices were flowing but stoch_scores
        had gone empty, silently causing every grid-cell update to `continue`.
        """
        if self._strength_health_check_done:
            return
        self._strength_health_check_done = True

        if self._last_result is None:
            # No data at all within 10s — separate issue (MT5 not connected).
            # Staleness watchdog will fire its own alert; don't duplicate.
            logger.warning("[HEALTH] No MT5 data arrived in first 10s — skipping strength check")
            return

        result = self._last_result
        required_tfs = ["M5", "M15", "H1", "H4"]
        expected = len(CURRENCIES)  # 8

        partial = []
        for tf in required_tfs:
            tf_scores = result.stoch_scores.get(tf, {})
            if len(tf_scores) < expected:
                partial.append(f"{tf}={len(tf_scores)}/{expected}")

        close_px_count = len(result.close_prices)
        if partial:
            summary = " ".join(partial)
            msg = (
                f"Currency-strength engine incomplete 10s after startup.\n\n"
                f"Missing scores: {summary}\n"
                f"Close prices available: {close_px_count}\n\n"
                f"This usually means MT5's internal bar cache is out of sync with the\n"
                f"Python bridge after a reconnect. The grid won't update even though\n"
                f"open-trade P/L keeps moving.\n\n"
                f"FIX (in this order):\n"
                f"  1. Close TAKUMI\n"
                f"  2. Close MT5 completely\n"
                f"  3. Open MT5 — wait for green 'connected' light and chart showing live prices\n"
                f"  4. Right-click any chart → Refresh (forces bar-history reload)\n"
                f"  5. Open TAKUMI"
            )
            logger.error("[HEALTH] Strength engine failed init: %s  close_prices=%d",
                         summary, close_px_count)
            if hasattr(self, "_health_alerts"):
                self._health_alerts.notify(
                    "critical", "Strength Engine", msg,
                    dedup_key="strength_engine_incomplete_startup",
                )
        else:
            logger.info(
                "[HEALTH] Strength engine OK — %d currencies × %d TFs populated "
                "(close_prices=%d)",
                expected, len(required_tfs), close_px_count,
            )

    def _update_alert_display(
        self,
        candidates: dict[str, tuple[str, dict[str, float]]],
        fired: list[str],
        conviction_results: dict[str, ConvictionResult] | None = None,
    ) -> None:
        """Update the trend alert panel — only add newly fired alerts to history.

        Args:
            candidates: All current alert candidates.
            fired: Pairs that actually passed the cooldown and fired this cycle.
            conviction_results: Optional conviction data per pair.
        """
        now_str = datetime.now(_jst()).strftime("%H:%M:%S")
        conv_map = conviction_results or {}

        # Only add pairs that the AlertManager actually fired (passed cooldown)
        for pair in fired:
            if pair not in candidates:
                continue
            direction, _scores = candidates[pair]
            color = "#1b8a2a" if direction == "BUY" else "#c62828"

            # Conviction info
            conv = conv_map.get(pair)
            conv_score = conv.conviction if conv else 100
            conv_tier = conv.tier if conv else "FULL"

            # Tier label and color
            if conv_tier == "FULL":
                tier_label = "FULL ALERT"
                tier_color = "#1b8a2a" if direction == "BUY" else "#c62828"
            else:
                tier_label = "DIMMED"
                tier_color = "#b08000"

            # Filter shorthand tags
            tags = ""
            if conv and conv.components:
                tr = conv.components.get("trend_regime")
                if tr:
                    tags += "\u2705" if tr.passed else "\u274c"
                vel = conv.components.get("strength_velocity")
                if vel:
                    tags += " \u26a1" if vel.passed else ""
                iso = conv.components.get("isolation")
                if iso:
                    tags += " \u2b50" if iso.score >= 16 else ""
                adr = conv.components.get("adr_position")
                if adr:
                    adr_pct = adr.details.get("adr_consumed_pct", 0)
                    tags += f" ADR:{adr_pct:.0f}%" if adr_pct > 0 else ""

            # Check if already tracking this pair
            tracking = self._trade_tracker.has_trade(pair)
            if tracking:
                track_text = (
                    '<span style="font-size:10pt; color:#999999;"> [TRACKING]</span>'
                )
            else:
                track_text = (
                    f' <a href="track:{pair}:{direction}" '
                    f'style="font-size:10pt; color:#4a6fa5; font-weight:bold;'
                    f' text-decoration:none;">[TRACK]</a>'
                )

            # Use diamond for PRIME-level conviction
            badge = "\u25c6" if conv_score >= 80 else ""

            entry = (
                f'<span style="font-size:18pt; color:#666666;">[{now_str}]</span> '
                f'<span style="font-size:14pt; color:{tier_color}; font-weight:bold;">'
                f" {tier_label} {conv_score}</span> "
                f'<span style="font-size:18pt; color:{color}; font-weight:bold;">'
                f" {badge} {pair} \u2014 {direction}</span>"
                f'<span style="font-size:10pt; color:#888;"> {tags}</span>'
                f"{track_text}"
            )
            self._alert_history.appendleft(entry)

        # Save alert history if new entries were added
        if fired:
            self._save_alert_history()

        # Render
        if not self._alert_history:
            self._alert_label.setText("No 4-timeframe alignments detected.")
            self._alert_label.setStyleSheet(
                "color: #888888; padding: 4px 6px; background: #ffffff;"
            )
        else:
            html = "<br>".join(self._alert_history)
            self._alert_label.setText(html)
            self._alert_label.setStyleSheet(
                "padding: 4px 6px; background: #ffffff;"
            )

    def _refresh_alert_panel(self) -> None:
        """Re-render the alert panel from history (e.g. after exit notifications)."""
        self._render_alert_panel_with_trades()

    def _render_alert_panel_with_trades(self) -> None:
        """Render active trades in OPEN TRADES panel, alerts in TREND ALERTS panel."""
        # ── OPEN TRADES panel ──
        if self._active_trade_html_parts:
            count = len(self._active_trade_html_parts)
            self._open_stats_label.setText(f"{count} active")
            wrapped_parts = []
            for i, part in enumerate(self._active_trade_html_parts):
                bg = "#e2e2e2" if i % 2 == 0 else "#ffffff"
                wrapped_parts.append(f'<div style="background:{bg}; padding:2px 4px; margin:1px 0; border-bottom:1px solid #e0e0e0;">{part}</div>')
            html = "".join(wrapped_parts)
            self._open_panel.setText(html)
            self._open_panel.setStyleSheet(
                "padding: 4px 6px; background: #ffffff;"
            )
        else:
            self._open_stats_label.setText("")
            self._open_panel.setText("No open trades.")
            self._open_panel.setStyleSheet(
                "color: #888888; padding: 4px 6px; background: #ffffff;"
            )

        # ── TREND ALERTS panel (notifications only) ──
        if self._alert_history:
            wrapped = []
            for i, entry in enumerate(self._alert_history):
                bg = "#e2e2e2" if i % 2 == 0 else "#ffffff"
                wrapped.append(f'<div style="background:{bg}; padding:2px 4px; margin:1px 0; border-bottom:1px solid #e0e0e0;">{entry}</div>')
            html = "".join(wrapped)
            self._alert_label.setText(html)
            self._alert_label.setStyleSheet(
                "padding: 4px 6px; background: #ffffff;"
            )
        else:
            self._alert_label.setText("No 4-timeframe alignments detected.")
            self._alert_label.setStyleSheet(
                "color: #888888; padding: 4px 6px; background: #ffffff;"
                " a { color: #4a6fa5; text-decoration: none; font-weight: bold; }"
            )

    def _process_range_states(
        self, range_states: list, strength_alert_pairs: set[str]
    ) -> None:
        """Process range detection results: highlight pairs + log alerts."""
        now_str = datetime.now(_jst()).strftime("%H:%M:%S")

        # Build lookup of range states by pair
        range_by_pair: dict[str, RangeState] = {}
        for rs in range_states:
            # Upgrade to PRIME if both strength alert + range LOADED/BREAKOUT
            if rs.pair in strength_alert_pairs and rs.tier in ("LOADED", "BREAKOUT"):
                rs.tier = "PRIME"
            range_by_pair[rs.pair] = rs

        # Tier colors for pair cell highlighting
        _TIER_BG = {
            "RANGE": "#e3f2fd",       # light blue
            "LOADED": "#f3e5f5",      # light purple
            "BREAKOUT": "#e0f7fa",    # light cyan
            "PRIME": "#fff8e1",       # light gold
            "STALE": "#f5f5f5",       # light grey
        }

        # Update pair name cells with range highlighting
        for row, pair in enumerate(DISPLAY_PAIRS):
            item = self._table.item(row, 0)
            if item is None:
                continue
            rs = range_by_pair.get(pair)
            if rs and rs.tier in _TIER_BG:
                item.setBackground(QColor(_TIER_BG[rs.tier]))
                # Show tier badge in pair name
                label = f"{pair} [{rs.tier}]" if rs.tier != "STALE" else pair
                item.setText(label)
            else:
                item.setBackground(QColor("#ffffff"))
                item.setText(pair)

        # Log significant range alerts
        _TIER_LOG_COLORS = {
            "RANGE": "#1565c0",       # blue
            "LOADED": "#7b1fa2",      # purple
            "BREAKOUT": "#00838f",    # cyan
            "PRIME": "#f57f17",       # gold
        }

        for rs in range_states:
            if rs.tier not in _TIER_LOG_COLORS:
                continue
            # Only log LOADED, BREAKOUT, PRIME (RANGE is visual-only)
            if rs.tier == "RANGE":
                continue

            color = _TIER_LOG_COLORS[rs.tier]
            arrow = "\u2191" if rs.predicted_direction == "BUY" else "\u2193"
            dir_text = rs.predicted_direction or "?"

            if rs.tier == "PRIME":
                entry = (
                    f'<span style="color:#999999;">[{now_str}]</span> '
                    f'<span style="color:{color}; font-weight:bold;">'
                    f"\u26a1 PRIME \u2014 {rs.pair} {arrow} {dir_text}</span>"
                    f'<span style="color:#555555;"> '
                    f"Strength + Range aligned | Q:{rs.quality_score:.0f} | "
                    f"\u0394:{rs.strength_delta:+.1f}</span>"
                )
            elif rs.tier == "BREAKOUT":
                entry = (
                    f'<span style="color:#999999;">[{now_str}]</span> '
                    f'<span style="color:{color}; font-weight:bold;">'
                    f"BREAKOUT \u2014 {rs.pair} {arrow}</span>"
                    f'<span style="color:#555555;"> '
                    f"Broke range {dir_text} | ADR: {rs.adr_consumed_pct:.0f}%</span>"
                )
            else:  # LOADED
                entry = (
                    f'<span style="color:#999999;">[{now_str}]</span> '
                    f'<span style="color:{color}; font-weight:bold;">'
                    f"LOADED \u2014 {rs.pair} {arrow} {dir_text}</span>"
                    f'<span style="color:#555555;"> '
                    f"Range {rs.range_pct:.1f}% | Q:{rs.quality_score:.0f} | "
                    f"\u0394:{rs.strength_delta:+.1f}"
                    f'{" \u2b06\ufe0f building" if rs.strength_building else ""}</span>'
                )

            self._alert_history.appendleft(entry)

        # Re-render alert display if we added range entries
        if any(rs.tier in ("LOADED", "BREAKOUT", "PRIME") for rs in range_states):
            html = "<br>".join(self._alert_history)
            self._alert_label.setText(html)
            self._alert_label.setStyleSheet(
                "padding: 4px 6px; background: #ffffff;"
            )

    def _update_tracked_trades(self, result: CalculationResult) -> None:
        """Update tracked trades with current prices, run exit evaluation.

        Active trade status is rendered as a pinned section at the top of
        the trend alerts panel (no separate panel needed).
        """
        # Always update closed trades panel
        self._update_closed_trades_panel()

        # Post-close observation (4h MAX-MFE/MAX-MAE tracking) — 10 standard
        # systems + 5 live-candle mirrors = 15 total.
        for _pt in (self._paper_trader, self._paper_trader_ss, self._paper_trader_atr, self._paper_trader_qm4,
                    self._paper_trader_a_tuned, self._paper_trader_b_tuned,
                    self._paper_trader_breakout, self._paper_trader_squeeze, self._paper_trader_divergence,
                    self._paper_trader_dtc_combo,
                    self._paper_trader_sv2_live, self._paper_trader_sv2_a_tuned_live,
                    self._paper_trader_sv2_ss_live, self._paper_trader_sv2_b_tuned_live,
                    self._paper_trader_sv2_atr_live,
                    self._paper_trader_sv2_upgraded,
                    # AU Gold suite (2026-04-24)
                    self._paper_trader_au1, self._paper_trader_au2,
                    self._paper_trader_au3, self._paper_trader_au4,
                    self._paper_trader_au5):
            if _pt.post_close_count > 0:
                _pt.post_close_cycle(
                    result.high_prices, result.low_prices, result.close_prices,
                )

        # Merge active trades from ALL systems for display
        active: dict[str, any] = {}
        for _pair, _trade in self._trade_tracker.active_trades.items():
            active[_pair] = _trade
        for _pair, _trade in self._trade_tracker_ss.active_trades.items():
            key = f"{_pair}_ss" if _pair in active else _pair
            active[key] = _trade
        for _pair, _trade in self._trade_tracker_atr.active_trades.items():
            key = f"{_pair}_atr" if _pair in active else _pair
            active[key] = _trade
        for _pair, _trade in self._trade_tracker_a_tuned.active_trades.items():
            key = f"{_pair}_at" if _pair in active else _pair
            active[key] = _trade
        for _pair, _trade in self._trade_tracker_b_tuned.active_trades.items():
            key = f"{_pair}_bt" if _pair in active else _pair
            active[key] = _trade
        for _pair, _trade in self._trade_tracker_qm4.active_trades.items():
            key = f"{_pair}_qm4" if _pair in active else _pair
            active[key] = _trade
        for _pair, _trade in self._trade_tracker_breakout.active_trades.items():
            key = f"{_pair}_brk" if _pair in active else _pair
            active[key] = _trade
        for _pair, _trade in self._trade_tracker_squeeze.active_trades.items():
            key = f"{_pair}_sqz" if _pair in active else _pair
            active[key] = _trade
        # Squeeze-REV (2026-04-29) — always uses unique suffix because it
        # mirrors every Squeeze entry on the same pair (collision guaranteed)
        for _pair, _trade in self._trade_tracker_squeeze_rev.active_trades.items():
            active[f"{_pair}_sqzr"] = _trade
        for _pair, _trade in self._trade_tracker_divergence.active_trades.items():
            key = f"{_pair}_div" if _pair in active else _pair
            active[key] = _trade
        # DTC-combo (the live system) — must appear in OPEN TRADES panel
        for _pair, _trade in self._trade_tracker_dtc_combo.active_trades.items():
            key = f"{_pair}_dtc" if _pair in active else _pair
            active[key] = _trade
        # ── 5 Live-candle mirror systems (2026-04-21) ──
        # Always use unique suffix keys (live mirrors will almost always
        # collide with their non-live twin on the same pair). Label/color
        # comes from entry_type via _sys_labels below.
        for _pair, _trade in self._trade_tracker_sv2_live.active_trades.items():
            active[f"{_pair}_sv2l"] = _trade
        for _pair, _trade in self._trade_tracker_sv2_a_tuned_live.active_trades.items():
            active[f"{_pair}_atl"] = _trade
        for _pair, _trade in self._trade_tracker_sv2_ss_live.active_trades.items():
            active[f"{_pair}_ssl"] = _trade
        for _pair, _trade in self._trade_tracker_sv2_b_tuned_live.active_trades.items():
            active[f"{_pair}_btl"] = _trade
        for _pair, _trade in self._trade_tracker_sv2_atr_live.active_trades.items():
            active[f"{_pair}_atrl"] = _trade
        # Sv2-upgraded (2026-04-23) — also unique suffix key since it shares
        # signal source with Sv2-live and they can collide on the same pair.
        for _pair, _trade in self._trade_tracker_sv2_upgraded.active_trades.items():
            active[f"{_pair}_svup"] = _trade
        # AU Gold suite (2026-04-24) — XAUUSD trades from 5 AU strategies.
        # Each strategy has its own unique suffix; they all trade "XAUUSD"
        # so suffix is the strategy tag. Gold trades appear alongside forex
        # in the OPEN TRADES panel.
        for _pair, _trade in self._trade_tracker_au1.active_trades.items():
            active[f"{_pair}_au1"] = _trade
        for _pair, _trade in self._trade_tracker_au2.active_trades.items():
            active[f"{_pair}_au2"] = _trade
        for _pair, _trade in self._trade_tracker_au3.active_trades.items():
            active[f"{_pair}_au3"] = _trade
        for _pair, _trade in self._trade_tracker_au4.active_trades.items():
            active[f"{_pair}_au4"] = _trade
        for _pair, _trade in self._trade_tracker_au5.active_trades.items():
            active[f"{_pair}_au5"] = _trade

        # Build composite currency scores (needed even if no active trades)
        composite: dict[str, float] = {}
        for ccy in CURRENCIES:
            total = 0.0
            count = 0
            for tf in TIMEFRAME_LABELS:
                tr = result.timeframes.get(tf)
                if tr and ccy in tr.currency_scores:
                    total += tr.currency_scores[ccy]
                    count += 1
            if count > 0:
                composite[ccy] = total / count
        self._last_composite_scores = composite

        # Build HTF-only composite (M5+M15+H1, excluding M1) — matches
        # backtester._compute_htf_composite_scores() for spread-collapse exit.
        _HTF_TFS = ("M5", "M15", "H1")
        htf_composite: dict[str, float] = {}
        for ccy in CURRENCIES:
            total = 0.0
            count = 0
            for tf in _HTF_TFS:
                tr = result.timeframes.get(tf)
                if tr and ccy in tr.currency_scores:
                    total += tr.currency_scores[ccy]
                    count += 1
            if count > 0:
                htf_composite[ccy] = total / count

        if not active:
            self._active_trade_html_parts = []
            self._render_alert_panel_with_trades()
            return

        # Get M1 pair scores
        m1_result = result.timeframes.get("M1")
        current_m1_scores = m1_result.pair_scores if m1_result else {}

        # ADR consumed info from range states
        adr_consumed_map: dict[str, float] = {}
        for rs in result.range_states:
            adr_consumed_map[rs.pair] = rs.adr_consumed_pct

        trade_html_parts: list[str] = []

        for _active_key, trade in active.items():
            pair = trade.pair  # use the real pair name, not the suffixed key
            # Update price from close_prices
            if pair in result.close_prices:
                self._trade_tracker.update_price(pair, result.close_prices[pair])
                # Also update System B and C trackers
                if pair in self._trade_tracker_ss.active_trades:
                    self._trade_tracker_ss.update_price(pair, result.close_prices[pair])
                if pair in self._trade_tracker_atr.active_trades:
                    self._trade_tracker_atr.update_price(pair, result.close_prices[pair])
                if pair in self._trade_tracker_a_tuned.active_trades:
                    self._trade_tracker_a_tuned.update_price(pair, result.close_prices[pair])
                if pair in self._trade_tracker_b_tuned.active_trades:
                    self._trade_tracker_b_tuned.update_price(pair, result.close_prices[pair])
                if pair in self._trade_tracker_qm4.active_trades:
                    self._trade_tracker_qm4.update_price(pair, result.close_prices[pair])
                if pair in self._trade_tracker_breakout.active_trades:
                    self._trade_tracker_breakout.update_price(pair, result.close_prices[pair])
                if pair in self._trade_tracker_squeeze.active_trades:
                    self._trade_tracker_squeeze.update_price(pair, result.close_prices[pair])
                if pair in self._trade_tracker_squeeze_rev.active_trades:
                    self._trade_tracker_squeeze_rev.update_price(pair, result.close_prices[pair])
                if pair in self._trade_tracker_divergence.active_trades:
                    self._trade_tracker_divergence.update_price(pair, result.close_prices[pair])
                if pair in self._trade_tracker_dtc_combo.active_trades:
                    self._trade_tracker_dtc_combo.update_price(pair, result.close_prices[pair])
                # Live-candle mirror trackers
                if pair in self._trade_tracker_sv2_live.active_trades:
                    self._trade_tracker_sv2_live.update_price(pair, result.close_prices[pair])
                if pair in self._trade_tracker_sv2_a_tuned_live.active_trades:
                    self._trade_tracker_sv2_a_tuned_live.update_price(pair, result.close_prices[pair])
                if pair in self._trade_tracker_sv2_ss_live.active_trades:
                    self._trade_tracker_sv2_ss_live.update_price(pair, result.close_prices[pair])
                if pair in self._trade_tracker_sv2_b_tuned_live.active_trades:
                    self._trade_tracker_sv2_b_tuned_live.update_price(pair, result.close_prices[pair])
                if pair in self._trade_tracker_sv2_atr_live.active_trades:
                    self._trade_tracker_sv2_atr_live.update_price(pair, result.close_prices[pair])
                if pair in self._trade_tracker_sv2_upgraded.active_trades:
                    self._trade_tracker_sv2_upgraded.update_price(pair, result.close_prices[pair])
            # AU Gold: XAUUSD is NOT in result.close_prices (it's on the
            # separate xau_* data channel), so this is an ELIF sibling of
            # the forex branch above — NOT nested inside it.
            elif pair == "XAUUSD" and result.xau_price > 0:
                for _au_tr in (self._trade_tracker_au1, self._trade_tracker_au2,
                                self._trade_tracker_au3, self._trade_tracker_au4,
                                self._trade_tracker_au5):
                    if "XAUUSD" in _au_tr.active_trades:
                        _au_tr.update_price("XAUUSD", result.xau_price)

            # ── Update Peak/Worst using M1 High/Low (matches backtester) ──
            h_price = result.high_prices.get(pair)
            l_price = result.low_prices.get(pair)
            if h_price is not None and l_price is not None:
                from takumi_trader.core.trade_tracker import pip_value as _pv
                _pip = _pv(pair)

                def _update_peak_worst(t):
                    if t.direction == "BUY":
                        _best = (h_price - t.entry_price) / _pip
                        _worst = (l_price - t.entry_price) / _pip
                    else:
                        _best = (t.entry_price - l_price) / _pip
                        _worst = (t.entry_price - h_price) / _pip
                    if _best > t.peak_pnl_pips:
                        t.peak_pnl_pips = _best
                    if _worst < t.worst_pnl_pips:
                        t.worst_pnl_pips = _worst

                _update_peak_worst(trade)
                # Also update System B and C trades
                ss_trade = self._trade_tracker_ss.active_trades.get(pair)
                if ss_trade:
                    _update_peak_worst(ss_trade)
                atr_trade = self._trade_tracker_atr.active_trades.get(pair)
                if atr_trade:
                    _update_peak_worst(atr_trade)
                qm4_trade = self._trade_tracker_qm4.active_trades.get(pair)
                if qm4_trade:
                    _update_peak_worst(qm4_trade)
                # Live-candle mirror trades on same pair
                for _live_tracker in (
                    self._trade_tracker_sv2_live,
                    self._trade_tracker_sv2_a_tuned_live,
                    self._trade_tracker_sv2_ss_live,
                    self._trade_tracker_sv2_b_tuned_live,
                    self._trade_tracker_sv2_atr_live,
                    self._trade_tracker_sv2_upgraded,  # 2026-04-23
                    self._trade_tracker_squeeze_rev,    # 2026-04-29
                ):
                    _lt = _live_tracker.active_trades.get(pair)
                    if _lt:
                        _update_peak_worst(_lt)
            # AU Gold peak/worst: XAUUSD is NOT in result.high_prices, so
            # this is an ELIF sibling — not nested inside the forex branch.
            # Uses result.xau_high/low from the separate gold data channel.
            elif pair == "XAUUSD" and result.xau_high > 0 and result.xau_low > 0:
                _xh = result.xau_high
                _xl = result.xau_low
                for _au_tr in (self._trade_tracker_au1, self._trade_tracker_au2,
                                self._trade_tracker_au3, self._trade_tracker_au4,
                                self._trade_tracker_au5):
                    _au_t = _au_tr.active_trades.get("XAUUSD")
                    if _au_t is not None:
                        _au_pip = 0.01  # gold pip
                        if _au_t.direction == "BUY":
                            _best = (_xh - _au_t.entry_price) / _au_pip
                            _worst = (_xl - _au_t.entry_price) / _au_pip
                        else:
                            _best = (_au_t.entry_price - _xl) / _au_pip
                            _worst = (_au_t.entry_price - _xh) / _au_pip
                        if _best > _au_t.peak_pnl_pips:
                            _au_t.peak_pnl_pips = _best
                        if _worst < _au_t.worst_pnl_pips:
                            _au_t.worst_pnl_pips = _worst

            # ── Forex-only exit logic (skipped for AU Gold XAUUSD trades) ──
            # AU trades close ONLY via paper_trader.update_cycle()'s SL/TP check
            # (already wired with xau_high/low/price). Running the forex
            # exit engine on XAUUSD would:
            #   - Pollute exit_engine._peak_delta with XAU pseudo-deltas
            #     (current_ccy_scores.get("XAU", 0.0) returns 0)
            #   - Trigger counter-momentum exit on USD-velocity alone, then
            #     try to close via self._paper_trader (the FOREX Sv2 trader,
            #     wrong tracker for XAUUSD) — silent no-op but ugly
            #   - Run MT5 spread-collapse on a pair we never mirror to MT5
            forex_only = (pair != "XAUUSD")

            # Get flow bias
            flow_state = result.flow_states.get(pair)
            flow_bias = flow_state.flow_bias if flow_state else None

            # Run exit evaluation
            m1_score = current_m1_scores.get(pair)
            prev_m1_score = self._prev_m1_pair_scores.get(pair)

            if forex_only:
                exit_result = self._exit_engine.evaluate(
                    trade=trade,
                    current_ccy_scores=composite,
                    m1_pair_score=m1_score,
                    m1_pair_score_prev=prev_m1_score,
                    adr_consumed_pct=adr_consumed_map.get(pair, 0.0),
                    flow_bias=flow_bias,
                )
            else:
                exit_result = None  # AU trades — forex exit engine N/A

            # Exit notifications disabled (both CLOSE and URGENT)
            if False:
                now_ts = time.time()
                last_exit_ts = self._last_exit_alert_time.get(pair, 0.0)
                if now_ts - last_exit_ts >= self._exit_alert_cooldown:
                    self._last_exit_alert_time[pair] = now_ts
                    now_str = datetime.now(_jst()).strftime("%H:%M:%S")
                    urg = trade.exit_urgency
                    urg_c = "#d50000" if urg == "URGENT" else "#e65100"
                    reasons_txt = ", ".join(exit_result.reasons[:3]) if exit_result.reasons else ""
                    exit_html = (
                        f'<span style="font-size:9pt; color:#666;">[{now_str}]</span> '
                        f'<span style="font-size:10pt; color:{urg_c}; font-weight:bold;">'
                        f"\u26a0 {urg}</span> "
                        f'<span style="font-size:10pt; font-weight:bold;">'
                        f"{trade.direction} {pair}</span> "
                        f'<span style="font-size:9pt; color:#666;">'
                        f"P/L: {trade.pnl_pips:+.1f}p | {reasons_txt}</span>"
                    )
                    self._alert_history.appendleft(exit_html)

                # ── Paper trade auto-close: backtester-style spread check ──
                # The exit engine votes are used for UI display only.
                # Paper trades close ONLY when the base/quote spread collapses
                # below _EXIT_SPREAD_THRESHOLD (same logic as backtester._check_exit).
                # This avoids the overly aggressive 5-detector vote system.
                pass  # paper exit handled below via spread check

                # ── cTrader auto-close on URGENT ──
                if (
                    trade.exit_urgency == "URGENT"
                    and self._ctrader_bridge
                    and self._ctrader_pos_mgr
                    and self._ctrader_config.get("ctrader_enabled")
                    and self._ctrader_config.get("ctrader_auto_close")
                    and self._ctrader_bridge.is_connected
                ):
                    ct_pos = self._ctrader_pos_mgr.get_position(pair)
                    if ct_pos:
                        self._ctrader_bridge.close_position(
                            ct_pos.position_id, ct_pos.volume
                        )

            # ── MT5 spread-collapse exit (forex pairs only) ──
            if forex_only and htf_composite and self._mt5_pos_mgr.has_position(pair):
                _base_mt5, _quote_mt5 = pair[:3], pair[3:]
                _b_mt5 = htf_composite.get(_base_mt5, 0.0)
                _q_mt5 = htf_composite.get(_quote_mt5, 0.0)
                mt5_pos = self._mt5_pos_mgr.get_position(pair)
                if mt5_pos:
                    if mt5_pos.direction == "BUY":
                        _sp_mt5 = _b_mt5 - _q_mt5
                    else:
                        _sp_mt5 = _q_mt5 - _b_mt5
                    if _sp_mt5 < _PAPER_EXIT_SPREAD_THRESHOLD:
                        if self._mt5_config.get("mt5_auto_close", True):
                            self._mt5_trader.close_position(pair)

            # ── Paper trade: Counter-Momentum Exit (Stoch v2 + Legacy) ──
            # Uses Stoch engine velocity (QM4-style 0-10 scores) for exit detection.
            # Falls back to legacy momentum phases if stoch data unavailable.
            # Only exits when strong counter-momentum detected → trade edge gone.
            # SKIPPED for XAU: pair[:3]="XAU" isn't in stoch_velocities so the
            # logic would react to USD velocity alone, then call the wrong
            # tracker (self._paper_trader is the FOREX Sv2 trader) to close.
            _trade_age_s = time.time() - trade.entry_time if trade.entry_time > 0 else 999
            _MIN_AGE_FOR_EXIT = 120  # 2 min grace period
            _exit_reason = ""

            if forex_only and trade.is_paper and _trade_age_s >= _MIN_AGE_FOR_EXIT:
                _base_ccy, _quote_ccy = pair[:3], pair[3:]

                # ── Primary: Stoch engine exit (QM4-style velocity) ──
                if hasattr(result, 'stoch_velocities') and result.stoch_velocities:
                    _base_vel = result.stoch_velocities.get(_base_ccy, 0.0)
                    _quote_vel = result.stoch_velocities.get(_quote_ccy, 0.0)
                    _EXIT_VEL = 2.0  # velocity threshold for counter-momentum

                    if trade.direction == "BUY":
                        _base_reversing = _base_vel < -_EXIT_VEL
                        _quote_reversing = _quote_vel > _EXIT_VEL
                    else:
                        _base_reversing = _base_vel > _EXIT_VEL
                        _quote_reversing = _quote_vel < -_EXIT_VEL

                    if _base_reversing and _quote_reversing:
                        _exit_reason = "counter_momentum"
                    elif _base_reversing or _quote_reversing:
                        # Single-side reversal: count confirmations
                        self._sc_confirm_count[pair] = self._sc_confirm_count.get(pair, 0) + 1
                        if self._sc_confirm_count[pair] >= 5:
                            _exit_reason = "momentum_fading"
                    else:
                        self._sc_confirm_count[pair] = 0

                # ── Fallback: Legacy momentum phases ──
                elif result.momentum_phases:
                    _base_phase = result.momentum_phases.get(_base_ccy)
                    _quote_phase = result.momentum_phases.get(_quote_ccy)

                    if _base_phase and _quote_phase:
                        _COUNTER_MAGS = ("explosive",)
                        _MIN_VEL = 1.5

                        if trade.direction == "BUY":
                            _base_reversing = (
                                _base_phase.velocity < -_MIN_VEL
                                and _base_phase.accel_magnitude in _COUNTER_MAGS
                            )
                            _quote_reversing = (
                                _quote_phase.velocity > _MIN_VEL
                                and _quote_phase.accel_magnitude in _COUNTER_MAGS
                            )
                        else:
                            _base_reversing = (
                                _base_phase.velocity > _MIN_VEL
                                and _base_phase.accel_magnitude in _COUNTER_MAGS
                            )
                            _quote_reversing = (
                                _quote_phase.velocity < -_MIN_VEL
                                and _quote_phase.accel_magnitude in _COUNTER_MAGS
                            )

                        if _base_reversing and _quote_reversing:
                            _exit_reason = "counter_momentum"
                        else:
                            self._sc_confirm_count[pair] = 0

                if _exit_reason:
                    close_p = result.close_prices.get(pair, trade.current_price)
                    rec = self._paper_trader._close_and_journal(
                        pair, _exit_reason, close_p
                    )
                    if rec:
                        self._paper_trader.save_journal()
                        self._save_trades()
                        self._refresh_perf_paper_tab()
                        self._exit_engine.clear_trade(pair)
                        self._sc_confirm_count.pop(pair, None)
                        self._last_exit_alert_time.pop(pair, None)
                        now_str = datetime.now(_jst()).strftime("%H:%M:%S")
                        _rc = "#e65100"
                        _exit_html = (
                            f'<span style="font-size:9pt; color:#666;">[{now_str}]</span> '
                            f'<span style="font-size:10pt; color:{_rc}; font-weight:bold;">'
                            f'\U0001f4dd PAPER EXIT</span> '
                            f'<span style="font-size:10pt; font-weight:bold;">'
                            f'{trade.direction} {pair}</span> '
                            f'<span style="font-size:9pt; color:#666;">'
                            f'{_exit_reason}  P/L: {rec.pnl_pips:+.1f}p  '
                            f'Peak:{rec.peak_pnl_pips:+.1f}p</span>'
                        )
                        self._alert_history.appendleft(_exit_html)
                        continue  # skip building inline HTML for closed trade

            # ── Build inline trade status for alert panel ──
            dir_color = "#1b8a2a" if trade.direction == "BUY" else "#c62828"
            pnl_color = "#1b8a2a" if trade.pnl_pips >= 0 else "#c62828"
            urg_colors = {"": "#888", "WATCH": "#f57f17", "CLOSE": "#e65100", "URGENT": "#d50000"}
            urg_color = urg_colors.get(trade.exit_urgency, "#888")

            # Vote dots
            vote_dots = ""
            for det, voted in trade.exit_votes.items():
                dot_c = "#d50000" if voted else "#cccccc"
                vote_dots += f'<span style="color:{dot_c};">{"\u25cf" if voted else "\u25cb"}</span>'

            # Urgency badge
            urg_badge = ""
            if trade.exit_urgency:
                urg_badge = (
                    f'<span style="color:{urg_color}; font-weight:bold;"> '
                    f'{trade.exit_urgency}</span>'
                )

            sl_tp_txt = ""
            if trade.is_paper and trade.sl_pips > 0:
                sl_tp_txt = f"SL:{trade.sl_pips:.1f}p  TP:{trade.tp_pips:.1f}p  "

            # Entry time for display
            _entry_str = datetime.fromtimestamp(
                trade.entry_time, tz=_jst()
            ).strftime("%H:%M:%S") if trade.entry_time > 0 else "?"

            # System type label
            _etype = getattr(trade, "entry_type", "stoch_v2")
            _sys_labels = {
                "stoch_v2": ("Sv2", "#4a6fa5"),
                "sv2_a_tuned": ("A-t", "#00897b"),
                "sv2_ss": ("SS", "#9c27b0"),
                "sv2_b_tuned": ("B-t", "#7b1fa2"),
                "sv2_atr": ("ATR", "#e65100"),
                "sv2_qm4": ("QM4", "#ff6f00"),
                "breakout": ("BRK", "#0288d1"),
                "squeeze": ("SQZ", "#7b1fa2"),
                # Squeeze-REV (2026-04-29): pink to differentiate from purple SQZ
                "squeeze_rev": ("SQZ-R", "#ad1457"),
                "divergence": ("DIV", "#00796b"),
                "dtc_combo": ("DTC", "#0277bd"),
                # Live-candle mirror systems — same short name + "-live"
                "sv2_live":         ("Sv2-live", "#1565c0"),
                "sv2_a_tuned_live": ("A-t-live", "#6a1b9a"),
                "sv2_ss_live":      ("SS-live",  "#00695c"),
                "sv2_b_tuned_live": ("B-t-live", "#ad1457"),
                "sv2_atr_live":     ("ATR-live", "#bf360c"),
                # Sv2-upgraded (2026-04-23): pink to stand out from the other systems
                "sv2_upgraded":     ("Sv2-up",   "#e91e63"),
                # AU Gold suite (2026-04-24): gold color palette distinguishes
                # XAUUSD trades from forex pair trades in the OPEN TRADES panel.
                "au1_london_breakout": ("AU1",    "#ffb300"),
                "au2_ny_orb":          ("AU2",    "#ff8f00"),
                "au3_trend_pullback":  ("AU3",    "#ff6f00"),
                "au4_usd_divergence":  ("AU4",    "#e65100"),
                "au5_asian_mean_rev":  ("AU5",    "#bf360c"),
            }
            _sys_name, _sys_color = _sys_labels.get(_etype, ("?", "#888"))

            entry = (
                f'<span style="font-size:13pt; color:#666;">[{_entry_str}]</span> '
                f'<span style="font-size:15pt; color:{dir_color}; font-weight:bold;">'
                f'\U0001f4c8 {trade.direction} {pair}</span>'
                f'<span style="font-size:16pt; color:{pnl_color}; font-weight:bold;"> '
                f'{trade.pnl_pips:+.1f}p</span>'
                f'{urg_badge}'
                f'<span style="font-size:13pt; color:#888;">  '
                f'{sl_tp_txt}</span>'
                f'<br><span style="font-size:11pt; color:{_sys_color}; font-weight:bold;">'
                f'{_sys_name}</span> '
                f'<span style="font-size:11pt; color:#888;">'
                f'Conv:{trade.entry_conviction}  '
                f'{trade.duration_minutes:.0f}m  '
                f'{vote_dots}</span>'
                f' <a href="close:{pair}" '
                f'style="font-size:9pt; color:#c62828; text-decoration:none;'
                f' font-weight:bold;">[X]</a>'
            )
            trade_html_parts.append(entry)

        # Store current M1 scores for next cycle's momentum comparison
        self._prev_m1_pair_scores = dict(current_m1_scores)

        # Store active trade HTML for rendering in alert panel
        self._active_trade_html_parts = trade_html_parts
        self._render_alert_panel_with_trades()

    def _auto_update_dukascopy(self) -> None:
        """Check what Dukascopy data is missing and download only the gap."""
        try:
            from datetime import date, timedelta, datetime
            from pathlib import Path
            import sys as _sys

            if getattr(_sys, "frozen", False):
                data_dir = Path(_sys.executable).parent / "data" / "dukascopy"
            else:
                data_dir = Path(__file__).resolve().parent.parent.parent / "data" / "dukascopy"

            from PyQt6.QtCore import QThread, pyqtSignal

            class _DlThread(QThread):
                done = pyqtSignal(str)

                def run(self_t):
                    try:
                        from takumi_trader.core.dukascopy_downloader import (
                            DukascopyDownloader,
                            ALL_28_PAIRS,
                        )
                        import pyarrow.parquet as pq

                        dl = DukascopyDownloader(data_dir)
                        yesterday = date.today() - timedelta(days=1)
                        total_new = 0
                        pairs_updated = 0

                        # Bug fix: `dl.ALL_PAIRS` doesn't exist on the class.
                        # The constant is a module-level name `ALL_28_PAIRS`.
                        for pair in ALL_28_PAIRS:
                            path = dl.get_parquet_path(pair)
                            if path.exists():
                                # Find the latest timestamp in existing data
                                try:
                                    table = pq.read_table(path, columns=["time"])
                                    if len(table) > 0:
                                        import pyarrow.compute as pc
                                        latest_ts = pc.max(table.column("time")).as_py()
                                        latest_date = datetime.utcfromtimestamp(latest_ts).date()
                                        if latest_date >= yesterday:
                                            continue  # already up to date
                                        start = latest_date + timedelta(days=1)
                                    else:
                                        start = yesterday - timedelta(days=365)
                                except Exception:
                                    start = yesterday - timedelta(days=7)
                            else:
                                # No file — download last year
                                start = yesterday - timedelta(days=365)

                            count = dl.download_pair(
                                pair, start, yesterday, skip_existing=True,
                            )
                            if count > 0:
                                total_new += count
                                pairs_updated += 1

                        msg = (
                            f"Dukascopy auto-update: {total_new} new candles "
                            f"across {pairs_updated} pairs"
                        )
                        self_t.done.emit(msg)
                    except Exception as e:
                        self_t.done.emit(f"Dukascopy auto-update failed: {e}")

            self._dk_auto_thread = _DlThread()
            self._dk_auto_thread.done.connect(
                lambda msg: logger.info(msg)
            )
            self._dk_auto_thread.start()
            logger.info("Dukascopy auto-update: checking for missing data...")

        except Exception as e:
            logger.warning("Dukascopy auto-update setup failed: %s", e)

    def _refresh_news_calendar(self) -> None:
        """Download current week's RED news events and update cache."""
        try:
            n = self._news_filter.download_current_week()
            if n > 0:
                self._news_filter.save_cache()
                logger.info("News calendar refreshed: %d new events", n)
        except Exception as e:
            logger.warning("News calendar refresh failed: %s", e)

    def _refresh_perf_paper_tab(self) -> None:
        """Refresh the Performance dialog's Paper Trades tab if it's open."""
        dlg = getattr(self, "_perf_dialog", None)
        if dlg is not None and dlg.isVisible():
            try:
                dlg._paper_records = dlg._load_paper_records()
                dlg._update_paper_pair_combo()
                dlg._render_paper()
            except Exception:
                pass

    def _update_closed_trades_panel(self) -> None:
        """Render the closed trades panel from paper trade journal.

        Note: since we now log trades at entry time too, the journal contains
        both OPEN and CLOSED records. Filter to only closed ones for this panel.
        """
        # Merge closed records from ALL 9 systems, sorted by close_time
        _all_journals = []
        _SYS_LABELS = {
            "stoch_v2": "Sv2", "sv2_ss": "SS", "sv2_atr": "ATR",
            "sv2_qm4": "QM4", "sv2_a_tuned": "A-t", "sv2_b_tuned": "B-t",
            "breakout": "BRK", "squeeze": "SQZ", "squeeze_rev": "SQZ-R",
            "divergence": "DIV",
        }
        for _pt in (self._paper_trader, self._paper_trader_ss,
                     self._paper_trader_atr, self._paper_trader_qm4,
                     self._paper_trader_a_tuned, self._paper_trader_b_tuned,
                     self._paper_trader_breakout, self._paper_trader_squeeze,
                     self._paper_trader_squeeze_rev,
                     self._paper_trader_divergence, self._paper_trader_dtc_combo):
            if _pt is None:
                continue
            for r in _pt.journal:
                if r.close_reason:
                    _all_journals.append(r)
        journal = sorted(_all_journals, key=lambda r: r.close_time)

        # If user cleared the panel, stay hidden until new trades close
        if self._closed_trades_suppressed:
            current_count = len(journal)
            if current_count <= getattr(self, "_closed_trades_hidden_count", 0):
                return  # No new closed trades since clear — stay hidden
            else:
                self._closed_trades_suppressed = False  # New trade closed, show again

        if not journal:
            self._closed_panel.setText("No closed trades yet.")
            self._closed_panel.setStyleSheet(
                "color: #888888; padding: 4px 6px; background: #ffffff;"
            )
            self._closed_stats_label.setText("")
            return

        # Summary stats label intentionally blank — user removed the P/L / TP / SL
        # header stats from the CLOSED TRADES panel.
        self._closed_stats_label.setText("")

        # Render individual trades (newest first, max 50)
        html_parts: list[str] = []
        for idx, rec in enumerate(reversed(journal[-50:])):
            pnl_color = "#1b8a2a" if rec.is_win else "#c62828"
            dir_color = "#1b8a2a" if rec.direction == "BUY" else "#c62828"

            # Calculate Risk:Reward Ratio
            if rec.sl_pips > 0:
                rrr = abs(rec.pnl_pips) / rec.sl_pips
                rrr_str = f"{rrr:.1f}R"
                if rec.is_win:
                    rrr_display = f'+{rrr_str}'
                else:
                    rrr_display = f'-{rrr_str}'
            else:
                rrr_display = "—"

            # Exit reason label
            reason_labels = {
                "sl_hit": "\u274c SL",
                "tp_hit": "\u2705 TP",
                "signal_exit": "\u26a0 Signal",
            }
            reason_colors = {
                "sl_hit": "#c62828",
                "tp_hit": "#1b8a2a",
                "signal_exit": "#e65100",
            }
            reason_label = reason_labels.get(rec.close_reason, rec.close_reason)
            reason_color = reason_colors.get(rec.close_reason, "#666")

            # Time string + system label
            time_str = rec.entry_time_str[-8:] if rec.entry_time_str else "—"
            _sys_lbl = _SYS_LABELS.get(getattr(rec, 'entry_type', ''), '')

            entry = (
                f'<span style="font-size:9pt; color:#666;">[{time_str}]</span> '
                f'<span style="font-size:8pt; color:#4a6fa5; font-weight:bold;">[{_sys_lbl}]</span> '
                f'<span style="font-size:10pt; color:{dir_color}; font-weight:bold;">'
                f'{rec.direction} {rec.pair}</span>  '
                f'<span style="font-size:11pt; color:{pnl_color}; font-weight:bold;">'
                f'{rec.pnl_pips:+.1f}p</span>  '
                f'<span style="font-size:10pt; color:{pnl_color}; font-weight:bold;">'
                f'{rrr_display}</span>  '
                f'<span style="font-size:10pt; color:{reason_color}; font-weight:bold;">'
                f'{reason_label}</span>  '
                f'<span style="font-size:9pt; color:#888;">'
                f'{rec.duration_minutes:.0f}min  '
                f'Peak:{rec.peak_pnl_pips:+.1f}p  Worst:{rec.worst_pnl_pips:+.1f}p  '
                f'SL:{rec.sl_pips:.1f}  TP:{rec.tp_pips:.1f}</span>'
            )

            # Post-close observation data
            if rec.post_close_complete:
                left = rec.post_close_max_mfe_pips - rec.pnl_pips
                left_color = "#e65100" if left > 2 else "#888"
                entry += (
                    f'<span style="font-size:8pt; color:#4a6fa5;">  '
                    f'[4h: MFE:{rec.post_close_max_mfe_pips:+.1f}p '
                    f'MAE:{rec.post_close_max_mae_pips:.1f}p '
                    f'End:{rec.post_close_final_pips:+.1f}p]</span>'
                )
            elif rec.close_time > 0 and not rec.post_close_complete:
                entry += (
                    f'<span style="font-size:8pt; color:#999;">  '
                    f'[\U0001f50d {rec.post_close_minutes:.0f}m...]</span>'
                )

            # Alternating background for readability
            bg = "#e2e2e2" if idx % 2 == 0 else "#ffffff"
            entry = f'<div style="background:{bg}; padding:2px 4px; margin:1px 0; border-bottom:1px solid #e0e0e0;">{entry}</div>'
            html_parts.append(entry)

        html = "".join(html_parts)
        self._closed_panel.setText(html)
        self._closed_panel.setStyleSheet(
            "padding: 4px 6px; background: #ffffff;"
        )

    def _track_trade(self, pair: str, direction: str, entry_price: float) -> None:
        """Start tracking a trade for a pair.

        Args:
            pair: Currency pair symbol.
            direction: "BUY" or "SELL".
            entry_price: Price at trade entry.
        """
        session = get_current_session()

        # Calculate dynamic target
        # Use rough ATR estimate from pair score magnitude
        adr_consumed = 0.0  # Will be updated on next cycle
        strength_delta = 0.0
        base, quote = pair[:3], pair[3:]
        if base in self._last_composite_scores and quote in self._last_composite_scores:
            strength_delta = abs(
                self._last_composite_scores[base] - self._last_composite_scores[quote]
            )

        # Get conviction score from cached results
        conv = self._latest_conviction.get(pair)
        conviction = conv.conviction if conv else 100

        # Default ATR-based target
        is_jpy = "JPY" in pair
        base_target = 12.0 if is_jpy else 8.0
        target, _ = calculate_dynamic_target(
            pair=pair,
            atr_pips=base_target,
            adr_consumed_pct=adr_consumed,
            strength_delta=strength_delta,
            session=session,
            conviction=conviction,
        )

        trade = self._trade_tracker.open_trade(
            pair=pair,
            direction=direction,
            entry_price=entry_price,
            currency_scores=self._last_composite_scores,
            target_pips=target,
        )
        if trade:
            trade.entry_conviction = conviction
            self._save_trades()
            logger.info("Trade tracked: %s %s target=%.1f conv=%d",
                         direction, pair, target, conviction)

    def _close_tracked_trade(self, pair: str) -> None:
        """Close a tracked trade."""
        trade = self._trade_tracker.get_trade(pair)
        if trade and trade.is_paper:
            # Close via paper trader to journal it properly
            self._paper_trader._close_and_journal(
                pair, "manual", trade.current_price
            )
            self._paper_trader.save_journal()
            self._refresh_perf_paper_tab()
        else:
            self._trade_tracker.close_trade(pair, reason="manual")
        if trade:
            self._exit_engine.clear_trade(pair)
            self._last_exit_alert_time.pop(pair, None)
            self._save_trades()
            logger.info("Trade closed: %s P/L=%.1f pips", pair, trade.pnl_pips)

    def _on_alert_link_clicked(self, url: str) -> None:
        """Handle clicks on TRACK / CLOSE links in the alert panel.

        Link formats: 'track:EURUSD:BUY' or 'close:EURUSD'
        """
        if url.startswith("close:"):
            pair = url.split(":")[1]
            self._close_tracked_trade(pair)
            return
        if not url.startswith("track:"):
            return
        parts = url.split(":")
        if len(parts) != 3:
            return
        _, pair, direction = parts

        if self._trade_tracker.has_trade(pair):
            logger.info("Already tracking %s", pair)
            return

        # Get the latest close price for this pair
        entry_price = self._latest_close_prices.get(pair, 0.0)
        if entry_price <= 0:
            logger.warning("No close price available for %s", pair)
            return

        self._track_trade(pair, direction, entry_price)
        logger.info("Trade tracked via link: %s %s @ %.5f", direction, pair, entry_price)

    def _on_connection_status(self, connected: bool, message: str) -> None:
        """Update status bar with connection info + popup on disconnect."""
        if connected:
            self._status_dot.setStyleSheet("color: #2e7d32;")
            self._status_label.setText(message)
            self.statusBar().setStyleSheet("background: #e8e8e8; color: #555555;")
            # If we previously alerted on disconnect, mark it resolved
            if hasattr(self, "_health_alerts"):
                self._health_alerts.notify_recovery(
                    "MT5", f"Reconnected: {message}",
                    dedup_key="mt5_disconnect",
                )
        else:
            self._status_dot.setStyleSheet("color: #d32f2f;")
            self._status_label.setText(message)
            self.statusBar().setStyleSheet("background: #ffcdd2; color: #b71c1c;")
            # POPUP: MT5 is the data feed for everything. Loss = full stop.
            if hasattr(self, "_health_alerts"):
                self._health_alerts.notify(
                    "critical", "MT5",
                    f"MT5 connection lost: {message}\n\n"
                    "TAKUMI cannot receive prices. Paper trades and live "
                    "cTrader orders are PAUSED until reconnection.\n\n"
                    "Action:\n"
                    "  1. Check MT5 terminal is running and logged in\n"
                    "  2. Verify internet / broker server reachable\n"
                    "  3. Restart MT5 if needed (TAKUMI auto-reconnects)",
                    dedup_key="mt5_disconnect",
                )

    # ── Shadow-sim worker signal slots (Phase D.4) ───────────────────
    def _on_sim_cycle_stats(self, stats: dict) -> None:
        """Receive per-cycle stats from ShadowSimWorker.

        Phase D.4 only logs at INFO; Phase E will add a dedicated
        stats panel in the LiveCandleDialog Sv2 tab. Bot-side INFO
        log gives the operator (Ryosuke) a heartbeat that the worker
        is alive and producing data."""
        try:
            logger.info(
                "[SHADOW SIM-CYCLE] permanent_failed=%d sim_attempted=%d "
                "sim_succeeded=%d transient_retries=%d calibrations=%d "
                "first_run=%s",
                stats.get("permanent_failed_this_cycle", 0),
                stats.get("sim_attempted_this_cycle", 0),
                stats.get("sim_succeeded_this_cycle", 0),
                stats.get("sim_transient_retries_this_cycle", 0),
                stats.get("calibrations_written_this_cycle", 0),
                stats.get("first_run_active", "?"),
            )
        except Exception:
            pass  # logging must never break the slot

    def _on_sim_drift_warning(self, message: str) -> None:
        """Forward calibration-drift warnings to the operator alert path.

        Drift = sim systematically over- or under-pessimistic vs real
        execution. The simulator's _check_calibration_drift fires this
        when the rolling window of N calibrations leaves the warn band.
        Critical-level alert because miscalibrated pessimism corrupts
        downstream Edge Miner inferences."""
        logger.warning("[SHADOW DRIFT] %s", message)
        if hasattr(self, "_health_alerts"):
            try:
                self._health_alerts.notify(
                    "warning", "Shadow",
                    f"Calibration drift detected:\n\n{message}\n\n"
                    "The pessimistic simulator is no longer matching real "
                    "executions. Edge Miner training data quality is "
                    "compromised until pessimism config is recalibrated.",
                    dedup_key="shadow_drift",
                )
            except Exception:
                pass

    def _on_sim_fatal(self, message: str) -> None:
        """Dead-man's-switch: shadow worker died with an unrecoverable error.

        The worker emits this only if its top-level run() catch fires —
        which in current code is unreachable, but defensive. Surface
        critically: shadow logging stops accumulating data until restart."""
        logger.critical("[SHADOW FATAL] %s", message)
        if hasattr(self, "_health_alerts"):
            try:
                self._health_alerts.notify(
                    "critical", "Shadow",
                    f"Shadow-sim worker died:\n\n{message}\n\n"
                    "Calibration data has stopped accumulating. Trading "
                    "continues normally, but Edge Miner inference quality "
                    "will not improve until TAKUMI is restarted.",
                    dedup_key="shadow_fatal",
                )
            except Exception:
                pass

    def _retroactive_sl_tp_check(self) -> None:
        """Check all active trades against historical candles on startup."""
        for label, pt in [
            ("Sv2", self._paper_trader),
            ("Sv2-tuned", self._paper_trader_a_tuned),
            ("SS", self._paper_trader_ss),
            ("SS-tuned", self._paper_trader_b_tuned),
            ("ATR", self._paper_trader_atr),
            ("QM4", self._paper_trader_qm4),
            ("Breakout", self._paper_trader_breakout),
            ("Squeeze", self._paper_trader_squeeze),
            ("Divergence", self._paper_trader_divergence),
            ("DTC-combo", self._paper_trader_dtc_combo),
        ]:
            active = pt.get_active_paper_trades()
            if active:
                closed = pt.retroactive_sl_tp_check()
                if closed:
                    logger.info("[%s] Retroactive check: closed %d trades", label, closed)

    # ── Always on Top ────────────────────────────────────────────────

    def _open_backtest(self) -> None:
        """Open the backtest simulator dialog on the primary monitor."""
        from takumi_trader.ui.backtest_dialog import BacktestDialog
        from PyQt6.QtCore import Qt
        from PyQt6.QtGui import QGuiApplication
        dlg = BacktestDialog()
        dlg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        # Center on primary screen
        primary = QGuiApplication.primaryScreen()
        if primary:
            geo = primary.availableGeometry()
            dlg.move(
                geo.x() + (geo.width() - dlg.width()) // 2,
                geo.y() + (geo.height() - dlg.height()) // 2,
            )
        # Store reference to prevent garbage collection
        self._backtest_dialog = dlg
        dlg.show()

    def _open_livecan(self) -> None:
        """Open the Live Candle CSI systems dialog.

        Shows performance of the 5 "-live" paper systems that mirror
        A/B/C/D/E but use the live-candle engine (computes every
        worker cycle on forming bars) instead of candle-close-only.
        The dialog uses the same rich layout as Alert Performance
        (pair filter, HTML summary, equity curve, full trade table).
        """
        from takumi_trader.ui.live_candle_dialog import LiveCandleDialog
        from PyQt6.QtCore import Qt as _Qt
        from PyQt6.QtGui import QGuiApplication as _QGA
        # BUG FIX 2026-04-22: previously passed the TRACKER files
        # (tracked_trades_*_live.json) which hold ONLY currently-open
        # trades. Since all of yesterday's trades closed overnight,
        # those files were empty at startup and the dialog showed "0
        # trades" even though the JOURNAL files (paper_trades_*_live.json)
        # held the full history. Pass the JOURNAL paths — matches the
        # pattern used for the candle-close systems in the Alert
        # Performance dialog.
        dlg = LiveCandleDialog(
            self,
            sv2_live_trades_file=_DATA_DIR / "paper_trades_sv2_live.json",
            sv2_live_paper_trader=self._paper_trader_sv2_live,
            sv2_a_tuned_live_trades_file=_DATA_DIR / "paper_trades_sv2_a_tuned_live.json",
            sv2_a_tuned_live_paper_trader=self._paper_trader_sv2_a_tuned_live,
            sv2_ss_live_trades_file=_DATA_DIR / "paper_trades_sv2_ss_live.json",
            sv2_ss_live_paper_trader=self._paper_trader_sv2_ss_live,
            sv2_b_tuned_live_trades_file=_DATA_DIR / "paper_trades_sv2_b_tuned_live.json",
            sv2_b_tuned_live_paper_trader=self._paper_trader_sv2_b_tuned_live,
            sv2_atr_live_trades_file=_DATA_DIR / "paper_trades_sv2_atr_live.json",
            sv2_atr_live_paper_trader=self._paper_trader_sv2_atr_live,
            # AU Gold suite (2026-04-24) — XAUUSD-only paper systems
            au1_trades_file=_DATA_DIR / "paper_trades_au1_london.json",
            au1_paper_trader=self._paper_trader_au1,
            au2_trades_file=_DATA_DIR / "paper_trades_au2_ny_orb.json",
            au2_paper_trader=self._paper_trader_au2,
            au3_trades_file=_DATA_DIR / "paper_trades_au3_pullback.json",
            au3_paper_trader=self._paper_trader_au3,
            au4_trades_file=_DATA_DIR / "paper_trades_au4_divergence.json",
            au4_paper_trader=self._paper_trader_au4,
            au5_trades_file=_DATA_DIR / "paper_trades_au5_mean_rev.json",
            au5_paper_trader=self._paper_trader_au5,
        )
        dlg.setAttribute(_Qt.WidgetAttribute.WA_DeleteOnClose)
        primary = _QGA.primaryScreen()
        if primary:
            geo = primary.availableGeometry()
            dlg.move(
                geo.x() + (geo.width() - dlg.width()) // 2,
                geo.y() + (geo.height() - dlg.height()) // 2,
            )
        # Store reference to prevent GC
        self._livecan_dialog = dlg
        dlg.show()

    def _toggle_always_on_top(self) -> None:
        """Toggle the window staying on top of all other applications."""
        on_top = self._btn_pin.isChecked()
        flags = self.windowFlags()
        if on_top:
            self.setWindowFlags(flags | Qt.WindowType.WindowStaysOnTopHint)
            self._btn_pin.setText("\U0001f4cc Pinned")
        else:
            self.setWindowFlags(flags & ~Qt.WindowType.WindowStaysOnTopHint)
            self._btn_pin.setText("\U0001f4cc Pin on Top")
        self.show()  # Required after changing window flags

    # ── Compact Mode ──────────────────────────────────────────────────

    def _toggle_compact(self) -> None:
        """Toggle compact always-on-top mode."""
        self._compact = not self._compact

        if self._compact:
            self._normal_geometry = self.geometry()
            self._normal_flags = self.windowFlags()

            # Frameless + always-on-top (no title bar at all)
            self.setWindowFlags(
                Qt.WindowType.FramelessWindowHint
                | Qt.WindowType.WindowStaysOnTopHint
            )

            # Smaller font (1pt less than normal, minimum 7)
            compact_fs = max(7, self._font_size - 1)
            small_font = QFont("Consolas", compact_fs)
            for tbl in (self._table, self._ccy_table):
                for row in range(tbl.rowCount()):
                    for col in range(tbl.columnCount()):
                        item = tbl.item(row, col)
                        if item:
                            item.setFont(small_font)

            # Fixed heights for compact rows
            compact_row_h = max(18, 20 + (compact_fs - 9) * 2)
            for tbl in (self._table, self._ccy_table):
                tbl.verticalHeader().setDefaultSectionSize(compact_row_h)
            pairs_h = len(DISPLAY_PAIRS) * compact_row_h + 40
            ccy_h = len(CURRENCIES) * compact_row_h + 4
            self._table.setFixedHeight(pairs_h)
            self._ccy_table.setFixedHeight(ccy_h)

            # Remove toolbar entirely (hide doesn't collapse the dock area)
            self.removeToolBar(self._toolbar)
            self._toolbar.hide()
            self.menuBar().hide()
            self.statusBar().hide()
            self._filter_toolbar.hide()
            self._trades_header.hide()
            self._trades_scroll.hide()
            self._alert_header.hide()
            self._alert_scroll.hide()

            # Zero margins in compact mode
            self._main_layout.setContentsMargins(0, 0, 0, 0)

            # Add a thin drag bar at the top with an exit button
            self._compact_bar = QWidget()
            self._compact_bar.setFixedHeight(20)
            self._compact_bar.setStyleSheet("background: #e0e0e0;")
            bar_layout = QHBoxLayout(self._compact_bar)
            bar_layout.setContentsMargins(4, 0, 4, 0)
            bar_layout.setSpacing(2)

            drag_label = QLabel("TAKUMI Trader")
            drag_label.setFont(QFont("Segoe UI", 8))
            drag_label.setStyleSheet("color: #777777; background: #e0e0e0;")
            bar_layout.addWidget(drag_label, stretch=1)

            close_btn = QPushButton("\u2715")
            close_btn.setFixedSize(18, 18)
            close_btn.setStyleSheet(
                "background: #c0c0c0; color: #333; border-radius: 9px;"
                " font-size: 11px; padding: 0;"
            )
            close_btn.setToolTip("Exit Compact Mode")
            close_btn.clicked.connect(self._toggle_compact)
            bar_layout.addWidget(close_btn)

            self._main_layout.insertWidget(0, self._compact_bar)

            # Size window to fit drag bar + both tables
            total_h = 20 + pairs_h + ccy_h
            self.resize(480, total_h)
            self.show()
        else:
            # Remove compact bar
            if hasattr(self, "_compact_bar"):
                self._compact_bar.setParent(None)
                self._compact_bar.deleteLater()

            # Restore fonts and table heights to normal size
            self._apply_font_size()

            self.setWindowFlags(
                self._normal_flags
                if hasattr(self, "_normal_flags")
                else Qt.WindowType.Window
            )
            # Re-add toolbar
            self.addToolBar(self._toolbar)
            self._toolbar.show()
            self.menuBar().show()
            self.statusBar().show()
            self._filter_toolbar.show()
            self._trades_header.show()
            self._trades_scroll.show()
            self._alert_header.show()
            self._alert_scroll.show()

            # Restore margins
            self._main_layout.setContentsMargins(4, 4, 4, 4)
            if hasattr(self, "_normal_geometry"):
                self.setGeometry(self._normal_geometry)
            self.show()

    # ── Drag support for compact mode ─────────────────────────────────

    def mousePressEvent(self, event) -> None:
        """Track drag start in compact mode."""
        if self._compact and event.button() == Qt.MouseButton.LeftButton:
            self._drag_pos = event.globalPosition().toPoint() - self.frameGeometry().topLeft()
            event.accept()

    def mouseMoveEvent(self, event) -> None:
        """Handle window dragging in compact mode."""
        if self._compact and self._drag_pos and event.buttons() & Qt.MouseButton.LeftButton:
            self.move(event.globalPosition().toPoint() - self._drag_pos)
            event.accept()

    def mouseReleaseEvent(self, event) -> None:
        """Clear drag state."""
        self._drag_pos = None

    def contextMenuEvent(self, event) -> None:
        """Right-click menu — exit compact mode or open settings."""
        if self._compact:
            menu = QMenu(self)
            restore_action = menu.addAction("Exit Compact Mode")
            settings_action = menu.addAction("Settings")
            chosen = menu.exec(event.globalPos())
            if chosen == restore_action:
                self._toggle_compact()
            elif chosen == settings_action:
                self._open_settings()

    # ── Settings ──────────────────────────────────────────────────────

    def _open_settings(self) -> None:
        """Open the settings dialog and apply changes."""
        from PyQt6.QtGui import QGuiApplication as _QGA
        dialog = SettingsDialog(self)
        # Position at the top-center of the main (primary) screen
        primary = _QGA.primaryScreen()
        if primary:
            geo = primary.availableGeometry()
            dialog.move(
                geo.x() + (geo.width() - dialog.width()) // 2,
                geo.y(),
            )
        if dialog.exec():
            settings = dialog.get_settings()
            self._alert_mgr.sound_enabled = settings["sound_enabled"]
            self._alert_mgr.sound_file = settings["sound_file"]
            self._alert_mgr.update_cooldown(settings["cooldown_seconds"])
            if settings["font_size"] != self._font_size:
                self._font_size = settings["font_size"]
                self._apply_font_size()
            # Toggle compact mode if changed
            want_compact = settings.get("compact_mode", False)
            if want_compact != self._compact:
                self._toggle_compact()

            # ── Apply OCR mode change ──
            # OCR forced OFF (2026-05-14) — see startup-time comment in
            # _setup_ui near CsiWorker construction. Settings toggle is
            # a no-op until external QM4 software comes back online.
            new_ocr = False
            if hasattr(self, "_csi_worker"):
                self._csi_worker.set_ocr_mode(new_ocr)

            # ── Apply cTrader settings changes ──
            old_enabled = self._ctrader_config.get("ctrader_enabled", False)
            self._ctrader_config = settings  # update cached config
            new_enabled = settings.get("ctrader_enabled", False)
            if self._ctrader_bridge:
                if new_enabled and not old_enabled:
                    self._ctrader_bridge.start(settings)
                elif not new_enabled and old_enabled:
                    self._ctrader_bridge.stop()
                    self._ctrader_status_label.setText("")

    # ── CSI / QM4 alert handlers ──────────────────────────────────────

    def _on_csi_scores(self, scores: dict) -> None:
        """Handle fresh CSI scores from CsiWorker — run engine and emit alerts."""
        # Re-read sound settings in case they changed in Settings dialog
        cfg = load_settings()
        self._csi_sound_file = cfg.get("csi_sound_file", "")
        self._csi_sound_enabled = cfg.get("csi_sound_enabled", True)
        self._qm4_engine.cooldown_seconds = cfg.get("csi_cooldown_minutes", 5) * 60

        # Debug: log HTF sums to file for troubleshooting
        try:
            debug_lines = []
            from datetime import datetime as _dt
            now_s = _dt.now().strftime("%H:%M:%S")
            for ccy in ["USD", "CAD", "NZD", "AUD"]:
                d1 = scores.get((ccy, "D1"), -1)
                w1 = scores.get((ccy, "W1"), -1)
                mn = scores.get((ccy, "MN"), -1)
                s = d1 + w1 + mn
                debug_lines.append(f"{now_s} {ccy}: D1={d1:.1f} W1={w1:.1f} MN={mn:.1f} sum={s:.1f}")
            _DATA_DIR.mkdir(parents=True, exist_ok=True)
            with open(_DATA_DIR / "csi_debug.log", "a", encoding="utf-8") as f:
                f.write("\n".join(debug_lines) + "\n")
        except Exception:
            pass

        try:
            alerts = self._qm4_engine.check(scores)
        except Exception:
            logger.exception("CSI engine check error")
            return

        for alert in alerts:
            self._csi_log.append(alert)
            self._add_csi_alert_to_panel(alert)
            # Both main "Enable sound alerts" AND CSI "Enable CSI alert sound" must be on
            if self._alert_mgr.sound_enabled and self._csi_sound_enabled:
                from takumi_trader.core.alerts import play_sound, send_toast_notification, speak
                if self._csi_sound_file:
                    play_sound(self._csi_sound_file)
                # Voice announcement
                from takumi_trader.core.qm4_alerts import QM4PairAlert
                if isinstance(alert, QM4PairAlert):
                    speak(f"{alert.pair} {alert.alert_type} {alert.direction}")
                else:
                    speak(f"{alert.currency} {alert.alert_type} {alert.direction}")
                from takumi_trader.core.qm4_alerts import QM4PairAlert, QM4Alert
                if isinstance(alert, QM4PairAlert):
                    send_toast_notification(
                        f"CSI [{alert.alert_type} PAIR] {alert.pair} {alert.direction}",
                        f"spread={alert.spread:.1f} "
                        f"base={alert.base_alignment}/6 quote={alert.quote_alignment}/6",
                    )
                else:
                    scores_text = " | ".join(
                        f"{tf}={v:.1f}" for tf, v in alert.tf_scores.items()
                    )
                    send_toast_notification(
                        f"CSI [{alert.alert_type}] {alert.currency} {alert.direction}",
                        f"{scores_text}  align={alert.alignment}/6 "
                        f"depth={alert.depth_pct:.0f}%"
                        + (f"  → {alert.best_pair}" if alert.best_pair else ""),
                    )

            # ── System D: QM4 CSI-based paper trades ──
            # Only trade MTF, MTFC, CUM, PAIR alert types.
            # Kill switch (2026-05-14): if _QM4_TRADING_ENABLED is False,
            # skip the entire trade-opening branch. Alerts are still
            # logged + shown in the alerts panel (operator visibility);
            # only new trade openings are suppressed. Existing QM4
            # positions continue to close normally via update_cycle.
            if not _QM4_TRADING_ENABLED:
                continue
            _QM4_TRADE_TYPES = {"MTF", "MTFC", "CUM"}
            from takumi_trader.core.qm4_alerts import QM4PairAlert as _QPA, QM4Alert as _QA

            # Helper: check news filter for QM4 trades
            def _qm4_news_ok(pair: str) -> bool:
                if self._news_filter.loaded and self._news_filter.is_blackout(pair, time.time()):
                    logger.info("[QM4] News BLOCK: %s — RED news blackout", pair)
                    return False
                return True

            # Helper: check structural filter for QM4 trades
            def _qm4_structural_ok(pair: str, direction: str, entry_price: float) -> bool:
                # Respect the structural_enabled toggle
                if not self._filter_engine.settings.structural_enabled:
                    return True  # filter disabled by user
                if not hasattr(self, '_last_result') or not self._last_result:
                    return False  # no data = no trade
                _struct = getattr(self._last_result, 'structural_levels', {}).get(pair)
                if not _struct or entry_price <= 0:
                    return False  # no structural data = block trade
                from takumi_trader.core.pair_algo_settings import get_pair_settings
                _ps = get_pair_settings(pair)
                _h1_atr = getattr(self._last_result, 'h1_atr', {}).get(pair, 0.0)
                _pip = 0.01 if "JPY" in pair else 0.0001
                _tp = round(_ps.get("tp_atr", 1.0) * _h1_atr / _pip, 1) if _ps and _h1_atr > 0 else 0.0
                _sr = self._filter_engine._check_structural(pair, direction, entry_price, _tp, _struct)
                if not _sr.passed:
                    logger.info("[QM4] Structural BLOCK: %s %s — %s", direction, pair, _sr.reason)
                    return False
                return True

            _qm4_session = get_current_session()

            # QM4-specific NO_TRADE window: 05:00–07:57 JST
            _jst_now_qm4 = datetime.now(_jst())
            _qm4_hm = _jst_now_qm4.hour * 60 + _jst_now_qm4.minute
            _QM4_NO_TRADE_START = 5 * 60       # 05:00 JST
            _QM4_NO_TRADE_END   = 7 * 60 + 57  # 07:57 JST
            _qm4_trade_allowed = not (_QM4_NO_TRADE_START <= _qm4_hm <= _QM4_NO_TRADE_END)

            # ── QM4 QUALITY GATE FILTER ──
            # ADR consumption must be in healthy range: skip dead markets
            # (<50% = low volatility) and exhausted moves (>130% = extended)
            _QM4_ADR_MIN = 50.0
            _QM4_ADR_MAX = 130.0
            # Note: Hour blacklist removed per user request — only early-morning
            # NO_TRADE (05:00-07:57) remains. All pairs allowed.

            # Gate a candidate trade through the quality filter
            def _qm4_quality_ok(pair: str, direction: str) -> tuple[bool, str]:
                """Return (passed, reason_if_blocked)."""
                _adr = getattr(self._last_result, 'session_range_pct', {}).get(pair, 0.0) if hasattr(self, '_last_result') and self._last_result else 0.0
                if _adr < _QM4_ADR_MIN or _adr > _QM4_ADR_MAX:
                    return False, f"adr-out-of-range({_adr:.0f}%)"
                return True, ""

            if not _qm4_trade_allowed:
                pass  # silently skip — QM4 NO_TRADE window

            elif isinstance(alert, _QPA) and alert.alert_type in _QM4_TRADE_TYPES:
                # PAIR alert → trade the pair directly
                pair = alert.pair
                direction = alert.direction
                _qm4_key = f"{pair}_{direction}_{_qm4_session}"
                entry_price = self._latest_close_prices.get(pair, 0.0)
                _q_ok, _q_reason = _qm4_quality_ok(pair, direction)
                if _qm4_key in self._session_keys_qm4:
                    pass  # already traded this pair+direction in this session
                elif not _qm4_news_ok(pair):
                    pass  # blocked by RED news
                elif not math.isfinite(entry_price) or entry_price <= 0:
                    pass  # invalid price
                elif not _qm4_structural_ok(pair, direction, entry_price):
                    pass  # blocked by structural filter
                elif not _q_ok:
                    logger.info("[QM4] Quality gate BLOCK: %s %s — %s", direction, pair, _q_reason)
                else:
                    pt_qm4 = self._paper_trader_qm4.open_paper_trade(
                        pair=pair, direction=direction, entry_price=entry_price,
                        composite_scores=self._last_composite_scores,
                        conviction=0, session=get_current_session(),
                        h1_atr=getattr(self._last_result, 'h1_atr', {}).get(pair, 0.0) if hasattr(self, '_last_result') and self._last_result else 0.0,
                        entry_type="sv2_qm4",
                        adr_consumed_pct=getattr(self._last_result, 'session_range_pct', {}).get(pair, 0.0) if hasattr(self, '_last_result') and self._last_result else 0.0,
                    )
                    if pt_qm4:
                        # Stamp entry signals using last known result
                        if hasattr(self, '_last_result') and self._last_result:
                            _qm4_result = self._last_result
                            _pip = 0.01 if "JPY" in pair else 0.0001
                            _base, _quote = pair[:3], pair[3:]
                            # Stoch scores for ALL 7 TFs that QM4 cares about
                            for _tf, _ab, _aq in [
                                ("M5","entry_m5_base","entry_m5_quote"),
                                ("M15","entry_m15_base","entry_m15_quote"),
                                ("H1","entry_h1_base","entry_h1_quote"),
                                ("H4","entry_h4_base","entry_h4_quote"),
                                ("D1","entry_d1_base","entry_d1_quote"),
                                ("W1","entry_w1_base","entry_w1_quote"),
                                ("MN","entry_mn_base","entry_mn_quote"),
                            ]:
                                _tfs = getattr(_qm4_result, 'stoch_scores', {}).get(_tf, {})
                                setattr(pt_qm4, _ab, round(_tfs.get(_base, 0.0), 1))
                                setattr(pt_qm4, _aq, round(_tfs.get(_quote, 0.0), 1))
                            # Alignment count: # of M15..MN TFs at extreme (≤2 weak / ≥8 strong)
                            # Direction-aware: for BUY look at base strong + quote weak
                            _align = 0
                            for _tf in ("M15","H1","H4","D1","W1","MN"):
                                _tfs = getattr(_qm4_result, 'stoch_scores', {}).get(_tf, {})
                                _bs = _tfs.get(_base, 5.0)
                                _qs = _tfs.get(_quote, 5.0)
                                if direction == "BUY":
                                    if _bs >= 8.0 and _qs <= 2.0:
                                        _align += 1
                                else:
                                    if _bs <= 2.0 and _qs >= 8.0:
                                        _align += 1
                            pt_qm4.entry_alignment_count = _align
                            _bc = self._last_composite_scores.get(_base, 5.0)
                            _qc = self._last_composite_scores.get(_quote, 5.0)
                            pt_qm4.entry_div_spread = round(_bc - _qc, 1)
                            _h1a = getattr(_qm4_result, 'h1_atr', {}).get(pair, 0.0)
                            pt_qm4.entry_h1_atr_pips = round(_h1a / _pip, 1) if _h1a > 0 else 0.0
                            pt_qm4.entry_tier = "QM4"
                            pt_qm4.entry_structural = "OK"
                        self._session_keys_qm4.add(_qm4_key)
                        # Store alert type on the trade for display
                        pt_qm4.entry_type = "sv2_qm4"
                        pt_qm4.qm4_alert_type = f"PAIR/{alert.alert_type}"
                        self._stamp_qm4_extras(pt_qm4, pair, direction)
                        self._sync_qm4_journal(pt_qm4)
                        # QM4: paper only (no cTrader)
                        now_q = datetime.now(_jst()).strftime("%H:%M:%S")
                        dir_c = "#1b8a2a" if direction == "BUY" else "#c62828"
                        self._alert_history.appendleft(
                            f'<span style="font-size:9pt; color:#666;">[{now_q}]</span> '
                            f'<span style="font-size:10pt; color:#ff6f00; font-weight:bold;">'
                            f'\U0001f3af QM4 OPEN</span> '
                            f'<span style="font-size:10pt; color:{dir_c}; font-weight:bold;">'
                            f'{direction} {pair}</span> '
                            f'<span style="font-size:9pt; color:#888;">'
                            f'[{alert.alert_type} PAIR]</span>'
                        )

            elif isinstance(alert, _QA) and alert.alert_type in _QM4_TRADE_TYPES:
                # Currency alert → try candidate pairs in order (best first)
                # If best_pair is blocked by filters, fall back to next best
                _candidates = list(getattr(alert, 'candidate_pairs', []) or [])
                if alert.best_pair and alert.best_pair not in _candidates:
                    _candidates.insert(0, alert.best_pair)

                # Find the first candidate that passes all filters
                pair = ""
                direction = ""
                entry_price = 0.0
                for _cand in _candidates:
                    _base, _quote = _cand[:3], _cand[3:]
                    if alert.direction == "STRONG":
                        _dir = "BUY" if alert.currency == _base else "SELL"
                    else:  # WEAK
                        _dir = "SELL" if alert.currency == _base else "BUY"
                    # Session key — no re-entry on same pair+direction in same session
                    _qm4_cand_key = f"{_cand}_{_dir}_{_qm4_session}"
                    if _qm4_cand_key in self._session_keys_qm4:
                        continue
                    _px = self._latest_close_prices.get(_cand, 0.0)
                    if not math.isfinite(_px) or _px <= 0:
                        continue
                    if not _qm4_news_ok(_cand):
                        continue
                    if not _qm4_structural_ok(_cand, _dir, _px):
                        continue
                    # Quality gate: hour/pair blacklist, ADR range, alignment
                    _q_ok_c, _q_reason_c = _qm4_quality_ok(_cand, _dir)
                    if not _q_ok_c:
                        logger.info("[QM4] Quality gate BLOCK: %s %s — %s", _dir, _cand, _q_reason_c)
                        continue
                    # Skip if we already have an active trade on this pair in QM4
                    if self._trade_tracker_qm4.has_trade(_cand):
                        continue
                    pair = _cand
                    direction = _dir
                    entry_price = _px
                    break

                if pair:
                        pt_qm4 = self._paper_trader_qm4.open_paper_trade(
                            pair=pair, direction=direction, entry_price=entry_price,
                            composite_scores=self._last_composite_scores,
                            conviction=0, session=get_current_session(),
                            h1_atr=getattr(self._last_result, 'h1_atr', {}).get(pair, 0.0) if hasattr(self, '_last_result') and self._last_result else 0.0,
                            entry_type="sv2_qm4",
                            adr_consumed_pct=getattr(self._last_result, 'session_range_pct', {}).get(pair, 0.0) if hasattr(self, '_last_result') and self._last_result else 0.0,
                        )
                        if pt_qm4:
                            # Stamp entry signals
                            if hasattr(self, '_last_result') and self._last_result:
                                _qr = self._last_result
                                _pip = 0.01 if "JPY" in pair else 0.0001
                                _b, _q = pair[:3], pair[3:]
                                for _tf, _ab, _aq in [
                                    ("M5","entry_m5_base","entry_m5_quote"),
                                    ("M15","entry_m15_base","entry_m15_quote"),
                                    ("H1","entry_h1_base","entry_h1_quote"),
                                    ("H4","entry_h4_base","entry_h4_quote"),
                                    ("D1","entry_d1_base","entry_d1_quote"),
                                    ("W1","entry_w1_base","entry_w1_quote"),
                                    ("MN","entry_mn_base","entry_mn_quote"),
                                ]:
                                    _tfs = getattr(_qr, 'stoch_scores', {}).get(_tf, {})
                                    setattr(pt_qm4, _ab, round(_tfs.get(_b, 0.0), 1))
                                    setattr(pt_qm4, _aq, round(_tfs.get(_q, 0.0), 1))
                                # Alignment count
                                _align = 0
                                for _tf in ("M15","H1","H4","D1","W1","MN"):
                                    _tfs = getattr(_qr, 'stoch_scores', {}).get(_tf, {})
                                    _bs = _tfs.get(_b, 5.0)
                                    _qs = _tfs.get(_q, 5.0)
                                    if direction == "BUY":
                                        if _bs >= 8.0 and _qs <= 2.0:
                                            _align += 1
                                    else:
                                        if _bs <= 2.0 and _qs >= 8.0:
                                            _align += 1
                                pt_qm4.entry_alignment_count = _align
                                _bc = self._last_composite_scores.get(_b, 5.0)
                                _qc = self._last_composite_scores.get(_q, 5.0)
                                pt_qm4.entry_div_spread = round(_bc - _qc, 1)
                                _h1a = getattr(_qr, 'h1_atr', {}).get(pair, 0.0)
                                pt_qm4.entry_h1_atr_pips = round(_h1a / _pip, 1) if _h1a > 0 else 0.0
                                pt_qm4.entry_tier = "QM4"
                                pt_qm4.entry_structural = "OK"
                            self._session_keys_qm4.add(f"{pair}_{direction}_{_qm4_session}")
                            pt_qm4.entry_type = "sv2_qm4"
                            pt_qm4.qm4_alert_type = alert.alert_type
                            self._stamp_qm4_extras(pt_qm4, pair, direction)
                            self._sync_qm4_journal(pt_qm4)
                            # QM4: paper only (no cTrader)
                            now_q = datetime.now(_jst()).strftime("%H:%M:%S")
                            dir_c = "#1b8a2a" if direction == "BUY" else "#c62828"
                            self._alert_history.appendleft(
                                f'<span style="font-size:9pt; color:#666;">[{now_q}]</span> '
                                f'<span style="font-size:10pt; color:#ff6f00; font-weight:bold;">'
                                f'\U0001f3af QM4 OPEN</span> '
                                f'<span style="font-size:10pt; color:{dir_c}; font-weight:bold;">'
                                f'{direction} {pair}</span> '
                                f'<span style="font-size:9pt; color:#888;">'
                                f'[{alert.alert_type}] {alert.currency} {alert.direction}</span>'
                            )

    def _add_csi_alert_to_panel(self, alert) -> None:
        """Format a CSI alert as orange HTML and prepend to the TREND ALERTS panel."""
        from takumi_trader.core.qm4_alerts import QM4PairAlert, QM4Alert
        now_str = datetime.now(_jst()).strftime("%H:%M")

        # Type badge colour: MTF=blue, HTF=gold, XHTF=red-orange, PAIR=purple
        type_colors = {
            "MTF":  "#4a90d9",
            "HTF":  "#c8960a",
            "XHTF": "#d94a4a",
        }

        if isinstance(alert, QM4PairAlert):
            tc = type_colors.get(alert.alert_type, "#888")
            dir_color = "#1b8a2a" if alert.direction == "BUY" else "#c62828"
            entry = (
                f'<span style="color:{tc}; font-weight:bold;">'
                f'[CSI {alert.alert_type} PAIR]</span> '
                f'<span style="color:#666;">{now_str}</span>  '
                f'<span style="color:{dir_color}; font-weight:bold; font-size:13pt;">'
                f'\U0001f525 {alert.pair} {alert.direction}</span>'
                f'<span style="color:#888; font-size:10pt;"> '
                f'spread={alert.spread:.1f}  '
                f'base={alert.base_alignment}/6 quote={alert.quote_alignment}/6'
                f' [{alert.trigger_type}]</span>'
            )
        else:
            tc = type_colors.get(alert.alert_type, "#888")
            dir_color = "#c62828" if alert.direction == "WEAK" else "#1b8a2a"
            arrow = "\u25bc" if alert.direction == "WEAK" else "\u25b2"
            tf_parts = "  ".join(
                f"{tf}={v:.1f}" for tf, v in alert.tf_scores.items()
            )
            pair_hint = (
                f'  <span style="color:#4a6fa5;">→ {alert.best_pair}</span>'
                if alert.best_pair else ""
            )
            entry = (
                f'<span style="color:{tc}; font-weight:bold;">'
                f'[CSI {alert.alert_type}]</span> '
                f'<span style="color:#666;">{now_str}</span>  '
                f'<span style="color:{dir_color}; font-weight:bold; font-size:13pt;">'
                f'{arrow} {alert.currency} {alert.direction}</span>'
                f'<span style="color:#888; font-size:10pt;">  {tf_parts}  '
                f'align={alert.alignment}/6  depth={alert.depth_pct:.0f}%</span>'
                f'{pair_hint}'
            )

        self._alert_history.appendleft(entry)
        # Re-render alert panel
        html = "<br>".join(self._alert_history)
        self._alert_label.setText(html)
        self._alert_label.setStyleSheet(
            "color: #222222; padding: 4px 6px; background: #ffffff;"
            " a { color: #4a6fa5; text-decoration: none; font-weight: bold; }"
        )

    def _open_performance(self) -> None:
        """Open the alert performance statistics dialog (modeless, independent)."""
        from takumi_trader.ui.performance_dialog import PerformanceDialog
        active_count = self._alert_perf.get_active_count()
        bt_file = _DATA_DIR / "backtest_outcomes.json"
        # No parent → independent window, not pinned; show() → non-blocking
        _SS_JOURNAL = _DATA_DIR / "paper_trades_ss.json"
        _ATR_JOURNAL = _DATA_DIR / "paper_trades_atr.json"
        _QM4_JOURNAL = _DATA_DIR / "paper_trades_qm4.json"
        _A_TUNED_JOURNAL = _DATA_DIR / "paper_trades_a_tuned.json"
        _B_TUNED_JOURNAL = _DATA_DIR / "paper_trades_b_tuned.json"
        dlg = PerformanceDialog(
            None,
            outcomes_file=_OUTCOMES_FILE,
            active_count=active_count,
            backtest_file=bt_file if bt_file.exists() else None,
            paper_trades_file=_PAPER_TRADES_FILE,
            csi_log_file=_CSI_LOG_FILE,
            paper_trader=self._paper_trader,
            ss_trades_file=_SS_JOURNAL,
            ss_paper_trader=getattr(self, '_paper_trader_ss', None),
            atr_trades_file=_ATR_JOURNAL,
            atr_paper_trader=getattr(self, '_paper_trader_atr', None),
            qm4_trades_file=_QM4_JOURNAL,
            qm4_paper_trader=getattr(self, '_paper_trader_qm4', None),
            a_tuned_trades_file=_A_TUNED_JOURNAL,
            a_tuned_paper_trader=getattr(self, '_paper_trader_a_tuned', None),
            b_tuned_trades_file=_B_TUNED_JOURNAL,
            b_tuned_paper_trader=getattr(self, '_paper_trader_b_tuned', None),
            breakout_trades_file=_DATA_DIR / "paper_trades_breakout.json",
            breakout_paper_trader=getattr(self, '_paper_trader_breakout', None),
            squeeze_trades_file=_DATA_DIR / "paper_trades_squeeze.json",
            squeeze_paper_trader=getattr(self, '_paper_trader_squeeze', None),
            # Squeeze-REV (2026-04-29) — inverse-direction mirror of Squeeze
            squeeze_rev_trades_file=_DATA_DIR / "paper_trades_squeeze_rev.json",
            squeeze_rev_paper_trader=getattr(self, '_paper_trader_squeeze_rev', None),
            divergence_trades_file=_DATA_DIR / "paper_trades_divergence.json",
            divergence_paper_trader=getattr(self, '_paper_trader_divergence', None),
            dtc_combo_trades_file=_DATA_DIR / "paper_trades_dtc_combo.json",
            dtc_combo_paper_trader=getattr(self, '_paper_trader_dtc_combo', None),
            # Sv2-upgraded (2026-04-23) — live-candle engine + conv≥65 +
            # revenge cooldown + BE-stop. Displayed next to Sv2 in the tab
            # bar for side-by-side comparison.
            sv2_upgraded_trades_file=_DATA_DIR / "paper_trades_sv2_upgraded.json",
            sv2_upgraded_paper_trader=getattr(self, '_paper_trader_sv2_upgraded', None),
            # Phase E (2026-05-06) — shadow stats panel embedded in Sv2 tab.
            # Sourced from the same paths the worker uses; sim_worker may be
            # None if it failed to construct in main_window's startup block.
            shadow_journal_path=_DATA_DIR / "shadow_trades_Sv2.json",
            shadow_calibration_path=_DATA_DIR / "shadow_calibration_Sv2.json",
            shadow_sim_worker=getattr(self, "_shadow_sim_worker", None),
        )
        dlg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        # Center on primary monitor
        from PyQt6.QtGui import QGuiApplication as _QGA
        primary = _QGA.primaryScreen()
        if primary:
            geo = primary.availableGeometry()
            dlg.move(
                geo.x() + (geo.width() - dlg.width()) // 2,
                geo.y() + (geo.height() - dlg.height()) // 2,
            )
        dlg.show()
        # Keep reference so it isn't garbage-collected
        self._perf_dialog = dlg

    # ── cTrader signal handlers ─────────────────────────────────────

    def _ct_open_order(self, pair: str, direction: str, system_tag: str, max_pos: int = 28) -> None:
        """Send one cTrader order per pair+system — max 4 per pair (one per system).

        If cTrader is disconnected, the order is queued and retried automatically
        on subsequent cycles (up to 60 seconds). Dedup: the same pair+system
        key will never be queued or sent twice.

        For system_tag == "dtc_combo", the TP is scaled to match the DTC
        paper trade's tp_ratio_override (e.g., 0.75 × SL). This keeps live
        execution and paper stats in sync.
        """
        if not self._ctrader_config.get("ctrader_enabled") or not self._ctrader_config.get("ctrader_auto_open"):
            return
        if not hasattr(self, "_ct_open_positions"):
            self._ct_open_positions: set[str] = set()
        if not hasattr(self, "_ct_pending_orders"):
            self._ct_pending_orders: list[tuple[str, str, str, int, float]] = []

        key = f"{pair}_{system_tag}"
        # Already open OR already queued → skip
        if key in self._ct_open_positions:
            return
        if any(p[0] == pair and p[2] == system_tag for p in self._ct_pending_orders):
            return

        # Mark as open BEFORE doing anything else to prevent duplicates
        # across cycles. Cleared on rejection, position close, or queue expiry.
        self._ct_open_positions.add(key)

        if not self._ctrader_bridge or not self._ctrader_bridge.is_connected:
            # Queue for retry when cTrader reconnects (max 60s)
            self._ct_pending_orders.append((pair, direction, system_tag, max_pos, time.time()))
            logger.warning("cTrader disconnected — queued order: %s %s (system=%s)", direction, pair, system_tag)
            return

        # Pull DTC TP-ratio override from config if this is a DTC trade
        tp_override = None
        if system_tag == "dtc_combo":
            tp_override = self._dtc_combo_cfg.get("tp_ratio_override")

        lot_size, sl_price, tp_price, sl_pips, tp_pips = self._calc_ctrader_order_params(
            pair, direction, tp_ratio_override=tp_override,
        )
        self._ctrader_bridge.open_order(pair, direction, lot_size, sl_pips=sl_pips, tp_pips=tp_pips)
        logger.info("cTrader order sent: %s %s %.2f lots SL=%.1fp TP=%.1fp (system=%s)",
                    direction, pair, lot_size, sl_pips, tp_pips, system_tag)

    def _ct_flush_pending_orders(self) -> None:
        """Retry any queued orders that were blocked by cTrader disconnect."""
        if not hasattr(self, "_ct_pending_orders") or not self._ct_pending_orders:
            return
        now = time.time()

        # Prune expired orders regardless of connection state
        expired = [o for o in self._ct_pending_orders if now - o[4] > 60]
        for pair, direction, tag, _, _ in expired:
            key = f"{pair}_{tag}"
            self._ct_open_positions.discard(key)  # allow re-try on new signal
            logger.warning("cTrader pending order expired (>60s): %s %s (system=%s)", direction, pair, tag)
        self._ct_pending_orders = [o for o in self._ct_pending_orders if now - o[4] <= 60]

        if not self._ctrader_bridge or not self._ctrader_bridge.is_connected:
            return  # still disconnected, nothing to flush

        # Connected — flush all pending orders
        pending = self._ct_pending_orders
        self._ct_pending_orders = []
        for entry in pending:
            pair, direction, tag, max_pos, queued_at = entry
            # Re-check connection before EACH send — backoff could trigger mid-flush
            if not self._ctrader_bridge.is_connected:
                # Put remaining items back in queue with original timestamps
                self._ct_pending_orders.append(entry)
                logger.warning("cTrader disconnected mid-flush — re-queued %s (system=%s)", pair, tag)
                continue
            # Check paper trade is still open before sending
            tracker = {
                "sv2": self._trade_tracker,
                "ss": self._trade_tracker_ss,
                "atr": self._trade_tracker_atr,
                "qm4": self._trade_tracker_qm4,
                "dtc_combo": self._trade_tracker_dtc_combo,
            }.get(tag)
            if tracker and not tracker.has_trade(pair):
                key = f"{pair}_{tag}"
                self._ct_open_positions.discard(key)
                logger.info("cTrader pending order dropped — paper trade already closed: %s (system=%s)", pair, tag)
                continue
            # Pull DTC TP-ratio override from config if this is a DTC trade
            tp_override = None
            if tag == "dtc_combo":
                tp_override = self._dtc_combo_cfg.get("tp_ratio_override")
            # Send directly — _ct_open_positions was already marked when queued
            lot_size, sl_price, tp_price, sl_pips, tp_pips = self._calc_ctrader_order_params(
                pair, direction, tp_ratio_override=tp_override,
            )
            self._ctrader_bridge.open_order(pair, direction, lot_size, sl_pips=sl_pips, tp_pips=tp_pips)
            logger.info("cTrader order sent (from queue): %s %s %.2f lots SL=%.1fp TP=%.1fp (system=%s)",
                        direction, pair, lot_size, sl_pips, tp_pips, tag)

    def _calc_ctrader_order_params(
        self, pair: str, direction: str,
        tp_ratio_override: float | None = None,
    ) -> tuple[float, float, float, float, float]:
        """Calculate lot size, SL price, and TP price for a cTrader order.

        If tp_ratio_override is provided and > 0, TP is set to
        sl_pips × tp_ratio_override (used by DTC-combo to mirror its
        0.75× TP on the live order, matching the paper journal).

        Returns (lot_size, sl_price, tp_price, sl_pips, tp_pips).
        """
        try:
            from takumi_trader.core.pair_algo_settings import get_pair_settings

            balance = self._ctrader_config.get("ctrader_balance", 100000.0)
            risk_pct = self._ctrader_config.get("ctrader_risk_pct", 0.1) / 100.0
            risk_amount = balance * risk_pct

            ps = get_pair_settings(pair)
            pip = 0.01 if "JPY" in pair else 0.0001

            # Get SL/TP dynamically from ATR multipliers × live H1 ATR
            # (same calculation as paper_trader.open_paper_trade)
            h1_atr = 0.0
            if hasattr(self, '_last_result') and self._last_result:
                h1_atr = getattr(self._last_result, 'h1_atr', {}).get(pair, 0.0)
            # Fallback: check result cache from last data cycle
            if h1_atr <= 0:
                h1_atr = self._latest_close_prices.get(f"_h1_atr_{pair}", 0.0)

            if ps and h1_atr > 0:
                sl_pips = round(ps.get("sl_atr", 0.5) * h1_atr / pip, 1)
                tp_pips = round(ps.get("tp_atr", 1.0) * h1_atr / pip, 1)
            elif ps:
                sl_pips = ps.get("sl_pips", 10.0)
                tp_pips = ps.get("tp_pips", 10.0)
            else:
                sl_pips = 10.0
                tp_pips = 10.0

            if sl_pips <= 0:
                sl_pips = 10.0
            if tp_pips <= 0:
                tp_pips = sl_pips

            # DTC-combo (and any other path that supplies a ratio override)
            # replaces the pair-settings TP with a flat ratio of SL. This
            # must match the paper trade's TP so live execution doesn't
            # drift from recorded stats.
            if tp_ratio_override is not None and tp_ratio_override > 0:
                tp_pips = round(sl_pips * tp_ratio_override, 1)

            # Lot size calculation
            is_jpy_pair = "JPY" in pair
            if is_jpy_pair:
                pip_value_per_lot = 1000.0
            else:
                usdjpy = self._latest_close_prices.get("USDJPY", 150.0)
                pip_value_per_lot = 10.0 * usdjpy

            lot_size = risk_amount / (sl_pips * pip_value_per_lot)
            lot_size = max(0.01, round(lot_size, 2))

            # SL/TP price levels — round to 5 digits (3 for JPY pairs)
            entry_price = self._latest_close_prices.get(pair, 0.0)
            _digits = 3 if "JPY" in pair else 5
            if entry_price > 0:
                if direction == "BUY":
                    sl_price = round(entry_price - sl_pips * pip, _digits)
                    tp_price = round(entry_price + tp_pips * pip, _digits)
                else:
                    sl_price = round(entry_price + sl_pips * pip, _digits)
                    tp_price = round(entry_price - tp_pips * pip, _digits)
            else:
                sl_price = 0.0
                tp_price = 0.0

            logger.info(
                "cTrader order: %s %s lot=%.2f SL=%.5f (%.1fp) TP=%.5f (%.1fp)",
                direction, pair, lot_size, sl_price, sl_pips, tp_price, tp_pips,
            )
            return lot_size, sl_price, tp_price, sl_pips, tp_pips
        except Exception as e:
            logger.warning("Order params calc failed for %s: %s", pair, e)
            return 0.01, 0.0, 0.0, 10.0, 10.0

    def _on_ctrader_status(self, is_connected: bool, msg: str) -> None:
        """Handle cTrader connection status changes + popup on real disconnects."""
        if is_connected:
            self._ctrader_status_label.setText("cT: \u25cf Connected")
            self._ctrader_status_label.setStyleSheet(
                "color: #2e7d32; padding: 0 4px; font-weight: bold;"
            )
            logger.info("cTrader connected: %s", msg)
            if hasattr(self, "_health_alerts"):
                self._health_alerts.notify_recovery(
                    "cTrader", f"Reconnected: {msg}",
                    dedup_key="ctrader_disconnect",
                )
        else:
            self._ctrader_status_label.setText("cT: \u25cb Disconnected")
            self._ctrader_status_label.setStyleSheet(
                "color: #c62828; padding: 0 4px;"
            )
            logger.warning("cTrader disconnected: %s", msg)
            # Suppress popup if we're already shutting down — the cTrader
            # bridge disconnects cleanly when self._ctrader_bridge.stop() is
            # called from closeEvent, which previously triggered a benign
            # "Connection was closed cleanly" popup right as the user clicked
            # close.
            if getattr(self, "_shutting_down", False):
                return
            # Suppress noise during normal startup auth handshake AND for
            # clean teardown messages from the cTrader server itself
            # (twisted's ConnectionDone is a NORMAL FIN-handshake close, not
            # an error — server can disconnect us cleanly mid-session for
            # idle/maintenance and the bridge auto-reconnects).
            _noise = ("auth failed", "ALREADY_LOGGED_IN", "TimeoutError",
                      "Reconcile failed", "Deferred", "Connecting",
                      "ConnectionDone", "closed cleanly", "ConnectionLost")
            if any(n in (msg or "") for n in _noise):
                # Bridge auto-reconnects with backoff — logged but no popup
                return
            if hasattr(self, "_health_alerts"):
                self._health_alerts.notify(
                    "error", "cTrader",
                    f"Live mirror disconnected: {msg}\n\n"
                    "DTC paper trades continue normally.\n"
                    "cTrader live orders will be QUEUED (max 60s) and "
                    "auto-retry on reconnect.\n\n"
                    "Action:\n"
                    "  1. Check internet / openapi.ctrader.com reachable\n"
                    "  2. Verify cTrader access token is still valid\n"
                    "  3. TAKUMI auto-reconnects with backoff",
                    dedup_key="ctrader_disconnect",
                )

    def _stamp_qm4_extras(self, trade, pair: str, direction: str) -> None:
        """Stamp QM4-specific extras: pair settings, ranks, velocity, news, ATR slope.

        QM4 doesn't go through filter_engine so we recompute isolation ranks/gaps
        from composite scores directly. Velocity is pulled from result.velocity_data
        if available.
        """
        if trade is None:
            return
        try:
            base, quote = pair[:3], pair[3:]
            # Identify strong/weak currency by direction (BUY = base strong)
            if direction == "BUY":
                trade.entry_strong_ccy, trade.entry_weak_ccy = base, quote
            else:
                trade.entry_strong_ccy, trade.entry_weak_ccy = quote, base

            # Pair SL/TP ATR multipliers
            try:
                from takumi_trader.core.pair_algo_settings import get_pair_settings
                _ps = get_pair_settings(pair) or {}
                trade.entry_sl_atr_mult = float(_ps.get("sl_atr", 0.0) or 0.0)
                trade.entry_tp_atr_mult = float(_ps.get("tp_atr", 0.0) or 0.0)
            except Exception:
                pass

            # Isolation ranks/gaps from composite scores
            try:
                _cs = self._last_composite_scores or {}
                if _cs:
                    _sorted = sorted(_cs.items(), key=lambda x: x[1], reverse=True)
                    _ranking = {ccy: i + 1 for i, (ccy, _) in enumerate(_sorted)}
                    trade.entry_strong_rank = int(_ranking.get(trade.entry_strong_ccy, 0))
                    trade.entry_weak_rank = int(_ranking.get(trade.entry_weak_ccy, 0))
                    if len(_sorted) >= 2:
                        trade.entry_strong_top_gap = round(_sorted[0][1] - _sorted[1][1], 1)
                        trade.entry_weak_bottom_gap = round(_sorted[-2][1] - _sorted[-1][1], 1)
            except Exception:
                pass

            # Velocity (from result.velocity_data if present)
            try:
                _r = getattr(self, "_last_result", None)
                _vd = getattr(_r, "velocity_data", None) if _r else None
                if _vd:
                    _sv = _vd.get(trade.entry_strong_ccy, (0.0, False))
                    _wv = _vd.get(trade.entry_weak_ccy, (0.0, False))
                    trade.entry_strong_velocity = round(float(_sv[0]) if isinstance(_sv, tuple) else 0.0, 3)
                    trade.entry_weak_velocity = round(float(_wv[0]) if isinstance(_wv, tuple) else 0.0, 3)
            except Exception:
                pass

            # M5 TR slope
            try:
                import numpy as _np
                _tr_hist = getattr(self, "_tr_history", {}).get(pair)
                if _tr_hist and len(_tr_hist) >= 6:
                    _trl = list(_tr_hist)
                    _r_avg = float(_np.mean(_trl[-3:]))
                    _o_avg = float(_np.mean(_trl[-6:-3]))
                    if _o_avg > 0:
                        trade.entry_m5_tr_slope_ratio = round(_r_avg / _o_avg, 3)
            except Exception:
                pass

            # News timing
            try:
                if self._news_filter.loaded:
                    _now_ts = time.time()
                    _events = getattr(self._news_filter, "_events", []) or []
                    _candidates = [
                        float(e["time"]) for e in _events
                        if e.get("time", 0) <= _now_ts
                        and e.get("currency") in (base, quote)
                    ]
                    if _candidates:
                        trade.entry_minutes_since_news = round(
                            (_now_ts - max(_candidates)) / 60.0, 1
                        )
            except Exception:
                pass

            # ══════════════════════════════════════════════════════════════
            # BUG B FIX (2026-04-20): previously QM4 skipped all the market-
            # context fields that _stamp_entry_signals populates for A-E.
            # This block replicates the conv-independent parts of that stamp
            # logic so QM4 trade records have the same analytics coverage
            # as the stoch_v2-family systems. conv-dependent fields (tier,
            # conv_trend/velocity/isolation/structural) stay as QM4 sets them.
            # ══════════════════════════════════════════════════════════════
            _r = getattr(self, "_last_result", None)
            _entry_px = getattr(trade, "entry_price", 0.0) or 0.0
            _pip = 0.01 if "JPY" in pair else 0.0001
            import numpy as _np_qm

            # ── Tick volume ratio (current M1 vol / 15-bar avg) ──
            try:
                _tv_hist = self._m1_tick_volume_history.get(pair)
                _cur_tv = _r.tick_volumes.get(pair, 0) if _r else 0
                if _tv_hist and len(_tv_hist) >= 3 and _cur_tv > 0:
                    _avg_tv = float(_np_qm.mean(list(_tv_hist)))
                    if _avg_tv > 0:
                        trade.entry_tick_volume_ratio = round(_cur_tv / _avg_tv, 2)
            except Exception:
                pass

            # ── Momentum buildup seconds ──
            try:
                _qt = self._pair_first_qualify_time.get(pair)
                if _qt:
                    trade.entry_momentum_buildup_sec = int(time.time() - _qt)
            except Exception:
                pass

            # ── Distance to PREVIOUS-day/week/month high/low (signed pips) ──
            try:
                if _r and _entry_px > 0:
                    _struct = getattr(_r, "structural_levels", {}).get(pair, {}) or {}
                    for _fld, _key in [
                        ("entry_dist_day_high_pips", "prev_day_high"),
                        ("entry_dist_day_low_pips", "prev_day_low"),
                        ("entry_dist_week_high_pips", "prev_week_high"),
                        ("entry_dist_week_low_pips", "prev_week_low"),
                        ("entry_dist_month_high_pips", "prev_month_high"),
                        ("entry_dist_month_low_pips", "prev_month_low"),
                    ]:
                        _lvl = _struct.get(_key, 0.0)
                        if _lvl > 0:
                            setattr(trade, _fld, round((_lvl - _entry_px) / _pip, 1))
            except Exception:
                pass

            # ── Distance to nearest .00 / .000 round numbers ──
            try:
                if _entry_px > 0:
                    _step_100 = 100 * _pip
                    _step_1000 = 1000 * _pip
                    _nearest_100 = round(_entry_px / _step_100) * _step_100
                    _nearest_1000 = round(_entry_px / _step_1000) * _step_1000
                    trade.entry_dist_00_pips = round((_nearest_100 - _entry_px) / _pip, 1)
                    trade.entry_dist_000_pips = round((_nearest_1000 - _entry_px) / _pip, 1)
            except Exception:
                pass

            # ── Session minutes + day of week ──
            try:
                from takumi_trader.core.session_manager import minutes_since_session_start as _mins_sess
                trade.entry_session_minutes_in = _mins_sess(trade.entry_time)
                trade.entry_day_of_week = datetime.fromtimestamp(trade.entry_time, tz=_jst()).weekday()
            except Exception:
                pass

            # ── Previous trade result on same pair (QM4 journal) ──
            try:
                _pt_qm4 = self._paper_trader_qm4
                if _pt_qm4 and _pt_qm4._journal:
                    for _rec in reversed(_pt_qm4._journal):
                        if _rec.pair == pair and _rec.close_reason:
                            trade.entry_prev_trade_result = "win" if _rec.is_win else "loss"
                            break
            except Exception:
                pass

            # ── Concurrent trades across all 10 systems ──
            try:
                trade.entry_concurrent_trades = (
                    len(self._trade_tracker.active_trades) +
                    len(self._trade_tracker_ss.active_trades) +
                    len(self._trade_tracker_atr.active_trades) +
                    len(self._trade_tracker_qm4.active_trades) +
                    len(self._trade_tracker_a_tuned.active_trades) +
                    len(self._trade_tracker_b_tuned.active_trades) +
                    len(self._trade_tracker_breakout.active_trades) +
                    len(self._trade_tracker_squeeze.active_trades) +
                    len(self._trade_tracker_squeeze_rev.active_trades) +
                    len(self._trade_tracker_divergence.active_trades) +
                    len(self._trade_tracker_dtc_combo.active_trades)
                )
            except Exception:
                pass

            # ── M1 candle body % + direction ──
            try:
                if _r:
                    _h = _r.high_prices.get(pair, 0.0)
                    _l = _r.low_prices.get(pair, 0.0)
                    _c = _r.close_prices.get(pair, 0.0)
                    if _h > 0 and _l > 0 and _h > _l:
                        _range = _h - _l
                        _mid = (_h + _l) / 2
                        _body = abs(_c - _mid) * 2
                        trade.entry_m1_body_pct = round(min(100.0, _body / _range * 100), 1)
                        if _c > _mid + _range * 0.1:
                            trade.entry_m1_direction = "bull"
                        elif _c < _mid - _range * 0.1:
                            trade.entry_m1_direction = "bear"
                        else:
                            trade.entry_m1_direction = "doji"
            except Exception:
                pass

            # ── ATR ratio (current H1 ATR / last 20-bar avg) ──
            try:
                _atr_hist = self._h1_atr_history.get(pair)
                _h1_atr_cur = _r.h1_atr.get(pair, 0.0) if _r else 0.0
                if _atr_hist and len(_atr_hist) >= 3 and _h1_atr_cur > 0:
                    _avg_atr = float(_np_qm.mean(list(_atr_hist)))
                    if _avg_atr > 0:
                        trade.entry_atr_ratio = round(_h1_atr_cur / _avg_atr, 2)
            except Exception:
                pass

            # ═══ 14 NEW 2026-04-20 momentum/trend-start fields ═══
            _is_buy = (direction == "BUY")

            # (1) M1 consecutive bars aligned
            try:
                _hd = self._m1_direction_history.get(pair)
                if _hd:
                    _want = 1 if _is_buy else -1
                    _count = 0
                    for _d in reversed(_hd):
                        if _d == _want:
                            _count += 1
                        else:
                            break
                    if _count == 0 and _hd and _hd[-1] == -_want:
                        _against = 0
                        for _d in reversed(_hd):
                            if _d == -_want:
                                _against += 1
                            else:
                                break
                        _count = -_against
                    trade.entry_m1_consec_aligned = _count
            except Exception:
                pass

            # (2) Composite spread velocity (90s slope, direction-signed)
            try:
                _hs = self._composite_spread_history.get(pair)
                if _hs and len(_hs) >= 3:
                    _now = time.time()
                    _win = [(ts, v) for ts, v in _hs if _now - ts <= 90.0]
                    if len(_win) >= 2:
                        _t0, _v0 = _win[0]
                        _t1, _v1 = _win[-1]
                        _dt_min = (_t1 - _t0) / 60.0
                        if _dt_min > 0:
                            _slope = (_v1 - _v0) / _dt_min
                            trade.entry_composite_vel_90s = round(
                                _slope if _is_buy else -_slope, 3
                            )
            except Exception:
                pass

            # (3) M5 HH / HL
            try:
                _m5h = self._m5_bar_history.get(pair)
                if _m5h and len(_m5h) >= 3:
                    _last3 = list(_m5h)[-3:]
                    _highs = [b[0] for b in _last3]
                    _lows = [b[1] for b in _last3]
                    if _is_buy:
                        trade.entry_m5_higher_highs = (_highs[0] < _highs[1] < _highs[2])
                        trade.entry_m5_higher_lows  = (_lows[0]  < _lows[1]  < _lows[2])
                    else:
                        trade.entry_m5_higher_highs = (_highs[0] > _highs[1] > _highs[2])
                        trade.entry_m5_higher_lows  = (_lows[0]  > _lows[1]  > _lows[2])
            except Exception:
                pass

            # (4) VWAP distance in pips, signed by direction
            try:
                _vs = self._session_vwap.get(pair)
                if _vs and _vs.get("sum_vol", 0) > 0:
                    _vwap = _vs["sum_tpv"] / _vs["sum_vol"]
                    _cur = _r.close_prices.get(pair, 0.0) if _r else 0.0
                    if _cur > 0:
                        _raw = _cur - _vwap
                        trade.entry_vwap_dist_pips = round(
                            (_raw if _is_buy else -_raw) / _pip, 1
                        )
            except Exception:
                pass

            # (5) ADX H1 (Wilder 14)
            try:
                _hb = self._h1_bar_history.get(pair)
                if _hb and len(_hb) >= 29:
                    _bars = list(_hb)
                    _highs = _np_qm.array([b[0] for b in _bars], dtype=float)
                    _lows = _np_qm.array([b[1] for b in _bars], dtype=float)
                    _closes = _np_qm.array([b[2] for b in _bars], dtype=float)
                    _period = 14
                    _n = len(_bars)
                    _tr = _np_qm.zeros(_n - 1)
                    _pdm = _np_qm.zeros(_n - 1)
                    _mdm = _np_qm.zeros(_n - 1)
                    for _i in range(1, _n):
                        _tr[_i - 1] = max(
                            _highs[_i] - _lows[_i],
                            abs(_highs[_i] - _closes[_i - 1]),
                            abs(_lows[_i] - _closes[_i - 1]),
                        )
                        _up = _highs[_i] - _highs[_i - 1]
                        _dn = _lows[_i - 1] - _lows[_i]
                        _pdm[_i - 1] = _up if (_up > _dn and _up > 0) else 0.0
                        _mdm[_i - 1] = _dn if (_dn > _up and _dn > 0) else 0.0
                    def _wilder_avg(vals, p):
                        out = [sum(vals[:p]) / p]
                        for i in range(p, len(vals)):
                            out.append(out[-1] - out[-1] / p + vals[i] / p)
                        return out
                    _tr_s = _wilder_avg(_tr.tolist(), _period)
                    _pdm_s = _wilder_avg(_pdm.tolist(), _period)
                    _mdm_s = _wilder_avg(_mdm.tolist(), _period)
                    _pdi = [100 * _pdm_s[i] / _tr_s[i] if _tr_s[i] > 0 else 0.0 for i in range(len(_tr_s))]
                    _mdi = [100 * _mdm_s[i] / _tr_s[i] if _tr_s[i] > 0 else 0.0 for i in range(len(_tr_s))]
                    _dx = [
                        100 * abs(_pdi[i] - _mdi[i]) / (_pdi[i] + _mdi[i])
                        if _pdi[i] + _mdi[i] > 0 else 0.0
                        for i in range(len(_pdi))
                    ]
                    if len(_dx) >= _period:
                        _adx_s = _wilder_avg(_dx, _period)
                        trade.entry_adx_h1 = round(_adx_s[-1], 2)
            except Exception:
                pass

            # (6)(7) Bollinger Bands on M15
            try:
                _m15h = self._m15_close_history.get(pair)
                if _m15h and len(_m15h) >= 20:
                    _closes = list(_m15h)
                    _last20 = _closes[-20:]
                    _sma = float(_np_qm.mean(_last20))
                    _std = float(_np_qm.std(_last20))
                    _upper = _sma + 2 * _std
                    _lower = _sma - 2 * _std
                    _cur = _r.close_prices.get(pair, _closes[-1]) if _r else _closes[-1]
                    if _upper > _lower:
                        _pos = (_cur - _lower) / (_upper - _lower)
                        trade.entry_bb_position_m15 = round(max(0.0, min(1.0, _pos)), 3)
                    if len(_closes) >= 25:
                        _prior20 = _closes[-25:-5]
                        _prior_std = float(_np_qm.std(_prior20))
                        if _prior_std > 0:
                            trade.entry_bb_width_ratio_m15 = round((4 * _std) / (4 * _prior_std), 3)
            except Exception:
                pass

            # (8) Tick flow bias
            try:
                _fs = getattr(_r, "flow_states", None) or {} if _r else {}
                _st = _fs.get(pair)
                if _st is not None:
                    _bias = getattr(_st, "flow_bias", 0.0)
                    trade.entry_tick_flow_bias = round(_bias if _is_buy else -_bias, 3)
            except Exception:
                pass

            # (9) Volume ramp 5min
            try:
                _tvh = self._m1_tick_volume_history.get(pair)
                if _tvh and len(_tvh) >= 15:
                    _tv_list = list(_tvh)
                    _last5 = sum(_tv_list[-5:])
                    _prev10 = sum(_tv_list[-15:-5])
                    if _prev10 > 0:
                        trade.entry_volume_ramp_5m = round(
                            (_last5 / 5.0) / (_prev10 / 10.0), 2
                        )
            except Exception:
                pass

            # (10) Range compression (std 10 / std 30)
            try:
                _ch = self._m1_close_history.get(pair)
                if _ch and len(_ch) >= 30:
                    _cls = list(_ch)
                    _s10 = float(_np_qm.std(_cls[-10:]))
                    _s30 = float(_np_qm.std(_cls[-30:]))
                    if _s30 > 0:
                        trade.entry_range_compression = round(_s10 / _s30, 3)
            except Exception:
                pass

            # (11) Cross-pair confirm: QM4 doesn't have an Sv2 `fired` list in this scope,
            # so we count the CURRENT composite_scores where another pair sharing base or
            # quote has a strong alignment (stoch H1 extreme) in the trade direction.
            try:
                _ss = getattr(_r, "stoch_scores", {}) if _r else {}
                _h1_stoch = _ss.get("H1", {}) if _ss else {}
                if _h1_stoch:
                    _count = 0
                    for _ccy, _score in _h1_stoch.items():
                        if _ccy in (base, quote):
                            continue
                        if _is_buy:
                            # Confirming = another currency is NOT aligned with
                            # our strong side — i.e., its stoch doesn't extreme-strong
                            # or quote-strong. Simpler: count currencies with |score-5| > 2
                            if abs(_score - 5.0) > 2.0:
                                _count += 1
                        else:
                            if abs(_score - 5.0) > 2.0:
                                _count += 1
                    # Rough proxy — actually not as useful as in Sv2. Stamp zero if unsure.
                    trade.entry_cross_pair_confirm = 0  # intentional stub for QM4
            except Exception:
                pass

            # (12) Session volume percentile — stub (needs historical baseline)
            # Field stays at 0.0.

            # (13) M5 bar close strength in trade direction
            try:
                _m5h = self._m5_bar_history.get(pair)
                if _m5h:
                    _hh, _ll, _cc = _m5h[-1]
                    if _hh > _ll:
                        _raw = (_cc - _ll) / (_hh - _ll)
                        trade.entry_m5_close_strength = round(
                            _raw if _is_buy else (1.0 - _raw), 3
                        )
            except Exception:
                pass

        except Exception as exc:
            logger.debug("[QM4] _stamp_qm4_extras error: %s", exc)

    def _sync_qm4_journal(self, trade) -> None:
        """Copy stamped entry signals from a QM4 tracker trade to its journal record.

        QM4 stamps fields inline (not via _stamp_entry_signals), and the journal
        record is created in `open_paper_trade` BEFORE the stamping, so without
        this sync the journal record has zero values for the signal columns.
        """
        if trade is None:
            return
        try:
            pt = self._paper_trader_qm4
            jidx = getattr(trade, "_journal_idx", -1)
            if not (0 <= jidx < len(pt._journal)):
                return
            rec = pt._journal[jidx]
            for attr in (
                "entry_m5_base", "entry_m5_quote",
                "entry_m15_base", "entry_m15_quote",
                "entry_h1_base", "entry_h1_quote",
                "entry_h4_base", "entry_h4_quote",
                "entry_d1_base", "entry_d1_quote",
                "entry_w1_base", "entry_w1_quote",
                "entry_mn_base", "entry_mn_quote",
                "entry_alignment_count",
                "entry_div_spread", "entry_h1_atr_pips",
                "entry_structural", "entry_tier",
                "qm4_alert_type",
                # Pre-existing extras:
                "entry_sl_atr_mult", "entry_tp_atr_mult",
                "entry_strong_ccy", "entry_weak_ccy",
                "entry_strong_rank", "entry_weak_rank",
                "entry_strong_top_gap", "entry_weak_bottom_gap",
                "entry_strong_velocity", "entry_weak_velocity",
                "entry_m5_tr_slope_ratio", "entry_minutes_since_news",
                "entry_spread_price",
                "entry_alt_signal_1", "entry_alt_signal_2",
                "entry_alt_signal_3", "entry_alt_signal_4",
                # BUG B FIX (2026-04-20): QM4 market-context fields that
                # previously weren't stamped or synced.
                "entry_tick_volume_ratio", "entry_momentum_buildup_sec",
                "entry_dist_day_high_pips", "entry_dist_day_low_pips",
                "entry_dist_week_high_pips", "entry_dist_week_low_pips",
                "entry_dist_month_high_pips", "entry_dist_month_low_pips",
                "entry_dist_00_pips", "entry_dist_000_pips",
                "entry_session_minutes_in", "entry_day_of_week",
                "entry_prev_trade_result", "entry_concurrent_trades",
                "entry_m1_body_pct", "entry_m1_direction",
                "entry_atr_ratio",
                # 14 new 2026-04-20 momentum/trend-start fields
                "entry_m1_consec_aligned", "entry_composite_vel_90s",
                "entry_m5_higher_highs", "entry_m5_higher_lows",
                "entry_vwap_dist_pips", "entry_adx_h1",
                "entry_bb_position_m15", "entry_bb_width_ratio_m15",
                "entry_tick_flow_bias", "entry_volume_ramp_5m",
                "entry_range_compression", "entry_cross_pair_confirm",
                "entry_session_vol_pct", "entry_m5_close_strength",
            ):
                if hasattr(rec, attr) and hasattr(trade, attr):
                    setattr(rec, attr, getattr(trade, attr))
            pt.save_journal()
        except Exception as exc:
            logger.warning("[QM4] Journal sync failed: %s", exc)

    def _on_ctrader_order_opened(self, pair: str, position_id: int, direction: str) -> None:
        """Handle confirmed order fill from cTrader."""
        price = self._latest_close_prices.get(pair, 0.0)
        lot_size = self._ctrader_config.get("ctrader_lot_size", 0.01)
        self._ctrader_pos_mgr.register_open(pair, direction, position_id, lot_size, price)
        self._ctrader_pos_mgr.save(_CTRADER_POS_FILE)
        # Clear from pending set
        if hasattr(self, "_ctrader_pending"):
            self._ctrader_pending.discard(pair)

        # Note: paper trader already tracks the trade via its own TradeTracker.
        # Do NOT open a duplicate trade here — it creates phantom STND entries.

        # Log to alert panel
        now_str = datetime.now(_jst()).strftime("%H:%M:%S")
        color = "#1b8a2a" if direction == "BUY" else "#c62828"
        html = (
            f'<span style="font-size:9pt; color:#666;">[{now_str}]</span> '
            f'<span style="font-size:10pt; color:{color}; font-weight:bold;">'
            f"\U0001f4b9 cT OPENED</span> "
            f'<span style="font-size:10pt; font-weight:bold;">'
            f"{direction} {pair}</span> "
            f'<span style="font-size:9pt; color:#666;">pos={position_id}</span>'
        )
        self._alert_history.appendleft(html)
        self._refresh_alert_panel()
        logger.info("cTrader order opened: %s %s pos=%d", direction, pair, position_id)

    def _on_ctrader_order_closed(self, pair: str, position_id: int) -> None:
        """Handle confirmed position close from cTrader."""
        self._ctrader_pos_mgr.register_close(position_id)
        self._ctrader_pos_mgr.save(_CTRADER_POS_FILE)
        # Clear persistent tracking so the pair can be traded again.
        # dtc_combo is the only active live tag today; the others are
        # legacy and included so stale keys from prior versions also clear.
        if hasattr(self, "_ct_open_positions"):
            for tag in ("sv2", "ss", "atr", "qm4", "a_tuned", "b_tuned", "dtc_combo"):
                self._ct_open_positions.discard(f"{pair}_{tag}")

        # NOTE: We do NOT close the paper trackers here. Paper trades close
        # independently via paper_trader._check_sl_tp() in update_cycle.
        # Closing them here without going through _close_and_journal leaves
        # orphan "OPEN" journal records. Paper trades will close naturally
        # on their own SL/TP hits with correct journaling.

        now_str = datetime.now(_jst()).strftime("%H:%M:%S")
        html = (
            f'<span style="font-size:9pt; color:#666;">[{now_str}]</span> '
            f'<span style="font-size:10pt; color:#4a6fa5; font-weight:bold;">'
            f"\u2705 cT CLOSED</span> "
            f'<span style="font-size:10pt; font-weight:bold;">{pair}</span> '
            f'<span style="font-size:9pt; color:#666;">pos={position_id}</span>'
        )
        self._alert_history.appendleft(html)
        self._refresh_alert_panel()
        logger.info("cTrader position closed: %s pos=%d", pair, position_id)

    def _on_ctrader_order_error(self, pair: str, error: str) -> None:
        """Handle cTrader order error — suppress auth/reconnect noise + cascade.

        Cascade protection (2026-05-14):
          * Non-retryable server errors (CANT_ROUTE_REQUEST, etc.) — DO NOT
            clear the pair's position lock. Clearing it would let the
            strategy re-fire next cycle and hit the same server refusal,
            creating an alert-panel cascade (~1 entry per 3-5 sec).
          * Retryable errors (timeouts, auth races, etc.) — existing
            behavior preserved (clear lock so next cycle can retry).
          * Panel append also dedup'd by (pair, error-code) within 120s
            mirroring the popup's existing dedup_key cooldown.
        """
        non_retryable = _ct_is_non_retryable_error(error)

        # Clear position tracking ONLY for retryable errors.
        # dtc_combo is the only active live tag today; the others are
        # legacy and included so stale keys also clear.
        # For non-retryable: lock stays in place. Releases naturally
        # when the paper trade closes or the next legitimate signal
        # cycle for this pair triggers (M5 close + fresh entry).
        if pair and hasattr(self, "_ct_open_positions") and not non_retryable:
            for tag in ("sv2", "ss", "atr", "qm4", "dtc_combo"):
                self._ct_open_positions.discard(f"{pair}_{tag}")

        # Don't show connection/auth errors in TREND ALERTS (they auto-resolve)
        _noise = ("auth failed", "Reconcile failed", "ALREADY_LOGGED_IN",
                   "TimeoutError", "Deferred")
        if any(n in error for n in _noise):
            logger.warning("cTrader connection issue (auto-retry): %s", error[:100])
            return

        # ── Panel-side dedupe (2026-05-14) ──
        # Mirrors the popup's `cooldown=120` semantic but applied to the
        # TREND ALERTS appendleft path. Without this, a non-retryable
        # error that fires repeatedly via a strategy loop would spam the
        # panel with duplicate entries even though the popup is silent.
        # Key shape: "{pair}:{first-token-of-error}" — first token is
        # typically the error code (e.g., "CANT_ROUTE_REQUEST") for
        # cTrader rejections and the bare message for others.
        if not hasattr(self, "_ct_last_panel_alert"):
            self._ct_last_panel_alert: dict[str, float] = {}
        _err_token = error.split(":", 1)[0].strip()[:40] if ":" in error else error[:40]
        _panel_key = f"{pair or 'generic'}:{_err_token}"
        _now_ts = time.time()
        _last_ts = self._ct_last_panel_alert.get(_panel_key, 0.0)
        if _now_ts - _last_ts < 120:
            # Within cooldown — log only, skip panel append + popup.
            logger.debug(
                "cTrader order error (panel-deduped within 120s): %s",
                error[:100],
            )
            return
        self._ct_last_panel_alert[_panel_key] = _now_ts

        now_str = datetime.now(_jst()).strftime("%H:%M:%S")
        html = (
            f'<span style="font-size:9pt; color:#666;">[{now_str}]</span> '
            f'<span style="font-size:10pt; color:#d50000; font-weight:bold;">'
            f"\u274c cT ERROR</span> "
            f'<span style="font-size:9pt;">{pair}: {error[:100]}</span>'
        )
        self._alert_history.appendleft(html)
        self._refresh_alert_panel()
        logger.error("cTrader order error: %s — %s", pair, error)
        # POPUP: real broker rejection (insufficient margin, market closed,
        # symbol disabled, etc.) — operator needs to know
        if hasattr(self, "_health_alerts"):
            self._health_alerts.notify(
                "error", "cTrader",
                f"Order rejected on {pair or 'unknown pair'}:\n\n{error[:300]}\n\n"
                "The DTC paper trade still recorded normally. Live execution "
                "for this signal failed.\n\n"
                "Common causes:\n"
                "  • Insufficient margin\n"
                "  • Market closed (weekend/holiday)\n"
                "  • Symbol not enabled on this account\n"
                "  • Lot size below broker minimum",
                dedup_key=f"ctrader_reject_{pair or 'generic'}",
                cooldown=120,
            )

    def _on_ctrader_balance(self, balance: float) -> None:
        """Auto-update balance from cTrader account."""
        self._ctrader_config["ctrader_balance"] = balance
        # Also persist to QSettings
        from PyQt6.QtCore import QSettings
        s = QSettings("TAKUMITrader", "TAKUMITrader")
        s.setValue("ctrader/balance", balance)
        logger.info("cTrader balance auto-updated: %.2f", balance)

    def _on_ctrader_positions_synced(self, positions: list) -> None:
        """Handle reconciled positions from cTrader."""
        self._ctrader_pos_mgr.reconcile(positions)
        self._ctrader_pos_mgr.save(_CTRADER_POS_FILE)
        logger.info("cTrader positions synced: %d open", len(positions))

    # ── MT5 trading signal handlers ──────────────────────────────

    def _should_mirror_mt5(self, pair: str, entry_type: str) -> bool:
        """Return True if this trade matches the MT5 mirror config filters.

        Logic:
            - If mirror_combos is non-empty, ONLY combos match (exclusive).
            - Otherwise: empty list = no restriction on that axis.
              Both pair and system filters must pass (empty = pass).
            - After that: minute_blacklist_per_pair blocks the current JST
              minute-of-day if it falls in any pair's [start, end) window.
              TAKUMI paper trading still takes the trade — only MT5 skips.
        """
        cfg = self._mt5_mirror_cfg
        if not cfg.get("enabled"):
            return False
        combos = cfg.get("mirror_combos") or []
        if combos:
            if f"{pair}:{entry_type}" not in combos:
                return False
        else:
            pairs = cfg.get("mirror_pairs") or []
            if pairs and pair not in pairs:
                return False
            systems = cfg.get("mirror_systems") or []
            if systems and entry_type not in systems:
                return False
        # Per-pair minute-precise blacklist: skip MT5 mirror if current JST
        # minute-of-day is in any blocked [start, end) window for this pair.
        bl_map = cfg.get("minute_blacklist_per_pair") or {}
        if bl_map and pair in bl_map:
            now_jst = datetime.now(_jst())
            now_mins = now_jst.hour * 60 + now_jst.minute
            for entry in bl_map[pair]:
                try:
                    start_str, end_str = entry[0], entry[1]
                    sh, sm = [int(x) for x in start_str.split(":")]
                    eh, em = [int(x) for x in end_str.split(":")]
                    start_mins = sh * 60 + sm
                    end_mins = eh * 60 + em
                    if start_mins <= end_mins:
                        in_block = start_mins <= now_mins < end_mins
                    else:  # wraps past midnight
                        in_block = now_mins >= start_mins or now_mins < end_mins
                    if in_block:
                        logger.info(
                            "[MT5 MIRROR] %s %s skipped — %s JST in blocked window %s–%s",
                            entry_type, pair, now_jst.strftime("%H:%M"),
                            start_str, end_str,
                        )
                        return False
                except (ValueError, AttributeError, IndexError) as exc:
                    logger.warning("[MT5 MIRROR] Bad minute_blacklist entry %s for %s: %s",
                                   entry, pair, exc)
        return True

    def _mirror_to_mt5(self, trade, entry_type: str) -> None:
        """Mirror a freshly-opened paper trade to the live MT5 demo account.

        Only called from the 5 currency-strength systems' entry paths
        (Sv2, SS, ATR, A-tuned, B-tuned). QM4 and alt systems are NOT mirrored.
        Broker enforces SL/TP; no periodic close logic needed here.
        """
        if trade is None:
            return
        if not self._should_mirror_mt5(trade.pair, entry_type):
            return
        # Concurrent-position cap: 0 or negative = unlimited
        max_pos = int(self._mt5_mirror_cfg.get("max_positions", 0) or 0)
        if max_pos > 0 and self._mt5_pos_mgr.open_count >= max_pos:
            logger.info(
                "[MT5 MIRROR] %s %s skipped — max_positions (%d) reached",
                trade.direction, trade.pair, max_pos,
            )
            return
        # Already have a position on this pair? skip to avoid duplicates
        if self._mt5_pos_mgr.has_position(trade.pair):
            logger.info(
                "[MT5 MIRROR] %s %s skipped — position already exists on MT5",
                trade.direction, trade.pair,
            )
            return
        risk_pct = float(self._mt5_mirror_cfg.get("risk_pct", 3.0) or 3.0)
        try:
            ticket = self._mt5_trader.open_order(
                pair=trade.pair,
                direction=trade.direction,
                sl_price=trade.sl_price,
                tp_price=trade.tp_price,
                sl_pips=trade.sl_pips,
                tp_pips=trade.tp_pips,
                risk_pct=risk_pct,
            )
            if ticket:
                logger.info(
                    "[MT5 MIRROR] %s %s %s mirrored to MT5 ticket=%d (system=%s)",
                    trade.direction, trade.pair, trade.entry_price, ticket, entry_type,
                )
        except Exception as exc:
            logger.warning("[MT5 MIRROR] Failed to mirror %s %s: %s",
                           trade.direction, trade.pair, exc)

    # ────────────────────────────────────────────────────────────────
    # DTC-combo: filtered aggregate of SS + ATR + B-tuned
    # ────────────────────────────────────────────────────────────────

    def _load_dtc_combo_cfg(self) -> dict:
        """Load data/dtc_combo_config.json with safe defaults."""
        defaults = {
            "enabled": True,
            "source_systems": ["sv2_ss", "sv2_atr", "sv2_b_tuned"],
            "dedup_window_seconds": 120,
            "per_system_pair_blacklist": {},
            "time_blacklist_per_system": {},
        }
        cfg_path = _DATA_DIR / "dtc_combo_config.json"
        if not cfg_path.exists():
            logger.info("[DTC] No dtc_combo_config.json — using defaults (enabled=True)")
            return defaults
        try:
            import json as _json
            loaded = _json.loads(cfg_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                loaded.pop("_note", None)
                defaults.update(loaded)

            # ── Type-coerce / validate each field defensively ──
            # Any bad types fall back to safe defaults with a warning so a
            # human-edited config typo can't silently break live trading.

            # enabled: bool
            if not isinstance(defaults.get("enabled"), bool):
                logger.warning("[DTC] 'enabled' must be bool, got %r — defaulting to True",
                               defaults.get("enabled"))
                defaults["enabled"] = True

            # source_systems: list of str
            ss = defaults.get("source_systems")
            if not isinstance(ss, list) or not all(isinstance(x, str) for x in ss):
                logger.warning("[DTC] 'source_systems' must be list[str], got %r — resetting", ss)
                defaults["source_systems"] = ["sv2_ss", "sv2_atr", "sv2_b_tuned"]

            # dedup_window_seconds: numeric
            dw = defaults.get("dedup_window_seconds")
            if not isinstance(dw, (int, float)) or dw < 0:
                try:
                    defaults["dedup_window_seconds"] = float(dw)
                    if defaults["dedup_window_seconds"] < 0:
                        raise ValueError("negative")
                except (ValueError, TypeError):
                    logger.warning("[DTC] bad dedup_window_seconds %r — using 120", dw)
                    defaults["dedup_window_seconds"] = 120

            # tp_ratio_override: float or None
            tpr = defaults.get("tp_ratio_override")
            if tpr is not None:
                try:
                    tpr_f = float(tpr)
                    if tpr_f <= 0:
                        raise ValueError("must be > 0")
                    defaults["tp_ratio_override"] = tpr_f
                except (ValueError, TypeError):
                    logger.warning("[DTC] bad tp_ratio_override %r — disabling override", tpr)
                    defaults["tp_ratio_override"] = None

            # per_system_pair_blacklist: dict[str, list[str]]
            pbl = defaults.get("per_system_pair_blacklist")
            if not isinstance(pbl, dict):
                logger.warning("[DTC] 'per_system_pair_blacklist' must be dict — resetting")
                pbl = {}
            clean_pbl = {}
            for sys_key, pairs in pbl.items():
                if not isinstance(sys_key, str):
                    continue
                if not isinstance(pairs, list) or not all(isinstance(p, str) for p in pairs):
                    logger.warning("[DTC] pair_blacklist[%s] must be list[str] — skipping",
                                   sys_key)
                    continue
                clean_pbl[sys_key] = pairs
            defaults["per_system_pair_blacklist"] = clean_pbl

            # time_blacklist_per_system: dict[str, list[[str, str]]]
            import re as _re_dtc
            _time_re = _re_dtc.compile(r"^\d{1,2}:\d{2}$")
            tbl = defaults.get("time_blacklist_per_system")
            if not isinstance(tbl, dict):
                logger.warning("[DTC] 'time_blacklist_per_system' must be dict — resetting")
                tbl = {}
            clean_tbl = {}
            for sys_key, windows in tbl.items():
                if not isinstance(sys_key, str) or not isinstance(windows, list):
                    logger.warning("[DTC] time_blacklist[%s] must be list — skipping", sys_key)
                    continue
                clean_wins = []
                for w in windows:
                    if (isinstance(w, (list, tuple)) and len(w) == 2
                            and isinstance(w[0], str) and isinstance(w[1], str)
                            and _time_re.match(w[0]) and _time_re.match(w[1])):
                        try:
                            sh, sm = [int(x) for x in w[0].split(":")]
                            eh, em = [int(x) for x in w[1].split(":")]
                            if 0 <= sh <= 24 and 0 <= sm <= 59 and 0 <= eh <= 24 and 0 <= em <= 59:
                                clean_wins.append([w[0], w[1]])
                                continue
                        except (ValueError, AttributeError):
                            pass
                    logger.warning("[DTC] time_blacklist[%s] bad window %r — skipping",
                                   sys_key, w)
                clean_tbl[sys_key] = clean_wins
            defaults["time_blacklist_per_system"] = clean_tbl

            logger.info(
                "[DTC] Config loaded: enabled=%s sources=%s dedup=%ss tp_ovr=%s "
                "pair_bl=%s time_bl=%s",
                defaults["enabled"],
                defaults["source_systems"],
                defaults["dedup_window_seconds"],
                defaults.get("tp_ratio_override"),
                {k: len(v) for k, v in defaults["per_system_pair_blacklist"].items()},
                {k: len(v) for k, v in defaults["time_blacklist_per_system"].items()},
            )
        except Exception as exc:
            logger.warning("[DTC] Failed to load dtc_combo_config.json: %s — using defaults",
                           exc)
            # POPUP: silent fallback to empty defaults means DTC trades with NO
            # blacklists, which is unsafe. Operator must fix the config.
            if hasattr(self, "_health_alerts"):
                self._health_alerts.notify(
                    "error", "DTC Config",
                    f"data/dtc_combo_config.json failed to load:\n\n{exc}\n\n"
                    "DTC is now running with EMPTY filters (no pair blacklist, "
                    "no time blackout). Fix the JSON file and restart TAKUMI.",
                    dedup_key="dtc_config_load_fail",
                    cooldown=3600,
                )
        return defaults

    def _dtc_in_time_blackout(self, source_system: str) -> tuple[bool, str]:
        """Check if current JST minute-of-day is in a blocked window
        for the given source system. Returns (in_block, window_str)."""
        tbl_map = (self._dtc_combo_cfg.get("time_blacklist_per_system") or {})
        windows = tbl_map.get(source_system) or []
        if not windows:
            return False, ""
        now_jst = datetime.now(_jst())
        now_mins = now_jst.hour * 60 + now_jst.minute
        for entry in windows:
            try:
                start_str, end_str = entry[0], entry[1]
                sh, sm = [int(x) for x in start_str.split(":")]
                eh, em = [int(x) for x in end_str.split(":")]
                start_mins = sh * 60 + sm
                end_mins = eh * 60 + em
                if start_mins <= end_mins:
                    in_block = start_mins <= now_mins < end_mins
                else:
                    in_block = now_mins >= start_mins or now_mins < end_mins
                if in_block:
                    return True, f"{start_str}–{end_str}"
            except (ValueError, AttributeError, IndexError, TypeError) as exc:
                # Should not happen since _load_dtc_combo_cfg validates, but
                # log loudly if we somehow hit a malformed entry at runtime —
                # silent failure here could make a "blocked" window let trades
                # through without any visible signal.
                logger.warning("[DTC] Malformed time window %r for %s (%s) — SKIPPED",
                               entry, source_system, exc)
                continue
        return False, ""

    def _maybe_open_dtc_combo(self, source_pt, source_system: str, result, conv):
        """Mirror a freshly-opened source-system paper trade into DTC-combo
        if the per-system pair/time filters allow and the cross-system dedup
        window has elapsed.

        Called from the SS, ATR, B-tuned entry paths immediately after a
        source-system paper trade is created.

        Returns the created DTC TrackedTrade (so the caller can stamp entry
        context / analytics onto it), or None if the DTC trade was not opened
        for any reason.
        """
        if source_pt is None:
            return None
        cfg = self._dtc_combo_cfg
        if not cfg.get("enabled"):
            return None
        allowed_sources = cfg.get("source_systems") or []
        if allowed_sources and source_system not in allowed_sources:
            return None

        pair = source_pt.pair

        # Per-system pair blacklist
        pair_bl_map = cfg.get("per_system_pair_blacklist") or {}
        bl = pair_bl_map.get(source_system) or []
        if pair in bl:
            logger.info("[DTC] %s %s skipped — pair blocked for %s",
                        source_pt.direction, pair, source_system)
            return None

        # Per-system time blackout
        in_block, window = self._dtc_in_time_blackout(source_system)
        if in_block:
            logger.info("[DTC] %s %s skipped — %s JST in %s blocked window %s",
                        source_pt.direction, pair,
                        datetime.now(_jst()).strftime("%H:%M"),
                        source_system, window)
            return None

        # Cross-system dedup: skip if another source already fired DTC on
        # this pair within dedup_window_seconds
        dedup_sec = float(cfg.get("dedup_window_seconds", 120) or 120)
        now_ts = datetime.now(_jst()).timestamp()
        last_ts = self._dtc_combo_last_open_ts.get(pair, 0.0)
        if last_ts > 0 and (now_ts - last_ts) < dedup_sec:
            logger.info("[DTC] %s %s skipped — dedup (%.0fs since last DTC on pair)",
                        source_pt.direction, pair, now_ts - last_ts)
            return None

        # ── Quality filters (DTC-only, 2026-04-20) ──
        # Added after 7-day what-if analysis on 607 A-E trades showed:
        #   ATR  ≥ 0.7  saves +597p by blocking dead-market entries
        #   ADR  ≤ 70%  saves +405p by blocking exhausted-range entries
        #   ROOM ≥ 10p  saves +208p by blocking entries near day's extreme
        # All three stacked: +451p at 84.9% WR on kept subset.
        # Values read from source_pt (already stamped by _stamp_entry_signals
        # before this hook runs) + live session_range_pct from cycle result.
        # Paper A-E continue taking every signal for ongoing validation.
        qf_cfg = cfg.get("quality_filters") or {}
        if qf_cfg.get("enabled", True):
            # Filter 1: ATR ratio >= min (dead-market protection)
            atr_min = float(qf_cfg.get("atr_ratio_min", 0.7) or 0.0)
            atr_ratio = getattr(source_pt, "entry_atr_ratio", 0.0)
            # Only enforce when the field is actually measured (>0);
            # value of 0.0 means H1 ATR history wasn't deep enough to compute.
            if atr_min > 0 and 0.0 < atr_ratio < atr_min:
                logger.info(
                    "[DTC] %s %s skipped — ATR ratio %.2f < %.2f (dead market)",
                    source_pt.direction, pair, atr_ratio, atr_min,
                )
                return None

            # Filter 2: ADR consumed <= max (move-exhaustion protection)
            adr_max = float(qf_cfg.get("adr_max_pct", 70.0) or 0.0)
            adr_pct = float(result.session_range_pct.get(pair, 0.0) or 0.0)
            if adr_max > 0 and adr_pct > adr_max:
                logger.info(
                    "[DTC] %s %s skipped — ADR consumed %.0f%% > %.0f%% (move exhausted)",
                    source_pt.direction, pair, adr_pct, adr_max,
                )
                return None

            # Filter 3: room to today's extreme in trade direction >= min pips
            # BUY  wants positive dist_day_high  (room up toward day high)
            # SELL wants negative dist_day_low   (room down toward day low)
            room_min = float(qf_cfg.get("room_min_pips", 10.0) or 0.0)
            dh = float(getattr(source_pt, "entry_dist_day_high_pips", 0.0) or 0.0)
            dl = float(getattr(source_pt, "entry_dist_day_low_pips", 0.0) or 0.0)
            # Treat "both exactly 0" as "day range not measured yet" — allow.
            if room_min > 0 and (dh != 0.0 or dl != 0.0):
                if source_pt.direction == "BUY":
                    room = dh        # positive = room up to day high
                    extreme = "high"
                else:
                    room = -dl       # dist_day_low is negative when low is below price
                    extreme = "low"
                if room < room_min:
                    logger.info(
                        "[DTC] %s %s skipped — only %.1fp room to day %s (need ≥%.0fp)",
                        source_pt.direction, pair, room, extreme, room_min,
                    )
                    return None

        # Open the DTC paper trade using the SAME entry params as the source.
        # TP is overridden per dtc_combo_config.json (currently 0.75 × SL,
        # vs the source systems' ~0.5 × SL). Sweet spot from R:R analysis:
        # higher total return for modest WR sacrifice, modest DD increase.
        tp_override = cfg.get("tp_ratio_override")
        try:
            pt_dtc = self._paper_trader_dtc_combo.open_paper_trade(
                pair=pair,
                direction=source_pt.direction,
                entry_price=source_pt.entry_price,
                composite_scores=result.composite_scores,
                conviction=conv.conviction if conv else 0,
                session=get_current_session(),
                h1_atr=result.h1_atr.get(pair, 0.0),
                entry_type="dtc_combo",
                adr_consumed_pct=result.session_range_pct.get(pair, 0.0),
                tp_ratio_override=tp_override,
            )
        except Exception as exc:
            logger.warning("[DTC] open_paper_trade failed for %s %s: %s",
                           source_pt.direction, pair, exc)
            return None

        if pt_dtc is None:
            # Tracker already has this pair or entry_type rejected
            return None

        self._dtc_combo_last_open_ts[pair] = now_ts
        # Tag the trade with which source system triggered this DTC entry —
        # lets the Performance dialog show SS / ATR / B-tuned lineage so we
        # can later analyse whether one source feeds DTC more profitably.
        # Also sync to journal + persist the tracker immediately so the tag
        # survives a crash/restart before the follow-up _stamp_entry_signals
        # call can propagate it.
        try:
            pt_dtc.dtc_source_system = source_system
            self._paper_trader_dtc_combo.sync_trade_to_journal(pt_dtc, save=True)
        except Exception as _tag_exc:
            logger.warning("[DTC] failed to stamp source tag on %s: %s",
                           pair, _tag_exc)
        logger.info("[DTC] %s %s opened (source=%s, SL=%.1fp TP=%.1fp)",
                    source_pt.direction, pair, source_system,
                    pt_dtc.sl_pips, pt_dtc.tp_pips)

        # ── cTrader live mirror ──
        # DTC-combo is now the SOLE system routed to cTrader. All other paper
        # systems stay paper-only. The bridge handles its own enabled/auto_open
        # config; we just gate on connection status here.
        if (self._ctrader_bridge is not None
                and self._ctrader_bridge.is_connected
                and self._ctrader_config.get("ctrader_enabled")
                and self._ctrader_config.get("ctrader_auto_open")):
            try:
                max_pos = int(self._ctrader_config.get("ctrader_max_positions", 28) or 28)
                self._ct_open_order(pair, pt_dtc.direction, "dtc_combo", max_pos)
            except Exception as exc:
                logger.warning("[DTC→cTrader] Failed to mirror %s %s: %s",
                               pt_dtc.direction, pair, exc)
        else:
            logger.info("[DTC→cTrader] %s %s NOT sent to cTrader "
                        "(bridge_connected=%s, enabled=%s, auto_open=%s)",
                        pt_dtc.direction, pair,
                        self._ctrader_bridge.is_connected if self._ctrader_bridge else False,
                        self._ctrader_config.get("ctrader_enabled"),
                        self._ctrader_config.get("ctrader_auto_open"))

        return pt_dtc

    def _on_mt5_order_opened(self, pair: str, ticket: int, direction: str, lots: float) -> None:
        now_str = datetime.now(_jst()).strftime("%H:%M:%S")
        dir_c = "#1b8a2a" if direction == "BUY" else "#c62828"
        html = (
            f'<span style="font-size:9pt; color:#666;">[{now_str}]</span> '
            f'<span style="font-size:10pt; color:{dir_c}; font-weight:bold;">'
            f'\U0001f4b9 MT5 OPEN</span> '
            f'<span style="font-size:10pt; color:{dir_c};"><b>{direction}</b> {pair}</span> '
            f'<span style="font-size:9pt;">{lots:.2f} lots | ticket #{ticket}</span>'
        )
        self._alert_history.appendleft(html)
        self._refresh_alert_panel()

    def _on_mt5_order_closed(self, pair: str, ticket: int, pnl_pips: float) -> None:
        now_str = datetime.now(_jst()).strftime("%H:%M:%S")
        pnl_c = "#1b8a2a" if pnl_pips >= 0 else "#c62828"
        html = (
            f'<span style="font-size:9pt; color:#666;">[{now_str}]</span> '
            f'<span style="font-size:10pt; color:#e65100; font-weight:bold;">'
            f'\U0001f4dd MT5 CLOSE</span> '
            f'<span style="font-size:10pt;">{pair}</span> '
            f'<span style="font-size:10pt; color:{pnl_c}; font-weight:bold;">'
            f'{pnl_pips:+.1f}p</span> '
            f'<span style="font-size:9pt;">ticket #{ticket}</span>'
        )
        self._alert_history.appendleft(html)
        self._refresh_alert_panel()

    def _on_mt5_order_error(self, pair: str, error: str) -> None:
        now_str = datetime.now(_jst()).strftime("%H:%M:%S")
        html = (
            f'<span style="font-size:9pt; color:#666;">[{now_str}]</span> '
            f'<span style="font-size:10pt; color:#d50000; font-weight:bold;">'
            f'\u274c MT5 ERROR</span> '
            f'<span style="font-size:9pt;">{pair}: {error}</span>'
        )
        self._alert_history.appendleft(html)
        self._refresh_alert_panel()
        logger.error("MT5 order error: %s - %s", pair, error)
        # POPUP: real broker / terminal error
        if hasattr(self, "_health_alerts"):
            self._health_alerts.notify(
                "error", "MT5",
                f"Order failed on {pair or 'unknown pair'}:\n\n{str(error)[:300]}\n\n"
                "Common causes:\n"
                "  • Algo trading disabled in MT5 (Tools → Options → Expert Advisors)\n"
                "  • Insufficient margin\n"
                "  • Symbol not in Market Watch\n"
                "  • Market closed",
                dedup_key=f"mt5_reject_{pair or 'generic'}",
                cooldown=120,
            )

    def _on_filters_changed(self) -> None:
        """Handle filter toggle from toolbar — immediate recalculation."""
        # The next data cycle will pick up the new filter settings automatically
        # since we read from self._filter_toolbar.filter_settings each cycle.
        logger.info("Filter settings changed: %s", self._filter_toolbar.filter_settings)

    # ── Window geometry persistence ──────────────────────────────────

    def _save_geometry(self) -> None:
        """Save window position, size, and splitter state to QSettings."""
        s = QSettings("TAKUMITrader", "TAKUMITrader")
        s.setValue("window/geometry", self.saveGeometry())
        s.setValue("window/splitter", self._panels_splitter.saveState())
        # Also save explicit pos/size as fallback (survives monitor changes)
        pos = self.pos()
        size = self.size()
        s.setValue("window/x", pos.x())
        s.setValue("window/y", pos.y())
        s.setValue("window/w", size.width())
        s.setValue("window/h", size.height())

    def _restore_geometry(self) -> None:
        """Restore window position, size, and splitter state from QSettings."""
        s = QSettings("TAKUMITrader", "TAKUMITrader")
        geometry = s.value("window/geometry")
        restored = False
        if geometry is not None:
            restored = self.restoreGeometry(geometry)
        # Fallback: use explicit pos/size if restoreGeometry failed or
        # placed the window on a disconnected monitor
        if not restored or not self._is_on_visible_screen():
            x = s.value("window/x", type=int)
            y = s.value("window/y", type=int)
            w = s.value("window/w", type=int)
            h = s.value("window/h", type=int)
            if x is not None and w and h:
                self.move(x, y)
                self.resize(w, h)
        splitter_state = s.value("window/splitter")
        if splitter_state is not None:
            self._panels_splitter.restoreState(splitter_state)

    def _is_on_visible_screen(self) -> bool:
        """Check if the window center is on any connected screen."""
        from PyQt6.QtGui import QGuiApplication
        center = self.geometry().center()
        for screen in QGuiApplication.screens():
            if screen.availableGeometry().contains(center):
                return True
        return False

    # ── Cleanup ───────────────────────────────────────────────────────

    def closeEvent(self, event) -> None:
        """Save geometry, persist trades/alerts, stop worker, and quit app on close."""
        # Flag a shutdown-in-progress so downstream handlers (e.g. the cTrader
        # disconnect popup) can suppress benign teardown notifications.
        # Without this, stopping the cTrader bridge triggers a clean
        # ConnectionDone disconnect, which previously raised an error popup.
        self._shutting_down = True
        self._save_geometry()
        self._save_trades()
        self._save_alert_history()
        self._alert_perf.save_active(_ACTIVE_PERF_FILE)
        # Stop cTrader bridge and persist positions
        if self._ctrader_bridge:
            self._ctrader_bridge.stop()
        if self._ctrader_pos_mgr:
            self._ctrader_pos_mgr.save(_CTRADER_POS_FILE)
        if hasattr(self, "_csi_worker"):
            self._csi_worker.stop()
        self._worker.stop()
        self._worker.wait(3000)
        # Shadow-sim worker last (Phase D.4): drains its own state via
        # cooperative stop + wait. Stopping last means MT5Worker's
        # in-flight shadow_logger writes have all completed before the
        # sim worker's mark_decision/write_simulation calls start
        # contending — though all writes are atomic-flush, the cleaner
        # ordering keeps log lines easier to read.
        if getattr(self, "_shadow_sim_worker", None) is not None:
            try:
                self._shadow_sim_worker.stop()
                self._shadow_sim_worker.wait(3000)
            except Exception as exc:
                logger.warning("[SHADOW] sim worker shutdown raised: %s", exc)
        # F.1 (2026-05-14): explicit final flush of the shadow journal
        # so the last throttle window's mutations land on disk before
        # we exit. log_signal / log_strength_reject / mark_decision now
        # batch-flush every _FLUSH_THROTTLE_SEC (30s); without this
        # explicit force_flush we'd lose up to 30s of capture data on
        # every clean shutdown.
        if getattr(self, "_shadow_logger_sv2", None) is not None:
            try:
                self._shadow_logger_sv2.force_flush()
            except Exception as exc:
                logger.warning("[SHADOW] final force_flush raised: %s", exc)
        # Close all independent windows (Performance, Backtest, etc.)
        from PyQt6.QtWidgets import QApplication
        for w in QApplication.topLevelWidgets():
            if w is not self:
                w.close()
        event.accept()

    # ── Persistence ────────────────────────────────────────────────────

    def _save_trades(self) -> None:
        """Persist active trades for all systems to disk."""
        try:
            _DATA_DIR.mkdir(parents=True, exist_ok=True)
            self._trade_tracker.save_to_file(_TRADES_FILE)
            self._trade_tracker_ss.save_to_file(_TRADES_FILE_SS)
            self._trade_tracker_atr.save_to_file(_TRADES_FILE_ATR)
            self._trade_tracker_qm4.save_to_file(_TRADES_FILE_QM4)
            self._trade_tracker_a_tuned.save_to_file(_TRADES_FILE_A_TUNED)
            self._trade_tracker_b_tuned.save_to_file(_TRADES_FILE_B_TUNED)
            self._trade_tracker_breakout.save_to_file(_TRADES_FILE_BREAKOUT)
            self._trade_tracker_squeeze.save_to_file(_TRADES_FILE_SQUEEZE)
            # Squeeze-REV (2026-04-29) — inverse-direction mirror of Squeeze
            self._trade_tracker_squeeze_rev.save_to_file(_TRADES_FILE_SQUEEZE_REV)
            self._trade_tracker_divergence.save_to_file(_TRADES_FILE_DIVERGENCE)
            self._trade_tracker_dtc_combo.save_to_file(_TRADES_FILE_DTC_COMBO)
            # Live-candle systems (2026-04-21)
            self._trade_tracker_sv2_live.save_to_file(_TRADES_FILE_SV2_LIVE)
            self._trade_tracker_sv2_a_tuned_live.save_to_file(_TRADES_FILE_A_TUNED_LIVE)
            self._trade_tracker_sv2_ss_live.save_to_file(_TRADES_FILE_SS_LIVE)
            self._trade_tracker_sv2_b_tuned_live.save_to_file(_TRADES_FILE_B_TUNED_LIVE)
            self._trade_tracker_sv2_atr_live.save_to_file(_TRADES_FILE_ATR_LIVE)
            # Sv2-upgraded (2026-04-23)
            self._trade_tracker_sv2_upgraded.save_to_file(_TRADES_FILE_SV2_UPGRADED)
            # AU Gold suite (2026-04-24) — XAUUSD-only paper trades
            self._trade_tracker_au1.save_to_file(_TRADES_FILE_AU1)
            self._trade_tracker_au2.save_to_file(_TRADES_FILE_AU2)
            self._trade_tracker_au3.save_to_file(_TRADES_FILE_AU3)
            self._trade_tracker_au4.save_to_file(_TRADES_FILE_AU4)
            self._trade_tracker_au5.save_to_file(_TRADES_FILE_AU5)
        except Exception as exc:
            # Saves run on every cycle; failure means trade state can't
            # survive restart. Surface it loudly so the operator can free
            # disk / fix permissions / etc.
            logger.error("[SAVE] _save_trades failed: %s", exc, exc_info=True)
            if hasattr(self, "_health_alerts"):
                self._health_alerts.notify(
                    "warning", "Journal",
                    f"Failed to save active trades to disk:\n\n{exc}\n\n"
                    "Trades remain tracked in memory but won't survive a "
                    "TAKUMI restart. Check disk space and file permissions "
                    "in the data/ directory.",
                    dedup_key="save_trades_fail",
                    cooldown=600,
                )

    def _clear_alerts(self) -> None:
        """Clear all trend alert notifications."""
        self._alert_history.clear()
        self._alert_label.setText("No 4-timeframe alignments detected.")
        self._alert_label.setStyleSheet(
            "color: #888888; padding: 4px 6px; background: #ffffff;"
            " a { color: #4a6fa5; text-decoration: none; font-weight: bold; }"
        )
        self._save_alert_history()

    def _clear_closed_trades(self) -> None:
        """Clear the closed trades panel and suppress until new trades close."""
        try:
            self._closed_panel.setText("No closed trades yet.")
            self._closed_panel.setStyleSheet(
                "color: #888888; padding: 4px 6px; background: #ffffff;"
            )
            self._closed_stats_label.setText("")
            # Remember current journal size — only show again when NEW trades close
            self._closed_trades_hidden_count = len(self._paper_trader.journal)
            self._closed_trades_suppressed = True
        except Exception:
            logger.exception("Error clearing closed trades display")

    def _save_alert_history(self) -> None:
        """Persist alert history to disk."""
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        try:
            data = list(self._alert_history)
            _ALERTS_FILE.write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception:
            logger.exception("Failed to save alert history")

    def _load_alert_history(self) -> None:
        """Restore alert history from disk."""
        if not _ALERTS_FILE.exists():
            return
        try:
            data = json.loads(_ALERTS_FILE.read_text(encoding="utf-8"))
            for entry in data:
                self._alert_history.append(entry)
            logger.info("Restored %d alert history entries", len(data))
        except Exception:
            logger.exception("Failed to load alert history")
