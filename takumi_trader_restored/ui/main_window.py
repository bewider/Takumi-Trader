"""Main application window: table, alert list, compact toggle, status bar."""

from __future__ import annotations

import json
import logging
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
from takumi_trader.ui.settings_dialog import SettingsDialog, load_settings

logger = logging.getLogger(__name__)

# Number of columns: Pair/Currency + Range% + one per timeframe
_NUM_TF = len(TIMEFRAME_LABELS)  # 4: M1, M5, M15, H1
_TOTAL_COLS = 2 + _NUM_TF        # 6: Pair | Range | M1 | M5 | M15 | H1
_RANGE_COL = 1                   # Column index for the Range% column
_TF_COL_OFFSET = 2               # Timeframe columns start at index 2

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

# Paper trade exit: same spread threshold as backtester._check_exit()
# Exit when base/quote composite spread drops below this value.
_PAPER_EXIT_SPREAD_THRESHOLD = 4.0

# Per-TF alert thresholds for currency strength
# Matched to backtest-optimized threshold_m1=5.5 across all 27 pairs
_ALERT_THRESHOLDS: dict[str, float] = {
    "M1": 5.5,
    "M5": 5.0,
    "M15": 4.5,
    "H1": 4.0,
}

# Minimum composite divergence spread to trigger alert (base_ccy - quote_ccy)
_MIN_DIVERGENCE_SPREAD = 12.0


def _cell_colors(score: float) -> tuple[str, str]:
    """Return (bg_color, text_color) for a given pair score."""
    for threshold, bg, fg in _CELL_STYLES:
        if score >= threshold:
            return bg, fg
    return _EXTREME_NEG


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
                tf: deque(maxlen=5) for tf in TIMEFRAME_LABELS
            }

        # Score history for individual currencies (momentum arrows)
        _CCY_TFS = list(TIMEFRAME_LABELS) + ["H4"]
        self._ccy_score_history: dict[str, dict[str, deque]] = {}
        for ccy in CURRENCIES:
            self._ccy_score_history[ccy] = {
                tf: deque(maxlen=5) for tf in _CCY_TFS
            }

        # Alert history (most recent first, capped at _MAX_ALERT_HISTORY)
        self._alert_history: deque[str] = deque(maxlen=_MAX_ALERT_HISTORY)
        self._load_alert_history()

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

        # Trade tracking + exit engine
        self._trade_tracker = TradeTracker(max_trades=7)
        self._trade_tracker.load_from_file(_TRADES_FILE)
        self._exit_engine = ExitEngine()
        self._alert_perf = AlertPerformanceTracker()
        self._alert_perf.load_active(_ACTIVE_PERF_FILE)
        self._paper_trader = PaperTrader(
            trade_tracker=self._trade_tracker,
            journal_path=_PAPER_TRADES_FILE,
        )
        self._paper_trader.load_journal()
        self._prev_m1_pair_scores: dict[str, float] = {}  # for momentum stall detection
        self._last_composite_scores: dict[str, float] = {}  # latest composite ccy scores
        # Cache latest alert candidates so TRACK links can access direction info
        self._latest_alert_candidates: dict[str, tuple[str, dict[str, float]]] = {}
        # Cache latest close prices per pair from M1 data
        self._latest_close_prices: dict[str, float] = {}
        # Cache latest conviction results
        self._latest_conviction: dict[str, ConvictionResult] = {}
        # Track last exit alert time per pair to avoid spamming
        self._last_exit_alert_time: dict[str, float] = {}
        self._exit_alert_cooldown = 60  # seconds between exit notifications for same pair
        self._gap_fill_done = False  # one-time gap fill on first data cycle

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

        # Worker
        self._worker = MT5Worker(poll_interval=1.0)
        self._worker.data_ready.connect(self._on_data)
        self._worker.connection_status.connect(self._on_connection_status)
        self._worker.start()

        # ── cTrader auto-trading bridge ──
        self._ctrader_config = settings  # reuse already-loaded settings dict
        self._ctrader_bridge: CTraderBridge | None = None
        self._ctrader_pos_mgr: CTraderPositionManager | None = None
        if _CTRADER_AVAILABLE:
            try:
                self._ctrader_bridge = CTraderBridge(self)
                self._ctrader_pos_mgr = CTraderPositionManager()
                self._ctrader_pos_mgr.load(_CTRADER_POS_FILE)
                self._ctrader_bridge.connected.connect(self._on_ctrader_status)
                self._ctrader_bridge.order_opened.connect(self._on_ctrader_order_opened)
                self._ctrader_bridge.order_closed.connect(self._on_ctrader_order_closed)
                self._ctrader_bridge.order_error.connect(self._on_ctrader_order_error)
                self._ctrader_bridge.positions_synced.connect(self._on_ctrader_positions_synced)
                if settings.get("ctrader_enabled"):
                    self._ctrader_bridge.start(settings)
            except Exception as exc:
                logger.error("Failed to init cTrader bridge: %s", exc)
                self._ctrader_bridge = None
                self._ctrader_pos_mgr = None

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

        # Apply compact mode if previously enabled
        if settings.get("compact_mode", False):
            self._toggle_compact()

        # Auto-open Performance and Backtest windows on startup (after event loop starts)
        from PyQt6.QtCore import QTimer
        QTimer.singleShot(500, self._open_performance)
        QTimer.singleShot(800, self._open_backtest)

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
            "QToolBar { spacing: 2px; padding: 1px; }"
            " QToolBar::separator { width: 4px; }"
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
        self._table.setHorizontalHeaderLabels(["Pair", "Range"] + TIMEFRAME_LABELS)
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

        # ── Currency Strength Table: Currency | M1 | M5 | M15 | H1 | H4 ──
        _CCY_COLS = 6  # Currency + M1 + M5 + M15 + H1 + H4
        _CCY_TF_LABELS = list(TIMEFRAME_LABELS) + ["H4"]
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
        # Row height scales with font: base 24px at size 10, +2px per point
        row_h = max(20, 24 + (self._font_size - 10) * 2)

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
            QHeaderView::section { background: #e0e0e0; color: #333333;
                                   border: 1px solid #d0d0d0; padding: 4px;
                                   font-weight: bold; }
            QStatusBar { background: #e8e8e8; color: #555555; }
            """
        )

    # ── Data Handling ─────────────────────────────────────────────────

    def _on_data(self, result: CalculationResult) -> None:
        """Handle new data from the worker thread."""
        # One-time gap fill for active perf alerts after MT5 connects
        if not self._gap_fill_done and result.connected:
            self._gap_fill_done = True
            if self._alert_perf.get_active_count() > 0:
                self._alert_perf.fill_gaps_from_mt5()

        alert_candidates: dict[str, tuple[str, dict[str, float]]] = {}

        # ── Update pair scores table ──
        for row, pair in enumerate(DISPLAY_PAIRS):
            tf_scores: dict[str, float] = {}
            for col_idx, tf in enumerate(TIMEFRAME_LABELS):
                col = col_idx + _TF_COL_OFFSET
                tf_result = result.timeframes.get(tf)
                if tf_result is None:
                    continue
                score = tf_result.pair_scores.get(pair)
                if score is None:
                    continue

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

        # ── Update currency strength table (M1, M5, M15, H1 + H4) ──
        for row, ccy in enumerate(CURRENCIES):
            # M1, M5, M15, H1 — columns 1-4
            for col_idx, tf in enumerate(TIMEFRAME_LABELS):
                col = col_idx + 1  # TFs start at column 1 in currency table
                tf_result = result.timeframes.get(tf)
                if tf_result is None:
                    continue
                score = tf_result.currency_scores.get(ccy)
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

                bg, fg = _cell_colors(score)
                text = f"{score:+.1f} {arrow}"

                item = self._ccy_table.item(row, col)
                if item:
                    item.setText(text)
                    item.setBackground(QColor(bg))
                    item.setForeground(QColor(fg))

            # H4 — column 5 (last column in currency table)
            h4_col = 5
            htf_data = result.htf_regimes.get(ccy, {}).get("H4")
            if htf_data:
                _regime_str, h4_strength = htf_data
                self._ccy_score_history[ccy]["H4"].append(h4_strength)

                h4_history = self._ccy_score_history[ccy]["H4"]
                if len(h4_history) >= 5:
                    delta = h4_history[-1] - h4_history[0]
                    if delta > 0.3:
                        arrow, _ = _ARROW_UP
                    elif delta < -0.3:
                        arrow, _ = _ARROW_DOWN
                    else:
                        arrow, _ = _ARROW_FLAT
                else:
                    arrow, _ = _ARROW_FLAT

                bg, fg = _cell_colors(h4_strength)
                text = f"{h4_strength:+.1f} {arrow}"

                item = self._ccy_table.item(row, h4_col)
                if item:
                    item.setText(text)
                    item.setBackground(QColor(bg))
                    item.setForeground(QColor(fg))

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

        # ── Standard entries: all 4 TFs at extreme thresholds ──
        _accel_entry_types: dict[str, str] = {}

        for pair in list(DISPLAY_PAIRS):
            if pair in alert_candidates:
                continue
            base, quote = pair[:3], pair[3:]
            base_sc = ccy_per_tf.get(base, {})
            quote_sc = ccy_per_tf.get(quote, {})
            if len(base_sc) < _NUM_TF or len(quote_sc) < _NUM_TF:
                continue

            base_strong_all = all(
                base_sc.get(tf, 0) >= _ALERT_THRESHOLDS[tf] for tf in TIMEFRAME_LABELS
            )
            quote_weak_all = all(
                quote_sc.get(tf, 0) <= -_ALERT_THRESHOLDS[tf] for tf in TIMEFRAME_LABELS
            )
            base_weak_all = all(
                base_sc.get(tf, 0) <= -_ALERT_THRESHOLDS[tf] for tf in TIMEFRAME_LABELS
            )
            quote_strong_all = all(
                quote_sc.get(tf, 0) >= _ALERT_THRESHOLDS[tf] for tf in TIMEFRAME_LABELS
            )

            spread = sum(base_sc.values()) / _NUM_TF - sum(quote_sc.values()) / _NUM_TF
            tf_display = {
                tf: base_sc.get(tf, 0.0) - quote_sc.get(tf, 0.0)
                for tf in TIMEFRAME_LABELS
            }

            if base_strong_all and quote_weak_all and spread >= _MIN_DIVERGENCE_SPREAD:
                alert_candidates[pair] = ("BUY", tf_display)
                _accel_entry_types[pair] = "standard"
            elif base_weak_all and quote_strong_all and (-spread) >= _MIN_DIVERGENCE_SPREAD:
                alert_candidates[pair] = ("SELL", tf_display)
                _accel_entry_types[pair] = "standard"

        # ── ACCEL entries: catch momentum shifts before full 4-TF alignment ──
        if hasattr(result, 'accel_candidates') and result.accel_candidates:
            for pair, (accel_dir, accel_reason) in result.accel_candidates.items():
                if pair not in alert_candidates:
                    base, quote = pair[:3], pair[3:]
                    base_sc = ccy_per_tf.get(base, {})
                    quote_sc = ccy_per_tf.get(quote, {})
                    tf_display = {
                        tf: base_sc.get(tf, 0.0) - quote_sc.get(tf, 0.0)
                        for tf in TIMEFRAME_LABELS
                    }
                    alert_candidates[pair] = (accel_dir, tf_display)
                    _accel_entry_types[pair] = "acceleration"

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
            if _struct_data:
                _pip = _struct_data.get("pip", 0.0001)
                from takumi_trader.core.pair_algo_settings import get_pair_settings as _gps
                _ps = _gps(pair)
                if _ps and result.h1_atr.get(pair, 0) > 0:
                    _tp_pips_filter = _ps.get("tp_atr", 0.5) * result.h1_atr[pair] / _pip

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
            )
            conviction_results[pair] = conv

        # Only fire FULL alerts with sound; DIMMED show quietly
        full_candidates: dict[str, tuple[str, dict[str, float]]] = {}
        for pair, (direction, scores) in alert_candidates.items():
            conv = conviction_results.get(pair)
            if conv and conv.tier != "SUPPRESSED":
                full_candidates[pair] = (direction, scores)

        # Cache for TRACK button usage
        self._latest_alert_candidates = dict(alert_candidates)
        self._latest_conviction = conviction_results
        if result.close_prices:
            self._latest_close_prices.update(result.close_prices)

        # Fire notifications only for FULL tier alerts
        fire_candidates: dict[str, tuple[str, dict[str, float]]] = {
            p: v for p, v in full_candidates.items()
            if conviction_results.get(p, ConvictionResult()).tier == "FULL"
        }
        fired = self._alert_mgr.check_and_fire(fire_candidates)
        self._update_alert_display(full_candidates, fired, conviction_results)

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
        try:
            _jst_now_pt = datetime.now(_jst())
            _jst_hm_pt = _jst_now_pt.hour * 60 + _jst_now_pt.minute
            _paper_trade_allowed = not (301 <= _jst_hm_pt <= 419)  # 5:01–6:59 blocked
        except Exception:
            _paper_trade_allowed = True

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
                    continue
                direction, _ = full_candidates[pair]
                entry_price = result.close_prices.get(pair, 0.0)
                if entry_price <= 0:
                    continue
                conv = conviction_results.get(pair)
                _etype = _accel_entry_types.get(pair, "standard")
                pt = self._paper_trader.open_paper_trade(
                    pair=pair,
                    direction=direction,
                    entry_price=entry_price,
                    composite_scores=result.composite_scores,
                    conviction=conv.conviction if conv else 0,
                    session=get_current_session(),
                    h1_atr=result.h1_atr.get(pair, 0.0),
                    entry_type=_etype,
                )
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

        # ── cTrader auto-open on fired alerts ──
        if self._ctrader_bridge and self._ctrader_pos_mgr:
            # No new trades 5:01–6:59 JST — only tracking/recording
            try:
                _jst_now = datetime.now(_jst())
                _jst_hm = _jst_now.hour * 60 + _jst_now.minute
                _ctrader_trade_allowed = not (301 <= _jst_hm <= 419)  # 5:01–6:59 blocked
            except Exception:
                _ctrader_trade_allowed = True  # fallback: allow trades

            if (
                _ctrader_trade_allowed
                and self._ctrader_config.get("ctrader_enabled")
                and self._ctrader_config.get("ctrader_auto_open")
                and self._ctrader_bridge.is_connected
            ):
                max_pos = self._ctrader_config.get("ctrader_max_positions", 3)
                lot_size = self._ctrader_config.get("ctrader_lot_size", 0.01)
                for pair in fired:
                    if pair not in full_candidates:
                        continue
                    direction, _ = full_candidates[pair]
                    if self._ctrader_pos_mgr.has_position(pair, direction):
                        continue
                    if self._ctrader_pos_mgr.open_count >= max_pos:
                        continue
                    self._ctrader_bridge.open_order(pair, direction, lot_size)

        # ── MT5 auto-open on fired alerts ──
        if self._mt5_config.get("mt5_trading_enabled") and self._mt5_config.get("mt5_auto_open", True):
            try:
                _jst_now_mt5 = datetime.now(_jst())
                _jst_hm_mt5 = _jst_now_mt5.hour * 60 + _jst_now_mt5.minute
                _mt5_trade_allowed = not (301 <= _jst_hm_mt5 <= 419)
            except Exception:
                _mt5_trade_allowed = True

            if _mt5_trade_allowed:
                max_pos_mt5 = self._mt5_config.get("mt5_max_positions", 5)
                risk_pct = self._mt5_config.get("mt5_risk_pct", 1.0)
                for pair in fired:
                    if pair not in full_candidates:
                        continue
                    # News filter
                    if self._news_filter.loaded and self._news_filter.is_blackout(pair, time.time()):
                        continue
                    # Duplicate check (1 per pair)
                    if self._mt5_pos_mgr.has_position(pair):
                        continue
                    # Max positions check
                    if self._mt5_pos_mgr.open_count >= max_pos_mt5:
                        continue

                    direction, _ = full_candidates[pair]
                    entry_price = result.close_prices.get(pair, 0.0)
                    if entry_price <= 0:
                        continue

                    # Calculate SL/TP from pair settings + ATR
                    from takumi_trader.core.pair_algo_settings import get_pair_settings
                    from takumi_trader.core.trade_tracker import pip_value
                    ps = get_pair_settings(pair)
                    h1_atr = result.h1_atr.get(pair, 0.0)
                    pip = pip_value(pair)

                    if ps and h1_atr > 0:
                        sl_pips = round(ps.get("sl_atr", 0.3) * h1_atr / pip, 1)
                        tp_pips = round(ps.get("tp_atr", 1.0) * h1_atr / pip, 1)
                    elif ps:
                        sl_pips = ps.get("sl_pips", 10.0)
                        tp_pips = ps.get("tp_pips", 20.0)
                    else:
                        sl_pips = 10.0
                        tp_pips = 20.0

                    if direction == "BUY":
                        sl_price = entry_price - sl_pips * pip
                        tp_price = entry_price + tp_pips * pip
                    else:
                        sl_price = entry_price + sl_pips * pip
                        tp_price = entry_price - tp_pips * pip

                    self._mt5_trader.open_order(
                        pair=pair,
                        direction=direction,
                        sl_price=sl_price,
                        tp_price=tp_price,
                        sl_pips=sl_pips,
                        tp_pips=tp_pips,
                        risk_pct=risk_pct,
                    )

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

        # Update status timestamp + session
        now = datetime.now(_jst()).strftime("%H:%M:%S")
        self._status_label.setText(f"Last update: {now}  |  Poll: 1s")
        if result.session_label:
            self._session_label.setText(f"\U0001f30d {result.session_label}")

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
            html = "<br>".join(self._active_trade_html_parts)
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
            html = "<br>".join(self._alert_history)
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

        # Post-close observation (4h MAX-MFE/MAX-MAE tracking)
        if self._paper_trader.post_close_count > 0:
            self._paper_trader.post_close_cycle(
                result.high_prices, result.low_prices, result.close_prices,
            )

        active = self._trade_tracker.active_trades

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

        for pair, trade in active.items():
            # Update price from close_prices
            if pair in result.close_prices:
                self._trade_tracker.update_price(pair, result.close_prices[pair])

            # ── Update Peak/Worst using M1 High/Low (matches backtester) ──
            h_price = result.high_prices.get(pair)
            l_price = result.low_prices.get(pair)
            if h_price is not None and l_price is not None:
                from takumi_trader.core.trade_tracker import pip_value as _pv
                _pip = _pv(pair)
                if trade.direction == "BUY":
                    _best = (h_price - trade.entry_price) / _pip
                    _worst = (l_price - trade.entry_price) / _pip
                else:
                    _best = (trade.entry_price - l_price) / _pip
                    _worst = (trade.entry_price - h_price) / _pip
                if _best > trade.peak_pnl_pips:
                    trade.peak_pnl_pips = _best
                if _worst < trade.worst_pnl_pips:
                    trade.worst_pnl_pips = _worst

            # Get flow bias
            flow_state = result.flow_states.get(pair)
            flow_bias = flow_state.flow_bias if flow_state else None

            # Run exit evaluation
            m1_score = current_m1_scores.get(pair)
            prev_m1_score = self._prev_m1_pair_scores.get(pair)

            exit_result = self._exit_engine.evaluate(
                trade=trade,
                current_ccy_scores=composite,
                m1_pair_score=m1_score,
                m1_pair_score_prev=prev_m1_score,
                adr_consumed_pct=adr_consumed_map.get(pair, 0.0),
                flow_bias=flow_bias,
            )

            # Fire exit notification to alert history if CLOSE or URGENT
            if trade.exit_urgency in ("CLOSE", "URGENT"):
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
                    # Also send toast + sound for URGENT
                    if urg == "URGENT":
                        from takumi_trader.core.alerts import send_toast_notification, play_sound
                        send_toast_notification(
                            "TAKUMI Trader",
                            f"\u26a0 URGENT EXIT: {trade.direction} {pair} — {trade.pnl_pips:+.1f}p — {reasons_txt}",
                        )
                        if self._alert_mgr.sound_enabled and self._alert_mgr.sound_file:
                            play_sound(self._alert_mgr.sound_file)

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

            # ── MT5 spread-collapse exit ──
            if htf_composite and self._mt5_pos_mgr.has_position(pair):
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

            # ── Paper trade: backtester-style spread exit ──
            # Only exit when base-quote HTF composite spread collapses (same as
            # backtester._check_exit with use_htf_exit=True).
            # Uses M5+M15+H1 scores (excluding M1) — same as backtester.
            # Skip spread check for first 2 minutes to let the trade breathe
            # (matches backtester which checks exit starting from bar AFTER entry).
            _trade_age_s = time.time() - trade.entry_time if trade.entry_time > 0 else 999
            if trade.is_paper and htf_composite and _trade_age_s >= 120:
                _base_ccy, _quote_ccy = pair[:3], pair[3:]
                _b_sc = htf_composite.get(_base_ccy, 0.0)
                _q_sc = htf_composite.get(_quote_ccy, 0.0)
                if trade.direction == "BUY":
                    _spread = _b_sc - _q_sc
                else:
                    _spread = _q_sc - _b_sc

                _exit_reason = ""
                if _spread < 0:
                    _exit_reason = "direction_flipped"
                elif _spread < _PAPER_EXIT_SPREAD_THRESHOLD:
                    _exit_reason = "spread_collapsed"

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

            entry = (
                f'<span style="font-size:9pt; color:#666;">[{_entry_str}]</span> '
                f'<span style="font-size:10pt; color:{dir_color}; font-weight:bold;">'
                f'\U0001f4c8 {trade.direction} {pair}</span>'
                f'<span style="font-size:11pt; color:{pnl_color}; font-weight:bold;"> '
                f'{trade.pnl_pips:+.1f}p</span>'
                f'{urg_badge}'
                f'<span style="font-size:9pt; color:#888;">  '
                f'{sl_tp_txt}'
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
        """Render the closed trades panel from paper trade journal."""
        journal = self._paper_trader.journal
        if not journal:
            self._closed_panel.setText("No closed trades yet.")
            self._closed_panel.setStyleSheet(
                "color: #888888; padding: 4px 6px; background: #ffffff;"
            )
            self._closed_stats_label.setText("")
            return

        # Summary stats in the header
        stats = self._paper_trader.get_stats()
        total_pnl = stats["total_pnl"]
        pnl_c = "#1b8a2a" if total_pnl >= 0 else "#c62828"
        self._closed_stats_label.setText(
            f'P/L: {total_pnl:+.1f}p | '
            f'TP:{stats["tp_hits"]} SL:{stats["sl_hits"]}'
        )
        self._closed_stats_label.setStyleSheet(
            f"color: {pnl_c}; font-weight: bold; padding: 0 8px; font-size: 9pt;"
        )

        # Render individual trades (newest first, max 50)
        html_parts: list[str] = []
        for rec in reversed(journal[-50:]):
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

            # Time string
            time_str = rec.entry_time_str[-8:] if rec.entry_time_str else "—"

            entry = (
                f'<span style="font-size:9pt; color:#666;">[{time_str}]</span> '
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

            html_parts.append(entry)

        html = "<br>".join(html_parts)
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
        """Update status bar with connection info."""
        if connected:
            self._status_dot.setStyleSheet("color: #2e7d32;")
            self._status_label.setText(message)
            self.statusBar().setStyleSheet("background: #e8e8e8; color: #555555;")
        else:
            self._status_dot.setStyleSheet("color: #d32f2f;")
            self._status_label.setText(message)
            self.statusBar().setStyleSheet("background: #ffcdd2; color: #b71c1c;")

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
        # Center on primary monitor
        primary = _QGA.primaryScreen()
        if primary:
            geo = primary.availableGeometry()
            dialog.move(
                geo.x() + (geo.width() - dialog.width()) // 2,
                geo.y() + (geo.height() - dialog.height()) // 2,
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

    def _open_performance(self) -> None:
        """Open the alert performance statistics dialog (modeless, independent)."""
        from takumi_trader.ui.performance_dialog import PerformanceDialog
        active_count = self._alert_perf.get_active_count()
        bt_file = _DATA_DIR / "backtest_outcomes.json"
        # No parent → independent window, not pinned; show() → non-blocking
        dlg = PerformanceDialog(
            None,
            outcomes_file=_OUTCOMES_FILE,
            active_count=active_count,
            backtest_file=bt_file if bt_file.exists() else None,
            paper_trades_file=_PAPER_TRADES_FILE,
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

    def _on_ctrader_status(self, is_connected: bool, msg: str) -> None:
        """Handle cTrader connection status changes."""
        if is_connected:
            self._ctrader_status_label.setText("cT: \u25cf Connected")
            self._ctrader_status_label.setStyleSheet(
                "color: #2e7d32; padding: 0 4px; font-weight: bold;"
            )
            logger.info("cTrader connected: %s", msg)
        else:
            self._ctrader_status_label.setText("cT: \u25cb Disconnected")
            self._ctrader_status_label.setStyleSheet(
                "color: #c62828; padding: 0 4px;"
            )
            logger.warning("cTrader disconnected: %s", msg)

    def _on_ctrader_order_opened(self, pair: str, position_id: int, direction: str) -> None:
        """Handle confirmed order fill from cTrader."""
        price = self._latest_close_prices.get(pair, 0.0)
        lot_size = self._ctrader_config.get("ctrader_lot_size", 0.01)
        self._ctrader_pos_mgr.register_open(pair, direction, position_id, lot_size, price)
        self._ctrader_pos_mgr.save(_CTRADER_POS_FILE)

        # Also register in TradeTracker so exit engine monitors it
        if pair not in self._trade_tracker.active_trades:
            self._trade_tracker.open_trade(pair, direction, price)

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

        # Also close in TradeTracker
        if pair in self._trade_tracker.active_trades:
            self._trade_tracker.close_trade(pair)

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
        """Handle cTrader order error."""
        now_str = datetime.now(_jst()).strftime("%H:%M:%S")
        html = (
            f'<span style="font-size:9pt; color:#666;">[{now_str}]</span> '
            f'<span style="font-size:10pt; color:#d50000; font-weight:bold;">'
            f"\u274c cT ERROR</span> "
            f'<span style="font-size:9pt;">{pair}: {error}</span>'
        )
        self._alert_history.appendleft(html)
        self._refresh_alert_panel()
        logger.error("cTrader order error: %s — %s", pair, error)

    def _on_ctrader_positions_synced(self, positions: list) -> None:
        """Handle reconciled positions from cTrader."""
        self._ctrader_pos_mgr.reconcile(positions)
        self._ctrader_pos_mgr.save(_CTRADER_POS_FILE)
        logger.info("cTrader positions synced: %d open", len(positions))

    # ── MT5 trading signal handlers ──────────────────────────────

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

    def _on_filters_changed(self) -> None:
        """Handle filter toggle from toolbar — immediate recalculation."""
        # The next data cycle will pick up the new filter settings automatically
        # since we read from self._filter_toolbar.filter_settings each cycle.
        logger.info("Filter settings changed: %s", self._filter_toolbar.filter_settings)

    # ── Window geometry persistence ──────────────────────────────────

    def _save_geometry(self) -> None:
        """Save window position, size, and state to QSettings."""
        s = QSettings("TAKUMITrader", "TAKUMITrader")
        s.setValue("window/geometry", self.saveGeometry())

    def _restore_geometry(self) -> None:
        """Restore window position, size, and state from QSettings."""
        s = QSettings("TAKUMITrader", "TAKUMITrader")
        geometry = s.value("window/geometry")
        if geometry is not None:
            self.restoreGeometry(geometry)

    # ── Cleanup ───────────────────────────────────────────────────────

    def closeEvent(self, event) -> None:
        """Save geometry, persist trades/alerts, stop worker, and quit app on close."""
        self._save_geometry()
        self._save_trades()
        self._save_alert_history()
        self._alert_perf.save_active(_ACTIVE_PERF_FILE)
        # Stop cTrader bridge and persist positions
        if self._ctrader_bridge:
            self._ctrader_bridge.stop()
        if self._ctrader_pos_mgr:
            self._ctrader_pos_mgr.save(_CTRADER_POS_FILE)
        self._worker.stop()
        self._worker.wait(3000)
        # Close all independent windows (Performance, Backtest, etc.)
        from PyQt6.QtWidgets import QApplication
        for w in QApplication.topLevelWidgets():
            if w is not self:
                w.close()
        event.accept()

    # ── Persistence ────────────────────────────────────────────────────

    def _save_trades(self) -> None:
        """Persist active trades to disk."""
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._trade_tracker.save_to_file(_TRADES_FILE)

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
        """Clear the closed trades journal and panel."""
        try:
            if _PAPER_TRADES_FILE.exists():
                _PAPER_TRADES_FILE.unlink()
            # Reload empty journal and stop post-close watching
            self._paper_trader._journal.clear()
            self._paper_trader._post_close_watching.clear()
            self._closed_panel.setText("No closed trades yet.")
            self._closed_panel.setStyleSheet(
                "color: #888888; padding: 4px 6px; background: #ffffff;"
            )
            self._closed_stats_label.setText("")
            # Refresh Performance dialog if open
            self._refresh_perf_paper_tab()
        except Exception:
            logger.exception("Error clearing closed trades")

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
