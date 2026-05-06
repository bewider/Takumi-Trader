"""Alert Performance Statistics Dialog — MAE/MFE analysis (Stage 1)."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

from PyQt6.QtCore import Qt, QPointF, QRectF, QSettings, QByteArray
from PyQt6.QtGui import (
    QBrush,
    QColor,
    QFont,
    QFontMetrics,
    QGuiApplication,
    QLinearGradient,
    QPainter,
    QPainterPath,
    QPen,
)
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from takumi_trader.core.alert_performance import AlertOutcome, AlertPerformanceTracker
from takumi_trader.core.paper_trader import PaperTrader, PaperTradeRecord

# ── JST timezone (lazy, frozen-exe safe) ──────────────────────────
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


def _to_jst_str(unix_ts: float, fmt: str = "%m-%d %H:%M") -> str:
    """Convert a Unix timestamp to a JST-formatted string."""
    try:
        return datetime.fromtimestamp(unix_ts, tz=_jst()).strftime(fmt)
    except Exception:
        return "—"


# ── Sortable numeric table item ──────────────────────────────────
class _NumericItem(QTableWidgetItem):
    """QTableWidgetItem that sorts by numeric value, not string."""

    def __init__(self, display: str, sort_value: float) -> None:
        super().__init__(display)
        self._sort_value = sort_value

    def __lt__(self, other: QTableWidgetItem) -> bool:
        if isinstance(other, _NumericItem):
            return self._sort_value < other._sort_value
        return super().__lt__(other)


class EquityCurveWidget(QWidget):
    """Custom-painted equity curve chart — no external dependencies."""

    _BG = QColor("#fafafa")
    _GRID = QColor("#e8ecf0")
    _AXIS = QColor("#999999")
    _LINE_WIN = QColor("#1b8a2a")
    _LINE_LOSS = QColor("#c62828")
    _FILL_WIN = QColor(27, 138, 42, 35)      # green 14% alpha
    _FILL_LOSS = QColor(198, 40, 40, 25)      # red 10% alpha
    _DOT_WIN = QColor("#1b8a2a")
    _DOT_LOSS = QColor("#c62828")
    _BASELINE = QColor("#4a6fa5")
    _TEXT = QColor("#555555")
    _TITLE = QColor("#2a5a8a")

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._equity_points: list[float] = []   # cumulative equity values
        self._trade_results: list[bool] = []    # win/loss per trade
        self._start_capital = 1000.0
        self._risk_pct = 0.03                   # 3%
        self.setMinimumSize(280, 200)

    def set_data(
        self,
        records: list[PaperTradeRecord],
        start_capital: float = 1000.0,
        risk_pct: float = 0.03,
    ) -> None:
        """Compute equity curve from trade records."""
        self._start_capital = start_capital
        self._risk_pct = risk_pct
        self._equity_points = [start_capital]
        self._trade_results = []

        balance = start_capital
        for r in records:
            risk_amount = balance * risk_pct
            # R-multiple: pnl_pips / sl_pips (risk unit)
            if r.sl_pips > 0:
                r_mult = r.pnl_pips / r.sl_pips
            else:
                r_mult = r.pnl_pips / 10.0  # fallback
            pnl_dollars = risk_amount * r_mult
            balance += pnl_dollars
            self._equity_points.append(round(balance, 2))
            self._trade_results.append(r.is_win)

        self.update()

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        w = self.width()
        h = self.height()

        # Background
        painter.fillRect(0, 0, w, h, self._BG)

        # Margins
        ml, mr, mt, mb = 58, 18, 36, 32
        cw = w - ml - mr   # chart width
        ch = h - mt - mb   # chart height

        if cw < 40 or ch < 40:
            painter.end()
            return

        # Title
        title_font = QFont("Segoe UI", 11, QFont.Weight.Bold)
        painter.setFont(title_font)
        painter.setPen(self._TITLE)
        painter.drawText(QRectF(0, 6, w, 26), Qt.AlignmentFlag.AlignCenter, "Equity Curve")

        # Subtitle
        sub_font = QFont("Segoe UI", 8)
        painter.setFont(sub_font)
        painter.setPen(self._AXIS)
        painter.drawText(
            QRectF(0, 22, w, 16), Qt.AlignmentFlag.AlignCenter,
            f"Start: ${self._start_capital:,.0f}  |  Risk: {self._risk_pct*100:.0f}% per trade"
        )

        pts = self._equity_points
        if len(pts) < 2:
            painter.setFont(QFont("Segoe UI", 10))
            painter.setPen(self._AXIS)
            painter.drawText(
                QRectF(ml, mt, cw, ch), Qt.AlignmentFlag.AlignCenter,
                "No trades yet"
            )
            painter.end()
            return

        n = len(pts)
        y_min = min(pts) * 0.98
        y_max = max(pts) * 1.02
        if y_max == y_min:
            y_max = y_min + 1

        def to_x(i: int) -> float:
            return ml + (i / (n - 1)) * cw

        def to_y(val: float) -> float:
            return mt + ch - ((val - y_min) / (y_max - y_min)) * ch

        # Grid lines (5 horizontal)
        painter.setPen(QPen(self._GRID, 1, Qt.PenStyle.SolidLine))
        label_font = QFont("Segoe UI", 7)
        painter.setFont(label_font)
        fm = QFontMetrics(label_font)
        for i in range(6):
            frac = i / 5
            val = y_min + frac * (y_max - y_min)
            yy = to_y(val)
            painter.setPen(QPen(self._GRID, 1, Qt.PenStyle.DashLine))
            painter.drawLine(QPointF(ml, yy), QPointF(ml + cw, yy))
            # Y-axis labels
            painter.setPen(self._AXIS)
            label = f"${val:,.0f}"
            painter.drawText(QRectF(2, yy - 8, ml - 6, 16),
                             Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                             label)

        # Baseline (starting capital)
        base_y = to_y(self._start_capital)
        painter.setPen(QPen(self._BASELINE, 1, Qt.PenStyle.DotLine))
        painter.drawLine(QPointF(ml, base_y), QPointF(ml + cw, base_y))

        # Build path for the equity line
        path = QPainterPath()
        path.moveTo(to_x(0), to_y(pts[0]))
        for i in range(1, n):
            path.lineTo(to_x(i), to_y(pts[i]))

        # Fill area between line and baseline
        fill_path = QPainterPath(path)
        fill_path.lineTo(to_x(n - 1), base_y)
        fill_path.lineTo(to_x(0), base_y)
        fill_path.closeSubpath()

        final_above = pts[-1] >= self._start_capital
        painter.setBrush(QBrush(self._FILL_WIN if final_above else self._FILL_LOSS))
        painter.setPen(Qt.PenStyle.NoPen)
        painter.drawPath(fill_path)

        # Draw the equity line with gradient coloring
        line_color = self._LINE_WIN if final_above else self._LINE_LOSS
        painter.setPen(QPen(line_color, 2.2, Qt.PenStyle.SolidLine))
        painter.setBrush(Qt.BrushStyle.NoBrush)
        painter.drawPath(path)

        # Trade dots
        for i in range(1, n):
            cx = to_x(i)
            cy = to_y(pts[i])
            is_win = self._trade_results[i - 1] if i - 1 < len(self._trade_results) else True
            dot_color = self._DOT_WIN if is_win else self._DOT_LOSS
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(dot_color)
            painter.drawEllipse(QPointF(cx, cy), 3.5, 3.5)

        # X-axis trade numbers (show a few)
        painter.setFont(label_font)
        painter.setPen(self._AXIS)
        # Show labels at reasonable intervals
        step = max(1, (n - 1) // 8)
        for i in range(0, n, step):
            xx = to_x(i)
            painter.drawText(QRectF(xx - 16, mt + ch + 4, 32, 16),
                             Qt.AlignmentFlag.AlignCenter, str(i))
        # Always show last
        if (n - 1) % step != 0:
            xx = to_x(n - 1)
            painter.drawText(QRectF(xx - 16, mt + ch + 4, 32, 16),
                             Qt.AlignmentFlag.AlignCenter, str(n - 1))

        # X-axis label
        painter.drawText(QRectF(ml, mt + ch + 16, cw, 14),
                         Qt.AlignmentFlag.AlignCenter, "Trade #")

        # Final equity value label
        final_val = pts[-1]
        pnl_pct = (final_val - self._start_capital) / self._start_capital * 100
        final_color = self._LINE_WIN if final_val >= self._start_capital else self._LINE_LOSS
        val_font = QFont("Segoe UI", 9, QFont.Weight.Bold)
        painter.setFont(val_font)
        painter.setPen(QPen(final_color))
        fx = to_x(n - 1)
        fy = to_y(final_val)
        # Position label to left if near right edge
        label_text = f"${final_val:,.0f} ({pnl_pct:+.1f}%)"
        lbl_w = QFontMetrics(val_font).horizontalAdvance(label_text) + 8
        lx = fx - lbl_w - 4 if fx + lbl_w + 4 > ml + cw else fx + 6
        ly = fy - 16 if fy > mt + 20 else fy + 4
        painter.drawText(QRectF(lx, ly, lbl_w, 16),
                         Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                         label_text)

        # Chart border
        painter.setPen(QPen(self._GRID, 1))
        painter.setBrush(Qt.BrushStyle.NoBrush)
        painter.drawRect(QRectF(ml, mt, cw, ch))

        painter.end()


class PerformanceDialog(QWidget):
    """Independent window showing aggregate alert performance statistics."""

    def __init__(
        self,
        parent: QWidget | None = None,
        outcomes_file: Path | None = None,
        active_count: int = 0,
        backtest_file: Path | None = None,
        paper_trades_file: Path | None = None,
    ) -> None:
        # No parent → independent top-level window (not pinned with main)
        super().__init__(None)
        self._outcomes_file = outcomes_file
        self._backtest_file = backtest_file
        self._paper_trades_file = paper_trades_file
        self._active_count = active_count
        self._all_outcomes: list[AlertOutcome] = []
        self._bt_outcomes: list[AlertOutcome] = []
        self._paper_records: list[PaperTradeRecord] = []
        self.setWindowTitle("Alert Performance (MAE / MFE)")
        self.setWindowFlags(
            Qt.WindowType.Window
            | Qt.WindowType.WindowCloseButtonHint
            | Qt.WindowType.WindowMinMaxButtonsHint
        )
        self.setMinimumSize(780, 600)
        self._setup_ui()
        self._restore_layout()
        self._refresh()

    _SETTINGS_KEY = "PerformanceDialog"

    def _save_layout(self) -> None:
        """Save window geometry and splitter positions to QSettings."""
        s = QSettings("TakumiTrader", "PerformanceDialog")
        s.setValue("geometry", self.saveGeometry())
        if hasattr(self, "_paper_v_splitter"):
            s.setValue("paper_v_splitter", self._paper_v_splitter.saveState())
        if hasattr(self, "_paper_h_splitter"):
            s.setValue("paper_h_splitter", self._paper_h_splitter.saveState())
        s.setValue("active_tab", self._tabs.currentIndex())

    def _restore_layout(self) -> None:
        """Restore window geometry and splitter positions from QSettings."""
        s = QSettings("TakumiTrader", "PerformanceDialog")
        geo = s.value("geometry")
        if geo and isinstance(geo, QByteArray):
            self.restoreGeometry(geo)
        else:
            # First launch: maximize
            self.showMaximized()

        # Restore splitters after a short delay (widgets need to be laid out first)
        from PyQt6.QtCore import QTimer
        QTimer.singleShot(50, self._restore_splitters)

    def _restore_splitters(self) -> None:
        """Restore splitter states (called after layout is ready)."""
        s = QSettings("TakumiTrader", "PerformanceDialog")
        v_state = s.value("paper_v_splitter")
        if v_state and isinstance(v_state, QByteArray) and hasattr(self, "_paper_v_splitter"):
            self._paper_v_splitter.restoreState(v_state)
        h_state = s.value("paper_h_splitter")
        if h_state and isinstance(h_state, QByteArray) and hasattr(self, "_paper_h_splitter"):
            self._paper_h_splitter.restoreState(h_state)
        tab_idx = s.value("active_tab", 0, type=int)
        if 0 <= tab_idx < self._tabs.count():
            self._tabs.setCurrentIndex(tab_idx)

    def closeEvent(self, event) -> None:
        """Save layout on close."""
        self._save_layout()
        super().closeEvent(event)

    _TABLE_STYLE = """
        QTableWidget {
            background: white; border: 1px solid #e0e0e0;
            font-size: 11px; gridline-color: #f0f0f0;
        }
        QTableWidget::item { padding: 2px 6px; }
        QHeaderView::section {
            background: #f0f4f8; color: #4a6fa5; font-size: 10px;
            font-weight: bold; text-transform: uppercase;
            padding: 5px 6px; border: none;
            border-bottom: 2px solid #d0d8e0;
            border-right: 1px solid #e0e0e0;
        }
        QHeaderView::section:hover { background: #dce6f0; }
        QTableWidget::item:selected { background: #d6e4f5; color: #333; }
    """

    def _build_page(self) -> tuple[QWidget, QLabel, QLabel, QTableWidget, QComboBox]:
        """Build one performance page (summary + trades table) and return its widgets."""
        page = QWidget()
        page_layout = QVBoxLayout(page)
        page_layout.setContentsMargins(0, 6, 0, 0)

        # Pair filter row
        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("Pair:"))
        pair_combo = QComboBox()
        pair_combo.setMinimumWidth(110)
        filter_row.addWidget(pair_combo)
        filter_row.addStretch()
        page_layout.addLayout(filter_row)

        # Splitter: top = HTML summary, bottom = sortable trades table
        splitter = QSplitter(Qt.Orientation.Vertical)

        # Top: HTML summary in scroll area
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("QScrollArea { border: none; background: #fafafa; }")
        content_label = QLabel()
        content_label.setWordWrap(True)
        content_label.setTextFormat(Qt.TextFormat.RichText)
        content_label.setAlignment(Qt.AlignmentFlag.AlignTop)
        content_label.setFont(QFont("Segoe UI", 10))
        content_label.setStyleSheet("padding: 10px; background: #fafafa;")
        scroll.setWidget(content_label)
        splitter.addWidget(scroll)

        # Bottom: Recent trades table (sortable)
        table_container = QWidget()
        table_layout = QVBoxLayout(table_container)
        table_layout.setContentsMargins(0, 4, 0, 0)

        table_title = QLabel()
        table_title.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
        table_title.setStyleSheet("color: #2a5a8a; padding: 4px 0;")
        table_layout.addWidget(table_title)

        trades_table = QTableWidget()
        trades_table.setAlternatingRowColors(True)
        trades_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        trades_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        trades_table.setSortingEnabled(True)
        trades_table.verticalHeader().setVisible(False)
        trades_table.verticalHeader().setDefaultSectionSize(22)
        trades_table.setStyleSheet(self._TABLE_STYLE)
        table_layout.addWidget(trades_table)
        splitter.addWidget(table_container)

        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 7)
        page_layout.addWidget(splitter)

        return page, content_label, table_title, trades_table, pair_combo

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)

        # Button row
        btn_row = QHBoxLayout()
        btn_refresh = QPushButton("Refresh")
        btn_refresh.clicked.connect(self._refresh)
        btn_row.addWidget(btn_refresh)

        btn_clear = QPushButton("Clear History")
        btn_clear.clicked.connect(self._clear_history)
        btn_row.addWidget(btn_clear)

        btn_row.addStretch()
        layout.addLayout(btn_row)

        # Tab widget with two pages
        self._tabs = QTabWidget()

        # ── Live Trades tab ──
        live_page, self._content_label, self._table_title, self._trades_table, self._pair_combo = self._build_page()
        self._pair_combo.currentTextChanged.connect(self._on_pair_changed)
        self._tabs.addTab(live_page, "\U0001f4ca Live Trades")

        # ── Paper Trades tab ──
        # Layout: top row = [summary stats | equity chart], bottom = full-width table
        paper_page = QWidget()
        paper_layout = QVBoxLayout(paper_page)
        paper_layout.setContentsMargins(0, 6, 0, 0)

        # Pair filter row
        paper_filter_row = QHBoxLayout()
        paper_filter_row.addWidget(QLabel("Pair:"))
        self._paper_pair_combo = QComboBox()
        self._paper_pair_combo.setMinimumWidth(110)
        self._paper_pair_combo.currentTextChanged.connect(self._on_paper_pair_changed)
        paper_filter_row.addWidget(self._paper_pair_combo)
        paper_filter_row.addStretch()
        paper_layout.addLayout(paper_filter_row)

        # Vertical splitter: top = [summary + equity], bottom = trade table
        self._paper_v_splitter = QSplitter(Qt.Orientation.Vertical)

        # -- Top section: horizontal splitter [summary | equity chart] --
        self._paper_h_splitter = QSplitter(Qt.Orientation.Horizontal)

        # Summary stats (left)
        paper_scroll = QScrollArea()
        paper_scroll.setWidgetResizable(True)
        paper_scroll.setStyleSheet("QScrollArea { border: none; background: #fafafa; }")
        self._paper_content_label = QLabel()
        self._paper_content_label.setWordWrap(True)
        self._paper_content_label.setTextFormat(Qt.TextFormat.RichText)
        self._paper_content_label.setAlignment(Qt.AlignmentFlag.AlignTop)
        self._paper_content_label.setFont(QFont("Segoe UI", 10))
        self._paper_content_label.setStyleSheet("padding: 10px; background: #fafafa;")
        paper_scroll.setWidget(self._paper_content_label)
        self._paper_h_splitter.addWidget(paper_scroll)

        # Equity chart (right)
        self._equity_chart = EquityCurveWidget()
        self._equity_chart.setMinimumWidth(300)
        self._paper_h_splitter.addWidget(self._equity_chart)
        self._paper_h_splitter.setStretchFactor(0, 1)
        self._paper_h_splitter.setStretchFactor(1, 1)

        self._paper_v_splitter.addWidget(self._paper_h_splitter)

        # -- Bottom section: full-width trade table --
        table_container = QWidget()
        table_layout = QVBoxLayout(table_container)
        table_layout.setContentsMargins(0, 4, 0, 0)

        self._paper_table_title = QLabel()
        self._paper_table_title.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
        self._paper_table_title.setStyleSheet("color: #2a5a8a; padding: 4px 0;")
        table_layout.addWidget(self._paper_table_title)

        self._paper_trades_table = QTableWidget()
        self._paper_trades_table.setAlternatingRowColors(True)
        self._paper_trades_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._paper_trades_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._paper_trades_table.setSortingEnabled(True)
        self._paper_trades_table.verticalHeader().setVisible(False)
        self._paper_trades_table.verticalHeader().setDefaultSectionSize(22)
        self._paper_trades_table.setStyleSheet(self._TABLE_STYLE)
        table_layout.addWidget(self._paper_trades_table)

        self._paper_v_splitter.addWidget(table_container)
        self._paper_v_splitter.setStretchFactor(0, 4)
        self._paper_v_splitter.setStretchFactor(1, 6)

        # Auto-save splitter positions on drag
        self._paper_h_splitter.splitterMoved.connect(self._save_layout)
        self._paper_v_splitter.splitterMoved.connect(self._save_layout)

        paper_layout.addWidget(self._paper_v_splitter)
        self._tabs.addTab(paper_page, "\U0001f4dd Paper Trades")

        # ── Backtest tab ──
        bt_page, self._bt_content_label, self._bt_table_title, self._bt_trades_table, self._bt_pair_combo = self._build_page()
        self._bt_pair_combo.currentTextChanged.connect(self._on_bt_pair_changed)
        self._tabs.addTab(bt_page, "\U0001f504 Backtest")

        layout.addWidget(self._tabs)

        self.setStyleSheet("""
            QWidget { background: #f0f2f5; color: #333; }
            QPushButton { background: #4a6fa5; color: white; border: none;
                          padding: 6px 14px; border-radius: 4px; font-size: 11px; }
            QPushButton:hover { background: #5a83bf; }
            QComboBox { padding: 4px 8px; border: 1px solid #ccc; border-radius: 3px;
                        background: white; color: #333; font-size: 11px; }
            QComboBox QAbstractItemView { color: #333; background: white; }
            QLabel { color: #333; }
            QTabWidget::pane { border: 1px solid #d0d0d0; background: #f0f2f5; }
            QTabBar::tab { background: #e0e4e8; color: #555; padding: 8px 16px;
                           border: 1px solid #ccc; border-bottom: none;
                           border-top-left-radius: 4px; border-top-right-radius: 4px;
                           margin-right: 2px; font-size: 11px; font-weight: bold; }
            QTabBar::tab:selected { background: #f0f2f5; color: #1a3a5c;
                                    border-bottom: 2px solid #4a6fa5; }
            QTabBar::tab:hover { background: #d0d8e0; }
        """)

    def _refresh(self) -> None:
        # Load live outcomes
        self._all_outcomes = (
            AlertPerformanceTracker.load_history(self._outcomes_file)
            if self._outcomes_file else []
        )
        self._update_pair_combo(self._pair_combo, self._all_outcomes)
        self._render_live()

        # Load paper trades
        self._paper_records = self._load_paper_records()
        self._update_paper_pair_combo()
        self._render_paper()

        # Load backtest outcomes
        if self._backtest_file and self._backtest_file.exists():
            self._bt_outcomes = AlertPerformanceTracker.load_history(self._backtest_file)
        else:
            self._bt_outcomes = []
        self._update_pair_combo(self._bt_pair_combo, self._bt_outcomes)
        self._render_backtest()

    def _update_pair_combo(self, combo: QComboBox, outcomes: list[AlertOutcome]) -> None:
        pairs = sorted(set(o.pair for o in outcomes))
        current = combo.currentText()
        combo.blockSignals(True)
        combo.clear()
        combo.addItem("ALL")
        combo.addItems(pairs)
        if current and current in (["ALL"] + pairs):
            combo.setCurrentText(current)
        combo.blockSignals(False)

    def _on_pair_changed(self) -> None:
        self._render_live()

    def _on_bt_pair_changed(self) -> None:
        self._render_backtest()

    def _on_paper_pair_changed(self) -> None:
        self._render_paper()

    def _load_paper_records(self) -> list[PaperTradeRecord]:
        """Load paper trade records from JSON file."""
        if not self._paper_trades_file or not self._paper_trades_file.exists():
            return []
        try:
            import json
            data = json.loads(self._paper_trades_file.read_text(encoding="utf-8"))
            records = []
            for d in data:
                r = PaperTradeRecord()
                for k, v in d.items():
                    if hasattr(r, k):
                        setattr(r, k, v)
                records.append(r)
            return records
        except Exception:
            return []

    def _update_paper_pair_combo(self) -> None:
        pairs = sorted(set(r.pair for r in self._paper_records))
        current = self._paper_pair_combo.currentText()
        self._paper_pair_combo.blockSignals(True)
        self._paper_pair_combo.clear()
        self._paper_pair_combo.addItem("ALL")
        self._paper_pair_combo.addItems(pairs)
        if current and current in (["ALL"] + pairs):
            self._paper_pair_combo.setCurrentText(current)
        self._paper_pair_combo.blockSignals(False)

    def _render_paper(self) -> None:
        selected = self._paper_pair_combo.currentText()
        if selected and selected != "ALL":
            records = [r for r in self._paper_records if r.pair == selected]
        else:
            records = self._paper_records

        self._paper_content_label.setText(self._build_paper_html(records, selected))
        self._populate_paper_table(records)

        # Update equity curve (chronological order)
        sorted_records = sorted(records, key=lambda r: r.entry_time)
        self._equity_chart.set_data(sorted_records, start_capital=1000.0, risk_pct=0.03)

    def _build_paper_html(self, records: list[PaperTradeRecord], pair_filter: str) -> str:
        """Build HTML summary for paper trades."""
        p = [_CSS]
        title = f"Paper Trades: {pair_filter}" if pair_filter != "ALL" else "Paper Trades — Overall"
        p.append(f'<div class="header">{title}</div>')
        p.append(f'<div class="subtitle">{len(records)} completed paper trades</div>')

        if not records:
            p.append('<div class="empty">No paper trades yet. '
                     'Paper trades open automatically on FULL alerts with optimized SL/TP.</div>')
            return "".join(p)

        # Summary stats
        wins = [r for r in records if r.is_win]
        losses = [r for r in records if not r.is_win]
        total = len(records)
        total_pnl = sum(r.pnl_pips for r in records)
        wr = len(wins) / total * 100 if total else 0
        avg_win = sum(r.pnl_pips for r in wins) / len(wins) if wins else 0
        avg_loss = sum(r.pnl_pips for r in losses) / len(losses) if losses else 0
        avg_dur = sum(r.duration_minutes for r in records) / total if total else 0

        sl_hits = sum(1 for r in records if r.close_reason == "sl_hit")
        tp_hits = sum(1 for r in records if r.close_reason == "tp_hit")
        sig_exits = sum(1 for r in records if r.close_reason == "signal_exit")

        pnl_color = "#1b8a2a" if total_pnl >= 0 else "#c62828"

        p.append('<div class="section-title">Summary</div>')
        p.append('<table class="stats-grid"><tr>')
        p.append(f'<td class="stat-card"><div class="stat-value">{total}</div>'
                 f'<div class="stat-label">Total Trades</div></td>')
        p.append(f'<td class="stat-card"><div class="stat-value">{wr:.1f}%</div>'
                 f'<div class="stat-label">Win Rate ({len(wins)}W / {len(losses)}L)</div></td>')
        p.append(f'<td class="stat-card"><div class="stat-value" style="color:{pnl_color};">'
                 f'{total_pnl:+.1f}p</div><div class="stat-label">Total P/L</div></td>')
        p.append(f'<td class="stat-card"><div class="stat-value">{avg_dur:.0f}m</div>'
                 f'<div class="stat-label">Avg Duration</div></td>')
        p.append('</tr></table>')

        p.append('<div class="section-title">Exit Breakdown</div>')
        p.append('<table class="stats-grid"><tr>')
        p.append(f'<td class="stat-card"><div class="stat-value" style="color:#1b8a2a;">'
                 f'{tp_hits}</div><div class="stat-label">TP Hits</div></td>')
        p.append(f'<td class="stat-card"><div class="stat-value" style="color:#c62828;">'
                 f'{sl_hits}</div><div class="stat-label">SL Hits</div></td>')
        p.append(f'<td class="stat-card"><div class="stat-value" style="color:#e65100;">'
                 f'{sig_exits}</div><div class="stat-label">Signal Exits</div></td>')
        p.append(f'<td class="stat-card"><div class="stat-value" style="color:#1b8a2a;">'
                 f'{avg_win:+.1f}p</div><div class="stat-label">Avg Win</div></td>')
        p.append(f'<td class="stat-card"><div class="stat-value" style="color:#c62828;">'
                 f'{avg_loss:+.1f}p</div><div class="stat-label">Avg Loss</div></td>')
        p.append('</tr></table>')

        # Entry type breakdown (Standard vs Acceleration)
        std_trades = [r for r in records if getattr(r, 'entry_type', 'standard') == 'standard']
        accel_trades = [r for r in records if getattr(r, 'entry_type', 'standard') == 'acceleration']
        if accel_trades:  # Only show if there are acceleration trades
            p.append('<div class="section-title">Entry Type Breakdown</div>')
            p.append('<table style="width:100%; border-collapse:collapse; font-size:11px;">')
            p.append('<tr style="background:#f0f4f8; color:#4a6fa5; font-weight:bold;">'
                     '<td style="padding:4px 8px;">Type</td>'
                     '<td>Trades</td><td>WR</td><td>Total P/L</td>'
                     '<td>Avg P/L</td><td>TP</td><td>SL</td><td>Signal</td></tr>')
            for label, group, color in [
                ("Standard", std_trades, "#2a5a8a"),
                ("\u26a1 Acceleration", accel_trades, "#ff9800"),
            ]:
                if not group:
                    continue
                gw = sum(1 for r in group if r.is_win)
                gt = len(group)
                gpnl = sum(r.pnl_pips for r in group)
                gwr = gw / gt * 100 if gt else 0
                gavg = gpnl / gt if gt else 0
                gtp = sum(1 for r in group if r.close_reason == "tp_hit")
                gsl = sum(1 for r in group if r.close_reason == "sl_hit")
                gsig = sum(1 for r in group if r.close_reason == "signal_exit")
                gc = "#1b8a2a" if gpnl >= 0 else "#c62828"
                ac = "#1b8a2a" if gavg >= 0 else "#c62828"
                p.append(f'<tr><td style="padding:3px 8px; font-weight:bold; color:{color};">'
                         f'{label}</td>'
                         f'<td>{gt}</td><td>{gwr:.0f}%</td>'
                         f'<td style="color:{gc};">{gpnl:+.1f}p</td>'
                         f'<td style="color:{ac};">{gavg:+.1f}p</td>'
                         f'<td>{gtp}</td><td>{gsl}</td><td>{gsig}</td></tr>')
            p.append('</table>')

        # Per-pair breakdown (only for ALL view)
        if pair_filter == "ALL" and total > 1:
            from collections import defaultdict
            pair_stats: dict[str, list] = defaultdict(list)
            for r in records:
                pair_stats[r.pair].append(r)

            p.append('<div class="section-title">Per-Pair Performance</div>')
            p.append('<table style="width:100%; border-collapse:collapse; font-size:11px;">')
            p.append('<tr style="background:#f0f4f8; color:#4a6fa5; font-weight:bold;">'
                     '<td style="padding:4px 8px;">Pair</td>'
                     '<td>Trades</td><td>WR</td><td>Total P/L</td>'
                     '<td>TP</td><td>SL</td><td>Signal</td></tr>')
            for pair_name in sorted(pair_stats.keys()):
                recs = pair_stats[pair_name]
                pw = sum(1 for r in recs if r.is_win)
                ptotal = len(recs)
                ppnl = sum(r.pnl_pips for r in recs)
                pwr = pw / ptotal * 100 if ptotal else 0
                ptp = sum(1 for r in recs if r.close_reason == "tp_hit")
                psl = sum(1 for r in recs if r.close_reason == "sl_hit")
                psig = sum(1 for r in recs if r.close_reason == "signal_exit")
                pc = "#1b8a2a" if ppnl >= 0 else "#c62828"
                p.append(f'<tr><td style="padding:3px 8px; font-weight:bold;">{pair_name}</td>'
                         f'<td>{ptotal}</td><td>{pwr:.0f}%</td>'
                         f'<td style="color:{pc};">{ppnl:+.1f}p</td>'
                         f'<td>{ptp}</td><td>{psl}</td><td>{psig}</td></tr>')
            p.append('</table>')

            # Session breakdown
            sess_stats: dict[str, list] = defaultdict(list)
            for r in records:
                sess_stats[r.session or "Unknown"].append(r)

            if sess_stats:
                p.append('<div class="section-title">Session Performance</div>')
                p.append('<table style="width:100%; border-collapse:collapse; font-size:11px;">')
                p.append('<tr style="background:#f0f4f8; color:#4a6fa5; font-weight:bold;">'
                         '<td style="padding:4px 8px;">Session</td>'
                         '<td>Trades</td><td>WR</td><td>Total P/L</td>'
                         '<td>TP</td><td>SL</td><td>Signal</td><td>Avg P/L</td></tr>')
                # Sort by total P/L descending
                for sess_name in sorted(sess_stats.keys(),
                                        key=lambda s: sum(r.pnl_pips for r in sess_stats[s]),
                                        reverse=True):
                    srecs = sess_stats[sess_name]
                    sw = sum(1 for r in srecs if r.is_win)
                    stotal = len(srecs)
                    spnl = sum(r.pnl_pips for r in srecs)
                    swr = sw / stotal * 100 if stotal else 0
                    stp = sum(1 for r in srecs if r.close_reason == "tp_hit")
                    ssl = sum(1 for r in srecs if r.close_reason == "sl_hit")
                    ssig = sum(1 for r in srecs if r.close_reason == "signal_exit")
                    savg = spnl / stotal if stotal else 0
                    sc = "#1b8a2a" if spnl >= 0 else "#c62828"
                    ac = "#1b8a2a" if savg >= 0 else "#c62828"
                    p.append(f'<tr><td style="padding:3px 8px; font-weight:bold;">{sess_name}</td>'
                             f'<td>{stotal}</td><td>{swr:.0f}%</td>'
                             f'<td style="color:{sc};">{spnl:+.1f}p</td>'
                             f'<td>{stp}</td><td>{ssl}</td><td>{ssig}</td>'
                             f'<td style="color:{ac};">{savg:+.1f}p</td></tr>')
                p.append('</table>')

        return "".join(p)

    def _populate_paper_table(self, records: list[PaperTradeRecord]) -> None:
        """Fill the paper trades table."""
        table = self._paper_trades_table
        title = self._paper_table_title
        title.setText(f"Paper Trades ({len(records)})")

        cols = ["Time", "Pair", "Dir", "Type", "Session", "Entry", "Close", "SL", "TP",
                "P/L", "Peak", "Worst", "Exit", "Dur", "Conv",
                "4h MFE", "4h MAE", "4h End"]
        table.setSortingEnabled(False)
        table.setColumnCount(len(cols))
        table.setHorizontalHeaderLabels(cols)
        table.setRowCount(len(records))

        for row, r in enumerate(reversed(records)):  # newest first
            table.setItem(row, 0, QTableWidgetItem(
                _to_jst_str(r.entry_time, "%m-%d %H:%M") if r.entry_time else "—"
            ))
            table.setItem(row, 1, QTableWidgetItem(r.pair))

            dir_item = QTableWidgetItem(r.direction)
            dir_item.setForeground(QColor("#1b8a2a" if r.direction == "BUY" else "#c62828"))
            table.setItem(row, 2, dir_item)

            # Entry type (Standard / Accel)
            etype = getattr(r, 'entry_type', 'standard')
            etype_label = "STND" if etype == "standard" else "\u26a1ACL"
            etype_item = QTableWidgetItem(etype_label)
            etype_item.setForeground(QColor("#2a5a8a" if etype == "standard" else "#ff9800"))
            if etype == "acceleration":
                etype_item.setFont(QFont("Segoe UI", 9, QFont.Weight.Bold))
            table.setItem(row, 3, etype_item)

            # Session
            sess_item = QTableWidgetItem(r.session or "—")
            sess_item.setForeground(QColor("#4a6fa5"))
            table.setItem(row, 4, sess_item)

            table.setItem(row, 5, _NumericItem(f"{r.entry_price:.5f}", r.entry_price))
            table.setItem(row, 6, _NumericItem(f"{r.close_price:.5f}", r.close_price))
            table.setItem(row, 7, _NumericItem(f"{r.sl_pips:.1f}", r.sl_pips))
            table.setItem(row, 8, _NumericItem(f"{r.tp_pips:.1f}", r.tp_pips))

            pnl_item = _NumericItem(f"{r.pnl_pips:+.1f}", r.pnl_pips)
            pnl_item.setForeground(QColor("#1b8a2a" if r.pnl_pips >= 0 else "#c62828"))
            pnl_item.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
            table.setItem(row, 9, pnl_item)

            table.setItem(row, 10, _NumericItem(f"{r.peak_pnl_pips:+.1f}", r.peak_pnl_pips))
            table.setItem(row, 11, _NumericItem(f"{r.worst_pnl_pips:+.1f}", r.worst_pnl_pips))

            reason_labels = {"sl_hit": "SL", "tp_hit": "TP", "signal_exit": "Signal"}
            reason_colors = {"sl_hit": "#c62828", "tp_hit": "#1b8a2a", "signal_exit": "#e65100"}
            reason_item = QTableWidgetItem(reason_labels.get(r.close_reason, r.close_reason))
            reason_item.setForeground(QColor(reason_colors.get(r.close_reason, "#666")))
            table.setItem(row, 12, reason_item)

            table.setItem(row, 13, _NumericItem(f"{r.duration_minutes:.0f}m", r.duration_minutes))
            table.setItem(row, 14, _NumericItem(str(r.entry_conviction), float(r.entry_conviction)))

            # Post-close 4h observation columns
            if r.post_close_complete:
                mfe4 = _NumericItem(f"{r.post_close_max_mfe_pips:+.1f}", r.post_close_max_mfe_pips)
                mfe4.setForeground(QColor("#1b8a2a"))
                table.setItem(row, 15, mfe4)

                mae4 = _NumericItem(f"{r.post_close_max_mae_pips:.1f}", r.post_close_max_mae_pips)
                mae4.setForeground(QColor("#c62828"))
                table.setItem(row, 16, mae4)

                end4 = _NumericItem(f"{r.post_close_final_pips:+.1f}", r.post_close_final_pips)
                end4.setForeground(QColor("#1b8a2a" if r.post_close_final_pips >= 0 else "#c62828"))
                table.setItem(row, 17, end4)
            else:
                watching_txt = "..." if r.close_time > 0 and not r.post_close_complete else "—"
                table.setItem(row, 15, QTableWidgetItem(watching_txt))
                table.setItem(row, 16, QTableWidgetItem(watching_txt))
                table.setItem(row, 17, QTableWidgetItem(watching_txt))

        table.setSortingEnabled(True)
        header = table.horizontalHeader()
        header.setStretchLastSection(True)
        for col in range(len(cols)):
            header.setSectionResizeMode(col, QHeaderView.ResizeMode.ResizeToContents)

    def _render_live(self) -> None:
        selected = self._pair_combo.currentText()
        if selected and selected != "ALL":
            outcomes = [o for o in self._all_outcomes if o.pair == selected]
        else:
            outcomes = self._all_outcomes
        self._content_label.setText(self._build_html(outcomes, selected))
        self._populate_trades_table(outcomes, self._trades_table, self._table_title)

    def _render_backtest(self) -> None:
        selected = self._bt_pair_combo.currentText()
        if selected and selected != "ALL":
            outcomes = [o for o in self._bt_outcomes if o.pair == selected]
        else:
            outcomes = self._bt_outcomes
        bt_title = f"Backtest: {selected}" if selected != "ALL" else "Backtest Results"
        self._bt_content_label.setText(self._build_html(outcomes, selected, title_prefix="Backtest"))
        self._populate_trades_table(outcomes, self._bt_trades_table, self._bt_table_title)

    def _clear_history(self) -> None:
        try:
            tab_idx = self._tabs.currentIndex()
            tab_names = {0: "live trade", 1: "paper trade", 2: "backtest"}
            tab_name = tab_names.get(tab_idx, "")
            reply = QMessageBox.question(
                self, "Clear History",
                f"Delete all {tab_name} performance history? This cannot be undone.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            )
            if reply == QMessageBox.StandardButton.Yes:
                targets = {0: self._outcomes_file, 1: self._paper_trades_file, 2: self._backtest_file}
                target = targets.get(tab_idx)
                if target:
                    try:
                        target.unlink(missing_ok=True)
                    except OSError:
                        pass
                self._refresh()
        except Exception as e:
            import traceback
            logging.getLogger(__name__).error("Clear history error: %s\n%s", e, traceback.format_exc())

    def _build_html(self, outcomes: list[AlertOutcome], pair_filter: str, title_prefix: str = "") -> str:
        p = [_CSS]

        if title_prefix:
            title = f"{title_prefix}: {pair_filter}" if pair_filter != "ALL" else f"{title_prefix} — Overall Performance"
        else:
            title = f"Performance: {pair_filter}" if pair_filter != "ALL" else "Overall Performance"
        p.append(f'<div class="header">{title}</div>')
        p.append(f'<div class="subtitle">{self._active_count} active | {len(outcomes)} completed</div>')

        if not outcomes:
            p.append('<div class="empty">No completed outcomes yet. '
                     'Alerts are tracked automatically from entry through exit + 4 hours.</div>')
            return "".join(p)

        p.append(self._summary_cards(outcomes))
        p.append(self._exit_analysis(outcomes))
        if pair_filter == "ALL":
            p.append(self._pair_table(outcomes))
        p.append(self._tier_table(outcomes))
        p.append(self._session_table(outcomes))
        p.append(self._optimal_levels(outcomes))

        return "".join(p)

    # ── Sortable trades table ─────────────────────────────────────

    _COL_HEADERS = [
        "#", "Time (JST)", "Dir", "Pair", "Conv",
        "MFE", "MAE", "@Exit", "T-Exit", "Reason",
        "MAX-MFE", "MAX-MAE", "Final", "Duration", "Session",
    ]

    # Session windows in JST (hour, minute) → label
    _SESSIONS = [
        ((7, 0), (8, 44), "Australia"),
        ((8, 45), (9, 35), "Tokyo OPEN"),
        ((9, 36), (12, 8), "Morning"),
        ((12, 9), (14, 44), "Afternoon"),
        ((14, 45), (15, 25), "Frankfurt OPEN"),
        ((15, 26), (15, 44), "EU"),
        ((15, 45), (16, 35), "London OPEN"),
        ((16, 36), (20, 44), "London"),
        ((20, 45), (21, 35), "US OPEN"),
        ((21, 36), (23, 59), "US"),
        ((0, 0), (5, 0), "US"),
        ((5, 1), (6, 59), "NO TRADE"),
    ]

    @staticmethod
    def _get_session(unix_ts: float) -> str:
        """Return the trading session label for a JST timestamp."""
        try:
            dt = datetime.fromtimestamp(unix_ts, tz=_jst())
            hm = dt.hour * 60 + dt.minute  # minutes since midnight
            for (sh, sm), (eh, em), label in PerformanceDialog._SESSIONS:
                start = sh * 60 + sm
                end = eh * 60 + em
                if start <= hm <= end:
                    return label
            return ""
        except Exception:
            return ""

    def _populate_trades_table(
        self,
        outcomes: list[AlertOutcome],
        tbl: QTableWidget | None = None,
        title_label: QLabel | None = None,
    ) -> None:
        if tbl is None:
            tbl = self._trades_table
        if title_label is None:
            title_label = self._table_title

        # Take 200 most recent, then sort oldest-first for numbering
        recent = sorted(outcomes, key=lambda o: o.entry_time, reverse=True)[:200]
        recent.sort(key=lambda o: o.entry_time)  # oldest first for #1 = first trade
        total = len(outcomes)

        title_label.setText(
            f"Recent Alerts — {len(recent)} of {total} trades shown "
            f"(click column headers to sort)"
        )
        tbl.setSortingEnabled(False)  # disable while populating
        tbl.setRowCount(0)
        tbl.setColumnCount(len(self._COL_HEADERS))
        tbl.setHorizontalHeaderLabels(self._COL_HEADERS)
        tbl.setRowCount(len(recent))

        green = QColor("#0a7a0a")
        red = QColor("#cc2222")
        grey = QColor("#888888")
        dark = QColor("#1a3a5c")
        text = QColor("#333333")
        bold_font = QFont("Segoe UI", 10, QFont.Weight.Bold)

        for row, o in enumerate(recent):
            # # (row number)
            item_num = _NumericItem(str(row + 1), float(row + 1))
            item_num.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            item_num.setForeground(grey)
            tbl.setItem(row, 0, item_num)

            # Time (JST)
            time_jst = _to_jst_str(o.entry_time) if o.entry_time else o.entry_time_str[5:16]
            item_time = _NumericItem(time_jst, o.entry_time)
            item_time.setForeground(grey)
            tbl.setItem(row, 1, item_time)

            # Direction
            item_dir = QTableWidgetItem(o.direction)
            item_dir.setForeground(green if o.direction == "BUY" else red)
            item_dir.setFont(bold_font)
            item_dir.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            tbl.setItem(row, 2, item_dir)

            # Pair
            item_pair = QTableWidgetItem(o.pair)
            item_pair.setFont(bold_font)
            item_pair.setForeground(dark)
            tbl.setItem(row, 3, item_pair)

            # Conviction
            item_conv = _NumericItem(str(o.conviction_score), float(o.conviction_score))
            item_conv.setForeground(text)
            item_conv.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            tbl.setItem(row, 4, item_conv)

            # MFE
            item_mfe = _NumericItem(f"+{o.mfe_pips:.1f}", o.mfe_pips)
            item_mfe.setForeground(green)
            item_mfe.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            tbl.setItem(row, 5, item_mfe)

            # MAE
            item_mae = _NumericItem(f"-{o.mae_pips:.1f}", o.mae_pips)
            item_mae.setForeground(red)
            item_mae.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            tbl.setItem(row, 6, item_mae)

            # @Exit
            item_exit = _NumericItem(f"{o.exit_signal_pnl_pips:+.1f}", o.exit_signal_pnl_pips)
            item_exit.setForeground(green if o.exit_signal_pnl_pips > 0 else red)
            item_exit.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            tbl.setItem(row, 7, item_exit)

            # T-Exit
            item_texit = _NumericItem(f"{o.time_to_exit_minutes:.0f}m", o.time_to_exit_minutes)
            item_texit.setForeground(text)
            item_texit.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            tbl.setItem(row, 8, item_texit)

            # Reason
            item_reason = QTableWidgetItem(o.exit_reason or "")
            item_reason.setForeground(grey)
            tbl.setItem(row, 9, item_reason)

            # MAX-MFE (entire observation window)
            max_mfe = getattr(o, "max_mfe_pips", o.mfe_pips)
            item_max_mfe = _NumericItem(f"+{max_mfe:.1f}", max_mfe)
            item_max_mfe.setForeground(green)
            item_max_mfe.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            tbl.setItem(row, 10, item_max_mfe)

            # MAX-MAE (entire observation window)
            max_mae = getattr(o, "max_mae_pips", o.mae_pips)
            item_max_mae = _NumericItem(f"-{max_mae:.1f}", max_mae)
            item_max_mae.setForeground(red)
            item_max_mae.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            tbl.setItem(row, 11, item_max_mae)

            # Final
            item_final = _NumericItem(f"{o.final_pnl_pips:+.1f}", o.final_pnl_pips)
            item_final.setForeground(green if o.final_pnl_pips > 0 else red)
            item_final.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            tbl.setItem(row, 12, item_final)

            # Duration (entry to exit signal)
            if o.exit_signal_time and o.entry_time and o.exit_signal_time > o.entry_time:
                dur_min = (o.exit_signal_time - o.entry_time) / 60.0
                if dur_min >= 60:
                    dur_h = int(dur_min // 60)
                    dur_m = int(dur_min % 60)
                    dur_str = f"{dur_h}h{dur_m:02d}m"
                else:
                    dur_str = f"{dur_min:.0f}m"
                item_dur = _NumericItem(dur_str, dur_min)
            else:
                item_dur = _NumericItem("", 0.0)
            item_dur.setForeground(text)
            item_dur.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            tbl.setItem(row, 13, item_dur)

            # Session
            session_label = self._get_session(o.entry_time) if o.entry_time else ""
            item_sess = QTableWidgetItem(session_label)
            # Color-code sessions
            sess_colors = {
                "Australia": QColor("#2e86c1"),
                "Tokyo OPEN": QColor("#e74c3c"),
                "Morning": QColor("#8e44ad"),
                "Afternoon": QColor("#d68910"),
                "Frankfurt OPEN": QColor("#e74c3c"),
                "EU": QColor("#27ae60"),
                "London OPEN": QColor("#e74c3c"),
                "London": QColor("#27ae60"),
                "US OPEN": QColor("#e74c3c"),
                "US": QColor("#2980b9"),
                "NO TRADE": QColor("#cc2222"),
            }
            item_sess.setForeground(sess_colors.get(session_label, grey))
            item_sess.setFont(bold_font)
            item_sess.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            tbl.setItem(row, 14, item_sess)

        tbl.setSortingEnabled(True)

        # Column widths — all fit content, no stretching
        header = tbl.horizontalHeader()
        header.setStretchLastSection(False)
        for col in range(tbl.columnCount()):
            header.setSectionResizeMode(col, QHeaderView.ResizeMode.ResizeToContents)

        # Default sort: oldest first (#1 = first trade opened)
        tbl.sortByColumn(1, Qt.SortOrder.AscendingOrder)

    # ── HTML Sections ─────────────────────────────────────────────

    def _summary_cards(self, outcomes: list[AlertOutcome]) -> str:
        n = len(outcomes)
        if n == 0:
            return ""
        wins = sum(1 for o in outcomes if o.exit_signal_pnl_pips > 0)
        wr = wins / n * 100
        avg_mfe = sum(o.mfe_pips for o in outcomes) / n
        avg_mae = sum(o.mae_pips for o in outcomes) / n
        avg_exit = sum(o.exit_signal_pnl_pips for o in outcomes) / n
        avg_final = sum(o.final_pnl_pips for o in outcomes) / n
        edge = avg_mfe / avg_mae if avg_mae > 0 else 0
        avg_t_mfe = sum(o.time_to_mfe_minutes for o in outcomes) / n
        avg_t_exit = sum(o.time_to_exit_minutes for o in outcomes) / n

        wr_cls = "pos" if wr >= 50 else "neg"
        edge_cls = "pos" if edge >= 1.0 else "neg"
        exit_cls = "pos" if avg_exit > 0 else "neg"
        final_cls = "pos" if avg_final > 0 else "neg"

        # Date range from trade data
        date_range_str = ""
        all_times = [o.entry_time for o in outcomes if o.entry_time > 0]
        if all_times:
            earliest = datetime.utcfromtimestamp(min(all_times))
            latest = datetime.utcfromtimestamp(max(all_times))
            actual_days = max(1, (latest - earliest).days)
            date_range_str = (
                f'<tr><td>Period</td>'
                f'<td class="num" colspan="7"><b>{earliest.strftime("%Y-%m-%d")} \u2192 '
                f'{latest.strftime("%Y-%m-%d")}</b> ({actual_days} days)</td></tr>'
            )

        return f'''
        <div class="section-title">Summary (Entry to Exit Signal)</div>
        <table class="stats">
        {date_range_str}
        <tr>
            <td>Win Rate</td><td class="num {wr_cls}"><b>{wr:.1f}%</b> ({wins}/{n})</td>
            <td>Avg MFE</td><td class="num pos"><b>+{avg_mfe:.1f}</b> pips</td>
            <td>Avg MAE</td><td class="num neg"><b>-{avg_mae:.1f}</b> pips</td>
            <td>Edge Ratio</td><td class="num {edge_cls}"><b>{edge:.2f}</b></td>
        </tr>
        <tr>
            <td>P/L @ Exit</td><td class="num {exit_cls}"><b>{avg_exit:+.1f}</b> pips</td>
            <td>P/L @ End</td><td class="num {final_cls}"><b>{avg_final:+.1f}</b> pips</td>
            <td>Time to MFE</td><td class="num"><b>{avg_t_mfe:.0f}</b>m</td>
            <td>Time to Exit</td><td class="num"><b>{avg_t_exit:.0f}</b>m</td>
        </tr>
        </table>
        '''

    def _exit_analysis(self, outcomes: list[AlertOutcome]) -> str:
        n = len(outcomes)
        if n < 3:
            return ""

        better = sum(1 for o in outcomes if o.post_exit_mfe_pips > 2.0)
        worse = sum(1 for o in outcomes if o.post_exit_mae_pips > 2.0)
        avg_post_mfe = sum(o.post_exit_mfe_pips for o in outcomes) / n
        avg_post_mae = sum(o.post_exit_mae_pips for o in outcomes) / n

        reasons: dict[str, int] = defaultdict(int)
        for o in outcomes:
            reasons[o.exit_reason or "unknown"] += 1

        reason_rows = ""
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            pct = count / n * 100
            bar_w = int(pct * 2)
            reason_rows += f'''<tr>
                <td>{reason}</td><td class="num">{count}</td>
                <td class="num">{pct:.0f}%</td>
                <td><div class="bar" style="width:{bar_w}px"></div></td>
            </tr>'''

        return f'''
        <div class="section-title">Post-Exit (+4h) &amp; Exit Reasons</div>
        <table class="stats">
        <tr>
            <td>Improved &gt;2p</td><td class="num">{better} ({better*100//n}%)</td>
            <td>Worsened &gt;2p</td><td class="num">{worse} ({worse*100//n}%)</td>
            <td>Avg post-fav</td><td class="num pos">+{avg_post_mfe:.1f}p</td>
            <td>Avg post-adv</td><td class="num neg">-{avg_post_mae:.1f}p</td>
        </tr>
        </table>
        <table>
        <tr><th>Reason</th><th class="num">#</th><th class="num">%</th><th></th></tr>
        {reason_rows}
        </table>
        '''

    def _pair_table(self, outcomes: list[AlertOutcome]) -> str:
        pairs: dict[str, list[AlertOutcome]] = defaultdict(list)
        for o in outcomes:
            pairs[o.pair].append(o)

        sorted_pairs = sorted(pairs.items(), key=lambda x: len(x[1]), reverse=True)

        rows = ""
        for pair, group in sorted_pairs:
            n = len(group)
            wins = sum(1 for o in group if o.exit_signal_pnl_pips > 0)
            wr = wins / n * 100 if n else 0
            avg_mfe = sum(o.mfe_pips for o in group) / n
            avg_mae = sum(o.mae_pips for o in group) / n
            avg_exit = sum(o.exit_signal_pnl_pips for o in group) / n
            avg_t = sum(o.time_to_exit_minutes for o in group) / n
            wr_cls = "pos" if wr >= 50 else "neg"
            exit_cls = "pos" if avg_exit > 0 else "neg"
            rows += f'''<tr>
                <td class="pair">{pair}</td><td class="num">{n}</td>
                <td class="num {wr_cls}">{wr:.0f}%</td>
                <td class="num pos">+{avg_mfe:.1f}</td>
                <td class="num neg">-{avg_mae:.1f}</td>
                <td class="num {exit_cls}">{avg_exit:+.1f}</td>
                <td class="num">{avg_t:.0f}m</td>
            </tr>'''

        return f'''
        <div class="section-title">By Pair</div>
        <table>
        <tr><th>Pair</th><th class="num">#</th><th class="num">Win%</th>
            <th class="num">MFE</th><th class="num">MAE</th>
            <th class="num">Exit P/L</th><th class="num">T-Exit</th></tr>
        {rows}
        </table>
        '''

    def _tier_table(self, outcomes: list[AlertOutcome]) -> str:
        tiers: dict[str, list[AlertOutcome]] = defaultdict(list)
        for o in outcomes:
            tiers[o.conviction_tier or "UNKNOWN"].append(o)

        rows = ""
        for tier in ("FULL", "DIMMED", "UNKNOWN"):
            group = tiers.get(tier)
            if not group:
                continue
            rows += self._stat_row(tier, group)

        return f'''
        <div class="section-title">By Conviction Tier</div>
        <table>
        <tr><th>Tier</th><th class="num">#</th><th class="num">Win%</th>
            <th class="num">MFE</th><th class="num">MAE</th><th class="num">Exit P/L</th></tr>
        {rows}
        </table>
        '''

    def _session_table(self, outcomes: list[AlertOutcome]) -> str:
        sessions: dict[str, list[AlertOutcome]] = defaultdict(list)
        for o in outcomes:
            sessions[o.session or "unknown"].append(o)

        rows = ""
        for session in sorted(sessions.keys()):
            rows += self._stat_row(session.title(), sessions[session])

        return f'''
        <div class="section-title">By Session</div>
        <table>
        <tr><th>Session</th><th class="num">#</th><th class="num">Win%</th>
            <th class="num">MFE</th><th class="num">MAE</th><th class="num">Exit P/L</th></tr>
        {rows}
        </table>
        '''

    def _stat_row(self, label: str, group: list[AlertOutcome]) -> str:
        n = len(group)
        if n == 0:
            return ""
        wins = sum(1 for o in group if o.exit_signal_pnl_pips > 0)
        wr = wins / n * 100
        avg_mfe = sum(o.mfe_pips for o in group) / n
        avg_mae = sum(o.mae_pips for o in group) / n
        avg_exit = sum(o.exit_signal_pnl_pips for o in group) / n
        wr_cls = "pos" if wr >= 50 else "neg"
        exit_cls = "pos" if avg_exit > 0 else "neg"
        return f'''<tr>
            <td><b>{label}</b></td><td class="num">{n}</td>
            <td class="num {wr_cls}">{wr:.0f}%</td>
            <td class="num pos">+{avg_mfe:.1f}</td>
            <td class="num neg">-{avg_mae:.1f}</td>
            <td class="num {exit_cls}">{avg_exit:+.1f}</td>
        </tr>'''

    def _optimal_levels(self, outcomes: list[AlertOutcome]) -> str:
        if len(outcomes) < 5:
            return '<div class="section-title">Optimal Levels</div><div class="empty">Need 5+ alerts.</div>'

        winners = [o for o in outcomes if o.exit_signal_pnl_pips > 0]
        all_mae = sorted(o.mae_pips for o in outcomes)
        all_mfe = sorted(o.mfe_pips for o in outcomes)

        def pctl(vals: list[float], pct: float) -> float:
            if not vals:
                return 0.0
            return vals[min(int(len(vals) * pct / 100), len(vals) - 1)]

        w_mae = sorted(o.mae_pips for o in winners) if winners else []
        w_mae_90 = pctl(w_mae, 90) if w_mae else 0

        return f'''
        <div class="section-title">Optimal Levels</div>
        <table>
        <tr><th>Metric</th><th class="num">50th</th><th class="num">75th</th><th class="num">90th</th></tr>
        <tr><td>MAE (all)</td><td class="num">{pctl(all_mae, 50):.1f}</td>
            <td class="num">{pctl(all_mae, 75):.1f}</td><td class="num">{pctl(all_mae, 90):.1f}</td></tr>
        <tr><td>MFE (all)</td><td class="num">{pctl(all_mfe, 50):.1f}</td>
            <td class="num">{pctl(all_mfe, 75):.1f}</td><td class="num">{pctl(all_mfe, 90):.1f}</td></tr>
        </table>
        <div class="hint">Suggested SL: <b>{w_mae_90:.1f}</b> pips (90% of winners below) &nbsp;|&nbsp;
        Suggested TP: <b>{pctl(all_mfe, 50):.1f}</b> pips (50% of alerts reached)</div>
        '''


_CSS = """
<style>
    .header {
        font-size: 14px; font-weight: 700; color: #1a3a5c;
        margin-bottom: 1px;
    }
    .subtitle {
        font-size: 10px; color: #888; margin-bottom: 6px;
    }
    .section-title {
        font-size: 11px; font-weight: 700; color: #2a5a8a;
        border-bottom: 2px solid #3a7abf; padding-bottom: 2px;
        margin: 8px 0 4px 0;
    }
    .empty {
        font-size: 10px; color: #999; font-style: italic;
        padding: 4px 0;
    }
    .hint {
        font-size: 10px; color: #666; margin: 3px 0;
    }

    /* Tables */
    table {
        border-collapse: collapse; width: 100%; margin: 2px 0;
        background: white; border-radius: 3px;
        border: 1px solid #e0e0e0;
    }
    table.stats td {
        font-size: 10px; padding: 3px 6px;
    }
    th {
        background: #f0f4f8; color: #4a6fa5; font-size: 9px;
        font-weight: 700; text-transform: uppercase; letter-spacing: 0.3px;
        padding: 3px 6px; border-bottom: 2px solid #d0d8e0;
        text-align: left;
    }
    td {
        padding: 3px 6px; font-size: 10px; color: #333;
        border-bottom: 1px solid #f0f0f0;
    }
    tr:hover { background: #f8fafe; }
    tr:last-child td { border-bottom: none; }

    .num { text-align: right; font-family: 'Consolas', monospace; }
    th.num { text-align: right; }
    .pair { font-weight: 700; color: #1a3a5c; }
    .pos { color: #0a7a0a; font-weight: 600; }
    .neg { color: #cc2222; font-weight: 600; }

    .bar {
        height: 8px; background: #4a6fa5; border-radius: 2px;
        min-width: 2px;
    }
</style>
"""
