"""Pair Algorithm Settings Dialog.

Shows per-pair optimized calculation parameters with history.
Accessed from Settings > Pair Algo in the main window toolbar.
"""

from __future__ import annotations

import logging

from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont, QGuiApplication
from PyQt6.QtWidgets import (
    QComboBox,
    QDialog,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


def _center_on_primary(widget: QWidget) -> None:
    """Center a widget on the primary monitor."""
    primary = QGuiApplication.primaryScreen()
    if primary:
        geo = primary.availableGeometry()
        widget.move(
            geo.x() + (geo.width() - widget.width()) // 2,
            geo.y() + (geo.height() - widget.height()) // 2,
        )

from takumi_trader.core.pair_algo_settings import (
    delete_pair_settings,
    get_all_pairs_with_settings,
    get_pair_history,
    get_pair_settings,
    restore_from_history,
)
from takumi_trader.core.strength import DISPLAY_PAIRS

logger = logging.getLogger(__name__)

# ── Styling ──────────────────────────────────────────────────────────

_STYLE = """
QWidget {
    background-color: #f5f5f5;
    font-family: "Segoe UI", sans-serif;
    font-size: 11px;
}
QPushButton {
    background-color: #4a6fa5;
    color: white;
    border: none;
    padding: 6px 14px;
    border-radius: 4px;
    font-weight: bold;
    min-width: 60px;
}
QPushButton:hover { background-color: #5a7fb5; }
QPushButton:pressed { background-color: #3a5f95; }
QPushButton:disabled { background-color: #aaa; }
QPushButton.danger {
    background-color: #c0392b;
}
QPushButton.danger:hover { background-color: #e74c3c; }
QPushButton.secondary {
    background-color: #6c757d;
}
QPushButton.secondary:hover { background-color: #7c858d; }
QComboBox {
    padding: 4px 8px;
    border: 1px solid #ccc;
    border-radius: 3px;
    background: white;
    color: #222;
    min-width: 120px;
}
QComboBox QAbstractItemView {
    background: white;
    color: #222;
    selection-background-color: #4a6fa5;
    selection-color: white;
}
QTextEdit {
    background: white;
    border: 1px solid #ddd;
    border-radius: 4px;
    font-family: "Consolas", "Courier New", monospace;
    font-size: 11px;
}
"""

_CSS = """<style>
body { font-family: "Segoe UI", sans-serif; font-size: 12px; margin: 8px; }
h2 { color: #2c3e50; font-size: 15px; margin: 12px 0 6px 0;
     border-bottom: 2px solid #4a6fa5; padding-bottom: 3px; }
h3 { color: #34495e; font-size: 13px; margin: 10px 0 4px 0; }
table { border-collapse: collapse; width: 100%; margin: 6px 0; }
th { background: #4a6fa5; color: white; padding: 5px 8px;
     text-align: left; font-size: 11px; }
td { padding: 4px 8px; border-bottom: 1px solid #eee; font-size: 11px; }
tr:nth-child(even) { background: #f8f9fa; }
.param-name { color: #555; font-weight: normal; }
.param-value { color: #2c3e50; font-weight: bold; }
.stat-good { color: #27ae60; font-weight: bold; }
.stat-bad { color: #c0392b; font-weight: bold; }
.stat-neutral { color: #555; }
.box { border: 1px solid #ddd; border-radius: 6px; padding: 10px;
       margin: 8px 0; background: #fafafa; }
.date-label { color: #888; font-size: 10px; }
.source-label { color: #4a6fa5; font-weight: bold; font-size: 10px; }
.history-entry { border-left: 3px solid #bbb; padding-left: 8px;
                 margin: 6px 0; }
.history-entry.current { border-left-color: #27ae60; }
.no-data { color: #999; font-style: italic; padding: 20px;
           text-align: center; }
</style>"""


class PairAlgoDialog(QDialog):
    """Main Pair Algorithm Settings dialog.

    Shows a pair selector dropdown. Selecting a pair displays:
    - Current active settings with stats
    - "View History" button to see previous settings
    """

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Pair Algorithm Settings")
        self.setMinimumSize(600, 500)
        self.resize(700, 600)
        self.setStyleSheet(_STYLE)

        self._setup_ui()
        self._refresh_pair_list()
        _center_on_primary(self)

    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 12, 16, 12)
        root.setSpacing(8)

        # Title
        title = QLabel("Pair Algorithm Settings")
        title.setFont(QFont("Segoe UI", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        root.addWidget(title)

        subtitle = QLabel(
            "Optimized calculation parameters per pair. "
            "Set via Backtest Simulator > Optimize Params > Save & Set."
        )
        subtitle.setWordWrap(True)
        subtitle.setAlignment(Qt.AlignmentFlag.AlignCenter)
        subtitle.setStyleSheet("color: #666; font-size: 10px; margin-bottom: 6px;")
        root.addWidget(subtitle)

        # Pair selector row
        selector_row = QHBoxLayout()
        selector_row.addWidget(QLabel("Pair:"))

        self._pair_combo = QComboBox()
        self._pair_combo.setMinimumWidth(140)
        self._pair_combo.currentTextChanged.connect(self._on_pair_changed)
        selector_row.addWidget(self._pair_combo)

        selector_row.addSpacing(12)

        self._history_btn = QPushButton("View History")
        self._history_btn.setProperty("class", "secondary")
        self._history_btn.clicked.connect(self._open_history)
        self._history_btn.setEnabled(False)
        selector_row.addWidget(self._history_btn)

        self._delete_btn = QPushButton("Delete")
        self._delete_btn.setProperty("class", "danger")
        self._delete_btn.clicked.connect(self._delete_settings)
        self._delete_btn.setEnabled(False)
        selector_row.addWidget(self._delete_btn)

        selector_row.addStretch()

        self._refresh_btn = QPushButton("Refresh")
        self._refresh_btn.clicked.connect(self._refresh_pair_list)
        selector_row.addWidget(self._refresh_btn)

        root.addLayout(selector_row)

        # Content area
        self._content = QTextEdit()
        self._content.setReadOnly(True)
        root.addWidget(self._content, stretch=1)

        # Bottom buttons
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        btn_row.addWidget(close_btn)
        root.addLayout(btn_row)

    def _refresh_pair_list(self) -> None:
        """Refresh the pair dropdown with all pairs that have settings."""
        self._pair_combo.blockSignals(True)
        current = self._pair_combo.currentText()
        self._pair_combo.clear()

        # All DISPLAY_PAIRS, with those that have settings at the top
        pairs_with_settings = set(get_all_pairs_with_settings())

        # "Configured" group first
        configured = [p for p in DISPLAY_PAIRS if p in pairs_with_settings]
        unconfigured = [p for p in DISPLAY_PAIRS if p not in pairs_with_settings]

        if configured:
            for p in configured:
                self._pair_combo.addItem(f"\u2714 {p}")
        if unconfigured:
            for p in unconfigured:
                self._pair_combo.addItem(f"  {p}")

        # Restore selection if possible
        if current:
            for i in range(self._pair_combo.count()):
                if current in self._pair_combo.itemText(i):
                    self._pair_combo.setCurrentIndex(i)
                    break

        self._pair_combo.blockSignals(False)
        self._on_pair_changed(self._pair_combo.currentText())

    def _get_selected_pair(self) -> str:
        """Get the clean pair name from the combo (strip prefix)."""
        text = self._pair_combo.currentText().strip()
        # Remove checkmark or spaces prefix
        for prefix in ("\u2714 ", "  "):
            if text.startswith(prefix):
                text = text[len(prefix):]
        return text.strip()

    def _on_pair_changed(self, _text: str) -> None:
        """Called when the pair selection changes."""
        pair = self._get_selected_pair()
        if not pair:
            return

        settings = get_pair_settings(pair)
        history = get_pair_history(pair)

        self._history_btn.setEnabled(len(history) > 0)
        self._delete_btn.setEnabled(settings is not None)

        if settings is None:
            self._content.setHtml(
                f"{_CSS}<body>"
                f'<div class="no-data">'
                f"<h2>{pair}</h2>"
                f"<p>No optimized settings saved for this pair yet.</p>"
                f"<p>Run <b>Backtest Simulator → Optimize Params</b> "
                f"and click <b>Save &amp; Set</b> to store optimal settings.</p>"
                f"</div></body>"
            )
            return

        html = self._build_settings_html(pair, settings, len(history))
        self._content.setHtml(html)

    def _build_settings_html(self, pair: str, settings: dict, history_count: int) -> str:
        """Build HTML display of current settings."""
        parts = [_CSS, "<body>"]

        parts.append(f'<h2>{pair} — Current Settings</h2>')

        # Meta info
        set_date = settings.get("set_date", "unknown")
        source = settings.get("source", "unknown")
        bt_period = settings.get("backtest_period", "")

        parts.append(f'<div class="box">')
        parts.append(f'<span class="date-label">Set: {set_date}</span>')
        parts.append(f' &nbsp;|&nbsp; <span class="source-label">Source: {source}</span>')
        if bt_period:
            parts.append(f' &nbsp;|&nbsp; <span class="date-label">Period: {bt_period}</span>')
        parts.append('</div>')

        # Trading Parameters (Stoch v2)
        sl_atr = settings.get("sl_atr", 0)
        tp_atr = settings.get("tp_atr", 0)
        sl_pips = settings.get("sl_pips", 0)
        tp_pips = settings.get("tp_pips", 0)
        rr = tp_atr / sl_atr if sl_atr > 0 else 0

        parts.append('<h3>Trading Parameters (Stoch v2)</h3>')
        parts.append('<table>')
        parts.append('<tr><th>Parameter</th><th>Value</th></tr>')
        parts.append(f'<tr><td class="param-name">SL (ATR multiplier)</td>'
                     f'<td class="param-value">{sl_atr}x ATR (~{sl_pips:.1f} pips avg)</td></tr>')
        parts.append(f'<tr><td class="param-name">TP (ATR multiplier)</td>'
                     f'<td class="param-value">{tp_atr}x ATR (~{tp_pips:.1f} pips avg)</td></tr>')
        parts.append(f'<tr><td class="param-name">Risk:Reward</td>'
                     f'<td class="param-value">1:{rr:.1f}</td></tr>')
        parts.append(f'<tr><td class="param-name">Engine</td>'
                     f'<td class="param-value">Stoch v2 (per-pair Stochastic)</td></tr>')
        parts.append(f'<tr><td class="param-name">Entry TFs</td>'
                     f'<td class="stat-neutral">M5 + M15 (both must agree)</td></tr>')
        parts.append(f'<tr><td class="param-name">Trend Filter</td>'
                     f'<td class="stat-neutral">H1 + H4 + D1 (not against)</td></tr>')
        parts.append(f'<tr><td class="param-name">Exit</td>'
                     f'<td class="stat-neutral">SL/TP + Counter-momentum</td></tr>')
        parts.append('</table>')

        # Performance stats
        trades = settings.get("trades", 0)
        if trades > 0:
            wr = settings.get("wr", 0)
            total_r = settings.get("total_r", 0)
            avg_final = settings.get("avg_final", 0)
            avg_mfe = settings.get("avg_mfe", 0)
            avg_mae = settings.get("avg_mae", 0)

            wr_class = "stat-good" if wr >= 60 else "stat-bad" if wr < 50 else "stat-neutral"
            r_class = "stat-good" if total_r > 0 else "stat-bad"

            parts.append('<h3>Performance at Time of Setting</h3>')
            parts.append('<table>')
            parts.append(f'<tr><td>Trades</td><td class="param-value">{trades}</td></tr>')
            parts.append(f'<tr><td>Win Rate</td><td class="{wr_class}">{wr:.1f}%</td></tr>')
            parts.append(f'<tr><td>Avg Final PnL</td><td class="{r_class}">{avg_final:+.1f} pips</td></tr>')
            parts.append(f'<tr><td>Total R</td><td class="{r_class}">{total_r:+.1f}R</td></tr>')
            parts.append(f'<tr><td>Avg MFE</td><td class="stat-good">+{avg_mfe:.1f} pips</td></tr>')
            parts.append(f'<tr><td>Avg MAE</td><td class="stat-bad">{avg_mae:+.1f} pips</td></tr>')
            parts.append('</table>')

        # History note
        if history_count > 0:
            parts.append(
                f'<p style="color: #888; margin-top: 12px;">'
                f'{history_count} previous setting(s) in history. '
                f'Click <b>View History</b> to see or restore them.</p>'
            )

        parts.append('</body>')
        return "".join(parts)

    def _open_history(self) -> None:
        """Open the history dialog for the selected pair."""
        pair = self._get_selected_pair()
        if not pair:
            return

        dlg = PairAlgoHistoryDialog(pair, self)
        dlg.exec()
        # Refresh in case user restored a history entry
        self._refresh_pair_list()

    def _delete_settings(self) -> None:
        """Delete all settings for the selected pair."""
        pair = self._get_selected_pair()
        if not pair:
            return

        reply = QMessageBox.question(
            self,
            "Delete Settings",
            f"Delete all settings (current + history) for {pair}?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply == QMessageBox.StandardButton.Yes:
            delete_pair_settings(pair)
            self._refresh_pair_list()


class PairAlgoHistoryDialog(QDialog):
    """Dialog showing the settings history for a single pair."""

    def __init__(self, pair: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._pair = pair
        self.setWindowTitle(f"{pair} — Settings History")
        self.setMinimumSize(550, 450)
        self.resize(650, 500)
        self.setStyleSheet(_STYLE)
        self._setup_ui()
        _center_on_primary(self)

    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 12, 16, 12)

        title = QLabel(f"{self._pair} — Settings History")
        title.setFont(QFont("Segoe UI", 13, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        root.addWidget(title)

        # Content area
        self._content = QTextEdit()
        self._content.setReadOnly(True)
        root.addWidget(self._content, stretch=1)

        # Button row
        btn_row = QHBoxLayout()

        self._restore_combo = QComboBox()
        self._restore_combo.setMinimumWidth(200)
        btn_row.addWidget(QLabel("Restore entry:"))
        btn_row.addWidget(self._restore_combo)

        self._restore_btn = QPushButton("Restore")
        self._restore_btn.clicked.connect(self._restore_entry)
        btn_row.addWidget(self._restore_btn)

        btn_row.addStretch()

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        btn_row.addWidget(close_btn)

        root.addLayout(btn_row)

        self._load_history()

    def _load_history(self) -> None:
        """Load and display the history."""
        current = get_pair_settings(self._pair)
        history = get_pair_history(self._pair)

        self._restore_combo.clear()
        for i, entry in enumerate(history):
            date = entry.get("set_date", "unknown")
            source = entry.get("source", "?")
            wr = entry.get("wr", 0)
            self._restore_combo.addItem(f"#{i+1} — {date} ({source}, WR {wr:.0f}%)")

        self._restore_btn.setEnabled(len(history) > 0)

        html = self._build_history_html(current, history)
        self._content.setHtml(html)

    def _build_history_html(self, current: dict | None, history: list[dict]) -> str:
        parts = [_CSS, "<body>"]

        # Current settings (marked)
        if current:
            parts.append('<h2>Current Active Settings</h2>')
            parts.append(self._entry_html(current, is_current=True))

        if not history:
            parts.append('<p class="no-data">No previous settings in history.</p>')
        else:
            parts.append(f'<h2>History ({len(history)} entries)</h2>')
            for i, entry in enumerate(history):
                parts.append(f'<h3>#{i+1}</h3>')
                parts.append(self._entry_html(entry, is_current=False))

        parts.append('</body>')
        return "".join(parts)

    def _entry_html(self, entry: dict, is_current: bool = False) -> str:
        """Build HTML for a single settings entry."""
        cls = "history-entry current" if is_current else "history-entry"
        date = entry.get("set_date", "unknown")
        source = entry.get("source", "unknown")
        bt_period = entry.get("backtest_period", "")

        lines = [f'<div class="{cls}">']
        lines.append(f'<span class="date-label">{date}</span>')
        lines.append(f' &nbsp;|&nbsp; <span class="source-label">{source}</span>')
        if bt_period:
            lines.append(f' &nbsp;|&nbsp; <span class="date-label">Period: {bt_period}</span>')

        # Trading params (Stoch v2)
        sl_atr = entry.get("sl_atr", 0)
        tp_atr = entry.get("tp_atr", 0)
        sl_pips = entry.get("sl_pips", 0)
        tp_pips = entry.get("tp_pips", 0)
        if sl_atr > 0:
            lines.append(f'<br>SL=<b>{sl_atr}x</b> ATR (~{sl_pips:.1f}p) &nbsp;|&nbsp; '
                        f'TP=<b>{tp_atr}x</b> ATR (~{tp_pips:.1f}p) &nbsp;|&nbsp; '
                        f'R:R=1:{tp_atr/sl_atr:.1f}')

        # Stats
        trades = entry.get("trades", 0)
        if trades > 0:
            wr = entry.get("wr", 0)
            total_r = entry.get("total_r", 0)
            avg_final = entry.get("avg_final", 0)
            wr_class = "stat-good" if wr >= 60 else "stat-bad" if wr < 50 else "stat-neutral"
            r_class = "stat-good" if total_r > 0 else "stat-bad"
            lines.append(
                f'<br>Stats: '
                f'{trades} trades &nbsp;|&nbsp; '
                f'<span class="{wr_class}">WR {wr:.1f}%</span> &nbsp;|&nbsp; '
                f'<span class="{r_class}">{total_r:+.1f}R</span> &nbsp;|&nbsp; '
                f'Avg Final: {avg_final:+.1f} pips'
            )

        lines.append('</div>')
        return "".join(lines)

    def _restore_entry(self) -> None:
        """Restore the selected history entry as current."""
        idx = self._restore_combo.currentIndex()
        if idx < 0:
            return

        reply = QMessageBox.question(
            self,
            "Restore Settings",
            f"Restore history entry #{idx+1} as the current settings for {self._pair}?\n\n"
            "The current settings will be moved to history.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply == QMessageBox.StandardButton.Yes:
            if restore_from_history(self._pair, idx):
                self._load_history()
                QMessageBox.information(
                    self, "Restored",
                    f"Settings restored for {self._pair}."
                )
