"""TAKUMI Currency Strength Meter — Standalone App

Completely independent from the main TAKUMI Trader.
Uses its own MT5 connection. Run alongside the main app.

Usage:  python takumi_meter.py
"""

import sys
import logging
import time
from datetime import datetime

import numpy as np
import MetaTrader5 as mt5

from PyQt6.QtCore import Qt, QTimer, QSettings
from PyQt6.QtGui import QColor, QFont
from PyQt6.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
    QHeaderView,
)

logger = logging.getLogger(__name__)

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "NZD", "CHF"]

ALL_28_PAIRS = [
    "EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
    "EURGBP", "EURAUD", "EURNZD", "EURCAD", "EURCHF", "EURJPY",
    "GBPAUD", "GBPNZD", "GBPCAD", "GBPCHF", "GBPJPY",
    "AUDNZD", "AUDCAD", "AUDCHF", "AUDJPY",
    "NZDCAD", "NZDCHF", "NZDJPY",
    "CADCHF", "CADJPY",
    "CHFJPY",
]

# Map each currency to its pairs and base/quote role
_CCY_PAIRS: dict[str, list[tuple[str, bool]]] = {ccy: [] for ccy in CURRENCIES}
for _p in ALL_28_PAIRS:
    _CCY_PAIRS[_p[:3]].append((_p, True))
    _CCY_PAIRS[_p[3:]].append((_p, False))

# BIS Triennial Survey — global forex market share per currency (%)
# Used to weight pair contributions: pairs involving major counter-currencies
# have more impact on a currency's strength score.
_CCY_WEIGHT: dict[str, float] = {
    "USD": 88.5,
    "EUR": 30.5,
    "JPY": 16.7,
    "GBP": 12.9,
    "AUD":  6.4,
    "CAD":  6.2,
    "CHF":  5.2,
    "NZD":  1.7,
}

def _counter_weight(pair: str, ccy: str) -> float:
    """Get the BIS weight of the counter-currency in a pair.

    For AUD computing via AUDUSD: counter is USD → weight 88.5
    For CHF computing via NZDCHF: counter is NZD → weight 1.7
    """
    base, quote = pair[:3], pair[3:]
    counter = quote if ccy == base else base
    return _CCY_WEIGHT.get(counter, 5.0)

# Timeframes: (label, MT5 constant, RSI period, fetch bars)
# Shorter periods for faster TFs (more reactive), longer for slower TFs (smoother)
# Per-TF config: (label, mt5_const, method, period, power, fetch_bars)
# Optimized per-TF to match QM4 FSM (v3 — calibrated from simultaneous screenshots):
#   M15: Stochastic(3) power 0.7 — very close match (avg err ~1.0)
#   H1:  Stochastic(5) power 0.5 — needs spread, QM4 shows 0.8-9.5
#   H4:  Stochastic(7) power 0.5 — moderate spread
#   D1+: Blend RSI(14)+Stoch(14) power 0.5 — long-term trend
# v6: Back to Stochastic (proven best for extremes) + shorter HTF lookbacks
#   Stoch produces 0-100 range → power scaling pushes to match QM4 extremes
#   Short lookbacks on D1/W1/MN (5 bars) for recent macro momentum
TIMEFRAMES = [
    ("M15", mt5.TIMEFRAME_M15, "stoch",  2,  0.7,  30),
    ("H1",  mt5.TIMEFRAME_H1,  "stoch",  5,  0.5,  40),
    ("H4",  mt5.TIMEFRAME_H4,  "stoch",  7,  0.5,  50),
    ("D1",  mt5.TIMEFRAME_D1,  "stoch",  5,  0.5,  30),
    ("W1",  mt5.TIMEFRAME_W1,  "stoch",  5,  0.5,  30),
    ("MN",  mt5.TIMEFRAME_MN1, "stoch",  5,  0.5,  30),
]


def _rsi_wilder(closes: np.ndarray, period: int) -> float:
    """Compute Wilder's smoothed RSI."""
    if len(closes) < period + 2:
        return 50.0
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    if len(gains) < period:
        return 50.0
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)


def _cell_color(value: float) -> tuple[QColor, QColor]:
    """Return (background, foreground) colors — extremes only.

    Only highlight clear extremes:
    - >= 9.0: green (very strong)
    - <= 1.0: red (very weak)
    - Everything else: neutral
    """
    if value >= 9.0:
        return QColor("#1a8a1a"), QColor("#ffffff")
    elif value <= 1.0:
        return QColor("#c62828"), QColor("#ffffff")
    else:
        return QColor("#f8f8f8"), QColor("#333333")


def _stochastic(closes: np.ndarray, highs: np.ndarray, lows: np.ndarray, period: int) -> float:
    """Compute Stochastic Oscillator %K."""
    if len(closes) < period:
        return 50.0
    h = np.max(highs[-period:])
    l = np.min(lows[-period:])
    if h == l:
        return 50.0
    return ((closes[-1] - l) / (h - l)) * 100.0


def compute_scores() -> dict[tuple[str, str], float]:
    """Compute currency strength per pair using Stochastic/RSI/Blend.

    For each pair, computes the indicator value (0-100), then averages
    across all 7 pairs per currency (adjusted for base/quote).
    Power scaling pushes values toward extremes for clearer signals.

    This is our proven best match to QM4 FSM (~1.0 avg error on M15).
    """
    scores: dict[tuple[str, str], float] = {}

    for tf_label, tf_const, method, period, power, fetch_bars in TIMEFRAMES:
        pair_scores: dict[str, float] = {}
        for pair in ALL_28_PAIRS:
            try:
                rates = mt5.copy_rates_from_pos(pair, tf_const, 0, fetch_bars)
                if rates is None or len(rates) < period + 2:
                    continue
                closes = rates["close"].astype(np.float64)
                highs = rates["high"].astype(np.float64)
                lows = rates["low"].astype(np.float64)

                if method == "stoch":
                    pair_scores[pair] = _stochastic(closes, highs, lows, period)
                elif method == "rsi":
                    pair_scores[pair] = _rsi_wilder(closes, period)
                elif method == "blend":
                    rv = _rsi_wilder(closes, period)
                    sv = _stochastic(closes, highs, lows, period)
                    pair_scores[pair] = rv * 0.5 + sv * 0.5
            except Exception:
                continue

        for ccy in CURRENCIES:
            vals: list[float] = []
            for pair, is_base in _CCY_PAIRS[ccy]:
                if pair in pair_scores:
                    v = pair_scores[pair]
                    vals.append(v if is_base else 100.0 - v)
            if vals:
                avg = float(np.mean(vals))
                if power != 1.0:
                    centered = (avg - 50.0) / 50.0
                    scaled = np.sign(centered) * abs(centered) ** power
                    raw_score = (scaled + 1.0) * 5.0
                else:
                    raw_score = avg / 10.0
                scores[(ccy, tf_label)] = round(max(0.0, min(10.0, raw_score)), 1)
            else:
                scores[(ccy, tf_label)] = 5.0

    return scores


class MeterWindow(QWidget):
    """Standalone currency strength meter window."""

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Currency Strength Meter")
        self.setWindowFlags(
            Qt.WindowType.Window | Qt.WindowType.WindowStaysOnTopHint
        )

        # Restore geometry
        s = QSettings("TAKUMITrader", "Meter")
        geo = s.value("geometry")
        if geo:
            self.restoreGeometry(geo)
        else:
            self.resize(520, 300)

        self._mt5_ok = False
        self._running = True

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)

        # Header
        header = QHBoxLayout()
        title = QLabel("Currency Strength Data")
        title.setFont(QFont("Segoe UI", 14, QFont.Weight.Bold))
        title.setStyleSheet("color: white;")
        header.addWidget(title)

        self._time_label = QLabel("--:--:--")
        self._time_label.setFont(QFont("Segoe UI", 14))
        self._time_label.setStyleSheet("color: white;")
        self._time_label.setAlignment(Qt.AlignmentFlag.AlignRight)
        header.addWidget(self._time_label)

        self._data_btn = QPushButton("DATA")
        self._data_btn.setCheckable(True)
        self._data_btn.setChecked(True)
        self._data_btn.setStyleSheet(
            "QPushButton { background: #4a90d9; color: white; padding: 4px 14px;"
            " border-radius: 3px; font-weight: bold; font-size: 13px; }"
            " QPushButton:hover { background: #3a7bc8; }"
        )
        self._data_btn.toggled.connect(self._on_toggle)
        header.addWidget(self._data_btn)
        layout.addLayout(header)

        # Table
        tf_labels = [tf[0] for tf in TIMEFRAMES]
        self._table = QTableWidget(len(TIMEFRAMES), len(CURRENCIES))
        self._table.setHorizontalHeaderLabels(CURRENCIES)
        self._table.setVerticalHeaderLabels(tf_labels)
        self._table.verticalHeader().setDefaultSectionSize(36)
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self._table.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        self._table.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self._table.setStyleSheet("""
            QTableWidget {
                border: 1px solid #555; gridline-color: #444;
                font-size: 16px; font-weight: bold;
                color: #222;
            }
            QHeaderView::section {
                background: #e8e8e8; color: #222;
                padding: 4px; border: 1px solid #999;
                font-weight: bold; font-size: 14px;
            }
        """)

        for row in range(len(TIMEFRAMES)):
            for col in range(len(CURRENCIES)):
                item = QTableWidgetItem("—")
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self._table.setItem(row, col, item)

        layout.addWidget(self._table)

        # Status
        self._status = QLabel("Connecting to MT5...")
        self._status.setStyleSheet("color: #888; font-size: 11px;")
        layout.addWidget(self._status)

        # Timer — refresh every 3 seconds
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._update)
        self._timer.start(1000)

        # Initial update
        QTimer.singleShot(500, self._connect_mt5)

    def _connect_mt5(self) -> None:
        """Initialize MT5 connection."""
        if mt5.initialize():
            info = mt5.account_info()
            broker = info.server if info else "Unknown"
            self._mt5_ok = True
            self._status.setText(f"Connected: {broker}")
            self._update()
        else:
            self._status.setText("MT5 connection failed — retrying in 5s...")
            QTimer.singleShot(5000, self._connect_mt5)

    def _on_toggle(self, checked: bool) -> None:
        if checked:
            self._timer.start(1000)
            self._data_btn.setStyleSheet(
                "QPushButton { background: #4a90d9; color: white; padding: 4px 14px;"
                " border-radius: 3px; font-weight: bold; font-size: 13px; }"
            )
        else:
            self._timer.stop()
            self._data_btn.setStyleSheet(
                "QPushButton { background: #ccc; color: #666; padding: 4px 14px;"
                " border-radius: 3px; font-weight: bold; font-size: 13px; }"
            )

    def _update(self) -> None:
        """Fetch and display scores."""
        if not self._mt5_ok or not self._data_btn.isChecked():
            return

        try:
            t0 = time.time()
            scores = compute_scores()
            elapsed = (time.time() - t0) * 1000

            now = datetime.now()
            self._time_label.setText(now.strftime("%H:%M:%S"))

            for row, (tf_label, *_rest) in enumerate(TIMEFRAMES):
                for col, ccy in enumerate(CURRENCIES):
                    value = scores.get((ccy, tf_label), 5.0)
                    item = self._table.item(row, col)
                    if item:
                        item.setText(f"{value:.1f}")
                        bg, fg = _cell_color(value)
                        item.setBackground(bg)
                        item.setForeground(fg)

            self._status.setText(f"Updated {now.strftime('%H:%M:%S')} ({elapsed:.0f}ms)")

        except Exception as e:
            self._status.setText(f"Error: {e}")

    def closeEvent(self, event) -> None:
        """Save geometry and shut down."""
        s = QSettings("TAKUMITrader", "Meter")
        s.setValue("geometry", self.saveGeometry())
        mt5.shutdown()
        event.accept()


def main():
    logging.basicConfig(level=logging.INFO)
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    window = MeterWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
