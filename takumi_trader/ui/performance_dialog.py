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


# ── Deep analytics columns helper ───────────────────────────────
def _populate_analytics_cols(table, row: int, r, start_col: int) -> None:
    """Fill 21 analytics columns starting at start_col. Shared by all tables."""
    def _set_num(col, val, fmt="{:.1f}", color=None, zero_as_dots=True):
        if zero_as_dots and val == 0:
            table.setItem(row, col, QTableWidgetItem("..."))
            return
        item = _NumericItem(fmt.format(val), float(val))
        if color:
            item.setForeground(QColor(color))
        table.setItem(row, col, item)

    # Vol× — tick volume ratio
    _v = getattr(r, "entry_tick_volume_ratio", 0.0)
    if _v > 0:
        _c = "#1b8a2a" if _v >= 1.3 else "#c62828" if _v < 0.7 else "#888"
        _set_num(start_col + 0, _v, "{:.2f}", _c)
    else:
        table.setItem(row, start_col + 0, QTableWidgetItem("..."))

    # Build — momentum buildup seconds
    _b = getattr(r, "entry_momentum_buildup_sec", 0)
    if _b > 0:
        if _b < 60:
            _txt = f"{_b}s"
        else:
            _txt = f"{_b // 60}m"
        _c = "#c62828" if _b < 30 else "#e65100" if _b < 300 else "#1b8a2a"
        _item = _NumericItem(_txt, float(_b))
        _item.setForeground(QColor(_c))
        table.setItem(row, start_col + 1, _item)
    else:
        table.setItem(row, start_col + 1, QTableWidgetItem("..."))

    # DDayH, DDayL, DWkH, DWkL, DMoH, DMoL — distance to key levels (signed pips)
    for i, fld in enumerate(("entry_dist_day_high_pips", "entry_dist_day_low_pips",
                              "entry_dist_week_high_pips", "entry_dist_week_low_pips",
                              "entry_dist_month_high_pips", "entry_dist_month_low_pips")):
        _d = getattr(r, fld, 0.0)
        if _d != 0.0:
            _c = "#c62828" if abs(_d) < 10 else "#888"
            _set_num(start_col + 2 + i, _d, "{:+.0f}", _c, zero_as_dots=False)
        else:
            table.setItem(row, start_col + 2 + i, QTableWidgetItem("..."))

    # Cluster count
    _cl = getattr(r, "entry_cluster_count", 0)
    _c_col = "#1b8a2a" if _cl >= 2 else "#888"
    _set_num(start_col + 8, _cl, "{:.0f}", _c_col, zero_as_dots=False)

    # D00 — distance to nearest 100-pip round number (signed)
    _d00 = getattr(r, "entry_dist_00_pips", 0.0)
    if _d00 != 0.0 or getattr(r, "entry_price", 0) > 0:
        _c = "#c62828" if abs(_d00) < 15 else "#e65100" if abs(_d00) < 30 else "#888"
        _set_num(start_col + 9, _d00, "{:+.0f}", _c, zero_as_dots=False)
    else:
        table.setItem(row, start_col + 9, QTableWidgetItem("..."))

    # D000 — distance to nearest 1000-pip round number (signed)
    _d000 = getattr(r, "entry_dist_000_pips", 0.0)
    if _d000 != 0.0 or getattr(r, "entry_price", 0) > 0:
        _c = "#c62828" if abs(_d000) < 50 else "#e65100" if abs(_d000) < 100 else "#888"
        _set_num(start_col + 10, _d000, "{:+.0f}", _c, zero_as_dots=False)
    else:
        table.setItem(row, start_col + 10, QTableWidgetItem("..."))

    # Session minutes
    _sm = getattr(r, "entry_session_minutes_in", 0)
    _set_num(start_col + 11, _sm, "{:.0f}", None, zero_as_dots=False)

    # Day of week
    _dow = getattr(r, "entry_day_of_week", 0)
    _dow_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    table.setItem(row, start_col + 12, QTableWidgetItem(_dow_names[_dow] if 0 <= _dow < 7 else "..."))

    # Previous trade result
    _pr = getattr(r, "entry_prev_trade_result", "")
    if _pr:
        _pr_item = QTableWidgetItem(_pr)
        _pr_item.setForeground(QColor("#1b8a2a" if _pr == "win" else "#c62828"))
        table.setItem(row, start_col + 13, _pr_item)
    else:
        table.setItem(row, start_col + 13, QTableWidgetItem("..."))

    # Concurrent open trades
    _ct = getattr(r, "entry_concurrent_trades", 0)
    _set_num(start_col + 14, _ct, "{:.0f}", None, zero_as_dots=False)

    # M1 body %
    _mb = getattr(r, "entry_m1_body_pct", 0.0)
    if _mb > 0:
        _c = "#1b8a2a" if _mb >= 60 else "#e65100" if _mb >= 30 else "#c62828"
        _set_num(start_col + 15, _mb, "{:.0f}%", _c)
    else:
        table.setItem(row, start_col + 15, QTableWidgetItem("..."))

    # M1 direction
    _md = getattr(r, "entry_m1_direction", "")
    if _md:
        _md_item = QTableWidgetItem(_md)
        _md_colors = {"bull": "#1b8a2a", "bear": "#c62828", "doji": "#888"}
        _md_item.setForeground(QColor(_md_colors.get(_md, "#888")))
        table.setItem(row, start_col + 16, _md_item)
    else:
        table.setItem(row, start_col + 16, QTableWidgetItem("..."))

    # ATR ratio
    _ar = getattr(r, "entry_atr_ratio", 0.0)
    if _ar > 0:
        _c = "#c62828" if _ar < 0.7 else "#1b8a2a" if _ar > 1.3 else "#888"
        _set_num(start_col + 17, _ar, "{:.2f}", _c)
    else:
        table.setItem(row, start_col + 17, QTableWidgetItem("..."))

    # Journey: T2P (time to +5p profit in minutes)
    _t2p = getattr(r, "time_to_5p_profit_min", -1.0)
    if _t2p >= 0:
        _set_num(start_col + 18, _t2p, "{:.0f}m", "#1b8a2a" if _t2p < 30 else "#888", zero_as_dots=False)
    else:
        table.setItem(row, start_col + 18, QTableWidgetItem("never"))

    # PF1st (went profit first)
    _pf = getattr(r, "went_profit_first", False)
    _pf_item = QTableWidgetItem("Yes" if _pf else "No")
    _pf_item.setForeground(QColor("#1b8a2a" if _pf else "#c62828"))
    table.setItem(row, start_col + 19, _pf_item)

    # NrSL (near-SL count)
    _nsl = getattr(r, "near_sl_count", 0)
    _c = "#c62828" if _nsl >= 3 else "#e65100" if _nsl >= 1 else "#888"
    _set_num(start_col + 20, _nsl, "{:.0f}", _c, zero_as_dots=False)

    # NrTP (near-TP count)
    _ntp = getattr(r, "near_tp_count", 0)
    _c = "#1b8a2a" if _ntp >= 1 else "#888"
    _set_num(start_col + 21, _ntp, "{:.0f}", _c, zero_as_dots=False)

    # Bars (M1 bars to close)
    _bars = getattr(r, "bars_to_close", 0)
    _set_num(start_col + 22, _bars, "{:.0f}", None, zero_as_dots=False)

    # ── Conviction breakdown (4 sub-scores) ──
    _ct = getattr(r, "entry_conv_trend", 0)
    _set_num(start_col + 23, _ct, "{:.0f}", "#1b8a2a" if _ct >= 20 else "#e65100" if _ct >= 10 else "#888")
    _cv = getattr(r, "entry_conv_velocity", 0)
    _set_num(start_col + 24, _cv, "{:.0f}", "#1b8a2a" if _cv >= 14 else "#e65100" if _cv >= 7 else "#888")
    _ci = getattr(r, "entry_conv_isolation", 0)
    _set_num(start_col + 25, _ci, "{:.0f}", "#1b8a2a" if _ci >= 14 else "#e65100" if _ci >= 7 else "#888")
    _cs_struct = getattr(r, "entry_conv_structural", 0)
    _set_num(start_col + 26, _cs_struct, "{:.0f}", "#1b8a2a" if _cs_struct >= 10 else "#e65100" if _cs_struct >= 5 else "#888")

    # ── Pair-specific SL/TP ATR multipliers ──
    _slx = getattr(r, "entry_sl_atr_mult", 0.0)
    _set_num(start_col + 27, _slx, "{:.2f}", "#888")
    _tpx = getattr(r, "entry_tp_atr_mult", 0.0)
    _set_num(start_col + 28, _tpx, "{:.2f}", "#888")

    # ── Strong/Weak currency labels ──
    _sccy = getattr(r, "entry_strong_ccy", "")
    table.setItem(row, start_col + 29, QTableWidgetItem(_sccy or "..."))
    _wccy = getattr(r, "entry_weak_ccy", "")
    table.setItem(row, start_col + 30, QTableWidgetItem(_wccy or "..."))

    # ── Currency ranks (1-8) ──
    _sr = getattr(r, "entry_strong_rank", 0)
    _set_num(start_col + 31, _sr, "{:.0f}", "#1b8a2a" if _sr == 1 else "#e65100" if _sr == 2 else "#888")
    _wr = getattr(r, "entry_weak_rank", 0)
    _set_num(start_col + 32, _wr, "{:.0f}", "#1b8a2a" if _wr == 8 else "#e65100" if _wr == 7 else "#888")

    # ── Isolation gaps ──
    _tg = getattr(r, "entry_strong_top_gap", 0.0)
    if _tg != 0.0:
        _c = "#1b8a2a" if _tg >= 2.0 else "#e65100" if _tg >= 1.0 else "#c62828"
        _set_num(start_col + 33, _tg, "{:.1f}", _c, zero_as_dots=False)
    else:
        table.setItem(row, start_col + 33, QTableWidgetItem("..."))
    _bg = getattr(r, "entry_weak_bottom_gap", 0.0)
    if _bg != 0.0:
        _c = "#1b8a2a" if _bg >= 2.0 else "#e65100" if _bg >= 1.0 else "#c62828"
        _set_num(start_col + 34, _bg, "{:.1f}", _c, zero_as_dots=False)
    else:
        table.setItem(row, start_col + 34, QTableWidgetItem("..."))

    # ── Velocities (points/min, signed) ──
    _svel = getattr(r, "entry_strong_velocity", 0.0)
    if _svel != 0.0:
        _c = "#1b8a2a" if abs(_svel) >= 0.6 else "#888"
        _set_num(start_col + 35, _svel, "{:+.2f}", _c, zero_as_dots=False)
    else:
        table.setItem(row, start_col + 35, QTableWidgetItem("..."))
    _wvel = getattr(r, "entry_weak_velocity", 0.0)
    if _wvel != 0.0:
        _c = "#1b8a2a" if abs(_wvel) >= 0.6 else "#888"
        _set_num(start_col + 36, _wvel, "{:+.2f}", _c, zero_as_dots=False)
    else:
        table.setItem(row, start_col + 36, QTableWidgetItem("..."))

    # ── M5 TR slope ratio ──
    _slope = getattr(r, "entry_m5_tr_slope_ratio", 0.0)
    if _slope > 0:
        _c = "#1b8a2a" if _slope >= 1.2 else "#e65100" if _slope >= 1.0 else "#c62828"
        _set_num(start_col + 37, _slope, "{:.2f}", _c)
    else:
        table.setItem(row, start_col + 37, QTableWidgetItem("..."))

    # ── Minutes since last news on either currency ──
    _nm = getattr(r, "entry_minutes_since_news", -1.0)
    if _nm >= 0:
        if _nm < 60:
            _txt = f"{_nm:.0f}m"
        elif _nm < 1440:
            _txt = f"{_nm / 60:.1f}h"
        else:
            _txt = f"{_nm / 1440:.1f}d"
        _c = "#c62828" if _nm < 30 else "#e65100" if _nm < 240 else "#888"
        _item = _NumericItem(_txt, float(_nm))
        _item.setForeground(QColor(_c))
        table.setItem(row, start_col + 38, _item)
    else:
        table.setItem(row, start_col + 38, QTableWidgetItem("..."))


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

    _VIEW_SIZE = 100  # max visible trades in viewport

    # Per-source overlay colors (used when split_by_attr is active)
    _SPLIT_COLORS: dict[str, QColor] = {
        "sv2_ss":      QColor(156,  39, 176, 150),  # purple ~59% alpha
        "sv2_atr":     QColor(230,  81,   0, 150),  # orange ~59% alpha
        "sv2_b_tuned": QColor(  0, 131, 143, 150),  # cyan ~59% alpha
    }
    _SPLIT_LABELS: dict[str, str] = {
        "sv2_ss":      "SS",
        "sv2_atr":     "ATR",
        "sv2_b_tuned": "B-tn",
    }

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._equity_points: list[float] = []   # cumulative equity values
        self._trade_results: list[bool] = []    # win/loss per trade
        self._records: list[PaperTradeRecord] = []  # original trade records
        self._start_capital = 1000.0
        self._risk_pct = 0.03                   # 3%
        self._hover_index: int = -1             # global index of hovered dot (-1 = none)
        self._view_start: int = -1              # -1 = auto (show last 100)
        self._drag_last_x: float | None = None  # for click-drag panning
        # Per-source sub-curves: { src_key: [balance_after_each_global_trade] }
        # Each list has the same length as _equity_points. A source's balance
        # only changes on its own trades; otherwise it carries forward.
        self._split_curves: dict[str, list[float]] = {}
        self.setMinimumSize(280, 200)
        self.setMouseTracking(True)

    def set_data(
        self,
        records: list[PaperTradeRecord],
        start_capital: float = 1000.0,
        risk_pct: float = 0.03,
        split_by_attr: str | None = None,
    ) -> None:
        """Compute equity curve from trade records.

        split_by_attr: optional record attribute to split into faint per-group
        sub-curves drawn behind the main curve (e.g. "dtc_source_system").
        """
        self._start_capital = start_capital
        self._risk_pct = risk_pct
        self._equity_points = [start_capital]
        self._trade_results = []
        self._split_curves = {}
        # Equity curve: only closed trades have a real outcome.
        # OPEN trades have pnl_pips=0 + is_win=False — would add flat-line phantom losses.
        closed_only = [r for r in records if r.close_reason]
        self._records = closed_only

        balance = start_capital
        for r in closed_only:
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

        # Per-group sub-curves. Each starts at start_capital and only updates
        # on its own group's trades (carries forward otherwise). Drawn on the
        # same global x-axis as the main curve so the user sees timing too.
        if split_by_attr:
            # Discover groups
            _group_keys: set[str] = set()
            for r in closed_only:
                _g = (getattr(r, split_by_attr, "") or "")
                if _g:
                    _group_keys.add(_g)
            # Initialize: same length as _equity_points, all start at start_capital
            for _gk in _group_keys:
                self._split_curves[_gk] = [start_capital]
            # Walk trades; for each trade, only the matching group compounds
            _group_balance = {gk: start_capital for gk in _group_keys}
            for r in closed_only:
                _g = (getattr(r, split_by_attr, "") or "")
                if _g in _group_balance:
                    _ra = _group_balance[_g] * risk_pct
                    _rm = r.pnl_pips / r.sl_pips if r.sl_pips > 0 else r.pnl_pips / 10.0
                    _group_balance[_g] += _ra * _rm
                # All groups append (the non-matching ones stay flat)
                for _gk in _group_keys:
                    self._split_curves[_gk].append(round(_group_balance[_gk], 2))

        # Auto-scroll to end
        self._view_start = -1
        self.update()

    def _visible_range(self) -> tuple[int, int]:
        """Return (start, end) indices into _equity_points for the visible window."""
        n = len(self._equity_points)
        if n <= self._VIEW_SIZE + 1:
            return 0, n
        if self._view_start < 0:
            # Auto = show last VIEW_SIZE trades (VIEW_SIZE+1 points)
            return n - self._VIEW_SIZE - 1, n
        start = max(0, min(self._view_start, n - self._VIEW_SIZE - 1))
        return start, start + self._VIEW_SIZE + 1

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        w = self.width()
        h = self.height()

        # Background
        painter.fillRect(0, 0, w, h, self._BG)

        # Margins (extra bottom for date labels + scrollbar)
        ml, mr, mt, mb = 58, 18, 36, 48
        cw = w - ml - mr   # chart width
        ch = h - mt - mb   # chart height

        if cw < 40 or ch < 40:
            painter.end()
            return

        all_pts = self._equity_points
        total_trades = len(all_pts) - 1  # number of trades

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

        if len(all_pts) < 2:
            painter.setFont(QFont("Segoe UI", 10))
            painter.setPen(self._AXIS)
            painter.drawText(
                QRectF(ml, mt, cw, ch), Qt.AlignmentFlag.AlignCenter,
                "No trades yet"
            )
            painter.end()
            return

        # Visible window
        vs, ve = self._visible_range()
        pts = all_pts[vs:ve]
        n = len(pts)
        if n < 2:
            painter.end()
            return

        # Y-range computed across MAIN curve + any visible sub-curves so that
        # an extreme sub-curve doesn't clip off-screen.
        _y_pool = list(pts)
        if self._split_curves:
            for _gk, _gpts in self._split_curves.items():
                if len(_gpts) >= ve:
                    _y_pool.extend(_gpts[vs:ve])
        y_min = min(_y_pool) * 0.98
        y_max = max(_y_pool) * 1.02
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
        for i in range(6):
            frac = i / 5
            val = y_min + frac * (y_max - y_min)
            yy = to_y(val)
            painter.setPen(QPen(self._GRID, 1, Qt.PenStyle.DashLine))
            painter.drawLine(QPointF(ml, yy), QPointF(ml + cw, yy))
            painter.setPen(self._AXIS)
            label = f"${val:,.0f}"
            painter.drawText(QRectF(2, yy - 8, ml - 6, 16),
                             Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                             label)

        # Baseline (starting capital) — only if visible
        if y_min <= self._start_capital * 1.02 and y_max >= self._start_capital * 0.98:
            base_y = to_y(self._start_capital)
            painter.setPen(QPen(self._BASELINE, 1, Qt.PenStyle.DotLine))
            painter.drawLine(QPointF(ml, base_y), QPointF(ml + cw, base_y))

        # ── Per-source sub-curves (drawn UNDER the main equity line) ──
        # Each source's curve compounds only on its own trades, so it's a
        # stair-stepped line. We draw them faintly so the main curve still
        # dominates visually.
        if self._split_curves:
            # Stable color order — sort by total profit desc so the strongest
            # source's legend entry appears first.
            _split_sorted = sorted(self._split_curves.items(),
                                    key=lambda kv: -(kv[1][-1] - self._start_capital))
            for _gk, _gpts in _split_sorted:
                if len(_gpts) < ve:
                    continue  # shouldn't happen, but be safe
                _color = self._SPLIT_COLORS.get(_gk, QColor(120, 120, 120, 130))
                painter.setPen(QPen(_color, 1.4, Qt.PenStyle.SolidLine))
                painter.setBrush(Qt.BrushStyle.NoBrush)
                _spath = QPainterPath()
                _spath.moveTo(to_x(0), to_y(_gpts[vs]))
                for _i in range(1, n):
                    _spath.lineTo(to_x(_i), to_y(_gpts[vs + _i]))
                painter.drawPath(_spath)

            # Legend in upper-left of chart area
            _legend_font = QFont("Segoe UI", 8, QFont.Weight.Bold)
            painter.setFont(_legend_font)
            _lfm = QFontMetrics(_legend_font)
            _lx = ml + 8
            _ly = mt + 6
            for _gk, _gpts in _split_sorted:
                _label = self._SPLIT_LABELS.get(_gk, _gk[:6])
                _final_d = _gpts[-1] - self._start_capital
                _label_full = f"{_label}  ${_gpts[-1]:,.0f}  ({_final_d:+,.0f})"
                _color = self._SPLIT_COLORS.get(_gk, QColor(120, 120, 120, 200))
                # Color swatch
                painter.setPen(Qt.PenStyle.NoPen)
                painter.setBrush(_color)
                painter.drawRect(QRectF(_lx, _ly + 4, 10, 3))
                # Label text
                painter.setPen(_color)
                painter.drawText(QRectF(_lx + 14, _ly, 200, 14),
                                  Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                                  _label_full)
                _ly += 14

        # Build path for the equity line
        path = QPainterPath()
        path.moveTo(to_x(0), to_y(pts[0]))
        for i in range(1, n):
            path.lineTo(to_x(i), to_y(pts[i]))

        # Fill area between line and bottom of visible range
        fill_base_val = pts[0]  # use first visible point as fill reference
        fill_base_y = to_y(fill_base_val)
        fill_path = QPainterPath(path)
        fill_path.lineTo(to_x(n - 1), fill_base_y)
        fill_path.lineTo(to_x(0), fill_base_y)
        fill_path.closeSubpath()

        final_above = pts[-1] >= pts[0]
        painter.setBrush(QBrush(self._FILL_WIN if final_above else self._FILL_LOSS))
        painter.setPen(Qt.PenStyle.NoPen)
        painter.drawPath(fill_path)

        # Draw the equity line
        line_color = self._LINE_WIN if final_above else self._LINE_LOSS
        painter.setPen(QPen(line_color, 2.2, Qt.PenStyle.SolidLine))
        painter.setBrush(Qt.BrushStyle.NoBrush)
        painter.drawPath(path)

        # Trade dots
        for i in range(1, n):
            cx = to_x(i)
            cy = to_y(pts[i])
            global_idx = vs + i  # index into _trade_results / _records
            is_win = self._trade_results[global_idx - 1] if global_idx - 1 < len(self._trade_results) else True
            dot_color = self._DOT_WIN if is_win else self._DOT_LOSS
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(dot_color)
            painter.drawEllipse(QPointF(cx, cy), 3.5, 3.5)

        # X-axis date/time labels
        from datetime import datetime, timezone, timedelta
        _JST = timezone(timedelta(hours=9))
        xaxis_font = QFont("Segoe UI", 8)
        painter.setFont(xaxis_font)
        xfm = QFontMetrics(xaxis_font)
        painter.setPen(self._AXIS)

        # Collect entry times for visible trades
        _rec_times: list[float] = []
        for i in range(n):
            gi = vs + i  # global equity-point index
            ri = gi - 1  # record index (equity[0] = start, no record)
            if 0 <= ri < len(self._records):
                _rec_times.append(self._records[ri].entry_time)
            elif i == 0 and ri < 0 and len(self._records) > 0:
                # First point (start capital) — use first record's time
                _rec_times.append(self._records[0].entry_time)
            else:
                _rec_times.append(0.0)

        if _rec_times and any(t > 0 for t in _rec_times):
            t_min = min(t for t in _rec_times if t > 0)
            t_max = max(t for t in _rec_times if t > 0)
            t_span = t_max - t_min
            # Choose interval: 4h if span < 3 days, 8h otherwise
            if t_span < 3 * 86400:
                interval = 4 * 3600  # 4 hours
            else:
                interval = 8 * 3600  # 8 hours

            # Find time ticks aligned to interval
            first_tick = (int(t_min) // interval + 1) * interval
            ticks: list[float] = []
            t = first_tick
            while t <= t_max:
                ticks.append(t)
                t += interval

            # Min pixel spacing between labels
            min_px = xfm.horizontalAdvance("04-08 12:00") + 16
            last_drawn_x = -999.0

            for tick in ticks:
                # Find which visible point is closest to this tick
                best_i = 0
                best_dt = abs(_rec_times[0] - tick) if _rec_times[0] > 0 else 1e18
                for i in range(1, n):
                    if _rec_times[i] > 0:
                        dt = abs(_rec_times[i] - tick)
                        if dt < best_dt:
                            best_dt = dt
                            best_i = i
                xx = to_x(best_i)
                if xx - last_drawn_x < min_px:
                    continue
                dt_obj = datetime.fromtimestamp(tick, tz=_JST)
                lbl = dt_obj.strftime("%m-%d %H:%M")
                painter.drawText(QRectF(xx - 40, mt + ch + 4, 80, 16),
                                 Qt.AlignmentFlag.AlignCenter, lbl)
                last_drawn_x = xx

        # Final visible equity value label
        final_val = pts[-1]
        pnl_pct = (final_val - self._start_capital) / self._start_capital * 100
        final_color = self._LINE_WIN if final_val >= self._start_capital else self._LINE_LOSS
        val_font = QFont("Segoe UI", 9, QFont.Weight.Bold)
        painter.setFont(val_font)
        painter.setPen(QPen(final_color))
        fx = to_x(n - 1)
        fy = to_y(final_val)
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

        # ── Scrollbar (only if scrollable) ──
        if total_trades > self._VIEW_SIZE:
            sb_y = mt + ch + 32
            sb_h = 6
            sb_full_w = cw
            # Thumb proportional size
            ratio = self._VIEW_SIZE / total_trades
            thumb_w = max(30, sb_full_w * ratio)
            max_scroll = total_trades - self._VIEW_SIZE
            scroll_pos = vs / max_scroll if max_scroll > 0 else 1.0
            thumb_x = ml + scroll_pos * (sb_full_w - thumb_w)

            # Track
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(QColor("#e0e0e0"))
            painter.drawRoundedRect(QRectF(ml, sb_y, sb_full_w, sb_h), 3, 3)
            # Thumb
            painter.setBrush(QColor("#aaaaaa"))
            painter.drawRoundedRect(QRectF(thumb_x, sb_y, thumb_w, sb_h), 3, 3)

        # ── Hover tooltip ──
        hi = self._hover_index  # global index
        if vs < hi < ve and hi - 1 < len(self._records):
            rec = self._records[hi - 1]
            local_i = hi - vs
            cx = to_x(local_i)
            cy = to_y(all_pts[hi])

            # Highlight dot
            painter.setPen(QPen(QColor("#ffffff"), 2))
            dot_c = self._DOT_WIN if rec.is_win else self._DOT_LOSS
            painter.setBrush(dot_c)
            painter.drawEllipse(QPointF(cx, cy), 6, 6)

            # Tooltip text
            lines = [
                f"#{hi}  {rec.pair}  {rec.direction}",
                f"{rec.entry_time_str}",
                f"PnL: {rec.pnl_pips:+.1f} pips  ({'WIN' if rec.is_win else 'LOSS'})",
                f"SL: {rec.sl_pips:.1f}  TP: {rec.tp_pips:.1f}",
                f"Equity: ${all_pts[hi]:,.0f}",
            ]
            if rec.close_reason:
                lines.insert(3, f"Exit: {rec.close_reason}")

            tip_font = QFont("Segoe UI", 16)
            painter.setFont(tip_font)
            tfm = QFontMetrics(tip_font)
            line_h = tfm.height() + 2
            tip_w = max(tfm.horizontalAdvance(ln) for ln in lines) + 16
            tip_h = line_h * len(lines) + 10

            tx = cx + 12
            ty = cy - tip_h / 2
            if tx + tip_w > ml + cw:
                tx = cx - tip_w - 12
            if ty < mt:
                ty = mt + 2
            if ty + tip_h > mt + ch:
                ty = mt + ch - tip_h - 2

            tip_rect = QRectF(tx, ty, tip_w, tip_h)
            painter.setPen(QPen(QColor("#cccccc"), 1))
            painter.setBrush(QColor(255, 255, 255, 240))
            painter.drawRoundedRect(tip_rect, 4, 4)

            painter.setPen(QColor("#333333"))
            for li, line in enumerate(lines):
                painter.drawText(
                    QRectF(tx + 8, ty + 5 + li * line_h, tip_w - 16, line_h),
                    Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                    line,
                )

        painter.end()

    # ── Mouse interaction ──

    def mouseMoveEvent(self, event) -> None:  # noqa: N802
        """Detect hovered dot + drag-pan."""
        mx = event.position().x()
        my = event.position().y()

        # Drag panning
        if self._drag_last_x is not None:
            dx = mx - self._drag_last_x
            self._drag_last_x = mx
            self._pan_by_pixels(-dx)
            return

        # Hover detection
        all_pts = self._equity_points
        if len(all_pts) < 2:
            return

        w = self.width()
        h = self.height()
        ml, mr, mt, mb = 58, 18, 36, 48
        cw = w - ml - mr
        ch = h - mt - mb
        if cw < 40 or ch < 40:
            return

        vs, ve = self._visible_range()
        pts = all_pts[vs:ve]
        n = len(pts)
        if n < 2:
            return

        y_min = min(pts) * 0.98
        y_max = max(pts) * 1.02
        if y_max == y_min:
            y_max = y_min + 1

        best = -1
        best_dist = 15.0
        for i in range(1, n):
            cx = ml + (i / (n - 1)) * cw
            cy = mt + ch - ((pts[i] - y_min) / (y_max - y_min)) * ch
            d = ((mx - cx) ** 2 + (my - cy) ** 2) ** 0.5
            if d < best_dist:
                best_dist = d
                best = vs + i  # convert to global index

        if best != self._hover_index:
            self._hover_index = best
            self.update()

    def mousePressEvent(self, event) -> None:  # noqa: N802
        if event.button() == Qt.MouseButton.LeftButton:
            self._drag_last_x = event.position().x()
            self.setCursor(Qt.CursorShape.ClosedHandCursor)

    def mouseReleaseEvent(self, event) -> None:  # noqa: N802
        if event.button() == Qt.MouseButton.LeftButton:
            self._drag_last_x = None
            self.setCursor(Qt.CursorShape.ArrowCursor)

    def wheelEvent(self, event) -> None:  # noqa: N802
        """Scroll viewport with mouse wheel."""
        total = len(self._equity_points)
        if total <= self._VIEW_SIZE + 1:
            return
        delta = event.angleDelta().y()
        step = max(1, self._VIEW_SIZE // 10)  # scroll ~10 trades per notch
        if delta > 0:
            self._scroll_to(self._current_start() - step)
        else:
            self._scroll_to(self._current_start() + step)

    def leaveEvent(self, event) -> None:  # noqa: N802
        changed = False
        if self._hover_index != -1:
            self._hover_index = -1
            changed = True
        if self._drag_last_x is not None:
            self._drag_last_x = None
            self.setCursor(Qt.CursorShape.ArrowCursor)
            changed = True
        if changed:
            self.update()

    def _current_start(self) -> int:
        vs, _ = self._visible_range()
        return vs

    def _scroll_to(self, pos: int) -> None:
        total = len(self._equity_points)
        max_start = max(0, total - self._VIEW_SIZE - 1)
        pos = max(0, min(pos, max_start))
        self._view_start = pos
        self._hover_index = -1
        self.update()

    def _pan_by_pixels(self, dx_pixels: float) -> None:
        """Convert pixel drag distance to trade-index scroll."""
        w = self.width()
        ml, mr = 58, 18
        cw = w - ml - mr
        if cw <= 0:
            return
        vs, ve = self._visible_range()
        n = ve - vs
        if n < 2:
            return
        trades_per_px = (n - 1) / cw
        shift = int(round(dx_pixels * trades_per_px))
        if shift != 0:
            self._scroll_to(self._current_start() + shift)


class PerformanceDialog(QWidget):
    """Independent window showing aggregate alert performance statistics."""

    def __init__(
        self,
        parent: QWidget | None = None,
        outcomes_file: Path | None = None,
        active_count: int = 0,
        backtest_file: Path | None = None,
        paper_trades_file: Path | None = None,
        csi_log_file: Path | None = None,
        paper_trader=None,
        ss_trades_file: Path | None = None,
        ss_paper_trader=None,
        atr_trades_file: Path | None = None,
        atr_paper_trader=None,
        qm4_trades_file: Path | None = None,
        qm4_paper_trader=None,
        a_tuned_trades_file: Path | None = None,
        a_tuned_paper_trader=None,
        b_tuned_trades_file: Path | None = None,
        b_tuned_paper_trader=None,
        breakout_trades_file: Path | None = None,
        breakout_paper_trader=None,
        squeeze_trades_file: Path | None = None,
        squeeze_paper_trader=None,
        squeeze_rev_trades_file: Path | None = None,
        squeeze_rev_paper_trader=None,
        divergence_trades_file: Path | None = None,
        divergence_paper_trader=None,
        dtc_combo_trades_file: Path | None = None,
        dtc_combo_paper_trader=None,
        # ── Live-candle engine systems (2026-04-21) ──
        # When include_standard_tabs=False AND these are set, the dialog
        # ONLY shows the 5 "-live" tabs. Used by LiveCandleDialog.
        sv2_live_trades_file: Path | None = None,
        sv2_live_paper_trader=None,
        sv2_a_tuned_live_trades_file: Path | None = None,
        sv2_a_tuned_live_paper_trader=None,
        sv2_ss_live_trades_file: Path | None = None,
        sv2_ss_live_paper_trader=None,
        sv2_b_tuned_live_trades_file: Path | None = None,
        sv2_b_tuned_live_paper_trader=None,
        sv2_atr_live_trades_file: Path | None = None,
        sv2_atr_live_paper_trader=None,
        # Sv2-upgraded (2026-04-23) — live-candle engine + conv≥65 + revenge
        # cooldown + BE-stop. Shown in the standard Alert Performance dialog
        # next to Sv2 for direct comparison.
        sv2_upgraded_trades_file: Path | None = None,
        sv2_upgraded_paper_trader=None,
        # AU Gold suite (2026-04-24) — XAUUSD-only paper systems shown in
        # LiveCandleDialog (when include_standard_tabs=False).
        au1_trades_file: Path | None = None,
        au1_paper_trader=None,
        au2_trades_file: Path | None = None,
        au2_paper_trader=None,
        au3_trades_file: Path | None = None,
        au3_paper_trader=None,
        au4_trades_file: Path | None = None,
        au4_paper_trader=None,
        au5_trades_file: Path | None = None,
        au5_paper_trader=None,
        include_standard_tabs: bool = True,
        # Phase E (2026-05-06): shadow stats panel embedded in Sv2 tab.
        # All four are optional — panel is only built if shadow_journal
        # is provided. shadow_sim_worker can be None if worker failed
        # to start; panel runs in standalone mode.
        shadow_journal_path: Path | None = None,
        shadow_calibration_path: Path | None = None,
        shadow_sim_worker=None,
    ) -> None:
        # No parent → independent top-level window (not pinned with main)
        super().__init__(None)
        self._outcomes_file = outcomes_file
        self._backtest_file = backtest_file
        self._paper_trades_file = paper_trades_file
        self._csi_log_file = csi_log_file
        self._paper_trader = paper_trader
        # Phase E shadow-panel deps (stored for use in _build_ui Sv2 tab)
        self._shadow_journal_path = shadow_journal_path
        self._shadow_calibration_path = shadow_calibration_path
        self._shadow_sim_worker = shadow_sim_worker
        self._sv2_shadow_panel = None  # built in Sv2 tab if deps present
        self._ss_trades_file = ss_trades_file
        self._ss_paper_trader = ss_paper_trader
        self._ss_records: list[PaperTradeRecord] = []
        self._atr_trades_file = atr_trades_file
        self._atr_paper_trader = atr_paper_trader
        self._atr_records: list[PaperTradeRecord] = []
        self._qm4_trades_file = qm4_trades_file
        self._qm4_paper_trader = qm4_paper_trader
        self._qm4_records: list[PaperTradeRecord] = []
        self._a_tuned_trades_file = a_tuned_trades_file
        self._a_tuned_paper_trader = a_tuned_paper_trader
        self._a_tuned_records: list[PaperTradeRecord] = []
        self._b_tuned_trades_file = b_tuned_trades_file
        self._b_tuned_paper_trader = b_tuned_paper_trader
        self._b_tuned_records: list[PaperTradeRecord] = []
        self._breakout_trades_file = breakout_trades_file
        self._breakout_paper_trader = breakout_paper_trader
        self._breakout_records: list[PaperTradeRecord] = []
        self._squeeze_trades_file = squeeze_trades_file
        self._squeeze_paper_trader = squeeze_paper_trader
        self._squeeze_records: list[PaperTradeRecord] = []
        # Squeeze-REV (2026-04-29) — inverse-direction mirror of Squeeze
        self._squeeze_rev_trades_file = squeeze_rev_trades_file
        self._squeeze_rev_paper_trader = squeeze_rev_paper_trader
        self._squeeze_rev_records: list[PaperTradeRecord] = []
        self._divergence_trades_file = divergence_trades_file
        self._divergence_paper_trader = divergence_paper_trader
        self._divergence_records: list[PaperTradeRecord] = []
        self._dtc_combo_trades_file = dtc_combo_trades_file
        self._dtc_combo_paper_trader = dtc_combo_paper_trader
        self._dtc_combo_records: list[PaperTradeRecord] = []
        # ── Live-candle engine systems (2026-04-21) ──
        self._include_standard_tabs = include_standard_tabs
        self._sv2_live_trades_file = sv2_live_trades_file
        self._sv2_live_paper_trader = sv2_live_paper_trader
        self._sv2_live_records: list[PaperTradeRecord] = []
        self._sv2_a_tuned_live_trades_file = sv2_a_tuned_live_trades_file
        self._sv2_a_tuned_live_paper_trader = sv2_a_tuned_live_paper_trader
        self._sv2_a_tuned_live_records: list[PaperTradeRecord] = []
        self._sv2_ss_live_trades_file = sv2_ss_live_trades_file
        self._sv2_ss_live_paper_trader = sv2_ss_live_paper_trader
        self._sv2_ss_live_records: list[PaperTradeRecord] = []
        self._sv2_b_tuned_live_trades_file = sv2_b_tuned_live_trades_file
        self._sv2_b_tuned_live_paper_trader = sv2_b_tuned_live_paper_trader
        self._sv2_b_tuned_live_records: list[PaperTradeRecord] = []
        self._sv2_atr_live_trades_file = sv2_atr_live_trades_file
        self._sv2_atr_live_paper_trader = sv2_atr_live_paper_trader
        self._sv2_atr_live_records: list[PaperTradeRecord] = []
        # Sv2-upgraded (2026-04-23)
        self._sv2_upgraded_trades_file = sv2_upgraded_trades_file
        self._sv2_upgraded_paper_trader = sv2_upgraded_paper_trader
        self._sv2_upgraded_records: list[PaperTradeRecord] = []
        # AU Gold suite (2026-04-24) — XAUUSD only, paper-only.
        self._au1_trades_file = au1_trades_file
        self._au1_paper_trader = au1_paper_trader
        self._au1_records: list[PaperTradeRecord] = []
        self._au2_trades_file = au2_trades_file
        self._au2_paper_trader = au2_paper_trader
        self._au2_records: list[PaperTradeRecord] = []
        self._au3_trades_file = au3_trades_file
        self._au3_paper_trader = au3_paper_trader
        self._au3_records: list[PaperTradeRecord] = []
        self._au4_trades_file = au4_trades_file
        self._au4_paper_trader = au4_paper_trader
        self._au4_records: list[PaperTradeRecord] = []
        self._au5_trades_file = au5_trades_file
        self._au5_paper_trader = au5_paper_trader
        self._au5_records: list[PaperTradeRecord] = []
        self._active_count = active_count
        self._all_outcomes: list[AlertOutcome] = []
        self._bt_outcomes: list[AlertOutcome] = []
        self._paper_records: list[PaperTradeRecord] = []
        self._csi_records: list[dict] = []
        self.setWindowTitle("Alert Performance (MAE / MFE)")
        self.setWindowFlags(
            Qt.WindowType.Window
            | Qt.WindowType.WindowCloseButtonHint
            | Qt.WindowType.WindowMinMaxButtonsHint
        )
        self.setMinimumSize(780, 600)
        self._setup_ui()
        self._restore_layout()
        # Defer heavy data loading so the window shows immediately
        from PyQt6.QtCore import QTimer
        QTimer.singleShot(100, self._refresh)
        # Auto-refresh all tabs every 60 seconds
        self._auto_refresh_timer = QTimer(self)
        self._auto_refresh_timer.timeout.connect(self._refresh)
        self._auto_refresh_timer.start(60_000)

    _SETTINGS_KEY = "PerformanceDialog"

    def _save_layout(self) -> None:
        """Save window geometry and splitter positions to QSettings."""
        s = QSettings("TakumiTrader", self._SETTINGS_KEY)
        s.setValue("geometry", self.saveGeometry())
        if hasattr(self, "_paper_v_splitter"):
            s.setValue("paper_v_splitter", self._paper_v_splitter.saveState())
        if hasattr(self, "_paper_h_splitter"):
            s.setValue("paper_h_splitter", self._paper_h_splitter.saveState())
        s.setValue("active_tab", self._tabs.currentIndex())

    def _restore_layout(self) -> None:
        """Restore window geometry and splitter positions from QSettings."""
        s = QSettings("TakumiTrader", self._SETTINGS_KEY)
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
        s = QSettings("TakumiTrader", self._SETTINGS_KEY)
        v_state = s.value("paper_v_splitter")
        if v_state and isinstance(v_state, QByteArray) and hasattr(self, "_paper_v_splitter"):
            self._paper_v_splitter.restoreState(v_state)
        h_state = s.value("paper_h_splitter")
        if h_state and isinstance(h_state, QByteArray) and hasattr(self, "_paper_h_splitter"):
            self._paper_h_splitter.restoreState(h_state)
        # ── Tab-index migration ──
        # DTC-combo was inserted at index 0, pushing all prior tabs +1.
        # For a user with a PREVIOUSLY saved active_tab, bump it by +1 so
        # they land on the tab they were viewing before, not a different
        # one. For a first-time user (no saved tab), leave the default of
        # 0 so they land on DTC-combo. layout_version=2 gates this one-shot.
        # NOTE: only applies in standard-tabs mode (not LiveCandleDialog).
        layout_version = s.value("layout_version", 0, type=int)
        tab_idx = s.value("active_tab", 0, type=int)
        if self._include_standard_tabs and layout_version < 2:
            if s.contains("active_tab"):
                # Existing user with saved state → shift past the new tab
                tab_idx = tab_idx + 1
                s.setValue("active_tab", tab_idx)
            # Mark migration applied so we don't re-shift next launch
            s.setValue("layout_version", 2)
            s.sync()
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

    def _build_paper_page(self, pair_changed_slot, title_color="#2a5a8a"):
        """Build a full Paper Trades-style page: [summary | equity] + table."""
        page = QWidget()
        page_layout = QVBoxLayout(page)
        page_layout.setContentsMargins(0, 6, 0, 0)

        # Pair filter + Tier-1 feature toggle
        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("Pair:"))
        pair_combo = QComboBox()
        pair_combo.setMinimumWidth(110)
        pair_combo.currentTextChanged.connect(pair_changed_slot)
        filter_row.addWidget(pair_combo)

        from PyQt6.QtWidgets import QCheckBox
        feat_toggle = QCheckBox("Show All Features (174 cols)")
        feat_toggle.setToolTip(
            "Toggle visibility of all 174 feat_* columns: \n"
            "• Microstructure (CVD, Amihud, Kyle's λ)\n"
            "• Volatility (Parkinson, GK, YZ, ATR percentile, jump detection)\n"
            "• Regimes (ADX, +DI/-DI, Choppiness, Hurst, Aroon, Vortex, "
            "KAMA, SuperTrend, Ichimoku, regime classifier)\n"
            "• Statistics (autocorr, partial ACF, FFT, half-life)\n"
            "• Levels (round numbers, prior OHLC, 5 pivot systems, VWAP, POC/VAH/VAL)\n"
            "• Patterns (FVG, equal highs/lows, candlestick, trendline)\n"
            "• Adversarial (stop-hunt, sweep, magnetism, tick burst)\n"
            "• CSI metrics, currency baskets (DXY/EUR/JPY/GBP/AUD), carry\n"
            "• Network (VIX, gold, oil, equities, bonds, BTC, calendar, "
            "sentiment, COT) — auto-refreshed every 30 min\n"
            "Hidden by default to keep the table readable."
        )
        feat_toggle.setChecked(False)
        filter_row.addWidget(feat_toggle)
        filter_row.addStretch()
        page_layout.addLayout(filter_row)

        # Vertical splitter: top = [summary + equity], bottom = table
        v_splitter = QSplitter(Qt.Orientation.Vertical)

        # Top: horizontal splitter [summary | equity chart]
        h_splitter = QSplitter(Qt.Orientation.Horizontal)

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
        h_splitter.addWidget(scroll)

        equity_chart = EquityCurveWidget()
        equity_chart.setMinimumWidth(300)
        h_splitter.addWidget(equity_chart)
        h_splitter.setStretchFactor(0, 1)
        h_splitter.setStretchFactor(1, 1)

        v_splitter.addWidget(h_splitter)

        # Bottom: table
        table_container = QWidget()
        table_layout = QVBoxLayout(table_container)
        table_layout.setContentsMargins(0, 4, 0, 0)

        table_title = QLabel()
        table_title.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
        table_title.setStyleSheet(f"color: {title_color}; padding: 4px 0;")
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

        # ── Tier-1 features visibility toggle (added 2026-04-29) ──
        # Hides/shows logical columns [tier1_col_start .. tier1_col_end-1]
        # stamped on the QTableWidget as properties when the populate
        # method runs. Toggle state defaults to OFF so the table stays
        # narrow on first open.
        def _apply_tier1_visibility(checked: bool):
            start = trades_table.property("tier1_col_start")
            end = trades_table.property("tier1_col_end")
            if start is None or end is None:
                return  # populate hasn't run yet — initial state set there
            try:
                for col in range(int(start), int(end)):
                    trades_table.setColumnHidden(col, not checked)
            except Exception:
                pass

        feat_toggle.toggled.connect(_apply_tier1_visibility)
        trades_table.setProperty("_feat_toggle_checked", False)
        feat_toggle.toggled.connect(
            lambda checked: trades_table.setProperty("_feat_toggle_checked", checked)
        )

        v_splitter.addWidget(table_container)
        v_splitter.setStretchFactor(0, 1)
        v_splitter.setStretchFactor(1, 1)

        page_layout.addWidget(v_splitter)

        # Set default 50/50 splits after layout is ready
        from PyQt6.QtCore import QTimer
        def _set_defaults():
            total_h = v_splitter.height()
            total_w = h_splitter.width()
            if total_h > 100:
                v_splitter.setSizes([total_h // 2, total_h // 2])
            if total_w > 100:
                h_splitter.setSizes([total_w // 2, total_w // 2])
        QTimer.singleShot(200, _set_defaults)

        return page, content_label, table_title, trades_table, pair_combo, equity_chart

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

        # If this is a live-only instance (LiveCandleDialog), skip the standard
        # 13 tabs and build ONLY the 5 live tabs at the bottom of _setup_ui.
        if not self._include_standard_tabs:
            self._setup_ui_live_only(layout)
            return

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

        # Tier-1 feature toggle (parity with _build_paper_page — Sv2 was
        # built inline before the helper existed, so it missed the toggle).
        from PyQt6.QtWidgets import QCheckBox
        self._paper_feat_toggle = QCheckBox("Show All Features (174 cols)")
        self._paper_feat_toggle.setToolTip(
            "Toggle visibility of all 174 feat_* columns: \n"
            "• Microstructure (CVD, Amihud, Kyle's λ)\n"
            "• Volatility (Parkinson, GK, YZ, ATR percentile, jump detection)\n"
            "• Regimes (ADX, +DI/-DI, Choppiness, Hurst, Aroon, Vortex, "
            "KAMA, SuperTrend, Ichimoku, regime classifier)\n"
            "• Statistics (autocorr, partial ACF, FFT, half-life)\n"
            "• Levels (round numbers, prior OHLC, 5 pivot systems, VWAP, POC/VAH/VAL)\n"
            "• Patterns (FVG, equal highs/lows, candlestick, trendline)\n"
            "• Adversarial (stop-hunt, sweep, magnetism, tick burst)\n"
            "• CSI metrics, currency baskets (DXY/EUR/JPY/GBP/AUD), carry\n"
            "• Network (VIX, gold, oil, equities, bonds, BTC, calendar, "
            "sentiment, COT) — auto-refreshed every 30 min\n"
            "Hidden by default to keep the table readable."
        )
        self._paper_feat_toggle.setChecked(False)
        paper_filter_row.addWidget(self._paper_feat_toggle)
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

        # ── Wire Tier-1 feature toggle to the Sv2 table ──
        # Mirrors the logic in _build_paper_page: live show/hide plus a
        # persisted property that _populate_paper_table reads on every
        # render so the column state survives re-population.
        def _apply_sv2_tier1_visibility(checked: bool) -> None:
            start = self._paper_trades_table.property("tier1_col_start")
            end = self._paper_trades_table.property("tier1_col_end")
            if start is None or end is None:
                return  # populate hasn't run yet — initial state set there
            try:
                for col in range(int(start), int(end)):
                    self._paper_trades_table.setColumnHidden(col, not checked)
            except Exception:
                pass

        self._paper_feat_toggle.toggled.connect(_apply_sv2_tier1_visibility)
        self._paper_trades_table.setProperty("_feat_toggle_checked", False)
        self._paper_feat_toggle.toggled.connect(
            lambda checked: self._paper_trades_table.setProperty(
                "_feat_toggle_checked", checked)
        )

        self._paper_v_splitter.addWidget(table_container)

        # Phase E (2026-05-06): shadow stats panel as third splitter
        # pane below the trade table. Built only if the host (main_window)
        # provided shadow paths; defensive try/except so a panel-construction
        # failure can't take down the dialog.
        if self._shadow_journal_path is not None:
            try:
                from takumi_trader.ui.shadow_stats_panel import ShadowStatsPanel
                self._sv2_shadow_panel = ShadowStatsPanel(
                    shadow_journal_path=self._shadow_journal_path,
                    calibration_log_path=self._shadow_calibration_path,
                    sim_worker=self._shadow_sim_worker,
                    paper_journal_path=self._paper_trades_file,
                    parent=paper_page,
                )
                self._paper_v_splitter.addWidget(self._sv2_shadow_panel)
                self._paper_v_splitter.setStretchFactor(2, 3)
            except Exception as exc:
                # Logging via standard library (PerformanceDialog
                # already uses the project logger pattern elsewhere)
                import logging as _logging
                _logging.getLogger(__name__).warning(
                    "Failed to build ShadowStatsPanel for Sv2 tab: %s", exc,
                    exc_info=True,
                )
                self._sv2_shadow_panel = None

        self._paper_v_splitter.setStretchFactor(0, 4)
        self._paper_v_splitter.setStretchFactor(1, 6)

        # Auto-save splitter positions on drag
        self._paper_h_splitter.splitterMoved.connect(self._save_layout)
        self._paper_v_splitter.splitterMoved.connect(self._save_layout)

        paper_layout.addWidget(self._paper_v_splitter)
        self._tabs.addTab(paper_page, "\u26a1 Sv2")

        # ── Sv2-upgraded (2026-04-23) ──
        # Live-candle engine variant of Sv2 with three extra gates (conv≥65,
        # revenge cooldown 60m, BE-stop at +7p peak). Placed right next to
        # Sv2 for side-by-side comparison in the Performance dialog.
        (sv2up_page, self._sv2_upgraded_content_label,
         self._sv2_upgraded_table_title, self._sv2_upgraded_trades_table,
         self._sv2_upgraded_pair_combo, self._sv2_upgraded_equity_chart
         ) = self._build_paper_page(
            pair_changed_slot=self._render_sv2_upgraded, title_color="#e91e63")
        self._tabs.addTab(sv2up_page, "\u26a1 Sv2-upgraded")

        # ── System A-tuned: Sv2 tuned variant ──
        (a_tuned_page, self._a_tuned_content_label, self._a_tuned_table_title,
         self._a_tuned_trades_table, self._a_tuned_pair_combo,
         self._a_tuned_equity_chart) = self._build_paper_page(
            pair_changed_slot=self._render_a_tuned, title_color="#00897b")
        self._tabs.addTab(a_tuned_page, "\U0001f505 Sv2-tuned")

        # ── System B: Sv2+SS tab (same layout as Paper Trades) ──
        (ss_page, self._ss_content_label, self._ss_table_title,
         self._ss_trades_table, self._ss_pair_combo,
         self._ss_equity_chart) = self._build_paper_page(
            pair_changed_slot=self._render_ss, title_color="#9c27b0")
        self._tabs.addTab(ss_page, "\U0001f9ea Sv2+SS")

        # ── System B-tuned: Sv2+SS tuned variant ──
        (b_tuned_page, self._b_tuned_content_label, self._b_tuned_table_title,
         self._b_tuned_trades_table, self._b_tuned_pair_combo,
         self._b_tuned_equity_chart) = self._build_paper_page(
            pair_changed_slot=self._render_b_tuned, title_color="#7b1fa2")
        self._tabs.addTab(b_tuned_page, "\U0001f505 Sv2+SS-tuned")

        # ── System C: Sv2+ATR tab (same layout as Paper Trades) ──
        (atr_page, self._atr_content_label, self._atr_table_title,
         self._atr_trades_table, self._atr_pair_combo,
         self._atr_equity_chart) = self._build_paper_page(
            pair_changed_slot=self._render_atr, title_color="#e65100")
        self._tabs.addTab(atr_page, "\U0001f4c8 Sv2+ATR")

        # ── System D: QM4 CSI tab ──
        (qm4_page, self._qm4_content_label, self._qm4_table_title,
         self._qm4_trades_table, self._qm4_pair_combo,
         self._qm4_equity_chart) = self._build_paper_page(
            pair_changed_slot=self._render_qm4, title_color="#ff6f00")
        self._tabs.addTab(qm4_page, "\U0001f3af QM4")

        # ── System E: Breakout (Session Range) ──
        (brk_page, self._breakout_content_label, self._breakout_table_title,
         self._breakout_trades_table, self._breakout_pair_combo,
         self._breakout_equity_chart) = self._build_paper_page(
            pair_changed_slot=self._render_breakout, title_color="#0288d1")
        self._tabs.addTab(brk_page, "\U0001f4ca Breakout")

        # ── System F: Squeeze (BB + KC) ──
        (sqz_page, self._squeeze_content_label, self._squeeze_table_title,
         self._squeeze_trades_table, self._squeeze_pair_combo,
         self._squeeze_equity_chart) = self._build_paper_page(
            pair_changed_slot=self._render_squeeze, title_color="#7b1fa2")
        self._tabs.addTab(sqz_page, "\U0001f504 Squeeze")

        # ── System F-REV: Squeeze-REV (inverse-direction mirror) ──
        # Pink to differentiate from purple SQZ. Hypothesis: PF<1 system
        # flipped becomes PF>1 (less 2x spread cost).
        (sqzr_page, self._squeeze_rev_content_label, self._squeeze_rev_table_title,
         self._squeeze_rev_trades_table, self._squeeze_rev_pair_combo,
         self._squeeze_rev_equity_chart) = self._build_paper_page(
            pair_changed_slot=self._render_squeeze_rev, title_color="#ad1457")
        self._tabs.addTab(sqzr_page, "\U0001f504 Squeeze-REV")

        # ── System G: Divergence (Correlation) ──
        (div_page, self._divergence_content_label, self._divergence_table_title,
         self._divergence_trades_table, self._divergence_pair_combo,
         self._divergence_equity_chart) = self._build_paper_page(
            pair_changed_slot=self._render_divergence, title_color="#00796b")
        self._tabs.addTab(div_page, "\U0001f4c8 Divergence")

        # ── DTC-combo: filtered aggregate of SS+ATR+B-tuned (FIRST tab) ──
        # Inserted at index 0 so it appears before Live Trades. Per-system
        # filters + 120s same-pair dedup applied at signal-time in
        # main_window._maybe_open_dtc_combo. This is the "deployment" tab —
        # what the live cTrader bridge actually trades.
        (dtc_page, self._dtc_combo_content_label, self._dtc_combo_table_title,
         self._dtc_combo_trades_table, self._dtc_combo_pair_combo,
         self._dtc_combo_equity_chart) = self._build_paper_page(
            pair_changed_slot=self._render_dtc_combo, title_color="#0277bd")

        # Inject a Source filter combo into the existing pair-filter row.
        # Lets the user drill into "show only ATR-sourced DTC trades" etc.
        # without losing the per-source breakdown above (which always shows
        # all sources for cross-feeder comparison).
        try:
            _dtc_filter_row = dtc_page.layout().itemAt(0).layout()
            _dtc_filter_row.addSpacing(12)
            _dtc_filter_row.addWidget(QLabel("Source:"))
            self._dtc_combo_source_combo = QComboBox()
            self._dtc_combo_source_combo.setMinimumWidth(110)
            # Populated dynamically in _render_dtc_combo as records load
            self._dtc_combo_source_combo.addItems(["ALL", "SS", "ATR", "B-tuned"])
            self._dtc_combo_source_combo.currentTextChanged.connect(self._render_dtc_combo)
            _dtc_filter_row.addWidget(self._dtc_combo_source_combo)
        except Exception:
            # Fallback: source filter unavailable, pair filter still works
            self._dtc_combo_source_combo = None

        self._tabs.insertTab(0, dtc_page, "\U0001f3af DTC-combo")
        # Make DTC-combo the default selected tab on dialog open
        self._tabs.setCurrentIndex(0)

        # ── Backtest tab ──
        bt_page, self._bt_content_label, self._bt_table_title, self._bt_trades_table, self._bt_pair_combo = self._build_page()
        self._bt_pair_combo.currentTextChanged.connect(self._on_bt_pair_changed)
        self._tabs.addTab(bt_page, "\U0001f504 Backtest")

        # ── CSI Alerts tab ──
        csi_page = QWidget()
        csi_layout = QVBoxLayout(csi_page)
        csi_layout.setContentsMargins(0, 6, 0, 0)

        # Filter row
        csi_filter_row = QHBoxLayout()
        csi_filter_row.addWidget(QLabel("Type:"))
        self._csi_type_combo = QComboBox()
        self._csi_type_combo.setMinimumWidth(90)
        self._csi_type_combo.addItems(["ALL", "MTF", "MTFC", "HTF", "XHTF", "HTFC", "CUM", "PAIR"])
        self._csi_type_combo.currentTextChanged.connect(self._render_csi)
        csi_filter_row.addWidget(self._csi_type_combo)
        csi_filter_row.addSpacing(12)
        csi_filter_row.addWidget(QLabel("Ccy/Pair:"))
        self._csi_ccy_combo = QComboBox()
        self._csi_ccy_combo.setMinimumWidth(100)
        self._csi_ccy_combo.currentTextChanged.connect(self._render_csi)
        csi_filter_row.addWidget(self._csi_ccy_combo)
        csi_filter_row.addStretch()
        csi_layout.addLayout(csi_filter_row)

        # Splitter: HTML summary (top) + sortable table (bottom)
        csi_splitter = QSplitter(Qt.Orientation.Vertical)

        csi_scroll = QScrollArea()
        csi_scroll.setWidgetResizable(True)
        csi_scroll.setStyleSheet("QScrollArea { border: none; background: #fafafa; }")
        self._csi_summary_label = QLabel()
        self._csi_summary_label.setWordWrap(True)
        self._csi_summary_label.setTextFormat(Qt.TextFormat.RichText)
        self._csi_summary_label.setAlignment(Qt.AlignmentFlag.AlignTop)
        self._csi_summary_label.setFont(QFont("Segoe UI", 10))
        self._csi_summary_label.setStyleSheet("padding: 10px; background: #fafafa;")
        csi_scroll.setWidget(self._csi_summary_label)
        csi_splitter.addWidget(csi_scroll)

        csi_table_container = QWidget()
        csi_table_layout = QVBoxLayout(csi_table_container)
        csi_table_layout.setContentsMargins(0, 4, 0, 0)
        self._csi_table_title = QLabel()
        self._csi_table_title.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
        self._csi_table_title.setStyleSheet("color: #2a5a8a; padding: 4px 0;")
        csi_table_layout.addWidget(self._csi_table_title)
        self._csi_table = QTableWidget()
        self._csi_table.setAlternatingRowColors(True)
        self._csi_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._csi_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._csi_table.setSortingEnabled(True)
        self._csi_table.verticalHeader().setVisible(False)
        self._csi_table.verticalHeader().setDefaultSectionSize(22)
        self._csi_table.setStyleSheet(self._TABLE_STYLE)
        csi_table_layout.addWidget(self._csi_table)
        csi_splitter.addWidget(csi_table_container)
        csi_splitter.setStretchFactor(0, 3)
        csi_splitter.setStretchFactor(1, 7)
        csi_layout.addWidget(csi_splitter)

        self._tabs.addTab(csi_page, "\U0001f514 CSI Alerts")

        layout.addWidget(self._tabs)
        self._apply_dialog_stylesheet()

    def _apply_dialog_stylesheet(self) -> None:
        """Shared stylesheet used by both standard and live-only layouts."""
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

    def _setup_ui_live_only(self, layout) -> None:
        """Build ONLY the 5 live-candle system tabs (for LiveCandleDialog).

        Uses the SAME `_build_paper_page` helper as the standard dialog, so
        the 5 tabs have identical layout: pair filter, HTML summary, equity
        curve, full sortable trades table with all MAE/MFE/context columns.
        Called from `_setup_ui` when `include_standard_tabs=False`.
        """
        # Sv2-live (mirrors A)
        (sv2l_page, self._sv2_live_content_label, self._sv2_live_table_title,
         self._sv2_live_trades_table, self._sv2_live_pair_combo,
         self._sv2_live_equity_chart) = self._build_paper_page(
            pair_changed_slot=self._render_sv2_live, title_color="#1565c0")
        self._tabs.addTab(sv2l_page, "\u26A1 Sv2-live")

        # Sv2-Tun-live (mirrors D/A-tuned)
        (sv2atl_page, self._sv2_a_tuned_live_content_label,
         self._sv2_a_tuned_live_table_title, self._sv2_a_tuned_live_trades_table,
         self._sv2_a_tuned_live_pair_combo, self._sv2_a_tuned_live_equity_chart
         ) = self._build_paper_page(
            pair_changed_slot=self._render_sv2_a_tuned_live, title_color="#6a1b9a")
        self._tabs.addTab(sv2atl_page, "\u26A1 Sv2-Tun-live")

        # Sv2+SS-live (mirrors B)
        (sv2ssl_page, self._sv2_ss_live_content_label, self._sv2_ss_live_table_title,
         self._sv2_ss_live_trades_table, self._sv2_ss_live_pair_combo,
         self._sv2_ss_live_equity_chart) = self._build_paper_page(
            pair_changed_slot=self._render_sv2_ss_live, title_color="#00695c")
        self._tabs.addTab(sv2ssl_page, "\u26A1 Sv2+SS-live")

        # Sv2+SS-Tun-live (mirrors E/B-tuned)
        (sv2btl_page, self._sv2_b_tuned_live_content_label,
         self._sv2_b_tuned_live_table_title, self._sv2_b_tuned_live_trades_table,
         self._sv2_b_tuned_live_pair_combo, self._sv2_b_tuned_live_equity_chart
         ) = self._build_paper_page(
            pair_changed_slot=self._render_sv2_b_tuned_live, title_color="#ad1457")
        self._tabs.addTab(sv2btl_page, "\u26A1 Sv2+SS-Tun-live")

        # Sv2+ATR-live (mirrors C)
        (sv2atrl_page, self._sv2_atr_live_content_label,
         self._sv2_atr_live_table_title, self._sv2_atr_live_trades_table,
         self._sv2_atr_live_pair_combo, self._sv2_atr_live_equity_chart
         ) = self._build_paper_page(
            pair_changed_slot=self._render_sv2_atr_live, title_color="#e65100")
        self._tabs.addTab(sv2atrl_page, "\u26A1 Sv2+ATR-live")

        # ── AU Gold suite (2026-04-24) — XAUUSD only ──
        # Gold-coloured tabs to visually separate from FX systems.
        (au1_page, self._au1_content_label,
         self._au1_table_title, self._au1_trades_table,
         self._au1_pair_combo, self._au1_equity_chart
         ) = self._build_paper_page(
            pair_changed_slot=self._render_au1, title_color="#ffb300")
        self._tabs.addTab(au1_page, "\U0001F947 AU1 London")

        (au2_page, self._au2_content_label,
         self._au2_table_title, self._au2_trades_table,
         self._au2_pair_combo, self._au2_equity_chart
         ) = self._build_paper_page(
            pair_changed_slot=self._render_au2, title_color="#ff8f00")
        self._tabs.addTab(au2_page, "\U0001F947 AU2 NY-ORB")

        (au3_page, self._au3_content_label,
         self._au3_table_title, self._au3_trades_table,
         self._au3_pair_combo, self._au3_equity_chart
         ) = self._build_paper_page(
            pair_changed_slot=self._render_au3, title_color="#ff6f00")
        self._tabs.addTab(au3_page, "\U0001F947 AU3 Pullback")

        (au4_page, self._au4_content_label,
         self._au4_table_title, self._au4_trades_table,
         self._au4_pair_combo, self._au4_equity_chart
         ) = self._build_paper_page(
            pair_changed_slot=self._render_au4, title_color="#e65100")
        self._tabs.addTab(au4_page, "\U0001F947 AU4 USD-Div")

        (au5_page, self._au5_content_label,
         self._au5_table_title, self._au5_trades_table,
         self._au5_pair_combo, self._au5_equity_chart
         ) = self._build_paper_page(
            pair_changed_slot=self._render_au5, title_color="#bf360c")
        self._tabs.addTab(au5_page, "\U0001F947 AU5 MeanRev")

        layout.addWidget(self._tabs)
        # Default to first tab (Sv2-live) on open
        self._tabs.setCurrentIndex(0)
        self._apply_dialog_stylesheet()

    def _refresh(self) -> None:
        # ── Live-only mode (LiveCandleDialog): refresh ONLY the 5 live tabs ──
        if not self._include_standard_tabs:
            self._refresh_live_only()
            return

        # Force save paper journal from memory to disk before loading
        for _pt in [self._paper_trader, self._a_tuned_paper_trader,
                     self._ss_paper_trader, self._b_tuned_paper_trader,
                     self._atr_paper_trader, self._qm4_paper_trader,
                     self._breakout_paper_trader, self._squeeze_paper_trader,
                     self._squeeze_rev_paper_trader,  # 2026-04-29
                     self._divergence_paper_trader, self._dtc_combo_paper_trader,
                     self._sv2_upgraded_paper_trader]:
            if _pt is not None:
                try:
                    _pt.save_journal()
                except Exception:
                    pass

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

        # Load CSI alert log
        self._csi_records = self._load_csi_records()
        self._update_csi_combos()
        self._render_csi()

        # Load System A-tuned trades
        self._a_tuned_records = self._load_tuned_records(self._a_tuned_trades_file, self._a_tuned_paper_trader)
        self._update_generic_pair_combo(self._a_tuned_pair_combo, self._a_tuned_records)
        self._render_a_tuned()

        # Load System B (Sv2+SS) trades
        self._ss_records = self._load_ss_records()
        self._update_ss_pair_combo()
        self._render_ss()

        # Load System B-tuned trades
        self._b_tuned_records = self._load_tuned_records(self._b_tuned_trades_file, self._b_tuned_paper_trader)
        self._update_generic_pair_combo(self._b_tuned_pair_combo, self._b_tuned_records)
        self._render_b_tuned()

        # Load System C (Sv2+ATR) trades
        self._atr_records = self._load_atr_records()
        self._update_atr_pair_combo()
        self._render_atr()

        # Load Sv2-upgraded trades (2026-04-23)
        self._sv2_upgraded_records = self._load_generic_records(self._sv2_upgraded_trades_file)
        self._update_generic_pair_combo(self._sv2_upgraded_pair_combo, self._sv2_upgraded_records)
        self._render_sv2_upgraded()

        # Load System D (QM4) trades
        self._qm4_records = self._load_qm4_records()
        self._update_qm4_pair_combo()
        self._render_qm4()

        # Load Systems E/F/G + Squeeze-REV trades
        for _attr, _file_attr, _combo_attr, _render_fn in [
            ("_breakout_records", "_breakout_trades_file", "_breakout_pair_combo", "_render_breakout"),
            ("_squeeze_records", "_squeeze_trades_file", "_squeeze_pair_combo", "_render_squeeze"),
            # Squeeze-REV (2026-04-29)
            ("_squeeze_rev_records", "_squeeze_rev_trades_file", "_squeeze_rev_pair_combo", "_render_squeeze_rev"),
            ("_divergence_records", "_divergence_trades_file", "_divergence_pair_combo", "_render_divergence"),
            ("_dtc_combo_records", "_dtc_combo_trades_file", "_dtc_combo_pair_combo", "_render_dtc_combo"),
        ]:
            _tf = getattr(self, _file_attr, None)
            recs = self._load_generic_records(_tf)
            setattr(self, _attr, recs)
            self._update_generic_pair_combo(getattr(self, _combo_attr), recs)
            getattr(self, _render_fn)()

    def _refresh_live_only(self) -> None:
        """Refresh only the 5 live-candle system tabs.

        Used when `include_standard_tabs=False` (LiveCandleDialog). Mirrors
        the load/combo/render cycle from `_refresh`, restricted to the live
        traders and files.
        """
        # Flush in-memory journals to disk first so stats are current
        for _pt in [
            self._sv2_live_paper_trader, self._sv2_a_tuned_live_paper_trader,
            self._sv2_ss_live_paper_trader, self._sv2_b_tuned_live_paper_trader,
            self._sv2_atr_live_paper_trader,
            # AU Gold suite (2026-04-24)
            self._au1_paper_trader, self._au2_paper_trader,
            self._au3_paper_trader, self._au4_paper_trader,
            self._au5_paper_trader,
        ]:
            if _pt is not None:
                try:
                    _pt.save_journal()
                except Exception:
                    pass

        # Load + refresh each live system
        for _attr, _file_attr, _combo_attr, _render_fn in [
            ("_sv2_live_records",         "_sv2_live_trades_file",
             "_sv2_live_pair_combo",      "_render_sv2_live"),
            ("_sv2_a_tuned_live_records", "_sv2_a_tuned_live_trades_file",
             "_sv2_a_tuned_live_pair_combo", "_render_sv2_a_tuned_live"),
            ("_sv2_ss_live_records",      "_sv2_ss_live_trades_file",
             "_sv2_ss_live_pair_combo",   "_render_sv2_ss_live"),
            ("_sv2_b_tuned_live_records", "_sv2_b_tuned_live_trades_file",
             "_sv2_b_tuned_live_pair_combo", "_render_sv2_b_tuned_live"),
            ("_sv2_atr_live_records",     "_sv2_atr_live_trades_file",
             "_sv2_atr_live_pair_combo",  "_render_sv2_atr_live"),
            # AU Gold suite (2026-04-24)
            ("_au1_records", "_au1_trades_file",
             "_au1_pair_combo", "_render_au1"),
            ("_au2_records", "_au2_trades_file",
             "_au2_pair_combo", "_render_au2"),
            ("_au3_records", "_au3_trades_file",
             "_au3_pair_combo", "_render_au3"),
            ("_au4_records", "_au4_trades_file",
             "_au4_pair_combo", "_render_au4"),
            ("_au5_records", "_au5_trades_file",
             "_au5_pair_combo", "_render_au5"),
        ]:
            _tf = getattr(self, _file_attr, None)
            recs = self._load_generic_records(_tf)
            setattr(self, _attr, recs)
            self._update_generic_pair_combo(getattr(self, _combo_attr), recs)
            getattr(self, _render_fn)()

    def _load_generic_records(self, trades_file) -> list[PaperTradeRecord]:
        """Load records from a generic paper trades JSON file."""
        if not trades_file or not trades_file.exists():
            return []
        try:
            import json as _json
            data = _json.loads(trades_file.read_text(encoding="utf-8"))
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

        self._paper_content_label.setText(
            self._build_paper_html(records, selected, system_label="Sv2"))
        # Use the shared generic populate so the 174 feat_* columns + the
        # toggle property wiring match every other paper-system tab.
        # (Sv2's old _populate_paper_table predated the tier-1/2/3 column
        # block — keeping it caused the "Show All Features" toggle to do
        # nothing on this tab.)
        self._populate_paper_table_generic(
            records, self._paper_trades_table, self._paper_table_title, "Sv2")

        # Update equity curve (chronological order)
        sorted_records = sorted(records, key=lambda r: r.entry_time)
        self._equity_chart.set_data(sorted_records, start_capital=1000.0, risk_pct=0.03)

    # Systems where the ADR<=70 quality filter is APPLIED at signal time
    # (see main_window.py _ADR_QUALITY_SYSTEMS). Used by the ADR breakdown
    # section to show "filter ACTIVE" instead of the what-if verdict on
    # those tabs.
    _ADR_FILTER_ACTIVE_SYSTEMS = {"Sv2", "Sv2-tuned"}

    def _build_paper_html(
        self, records: list[PaperTradeRecord], pair_filter: str,
        system_label: str | None = None,
    ) -> str:
        """Build HTML summary for paper trades.

        system_label: e.g. "Sv2", "SS", "DTC-combo". Used to render an
        accurate ADR verdict per tab (active filter vs what-if analysis).
        """
        p = [_CSS]
        title = f"Paper Trades: {pair_filter}" if pair_filter != "ALL" else "Paper Trades — Overall"
        p.append(f'<div class="header">{title}</div>')
        _open_n = sum(1 for r in records if r.close_time == 0 and not r.close_reason)
        _sub = f"{len(records)} paper trades"
        if _open_n:
            _sub += f" ({_open_n} open)"
        p.append(f'<div class="subtitle">{_sub}</div>')

        if not records:
            p.append('<div class="empty">No paper trades yet. '
                     'Paper trades open automatically on FULL alerts with optimized SL/TP.</div>')
            return "".join(p)

        # Summary stats (exclude open trades — they have no exit data yet)
        _closed = [r for r in records if r.close_time > 0 or r.close_reason]
        _open_count = len(records) - len(_closed)
        wins = [r for r in _closed if r.is_win]
        losses = [r for r in _closed if not r.is_win]
        total = len(_closed)
        total_pnl = sum(r.pnl_pips for r in _closed)
        wr = len(wins) / total * 100 if total else 0
        avg_win = sum(r.pnl_pips for r in wins) / len(wins) if wins else 0
        avg_loss = sum(r.pnl_pips for r in losses) / len(losses) if losses else 0
        avg_dur = sum(r.duration_minutes for r in _closed) / total if total else 0

        sl_hits = sum(1 for r in _closed if r.close_reason == "sl_hit")
        tp_hits = sum(1 for r in _closed if r.close_reason == "tp_hit")
        sig_exits = sum(1 for r in _closed if r.close_reason == "signal_exit")

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

        # ── Daily Averages & Projected Returns ──
        from datetime import datetime, timezone, timedelta as _td
        _JST_TZ = timezone(_td(hours=9))
        if total >= 2:
            # Compute trading days (unique dates in JST) — closed trades only
            _dates = set()
            for r in _closed:
                if r.entry_time > 0:
                    _dt = datetime.fromtimestamp(r.entry_time, tz=_JST_TZ)
                    _dates.add(_dt.date())
            trading_days = max(1, len(_dates))

            pips_per_day = total_pnl / trading_days
            trades_per_day = total / trading_days
            pips_per_day_c = "#1b8a2a" if pips_per_day >= 0 else "#c62828"

            # Projected returns using compound 3% risk per trade
            # R-multiple per trade: avg_pnl / avg_sl  (closed only)
            avg_sl = sum(r.sl_pips for r in _closed) / total if total else 10
            avg_pnl_all = total_pnl / total if total else 0
            avg_r = avg_pnl_all / avg_sl if avg_sl > 0 else 0
            # Daily return: trades_per_day × avg_R × risk_pct
            _risk = 0.03
            daily_return_pct = trades_per_day * avg_r * _risk * 100
            weekly_return_pct = daily_return_pct * 5
            monthly_return_pct = daily_return_pct * 22
            daily_c = "#1b8a2a" if daily_return_pct >= 0 else "#c62828"
            weekly_c = "#1b8a2a" if weekly_return_pct >= 0 else "#c62828"
            monthly_c = "#1b8a2a" if monthly_return_pct >= 0 else "#c62828"

            p.append('<div class="section-title">Daily Averages & Projected Returns</div>')
            p.append('<table class="stats-grid"><tr>')
            p.append(f'<td class="stat-card"><div class="stat-value" style="color:{pips_per_day_c};">'
                     f'{pips_per_day:+.1f}p</div>'
                     f'<div class="stat-label">Pips / Day</div></td>')
            p.append(f'<td class="stat-card"><div class="stat-value">'
                     f'{trades_per_day:.1f}</div>'
                     f'<div class="stat-label">Trades / Day</div></td>')
            p.append(f'<td class="stat-card"><div class="stat-value" style="color:{daily_c};">'
                     f'{daily_return_pct:+.2f}%</div>'
                     f'<div class="stat-label">Daily Return</div></td>')
            p.append(f'<td class="stat-card"><div class="stat-value" style="color:{weekly_c};">'
                     f'{weekly_return_pct:+.1f}%</div>'
                     f'<div class="stat-label">Weekly Return</div></td>')
            p.append(f'<td class="stat-card"><div class="stat-value" style="color:{monthly_c};">'
                     f'{monthly_return_pct:+.1f}%</div>'
                     f'<div class="stat-label">Monthly Return</div></td>')
            p.append('</tr></table>')
            p.append(f'<div style="font-size:9px; color:#999; margin-top:2px;">'
                     f'Based on {trading_days} trading days, {_risk*100:.0f}% risk/trade, '
                     f'avg SL={avg_sl:.1f}p, avg R={avg_r:+.2f}</div>')

        # ── Maximum Drawdown ──
        if total >= 2:
            # Simulate equity curve (same method as EquityCurveWidget)
            _dd_balance = 1000.0
            _dd_peak = _dd_balance
            _dd_max_dd = 0.0
            _dd_max_dd_pct = 0.0
            sorted_recs = sorted(records, key=lambda r: r.entry_time)
            for r in sorted_recs:
                _dd_risk_amt = _dd_balance * _risk
                _dd_r_mult = r.pnl_pips / r.sl_pips if r.sl_pips > 0 else r.pnl_pips / 10.0
                _dd_balance += _dd_risk_amt * _dd_r_mult
                if _dd_balance > _dd_peak:
                    _dd_peak = _dd_balance
                _dd_drawdown = _dd_peak - _dd_balance
                _dd_drawdown_pct = (_dd_drawdown / _dd_peak * 100) if _dd_peak > 0 else 0
                if _dd_drawdown_pct > _dd_max_dd_pct:
                    _dd_max_dd_pct = _dd_drawdown_pct
                    _dd_max_dd = _dd_drawdown

            _dd_color = "#1b8a2a" if _dd_max_dd_pct < 10 else "#e65100" if _dd_max_dd_pct < 20 else "#c62828"
            p.append('<div class="section-title">Risk Metrics</div>')
            p.append('<table class="stats-grid"><tr>')
            p.append(f'<td class="stat-card"><div class="stat-value" style="color:{_dd_color};">'
                     f'{_dd_max_dd_pct:.1f}%</div>'
                     f'<div class="stat-label">Max Drawdown</div></td>')
            p.append(f'<td class="stat-card"><div class="stat-value" style="color:{_dd_color};">'
                     f'${_dd_max_dd:,.0f}</div>'
                     f'<div class="stat-label">Max DD ($1k start)</div></td>')
            # Profit factor (closed trades only)
            _gross_wins = sum(r.pnl_pips for r in _closed if r.is_win)
            _gross_losses = abs(sum(r.pnl_pips for r in _closed if not r.is_win))
            _pf = _gross_wins / _gross_losses if _gross_losses > 0 else 999
            _pf_color = "#1b8a2a" if _pf >= 2.0 else "#e65100" if _pf >= 1.5 else "#c62828"
            p.append(f'<td class="stat-card"><div class="stat-value" style="color:{_pf_color};">'
                     f'{_pf:.2f}</div>'
                     f'<div class="stat-label">Profit Factor</div></td>')
            # Recovery factor (total profit / max drawdown)
            _final_pnl_pct = (_dd_balance - 1000.0) / 1000.0 * 100
            _rf = _final_pnl_pct / _dd_max_dd_pct if _dd_max_dd_pct > 0 else 999
            _rf_color = "#1b8a2a" if _rf >= 3.0 else "#e65100" if _rf >= 1.5 else "#c62828"
            p.append(f'<td class="stat-card"><div class="stat-value" style="color:{_rf_color};">'
                     f'{_rf:.1f}</div>'
                     f'<div class="stat-label">Recovery Factor</div></td>')
            p.append('</tr></table>')

        # ── ADR Quality Breakdown ──
        # Shows win rate at different ADR-consumption levels at entry.
        # Helps spot whether this system would benefit from an ADR<=X filter.
        # Validated insights from raw-history analysis:
        #   • Sv2 + ADR<=70: $863 → $1,101 (+28%)  [filter ALREADY applied to live]
        #   • A-tuned + ADR<=70: $1,189 → $1,480 (+24%) [filter ALREADY applied]
        #   • SS/ATR/B-tuned: pair blacklist already covers this; ADR is redundant
        _with_adr = [r for r in _closed
                     if getattr(r, "adr_consumed_pct", None) is not None
                     and r.adr_consumed_pct > 0]
        if _with_adr and len(_with_adr) >= 5:
            buckets = [
                ("0-30%",   lambda x: x <= 30,                "#2e7d32"),
                ("30-50%",  lambda x: 30 < x <= 50,           "#558b2f"),
                ("50-70%",  lambda x: 50 < x <= 70,           "#f57c00"),
                ("70-90%",  lambda x: 70 < x <= 90,           "#e65100"),
                (">90%",    lambda x: x > 90,                 "#c62828"),
            ]
            p.append('<div class="section-title">ADR Quality Breakdown '
                     '<span style="font-weight:normal; color:#888; font-size:9pt;">'
                     '(entry context: how much of avg daily range was already '
                     'consumed when trade fired)</span></div>')
            p.append('<table class="stats-grid"><tr>')
            for label, predicate, color in buckets:
                bucket_trades = [r for r in _with_adr if predicate(r.adr_consumed_pct)]
                bn = len(bucket_trades)
                if bn == 0:
                    p.append(f'<td class="stat-card">'
                             f'<div class="stat-value" style="color:#bbb;">—</div>'
                             f'<div class="stat-label">{label}<br>0 trades</div></td>')
                    continue
                bw = sum(1 for r in bucket_trades if r.is_win)
                bp = sum(r.pnl_pips for r in bucket_trades)
                bwr = bw / bn * 100
                avg_p = bp / bn
                wr_color = ("#1b8a2a" if bwr >= 75 else "#e65100"
                            if bwr >= 60 else "#c62828")
                p.append(f'<td class="stat-card">'
                         f'<div class="stat-value" style="color:{wr_color};">'
                         f'{bwr:.0f}%</div>'
                         f'<div class="stat-label">{label} ADR<br>'
                         f'{bn} tr / {avg_p:+.1f}p avg</div></td>')
            p.append('</tr></table>')

            # Filter-impact summary: what would ADR<=70 do?
            _adr_70_kept = [r for r in _with_adr if r.adr_consumed_pct <= 70]
            _adr_70_blocked = [r for r in _with_adr if r.adr_consumed_pct > 70]
            if _adr_70_blocked:
                bk_n = len(_adr_70_blocked)
                bk_w = sum(1 for r in _adr_70_blocked if r.is_win)
                bk_pnl = sum(r.pnl_pips for r in _adr_70_blocked)
                bk_avg = bk_pnl / bk_n
                bk_wr = bk_w / bk_n * 100
                kp_n = len(_adr_70_kept)
                kp_w = sum(1 for r in _adr_70_kept if r.is_win)
                kp_pnl = sum(r.pnl_pips for r in _adr_70_kept)
                kp_wr = kp_w / kp_n * 100 if kp_n else 0

                # Verdict — system-aware
                _filter_active = (system_label in self._ADR_FILTER_ACTIVE_SYSTEMS)

                if _filter_active:
                    # On tabs where the filter is already running live, the
                    # >70% bucket should be empty going forward. Any trades
                    # there are pre-filter historical noise.
                    verdict = ("✅ ADR≤70 filter is ACTIVE — entries above 70% "
                               "are blocked at signal time. Old high-ADR trades "
                               "above are pre-filter history.")
                    verdict_color = "#1b8a2a"
                    impact_label = "Historical: pre-filter trades that would now be blocked"
                else:
                    # What-if analysis — would adding this filter help?
                    if bk_avg < -0.5:
                        verdict = ("✅ ADR≤70 filter WOULD HELP — consider adding "
                                   "this system to _ADR_QUALITY_SYSTEMS")
                        verdict_color = "#1b8a2a"
                    elif bk_avg > 1.0:
                        verdict = ("⚠️ ADR≤70 filter would HURT — blocked trades "
                                   "are net-profitable, do not apply")
                        verdict_color = "#c62828"
                    else:
                        verdict = ("↔️ ADR≤70 filter is BORDERLINE — pair-blacklist "
                                   "likely already covers this; statistical view only")
                        verdict_color = "#e65100"
                    impact_label = "If ADR≤70 filter were added, it would"

                p.append(f'<div style="margin-top:8px; padding:8px; background:#f5f5f5; '
                         f'border-left:4px solid {verdict_color}; font-size:10pt;">'
                         f'<b>{impact_label}:</b> block '
                         f'<b>{bk_n}</b> trades '
                         f'(WR {bk_wr:.0f}%, P/L {bk_pnl:+.1f}p, avg {bk_avg:+.2f}p), '
                         f'keep <b>{kp_n}</b> '
                         f'(WR {kp_wr:.0f}%, P/L {kp_pnl:+.1f}p)<br>'
                         f'<span style="color:{verdict_color}; font-weight:bold;">'
                         f'→ {verdict}</span></div>')

        # ── DTC-combo: per-source breakdown ──
        # Shows which feeder system (SS / ATR / B-tuned) is producing the
        # best DTC outcomes. Closed trades only. Always renders unfiltered
        # (i.e. ignores any source filter on the page) so the user can
        # compare lineages side-by-side.
        if system_label == "DTC-combo":
            _src_groups: dict[str, list] = {}
            for r in _closed:
                src = getattr(r, "dtc_source_system", "") or ""
                if not src:
                    continue
                _src_groups.setdefault(src, []).append(r)
            if _src_groups:
                _src_meta = {
                    "sv2_ss":      ("SS",      "#9c27b0"),
                    "sv2_atr":     ("ATR",     "#e65100"),
                    "sv2_b_tuned": ("B-tuned", "#00838f"),
                }
                p.append('<div class="section-title">DTC Source Breakdown '
                         '<span style="font-weight:normal; color:#888; font-size:9pt;">'
                         '(which feeder system signaled each DTC trade — closed only)</span></div>')
                p.append('<table style="width:100%; border-collapse:collapse; font-size:11px;">')
                p.append('<tr style="background:#f0f4f8; color:#4a6fa5; font-weight:bold;">'
                         '<td style="padding:4px 8px;">Source</td>'
                         '<td>Trades</td><td>WR</td><td>Total P/L</td>'
                         '<td>Avg P/L</td><td>Avg R</td><td>TP</td><td>SL</td><td>Signal</td></tr>')
                # Sort by total P/L desc — best feeder first
                _src_sorted = sorted(_src_groups.items(),
                                     key=lambda kv: -sum(r.pnl_pips for r in kv[1]))
                for src_key, group in _src_sorted:
                    label, color = _src_meta.get(src_key, (src_key, "#666"))
                    gw = sum(1 for r in group if r.is_win)
                    gt = len(group)
                    gpnl = sum(r.pnl_pips for r in group)
                    gwr = gw / gt * 100 if gt else 0
                    gavg = gpnl / gt if gt else 0
                    # Avg R-multiple per trade (P/L pips / SL pips)
                    _g_r = [r.pnl_pips / r.sl_pips for r in group if r.sl_pips > 0]
                    g_avg_r = sum(_g_r) / len(_g_r) if _g_r else 0.0
                    gtp = sum(1 for r in group if r.close_reason == "tp_hit")
                    gsl = sum(1 for r in group if r.close_reason == "sl_hit")
                    gsig = sum(1 for r in group if r.close_reason == "signal_exit")
                    gc = "#1b8a2a" if gpnl >= 0 else "#c62828"
                    ac = "#1b8a2a" if gavg >= 0 else "#c62828"
                    rc = "#1b8a2a" if g_avg_r >= 0 else "#c62828"
                    p.append(f'<tr><td style="padding:3px 8px; font-weight:bold; color:{color};">'
                             f'{label}</td>'
                             f'<td>{gt}</td><td>{gwr:.0f}%</td>'
                             f'<td style="color:{gc};">{gpnl:+.1f}p</td>'
                             f'<td style="color:{ac};">{gavg:+.1f}p</td>'
                             f'<td style="color:{rc};">{g_avg_r:+.2f}R</td>'
                             f'<td>{gtp}</td><td>{gsl}</td><td>{gsig}</td></tr>')
                p.append('</table>')
                # Trades missing source tag (legacy DTC entries from before tagging)
                _untagged = sum(1 for r in _closed
                                if not (getattr(r, "dtc_source_system", "") or ""))
                if _untagged:
                    p.append(f'<div style="font-size:9px; color:#999; margin-top:2px;">'
                             f'+ {_untagged} legacy DTC trades without source tag '
                             f'(closed before source-tracking was added)</div>')

        # Entry type breakdown (Standard vs Acceleration) — closed trades only
        std_trades = [r for r in _closed if getattr(r, 'entry_type', 'standard') == 'standard']
        accel_trades = [r for r in _closed if getattr(r, 'entry_type', 'standard') == 'acceleration']
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
            for r in _closed:
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

            # Session breakdown — closed trades only
            sess_stats: dict[str, list] = defaultdict(list)
            for r in _closed:
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

        cols = ["#", "Time", "Pair", "Dir", "Type", "Session", "Entry", "Close", "SL", "TP",
                "P/L", "Peak", "Worst", "PB", "Exit", "Dur", "Conv", "ADR%",
                "4h MFE", "4h MAE", "4h End",
                "M5b", "M5q", "M15b", "M15q", "H1b", "H1q", "H4b", "H4q",
                "Spread", "StdDev", "H1ATR", "Struct", "Tier",
                "Vol×", "Build", "DDayH", "DDayL", "DWkH", "DWkL", "DMoH", "DMoL",
                "Clust", "D00", "D000", "SessM", "DoW", "PrevR", "Conc", "M1Bod", "M1Dir", "ATR×",
                "T2P", "PF1st", "NrSL", "NrTP", "Bars",
                "ConvT", "ConvV", "ConvI", "ConvS", "SLx", "TPx",
                "StrCcy", "WkCcy", "StrR", "WkR", "TopGap", "BotGap",
                "StrVel", "WkVel", "M5Slop", "NewsAge"]
        table.setSortingEnabled(False)
        table.setColumnCount(len(cols))
        table.setHorizontalHeaderLabels(cols)
        table.setRowCount(len(records))

        total = len(records)
        for row, r in enumerate(reversed(records)):  # newest first, but #1 = oldest
            # Trade number: #1 = oldest, #N = newest (displayed top = newest)
            trade_num = total - row
            num_item = _NumericItem(str(trade_num), float(trade_num))
            num_item.setForeground(QColor("#888"))
            table.setItem(row, 0, num_item)

            table.setItem(row, 1, QTableWidgetItem(
                _to_jst_str(r.entry_time, "%m-%d %H:%M") if r.entry_time else "—"
            ))
            table.setItem(row, 2, QTableWidgetItem(r.pair))

            dir_item = QTableWidgetItem(r.direction)
            dir_item.setForeground(QColor("#1b8a2a" if r.direction == "BUY" else "#c62828"))
            table.setItem(row, 3, dir_item)

            # Entry type
            etype = getattr(r, 'entry_type', 'standard')
            if etype == "stoch_v2":
                etype_label = "\u26a1Sv2"
                etype_color = "#2e7d32"
            elif etype == "acceleration":
                etype_label = "\u26a1ACL"
                etype_color = "#ff9800"
            else:
                etype_label = "STND"
                etype_color = "#2a5a8a"
            etype_item = QTableWidgetItem(etype_label)
            etype_item.setForeground(QColor(etype_color))
            etype_item.setFont(QFont("Segoe UI", 9, QFont.Weight.Bold))
            table.setItem(row, 4, etype_item)

            # Session
            sess_item = QTableWidgetItem(r.session or "—")
            sess_item.setForeground(QColor("#4a6fa5"))
            table.setItem(row, 5, sess_item)

            table.setItem(row, 6, _NumericItem(f"{r.entry_price:.5f}", r.entry_price))

            _is_open_sv2 = r.close_time == 0 and not r.close_reason

            if _is_open_sv2:
                _open_item = QTableWidgetItem("OPEN")
                _open_item.setForeground(QColor("#2196f3"))
                _open_item.setFont(QFont("Segoe UI", 9, QFont.Weight.Bold))
                table.setItem(row, 7, _open_item)  # Close
                table.setItem(row, 8, _NumericItem(f"{r.sl_pips:.1f}", r.sl_pips))
                table.setItem(row, 9, _NumericItem(f"{r.tp_pips:.1f}", r.tp_pips))
                table.setItem(row, 10, QTableWidgetItem("—"))  # P/L
                table.setItem(row, 11, QTableWidgetItem("—"))  # Peak
                table.setItem(row, 12, QTableWidgetItem("—"))  # Worst
                table.setItem(row, 13, QTableWidgetItem("—"))  # PB
                _open_exit = QTableWidgetItem("OPEN")
                _open_exit.setForeground(QColor("#2196f3"))
                _open_exit.setFont(QFont("Segoe UI", 9, QFont.Weight.Bold))
                table.setItem(row, 14, _open_exit)  # Exit
                table.setItem(row, 15, QTableWidgetItem("—"))  # Duration
            else:
                table.setItem(row, 7, _NumericItem(f"{r.close_price:.5f}", r.close_price))
                table.setItem(row, 8, _NumericItem(f"{r.sl_pips:.1f}", r.sl_pips))
                table.setItem(row, 9, _NumericItem(f"{r.tp_pips:.1f}", r.tp_pips))

                pnl_item = _NumericItem(f"{r.pnl_pips:+.1f}", r.pnl_pips)
                pnl_item.setForeground(QColor("#1b8a2a" if r.pnl_pips >= 0 else "#c62828"))
                pnl_item.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
                table.setItem(row, 10, pnl_item)

                table.setItem(row, 11, _NumericItem(f"{r.peak_pnl_pips:+.1f}", r.peak_pnl_pips))
                table.setItem(row, 12, _NumericItem(f"{r.worst_pnl_pips:+.1f}", r.worst_pnl_pips))

                # PB (pullback before TP) — only for TP hits
                if r.close_reason == "tp_hit" and r.worst_pnl_pips < 0:
                    pb_pips = abs(r.worst_pnl_pips)
                    pb_pct = (pb_pips / r.sl_pips * 100) if r.sl_pips > 0 else 0
                    pb_item = _NumericItem(f"{pb_pips:.1f}p ({pb_pct:.0f}%)", pb_pips)
                    pb_item.setForeground(QColor("#e65100"))
                    table.setItem(row, 13, pb_item)
                else:
                    table.setItem(row, 13, QTableWidgetItem("—"))

                reason_labels = {"sl_hit": "SL", "tp_hit": "TP", "signal_exit": "Signal"}
                reason_colors = {"sl_hit": "#c62828", "tp_hit": "#1b8a2a", "signal_exit": "#e65100"}
                reason_item = QTableWidgetItem(reason_labels.get(r.close_reason, r.close_reason))
                reason_item.setForeground(QColor(reason_colors.get(r.close_reason, "#666")))
                table.setItem(row, 14, reason_item)

                table.setItem(row, 15, _NumericItem(f"{r.duration_minutes:.0f}m", r.duration_minutes))
            table.setItem(row, 16, _NumericItem(str(r.entry_conviction), float(r.entry_conviction)))

            # ADR consumed at entry
            adr_pct = getattr(r, 'adr_consumed_pct', 0.0)
            adr_item = _NumericItem(f"{adr_pct:.0f}%", adr_pct)
            if adr_pct >= 80:
                adr_item.setForeground(QColor("#c62828"))
            elif adr_pct >= 50:
                adr_item.setForeground(QColor("#e65100"))
            else:
                adr_item.setForeground(QColor("#2e7d32"))
            table.setItem(row, 17, adr_item)

            # Post-close 4h observation columns
            if r.post_close_complete:
                mfe4 = _NumericItem(f"{r.post_close_max_mfe_pips:+.1f}", r.post_close_max_mfe_pips)
                mfe4.setForeground(QColor("#1b8a2a"))
                table.setItem(row, 18, mfe4)

                mae4 = _NumericItem(f"{r.post_close_max_mae_pips:.1f}", r.post_close_max_mae_pips)
                mae4.setForeground(QColor("#c62828"))
                table.setItem(row, 19, mae4)

                end4 = _NumericItem(f"{r.post_close_final_pips:+.1f}", r.post_close_final_pips)
                end4.setForeground(QColor("#1b8a2a" if r.post_close_final_pips >= 0 else "#c62828"))
                table.setItem(row, 20, end4)
            else:
                watching_txt = "..." if r.close_time > 0 and not r.post_close_complete else "—"
                table.setItem(row, 18, QTableWidgetItem(watching_txt))
                table.setItem(row, 19, QTableWidgetItem(watching_txt))
                table.setItem(row, 20, QTableWidgetItem(watching_txt))

            # ── Entry signal data columns (21-33) ──
            _signal_fields = [
                (21, "entry_m5_base"),  (22, "entry_m5_quote"),
                (23, "entry_m15_base"), (24, "entry_m15_quote"),
                (25, "entry_h1_base"),  (26, "entry_h1_quote"),
                (27, "entry_h4_base"),  (28, "entry_h4_quote"),
            ]
            for col_idx, field in _signal_fields:
                val = getattr(r, field, 0.0)
                if val > 0:
                    item = _NumericItem(f"{val:.1f}", val)
                    if val >= 7.0:
                        item.setForeground(QColor("#1b8a2a"))
                    elif val <= 3.0:
                        item.setForeground(QColor("#c62828"))
                    else:
                        item.setForeground(QColor("#888888"))
                    table.setItem(row, col_idx, item)
                else:
                    table.setItem(row, col_idx, QTableWidgetItem("..."))

            _spread = getattr(r, "entry_div_spread", 0.0)
            if _spread != 0.0:
                _sp_item = _NumericItem(f"{_spread:+.1f}", _spread)
                _sp_item.setForeground(QColor("#1b8a2a" if abs(_spread) >= 12 else "#e65100" if abs(_spread) >= 8 else "#888"))
                table.setItem(row, 29, _sp_item)
            else:
                table.setItem(row, 29, QTableWidgetItem("..."))

            _std = getattr(r, "entry_spread_std", 0.0)
            if _std > 0:
                _std_item = _NumericItem(f"{_std:.2f}", _std)
                _std_item.setForeground(QColor("#1b8a2a" if _std <= 1.5 else "#e65100" if _std <= 3.0 else "#c62828"))
                table.setItem(row, 30, _std_item)
            else:
                table.setItem(row, 30, QTableWidgetItem("..."))

            _atr = getattr(r, "entry_h1_atr_pips", 0.0)
            if _atr > 0:
                table.setItem(row, 31, _NumericItem(f"{_atr:.1f}", _atr))
            else:
                table.setItem(row, 31, QTableWidgetItem("..."))

            _struct = getattr(r, "entry_structural", "")
            if _struct:
                _struct_item = QTableWidgetItem(_struct[:25])
                _struct_item.setForeground(QColor("#1b8a2a" if _struct == "OK" else "#c62828"))
                table.setItem(row, 32, _struct_item)
            else:
                table.setItem(row, 32, QTableWidgetItem("..."))

            _tier = getattr(r, "entry_tier", "")
            if _tier:
                _tier_item = QTableWidgetItem(_tier)
                _tier_colors = {"FULL": "#1b8a2a", "DIMMED": "#e65100", "SUPPRESSED": "#c62828", "QM4": "#ff6f00"}
                _tier_item.setForeground(QColor(_tier_colors.get(_tier, "#888")))
                table.setItem(row, 33, _tier_item)
            else:
                table.setItem(row, 33, QTableWidgetItem("..."))

            # ── Deep analytics columns (34-54) ──
            _populate_analytics_cols(table, row, r, start_col=34)

        table.setSortingEnabled(True)
        header = table.horizontalHeader()
        header.setStretchLastSection(True)
        # Resize to fit content ONCE (not as permanent mode which is slow)
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        for col in range(len(cols)):
            header.resizeSection(col, max(
                header.sectionSizeHint(col),
                table.sizeHintForColumn(col) + 12,
            ))

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

    # ── CSI Alerts tab helpers ─────────────────────────────────────

    def _load_csi_records(self) -> list[dict]:
        """Load CSI alert log records from JSON Lines file."""
        if not self._csi_log_file or not self._csi_log_file.exists():
            return []
        import json as _json
        records: list[dict] = []
        with self._csi_log_file.open(encoding="utf-8") as fh:
            for raw in fh:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    records.append(_json.loads(raw))
                except Exception:
                    pass
        return records

    def _update_csi_combos(self) -> None:
        """Populate the Ccy/Pair filter combo with unique values from the log."""
        items: set[str] = set()
        for r in self._csi_records:
            if r.get("kind") == "currency":
                items.add(r.get("currency", ""))
            else:
                items.add(r.get("pair", ""))
        items.discard("")
        current = self._csi_ccy_combo.currentText()
        self._csi_ccy_combo.blockSignals(True)
        self._csi_ccy_combo.clear()
        self._csi_ccy_combo.addItem("ALL")
        self._csi_ccy_combo.addItems(sorted(items))
        if current in (["ALL"] + sorted(items)):
            self._csi_ccy_combo.setCurrentText(current)
        self._csi_ccy_combo.blockSignals(False)

    def _render_csi(self) -> None:
        """Filter and display CSI alerts based on current combo selections."""
        type_filter = self._csi_type_combo.currentText()
        ccy_filter  = self._csi_ccy_combo.currentText()

        records = self._csi_records
        if type_filter and type_filter != "ALL":
            if type_filter == "PAIR":
                records = [r for r in records if r.get("kind") == "pair"]
            else:
                records = [
                    r for r in records
                    if r.get("kind") == "currency" and r.get("alert_type") == type_filter
                ]
        if ccy_filter and ccy_filter != "ALL":
            records = [
                r for r in records
                if r.get("currency") == ccy_filter or r.get("pair") == ccy_filter
            ]

        self._csi_summary_label.setText(self._build_csi_html(records))
        self._populate_csi_table(records)
        n = len(records)
        t = "all" if (type_filter == "ALL" and ccy_filter == "ALL") else f"{type_filter}/{ccy_filter}"
        self._csi_table_title.setText(f"CSI Alert Log — {n} entries ({t})")

    def _build_csi_html(self, records: list[dict]) -> str:
        """Build HTML summary for the CSI tab."""
        p = [_CSS]
        p.append('<div class="header">CSI Alert History</div>')
        p.append(f'<div class="subtitle">{len(records)} alerts in current filter</div>')

        if not records:
            p.append('<div class="empty">No CSI alerts yet. '
                     'CSI alerts fire when a currency reaches extreme strength across '
                     'MTF / HTF / XHTF timeframes, or when a strong+weak pair is found.</div>')
            return "".join(p)

        # Count by type
        from collections import Counter
        type_counts: Counter = Counter()
        dir_counts:  Counter = Counter()
        ccy_counts:  Counter = Counter()
        today_count = 0
        import time as _time
        now = _time.time()
        day_s = 86400.0

        for r in records:
            atype = r.get("alert_type", "?")
            kind  = r.get("kind", "currency")
            label = f"PAIR/{atype}" if kind == "pair" else atype
            type_counts[label] += 1
            dir_counts[r.get("direction", "?")] += 1
            if now - r.get("timestamp", 0) < day_s:
                today_count += 1
            if kind == "currency":
                ccy_counts[r.get("currency", "?")] += 1
            else:
                for c in [r.get("pair", "")[:3], r.get("pair", "")[3:]]:
                    if c:
                        ccy_counts[c] += 1

        # Summary cards
        p.append('<table style="border-collapse:collapse; margin:8px 0;">')
        p.append('<tr>')
        for label, cnt in sorted(type_counts.items()):
            p.append(
                f'<td style="padding:6px 14px; background:#e8f0fe; border-radius:4px; '
                f'margin:2px; font-weight:bold; color:#2a5a8a; text-align:center;">'
                f'{label}<br><span style="font-size:16px;">{cnt}</span></td>'
            )
        p.append('</tr></table>')

        p.append(f'<div style="margin:4px 0 8px; color:#555;">Today: {today_count}</div>')

        # Top 3 currencies
        top3 = ccy_counts.most_common(3)
        if top3:
            p.append('<div style="font-weight:bold; color:#2a5a8a; margin-top:8px;">Most active currencies:</div>')
            p.append('<ol style="margin:4px 0 0 18px; color:#444;">')
            for ccy, cnt in top3:
                p.append(f'<li>{ccy} — {cnt} alerts</li>')
            p.append('</ol>')

        return "".join(p)

    # CSI table column headers
    _CSI_COLS = [
        "#", "Date/Time", "Type", "Dir", "Ccy/Pair",
        "Align", "TF1", "TF2", "TF3", "Sum", "Depth%", "Trigger", "Rec.Pair",
    ]

    def _populate_csi_table(self, records: list[dict]) -> None:
        """Fill the CSI sortable table from log records (newest first)."""
        tbl = self._csi_table
        tbl.setSortingEnabled(False)
        tbl.setColumnCount(len(self._CSI_COLS))
        tbl.setHorizontalHeaderLabels(self._CSI_COLS)
        tbl.setRowCount(len(records))

        # Reverse so newest at top
        for row, r in enumerate(reversed(records)):
            num = len(records) - row

            ts  = r.get("timestamp", 0.0)
            dt  = _to_jst_str(ts, "%m-%d %H:%M")
            kind = r.get("kind", "currency")

            if kind == "currency":
                atype   = r.get("alert_type", "")
                direc   = r.get("direction", "")
                subj    = r.get("currency", "")
                align   = r.get("alignment", 0)
                depth   = r.get("depth_pct", 0.0)
                trigger = r.get("reason", "")
                rec_pair = r.get("best_pair", "")
                tf_s    = r.get("tf_scores", {})
                tf_vals = list(tf_s.values())[:3]
                total   = r.get("cumulative", 0.0)
            else:
                atype   = r.get("alert_type", "") + " PAIR"
                direc   = r.get("direction", "")
                subj    = r.get("pair", "")
                b_align = r.get("base_alignment", 0)
                q_align = r.get("quote_alignment", 0)
                align   = max(b_align, q_align)
                depth   = 0.0
                trigger = r.get("trigger_type", "")
                rec_pair = ""
                spread  = r.get("spread", 0.0)
                b_scores = list(r.get("base_scores", {}).values())[:3]
                q_scores = list(r.get("quote_scores", {}).values())[:3]
                tf_vals  = b_scores
                total    = spread

            cells = [
                (str(num),         float(num)),
                (dt,               ts),
                (atype,            0.0),
                (direc,            0.0),
                (subj,             0.0),
                (str(align),       float(align)),
                (f"{tf_vals[0]:.1f}" if len(tf_vals) > 0 else "—", tf_vals[0] if tf_vals else 0.0),
                (f"{tf_vals[1]:.1f}" if len(tf_vals) > 1 else "—", tf_vals[1] if len(tf_vals) > 1 else 0.0),
                (f"{tf_vals[2]:.1f}" if len(tf_vals) > 2 else "—", tf_vals[2] if len(tf_vals) > 2 else 0.0),
                (f"{total:.1f}",   total),
                (f"{depth:.0f}%",  depth),
                (trigger,          0.0),
                (rec_pair,         0.0),
            ]

            for col, (text, sort_val) in enumerate(cells):
                item = _NumericItem(text, sort_val) if isinstance(sort_val, float) else QTableWidgetItem(text)
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                # Colour direction column
                if col == 3:
                    if direc in ("WEAK", "SELL"):
                        item.setForeground(QColor("#c62828"))
                    elif direc in ("STRONG", "BUY"):
                        item.setForeground(QColor("#1b8a2a"))
                tbl.setItem(row, col, item)

        tbl.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        tbl.setColumnWidth(0, 36)
        tbl.setSortingEnabled(True)

    # ── System B (Sv2+SS) tab helpers ────────────────────────────

    def _load_tuned_records(self, trades_file, paper_trader) -> list[PaperTradeRecord]:
        """Generic loader for tuned system records."""
        if paper_trader and hasattr(paper_trader, '_journal') and paper_trader._journal:
            return list(paper_trader._journal)
        if not trades_file or not trades_file.exists():
            return []
        try:
            import json as _json
            data = _json.loads(trades_file.read_text(encoding="utf-8"))
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

    def _update_generic_pair_combo(self, combo: QComboBox, records: list[PaperTradeRecord]) -> None:
        """Generic pair combo updater for any paper trade tab."""
        pairs = sorted(set(r.pair for r in records))
        current = combo.currentText()
        combo.blockSignals(True)
        combo.clear()
        combo.addItem("ALL")
        combo.addItems(pairs)
        if current in (["ALL"] + pairs):
            combo.setCurrentText(current)
        combo.blockSignals(False)

    def _load_ss_records(self) -> list[PaperTradeRecord]:
        if not self._ss_trades_file or not self._ss_trades_file.exists():
            return []
        try:
            import json as _json
            data = _json.loads(self._ss_trades_file.read_text(encoding="utf-8"))
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

    def _update_ss_pair_combo(self) -> None:
        pairs = sorted(set(r.pair for r in self._ss_records))
        current = self._ss_pair_combo.currentText()
        self._ss_pair_combo.blockSignals(True)
        self._ss_pair_combo.clear()
        self._ss_pair_combo.addItem("ALL")
        self._ss_pair_combo.addItems(pairs)
        if current in (["ALL"] + pairs):
            self._ss_pair_combo.setCurrentText(current)
        self._ss_pair_combo.blockSignals(False)

    def _render_a_tuned(self) -> None:
        selected = self._a_tuned_pair_combo.currentText()
        if selected and selected != "ALL":
            records = [r for r in self._a_tuned_records if r.pair == selected]
        else:
            records = self._a_tuned_records
        self._a_tuned_content_label.setText(
            self._build_paper_html(records, selected, system_label="Sv2-tuned"))
        self._populate_paper_table_generic(records, self._a_tuned_trades_table, self._a_tuned_table_title, "Sv2-tuned")
        sorted_records = sorted(records, key=lambda r: r.entry_time)
        self._a_tuned_equity_chart.set_data(sorted_records, start_capital=1000.0, risk_pct=0.03)

    def _render_ss(self) -> None:
        selected = self._ss_pair_combo.currentText()
        if selected and selected != "ALL":
            records = [r for r in self._ss_records if r.pair == selected]
        else:
            records = self._ss_records

        self._ss_content_label.setText(
            self._build_paper_html(records, selected, system_label="SS"))
        self._populate_paper_table_generic(records, self._ss_trades_table, self._ss_table_title, "Sv2+SS")
        sorted_records = sorted(records, key=lambda r: r.entry_time)
        self._ss_equity_chart.set_data(sorted_records, start_capital=1000.0, risk_pct=0.03)

    def _render_b_tuned(self) -> None:
        selected = self._b_tuned_pair_combo.currentText()
        if selected and selected != "ALL":
            records = [r for r in self._b_tuned_records if r.pair == selected]
        else:
            records = self._b_tuned_records
        self._b_tuned_content_label.setText(
            self._build_paper_html(records, selected, system_label="B-tuned"))
        self._populate_paper_table_generic(records, self._b_tuned_trades_table, self._b_tuned_table_title, "Sv2+SS-tuned")
        sorted_records = sorted(records, key=lambda r: r.entry_time)
        self._b_tuned_equity_chart.set_data(sorted_records, start_capital=1000.0, risk_pct=0.03)

    def _build_ss_html(self, records: list[PaperTradeRecord], pair_filter: str) -> str:
        p = [_CSS]
        title = f"Sv2+SS: {pair_filter}" if pair_filter != "ALL" else "System B — Sv2 + Spread Stability"
        p.append(f'<div class="header">{title}</div>')
        p.append(f'<div class="subtitle">{len(records)} completed trades</div>')

        if not records:
            p.append('<div class="empty">No Sv2+SS trades yet. '
                     'System B opens trades only when spread stability (StdDev &le; 3.0) is confirmed.</div>')
            return "".join(p)

        wins = [r for r in records if r.is_win]
        losses = [r for r in records if not r.is_win]
        total_pnl = sum(r.pnl_pips for r in records)
        avg_pnl = total_pnl / len(records)

        p.append('<table style="border-collapse:collapse; margin:8px 0;">')
        p.append('<tr>')
        for label, val in [
            ("Trades", str(len(records))),
            ("Win Rate", f"{len(wins)/len(records)*100:.1f}%"),
            ("Total P/L", f"{total_pnl:+.1f}p"),
            ("Avg P/L", f"{avg_pnl:+.1f}p"),
            ("TP Hits", str(sum(1 for r in records if r.close_reason == "tp_hit"))),
            ("SL Hits", str(sum(1 for r in records if r.close_reason == "sl_hit"))),
        ]:
            color = "#1b8a2a" if "+" in val else "#c62828" if "-" in val else "#2a5a8a"
            p.append(
                f'<td style="padding:6px 14px; background:#f0e6f6; border-radius:4px; '
                f'margin:2px; text-align:center;">'
                f'{label}<br><span style="font-size:16px; font-weight:bold; color:{color};">{val}</span></td>'
            )
        p.append('</tr></table>')

        return "".join(p)

    def _populate_paper_table_generic(
        self, records: list[PaperTradeRecord], table: QTableWidget,
        title: QLabel, system_name: str,
    ) -> None:
        """Fill a paper trades table — same columns as System A Paper Trades.

        DTC-combo gets one extra trailing column ("Source") that shows which
        source system (SS/ATR/B-tuned) signaled each trade — useful for
        analysing which feeder system produces the best DTC outcomes.
        """
        title.setText(f"{system_name} Trades ({len(records)})")
        _is_dtc = system_name == "DTC-combo"
        cols = ["#", "Time", "Pair", "Dir", "Session", "Entry", "Close",
                "SL", "TP", "P/L", "Peak", "Worst", "PB", "Exit", "Dur",
                "Conv", "ADR%", "4H MFE", "4H MAE", "4H END",
                "M5b", "M5q", "M15b", "M15q", "H1b", "H1q", "H4b", "H4q",
                "Spread", "StdDev", "H1ATR", "Struct", "Tier",
                "Vol×", "Build", "DDayH", "DDayL", "DWkH", "DWkL", "DMoH", "DMoL",
                "Clust", "D00", "D000", "SessM", "DoW", "PrevR", "Conc", "M1Bod", "M1Dir", "ATR×",
                "T2P", "PF1st", "NrSL", "NrTP", "Bars",
                "ConvT", "ConvV", "ConvI", "ConvS", "SLx", "TPx",
                "StrCcy", "WkCcy", "StrR", "WkR", "TopGap", "BotGap",
                "StrVel", "WkVel", "M5Slop", "NewsAge"]

        if _is_dtc:
            # "TYPE" column shows which of the 3 feeder systems (SS/ATR/B-tn)
            # signaled each DTC trade. Stored at logical index 72 (end of list)
            # but visually moved to position 4 (right after Dir) via the
            # header.moveSection() call at the bottom of this method, so the
            # user doesn't have to scroll right past 70 analytics columns.
            cols.append("TYPE")

        # ── Tier-1 + Tier-2 + Tier-3 feature columns (added 2026-04-30) ──
        # ~177 short-label columns mirroring the feat_* fields on
        # PaperTradeRecord. Hidden by default; toggled by the "Show Tier-1
        # Features" checkbox in the page header.
        # Logical indices: TIER1_COL_START..TIER1_COL_END-1
        TIER1_COL_LABELS = [
            # ─── Tier 1 (33) ────────────────────────────────────────
            # Microstructure (5)
            "CVD30m", "CVDdiv", "CVDpx", "Amihud", "KyleL",
            # Volatility T1 (6)
            "M15ATRpct", "H1ATRpct", "JumpDet", "M15YZ", "M15Skw", "M15Krt",
            # Regimes T1 (7)
            "ADX", "Chop", "Hurst", "KER", "Regime", "DonchPos", "AroonOsc",
            # Statistics T1 (2)
            "ACF1", "HalfLife",
            # CSI (3)
            "CSIdsp", "CSIstr", "CSIwk",
            # Cross-market T1 (1)
            "DXY",
            # Levels T1 (3)
            "D50p", "DBigFig", "POC",
            # Adversarial T1 (2)
            "StopHnt", "Sweep",
            # FX-specific T1 (3)
            "MoEnd", "LonFix", "ECBFix",
            # Schema (1)
            "FeatV",
            # ─── Tier 2 (Volatility additions, 18) ──────────────────
            "M15RV", "M15Park", "M15GK", "M15RS", "M15Bipw",
            "M15VolR", "M15VoV", "M15BBup", "M15BBlo", "M15BBwp",
            "M15KCup", "M15KClo", "M15BBKC",
            "H1RV", "H1Park", "H1YZ", "H1ATRp", "H1VolR",
            # Regimes additions (21)
            "+DI", "-DI", "AroonU", "AroonD", "Vortex+", "Vortex-",
            "KAMA", "STval", "STdir",
            "Tenkan", "Kijun", "SenA", "SenB", "AbvCld", "InCld", "BlwCld",
            "LRslp", "LRr2", "DFA", "TPers", "MRz",
            # Statistics additions (8)
            "ACF5", "ACF15", "PACF1", "PACF5", "Skw60", "Krt60", "FFTper", "FFTamp",
            # CSI deltas (8)
            "dUSD", "dEUR", "dGBP", "dJPY", "dCAD", "dAUD", "dNZD", "dCHF",
            # Cross-market additions (5)
            "EURidx", "JPYidx", "GBPidx", "AUDidx", "Carry",
            # Levels additions (30)
            "D25p", "DCent",
            "PDopn", "PDhi", "PDlo", "PDcls",
            "PWhi", "PWlo", "PMhi", "PMlo", "Yhi", "Ylo",
            "AsiaH", "AsiaL", "AsiaR", "LonH", "LonL",
            "PP", "R1", "R2", "S1", "S2",
            "FibPP", "FibR1", "FibS1", "CamR3", "CamS3",
            "VWAP", "sVWAP", "VAH", "VAL",
            # Patterns (4)
            "EqHi", "EqLo", "TLbrk", "Candle",
            # Adversarial additions (2)
            "RndMag", "TickZ",
            # FX-specific additions (5)
            "TYOFix", "TriArb", "DSTuk", "DSTus", "Hday",
            # Behavioral (5)
            "Sess", "FriLt", "SunOp", "Lunch", "DayQ",
            # ─── Tier 3 NETWORK (28) ────────────────────────────────
            "VIX", "VVIX", "SKEW", "MOVE",
            "Gold", "WTI", "Brent", "Cu", "NG",
            "SPX", "NDX", "N225", "DAX", "FTSE", "HSI", "BTC",
            "US10Y", "US2Y", "US3M", "Curve", "Real10Y", "HYoas", "IGoas", "TED",
            "EvtMin", "Blkout", "EvCnt", "EvTitl",
            "SnB", "SnQ", "NewsR", "Reddit",
            "COTbN", "COTqN",
        ]
        TIER1_COL_START = len(cols)
        cols += TIER1_COL_LABELS
        TIER1_COL_END = len(cols)
        # Cache on the table widget so the toggle handler can find them.
        table.setProperty("tier1_col_start", TIER1_COL_START)
        table.setProperty("tier1_col_end", TIER1_COL_END)

        table.setSortingEnabled(False)
        table.setColumnCount(len(cols))
        table.setHorizontalHeaderLabels(cols)
        table.setRowCount(len(records))

        total = len(records)
        for row, r in enumerate(reversed(records)):
            num = total - row
            table.setItem(row, 0, _NumericItem(str(num), float(num)))
            table.setItem(row, 1, QTableWidgetItem(
                _to_jst_str(r.entry_time, "%m-%d %H:%M") if r.entry_time else "—"
            ))
            table.setItem(row, 2, QTableWidgetItem(r.pair))

            dir_item = QTableWidgetItem(r.direction)
            dir_item.setForeground(QColor("#1b8a2a" if r.direction == "BUY" else "#c62828"))
            table.setItem(row, 3, dir_item)

            table.setItem(row, 4, QTableWidgetItem(r.session or "—"))
            table.setItem(row, 5, _NumericItem(f"{r.entry_price:.5f}", r.entry_price))

            _is_open = r.close_time == 0 and not r.close_reason

            if _is_open:
                # Trade still open — show OPEN marker for exit columns
                _open_item = QTableWidgetItem("OPEN")
                _open_item.setForeground(QColor("#2196f3"))
                _open_item.setFont(QFont("Segoe UI", 9, QFont.Weight.Bold))
                table.setItem(row, 6, _open_item)  # Close price
                table.setItem(row, 7, _NumericItem(f"{r.sl_pips:.1f}", r.sl_pips))
                table.setItem(row, 8, _NumericItem(f"{r.tp_pips:.1f}", r.tp_pips))
                table.setItem(row, 9, QTableWidgetItem("—"))   # P/L
                table.setItem(row, 10, QTableWidgetItem("—"))  # Peak
                table.setItem(row, 11, QTableWidgetItem("—"))  # Worst
                table.setItem(row, 12, QTableWidgetItem("—"))  # PB
                _open_exit = QTableWidgetItem("OPEN")
                _open_exit.setForeground(QColor("#2196f3"))
                _open_exit.setFont(QFont("Segoe UI", 9, QFont.Weight.Bold))
                table.setItem(row, 13, _open_exit)  # Exit
                table.setItem(row, 14, QTableWidgetItem("—"))  # Duration
            else:
                table.setItem(row, 6, _NumericItem(f"{r.close_price:.5f}", r.close_price))
                table.setItem(row, 7, _NumericItem(f"{r.sl_pips:.1f}", r.sl_pips))
                table.setItem(row, 8, _NumericItem(f"{r.tp_pips:.1f}", r.tp_pips))

                pnl_item = _NumericItem(f"{r.pnl_pips:+.1f}", r.pnl_pips)
                pnl_item.setForeground(QColor("#1b8a2a" if r.pnl_pips >= 0 else "#c62828"))
                pnl_item.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
                table.setItem(row, 9, pnl_item)

                table.setItem(row, 10, _NumericItem(f"{r.peak_pnl_pips:+.1f}", r.peak_pnl_pips))
                table.setItem(row, 11, _NumericItem(f"{r.worst_pnl_pips:+.1f}", r.worst_pnl_pips))

                # PB (pullback before TP)
                if r.close_reason == "tp_hit" and r.worst_pnl_pips < 0:
                    pb_pips = abs(r.worst_pnl_pips)
                    pb_pct = (pb_pips / r.sl_pips * 100) if r.sl_pips > 0 else 0
                    pb_item = _NumericItem(f"{pb_pips:.1f}p ({pb_pct:.0f}%)", pb_pips)
                    pb_item.setForeground(QColor("#e65100"))
                    table.setItem(row, 12, pb_item)
                else:
                    table.setItem(row, 12, QTableWidgetItem("—"))

                reason_labels = {"sl_hit": "SL", "tp_hit": "TP", "signal_exit": "Signal"}
                reason_colors = {"sl_hit": "#c62828", "tp_hit": "#1b8a2a", "signal_exit": "#e65100"}
                reason_item = QTableWidgetItem(reason_labels.get(r.close_reason, r.close_reason))
                reason_item.setForeground(QColor(reason_colors.get(r.close_reason, "#666")))
                table.setItem(row, 13, reason_item)

                table.setItem(row, 14, _NumericItem(f"{r.duration_minutes:.0f}m", r.duration_minutes))
            table.setItem(row, 15, _NumericItem(str(r.entry_conviction), float(r.entry_conviction)))

            adr_pct = getattr(r, 'adr_consumed_pct', 0.0)
            adr_item = _NumericItem(f"{adr_pct:.0f}%", adr_pct)
            if adr_pct >= 80:
                adr_item.setForeground(QColor("#c62828"))
            elif adr_pct >= 50:
                adr_item.setForeground(QColor("#e65100"))
            else:
                adr_item.setForeground(QColor("#2e7d32"))
            table.setItem(row, 16, adr_item)

            # Post-close 4h observation columns
            if r.post_close_complete:
                mfe4 = _NumericItem(f"{r.post_close_max_mfe_pips:+.1f}", r.post_close_max_mfe_pips)
                mfe4.setForeground(QColor("#1b8a2a"))
                table.setItem(row, 17, mfe4)

                mae4 = _NumericItem(f"{r.post_close_max_mae_pips:.1f}", r.post_close_max_mae_pips)
                mae4.setForeground(QColor("#c62828"))
                table.setItem(row, 18, mae4)

                end4 = _NumericItem(f"{r.post_close_final_pips:+.1f}", r.post_close_final_pips)
                end4.setForeground(QColor("#1b8a2a" if r.post_close_final_pips >= 0 else "#c62828"))
                table.setItem(row, 19, end4)
            else:
                watching_txt = "..." if r.close_time > 0 and not r.post_close_complete else "—"
                table.setItem(row, 17, QTableWidgetItem(watching_txt))
                table.setItem(row, 18, QTableWidgetItem(watching_txt))
                table.setItem(row, 19, QTableWidgetItem(watching_txt))

            # ── Entry signal data columns (20-32) ──
            _signal_fields = [
                (20, "entry_m5_base"),  (21, "entry_m5_quote"),
                (22, "entry_m15_base"), (23, "entry_m15_quote"),
                (24, "entry_h1_base"),  (25, "entry_h1_quote"),
                (26, "entry_h4_base"),  (27, "entry_h4_quote"),
            ]
            for col_idx, field in _signal_fields:
                val = getattr(r, field, 0.0)
                if val > 0:
                    item = _NumericItem(f"{val:.1f}", val)
                    # Color: green >= 7.0 (strong), red <= 3.0 (weak), orange between
                    if val >= 7.0:
                        item.setForeground(QColor("#1b8a2a"))
                    elif val <= 3.0:
                        item.setForeground(QColor("#c62828"))
                    else:
                        item.setForeground(QColor("#888888"))
                    table.setItem(row, col_idx, item)
                else:
                    table.setItem(row, col_idx, QTableWidgetItem("..."))

            # Spread (col 28)
            _spread = getattr(r, "entry_div_spread", 0.0)
            if _spread != 0.0:
                _sp_item = _NumericItem(f"{_spread:+.1f}", _spread)
                _sp_item.setForeground(QColor("#1b8a2a" if abs(_spread) >= 12 else "#e65100" if abs(_spread) >= 8 else "#888"))
                table.setItem(row, 28, _sp_item)
            else:
                table.setItem(row, 28, QTableWidgetItem("..."))

            # StdDev (col 29)
            _std = getattr(r, "entry_spread_std", 0.0)
            if _std > 0:
                _std_item = _NumericItem(f"{_std:.2f}", _std)
                _std_item.setForeground(QColor("#1b8a2a" if _std <= 1.5 else "#e65100" if _std <= 3.0 else "#c62828"))
                table.setItem(row, 29, _std_item)
            else:
                table.setItem(row, 29, QTableWidgetItem("..."))

            # H1ATR (col 30)
            _atr = getattr(r, "entry_h1_atr_pips", 0.0)
            if _atr > 0:
                table.setItem(row, 30, _NumericItem(f"{_atr:.1f}", _atr))
            else:
                table.setItem(row, 30, QTableWidgetItem("..."))

            # Struct (col 31)
            _struct = getattr(r, "entry_structural", "")
            if _struct:
                _struct_item = QTableWidgetItem(_struct[:25])
                _struct_item.setForeground(QColor("#1b8a2a" if _struct == "OK" else "#c62828"))
                table.setItem(row, 31, _struct_item)
            else:
                table.setItem(row, 31, QTableWidgetItem("..."))

            # Tier (col 32)
            _tier = getattr(r, "entry_tier", "")
            if _tier:
                _tier_item = QTableWidgetItem(_tier)
                _tier_colors = {"FULL": "#1b8a2a", "DIMMED": "#e65100", "SUPPRESSED": "#c62828", "QM4": "#ff6f00"}
                _tier_item.setForeground(QColor(_tier_colors.get(_tier, "#888")))
                table.setItem(row, 32, _tier_item)
            else:
                table.setItem(row, 32, QTableWidgetItem("..."))

            # ── Deep analytics columns (33-71) ──
            _populate_analytics_cols(table, row, r, start_col=33)

            # ── DTC-only: TYPE column (logical 72, moved to visual 4) ──
            if _is_dtc:
                _src = getattr(r, "dtc_source_system", "") or ""
                _src_labels = {
                    "sv2_ss":       ("\u26a1 SS",  "#9c27b0"),
                    "sv2_atr":      ("\u26a1 ATR", "#e65100"),
                    "sv2_b_tuned":  ("\u26a1 Btn", "#00838f"),
                }
                _label, _color = _src_labels.get(_src, ("\u2014", "#bbb"))
                _src_item = QTableWidgetItem(_label)
                _src_item.setForeground(QColor(_color))
                if _src:
                    _src_item.setFont(QFont("Segoe UI", 9, QFont.Weight.Bold))
                table.setItem(row, 72, _src_item)

            # ── Feature cells (cols TIER1_COL_START..TIER1_COL_END-1) ──
            # Format triplet: (value, format_string_or_None, is_numeric)
            # bool-typed values render as ✓/empty regardless of fmt.
            _tier1_values = [
                # ─── Tier 1 (33) ────────────────────────────────
                (getattr(r, "feat_cvd_30m", 0.0), "{:+.0f}", True),
                (getattr(r, "feat_cvd_divergent", False), None, False),
                (getattr(r, "feat_cvd_price_move_pips", 0.0), "{:+.1f}", True),
                (getattr(r, "feat_amihud_illiq_60m", 0.0), "{:.3f}", True),
                (getattr(r, "feat_kyle_lambda_60m", 0.0), "{:.2e}", True),
                (getattr(r, "feat_m15_atr14_pct_rank", 0.0), "{:.0f}%", True),
                (getattr(r, "feat_h1_atr14_pct_rank", 0.0), "{:.0f}%", True),
                (getattr(r, "feat_m15_jump_detected", False), None, False),
                (getattr(r, "feat_m15_yang_zhang", 0.0), "{:.4f}", True),
                (getattr(r, "feat_m15_realized_skew", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_m15_realized_kurt", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_h1_adx", 0.0), "{:.1f}", True),
                (getattr(r, "feat_h1_choppiness", 0.0), "{:.0f}", True),
                (getattr(r, "feat_h1_hurst", 0.5), "{:.2f}", True),
                (getattr(r, "feat_h1_kaufman_er", 0.0), "{:.2f}", True),
                (getattr(r, "feat_h1_regime", "") or "—", None, False),
                (getattr(r, "feat_h1_donchian_pos", 0.5), "{:.2f}", True),
                (getattr(r, "feat_h1_aroon_osc", 0.0), "{:+.0f}", True),
                (getattr(r, "feat_h1_acf_lag_1", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_h1_half_life_bars", 0.0), "{:.1f}", True),
                (getattr(r, "feat_csi_dispersion", 0.0), "{:.1f}", True),
                (getattr(r, "feat_csi_strong_count", 0), "{}", True),
                (getattr(r, "feat_csi_weak_count", 0), "{}", True),
                (getattr(r, "feat_dxy_synthetic", 0.0), "{:.2f}", True),
                (getattr(r, "feat_dist_to_50_pips", 0.0), "{:.1f}", True),
                (getattr(r, "feat_dist_to_big_figure_pips", 0.0), "{:.1f}", True),
                (getattr(r, "feat_h1_poc", 0.0), "{:.5f}", True),
                (getattr(r, "feat_h1_stop_hunt_score", 0.0), "{:.2f}", True),
                (getattr(r, "feat_h1_sweep_type", "") or "—", None, False),
                (getattr(r, "feat_is_month_end", False), None, False),
                (getattr(r, "feat_in_london_fix", False), None, False),
                (getattr(r, "feat_in_ecb_fix", False), None, False),
                (getattr(r, "feat_schema_version", 0), "{}", True),
                # ─── Tier 2 — Volatility additions (M15: 13, H1: 5) ──
                (getattr(r, "feat_m15_realized_var", 0.0), "{:.5f}", True),
                (getattr(r, "feat_m15_parkinson", 0.0), "{:.5f}", True),
                (getattr(r, "feat_m15_garman_klass", 0.0), "{:.5f}", True),
                (getattr(r, "feat_m15_rogers_satchell", 0.0), "{:.5f}", True),
                (getattr(r, "feat_m15_bipower", 0.0), "{:.5f}", True),
                (getattr(r, "feat_m15_vol_ratio", 0.0), "{:.2f}", True),
                (getattr(r, "feat_m15_vol_of_vol", 0.0), "{:.5f}", True),
                (getattr(r, "feat_m15_bb_upper", 0.0), "{:.5f}", True),
                (getattr(r, "feat_m15_bb_lower", 0.0), "{:.5f}", True),
                (getattr(r, "feat_m15_bb_width_pips", 0.0), "{:.1f}", True),
                (getattr(r, "feat_m15_kc_upper", 0.0), "{:.5f}", True),
                (getattr(r, "feat_m15_kc_lower", 0.0), "{:.5f}", True),
                (getattr(r, "feat_m15_bbkc_ratio", 1.0), "{:.2f}", True),
                (getattr(r, "feat_h1_realized_var", 0.0), "{:.5f}", True),
                (getattr(r, "feat_h1_parkinson", 0.0), "{:.5f}", True),
                (getattr(r, "feat_h1_yang_zhang", 0.0), "{:.5f}", True),
                (getattr(r, "feat_h1_atr14_pips", 0.0), "{:.1f}", True),
                (getattr(r, "feat_h1_vol_ratio", 0.0), "{:.2f}", True),
                # Regimes additions (21)
                (getattr(r, "feat_h1_plus_di", 0.0), "{:.1f}", True),
                (getattr(r, "feat_h1_minus_di", 0.0), "{:.1f}", True),
                (getattr(r, "feat_h1_aroon_up", 0.0), "{:.0f}", True),
                (getattr(r, "feat_h1_aroon_down", 0.0), "{:.0f}", True),
                (getattr(r, "feat_h1_vortex_plus", 0.0), "{:.2f}", True),
                (getattr(r, "feat_h1_vortex_minus", 0.0), "{:.2f}", True),
                (getattr(r, "feat_h1_kama", 0.0), "{:.5f}", True),
                (getattr(r, "feat_h1_supertrend_value", 0.0), "{:.5f}", True),
                (getattr(r, "feat_h1_supertrend_dir", 0), "{:+d}", True),
                (getattr(r, "feat_h1_ichimoku_tenkan", 0.0), "{:.5f}", True),
                (getattr(r, "feat_h1_ichimoku_kijun", 0.0), "{:.5f}", True),
                (getattr(r, "feat_h1_ichimoku_senkou_a", 0.0), "{:.5f}", True),
                (getattr(r, "feat_h1_ichimoku_senkou_b", 0.0), "{:.5f}", True),
                (getattr(r, "feat_h1_ichimoku_above_cloud", False), None, False),
                (getattr(r, "feat_h1_ichimoku_in_cloud", False), None, False),
                (getattr(r, "feat_h1_ichimoku_below_cloud", False), None, False),
                (getattr(r, "feat_h1_lr_slope", 0.0), "{:.6f}", True),
                (getattr(r, "feat_h1_lr_r2", 0.0), "{:.2f}", True),
                (getattr(r, "feat_h1_dfa", 0.5), "{:.2f}", True),
                (getattr(r, "feat_h1_trend_persistence", 0.0), "{:.2f}", True),
                (getattr(r, "feat_h1_mr_zscore", 0.0), "{:+.2f}", True),
                # Statistics additions (8)
                (getattr(r, "feat_h1_acf_lag_5", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_h1_acf_lag_15", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_h1_pacf_lag_1", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_h1_pacf_lag_5", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_h1_skew_60", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_h1_kurt_60", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_h1_fft_period_min", 0.0), "{:.0f}m", True),
                (getattr(r, "feat_h1_fft_amplitude_ratio", 0.0), "{:.2f}", True),
                # CSI deltas (8)
                (getattr(r, "feat_dUSD", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_dEUR", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_dGBP", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_dJPY", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_dCAD", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_dAUD", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_dNZD", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_dCHF", 0.0), "{:+.2f}", True),
                # Cross-market additions (5)
                (getattr(r, "feat_eur_index", 0.0), "{:.2f}", True),
                (getattr(r, "feat_jpy_index", 0.0), "{:.2f}", True),
                (getattr(r, "feat_gbp_index", 0.0), "{:.2f}", True),
                (getattr(r, "feat_aud_index", 0.0), "{:.2f}", True),
                (getattr(r, "feat_carry_pips_per_day", 0.0), "{:+.2f}", True),
                # Levels additions (30)
                (getattr(r, "feat_dist_to_25_pips", 0.0), "{:.1f}", True),
                (getattr(r, "feat_dist_to_century_pips", 0.0), "{:.1f}", True),
                (getattr(r, "feat_prev_day_open", 0.0), "{:.5f}", True),
                (getattr(r, "feat_prev_day_high", 0.0), "{:.5f}", True),
                (getattr(r, "feat_prev_day_low", 0.0), "{:.5f}", True),
                (getattr(r, "feat_prev_day_close", 0.0), "{:.5f}", True),
                (getattr(r, "feat_prev_week_high", 0.0), "{:.5f}", True),
                (getattr(r, "feat_prev_week_low", 0.0), "{:.5f}", True),
                (getattr(r, "feat_prev_month_high", 0.0), "{:.5f}", True),
                (getattr(r, "feat_prev_month_low", 0.0), "{:.5f}", True),
                (getattr(r, "feat_year_high", 0.0), "{:.5f}", True),
                (getattr(r, "feat_year_low", 0.0), "{:.5f}", True),
                (getattr(r, "feat_asian_session_high", 0.0), "{:.5f}", True),
                (getattr(r, "feat_asian_session_low", 0.0), "{:.5f}", True),
                (getattr(r, "feat_asian_session_range_pips", 0.0), "{:.1f}", True),
                (getattr(r, "feat_london_session_high", 0.0), "{:.5f}", True),
                (getattr(r, "feat_london_session_low", 0.0), "{:.5f}", True),
                (getattr(r, "feat_pivot_pp", 0.0), "{:.5f}", True),
                (getattr(r, "feat_pivot_r1", 0.0), "{:.5f}", True),
                (getattr(r, "feat_pivot_r2", 0.0), "{:.5f}", True),
                (getattr(r, "feat_pivot_s1", 0.0), "{:.5f}", True),
                (getattr(r, "feat_pivot_s2", 0.0), "{:.5f}", True),
                (getattr(r, "feat_fib_pp", 0.0), "{:.5f}", True),
                (getattr(r, "feat_fib_r1", 0.0), "{:.5f}", True),
                (getattr(r, "feat_fib_s1", 0.0), "{:.5f}", True),
                (getattr(r, "feat_cam_r3", 0.0), "{:.5f}", True),
                (getattr(r, "feat_cam_s3", 0.0), "{:.5f}", True),
                (getattr(r, "feat_h1_vwap", 0.0), "{:.5f}", True),
                (getattr(r, "feat_h1_session_vwap", 0.0), "{:.5f}", True),
                (getattr(r, "feat_h1_vah", 0.0), "{:.5f}", True),
                (getattr(r, "feat_h1_val", 0.0), "{:.5f}", True),
                # Patterns (4)
                (getattr(r, "feat_h1_equal_highs", False), None, False),
                (getattr(r, "feat_h1_equal_lows", False), None, False),
                (getattr(r, "feat_h1_trendline_break", "") or "—", None, False),
                (getattr(r, "feat_h1_candle_pattern", "") or "—", None, False),
                # Adversarial additions (2)
                (getattr(r, "feat_h1_round_magnetism", 0.0), "{:.2f}", True),
                (getattr(r, "feat_h1_tick_burst_z", 0.0), "{:+.1f}", True),
                # FX-specific additions (5)
                (getattr(r, "feat_in_tokyo_fix", False), None, False),
                (getattr(r, "feat_triangular_arb_pips", 0.0), "{:+.1f}", True),
                (getattr(r, "feat_dst_active_uk", False), None, False),
                (getattr(r, "feat_dst_active_us", False), None, False),
                (getattr(r, "feat_holiday_label", "") or "—", None, False),
                # Behavioral (5)
                (getattr(r, "feat_session_label", "") or "—", None, False),
                (getattr(r, "feat_friday_late", False), None, False),
                (getattr(r, "feat_sunday_open", False), None, False),
                (getattr(r, "feat_lunch_hour", "") or "—", None, False),
                (getattr(r, "feat_days_into_quarter", 0), "{}", True),
                # ─── Tier 3 NETWORK (28) ───────────────────────────
                (getattr(r, "feat_vix", 0.0), "{:.1f}", True),
                (getattr(r, "feat_vvix", 0.0), "{:.1f}", True),
                (getattr(r, "feat_skew", 0.0), "{:.1f}", True),
                (getattr(r, "feat_move", 0.0), "{:.1f}", True),
                (getattr(r, "feat_gold_close", 0.0), "{:.2f}", True),
                (getattr(r, "feat_wti_close", 0.0), "{:.2f}", True),
                (getattr(r, "feat_brent_close", 0.0), "{:.2f}", True),
                (getattr(r, "feat_copper_close", 0.0), "{:.3f}", True),
                (getattr(r, "feat_natgas_close", 0.0), "{:.2f}", True),
                (getattr(r, "feat_sp500_close", 0.0), "{:.0f}", True),
                (getattr(r, "feat_nasdaq_close", 0.0), "{:.0f}", True),
                (getattr(r, "feat_nikkei_close", 0.0), "{:.0f}", True),
                (getattr(r, "feat_dax_close", 0.0), "{:.0f}", True),
                (getattr(r, "feat_ftse_close", 0.0), "{:.0f}", True),
                (getattr(r, "feat_hang_seng_close", 0.0), "{:.0f}", True),
                (getattr(r, "feat_btc_close", 0.0), "{:.0f}", True),
                (getattr(r, "feat_fred_us_10y", 0.0), "{:.2f}%", True),
                (getattr(r, "feat_fred_us_2y", 0.0), "{:.2f}%", True),
                (getattr(r, "feat_fred_us_3m", 0.0), "{:.2f}%", True),
                (getattr(r, "feat_fred_yield_curve_2_10", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_fred_real_10y", 0.0), "{:.2f}%", True),
                (getattr(r, "feat_fred_hy_oas", 0.0), "{:.2f}", True),
                (getattr(r, "feat_fred_ig_oas", 0.0), "{:.2f}", True),
                (getattr(r, "feat_fred_ted_spread", 0.0), "{:.2f}", True),
                (getattr(r, "feat_minutes_to_next_high_event", -1.0), "{:.0f}m", True),
                (getattr(r, "feat_news_blackout", False), None, False),
                (getattr(r, "feat_events_today_count", 0), "{}", True),
                (getattr(r, "feat_next_event_title", "") or "—", None, False),
                (getattr(r, "feat_news_sent_base", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_news_sent_quote", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_news_flow_rate", 0.0), "{:.1f}", True),
                (getattr(r, "feat_reddit_forex_sentiment", 0.0), "{:+.2f}", True),
                (getattr(r, "feat_cot_base_net", 0), "{:+d}", True),
                (getattr(r, "feat_cot_quote_net", 0), "{:+d}", True),
            ]
            for offset, (val, fmt, numeric) in enumerate(_tier1_values):
                col_idx = TIER1_COL_START + offset
                if isinstance(val, bool):
                    item = QTableWidgetItem("✓" if val else "")
                    if val:
                        item.setForeground(QColor("#1b8a2a"))
                elif fmt is not None and numeric:
                    item = _NumericItem(fmt.format(val), float(val))
                else:
                    item = QTableWidgetItem(str(val))
                table.setItem(row, col_idx, item)

        table.setSortingEnabled(True)
        header = table.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        for col in range(len(cols)):
            header.resizeSection(col, max(
                header.sectionSizeHint(col),
                table.sizeHintForColumn(col) + 12,
            ))

        # ── Apply Tier-1 visibility based on the checkbox state ──
        # The toggle was created in _build_paper_page; its current state
        # was stashed as a property on the table widget. Hidden by default.
        try:
            _show_tier1 = bool(table.property("_feat_toggle_checked"))
        except Exception:
            _show_tier1 = False
        for col in range(TIER1_COL_START, TIER1_COL_END):
            table.setColumnHidden(col, not _show_tier1)

        # DTC-only: visually move the TYPE column (logical index 72) to
        # visual position 4 — right after Dir, before Session. Uses the
        # Qt header section-move API so we don't have to renumber 70+
        # hardcoded setItem indices. visualIndex(72) is idempotent —
        # calling it repeatedly after the first move is a no-op.
        if _is_dtc:
            try:
                _type_visual = header.visualIndex(72)
                if _type_visual != 4:
                    header.moveSection(_type_visual, 4)
            except Exception:
                pass  # column stays at end if reorder fails
            # Keep TYPE nicely sized — it's a short label column
            try:
                header.resizeSection(72, 72)
            except Exception:
                pass

    # ── System C (Sv2+ATR) tab helpers ────────────────────────────

    def _load_atr_records(self) -> list[PaperTradeRecord]:
        if not self._atr_trades_file or not self._atr_trades_file.exists():
            return []
        try:
            import json as _json
            data = _json.loads(self._atr_trades_file.read_text(encoding="utf-8"))
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

    def _update_atr_pair_combo(self) -> None:
        pairs = sorted(set(r.pair for r in self._atr_records))
        current = self._atr_pair_combo.currentText()
        self._atr_pair_combo.blockSignals(True)
        self._atr_pair_combo.clear()
        self._atr_pair_combo.addItem("ALL")
        self._atr_pair_combo.addItems(pairs)
        if current in (["ALL"] + pairs):
            self._atr_pair_combo.setCurrentText(current)
        self._atr_pair_combo.blockSignals(False)

    def _render_atr(self) -> None:
        selected = self._atr_pair_combo.currentText()
        if selected and selected != "ALL":
            records = [r for r in self._atr_records if r.pair == selected]
        else:
            records = self._atr_records
        self._atr_content_label.setText(
            self._build_paper_html(records, selected, system_label="ATR"))
        self._populate_paper_table_generic(records, self._atr_trades_table, self._atr_table_title, "Sv2+ATR")
        sorted_records = sorted(records, key=lambda r: r.entry_time)
        self._atr_equity_chart.set_data(sorted_records, start_capital=1000.0, risk_pct=0.03)

    def _render_sv2_upgraded(self) -> None:
        """Sv2-upgraded renderer (2026-04-23). Uses the same generic helpers
        as the other paper systems — identical layout/columns/metrics to Sv2
        so the two are directly comparable."""
        selected = self._sv2_upgraded_pair_combo.currentText()
        records = ([r for r in self._sv2_upgraded_records if r.pair == selected]
                   if selected and selected != "ALL" else self._sv2_upgraded_records)
        self._sv2_upgraded_content_label.setText(
            self._build_paper_html(records, selected, system_label="Sv2-upgraded"))
        self._populate_paper_table_generic(
            records, self._sv2_upgraded_trades_table,
            self._sv2_upgraded_table_title, "Sv2-upgraded")
        self._sv2_upgraded_equity_chart.set_data(
            sorted(records, key=lambda r: r.entry_time),
            start_capital=1000.0, risk_pct=0.03,
        )

    # ── 5 Live-Candle engine systems (mirrors of A/D/B/E/C) ───────
    # Each system uses the SAME _load_generic_records / _update_generic_pair_combo
    # / _build_paper_html / _populate_paper_table_generic helpers as the
    # standard tabs so the layout is 1:1 with the originals.

    def _render_sv2_live(self) -> None:
        selected = self._sv2_live_pair_combo.currentText()
        records = ([r for r in self._sv2_live_records if r.pair == selected]
                   if selected and selected != "ALL" else self._sv2_live_records)
        self._sv2_live_content_label.setText(
            self._build_paper_html(records, selected, system_label="Sv2-live"))
        self._populate_paper_table_generic(
            records, self._sv2_live_trades_table, self._sv2_live_table_title, "Sv2-live")
        self._sv2_live_equity_chart.set_data(
            sorted(records, key=lambda r: r.entry_time),
            start_capital=1000.0, risk_pct=0.03)

    def _render_sv2_a_tuned_live(self) -> None:
        selected = self._sv2_a_tuned_live_pair_combo.currentText()
        records = ([r for r in self._sv2_a_tuned_live_records if r.pair == selected]
                   if selected and selected != "ALL" else self._sv2_a_tuned_live_records)
        self._sv2_a_tuned_live_content_label.setText(
            self._build_paper_html(records, selected, system_label="Sv2-Tun-live"))
        self._populate_paper_table_generic(
            records, self._sv2_a_tuned_live_trades_table,
            self._sv2_a_tuned_live_table_title, "Sv2-Tun-live")
        self._sv2_a_tuned_live_equity_chart.set_data(
            sorted(records, key=lambda r: r.entry_time),
            start_capital=1000.0, risk_pct=0.03)

    def _render_sv2_ss_live(self) -> None:
        selected = self._sv2_ss_live_pair_combo.currentText()
        records = ([r for r in self._sv2_ss_live_records if r.pair == selected]
                   if selected and selected != "ALL" else self._sv2_ss_live_records)
        self._sv2_ss_live_content_label.setText(
            self._build_paper_html(records, selected, system_label="Sv2+SS-live"))
        self._populate_paper_table_generic(
            records, self._sv2_ss_live_trades_table,
            self._sv2_ss_live_table_title, "Sv2+SS-live")
        self._sv2_ss_live_equity_chart.set_data(
            sorted(records, key=lambda r: r.entry_time),
            start_capital=1000.0, risk_pct=0.03)

    def _render_sv2_b_tuned_live(self) -> None:
        selected = self._sv2_b_tuned_live_pair_combo.currentText()
        records = ([r for r in self._sv2_b_tuned_live_records if r.pair == selected]
                   if selected and selected != "ALL" else self._sv2_b_tuned_live_records)
        self._sv2_b_tuned_live_content_label.setText(
            self._build_paper_html(records, selected, system_label="Sv2+SS-Tun-live"))
        self._populate_paper_table_generic(
            records, self._sv2_b_tuned_live_trades_table,
            self._sv2_b_tuned_live_table_title, "Sv2+SS-Tun-live")
        self._sv2_b_tuned_live_equity_chart.set_data(
            sorted(records, key=lambda r: r.entry_time),
            start_capital=1000.0, risk_pct=0.03)

    def _render_sv2_atr_live(self) -> None:
        selected = self._sv2_atr_live_pair_combo.currentText()
        records = ([r for r in self._sv2_atr_live_records if r.pair == selected]
                   if selected and selected != "ALL" else self._sv2_atr_live_records)
        self._sv2_atr_live_content_label.setText(
            self._build_paper_html(records, selected, system_label="Sv2+ATR-live"))
        self._populate_paper_table_generic(
            records, self._sv2_atr_live_trades_table,
            self._sv2_atr_live_table_title, "Sv2+ATR-live")
        self._sv2_atr_live_equity_chart.set_data(
            sorted(records, key=lambda r: r.entry_time),
            start_capital=1000.0, risk_pct=0.03)

    # ── AU Gold suite (2026-04-24) — XAUUSD only ──────────────────
    # Re-uses the SAME generic helpers as the FX systems so columns,
    # equity curve, and HTML summary are identical for direct comparability.

    def _render_au1(self) -> None:
        selected = self._au1_pair_combo.currentText()
        records = ([r for r in self._au1_records if r.pair == selected]
                   if selected and selected != "ALL" else self._au1_records)
        self._au1_content_label.setText(
            self._build_paper_html(records, selected, system_label="AU1 London"))
        self._populate_paper_table_generic(
            records, self._au1_trades_table,
            self._au1_table_title, "AU1 London")
        self._au1_equity_chart.set_data(
            sorted(records, key=lambda r: r.entry_time),
            start_capital=1000.0, risk_pct=0.03)

    def _render_au2(self) -> None:
        selected = self._au2_pair_combo.currentText()
        records = ([r for r in self._au2_records if r.pair == selected]
                   if selected and selected != "ALL" else self._au2_records)
        self._au2_content_label.setText(
            self._build_paper_html(records, selected, system_label="AU2 NY-ORB"))
        self._populate_paper_table_generic(
            records, self._au2_trades_table,
            self._au2_table_title, "AU2 NY-ORB")
        self._au2_equity_chart.set_data(
            sorted(records, key=lambda r: r.entry_time),
            start_capital=1000.0, risk_pct=0.03)

    def _render_au3(self) -> None:
        selected = self._au3_pair_combo.currentText()
        records = ([r for r in self._au3_records if r.pair == selected]
                   if selected and selected != "ALL" else self._au3_records)
        self._au3_content_label.setText(
            self._build_paper_html(records, selected, system_label="AU3 Pullback"))
        self._populate_paper_table_generic(
            records, self._au3_trades_table,
            self._au3_table_title, "AU3 Pullback")
        self._au3_equity_chart.set_data(
            sorted(records, key=lambda r: r.entry_time),
            start_capital=1000.0, risk_pct=0.03)

    def _render_au4(self) -> None:
        selected = self._au4_pair_combo.currentText()
        records = ([r for r in self._au4_records if r.pair == selected]
                   if selected and selected != "ALL" else self._au4_records)
        self._au4_content_label.setText(
            self._build_paper_html(records, selected, system_label="AU4 USD-Div"))
        self._populate_paper_table_generic(
            records, self._au4_trades_table,
            self._au4_table_title, "AU4 USD-Div")
        self._au4_equity_chart.set_data(
            sorted(records, key=lambda r: r.entry_time),
            start_capital=1000.0, risk_pct=0.03)

    def _render_au5(self) -> None:
        selected = self._au5_pair_combo.currentText()
        records = ([r for r in self._au5_records if r.pair == selected]
                   if selected and selected != "ALL" else self._au5_records)
        self._au5_content_label.setText(
            self._build_paper_html(records, selected, system_label="AU5 MeanRev"))
        self._populate_paper_table_generic(
            records, self._au5_trades_table,
            self._au5_table_title, "AU5 MeanRev")
        self._au5_equity_chart.set_data(
            sorted(records, key=lambda r: r.entry_time),
            start_capital=1000.0, risk_pct=0.03)

    # ── Systems E/F/G: Breakout, Squeeze, Divergence ──────────────

    def _render_breakout(self) -> None:
        selected = self._breakout_pair_combo.currentText()
        records = [r for r in self._breakout_records if r.pair == selected] if selected and selected != "ALL" else self._breakout_records
        self._breakout_content_label.setText(
            self._build_paper_html(records, selected, system_label="Breakout"))
        self._populate_paper_table_generic(records, self._breakout_trades_table, self._breakout_table_title, "Breakout")
        self._breakout_equity_chart.set_data(sorted(records, key=lambda r: r.entry_time), start_capital=1000.0, risk_pct=0.03)

    def _render_squeeze(self) -> None:
        selected = self._squeeze_pair_combo.currentText()
        records = [r for r in self._squeeze_records if r.pair == selected] if selected and selected != "ALL" else self._squeeze_records
        self._squeeze_content_label.setText(
            self._build_paper_html(records, selected, system_label="Squeeze"))
        self._populate_paper_table_generic(records, self._squeeze_trades_table, self._squeeze_table_title, "Squeeze")
        self._squeeze_equity_chart.set_data(sorted(records, key=lambda r: r.entry_time), start_capital=1000.0, risk_pct=0.03)

    def _render_squeeze_rev(self) -> None:
        """Squeeze-REV renderer (2026-04-29). Inverse-direction mirror of Squeeze."""
        selected = self._squeeze_rev_pair_combo.currentText()
        records = ([r for r in self._squeeze_rev_records if r.pair == selected]
                   if selected and selected != "ALL" else self._squeeze_rev_records)
        self._squeeze_rev_content_label.setText(
            self._build_paper_html(records, selected, system_label="Squeeze-REV"))
        self._populate_paper_table_generic(
            records, self._squeeze_rev_trades_table,
            self._squeeze_rev_table_title, "Squeeze-REV")
        self._squeeze_rev_equity_chart.set_data(
            sorted(records, key=lambda r: r.entry_time),
            start_capital=1000.0, risk_pct=0.03,
        )

    def _render_divergence(self) -> None:
        selected = self._divergence_pair_combo.currentText()
        records = [r for r in self._divergence_records if r.pair == selected] if selected and selected != "ALL" else self._divergence_records
        self._divergence_content_label.setText(
            self._build_paper_html(records, selected, system_label="Divergence"))
        self._populate_paper_table_generic(records, self._divergence_trades_table, self._divergence_table_title, "Divergence")
        self._divergence_equity_chart.set_data(sorted(records, key=lambda r: r.entry_time), start_capital=1000.0, risk_pct=0.03)

    def _render_dtc_combo(self) -> None:
        selected = self._dtc_combo_pair_combo.currentText()
        records = (
            [r for r in self._dtc_combo_records if r.pair == selected]
            if selected and selected != "ALL"
            else list(self._dtc_combo_records)
        )

        # Apply Source filter (UI label → stored value mapping)
        _src_combo = getattr(self, "_dtc_combo_source_combo", None)
        _src_label = _src_combo.currentText() if _src_combo else "ALL"
        _src_label_to_key = {
            "SS":      "sv2_ss",
            "ATR":     "sv2_atr",
            "B-tuned": "sv2_b_tuned",
        }
        if _src_label and _src_label != "ALL":
            _src_key = _src_label_to_key.get(_src_label)
            if _src_key:
                records = [r for r in records
                           if (getattr(r, "dtc_source_system", "") or "") == _src_key]

        # HTML summary respects both filters BUT the per-source breakdown
        # inside it always shows all sources (built from _closed records of
        # whatever's passed in — so when Source!="ALL", breakdown narrows
        # naturally to that one feeder, which is fine: the user explicitly
        # asked to focus on it).
        self._dtc_combo_content_label.setText(
            self._build_paper_html(records, selected, system_label="DTC-combo"))
        self._populate_paper_table_generic(records, self._dtc_combo_trades_table,
                                            self._dtc_combo_table_title, "DTC-combo")
        # Equity chart: pass the unfiltered (by source) record set when source
        # is ALL so the per-source sub-curves can be drawn underneath. When a
        # specific source is selected, just plot that source's filtered curve.
        self._dtc_combo_equity_chart.set_data(
            sorted(records, key=lambda r: r.entry_time),
            start_capital=1000.0,
            risk_pct=0.03,
            split_by_attr="dtc_source_system" if _src_label == "ALL" else None,
        )

    def _build_system_html(
        self, records: list[PaperTradeRecord], pair_filter: str,
        title_prefix: str, empty_msg: str, card_bg: str,
    ) -> str:
        """Generic HTML summary builder for parallel systems."""
        p = [_CSS]
        title = f"{title_prefix}: {pair_filter}" if pair_filter != "ALL" else title_prefix
        p.append(f'<div class="header">{title}</div>')
        p.append(f'<div class="subtitle">{len(records)} completed trades</div>')
        if not records:
            p.append(f'<div class="empty">{empty_msg}</div>')
            return "".join(p)
        wins = [r for r in records if r.is_win]
        total_pnl = sum(r.pnl_pips for r in records)
        avg_pnl = total_pnl / len(records)
        p.append('<table style="border-collapse:collapse; margin:8px 0;"><tr>')
        for label, val in [
            ("Trades", str(len(records))),
            ("Win Rate", f"{len(wins)/len(records)*100:.1f}%"),
            ("Total P/L", f"{total_pnl:+.1f}p"),
            ("Avg P/L", f"{avg_pnl:+.1f}p"),
            ("TP Hits", str(sum(1 for r in records if r.close_reason == "tp_hit"))),
            ("SL Hits", str(sum(1 for r in records if r.close_reason == "sl_hit"))),
        ]:
            color = "#1b8a2a" if "+" in val else "#c62828" if "-" in val else "#2a5a8a"
            p.append(
                f'<td style="padding:6px 14px; background:{card_bg}; border-radius:4px; '
                f'margin:2px; text-align:center;">'
                f'{label}<br><span style="font-size:16px; font-weight:bold; color:{color};">{val}</span></td>'
            )
        p.append('</tr></table>')
        return "".join(p)

    # ── System D (QM4) tab helpers ──────────────────────────────────

    def _load_qm4_records(self) -> list[PaperTradeRecord]:
        if not self._qm4_trades_file or not self._qm4_trades_file.exists():
            return []
        try:
            import json as _json
            data = _json.loads(self._qm4_trades_file.read_text(encoding="utf-8"))
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

    def _update_qm4_pair_combo(self) -> None:
        pairs = sorted(set(r.pair for r in self._qm4_records))
        current = self._qm4_pair_combo.currentText()
        self._qm4_pair_combo.blockSignals(True)
        self._qm4_pair_combo.clear()
        self._qm4_pair_combo.addItem("ALL")
        self._qm4_pair_combo.addItems(pairs)
        if current in (["ALL"] + pairs):
            self._qm4_pair_combo.setCurrentText(current)
        self._qm4_pair_combo.blockSignals(False)

    def _render_qm4(self) -> None:
        selected = self._qm4_pair_combo.currentText()
        if selected and selected != "ALL":
            records = [r for r in self._qm4_records if r.pair == selected]
        else:
            records = self._qm4_records
        self._qm4_content_label.setText(
            self._build_paper_html(records, selected, system_label="QM4"))
        self._populate_qm4_table(records, self._qm4_trades_table, self._qm4_table_title)
        sorted_records = sorted(records, key=lambda r: r.entry_time)
        self._qm4_equity_chart.set_data(sorted_records, start_capital=1000.0, risk_pct=0.03)

    def _populate_qm4_table(
        self, records: list[PaperTradeRecord], table: QTableWidget, title: QLabel,
    ) -> None:
        """QM4 table — adds entry signal columns covering all 7 TFs QM4 evaluates."""
        title.setText(f"QM4 Trades ({len(records)})")
        cols = ["#", "Time", "Pair", "Dir", "TYPE", "Session", "Entry", "Close",
                "SL", "TP", "P/L", "Peak", "Worst", "PB", "Exit", "Dur",
                "Conv", "ADR%", "4H MFE", "4H MAE", "4H END",
                # ── Entry signal columns (21..) ──
                "M5B", "M5Q", "M15B", "M15Q", "H1B", "H1Q", "H4B", "H4Q",
                "D1B", "D1Q", "W1B", "W1Q", "MNB", "MNQ",
                "Align", "Spread", "H1ATR", "Struct",
                # ── QM4 extras (39..) ──
                "SLx", "TPx", "StrCcy", "WkCcy", "StrR", "WkR",
                "TopGap", "BotGap", "StrVel", "WkVel", "M5Slop", "NewsAge"]
        table.setSortingEnabled(False)
        table.setColumnCount(len(cols))
        table.setHorizontalHeaderLabels(cols)
        table.setRowCount(len(records))

        total = len(records)
        for row, r in enumerate(reversed(records)):
            num = total - row
            table.setItem(row, 0, _NumericItem(str(num), float(num)))
            table.setItem(row, 1, QTableWidgetItem(
                _to_jst_str(r.entry_time, "%m-%d %H:%M") if r.entry_time else "—"
            ))
            table.setItem(row, 2, QTableWidgetItem(r.pair))

            dir_item = QTableWidgetItem(r.direction)
            dir_item.setForeground(QColor("#1b8a2a" if r.direction == "BUY" else "#c62828"))
            table.setItem(row, 3, dir_item)

            # TYPE column — QM4 alert type
            atype = getattr(r, 'qm4_alert_type', '') or '—'
            type_item = QTableWidgetItem(atype)
            type_colors = {"MTF": "#4a90d9", "MTFC": "#4a90d9", "CUM": "#9c27b0"}
            tc = "#888"
            for k, v in type_colors.items():
                if k in atype:
                    tc = v
                    break
            if "PAIR" in atype:
                tc = "#ff6f00"
            type_item.setForeground(QColor(tc))
            type_item.setFont(QFont("Segoe UI", 9, QFont.Weight.Bold))
            table.setItem(row, 4, type_item)

            table.setItem(row, 5, QTableWidgetItem(r.session or "—"))
            table.setItem(row, 6, _NumericItem(f"{r.entry_price:.5f}", r.entry_price))

            _is_open_qm4 = r.close_time == 0 and not r.close_reason

            if _is_open_qm4:
                # Trade still open — show OPEN markers for exit-related cols
                _open_item = QTableWidgetItem("OPEN")
                _open_item.setForeground(QColor("#2196f3"))
                _open_item.setFont(QFont("Segoe UI", 9, QFont.Weight.Bold))
                table.setItem(row, 7, _open_item)  # Close
                table.setItem(row, 8, _NumericItem(f"{r.sl_pips:.1f}", r.sl_pips))
                table.setItem(row, 9, _NumericItem(f"{r.tp_pips:.1f}", r.tp_pips))
                table.setItem(row, 10, QTableWidgetItem("—"))   # P/L
                table.setItem(row, 11, QTableWidgetItem("—"))   # Peak
                table.setItem(row, 12, QTableWidgetItem("—"))   # Worst
                table.setItem(row, 13, QTableWidgetItem("—"))   # PB
                _open_exit = QTableWidgetItem("OPEN")
                _open_exit.setForeground(QColor("#2196f3"))
                _open_exit.setFont(QFont("Segoe UI", 9, QFont.Weight.Bold))
                table.setItem(row, 14, _open_exit)              # Exit
                table.setItem(row, 15, QTableWidgetItem("—"))   # Dur
            else:
                table.setItem(row, 7, _NumericItem(f"{r.close_price:.5f}", r.close_price))
                table.setItem(row, 8, _NumericItem(f"{r.sl_pips:.1f}", r.sl_pips))
                table.setItem(row, 9, _NumericItem(f"{r.tp_pips:.1f}", r.tp_pips))

                pnl_item = _NumericItem(f"{r.pnl_pips:+.1f}", r.pnl_pips)
                pnl_item.setForeground(QColor("#1b8a2a" if r.pnl_pips >= 0 else "#c62828"))
                pnl_item.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
                table.setItem(row, 10, pnl_item)

                table.setItem(row, 11, _NumericItem(f"{r.peak_pnl_pips:+.1f}", r.peak_pnl_pips))
                table.setItem(row, 12, _NumericItem(f"{r.worst_pnl_pips:+.1f}", r.worst_pnl_pips))

                # PB
                if r.close_reason == "tp_hit" and r.worst_pnl_pips < 0:
                    pb_pips = abs(r.worst_pnl_pips)
                    pb_pct = (pb_pips / r.sl_pips * 100) if r.sl_pips > 0 else 0
                    pb_item = _NumericItem(f"{pb_pips:.1f}p ({pb_pct:.0f}%)", pb_pips)
                    pb_item.setForeground(QColor("#e65100"))
                    table.setItem(row, 13, pb_item)
                else:
                    table.setItem(row, 13, QTableWidgetItem("—"))

                reason_labels = {"sl_hit": "SL", "tp_hit": "TP", "signal_exit": "Signal"}
                reason_colors = {"sl_hit": "#c62828", "tp_hit": "#1b8a2a", "signal_exit": "#e65100"}
                reason_item = QTableWidgetItem(reason_labels.get(r.close_reason, r.close_reason))
                reason_item.setForeground(QColor(reason_colors.get(r.close_reason, "#666")))
                table.setItem(row, 14, reason_item)

                table.setItem(row, 15, _NumericItem(f"{r.duration_minutes:.0f}m", r.duration_minutes))
            table.setItem(row, 16, _NumericItem(str(r.entry_conviction), float(r.entry_conviction)))

            adr_pct = getattr(r, 'adr_consumed_pct', 0.0)
            adr_item = _NumericItem(f"{adr_pct:.0f}%", adr_pct)
            if adr_pct >= 80:
                adr_item.setForeground(QColor("#c62828"))
            elif adr_pct >= 50:
                adr_item.setForeground(QColor("#e65100"))
            else:
                adr_item.setForeground(QColor("#2e7d32"))
            table.setItem(row, 17, adr_item)

            # 4H observation
            if r.post_close_complete:
                mfe4 = _NumericItem(f"{r.post_close_max_mfe_pips:+.1f}", r.post_close_max_mfe_pips)
                mfe4.setForeground(QColor("#1b8a2a"))
                table.setItem(row, 18, mfe4)
                mae4 = _NumericItem(f"{r.post_close_max_mae_pips:.1f}", r.post_close_max_mae_pips)
                mae4.setForeground(QColor("#c62828"))
                table.setItem(row, 19, mae4)
                end4 = _NumericItem(f"{r.post_close_final_pips:+.1f}", r.post_close_final_pips)
                end4.setForeground(QColor("#1b8a2a" if r.post_close_final_pips >= 0 else "#c62828"))
                table.setItem(row, 20, end4)
            else:
                watching_txt = "..." if r.close_time > 0 and not r.post_close_complete else "—"
                table.setItem(row, 18, QTableWidgetItem(watching_txt))
                table.setItem(row, 19, QTableWidgetItem(watching_txt))
                table.setItem(row, 20, QTableWidgetItem(watching_txt))

            # ── Entry signal columns: stoch scores across all 7 TFs ──
            _qm4_signal_fields = [
                (21, "entry_m5_base"),  (22, "entry_m5_quote"),
                (23, "entry_m15_base"), (24, "entry_m15_quote"),
                (25, "entry_h1_base"),  (26, "entry_h1_quote"),
                (27, "entry_h4_base"),  (28, "entry_h4_quote"),
                (29, "entry_d1_base"),  (30, "entry_d1_quote"),
                (31, "entry_w1_base"),  (32, "entry_w1_quote"),
                (33, "entry_mn_base"),  (34, "entry_mn_quote"),
            ]
            for col_idx, field in _qm4_signal_fields:
                val = getattr(r, field, 0.0)
                if val > 0:
                    item = _NumericItem(f"{val:.1f}", val)
                    if val >= 8.0:
                        item.setForeground(QColor("#1b8a2a"))
                    elif val <= 2.0:
                        item.setForeground(QColor("#c62828"))
                    else:
                        item.setForeground(QColor("#888888"))
                    table.setItem(row, col_idx, item)
                else:
                    table.setItem(row, col_idx, QTableWidgetItem("..."))

            # Align — # of 6 TFs (M15..MN) at extreme
            _align = getattr(r, "entry_alignment_count", 0)
            if _align > 0:
                _align_item = _NumericItem(f"{_align}/6", float(_align))
                _ac = "#1b8a2a" if _align >= 4 else "#e65100" if _align >= 2 else "#888"
                _align_item.setForeground(QColor(_ac))
                _align_item.setFont(QFont("Segoe UI", 9, QFont.Weight.Bold))
                table.setItem(row, 35, _align_item)
            else:
                table.setItem(row, 35, QTableWidgetItem("..."))

            # Spread — composite divergence
            _spread = getattr(r, "entry_div_spread", 0.0)
            if _spread != 0.0:
                _sp_item = _NumericItem(f"{_spread:+.1f}", _spread)
                _sp_item.setForeground(QColor("#1b8a2a" if abs(_spread) >= 12 else "#e65100" if abs(_spread) >= 8 else "#888"))
                table.setItem(row, 36, _sp_item)
            else:
                table.setItem(row, 36, QTableWidgetItem("..."))

            # H1 ATR
            _atr = getattr(r, "entry_h1_atr_pips", 0.0)
            if _atr > 0:
                table.setItem(row, 37, _NumericItem(f"{_atr:.1f}", _atr))
            else:
                table.setItem(row, 37, QTableWidgetItem("..."))

            # Struct (always "OK" for QM4 since it's a hard filter)
            _struct = getattr(r, "entry_structural", "")
            if _struct:
                _s_item = QTableWidgetItem(_struct[:25])
                _s_item.setForeground(QColor("#1b8a2a" if _struct == "OK" else "#c62828"))
                table.setItem(row, 38, _s_item)
            else:
                table.setItem(row, 38, QTableWidgetItem("..."))

            # ── QM4 extras (39..) ──
            def _qm4_set(col, val, fmt="{:.2f}", color=None, dots_when_zero=True):
                if dots_when_zero and val == 0:
                    table.setItem(row, col, QTableWidgetItem("..."))
                    return
                _it = _NumericItem(fmt.format(val), float(val))
                if color:
                    _it.setForeground(QColor(color))
                table.setItem(row, col, _it)

            _slx = getattr(r, "entry_sl_atr_mult", 0.0)
            _qm4_set(39, _slx, "{:.2f}", "#888")
            _tpx = getattr(r, "entry_tp_atr_mult", 0.0)
            _qm4_set(40, _tpx, "{:.2f}", "#888")
            table.setItem(row, 41, QTableWidgetItem(getattr(r, "entry_strong_ccy", "") or "..."))
            table.setItem(row, 42, QTableWidgetItem(getattr(r, "entry_weak_ccy", "") or "..."))
            _sr = getattr(r, "entry_strong_rank", 0)
            _qm4_set(43, _sr, "{:.0f}", "#1b8a2a" if _sr == 1 else "#e65100" if _sr == 2 else "#888")
            _wr = getattr(r, "entry_weak_rank", 0)
            _qm4_set(44, _wr, "{:.0f}", "#1b8a2a" if _wr == 8 else "#e65100" if _wr == 7 else "#888")
            _tg = getattr(r, "entry_strong_top_gap", 0.0)
            if _tg != 0.0:
                _c = "#1b8a2a" if _tg >= 2.0 else "#e65100" if _tg >= 1.0 else "#c62828"
                _qm4_set(45, _tg, "{:.1f}", _c, dots_when_zero=False)
            else:
                table.setItem(row, 45, QTableWidgetItem("..."))
            _bg = getattr(r, "entry_weak_bottom_gap", 0.0)
            if _bg != 0.0:
                _c = "#1b8a2a" if _bg >= 2.0 else "#e65100" if _bg >= 1.0 else "#c62828"
                _qm4_set(46, _bg, "{:.1f}", _c, dots_when_zero=False)
            else:
                table.setItem(row, 46, QTableWidgetItem("..."))
            _svel = getattr(r, "entry_strong_velocity", 0.0)
            if _svel != 0.0:
                _qm4_set(47, _svel, "{:+.2f}", "#1b8a2a" if abs(_svel) >= 0.6 else "#888", dots_when_zero=False)
            else:
                table.setItem(row, 47, QTableWidgetItem("..."))
            _wvel = getattr(r, "entry_weak_velocity", 0.0)
            if _wvel != 0.0:
                _qm4_set(48, _wvel, "{:+.2f}", "#1b8a2a" if abs(_wvel) >= 0.6 else "#888", dots_when_zero=False)
            else:
                table.setItem(row, 48, QTableWidgetItem("..."))
            _slope = getattr(r, "entry_m5_tr_slope_ratio", 0.0)
            if _slope > 0:
                _c = "#1b8a2a" if _slope >= 1.2 else "#e65100" if _slope >= 1.0 else "#c62828"
                _qm4_set(49, _slope, "{:.2f}", _c)
            else:
                table.setItem(row, 49, QTableWidgetItem("..."))
            _nm = getattr(r, "entry_minutes_since_news", -1.0)
            if _nm >= 0:
                if _nm < 60:
                    _txt = f"{_nm:.0f}m"
                elif _nm < 1440:
                    _txt = f"{_nm / 60:.1f}h"
                else:
                    _txt = f"{_nm / 1440:.1f}d"
                _c = "#c62828" if _nm < 30 else "#e65100" if _nm < 240 else "#888"
                _it = _NumericItem(_txt, float(_nm))
                _it.setForeground(QColor(_c))
                table.setItem(row, 50, _it)
            else:
                table.setItem(row, 50, QTableWidgetItem("..."))

        table.setSortingEnabled(True)
        header = table.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        for col in range(len(cols)):
            header.resizeSection(col, max(
                header.sectionSizeHint(col),
                table.sizeHintForColumn(col) + 12,
            ))

    def _clear_history(self) -> None:
        try:
            tab_idx = self._tabs.currentIndex()
            if not self._include_standard_tabs:
                # Live-only mode (LiveCandleDialog): 5 tabs only
                tab_config = {
                    0: ("Sv2-live",        self._sv2_live_trades_file,         self._sv2_live_paper_trader),
                    1: ("Sv2-Tun-live",    self._sv2_a_tuned_live_trades_file, self._sv2_a_tuned_live_paper_trader),
                    2: ("Sv2+SS-live",     self._sv2_ss_live_trades_file,      self._sv2_ss_live_paper_trader),
                    3: ("Sv2+SS-Tun-live", self._sv2_b_tuned_live_trades_file, self._sv2_b_tuned_live_paper_trader),
                    4: ("Sv2+ATR-live",    self._sv2_atr_live_trades_file,     self._sv2_atr_live_paper_trader),
                }
            else:
                # Tab order: DTC-combo | Live | Sv2 | Sv2-upgraded | Sv2-tuned | Sv2+SS | Sv2+SS-tuned | Sv2+ATR | QM4 | Breakout | Squeeze | Divergence | Backtest | CSI
                tab_config = {
                    0: ("DTC-combo",       self._dtc_combo_trades_file, self._dtc_combo_paper_trader),
                    1: ("live trade",      self._outcomes_file,       None),
                    2: ("Sv2 paper trade", self._paper_trades_file,   self._paper_trader),
                    3: ("Sv2-upgraded",    self._sv2_upgraded_trades_file, self._sv2_upgraded_paper_trader),
                    4: ("Sv2-tuned",       self._a_tuned_trades_file, self._a_tuned_paper_trader),
                    5: ("Sv2+SS",          self._ss_trades_file,      self._ss_paper_trader),
                    6: ("Sv2+SS-tuned",    self._b_tuned_trades_file, self._b_tuned_paper_trader),
                    7: ("Sv2+ATR",         self._atr_trades_file,     self._atr_paper_trader),
                    8: ("QM4",             self._qm4_trades_file,     self._qm4_paper_trader),
                    9: ("Breakout",        self._breakout_trades_file,  self._breakout_paper_trader),
                    10: ("Squeeze",        self._squeeze_trades_file,   self._squeeze_paper_trader),
                    # Squeeze-REV (2026-04-29) — inserted at idx 11; downstream tabs shift +1
                    11: ("Squeeze-REV",    self._squeeze_rev_trades_file, self._squeeze_rev_paper_trader),
                    12: ("Divergence",     self._divergence_trades_file, self._divergence_paper_trader),
                    13: ("backtest",       self._backtest_file,       None),
                    14: ("CSI alert",      self._csi_log_file,        None),
                }
            config = tab_config.get(tab_idx)
            if not config:
                return
            tab_name, target_file, paper_trader = config

            reply = QMessageBox.question(
                self, "Clear History",
                f"Delete all {tab_name} performance history? This cannot be undone.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            )
            if reply == QMessageBox.StandardButton.Yes:
                if target_file:
                    try:
                        target_file.unlink(missing_ok=True)
                    except OSError:
                        pass
                # Clear in-memory journal too (prevents re-save on next cycle)
                if paper_trader is not None and hasattr(paper_trader, 'clear_journal'):
                    paper_trader.clear_journal()
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
