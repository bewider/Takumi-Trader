"""Backtest Simulator Dialog — run backtests and view results."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path

import numpy as np

from takumi_trader.core.trade_tracker import pip_value

logger = logging.getLogger(__name__)

from PyQt6.QtCore import Qt, QDate, QThread, pyqtSignal
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDateEdit,
    QDoubleSpinBox,
    QHBoxLayout,
    QLabel,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from takumi_trader.core.alert_performance import AlertOutcome


# ── Worker threads ───────────────────────────────────────────────


class DownloadWorker(QThread):
    """Download Dukascopy M1 data on a background thread."""

    progress = pyqtSignal(str, int, int, int, int)  # pair, pair_idx, total_pairs, day, total_days
    finished = pyqtSignal(dict)  # {pair: count}
    error = pyqtSignal(str)

    def __init__(self, start: date, end: date, data_dir: Path,
                 single_pair: str | None = None) -> None:
        super().__init__()
        self.start_date = start
        self.end_date = end
        self.data_dir = data_dir
        self.single_pair = single_pair  # None = download all 28

    def run(self) -> None:
        try:
            from takumi_trader.core.dukascopy_downloader import DukascopyDownloader
            dl = DukascopyDownloader(self.data_dir)
            if self.single_pair:
                # Download only the selected pair
                def _pair_cb(pair: str, day: int, total_days: int) -> None:
                    self.progress.emit(pair, 1, 1, day, total_days)
                count = dl.download_pair(
                    self.single_pair, self.start_date, self.end_date,
                    progress_callback=_pair_cb, skip_existing=True,
                )
                self.finished.emit({self.single_pair: count})
            else:
                results = dl.download_all(
                    self.start_date, self.end_date,
                    progress_callback=self._on_progress,
                )
                self.finished.emit(results)
        except Exception as e:
            import traceback
            self.error.emit(f"{e}\n{traceback.format_exc()}")

    def _on_progress(self, pair: str, pair_idx: int, total_pairs: int,
                     day: int, total_days: int) -> None:
        self.progress.emit(pair, pair_idx, total_pairs, day, total_days)


class BacktestWorker(QThread):
    """Run the BacktestEngine on a background thread, including SL/TP optimization."""

    progress = pyqtSignal(int, int, int)  # current_bar, total_bars, trades_found
    status = pyqtSignal(str)              # status message for UI
    finished = pyqtSignal(dict)           # {"outcomes": [...], "all_combos": [...], "profile": {...}, "atr_combos": [...], "atr_profile": {...}}
    error = pyqtSignal(str)

    def __init__(self, config) -> None:
        super().__init__()
        self.config = config
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self) -> None:
        try:
            from takumi_trader.core.backtester import BacktestEngine
            import logging
            logger = logging.getLogger(__name__)

            engine = BacktestEngine(self.config, progress_callback=self._on_progress)
            engine._cancel_flag = self  # allow engine to check cancellation
            results = engine.run()
            if self._cancelled:
                self.error.emit("Cancelled by user")
                return

            n = len(results)
            logger.info("Backtest produced %d trades, serializing...", n)
            # Strip bar_running_mfe/mae from signal payload — they can be
            # very large and are only needed for the SL/TP optimization
            # which runs on this worker thread BEFORE the signal is emitted.
            outcome_dicts = []
            for o in results:
                d = asdict(o)
                d.pop("bar_running_mfe", None)
                d.pop("bar_running_mae", None)
                outcome_dicts.append(d)

            # Run SL/TP optimization on the worker thread (heavy computation)
            all_combos = []
            profile = {}
            atr_combos = []
            atr_profile = {}
            if n > 0:
                self.status.emit(f"Optimizing fixed-pip SL/TP across {n} trades...")
                logger.info("Starting fixed-pip SL/TP optimization for %d trades...", n)
                all_combos = _optimize_sl_tp(results)
                logger.info("Fixed-pip SL/TP optimization done: %d combos", len(all_combos))

                self.status.emit(f"Building pair x session profiles ({n} trades)...")
                logger.info("Building fixed-pip SL/TP profiles...")
                profile = _build_sltp_profile(results)
                logger.info("Fixed-pip profiles done: %d pairs", len(profile))

                # ATR-based optimization
                atr_count = sum(1 for r in results if r.entry_atr_pips > 0)
                if atr_count > 0:
                    self.status.emit(f"Optimizing ATR-based SL/TP across {atr_count} trades...")
                    logger.info("Starting ATR-based SL/TP optimization for %d trades...", atr_count)
                    atr_combos = _optimize_atr_sl_tp(results)
                    logger.info("ATR SL/TP optimization done: %d combos", len(atr_combos))

                    self.status.emit(f"Building ATR pair x session profiles ({atr_count} trades)...")
                    logger.info("Building ATR SL/TP profiles...")
                    atr_profile = _build_atr_sltp_profile(results)
                    logger.info("ATR profiles done: %d pairs", len(atr_profile))

            self.finished.emit({
                "outcomes": outcome_dicts,
                "all_combos": all_combos,
                "profile": profile,
                "atr_combos": atr_combos,
                "atr_profile": atr_profile,
            })
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            # Also write to crash log file for debugging
            try:
                crash_log = Path(__file__).resolve().parent.parent.parent / "data" / "backtest_crash.log"
                crash_log.parent.mkdir(parents=True, exist_ok=True)
                crash_log.write_text(f"{e}\n{tb}", encoding="utf-8")
            except Exception:
                pass
            self.error.emit(f"{e}\n{tb}")

    def _on_progress(self, current: int, total: int, trades: int) -> None:
        self.progress.emit(current, total, trades)


class OptimizerWorker(QThread):
    """Run deep parameter optimization on a background thread."""

    progress = pyqtSignal(int, int, str)  # current, total, status
    finished = pyqtSignal(dict)           # full_optimize result
    error = pyqtSignal(str)

    def __init__(self, config, pair: str) -> None:
        super().__init__()
        self.config = config
        self.pair = pair
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self) -> None:
        try:
            from takumi_trader.core.param_optimizer import full_optimize
            result = full_optimize(
                self.config, self.pair,
                fast_mode=True,
                progress_callback=self._on_progress,
            )
            self.finished.emit(result)
        except Exception as e:
            import traceback
            self.error.emit(f"{e}\n{traceback.format_exc()}")

    def _on_progress(self, current: int, total: int, status: str) -> None:
        self.progress.emit(current, total, status)


class OptimizeAllWorker(QThread):
    """Run optimization for ALL pairs sequentially on a background thread."""

    progress = pyqtSignal(int, int, str)  # pair_idx, total_pairs, status
    pair_done = pyqtSignal(str, dict)     # pair, result — emitted after each pair
    finished = pyqtSignal(dict)           # {"results": {pair: result}, "total_time": float}
    error = pyqtSignal(str)

    def __init__(self, config, pairs: list[str]) -> None:
        super().__init__()
        self.config = config
        self.pairs = pairs
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self) -> None:
        import time as _time
        try:
            from takumi_trader.core.param_optimizer import full_optimize

            t0 = _time.time()
            all_results: dict[str, dict] = {}
            n = len(self.pairs)

            for idx, pair in enumerate(self.pairs):
                if self._cancelled:
                    break

                elapsed = _time.time() - t0
                if idx > 0:
                    avg_per_pair = elapsed / idx
                    eta = avg_per_pair * (n - idx)
                    eta_str = f"ETA: {eta / 60:.0f}m" if eta > 120 else f"ETA: {eta:.0f}s"
                else:
                    eta_str = "calculating..."

                self.progress.emit(
                    idx, n,
                    f"Pair {idx + 1}/{n}: {pair} | {eta_str}"
                )

                def _inner_progress(cur, tot, msg, _pair=pair, _idx=idx, _n=n):
                    self.progress.emit(
                        _idx, _n,
                        f"Pair {_idx + 1}/{_n}: {_pair} | {msg}"
                    )

                try:
                    result = full_optimize(
                        self.config, pair,
                        fast_mode=True,
                        progress_callback=_inner_progress,
                    )
                    all_results[pair] = result
                    self.pair_done.emit(pair, result)
                except Exception as e:
                    logger.warning("Optimize failed for %s: %s", pair, e)
                    all_results[pair] = {"error": str(e), "calc_results": []}
                    self.pair_done.emit(pair, all_results[pair])

            total_time = _time.time() - t0
            self.finished.emit({
                "results": all_results,
                "total_time": total_time,
            })
        except Exception as e:
            import traceback
            self.error.emit(f"{e}\n{traceback.format_exc()}")


# ── HTML helpers ─────────────────────────────────────────────────

_CSS = """
<style>
body { font-family: Consolas, 'Courier New', monospace; font-size: 10pt; color: #222; margin: 8px; }
h2 { color: #2c3e50; margin: 18px 0 6px 0; font-size: 12pt; border-bottom: 2px solid #3498db; padding-bottom: 3px; }
h3 { color: #34495e; margin: 14px 0 4px 0; font-size: 10.5pt; }
.summary-box { background: #eaf2f8; border: 1px solid #aed6f1; border-radius: 6px; padding: 10px 14px; margin: 8px 0; }
.summary-box b { color: #2c3e50; }
.good { color: #27ae60; font-weight: bold; }
.bad { color: #c0392b; }
.neutral { color: #7f8c8d; }
table { border-collapse: collapse; margin: 4px 0 10px 0; font-size: 9.5pt; }
th { background: #34495e; color: white; padding: 4px 8px; text-align: right; font-weight: 600; }
th:first-child, th:nth-child(2), th:nth-child(3) { text-align: left; }
td { padding: 3px 8px; text-align: right; border-bottom: 1px solid #ddd; }
td:first-child, td:nth-child(2), td:nth-child(3) { text-align: left; }
tr:nth-child(even) { background: #f7f9fb; }
tr:hover { background: #e8f0fe; }
.profile-section { background: #fef9e7; border: 2px solid #f1c40f; border-radius: 6px; padding: 10px 14px; margin: 12px 0; }
.profile-section h2 { color: #d4ac0d; border-bottom-color: #f1c40f; }
.earnings-section { background: #e8f8f5; border: 2px solid #1abc9c; border-radius: 6px; padding: 10px 14px; margin: 12px 0; }
.earnings-section h2 { color: #16a085; border-bottom-color: #1abc9c; }
.big-number { font-size: 14pt; font-weight: bold; color: #27ae60; }
.r-total { font-size: 11pt; font-weight: bold; color: #27ae60; }
.rank { color: #7f8c8d; font-weight: bold; }
</style>
"""


def _pf_str(pf: float) -> str:
    return f"{pf:.1f}" if pf < 900 else "INF"


def _clr(val: float, fmt: str = "+.1f") -> str:
    """Colorize a number: green if positive, red if negative."""
    s = f"{val:{fmt}}"
    if val > 0:
        return f'<span class="good">{s}</span>'
    elif val < 0:
        return f'<span class="bad">{s}</span>'
    return f'<span class="neutral">{s}</span>'


def _exit_breakdown(c: dict) -> str:
    """Build TP/SL/Signal exit breakdown cells for HYBRID mode."""
    tp_hit = c.get("n_tp_hit", 0)
    sl_hit = c.get("n_sl_hit", 0)
    sig = c.get("n_signal", 0)
    return (
        f'<td class="good">{tp_hit}</td>'
        f'<td class="bad">{sl_hit}</td>'
        f'<td>{sig if sig > 0 else "-"}</td>'
    )


def _strict_cols(c: dict) -> str:
    """Build strict mode summary cells (WR, E[R], TP/SL counts)."""
    s_wr = c.get("strict_wr", 0)
    s_exp_r = c.get("strict_expectancy_r", 0)
    s_tp = c.get("strict_n_tp_hit", 0)
    s_sl = c.get("strict_n_sl_hit", 0)
    return (
        f'<td>{s_wr:.0f}%</td>'
        f'<td>{_clr(s_exp_r, "+.2f")}R</td>'
        f'<td class="good">{s_tp}</td>'
        f'<td class="bad">{s_sl}</td>'
    )


def _combo_row(c: dict, rank: int = 0) -> str:
    """Build one HTML <tr> for an SL/TP combo (hybrid + strict)."""
    sl = c["sl"]
    max_dd_r = c["max_dd"] / sl if sl > 0 else 0.0
    expect_r = c["expectancy"] / sl if sl > 0 else 0.0
    total_r = c["total_pnl"] / sl if sl > 0 else 0.0
    rank_td = f'<td class="rank">{rank}</td>' if rank else ""
    return (
        f"<tr>{rank_td}"
        f"<td>{sl:.1f}</td><td>{c['tp']:.1f}</td>"
        f"<td>{c['wr']:.0f}%</td>"
        f"<td>{_pf_str(c['pf'])}</td>"
        f"<td>{_clr(total_r, '+.1f')}R</td>"
        f"<td>{max_dd_r:.1f}R</td>"
        f"<td>{_clr(expect_r, '+.2f')}R</td>"
        f"<td>{c['rr']:.1f}</td>"
        f"<td>{c['max_consec_loss']}</td>"
        f"{_exit_breakdown(c)}"
        f"{_strict_cols(c)}"
        f"</tr>"
    )


def _combo_header(with_rank: bool = False) -> str:
    rank_th = '<th rowspan="2">#</th>' if with_rank else ""
    return (
        f"<tr>{rank_th}"
        '<th rowspan="2">SL</th><th rowspan="2">TP</th>'
        '<th colspan="10" style="background:#2c6e49;text-align:center;">── Hybrid (SL/TP + Signal Exit) ──</th>'
        '<th colspan="4" style="background:#7b3f00;text-align:center;">── Strict (Set &amp; Forget) ──</th>'
        "</tr>"
        "<tr>"
        "<th>WR%</th><th>PF</th>"
        "<th>TotalR</th><th>MaxDD</th><th>E[R]</th><th>R:R</th><th>ConsL</th>"
        "<th>TP hit</th><th>SL hit</th><th>Signal</th>"
        "<th>WR%</th><th>E[R]</th><th>TP</th><th>SL</th>"
        "</tr>"
    )


def _atr_combo_header(with_rank: bool = False) -> str:
    rank_th = '<th rowspan="2">#</th>' if with_rank else ""
    return (
        f"<tr>{rank_th}"
        '<th rowspan="2">SL (ATR)</th><th rowspan="2">TP (ATR)</th>'
        '<th rowspan="2">~SL pips</th><th rowspan="2">~TP pips</th>'
        '<th colspan="10" style="background:#2c6e49;text-align:center;">── Hybrid ──</th>'
        '<th colspan="4" style="background:#7b3f00;text-align:center;">── Strict ──</th>'
        "</tr>"
        "<tr>"
        "<th>WR%</th><th>PF</th><th>TotalR</th><th>MaxDD</th><th>E[R]</th>"
        "<th>R:R</th><th>ConsL</th>"
        "<th>TP hit</th><th>SL hit</th><th>Signal</th>"
        "<th>WR%</th><th>E[R]</th><th>TP</th><th>SL</th>"
        "</tr>"
    )


def _atr_combo_row(c: dict, rank: int = 0) -> str:
    """Build one HTML <tr> for an ATR-based SL/TP combo (hybrid + strict)."""
    total_r = c["total_r"]
    rank_td = f'<td class="rank">{rank}</td>' if rank else ""
    return (
        f"<tr>{rank_td}"
        f"<td>{c['sl_mult']:.1f}</td><td>{c['tp_mult']:.1f}</td>"
        f"<td>{c['sl_avg_pips']:.1f}</td><td>{c['tp_avg_pips']:.1f}</td>"
        f"<td>{c['wr']:.0f}%</td>"
        f"<td>{_pf_str(c['pf'])}</td>"
        f"<td>{_clr(total_r, '+.1f')}R</td>"
        f"<td>{c['max_dd_r']:.1f}R</td>"
        f"<td>{_clr(c['expectancy_r'], '+.2f')}R</td>"
        f"<td>{c['rr']:.1f}</td>"
        f"<td>{c['max_consec_loss']}</td>"
        f"{_exit_breakdown(c)}"
        f"{_strict_cols(c)}"
        f"</tr>"
    )


# ── SL/TP Optimization (module-level for worker thread) ──────────


def _simulate_sl_tp_for_trade(
    sl_pips: float, tp_pips: float,
    running_mfe: np.ndarray, running_mae: np.ndarray,
    signal_exit_pnl: float,
) -> tuple[float, str]:
    """Simulate SL/TP checking bar-by-bar for a single trade.

    Replays the running MFE/MAE history to find which was hit first:
    - If TP level is reached at bar N and SL was NOT yet reached → TP hit
    - If SL level is reached at bar N and TP was NOT yet reached → SL hit
    - If both reached at same bar → TP first (optimistic, matches paper trader)
    - If neither reached during Phase 1 → signal exit at spread-collapse P/L

    Returns (pnl_pips, exit_type) where exit_type is "tp_hit", "sl_hit", or "signal".
    """
    tp_bar = -1
    sl_bar = -1

    for bar_idx in range(len(running_mfe)):
        if tp_bar < 0 and running_mfe[bar_idx] >= tp_pips:
            tp_bar = bar_idx
        if sl_bar < 0 and running_mae[bar_idx] >= sl_pips:
            sl_bar = bar_idx
        # If both found, no need to continue
        if tp_bar >= 0 and sl_bar >= 0:
            break

    if tp_bar >= 0 and sl_bar >= 0:
        # Both triggered — check order (TP first on same bar = optimistic fill)
        if tp_bar <= sl_bar:
            return tp_pips, "tp_hit"
        else:
            return -sl_pips, "sl_hit"
    elif tp_bar >= 0:
        return tp_pips, "tp_hit"
    elif sl_bar >= 0:
        return -sl_pips, "sl_hit"
    else:
        return signal_exit_pnl, "signal"


def _precompute_arrays(outcomes: list) -> tuple:
    """Extract bar-by-bar histories + final arrays for fixed-pip SL/TP grid search."""
    n = len(outcomes)
    final = np.empty(n)
    bar_histories: list[tuple[np.ndarray, np.ndarray]] = []
    for i, o in enumerate(outcomes):
        final[i] = o.exit_signal_pnl_pips if o.exit_signal_pnl_pips != 0 else o.final_pnl_pips
        if o.bar_running_mfe and o.bar_running_mae:
            mfe_arr = np.array(o.bar_running_mfe, dtype=np.float64)
            mae_arr = np.array(o.bar_running_mae, dtype=np.float64)
        else:
            # Fallback for legacy data without bar history
            mfe_arr = np.array([o.mfe_pips], dtype=np.float64)
            mae_arr = np.array([o.mae_pips], dtype=np.float64)
        bar_histories.append((mfe_arr, mae_arr))
    return bar_histories, final


def _eval_sl_tp_fast(sl: float, tp: float, bar_histories, final) -> dict:
    """Evaluate a single SL/TP combo using bar-by-bar replay.

    Simulates real trading: checks SL/TP on each M1 bar in order.
    Computes both HYBRID (signal exit if neither SL/TP hit)
    and STRICT (set & forget — assume SL hit if neither triggered).
    """
    n = len(final)

    pnl = np.empty(n)
    exit_types = []
    for i in range(n):
        mfe_hist, mae_hist = bar_histories[i]
        trade_pnl, exit_type = _simulate_sl_tp_for_trade(
            sl, tp, mfe_hist, mae_hist, final[i],
        )
        pnl[i] = trade_pnl
        exit_types.append(exit_type)
    wins = int(np.sum(pnl > 0))
    losses = n - wins
    total_pnl = float(np.sum(pnl))
    wr = wins / n * 100 if n else 0

    # Exit category counts
    n_tp_hit = sum(1 for e in exit_types if e == "tp_hit")
    n_sl_hit = sum(1 for e in exit_types if e == "sl_hit")
    n_signal = sum(1 for e in exit_types if e == "signal")

    # Profit factor
    win_pnl = float(np.sum(pnl[pnl > 0])) if wins > 0 else 0.0
    loss_pnl = float(np.sum(np.abs(pnl[pnl < 0]))) if losses > 0 else 0.0
    pf = win_pnl / loss_pnl if loss_pnl > 0 else 999.0

    # Max drawdown + max consecutive losses
    equity = np.cumsum(pnl)
    peak = np.maximum.accumulate(equity)
    dd = peak - equity
    max_dd = float(np.max(dd)) if len(dd) > 0 else 0.0

    is_loss = pnl < 0
    max_consec = 0; cur = 0
    for v in is_loss:
        if v:
            cur += 1
            if cur > max_consec: max_consec = cur
        else:
            cur = 0

    # --- STRICT mode: SL/TP only, no signal exit ---
    pnl_strict = np.empty(n)
    strict_exit_types = []
    for i in range(n):
        mfe_hist, mae_hist = bar_histories[i]
        trade_pnl, exit_type = _simulate_sl_tp_for_trade(
            sl, tp, mfe_hist, mae_hist, -sl,  # If neither hit → assume SL
        )
        pnl_strict[i] = trade_pnl
        strict_exit_types.append(exit_type)

    strict_wins = int(np.sum(pnl_strict > 0))
    strict_total_pnl = float(np.sum(pnl_strict))
    strict_wr = strict_wins / n * 100 if n else 0
    strict_n_tp_hit = sum(1 for e in strict_exit_types if e == "tp_hit")
    strict_n_sl_hit = n - strict_n_tp_hit

    strict_win_pnl = float(np.sum(pnl_strict[pnl_strict > 0])) if strict_wins > 0 else 0.0
    strict_loss_pnl = float(np.sum(np.abs(pnl_strict[pnl_strict < 0]))) if (n - strict_wins) > 0 else 0.0
    strict_pf = strict_win_pnl / strict_loss_pnl if strict_loss_pnl > 0 else 999.0

    equity_s = np.cumsum(pnl_strict)
    peak_s = np.maximum.accumulate(equity_s)
    dd_s = peak_s - equity_s
    strict_max_dd = float(np.max(dd_s)) if len(dd_s) > 0 else 0.0

    is_loss_s = pnl_strict < 0
    strict_max_consec = 0; cur = 0
    for v in is_loss_s:
        if v:
            cur += 1
            if cur > strict_max_consec: strict_max_consec = cur
        else:
            cur = 0

    expectancy = total_pnl / n if n else 0
    strict_expectancy = strict_total_pnl / n if n else 0
    return {
        "sl": sl, "tp": tp, "wins": wins, "losses": losses, "trades": n,
        "wr": wr, "total_pnl": total_pnl, "avg_pnl": total_pnl / n if n else 0,
        "pf": pf, "max_dd": max_dd, "expectancy": expectancy,
        "expectancy_r": expectancy / sl if sl > 0 else 0.0,
        "max_dd_r": max_dd / sl if sl > 0 else 0.0,
        "rr": tp / sl if sl > 0 else 0,
        "max_consec_loss": max_consec,
        "n_tp_hit": n_tp_hit, "n_sl_hit": n_sl_hit, "n_signal": n_signal,
        # Strict mode
        "strict_wr": strict_wr, "strict_pf": strict_pf,
        "strict_total_pnl": strict_total_pnl,
        "strict_expectancy_r": strict_expectancy / sl if sl > 0 else 0.0,
        "strict_max_dd": strict_max_dd,
        "strict_max_dd_r": strict_max_dd / sl if sl > 0 else 0.0,
        "strict_max_consec_loss": strict_max_consec,
        "strict_n_tp_hit": strict_n_tp_hit, "strict_n_sl_hit": strict_n_sl_hit,
        "strict_total_r": strict_total_pnl / sl if sl > 0 else 0.0,
    }


def _optimize_sl_tp(outcomes: list, min_sl: float = 5.0) -> list[dict]:
    """Grid search over SL/TP combinations (coarse grid, numpy-accelerated)."""
    if not outcomes:
        return []
    arrays = _precompute_arrays(outcomes)
    sl_start = max(50, int(min_sl * 10))  # enforce minimum SL
    sl_range = [x / 10 for x in range(sl_start, 251, 10)]   # min_sl to 25.0 step 1.0
    tp_range = [x / 10 for x in range(30, 601, 10)]          # 3.0 to 60.0 step 1.0
    return [_eval_sl_tp_fast(sl, tp, *arrays) for sl in sl_range for tp in tp_range]


def _find_best_sltp(outcomes: list, min_wr: float = 65.0, min_sl: float = 5.0) -> dict | None:
    """Find best SL/TP for a group: WR >= min_wr, best expectancy in R."""
    n = len(outcomes)
    if n < 2:
        return None

    arrays = _precompute_arrays(outcomes)

    # Coarse grid: ~300 combos instead of 5000+
    sl_start = max(50, int(min_sl * 10))
    sl_range = [x / 10 for x in range(sl_start, 251, 10)]   # min_sl to 25.0 step 1.0
    tp_range = [x / 10 for x in range(20, 601, 10)]          # 2.0 to 60.0 step 1.0

    candidates = [_eval_sl_tp_fast(sl, tp, *arrays) for sl in sl_range for tp in tp_range]

    # Find best with descending WR thresholds — prioritize WR, then E[R]
    for threshold in [min_wr, 55.0, 0.0]:
        pool = [c for c in candidates if c["wr"] >= threshold and c["total_pnl"] > 0]
        if pool:
            pool.sort(key=lambda x: (x["wr"], x["expectancy_r"]), reverse=True)
            best_coarse = pool[0]
            # Fine-tune around the best coarse result
            sl_c, tp_c = best_coarse["sl"], best_coarse["tp"]
            fine_sl = [x / 10 for x in range(max(20, int(sl_c * 10) - 30), min(251, int(sl_c * 10) + 35), 5)]
            fine_tp = [x / 10 for x in range(max(20, int(tp_c * 10) - 30), min(601, int(tp_c * 10) + 35), 5)]
            fine_candidates = [_eval_sl_tp_fast(sl, tp, *arrays) for sl in fine_sl for tp in fine_tp]
            fine_pool = [c for c in fine_candidates if c["wr"] >= threshold and c["total_pnl"] > 0]
            if fine_pool:
                fine_pool.sort(key=lambda x: (x["wr"], x["expectancy_r"]), reverse=True)
                return fine_pool[0]
            return best_coarse

    candidates.sort(key=lambda x: (x["wr"], x["expectancy_r"]), reverse=True)
    return candidates[0] if candidates else None


def _build_sltp_profile(outcomes: list) -> dict:
    """Build optimal SL/TP profile for every pair+session combo."""
    pair_session: dict[tuple[str, str], list] = defaultdict(list)
    for o in outcomes:
        pair_session[(o.pair, o.session or "Unknown")].append(o)

    profile: dict[str, dict[str, dict]] = {}
    for (pair, session), group in pair_session.items():
        best = _find_best_sltp(group)
        if best:
            if pair not in profile:
                profile[pair] = {}
            profile[pair][session] = best
    return profile


# ── ATR-Based SL/TP Optimization (module-level for worker thread) ──


def _precompute_atr_arrays(outcomes: list) -> tuple | None:
    """Extract bar-by-bar MFE/MAE histories + final/ATR arrays for SL/TP simulation.

    Returns None if no outcomes have valid entry_atr_pips.
    The bar_histories list contains per-trade (running_mfe, running_mae) arrays
    for realistic bar-by-bar SL/TP hit-order determination.
    """
    # Filter to outcomes that have a valid ATR
    valid = [o for o in outcomes if o.entry_atr_pips > 0]
    if not valid:
        return None

    n = len(valid)
    final = np.empty(n)
    atr = np.empty(n)
    # Per-trade bar histories for bar-by-bar replay
    bar_histories: list[tuple[np.ndarray, np.ndarray]] = []

    for i, o in enumerate(valid):
        final[i] = o.exit_signal_pnl_pips if o.exit_signal_pnl_pips != 0 else o.final_pnl_pips
        atr[i] = o.entry_atr_pips

        # Use bar-by-bar history if available (Phase 1 only)
        if o.bar_running_mfe and o.bar_running_mae:
            mfe_arr = np.array(o.bar_running_mfe, dtype=np.float64)
            mae_arr = np.array(o.bar_running_mae, dtype=np.float64)
        else:
            # Fallback for legacy data without bar history:
            # single-element arrays with Phase 1 MFE/MAE
            mfe_arr = np.array([o.mfe_pips], dtype=np.float64)
            mae_arr = np.array([o.mae_pips], dtype=np.float64)
        bar_histories.append((mfe_arr, mae_arr))

    return bar_histories, final, atr


def _eval_atr_sl_tp_fast(
    sl_mult: float, tp_mult: float,
    bar_histories, final, atr,
    min_sl_pips: float = 5.0,
) -> dict | None:
    """Evaluate a single ATR-multiplier SL/TP combo using bar-by-bar replay.

    Simulates real trading: checks SL/TP on each M1 bar in order.
    If SL is hit before TP, trade closes at SL. If TP first, closes at TP.
    If neither hit during Phase 1, trade closes at spread-collapse signal exit P/L.
    """
    n = len(final)
    sl_pips_arr = sl_mult * atr
    tp_pips_arr = tp_mult * atr

    avg_sl = float(np.mean(sl_pips_arr))
    if avg_sl < min_sl_pips:
        return None

    avg_tp = float(np.mean(tp_pips_arr))

    # Bar-by-bar replay for each trade
    pnl = np.empty(n)
    exit_types = []  # "tp_hit", "sl_hit", "signal"

    for i in range(n):
        mfe_hist, mae_hist = bar_histories[i]
        trade_pnl, exit_type = _simulate_sl_tp_for_trade(
            sl_pips_arr[i], tp_pips_arr[i],
            mfe_hist, mae_hist,
            final[i],
        )
        pnl[i] = trade_pnl
        exit_types.append(exit_type)

    pnl_r = pnl / sl_pips_arr

    wins = int(np.sum(pnl > 0))
    losses = n - wins
    total_pnl = float(np.sum(pnl))
    total_r = float(np.sum(pnl_r))
    wr = wins / n * 100 if n else 0

    # Exit category counts
    n_tp_hit = sum(1 for e in exit_types if e == "tp_hit")
    n_sl_hit = sum(1 for e in exit_types if e == "sl_hit")
    n_signal = sum(1 for e in exit_types if e == "signal")

    # Profit factor in R terms
    win_r = float(np.sum(pnl_r[pnl_r > 0])) if wins > 0 else 0.0
    loss_r = float(np.sum(np.abs(pnl_r[pnl_r < 0]))) if losses > 0 else 0.0
    pf = win_r / loss_r if loss_r > 0 else 999.0

    # Max drawdown in R
    equity_r = np.cumsum(pnl_r)
    peak_r = np.maximum.accumulate(equity_r)
    dd_r = peak_r - equity_r
    max_dd_r = float(np.max(dd_r)) if len(dd_r) > 0 else 0.0

    # Consecutive losses
    is_loss = pnl < 0
    max_consec = 0
    cur = 0
    for v in is_loss:
        if v:
            cur += 1
            if cur > max_consec:
                max_consec = cur
        else:
            cur = 0

    # --- STRICT mode: SL/TP only, no signal exit ---
    # Re-simulate: if neither SL nor TP hit during Phase 1, assume trade
    # stays open until one of them is eventually hit (SL by default)
    pnl_strict = np.empty(n)
    strict_exit_types = []
    for i in range(n):
        mfe_hist, mae_hist = bar_histories[i]
        trade_pnl, exit_type = _simulate_sl_tp_for_trade(
            sl_pips_arr[i], tp_pips_arr[i],
            mfe_hist, mae_hist,
            -sl_pips_arr[i],  # If neither hit → assume SL eventually hit
        )
        pnl_strict[i] = trade_pnl
        strict_exit_types.append(exit_type)

    pnl_r_strict = pnl_strict / sl_pips_arr
    strict_wins = int(np.sum(pnl_strict > 0))
    strict_total_pnl = float(np.sum(pnl_strict))
    strict_total_r = float(np.sum(pnl_r_strict))
    strict_wr = strict_wins / n * 100 if n else 0
    strict_n_tp_hit = sum(1 for e in strict_exit_types if e == "tp_hit")
    strict_n_sl_hit = n - strict_n_tp_hit

    strict_win_r = float(np.sum(pnl_r_strict[pnl_r_strict > 0])) if strict_wins > 0 else 0.0
    strict_loss_r = float(np.sum(np.abs(pnl_r_strict[pnl_r_strict < 0]))) if (n - strict_wins) > 0 else 0.0
    strict_pf = strict_win_r / strict_loss_r if strict_loss_r > 0 else 999.0

    equity_r_s = np.cumsum(pnl_r_strict)
    peak_r_s = np.maximum.accumulate(equity_r_s)
    dd_r_s = peak_r_s - equity_r_s
    strict_max_dd_r = float(np.max(dd_r_s)) if len(dd_r_s) > 0 else 0.0

    is_loss_s = pnl_strict < 0
    strict_max_consec = 0
    cur = 0
    for v in is_loss_s:
        if v:
            cur += 1
            if cur > strict_max_consec:
                strict_max_consec = cur
        else:
            cur = 0

    expectancy_r = total_r / n if n else 0
    strict_expectancy_r = strict_total_r / n if n else 0
    rr = tp_mult / sl_mult if sl_mult > 0 else 0

    return {
        "sl_mult": sl_mult, "tp_mult": tp_mult,
        "sl_avg_pips": round(avg_sl, 1), "tp_avg_pips": round(avg_tp, 1),
        "wins": wins, "losses": losses, "trades": n,
        "wr": wr, "total_pnl": total_pnl, "total_r": total_r,
        "avg_pnl": total_pnl / n if n else 0,
        "pf": pf, "max_dd_r": max_dd_r, "expectancy_r": expectancy_r,
        "rr": rr, "max_consec_loss": max_consec,
        "n_tp_hit": n_tp_hit, "n_sl_hit": n_sl_hit, "n_signal": n_signal,
        # Keep sl/tp keys for compatibility with _project_strategy
        "sl": avg_sl, "tp": avg_tp,
        "max_dd": max_dd_r * avg_sl,  # approximate for compatibility
        # Strict mode
        "strict_wr": strict_wr, "strict_pf": strict_pf,
        "strict_total_pnl": strict_total_pnl, "strict_total_r": strict_total_r,
        "strict_expectancy_r": strict_expectancy_r,
        "strict_max_dd_r": strict_max_dd_r,
        "strict_max_consec_loss": strict_max_consec,
        "strict_n_tp_hit": strict_n_tp_hit, "strict_n_sl_hit": strict_n_sl_hit,
    }


def _optimize_atr_sl_tp(outcomes: list, min_sl_pips: float = 5.0) -> list[dict]:
    """Grid search over ATR-multiplier SL/TP combinations."""
    arrays = _precompute_atr_arrays(outcomes)
    if arrays is None:
        return []

    # SL range: 0.3 to 2.5 ATR (step 0.1)
    sl_range = [x / 10 for x in range(3, 26)]
    # TP range: 0.3 to 4.0 ATR (step 0.1) — no cap, optimizer picks best WR
    tp_range = [x / 10 for x in range(3, 41)]

    results = []
    for sl_m in sl_range:
        for tp_m in tp_range:
            c = _eval_atr_sl_tp_fast(sl_m, tp_m, *arrays, min_sl_pips=min_sl_pips)
            if c is not None:
                results.append(c)
    return results


def _find_best_atr_sltp(
    outcomes: list, min_wr: float = 65.0, min_sl_pips: float = 5.0,
) -> dict | None:
    """Find best ATR-based SL/TP for a group: WR >= min_wr, best expectancy in R."""
    arrays = _precompute_atr_arrays(outcomes)
    if arrays is None:
        return None
    n = len(arrays[0])  # bar_histories list length
    if n < 2:
        return None

    sl_range = [x / 10 for x in range(3, 26)]
    tp_range = [x / 10 for x in range(3, 41)]  # no cap — optimizer picks best WR

    candidates = []
    for sl_m in sl_range:
        for tp_m in tp_range:
            c = _eval_atr_sl_tp_fast(sl_m, tp_m, *arrays, min_sl_pips=min_sl_pips)
            if c is not None:
                candidates.append(c)

    if not candidates:
        return None

    for threshold in [min_wr, 55.0, 0.0]:
        pool = [c for c in candidates if c["wr"] >= threshold and c["total_pnl"] > 0]
        if pool:
            # Prioritize WR first, then E[R] as tiebreaker
            pool.sort(key=lambda x: (x["wr"], x["expectancy_r"]), reverse=True)
            return pool[0]

    candidates.sort(key=lambda x: (x["wr"], x["expectancy_r"]), reverse=True)
    return candidates[0] if candidates else None


def _build_atr_sltp_profile(outcomes: list) -> dict:
    """Build optimal ATR-based SL/TP profile for every pair+session combo."""
    pair_session: dict[tuple[str, str], list] = defaultdict(list)
    for o in outcomes:
        if o.entry_atr_pips > 0:
            pair_session[(o.pair, o.session or "Unknown")].append(o)

    profile: dict[str, dict[str, dict]] = {}
    for (pair, session), group in pair_session.items():
        best = _find_best_atr_sltp(group)
        if best:
            if pair not in profile:
                profile[pair] = {}
            profile[pair][session] = best
    return profile


# ── Dialog ───────────────────────────────────────────────────────


class BacktestDialog(QWidget):
    """Independent window for running backtests and viewing results."""

    _PERIOD_MAP = {
        "Custom date": 0,
        "3 days": 3,
        "7 days": 7,
        "14 days": 14,
        "30 days": 30,
        "60 days": 60,
        "90 days": 90,
    }

    def __init__(self) -> None:
        super().__init__(None)
        self._results: list[AlertOutcome] = []
        self._worker: BacktestWorker | None = None
        self._all_combos: list[dict] = []
        self._sltp_profile: dict = {}
        self._atr_combos: list[dict] = []
        self._atr_profile: dict = {}
        self._backtest_days: int = 7
        self._backtest_start: str = ""
        self._optimizer_worker: OptimizerWorker | None = None
        self._optimize_all_worker: OptimizeAllWorker | None = None
        self._last_optimize_result: dict | None = None  # stored for Save & Set
        self._optimize_all_results: dict[str, dict] = {}  # pair → result

        self.setWindowTitle("Backtest Simulator")
        self.setWindowFlags(
            Qt.WindowType.Window
            | Qt.WindowType.WindowCloseButtonHint
            | Qt.WindowType.WindowMinMaxButtonsHint
        )
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        self.setMinimumSize(700, 550)
        self.resize(780, 680)

        self._setup_ui()
        self._apply_style()

    def closeEvent(self, event) -> None:
        """Stop running workers before closing to prevent segfault."""
        for worker_attr in ("_worker", "_optimizer_worker", "_optimize_all_worker"):
            worker = getattr(self, worker_attr, None)
            if worker is not None:
                try:
                    worker.finished.disconnect()
                except Exception:
                    pass
                try:
                    worker.error.disconnect()
                except Exception:
                    pass
                worker.quit()
                worker.wait(3000)
                setattr(self, worker_attr, None)
        super().closeEvent(event)

    # ── UI construction ──────────────────────────────────────────

    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 12, 16, 12)
        root.setSpacing(8)

        # Title
        title = QLabel("Backtest Simulator")
        title.setFont(QFont("Segoe UI", 14, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        root.addWidget(title)

        # Period selector row
        period_row = QHBoxLayout()
        period_row.addWidget(QLabel("Period:"))
        self._period_combo = QComboBox()
        self._period_combo.addItems(list(self._PERIOD_MAP.keys()))
        self._period_combo.setCurrentText("30 days")
        self._period_combo.currentTextChanged.connect(self._on_period_changed)
        period_row.addWidget(self._period_combo)

        period_row.addSpacing(12)
        period_row.addWidget(QLabel("Start date:"))
        self._start_date = QDateEdit()
        self._start_date.setCalendarPopup(True)
        self._start_date.setDisplayFormat("yyyy-MM-dd")
        self._start_date.setDate(QDate(2025, 1, 3))
        self._start_date.setEnabled(False)  # only enabled for "Custom date"
        period_row.addWidget(self._start_date)

        period_row.addSpacing(12)

        period_row.addWidget(QLabel("Data:"))
        self._data_source = QComboBox()
        self._data_source.addItems(["MT5 (live)", "Dukascopy (local)"])
        self._data_source.setToolTip("MT5: live broker data (limited M1 history)\nDukascopy: downloaded local data (unlimited)")
        period_row.addWidget(self._data_source)

        self._download_btn = QPushButton("Download")
        self._download_btn.setToolTip("Download M1 history from Dukascopy (free)")
        self._download_btn.clicked.connect(self._download_dukascopy)
        period_row.addWidget(self._download_btn)

        self._update_all_btn = QPushButton("Update All")
        self._update_all_btn.setToolTip(
            "Update all 28 pairs — download only missing days since last update"
        )
        self._update_all_btn.clicked.connect(self._update_all_dukascopy)
        period_row.addWidget(self._update_all_btn)

        period_row.addStretch()
        root.addLayout(period_row)

        # Settings row
        settings_row = QHBoxLayout()

        settings_row.addWidget(QLabel("Exit spread:"))
        self._exit_spread_spin = QDoubleSpinBox()
        self._exit_spread_spin.setRange(1.0, 10.0)
        self._exit_spread_spin.setValue(4.0)
        self._exit_spread_spin.setSingleStep(0.5)
        self._exit_spread_spin.setDecimals(1)
        settings_row.addWidget(self._exit_spread_spin)

        settings_row.addSpacing(8)

        settings_row.addWidget(QLabel("Post-exit hrs:"))
        self._post_exit_spin = QDoubleSpinBox()
        self._post_exit_spin.setRange(1.0, 8.0)
        self._post_exit_spin.setValue(4.0)
        self._post_exit_spin.setSingleStep(0.5)
        self._post_exit_spin.setDecimals(1)
        settings_row.addWidget(self._post_exit_spin)

        settings_row.addSpacing(8)

        self._htf_check = QCheckBox("HTF exit (M5+M15+H1)")
        self._htf_check.setChecked(True)
        settings_row.addWidget(self._htf_check)

        settings_row.addStretch()
        root.addLayout(settings_row)

        # Filter toggles row
        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("Conviction Filters:"))
        filter_row.addSpacing(6)

        self._chk_htf = QCheckBox("HTF")
        self._chk_htf.setChecked(False)
        self._chk_htf.setToolTip("HTF Trend Regime filter (30 pts)")
        filter_row.addWidget(self._chk_htf)

        self._chk_vel = QCheckBox("VEL")
        self._chk_vel.setChecked(True)
        self._chk_vel.setToolTip("Strength Velocity filter (20 pts)")
        filter_row.addWidget(self._chk_vel)

        self._chk_isol = QCheckBox("ISOL")
        self._chk_isol.setChecked(True)
        self._chk_isol.setToolTip("Isolation Score filter (20 pts)")
        filter_row.addWidget(self._chk_isol)

        self._chk_adr = QCheckBox("STRUCT")
        self._chk_adr.setChecked(True)
        self._chk_adr.setToolTip("Structural filter: Key Level Proximity + TP Clearance (15 pts)")
        filter_row.addWidget(self._chk_adr)

        self._chk_news = QCheckBox("NEWS")
        self._chk_news.setChecked(False)
        self._chk_news.setToolTip("Block trades ±30min/+60min around RED news events")
        filter_row.addWidget(self._chk_news)

        self._chk_reentry = QCheckBox("Re-entry")
        self._chk_reentry.setChecked(True)
        self._chk_reentry.setToolTip(
            "Allow re-entry on same pair+direction within same session after trade closes.\n"
            "OFF = max 1 trade per pair per session (conservative)\n"
            "ON  = unlimited re-entries if previous trade closed (matches live)"
        )
        filter_row.addWidget(self._chk_reentry)

        self._chk_accel = QCheckBox("\u26a1 ACCEL")
        self._chk_accel.setChecked(True)
        self._chk_accel.setToolTip(
            "Enable acceleration-based early entries.\n"
            "Detects momentum shifts before full 4-TF alignment.\n"
            "Enters earlier with velocity + HTF agreement confirmation."
        )
        self._chk_accel.setStyleSheet("color: #ff9800; font-weight: bold;")
        filter_row.addWidget(self._chk_accel)

        filter_row.addSpacing(12)
        filter_row.addWidget(QLabel("Min conviction:"))

        self._conviction_spin = QSpinBox()
        self._conviction_spin.setRange(0, 100)
        self._conviction_spin.setValue(50)
        self._conviction_spin.setSuffix("%")
        self._conviction_spin.setSingleStep(5)
        filter_row.addWidget(self._conviction_spin)

        filter_row.addStretch()
        root.addLayout(filter_row)

        # Capital & risk row
        capital_row = QHBoxLayout()
        capital_row.addWidget(QLabel("Starting capital:"))
        self._capital_spin = QSpinBox()
        self._capital_spin.setRange(100, 10_000_000)
        self._capital_spin.setValue(1_000)
        self._capital_spin.setSingleStep(1000)
        self._capital_spin.setPrefix("$ ")
        self._capital_spin.setGroupSeparatorShown(True)
        capital_row.addWidget(self._capital_spin)

        capital_row.addSpacing(12)
        capital_row.addWidget(QLabel("Risk per trade:"))
        self._risk_spin = QDoubleSpinBox()
        self._risk_spin.setRange(0.1, 10.0)
        self._risk_spin.setValue(3.0)
        self._risk_spin.setSingleStep(0.1)
        self._risk_spin.setDecimals(1)
        self._risk_spin.setSuffix(" %")
        capital_row.addWidget(self._risk_spin)

        capital_row.addStretch()
        root.addLayout(capital_row)

        # Pair selector + optimize row
        pair_row = QHBoxLayout()
        pair_row.addWidget(QLabel("Pair:"))
        self._pair_combo = QComboBox()
        self._pair_combo.addItem("ALL PAIRS")
        from takumi_trader.core.strength import DISPLAY_PAIRS as _DP
        self._pair_combo.addItems(_DP)
        self._pair_combo.setToolTip("Select a single pair for focused analysis or optimization")
        pair_row.addWidget(self._pair_combo)

        pair_row.addSpacing(16)
        self._optimize_btn = QPushButton("\U0001f50d Optimize Params")
        self._optimize_btn.setToolTip(
            "Deep parameter optimization for selected pair.\n"
            "Tests 16 calculation parameter combinations (EMA, ROC decay,\n"
            "thresholds, acceleration filter). Takes ~4-5 minutes for 7-day data."
        )
        self._optimize_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._optimize_btn.clicked.connect(self._start_optimize)
        pair_row.addWidget(self._optimize_btn)

        pair_row.addSpacing(8)
        self._optimize_all_btn = QPushButton("\U0001f680 Optimize ALL Pairs")
        self._optimize_all_btn.setToolTip(
            "Run optimization for ALL 27 pairs sequentially.\n"
            "Auto-saves best settings for each pair.\n"
            "Takes ~30-50 minutes depending on data period."
        )
        self._optimize_all_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._optimize_all_btn.setStyleSheet(
            "QPushButton { background-color: #8e44ad; color: white; font-weight: bold; }"
            "QPushButton:hover { background-color: #9b59b6; }"
            "QPushButton:disabled { background-color: #aaa; }"
        )
        self._optimize_all_btn.clicked.connect(self._start_optimize_all)
        pair_row.addWidget(self._optimize_all_btn)

        pair_row.addStretch()
        root.addLayout(pair_row)

        # Run + Stop buttons row
        run_row = QHBoxLayout()
        self._run_btn = QPushButton("\u25b6  Run Backtest")
        self._run_btn.setFixedHeight(38)
        self._run_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._run_btn.clicked.connect(self._start_backtest)
        self._run_btn.setObjectName("run_btn")
        run_row.addWidget(self._run_btn)

        self._stop_btn = QPushButton("\u23f9  STOP")
        self._stop_btn.setFixedHeight(38)
        self._stop_btn.setFixedWidth(120)
        self._stop_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._stop_btn.setEnabled(False)
        self._stop_btn.clicked.connect(self._stop_all)
        self._stop_btn.setStyleSheet(
            "QPushButton { background: #c0392b; color: white; font-weight: bold; "
            "border-radius: 4px; font-size: 13px; }"
            "QPushButton:hover { background: #e74c3c; }"
            "QPushButton:disabled { background: #bdc3c7; color: #7f8c8d; }"
        )
        run_row.addWidget(self._stop_btn)
        root.addLayout(run_row)

        # Progress bar
        self._progress_bar = QProgressBar()
        self._progress_bar.setRange(0, 100)
        self._progress_bar.setValue(0)
        self._progress_bar.setTextVisible(True)
        root.addWidget(self._progress_bar)

        # Status label
        self._status_label = QLabel("Ready")
        self._status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        root.addWidget(self._status_label)

        # Results text (HTML)
        self._results_text = QTextEdit()
        self._results_text.setReadOnly(True)
        root.addWidget(self._results_text, stretch=1)

        # Button row
        btn_row = QHBoxLayout()
        btn_row.addStretch()

        self._save_set_btn = QPushButton("Save & Set Pair Algo")
        self._save_set_btn.setEnabled(False)
        self._save_set_btn.setToolTip(
            "Save the best optimized settings as the active algo for this pair"
        )
        self._save_set_btn.setStyleSheet(
            "QPushButton { background-color: #27ae60; }"
            "QPushButton:hover { background-color: #2ecc71; }"
            "QPushButton:disabled { background-color: #aaa; }"
        )
        self._save_set_btn.clicked.connect(self._save_and_set_pair_algo)
        btn_row.addWidget(self._save_set_btn)

        self._save_profile_btn = QPushButton("Save SL/TP Profile")
        self._save_profile_btn.setEnabled(False)
        self._save_profile_btn.setToolTip("Save optimal SL/TP per pair+session to JSON")
        self._save_profile_btn.clicked.connect(self._save_sltp_profile)
        btn_row.addWidget(self._save_profile_btn)

        self._view_perf_btn = QPushButton("View in Performance Dialog")
        self._view_perf_btn.setEnabled(False)
        self._view_perf_btn.clicked.connect(self._view_performance)
        btn_row.addWidget(self._view_perf_btn)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.close)
        btn_row.addWidget(close_btn)

        root.addLayout(btn_row)

    def _on_period_changed(self, text: str) -> None:
        self._start_date.setEnabled(text == "Custom date")

    def _download_dukascopy(self) -> None:
        """Start downloading M1 data from Dukascopy for the selected pair."""
        import sys
        if getattr(sys, 'frozen', False):
            data_dir = Path(sys.executable).parent / "data" / "dukascopy"
        else:
            data_dir = Path(__file__).resolve().parent.parent.parent / "data" / "dukascopy"

        qd_start = self._start_date.date()
        start = date(qd_start.year(), qd_start.month(), qd_start.day())
        end = date.today() - timedelta(days=1)  # up to yesterday

        # Download only the selected pair (or all if "ALL PAIRS")
        pair_text = self._pair_combo.currentText()
        single_pair = None if pair_text == "ALL PAIRS" else pair_text
        pair_label = single_pair or "all 28 pairs"

        self._download_btn.setEnabled(False)
        self._run_btn.setEnabled(False)
        self._status_label.setText(
            f"Downloading {pair_label}: {start} to {end} (skipping cached days)..."
        )
        self._progress_bar.setValue(0)

        self._dl_worker = DownloadWorker(start, end, data_dir, single_pair=single_pair)
        self._dl_worker.progress.connect(self._on_dl_progress)
        self._dl_worker.finished.connect(self._on_dl_finished)
        self._dl_worker.error.connect(self._on_dl_error)
        self._dl_worker.start()

    def _update_all_dukascopy(self) -> None:
        """Update all 28 pairs — download only missing days since last data."""
        import sys
        if getattr(sys, 'frozen', False):
            data_dir = Path(sys.executable).parent / "data" / "dukascopy"
        else:
            data_dir = Path(__file__).resolve().parent.parent.parent / "data" / "dukascopy"

        # Find the earliest "last date" across all pairs to use as start
        from takumi_trader.core.dukascopy_downloader import DukascopyDownloader
        dl = DukascopyDownloader(data_dir)

        # Check each pair's latest available date
        from takumi_trader.core.strength import DISPLAY_PAIRS
        latest_dates: dict[str, date | None] = {}
        has_any = False
        for pair in DISPLAY_PAIRS:
            existing = dl.get_existing_dates(pair)
            if existing:
                latest_dates[pair] = max(existing)
                has_any = True
            else:
                latest_dates[pair] = None

        if not has_any:
            # No data at all — tell user to use Download first
            self._status_label.setText(
                "No existing data found. Use 'Download' with a start date first."
            )
            return

        # Start from the earliest "last date" across pairs that have data
        # For pairs with no data, start from 90 days ago
        fallback_start = date.today() - timedelta(days=90)
        start = min(
            d if d is not None else fallback_start
            for d in latest_dates.values()
        )
        end = date.today() - timedelta(days=1)

        if start >= end:
            self._status_label.setText("All data is already up to date.")
            return

        gap_days = (end - start).days
        self._download_btn.setEnabled(False)
        self._update_all_btn.setEnabled(False)
        self._run_btn.setEnabled(False)
        self._status_label.setText(
            f"Updating all pairs: filling {gap_days} day gap ({start} to {end})..."
        )
        self._progress_bar.setValue(0)

        self._dl_worker = DownloadWorker(start, end, data_dir, single_pair=None)
        self._dl_worker.progress.connect(self._on_dl_progress)
        self._dl_worker.finished.connect(self._on_update_all_finished)
        self._dl_worker.error.connect(self._on_dl_error)
        self._dl_worker.start()

    def _on_update_all_finished(self, results: dict) -> None:
        self._dl_worker = None
        self._download_btn.setEnabled(True)
        self._update_all_btn.setEnabled(True)
        self._run_btn.setEnabled(True)
        self._progress_bar.setValue(100)

        total = sum(results.values())
        updated = sum(1 for v in results.values() if v > 0)
        if total == 0:
            self._status_label.setText("All data is already up to date.")
        else:
            self._status_label.setText(
                f"Update complete — {total:,} new candles for {updated} pairs."
            )
        self._data_source.setCurrentIndex(1)

    def _on_dl_progress(self, pair: str, pair_idx: int, total_pairs: int,
                        day: int, total_days: int) -> None:
        try:
            denom = max(1, total_pairs * total_days)
            overall = (max(0, pair_idx - 1) * total_days + day) / denom * 100
            self._progress_bar.setValue(min(99, int(overall)))
            self._status_label.setText(
                f"Downloading {pair} ({pair_idx}/{total_pairs}) — day {day}/{total_days}"
            )
        except Exception:
            pass

    def _on_dl_finished(self, results: dict) -> None:
        self._dl_worker = None
        self._download_btn.setEnabled(True)
        self._run_btn.setEnabled(True)
        self._progress_bar.setValue(100)

        total = sum(results.values())
        pairs_with_data = sum(1 for v in results.values() if v > 0)
        self._status_label.setText(
            f"Download complete — {total:,} candles for {pairs_with_data} pairs. "
            f"Select 'Dukascopy (local)' as data source to use."
        )
        # Auto-select Dukascopy
        self._data_source.setCurrentIndex(1)

    def _on_dl_error(self, msg: str) -> None:
        self._dl_worker = None
        self._download_btn.setEnabled(True)
        self._update_all_btn.setEnabled(True)
        self._run_btn.setEnabled(True)
        self._progress_bar.setValue(0)
        self._status_label.setText(f"Download error: {msg[:100]}")

    # ── Stylesheet ───────────────────────────────────────────────

    def _apply_style(self) -> None:
        self.setStyleSheet(
            """
            QWidget {
                background: #f5f5f5;
                color: #222222;
                font-family: "Segoe UI", sans-serif;
                font-size: 10pt;
            }
            QLabel {
                color: #222222;
                background: transparent;
            }
            QCheckBox {
                color: #222222;
                background: transparent;
            }
            QPushButton {
                background: #4a6fa5;
                color: white;
                border: none;
                border-radius: 4px;
                padding: 6px 16px;
            }
            QPushButton:hover {
                background: #3d5f8f;
            }
            QPushButton:disabled {
                background: #a0a0a0;
                color: #dddddd;
            }
            QPushButton#run_btn {
                font-size: 12pt;
                font-weight: bold;
            }
            QProgressBar {
                border: 1px solid #ccc;
                border-radius: 4px;
                text-align: center;
                height: 20px;
                color: #333333;
            }
            QProgressBar::chunk {
                background: #47b86b;
                border-radius: 3px;
            }
            QTextEdit {
                background: #ffffff;
                color: #222222;
                border: 1px solid #ccc;
                border-radius: 4px;
            }
            QComboBox {
                background: white;
                color: #222222;
                border: 1px solid #ccc;
                border-radius: 3px;
                padding: 3px 6px;
            }
            QComboBox QAbstractItemView {
                background: white;
                color: #222222;
            }
            QDoubleSpinBox, QSpinBox {
                background: white;
                color: #222222;
                border: 1px solid #ccc;
                border-radius: 3px;
                padding: 3px 6px;
                min-height: 22px;
            }
            QDateEdit {
                background: white;
                color: #222222;
                border: 1px solid #ccc;
                border-radius: 3px;
                padding: 3px 6px;
                min-height: 22px;
            }
            QDoubleSpinBox::up-button, QSpinBox::up-button, QDateEdit::up-button {
                subcontrol-origin: border;
                subcontrol-position: top right;
                width: 18px;
                border-left: 1px solid #ccc;
                border-bottom: 1px solid #ccc;
                border-top-right-radius: 3px;
                background: #e8e8e8;
            }
            QDoubleSpinBox::down-button, QSpinBox::down-button, QDateEdit::down-button {
                subcontrol-origin: border;
                subcontrol-position: bottom right;
                width: 18px;
                border-left: 1px solid #ccc;
                border-bottom-right-radius: 3px;
                background: #e8e8e8;
            }
            QDoubleSpinBox::up-button:hover, QSpinBox::up-button:hover, QDateEdit::up-button:hover,
            QDoubleSpinBox::down-button:hover, QSpinBox::down-button:hover, QDateEdit::down-button:hover {
                background: #d0d0d0;
            }
            QDoubleSpinBox::up-button:pressed, QSpinBox::up-button:pressed, QDateEdit::up-button:pressed,
            QDoubleSpinBox::down-button:pressed, QSpinBox::down-button:pressed, QDateEdit::down-button:pressed {
                background: #b8b8b8;
            }
            QDoubleSpinBox::up-arrow, QSpinBox::up-arrow, QDateEdit::up-arrow {
                image: none;
                width: 0; height: 0;
                border-left: 4px solid transparent;
                border-right: 4px solid transparent;
                border-bottom: 5px solid #444;
            }
            QDoubleSpinBox::down-arrow, QSpinBox::down-arrow, QDateEdit::down-arrow {
                image: none;
                width: 0; height: 0;
                border-left: 4px solid transparent;
                border-right: 4px solid transparent;
                border-top: 5px solid #444;
            }
            """
        )

    # ── Run backtest ─────────────────────────────────────────────

    def _start_backtest(self) -> None:
        from takumi_trader.core.backtester import BacktestConfig

        period_text = self._period_combo.currentText()
        if period_text == "Custom date":
            qd = self._start_date.date()
            start_date_str = qd.toString("yyyy-MM-dd")
            # Calculate days_back as fallback for summary label
            days = qd.daysTo(QDate.currentDate())
        else:
            start_date_str = ""
            days = self._PERIOD_MAP[period_text]

        # Single pair mode
        pair_text = self._pair_combo.currentText()
        single_pair = None if pair_text == "ALL PAIRS" else pair_text

        config = BacktestConfig(
            days_back=days,
            start_date=start_date_str,
            use_dukascopy=self._data_source.currentIndex() == 1,
            exit_spread_threshold=self._exit_spread_spin.value(),
            post_exit_hours=self._post_exit_spin.value(),
            use_htf_exit=self._htf_check.isChecked(),
            filter_htf=self._chk_htf.isChecked(),
            filter_vel=self._chk_vel.isChecked(),
            filter_isol=self._chk_isol.isChecked(),
            filter_structural=self._chk_adr.isChecked(),
            filter_news=self._chk_news.isChecked(),
            allow_session_reentry=self._chk_reentry.isChecked(),
            use_accel_entry=self._chk_accel.isChecked(),
            conviction_threshold=self._conviction_spin.value(),
            single_pair=single_pair,
        )

        self._run_btn.setEnabled(False)
        self._optimize_btn.setEnabled(False)
        self._optimize_all_btn.setEnabled(False)
        self._view_perf_btn.setEnabled(False)
        self._save_profile_btn.setEnabled(False)
        self._results_text.clear()
        self._progress_bar.setValue(0)
        self._status_label.setText("Running...")
        self._results = []
        self._backtest_days = days
        self._backtest_start = start_date_str

        self._worker = BacktestWorker(config)
        self._worker.progress.connect(self._on_progress)
        self._worker.status.connect(self._on_status)
        self._worker.finished.connect(self._on_finished)
        self._worker.error.connect(self._on_error)
        self._worker.start()
        self._enable_stop_btn()

    def _on_progress(self, current: int, total: int, trades: int) -> None:
        pct = int(current / total * 100) if total > 0 else 0
        self._progress_bar.setValue(pct)
        self._status_label.setText(
            f"Bar {current:,}/{total:,}  |  {trades} trades found"
        )

    def _on_status(self, msg: str) -> None:
        self._status_label.setText(msg)

    def _on_finished(self, payload: dict) -> None:
        try:
            result_dicts = payload["outcomes"]
            all_combos = payload.get("all_combos", [])
            profile = payload.get("profile", {})
            atr_combos = payload.get("atr_combos", [])
            atr_profile = payload.get("atr_profile", {})

            # Reconstruct AlertOutcome from dicts
            outcomes = []
            known = {f.name for f in AlertOutcome.__dataclass_fields__.values()}
            for d in result_dicts:
                filtered = {k: v for k, v in d.items() if k in known}
                outcomes.append(AlertOutcome(**filtered))

            self._results = outcomes
            self._all_combos = all_combos
            self._sltp_profile = profile
            self._atr_combos = atr_combos
            self._atr_profile = atr_profile
            self._worker = None
            self._reset_buttons()
            self._progress_bar.setValue(100)

            n = len(outcomes)
            self._status_label.setText(f"Building report for {n} trades...")

            html = self._build_html(outcomes, all_combos, profile, atr_combos, atr_profile)

            # Write crash-safe: save html to file first, then render
            html_file = Path(__file__).resolve().parent.parent.parent / "data" / "last_backtest.html"
            try:
                html_file.parent.mkdir(parents=True, exist_ok=True)
                html_file.write_text(html, encoding="utf-8")
            except Exception:
                pass

            # Auto-save to per-pair results folder
            filters_on = []
            if self._chk_htf.isChecked(): filters_on.append("HTF")
            if self._chk_vel.isChecked(): filters_on.append("VEL")
            if self._chk_isol.isChecked(): filters_on.append("ISOL")
            if self._chk_adr.isChecked(): filters_on.append("STRUCT")
            if self._chk_news.isChecked(): filters_on.append("NEWS")
            filter_str = "+".join(filters_on) if filters_on else "NOFILTER"
            wins = sum(1 for o in outcomes if o.final_pnl_pips > 0)
            wr = int(wins / n * 100) if n else 0
            details = f"{self._backtest_days}d_{filter_str}_{n}trades_{wr}WR"
            self._save_results_to_folder(html, "backtest", details)

            # Render HTML — use setHtml (more robust than insertHtml for large docs)
            self._results_text.clear()
            self._results_text.document().setMaximumBlockCount(0)
            html_kb = len(html) // 1024
            if html_kb > 2048:
                # Very large HTML (>2MB) — truncate to prevent Qt crash
                self._results_text.setPlainText(
                    f"Report too large for inline display ({html_kb} KB).\n"
                    f"Saved to: data/last_backtest.html\n\n"
                    f"Open that file in a browser to view the full report."
                )
            else:
                self._results_text.setHtml(html)
            self._results_text.moveCursor(
                self._results_text.textCursor().MoveOperation.Start
            )

            self._status_label.setText(f"Complete \u2014 {n} trades found")
            self._view_perf_btn.setEnabled(n > 0)
            self._save_profile_btn.setEnabled(n > 0)
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            try:
                crash_log = Path(__file__).resolve().parent.parent.parent / "data" / "backtest_crash.log"
                crash_log.parent.mkdir(parents=True, exist_ok=True)
                crash_log.write_text(f"_on_finished crash:\n{e}\n{tb}", encoding="utf-8")
            except Exception:
                pass
            self._on_error(f"{e}\n{tb}")

    def _save_results_to_folder(self, html: str, result_type: str, details: str,
                                pair_override: str | None = None) -> None:
        """Save HTML report to data/results/<pair>/ folder.

        Args:
            html: The HTML content to save.
            result_type: 'backtest' or 'optimize'.
            details: Summary string for filename (e.g. '7d_VEL+ISOL_355trades_61WR').
            pair_override: If set, use this pair name instead of the combo box.
        """
        try:
            from datetime import date as _date
            import sys

            if getattr(sys, 'frozen', False):
                base = Path(sys.executable).parent / "data" / "results"
            else:
                base = Path(__file__).resolve().parent.parent.parent / "data" / "results"

            # Determine pair folder
            pair_text = pair_override or self._pair_combo.currentText()
            pair_folder = pair_text if pair_text != "ALL PAIRS" else "_ALL"
            out_dir = base / pair_folder
            out_dir.mkdir(parents=True, exist_ok=True)

            # Clean details for filename
            safe_details = details.replace(" ", "_").replace("+", "+").replace("%", "pct")
            safe_details = "".join(c for c in safe_details if c.isalnum() or c in "_-+.")
            today = _date.today().strftime("%Y-%m-%d")
            filename = f"{today}_{result_type}_{safe_details}.html"

            out_file = out_dir / filename
            out_file.write_text(html, encoding="utf-8")
            logger.info("Results saved to %s", out_file)
        except Exception as e:
            logger.warning("Failed to save results to folder: %s", e)

    def _save_and_set_pair_algo(self) -> None:
        """Save the best optimization result as the active algo settings for this pair."""
        from PyQt6.QtWidgets import QMessageBox
        from takumi_trader.core.pair_algo_settings import save_pair_settings

        result = self._last_optimize_result
        if not result or not result.get("calc_results"):
            QMessageBox.warning(self, "No Results", "No optimization results to save.")
            return

        pair = self._pair_combo.currentText()
        if pair == "ALL PAIRS":
            QMessageBox.warning(self, "Select Pair", "Select a specific pair first.")
            return

        best = result["calc_results"][0]

        # Build backtest period string
        period_text = self._period_combo.currentText()
        if period_text == "Custom date":
            qd = self._start_date.date()
            bt_period = f"{qd.toString('yyyy-MM-dd')} → today ({self._backtest_days}d)"
        else:
            bt_period = f"{period_text} ({self._backtest_days}d)"

        # Include SL/TP if the best result uses ATR-based exits
        sltp_info = {}
        if best.sl_atr > 0:
            sltp_info = {
                "sl_atr": best.sl_atr,
                "tp_atr": best.tp_atr,
                "sl_pips": best.sl_pips,
                "tp_pips": best.tp_pips,
            }

        save_pair_settings(
            pair=pair,
            calc_params=best.calc_params,
            stats={
                "trades": best.trades,
                "wr": best.wr,
                "exp_r": best.exp_r,
                "avg_final": best.avg_final,
                "total_r": best.total_r,
                "avg_mfe": best.avg_mfe,
                "avg_mae": best.avg_mae,
            },
            sltp=sltp_info,
            source="optimizer",
            backtest_period=bt_period,
        )

        # Build save confirmation message
        sl_tp_msg = ""
        if best.sl_atr > 0:
            rr = best.tp_atr / best.sl_atr
            sl_tp_msg = (
                f"\nSL: {best.sl_atr}×ATR ({best.sl_pips:.0f} pips) | "
                f"TP: {best.tp_atr}×ATR ({best.tp_pips:.0f} pips) | RR 1:{rr:.1f}"
            )

        QMessageBox.information(
            self,
            "Settings Saved",
            f"Pair algo settings saved for {pair}.\n\n"
            f"Entry: {best.label}\n"
            f"{sl_tp_msg}\n"
            f"Performance: {best.trades} trades, WR {best.wr:.0f}%, "
            f"E[R]: {best.exp_r:+.2f}R/trade, Total: {best.total_r:+.1f}R\n\n"
            f"View in Settings → Pair Algo → {pair}",
        )

    def _stop_all(self) -> None:
        """Cancel all running workers."""
        stopped = False
        if self._worker:
            self._worker.cancel()
            stopped = True
        if self._optimizer_worker:
            self._optimizer_worker.cancel()
            stopped = True
        if self._optimize_all_worker:
            self._optimize_all_worker.cancel()
            stopped = True
        if hasattr(self, '_dl_worker') and self._dl_worker:
            self._dl_worker.terminate()
            self._dl_worker = None
            stopped = True
        if stopped:
            self._status_label.setText("Stopping... please wait")
            self._stop_btn.setEnabled(False)

    def _enable_stop_btn(self) -> None:
        """Enable stop button when a worker starts."""
        self._stop_btn.setEnabled(True)

    def _reset_buttons(self) -> None:
        """Reset all buttons to ready state."""
        self._run_btn.setEnabled(True)
        self._optimize_btn.setEnabled(True)
        self._optimize_all_btn.setEnabled(True)
        self._stop_btn.setEnabled(False)

    def _on_error(self, msg: str) -> None:
        self._worker = None
        self._optimizer_worker = None
        self._optimize_all_worker = None
        self._reset_buttons()
        self._progress_bar.setValue(0)
        if "Cancelled" in msg:
            self._status_label.setText("Stopped by user")
        else:
            self._status_label.setText(f"Error")
            self._results_text.setPlainText(f"Failed:\n{msg}")

    # ── Parameter Optimizer ──────────────────────────────────────

    def _start_optimize(self) -> None:
        """Start deep parameter optimization for selected pair."""
        from takumi_trader.core.backtester import BacktestConfig

        pair_text = self._pair_combo.currentText()
        if pair_text == "ALL PAIRS":
            self._status_label.setText("Select a specific pair for optimization")
            return

        period_text = self._period_combo.currentText()
        if period_text == "Custom date":
            qd = self._start_date.date()
            start_date_str = qd.toString("yyyy-MM-dd")
            days = qd.daysTo(QDate.currentDate())
        else:
            start_date_str = ""
            days = self._PERIOD_MAP[period_text]

        config = BacktestConfig(
            days_back=days,
            start_date=start_date_str,
            use_dukascopy=self._data_source.currentIndex() == 1,
            exit_spread_threshold=self._exit_spread_spin.value(),
            post_exit_hours=self._post_exit_spin.value(),
            use_htf_exit=self._htf_check.isChecked(),
            allow_session_reentry=self._chk_reentry.isChecked(),
        )

        self._run_btn.setEnabled(False)
        self._optimize_btn.setEnabled(False)
        self._view_perf_btn.setEnabled(False)
        self._save_profile_btn.setEnabled(False)
        self._results_text.clear()
        self._progress_bar.setValue(0)
        self._status_label.setText(f"Optimizing {pair_text}...")

        self._optimizer_worker = OptimizerWorker(config, pair_text)
        self._optimizer_worker.progress.connect(self._on_opt_progress)
        self._optimizer_worker.finished.connect(self._on_optimize_finished)
        self._optimizer_worker.error.connect(self._on_error)
        self._optimizer_worker.start()
        self._enable_stop_btn()

    def _on_opt_progress(self, current: int, total: int, status: str) -> None:
        if total > 0:
            pct = int(current / total * 100)
            self._progress_bar.setValue(pct)
        self._status_label.setText(status[:100])  # truncate long messages

    def _on_optimize_finished(self, result: dict) -> None:
        try:
            self._optimizer_worker = None
            self._reset_buttons()
            self._progress_bar.setValue(100)

            # Store for "Save & Set" button
            self._last_optimize_result = result
            self._save_set_btn.setEnabled(bool(result.get("calc_results")))

            calc_results = result.get("calc_results", [])
            filter_results = result.get("filter_results", [])
            best_label = result.get("best_combined_label", "")
            total_time = result.get("total_time", 0)

            pair_text = self._pair_combo.currentText()
            html = self._build_optimize_html(
                pair_text, calc_results, filter_results, best_label, total_time
            )

            try:
                crash_html = Path(__file__).resolve().parent.parent.parent / "data" / "last_optimize.html"
                crash_html.parent.mkdir(parents=True, exist_ok=True)
                crash_html.write_text(html, encoding="utf-8")
            except Exception:
                pass

            # Auto-save to per-pair results folder
            n_combos = len(calc_results)
            best_short = best_label.split(": ")[-1][:40] if best_label else "none"
            safe_best = "".join(c for c in best_short if c.isalnum() or c in "_-+.")
            details = f"{self._backtest_days}d_{n_combos}combos_best-{safe_best}"
            self._save_results_to_folder(html, "optimize", details)

            self._results_text.clear()
            self._results_text.setHtml(html)
            self._results_text.moveCursor(
                self._results_text.textCursor().MoveOperation.Start
            )
            self._status_label.setText(
                f"Optimization complete for {pair_text} in {total_time:.1f}s"
            )
        except Exception as e:
            import traceback
            self._on_error(f"{e}\n{traceback.format_exc()}")

    # ── Optimize All Pairs ─────────────────────────────────────

    def _start_optimize_all(self) -> None:
        """Start optimization for ALL pairs sequentially."""
        from PyQt6.QtWidgets import QMessageBox
        from takumi_trader.core.backtester import BacktestConfig
        from takumi_trader.core.strength import DISPLAY_PAIRS

        period_text = self._period_combo.currentText()
        if period_text == "Custom date":
            qd = self._start_date.date()
            start_date_str = qd.toString("yyyy-MM-dd")
            days = qd.daysTo(QDate.currentDate())
        else:
            start_date_str = ""
            days = self._PERIOD_MAP[period_text]

        # Confirm with user
        reply = QMessageBox.question(
            self,
            "Optimize All Pairs",
            f"This will optimize ALL {len(DISPLAY_PAIRS)} pairs sequentially.\n\n"
            f"Period: {period_text} ({days} days)\n"
            f"Estimated time: {len(DISPLAY_PAIRS) * 1.5:.0f}–{len(DISPLAY_PAIRS) * 3:.0f} minutes\n\n"
            f"Best settings will be auto-saved for each pair.\n"
            f"Continue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.Yes,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        config = BacktestConfig(
            days_back=days,
            start_date=start_date_str,
            use_dukascopy=self._data_source.currentIndex() == 1,
            exit_spread_threshold=self._exit_spread_spin.value(),
            post_exit_hours=self._post_exit_spin.value(),
            use_htf_exit=self._htf_check.isChecked(),
            allow_session_reentry=self._chk_reentry.isChecked(),
            use_accel_entry=self._chk_accel.isChecked(),
        )

        self._run_btn.setEnabled(False)
        self._optimize_btn.setEnabled(False)
        self._optimize_all_btn.setEnabled(False)
        self._view_perf_btn.setEnabled(False)
        self._save_profile_btn.setEnabled(False)
        self._save_set_btn.setEnabled(False)
        self._results_text.clear()
        self._progress_bar.setValue(0)
        self._optimize_all_results = {}
        self._backtest_days = days
        self._backtest_start = start_date_str

        # Build period string for save
        if period_text == "Custom date":
            qd = self._start_date.date()
            self._optimize_all_period = f"{qd.toString('yyyy-MM-dd')} → today ({days}d)"
        else:
            self._optimize_all_period = f"{period_text} ({days}d)"

        self._optimize_all_worker = OptimizeAllWorker(config, list(DISPLAY_PAIRS))
        self._optimize_all_worker.progress.connect(self._on_opt_all_progress)
        self._optimize_all_worker.pair_done.connect(self._on_opt_all_pair_done)
        self._optimize_all_worker.finished.connect(self._on_opt_all_finished)
        self._optimize_all_worker.error.connect(self._on_error)
        self._optimize_all_worker.start()
        self._enable_stop_btn()

        # Show initial status in results area
        self._results_text.setPlainText(
            f"Optimizing ALL {len(DISPLAY_PAIRS)} pairs...\n"
            f"Period: {self._optimize_all_period}\n\n"
            f"Results will appear here as each pair completes.\n"
        )

    def _on_opt_all_progress(self, current: int, total: int, status: str) -> None:
        if total > 0:
            pct = int(current / total * 100)
            self._progress_bar.setValue(pct)
        self._status_label.setText(status[:120])

    def _on_opt_all_pair_done(self, pair: str, result: dict) -> None:
        """Called when a single pair's optimization completes.

        Saves immediately after each pair so no work is lost on crash:
        1. Pair algo settings → data/pair_algo_settings.json
        2. Per-pair HTML report → data/results/<pair>/
        3. Running summary HTML → data/last_optimize_all.html
        """
        from takumi_trader.core.pair_algo_settings import save_pair_settings

        self._optimize_all_results[pair] = result
        calc_results = result.get("calc_results", [])

        # 1) Auto-save best settings for live trading
        if calc_results:
            best = calc_results[0]
            sltp_info = {}
            if best.sl_atr > 0:
                sltp_info = {
                    "sl_atr": best.sl_atr,
                    "tp_atr": best.tp_atr,
                    "sl_pips": best.sl_pips,
                    "tp_pips": best.tp_pips,
                }
            save_pair_settings(
                pair=pair,
                calc_params=best.calc_params,
                stats={
                    "trades": best.trades,
                    "wr": best.wr,
                    "exp_r": best.exp_r,
                    "avg_final": best.avg_final,
                    "total_r": best.total_r,
                    "avg_mfe": best.avg_mfe,
                    "avg_mae": best.avg_mae,
                },
                sltp=sltp_info,
                source="optimizer-all",
                backtest_period=getattr(self, '_optimize_all_period', ''),
            )

        # 2) Save per-pair detailed HTML report
        if calc_results:
            try:
                filter_results = result.get("filter_results", [])
                best_label = result.get("best_combined_label", "")
                total_time = result.get("total_time", 0)
                pair_html = self._build_optimize_html(
                    pair, calc_results, filter_results, best_label, total_time
                )
                self._save_results_to_folder(
                    pair_html, "optimize-all", f"{self._backtest_days}d",
                    pair_override=pair,
                )
            except Exception as e:
                logger.warning("Failed to save per-pair HTML for %s: %s", pair, e)

        # 2b) Save individual trade log CSV for chart review
        if calc_results and calc_results[0].outcomes:
            try:
                self._save_trade_log_csv(pair, calc_results[0])
            except Exception as e:
                logger.warning("Failed to save trade log CSV for %s: %s", pair, e)

        # 3) Save running summary so progress survives a crash
        try:
            summary_html = self._build_optimize_all_html(
                self._optimize_all_results, 0
            )
            html_file = Path(__file__).resolve().parent.parent.parent / "data" / "last_optimize_all.html"
            html_file.parent.mkdir(parents=True, exist_ok=True)
            html_file.write_text(summary_html, encoding="utf-8")
        except Exception:
            pass

        # Update live results display
        self._update_optimize_all_display()

    def _save_trade_log_csv(self, pair: str, best) -> None:
        """Save individual trade details as CSV for chart review.

        Includes date, time, session, direction, entry price, SL/TP levels,
        MFE, MAE, exit type (TP/SL/Signal), and P/L in pips and R-multiples.
        """
        import csv
        import sys
        from datetime import datetime as _dt, timezone as _tz

        if getattr(sys, 'frozen', False):
            base = Path(sys.executable).parent / "data" / "results"
        else:
            base = Path(__file__).resolve().parent.parent.parent / "data" / "results"

        out_dir = base / pair
        out_dir.mkdir(parents=True, exist_ok=True)
        csv_path = out_dir / "trade_log.csv"

        outcomes = sorted(best.outcomes, key=lambda o: o.entry_time)

        # Simulate SL/TP per trade to get exit type
        import numpy as np
        rows = []
        for trade_no, o in enumerate(outcomes, 1):
            # Compute per-trade SL/TP
            if best.sl_atr > 0 and o.entry_atr_pips > 0:
                sl_p = round(best.sl_atr * o.entry_atr_pips, 1)
                tp_p = round(best.tp_atr * o.entry_atr_pips, 1)
            else:
                sl_p = best.sl_pips
                tp_p = best.tp_pips

            # Bar-by-bar SL/TP simulation
            sig_pnl = o.exit_signal_pnl_pips if o.exit_signal_pnl_pips != 0 else o.final_pnl_pips
            if o.bar_running_mfe and o.bar_running_mae:
                mfe_arr = np.array(o.bar_running_mfe, dtype=np.float64)
                mae_arr = np.array(o.bar_running_mae, dtype=np.float64)
            else:
                mfe_arr = np.array([o.mfe_pips], dtype=np.float64)
                mae_arr = np.array([o.mae_pips], dtype=np.float64)

            trade_pnl, exit_type = _simulate_sl_tp_for_trade(
                sl_p, tp_p, mfe_arr, mae_arr, sig_pnl,
            )
            r_val = trade_pnl / sl_p if sl_p > 0 else 0.0

            entry_dt = _dt.fromtimestamp(o.entry_time, tz=_tz.utc)
            pip = pip_value(pair)

            # Calculate SL/TP price levels
            if o.direction == "BUY":
                sl_price = o.entry_price - sl_p * pip
                tp_price = o.entry_price + tp_p * pip
            else:
                sl_price = o.entry_price + sl_p * pip
                tp_price = o.entry_price - tp_p * pip

            n_bars = len(o.bar_running_mfe) if o.bar_running_mfe else 0

            rows.append({
                "trade_no": trade_no,
                "date": entry_dt.strftime("%Y-%m-%d"),
                "day": entry_dt.strftime("%a"),
                "time_utc": entry_dt.strftime("%H:%M"),
                "session": o.session or "",
                "direction": o.direction,
                "entry_price": round(o.entry_price, 5),
                "sl_price": round(sl_price, 5),
                "tp_price": round(tp_price, 5),
                "sl_pips": sl_p,
                "tp_pips": tp_p,
                "atr_pips": round(o.entry_atr_pips, 1),
                "mfe_pips": round(o.mfe_pips, 1),
                "mae_pips": round(o.mae_pips, 1),
                "signal_pnl": round(sig_pnl, 1),
                "exit_type": exit_type,
                "final_pnl_pips": round(trade_pnl, 1),
                "final_pnl_r": round(r_val, 2),
                "bars": n_bars,
                "conviction": o.conviction_score,
                "spread": round(o.strength_spread, 1),
            })

        if rows:
            fieldnames = list(rows[0].keys())
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            logger.info("Trade log saved: %s (%d trades)", csv_path, len(rows))

    def _update_optimize_all_display(self) -> None:
        """Update the results text with current optimize-all progress."""
        from takumi_trader.core.strength import DISPLAY_PAIRS

        lines = [
            f"OPTIMIZE ALL PAIRS — {len(self._optimize_all_results)}/{len(DISPLAY_PAIRS)} complete\n",
            f"Period: {getattr(self, '_optimize_all_period', '')}\n",
            f"{'─' * 90}\n",
            f"{'Pair':<10} {'Trades':>6} {'Hybrid WR':>10} {'Hybrid E[R]':>11} "
            f"{'Strict WR':>10} {'Strict E[R]':>11} {'SL/TP':>16} {'Status':<10}\n",
            f"{'─' * 90}\n",
        ]

        for pair in DISPLAY_PAIRS:
            if pair not in self._optimize_all_results:
                lines.append(f"{pair:<10} {'':>6} {'':>10} {'':>11} {'':>10} {'':>11} {'':>16} {'waiting...':<10}\n")
                continue

            result = self._optimize_all_results[pair]
            calc_results = result.get("calc_results", [])

            if result.get("error"):
                lines.append(f"{pair:<10} {'':>6} {'':>10} {'':>11} {'':>10} {'':>11} {'':>16} {'ERROR':<10}\n")
                continue

            if not calc_results:
                lines.append(f"{pair:<10} {'0':>6} {'':>10} {'':>11} {'':>10} {'':>11} {'':>16} {'no data':<10}\n")
                continue

            best = calc_results[0]
            sltp = f"SL{best.sl_atr}×/TP{best.tp_atr}×" if best.sl_atr > 0 else "signal exit"
            lines.append(
                f"{pair:<10} {best.trades:>6} {best.wr:>9.0f}% {best.exp_r:>+10.2f}R "
                f"{best.strict_wr:>9.0f}% {best.strict_exp_r:>+10.2f}R "
                f"{sltp:>16} {'SAVED':>10}\n"
            )

        self._results_text.setPlainText("".join(lines))

    def _on_opt_all_finished(self, payload: dict) -> None:
        """Called when ALL pairs are done."""
        try:
            self._optimize_all_worker = None
            self._reset_buttons()
            self._progress_bar.setValue(100)

            all_results = payload.get("results", {})
            total_time = payload.get("total_time", 0)

            # Build final HTML summary
            html = self._build_optimize_all_html(all_results, total_time)

            # Save HTML
            try:
                html_file = Path(__file__).resolve().parent.parent.parent / "data" / "last_optimize_all.html"
                html_file.parent.mkdir(parents=True, exist_ok=True)
                html_file.write_text(html, encoding="utf-8")
            except Exception:
                pass

            # Save to results folder
            n_pairs = len(all_results)
            n_success = sum(1 for r in all_results.values() if r.get("calc_results"))
            details = f"{self._backtest_days}d_{n_success}of{n_pairs}pairs"
            self._save_results_to_folder(html, "optimize-all", details)

            # Render
            self._results_text.clear()
            self._results_text.setHtml(html)
            self._results_text.moveCursor(
                self._results_text.textCursor().MoveOperation.Start
            )

            minutes = total_time / 60
            self._status_label.setText(
                f"All pairs optimized in {minutes:.1f} min — {n_success}/{n_pairs} pairs saved"
            )
        except Exception as e:
            import traceback
            self._on_error(f"{e}\n{traceback.format_exc()}")

    def _build_optimize_all_html(self, all_results: dict, total_time: float) -> str:
        """Build HTML summary for optimize-all results."""
        parts = [_CSS, "<body>"]
        n_pairs = len(all_results)
        n_success = sum(1 for r in all_results.values() if r.get("calc_results"))
        minutes = total_time / 60

        # Date range from first successful result
        date_range_str = ""
        for r in all_results.values():
            calc = r.get("calc_results", [])
            if calc and calc[0].outcomes:
                from datetime import datetime as dt
                all_times = [o.entry_time for o in calc[0].outcomes if o.entry_time > 0]
                if all_times:
                    earliest = dt.utcfromtimestamp(min(all_times))
                    latest = dt.utcfromtimestamp(max(all_times))
                    actual_days = max(1, (latest - earliest).days)
                    date_range_str = (
                        f'Period: <b>{earliest.strftime("%Y-%m-%d")} \u2192 '
                        f'{latest.strftime("%Y-%m-%d")}</b> ({actual_days} days)'
                    )
                    break

        parts.append(f"""
        <div class="summary-box">
        <h2>Optimize ALL Pairs — Complete</h2>
        <p><b>{n_success}</b> of <b>{n_pairs}</b> pairs optimized successfully in <b>{minutes:.1f}</b> minutes<br>
        {date_range_str}<br>
        Best settings auto-saved for each pair.</p>
        </div>
        """)

        # Summary table
        parts.append("<h2>ALL PAIRS — Best Strategy Per Pair</h2>")
        parts.append("<table>")
        parts.append(
            "<tr><th>#</th><th>Pair</th><th>Trades</th><th>Entry Settings</th>"
            "<th>SL (ATR)</th><th>TP (ATR)</th><th>R:R</th>"
            '<th colspan="3" style="background:#2c6e49;text-align:center;">── Hybrid ──</th>'
            '<th colspan="3" style="background:#7b3f00;text-align:center;">── Strict ──</th>'
            "</tr>"
            "<tr><th></th><th></th><th></th><th></th><th></th><th></th><th></th>"
            "<th>WR%</th><th>E[R]</th><th>Total R</th>"
            "<th>WR%</th><th>E[R]</th><th>Total R</th></tr>"
        )

        # Sort by hybrid E[R] descending
        sorted_pairs = []
        for pair, result in all_results.items():
            calc = result.get("calc_results", [])
            if calc:
                sorted_pairs.append((pair, calc[0]))
            else:
                sorted_pairs.append((pair, None))
        sorted_pairs.sort(key=lambda x: x[1].exp_r if x[1] else -999, reverse=True)

        for rank, (pair, best) in enumerate(sorted_pairs, 1):
            if best is None:
                parts.append(
                    f'<tr><td class="rank">{rank}</td><td>{pair}</td>'
                    f'<td colspan="11" class="neutral">No data / insufficient trades</td></tr>'
                )
                continue

            cp = best.calc_params
            entry_short = []
            if cp.get("ema_period", 8) != 8: entry_short.append(f"EMA{cp['ema_period']}")
            if cp.get("roc_decay", 0.3) != 0.3: entry_short.append(f"d={cp['roc_decay']}")
            if cp.get("threshold_m1", 6.5) != 6.5: entry_short.append(f"t={cp['threshold_m1']}")
            if cp.get("require_acceleration"): entry_short.append("ACC")
            entry_label = " ".join(entry_short) if entry_short else "DEFAULT"

            rr = best.tp_atr / best.sl_atr if best.sl_atr > 0 else 0
            row_style = ' style="background:#e8f8e8;"' if best.exp_r >= 1.0 else ""

            parts.append(
                f'<tr{row_style}><td class="rank">{rank}</td><td><b>{pair}</b></td>'
                f'<td>{best.trades}</td><td>{entry_label}</td>'
                f'<td>{best.sl_atr}</td><td>{best.tp_atr}</td>'
                f'<td>{rr:.1f}</td>'
                f'<td>{best.wr:.0f}%</td>'
                f'<td><b>{_clr(best.exp_r, "+.2f")}R</b></td>'
                f'<td>{_clr(best.total_r, "+.1f")}R</td>'
                f'<td>{best.strict_wr:.0f}%</td>'
                f'<td><b>{_clr(best.strict_exp_r, "+.2f")}R</b></td>'
                f'<td>{_clr(best.strict_total_r, "+.1f")}R</td></tr>'
            )

        parts.append("</table>")

        # Aggregate stats
        valid = [(p, b) for p, b in sorted_pairs if b is not None]
        if valid:
            total_trades = sum(b.trades for _, b in valid)
            avg_exp_r = sum(b.exp_r for _, b in valid) / len(valid)
            avg_strict_exp_r = sum(b.strict_exp_r for _, b in valid) / len(valid)
            total_hybrid_r = sum(b.total_r for _, b in valid)
            total_strict_r = sum(b.strict_total_r for _, b in valid)
            profitable_h = sum(1 for _, b in valid if b.exp_r > 0)
            profitable_s = sum(1 for _, b in valid if b.strict_exp_r > 0)

            parts.append(f"""
            <div class="summary-box">
            <h3>Aggregate Summary</h3>
            <table>
            <tr><td>Pairs optimized:</td><td><b>{len(valid)}</b></td>
                <td>&nbsp;&nbsp;Total trades:</td><td><b>{total_trades}</b></td></tr>
            <tr><td colspan="4" style="padding-top:6px;"><b style="color:#2c6e49;">Hybrid:</b></td></tr>
            <tr><td>Avg E[R]:</td><td><b>{_clr(avg_exp_r, "+.2f")}R</b></td>
                <td>&nbsp;&nbsp;Combined Total R:</td><td><b>{_clr(total_hybrid_r, "+.1f")}R</b></td></tr>
            <tr><td>Profitable pairs:</td><td><b>{profitable_h}/{len(valid)}</b></td>
                <td></td><td></td></tr>
            <tr><td colspan="4" style="padding-top:6px;"><b style="color:#7b3f00;">Strict:</b></td></tr>
            <tr><td>Avg E[R]:</td><td><b>{_clr(avg_strict_exp_r, "+.2f")}R</b></td>
                <td>&nbsp;&nbsp;Combined Total R:</td><td><b>{_clr(total_strict_r, "+.1f")}R</b></td></tr>
            <tr><td>Profitable pairs:</td><td><b>{profitable_s}/{len(valid)}</b></td>
                <td></td><td></td></tr>
            </table>
            </div>
            """)

        parts.append("</body>")
        return "".join(parts)

    def _build_optimize_html(
        self,
        pair: str,
        calc_results: list,
        filter_results: list[dict],
        best_label: str,
        total_time: float,
    ) -> str:
        """Build HTML report for joint entry+exit optimization results."""
        parts = [_CSS, "<body>"]

        # Extract date range from best result's outcomes
        date_range_str = ""
        if calc_results and calc_results[0].outcomes:
            from datetime import datetime as dt
            all_times = [o.entry_time for o in calc_results[0].outcomes if o.entry_time > 0]
            if all_times:
                earliest = dt.utcfromtimestamp(min(all_times))
                latest = dt.utcfromtimestamp(max(all_times))
                actual_days = max(1, (latest - earliest).days)
                date_range_str = (
                    f'<br>Period: <b>{earliest.strftime("%Y-%m-%d")} \u2192 '
                    f'{latest.strftime("%Y-%m-%d")}</b> ({actual_days} days)'
                )

        parts.append(f"""
        <div class="summary-box">
        <h2>Joint Entry + Exit Optimization: {pair}</h2>
        <p>Tested <b>{len(calc_results)}</b> entry+exit combos in <b>{total_time:.1f}s</b>
        {date_range_str}</p>
        {f'<p>Best: <b>{best_label}</b></p>' if best_label else ''}
        </div>
        """)

        if calc_results:
            # ── Top results table ──
            parts.append("<h2>TOP STRATEGIES (ranked by Expected R per trade)</h2>")
            parts.append("<table>")
            parts.append(
                "<tr><th>#</th><th>Entry + Exit Settings</th><th>Trades</th>"
                '<th colspan="5" style="background:#2c6e49;text-align:center;">── Hybrid (SL/TP + Signal Exit) ──</th>'
                '<th colspan="4" style="background:#7b3f00;text-align:center;">── Strict (Set &amp; Forget) ──</th>'
                "</tr>"
                "<tr><th></th><th></th><th></th>"
                "<th>WR%</th><th>E[R]</th><th>Total R</th>"
                "<th>TP hit</th><th>SL hit</th>"
                "<th>WR%</th><th>E[R]</th>"
                "<th>TP hit</th><th>SL hit</th></tr>"
            )
            for rank, r in enumerate(calc_results[:30], 1):
                # Exit breakdown columns (hybrid)
                if r.sl_atr > 0:
                    tp_str = f"{r.n_tp_hit}"
                    sl_str = f"{r.n_sl_hit}"
                    s_tp_str = f"{r.strict_n_tp_hit}"
                    s_sl_str = f"{r.strict_n_sl_hit}"
                else:
                    tp_str = "-"
                    sl_str = "-"
                    s_tp_str = "-"
                    s_sl_str = "-"
                # Highlight top 3
                row_style = ' style="background:#e8f8e8;"' if rank <= 3 else ""
                parts.append(
                    f'<tr{row_style}><td class="rank">{rank}</td><td>{r.label}</td>'
                    f'<td>{r.trades}</td>'
                    f'<td>{r.wr:.0f}%</td>'
                    f'<td><b>{_clr(r.exp_r, "+.2f")}R</b></td>'
                    f'<td>{_clr(r.total_r, "+.1f")}R</td>'
                    f'<td class="good">{tp_str}</td>'
                    f'<td class="bad">{sl_str}</td>'
                    f'<td>{r.strict_wr:.0f}%</td>'
                    f'<td><b>{_clr(r.strict_exp_r, "+.2f")}R</b></td>'
                    f'<td class="good">{s_tp_str}</td>'
                    f'<td class="bad">{s_sl_str}</td></tr>'
                )
            parts.append("</table>")

            # ── Best combined params detail box ──
            best = calc_results[0]
            cp = best.calc_params
            parts.append(f"""
            <div class="summary-box">
            <h3>Best Strategy for {pair}</h3>
            <table>
            <tr><td colspan="4"><b>Entry Parameters:</b></td></tr>
            <tr><td>EMA Period:</td><td><b>{cp.get('ema_period', 8)}</b></td>
                <td>&nbsp;&nbsp;ROC Decay:</td><td><b>{cp.get('roc_decay', 0.3)}</b></td></tr>
            <tr><td>Sensitivity:</td><td><b>{cp.get('sensitivity', 1.0)}</b></td>
                <td>&nbsp;&nbsp;M1 Threshold:</td><td><b>{cp.get('threshold_m1', 6.5)}</b></td></tr>
            <tr><td>Min Divergence:</td><td><b>{cp.get('min_divergence_spread', 12.0)}</b></td>
                <td>&nbsp;&nbsp;Require Accel:</td><td><b>{cp.get('require_acceleration', False)}</b></td></tr>
            """)

            if best.sl_atr > 0:
                rr = best.tp_atr / best.sl_atr
                parts.append(f"""
                <tr><td colspan="4"><b>Exit Parameters (ATR-based):</b></td></tr>
                <tr><td>SL:</td><td><b>{best.sl_atr}× ATR ({best.sl_pips:.0f} pips avg)</b></td>
                    <td>&nbsp;&nbsp;TP:</td><td><b>{best.tp_atr}× ATR ({best.tp_pips:.0f} pips avg)</b></td></tr>
                <tr><td>Risk:Reward:</td><td><b>1:{rr:.1f}</b></td>
                    <td></td><td></td></tr>

                <tr><td colspan="4" style="padding-top:8px;"><b style="color:#2c6e49;">HYBRID Mode (SL/TP + Signal Exit):</b></td></tr>
                <tr><td>TP hit:</td><td><b class="good">{best.n_tp_hit}</b> trades ({_clr(best.r_tp_hit, "+.1f")}R)</td>
                    <td>&nbsp;&nbsp;SL hit:</td><td><b class="bad">{best.n_sl_hit}</b> trades ({_clr(best.r_sl_hit, "+.1f")}R)</td></tr>
                """)
                if best.n_signal > 0:
                    parts.append(f"""
                    <tr><td>Signal exit:</td><td><b>{best.n_signal}</b> trades ({_clr(best.r_signal, "+.1f")}R)</td>
                        <td colspan="2"><i>Neither SL nor TP hit</i></td></tr>
                    """)
                parts.append(f"""
                <tr><td>WR:</td><td><b>{best.wr:.0f}%</b></td>
                    <td>&nbsp;&nbsp;E[R]:</td><td><b>{_clr(best.exp_r, "+.2f")}R</b> per trade</td></tr>
                <tr><td>Total R:</td><td><b>{_clr(best.total_r, "+.1f")}R</b></td>
                    <td></td><td></td></tr>

                <tr><td colspan="4" style="padding-top:8px;"><b style="color:#7b3f00;">STRICT Mode (Set &amp; Forget — SL/TP only):</b></td></tr>
                <tr><td>TP hit:</td><td><b class="good">{best.strict_n_tp_hit}</b> trades ({_clr(best.strict_r_tp_hit, "+.1f")}R)</td>
                    <td>&nbsp;&nbsp;SL hit:</td><td><b class="bad">{best.strict_n_sl_hit}</b> trades ({_clr(best.strict_r_sl_hit, "+.1f")}R)</td></tr>
                <tr><td>WR:</td><td><b>{best.strict_wr:.0f}%</b></td>
                    <td>&nbsp;&nbsp;E[R]:</td><td><b>{_clr(best.strict_exp_r, "+.2f")}R</b> per trade</td></tr>
                <tr><td>Total R:</td><td><b>{_clr(best.strict_total_r, "+.1f")}R</b></td>
                    <td></td><td></td></tr>
                """)
            else:
                parts.append("""
                <tr><td colspan="4"><b>Exit: Signal-based</b> (no fixed SL/TP)</td></tr>
                """)

            parts.append(f"""
            </table>
            <p>Trades: <b>{best.trades}</b> |
               Hybrid: WR <b>{best.wr:.0f}%</b>, E[R] <b>{_clr(best.exp_r, "+.2f")}R</b>, Total <b>{_clr(best.total_r, "+.1f")}R</b> |
               Strict: WR <b>{best.strict_wr:.0f}%</b>, E[R] <b>{_clr(best.strict_exp_r, "+.2f")}R</b>, Total <b>{_clr(best.strict_total_r, "+.1f")}R</b></p>
            </div>
            """)

        # ── Individual Trade List (best strategy) ──
        if calc_results and calc_results[0].outcomes:
            best = calc_results[0]
            outcomes_sorted = sorted(best.outcomes, key=lambda o: o.entry_time)
            n_trades = len(outcomes_sorted)

            # Simulate SL/TP for each trade to show per-trade outcome
            trade_exits: list[tuple[float, str]] = []
            if best.sl_atr > 0:
                for o in outcomes_sorted:
                    sl_p = best.sl_atr * o.entry_atr_pips if o.entry_atr_pips > 0 else best.sl_pips
                    tp_p = best.tp_atr * o.entry_atr_pips if o.entry_atr_pips > 0 else best.tp_pips
                    if o.bar_running_mfe and o.bar_running_mae:
                        mfe_arr = np.array(o.bar_running_mfe, dtype=np.float64)
                        mae_arr = np.array(o.bar_running_mae, dtype=np.float64)
                    else:
                        mfe_arr = np.array([o.mfe_pips], dtype=np.float64)
                        mae_arr = np.array([o.mae_pips], dtype=np.float64)
                    sig_pnl = o.exit_signal_pnl_pips if o.exit_signal_pnl_pips != 0 else o.final_pnl_pips
                    pnl, etype = _simulate_sl_tp_for_trade(sl_p, tp_p, mfe_arr, mae_arr, sig_pnl)
                    trade_exits.append((pnl, etype))
            else:
                for o in outcomes_sorted:
                    sig_pnl = o.exit_signal_pnl_pips if o.exit_signal_pnl_pips != 0 else o.final_pnl_pips
                    trade_exits.append((sig_pnl, "signal"))

            parts.append(f'<h2>INDIVIDUAL TRADES ({n_trades})</h2>')
            parts.append('<p style="font-size:11px;color:#888;">Click any column header to sort ↑↓ (works in browser view)</p>')
            parts.append('<table id="trades-table" style="font-size:11px;">')
            parts.append(
                '<tr>'
                '<th onclick="sortTable(0,\'num\')" style="cursor:pointer;"># ↕</th>'
                '<th onclick="sortTable(1,\'str\')" style="cursor:pointer;">Date ↕</th>'
                '<th onclick="sortTable(2,\'str\')" style="cursor:pointer;">Time ↕</th>'
                '<th onclick="sortTable(3,\'str\')" style="cursor:pointer;">Dir ↕</th>'
                '<th onclick="sortTable(4,\'str\')" style="cursor:pointer;">Session ↕</th>'
                '<th onclick="sortTable(5,\'num\')" style="cursor:pointer;">Entry ↕</th>'
                '<th onclick="sortTable(6,\'num\')" style="cursor:pointer;">ATR ↕</th>'
                '<th onclick="sortTable(7,\'num\')" style="cursor:pointer;">SL ↕</th>'
                '<th onclick="sortTable(8,\'num\')" style="cursor:pointer;">TP ↕</th>'
                '<th onclick="sortTable(9,\'num\')" style="cursor:pointer;">MFE ↕</th>'
                '<th onclick="sortTable(10,\'num\')" style="cursor:pointer;">MAE ↕</th>'
                '<th onclick="sortTable(11,\'num\')" style="cursor:pointer;">Signal P/L ↕</th>'
                '<th onclick="sortTable(12,\'num\')" style="cursor:pointer;">Bars ↕</th>'
                '<th onclick="sortTable(13,\'str\')" style="cursor:pointer;">Exit ↕</th>'
                '<th onclick="sortTable(14,\'num\')" style="cursor:pointer;">Final P/L ↕</th>'
                '</tr>'
            )
            from datetime import datetime as _dt, timezone as _tz
            cumulative_r = 0.0
            for idx, o in enumerate(outcomes_sorted, 1):
                entry_dt = _dt.fromtimestamp(o.entry_time, tz=_tz.utc)
                date_str = entry_dt.strftime("%Y-%m-%d")
                time_str = entry_dt.strftime("%H:%M")
                day_str = entry_dt.strftime("%a")

                sig_pnl = o.exit_signal_pnl_pips if o.exit_signal_pnl_pips != 0 else o.final_pnl_pips
                n_bars = len(o.bar_running_mfe) if o.bar_running_mfe else int(o.time_to_exit_minutes)

                trade_pnl, exit_type = trade_exits[idx - 1]

                # Compute SL/TP pips for this trade
                if best.sl_atr > 0 and o.entry_atr_pips > 0:
                    sl_p = round(best.sl_atr * o.entry_atr_pips, 1)
                    tp_p = round(best.tp_atr * o.entry_atr_pips, 1)
                else:
                    sl_p = best.sl_pips
                    tp_p = best.tp_pips

                r_val = trade_pnl / sl_p if sl_p > 0 else 0.0
                cumulative_r += r_val

                # Exit type display
                if exit_type == "tp_hit":
                    exit_disp = '<span class="good"><b>TP</b></span>'
                elif exit_type == "sl_hit":
                    exit_disp = '<span class="bad"><b>SL</b></span>'
                else:
                    exit_disp = '<span style="color:#888;">SIG</span>'

                dir_cls = "good" if o.direction == "BUY" else "bad"
                row_bg = ""
                if exit_type == "tp_hit":
                    row_bg = ' style="background:#e8f8e8;"'
                elif exit_type == "sl_hit":
                    row_bg = ' style="background:#fde8e8;"'

                parts.append(
                    f'<tr{row_bg}>'
                    f'<td style="font-weight:bold;font-size:13px;text-align:center;min-width:30px;">{idx}</td>'
                    f'<td>{date_str} {day_str}</td>'
                    f'<td>{time_str}</td>'
                    f'<td class="{dir_cls}"><b>{o.direction}</b></td>'
                    f'<td>{o.session or "—"}</td>'
                    f'<td>{o.entry_price:.5f}</td>'
                    f'<td>{o.entry_atr_pips:.1f}</td>'
                    f'<td>{sl_p:.1f}</td>'
                    f'<td>{tp_p:.1f}</td>'
                    f'<td class="good">{_clr(o.mfe_pips, "+.1f")}</td>'
                    f'<td class="bad">{_clr(-o.mae_pips, "+.1f")}</td>'
                    f'<td>{_clr(sig_pnl, "+.1f")}</td>'
                    f'<td>{n_bars}</td>'
                    f'<td>{exit_disp}</td>'
                    f'<td><b>{_clr(trade_pnl, "+.1f")}p</b> ({_clr(r_val, "+.1f")}R)</td>'
                    f'</tr>'
                )
            # Summary row
            total_pnl = sum(te[0] for te in trade_exits)
            parts.append(
                f'<tr style="border-top:2px solid #333;font-weight:bold;">'
                f'<td colspan="14" style="text-align:right;">TOTAL:</td>'
                f'<td>{_clr(total_pnl, "+.1f")}p ({_clr(cumulative_r, "+.1f")}R)</td></tr>'
            )
            parts.append("</table>")

        # ── Filter results ──
        if filter_results:
            parts.append("<h2>FILTER COMBINATIONS (on best entry+exit params)</h2>")
            parts.append("<table>")
            parts.append(
                "<tr><th>#</th><th>Filters</th><th>Trades</th><th>WR%</th>"
                "<th>Avg Final</th><th>E[R]</th><th>Total R</th></tr>"
            )
            for rank, r in enumerate(filter_results[:20], 1):
                parts.append(
                    f'<tr><td class="rank">{rank}</td><td>{r["label"]}</td>'
                    f'<td>{r["trades"]}</td><td>{r["wr"]:.0f}%</td>'
                    f'<td>{_clr(r["avg_final"], "+.1f")}</td>'
                    f'<td>{_clr(r["exp_r"], "+.2f")}R</td>'
                    f'<td>{_clr(r["total_r"], "+.1f")}R</td></tr>'
                )
            parts.append("</table>")

        if not calc_results and not filter_results:
            parts.append("<p>No results found. Try a longer period or different data source.</p>")

        # Sorting JavaScript (works when HTML is opened in browser)
        parts.append("""
        <script>
        let sortDir = {};
        function sortTable(colIdx, type) {
            const table = document.getElementById('trades-table');
            if (!table) return;
            const tbody = table.tBodies[0] || table;
            const rows = Array.from(tbody.querySelectorAll('tr')).slice(1); // skip header
            // Remove summary row (last row with colspan)
            const dataRows = rows.filter(r => !r.querySelector('td[colspan]'));
            const summaryRows = rows.filter(r => r.querySelector('td[colspan]'));

            sortDir[colIdx] = !(sortDir[colIdx] || false); // toggle
            const asc = sortDir[colIdx];

            dataRows.sort((a, b) => {
                let aVal = a.cells[colIdx] ? a.cells[colIdx].textContent.trim() : '';
                let bVal = b.cells[colIdx] ? b.cells[colIdx].textContent.trim() : '';
                if (type === 'num') {
                    aVal = parseFloat(aVal.replace(/[^\\d.\\-]/g, '')) || 0;
                    bVal = parseFloat(bVal.replace(/[^\\d.\\-]/g, '')) || 0;
                    return asc ? aVal - bVal : bVal - aVal;
                }
                return asc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
            });

            // Re-insert sorted rows + summary at end
            dataRows.forEach(r => tbody.appendChild(r));
            summaryRows.forEach(r => tbody.appendChild(r));
        }
        </script>
        """)

        parts.append("</body>")
        return "".join(parts)

    # ── HTML Summary builder ──────────────────────────────────────

    def _build_html(self, outcomes: list[AlertOutcome],
                    all_combos: list[dict] | None = None,
                    profile: dict | None = None,
                    atr_combos: list[dict] | None = None,
                    atr_profile: dict | None = None) -> str:
        n = len(outcomes)
        days = self._backtest_days
        start_str = self._backtest_start

        # Compute actual date range from trade data
        if n > 0:
            from datetime import datetime as dt
            earliest_ts = min(o.entry_time for o in outcomes)
            latest_ts = max(o.entry_time for o in outcomes)
            earliest_dt = dt.utcfromtimestamp(earliest_ts)
            latest_dt = dt.utcfromtimestamp(latest_ts)
            actual_days = max(1, (latest_dt - earliest_dt).days)
            actual_range = f"{earliest_dt.strftime('%Y-%m-%d')} \u2192 {latest_dt.strftime('%Y-%m-%d')}"
            # Use actual days for projections if different from requested
            if actual_days > 0:
                days = actual_days

        if n == 0:
            period = f"from {start_str}" if start_str else f"over {days} days"
            return (
                f"{_CSS}<body><h2>Backtest Complete</h2>"
                f"<p>0 trades found {period}.<br>"
                f"<b>Note:</b> MT5 typically only stores M1 data for 1-3 months. "
                f"Try a shorter period or use a preset (3/7/14/30 days).</p></body>"
            )

        wins = sum(1 for o in outcomes if o.final_pnl_pips > 0)
        wr = wins / n * 100

        avg_mfe = sum(o.mfe_pips for o in outcomes) / n
        avg_mae = sum(o.mae_pips for o in outcomes) / n
        avg_exit = sum(o.exit_signal_pnl_pips for o in outcomes) / n
        avg_final = sum(o.final_pnl_pips for o in outcomes) / n
        avg_max_mfe = sum(o.max_mfe_pips for o in outcomes) / n
        avg_max_mae = sum(o.max_mae_pips for o in outcomes) / n

        # Build filter label
        filters_on = []
        if self._chk_htf.isChecked(): filters_on.append("HTF")
        if self._chk_vel.isChecked(): filters_on.append("VEL")
        if self._chk_isol.isChecked(): filters_on.append("ISOL")
        if self._chk_adr.isChecked(): filters_on.append("STRUCT")
        if self._chk_news.isChecked(): filters_on.append("NEWS")
        filter_str = " + ".join(filters_on) if filters_on else "NONE"
        conv_thr = self._conviction_spin.value()

        period_label = f"{actual_range} ({days} days)" if n > 0 else f"over {days} days"
        parts = [_CSS, "<body>"]

        # ── Summary Box ──
        parts.append(f"""
        <div class="summary-box">
        <b>Backtest Complete</b> &mdash; {n} trades found<br>
        Period: <b>{period_label}</b><br>
        Filters: <b>{filter_str}</b> &nbsp;|&nbsp; Min conviction: <b>{conv_thr}%</b>
        <table>
        <tr><td>Win Rate:</td><td><b>{wr:.1f}%</b> ({wins}/{n})</td>
            <td>&nbsp;&nbsp;Avg MFE:</td><td>{_clr(avg_mfe, '+.1f')} pips</td></tr>
        <tr><td>Avg @Exit:</td><td>{_clr(avg_exit, '+.1f')} pips</td>
            <td>&nbsp;&nbsp;Avg MAE:</td><td>{_clr(-avg_mae, '+.1f')} pips</td></tr>
        <tr><td>Avg Final:</td><td>{_clr(avg_final, '+.1f')} pips</td>
            <td>&nbsp;&nbsp;Avg MAX-MFE:</td><td>{_clr(avg_max_mfe, '+.1f')} pips</td></tr>
        <tr><td></td><td></td>
            <td>&nbsp;&nbsp;Avg MAX-MAE:</td><td>{_clr(-avg_max_mae, '+.1f')} pips</td></tr>
        </table>
        </div>
        """)

        # ── SL/TP Optimization ──
        _MIN_SL = 5.0  # Minimum realistic SL
        if all_combos is None:
            all_combos = _optimize_sl_tp(outcomes)

        # Filter out unrealistic SL < 5 pips
        realistic_combos = [c for c in all_combos if c["sl"] >= _MIN_SL] if all_combos else []

        # Find THE recommended strategy for earnings projection: WR >= 60%, best expectancy in R
        recommended = None
        rec_pool = [c for c in realistic_combos if c["wr"] >= 60 and c["total_pnl"] > 0]
        if rec_pool:
            rec_pool.sort(key=lambda x: x["expectancy_r"], reverse=True)
            recommended = rec_pool[0]

        if realistic_combos:
            sections = [
                ("[REC] Recommended (WR\u226560%, best E[R])", sorted(
                    [c for c in realistic_combos if c["wr"] >= 60 and c["total_pnl"] > 0],
                    key=lambda x: x["expectancy_r"], reverse=True)[:5]),
                ("[TOP] Best Total Profit", sorted(
                    realistic_combos, key=lambda x: x["total_pnl"], reverse=True)[:5]),
                ("[REC] Sniper (WR\u226580%)", sorted(
                    [c for c in realistic_combos if c["wr"] >= 80 and c["total_pnl"] > 0],
                    key=lambda x: x["expectancy_r"], reverse=True)[:5]),
                ("[PF] Best Risk-Adjusted (PF)", sorted(
                    [c for c in realistic_combos if c["pf"] < 900 and c["total_pnl"] > 0],
                    key=lambda x: x["pf"] * (x["wins"] + x["losses"]) ** 0.5,
                    reverse=True)[:5]),
                ("[DD] Most Stable Equity (lowest MaxDD)", sorted(
                    [c for c in realistic_combos if c["total_pnl"] > 0 and (c["wins"] + c["losses"]) >= 10],
                    key=lambda x: x["max_dd"])[:5]),
            ]

            for title, combos in sections:
                if not combos:
                    continue
                # Total R for top combo
                top = combos[0]
                top_total_r = top["total_pnl"] / top["sl"] if top["sl"] > 0 else 0
                parts.append(f'<h3>{title} <span class="r-total">&nbsp;(top: {_clr(top_total_r, "+.1f")}R total)</span></h3>')
                parts.append(f"<table>{_combo_header()}")
                for c in combos:
                    parts.append(_combo_row(c))
                parts.append("</table>")

        # ── ATR-BASED SL/TP OPTIMIZATION ──
        atr_count = sum(1 for o in outcomes if o.entry_atr_pips > 0)
        if atr_combos is None and atr_count > 0:
            atr_combos = _optimize_atr_sl_tp(outcomes)

        atr_recommended = None
        if atr_combos:
            atr_realistic = [c for c in atr_combos if c["sl_avg_pips"] >= _MIN_SL]

            # Find recommended ATR strategy: WR >= 60%, best E[R]
            atr_rec_pool = [c for c in atr_realistic if c["wr"] >= 60 and c["total_pnl"] > 0]
            if atr_rec_pool:
                atr_rec_pool.sort(key=lambda x: x["expectancy_r"], reverse=True)
                atr_recommended = atr_rec_pool[0]

            if atr_realistic:
                parts.append('<h2>ATR-BASED SL/TP OPTIMIZATION</h2>')
                parts.append(f'<p>Trades with ATR data: <b>{atr_count}</b> / {n} &nbsp;|&nbsp; '
                             f'SL/TP normalized to each pair\'s H1 ATR(14) at entry time</p>')

                atr_sections = [
                    ("[REC] Recommended (WR\u226560%, best E[R])", sorted(
                        [c for c in atr_realistic if c["wr"] >= 60 and c["total_pnl"] > 0],
                        key=lambda x: x["expectancy_r"], reverse=True)[:5]),
                    ("[REC] Sniper (WR\u226580%)", sorted(
                        [c for c in atr_realistic if c["wr"] >= 80 and c["total_pnl"] > 0],
                        key=lambda x: x["expectancy_r"], reverse=True)[:5]),
                    ("[TOP] Best Total Profit", sorted(
                        atr_realistic, key=lambda x: x["total_pnl"], reverse=True)[:5]),
                    ("[PF] Best Risk-Adjusted (PF)", sorted(
                        [c for c in atr_realistic if c["pf"] < 900 and c["total_pnl"] > 0],
                        key=lambda x: x["pf"] * x["trades"] ** 0.5,
                        reverse=True)[:5]),
                ]

                for title, combos in atr_sections:
                    if not combos:
                        continue
                    top = combos[0]
                    parts.append(
                        f'<h3>{title} <span class="r-total">&nbsp;(top: {_clr(top["total_r"], "+.1f")}R total)</span></h3>'
                    )
                    parts.append(f"<table>{_atr_combo_header()}")
                    for c in combos:
                        parts.append(_atr_combo_row(c))
                    parts.append("</table>")

        # ── Session Ranking ──
        by_session: dict[str, list[AlertOutcome]] = defaultdict(list)
        for o in outcomes:
            by_session[o.session or "Unknown"].append(o)

        if by_session:
            parts.append("<h2>Session Ranking</h2><table>")
            parts.append("<tr><th>#</th><th>Session</th><th>Trades</th><th>Win%</th><th>Avg MFE</th><th>Avg Final</th></tr>")
            session_rows = []
            for sess, so in by_session.items():
                sn = len(so)
                sw = sum(1 for o in so if o.final_pnl_pips > 0)
                s_wr = sw / sn * 100 if sn else 0
                s_mfe = sum(o.mfe_pips for o in so) / sn if sn else 0
                s_final = sum(o.final_pnl_pips for o in so) / sn if sn else 0
                session_rows.append((sess, sn, s_wr, s_mfe, s_final))
            session_rows.sort(key=lambda x: x[4], reverse=True)
            for rank, (sess, sn, s_wr, s_mfe, s_final) in enumerate(session_rows, 1):
                parts.append(
                    f'<tr><td class="rank">{rank}</td><td>{sess}</td><td>{sn}</td>'
                    f'<td>{s_wr:.0f}%</td><td>{_clr(s_mfe, "+.1f")}</td><td>{_clr(s_final, "+.1f")}</td></tr>'
                )
            parts.append("</table>")

        # ── Pair Ranking ──
        by_pair: dict[str, list[AlertOutcome]] = defaultdict(list)
        for o in outcomes:
            by_pair[o.pair].append(o)

        if by_pair:
            parts.append("<h2>Pair Ranking</h2><table>")
            parts.append("<tr><th>#</th><th>Pair</th><th>Trades</th><th>Win%</th><th>Avg MFE</th><th>Avg MAE</th><th>Avg Final</th></tr>")
            pair_rows = []
            for pair_name, po in by_pair.items():
                pn = len(po)
                pw = sum(1 for o in po if o.final_pnl_pips > 0)
                p_wr = pw / pn * 100 if pn else 0
                p_mfe = sum(o.mfe_pips for o in po) / pn if pn else 0
                p_mae = sum(o.mae_pips for o in po) / pn if pn else 0
                p_final = sum(o.final_pnl_pips for o in po) / pn if pn else 0
                pair_rows.append((pair_name, pn, p_wr, p_mfe, p_mae, p_final))
            pair_rows.sort(key=lambda x: x[5], reverse=True)
            for rank, (pair_name, pn, p_wr, p_mfe, p_mae, p_final) in enumerate(pair_rows, 1):
                parts.append(
                    f'<tr><td class="rank">{rank}</td><td>{pair_name}</td><td>{pn}</td>'
                    f'<td>{p_wr:.0f}%</td><td>{_clr(p_mfe, "+.1f")}</td>'
                    f'<td>{_clr(-p_mae, "+.1f")}</td><td>{_clr(p_final, "+.1f")}</td></tr>'
                )
            parts.append("</table>")

        # ── Pair × Session Ranking ──
        pair_session: dict[tuple[str, str], list[AlertOutcome]] = defaultdict(list)
        for o in outcomes:
            pair_session[(o.pair, o.session or "Unknown")].append(o)

        ps_rows_display = []
        for (pair_name, sess), pso in pair_session.items():
            psn = len(pso)
            if psn < 2:
                continue
            psw = sum(1 for o in pso if o.final_pnl_pips > 0)
            ps_wr = psw / psn * 100 if psn else 0
            ps_mfe = sum(o.mfe_pips for o in pso) / psn if psn else 0
            ps_final = sum(o.final_pnl_pips for o in pso) / psn if psn else 0
            ps_rows_display.append((pair_name, sess, psn, ps_wr, ps_mfe, ps_final))

        if ps_rows_display:
            ps_rows_display.sort(key=lambda x: x[5], reverse=True)
            parts.append("<h2>Pair \u00d7 Session Ranking (min 2 trades)</h2><table>")
            parts.append("<tr><th>#</th><th>Pair</th><th>Session</th><th>Trades</th><th>Win%</th><th>Avg MFE</th><th>Avg Final</th></tr>")
            for rank, (pair_name, sess, psn, ps_wr, ps_mfe, ps_final) in enumerate(ps_rows_display[:25], 1):
                parts.append(
                    f'<tr><td class="rank">{rank}</td><td>{pair_name}</td><td>{sess}</td><td>{psn}</td>'
                    f'<td>{ps_wr:.0f}%</td><td>{_clr(ps_mfe, "+.1f")}</td><td>{_clr(ps_final, "+.1f")}</td></tr>'
                )
            parts.append("</table>")

        # ── OPTIMAL SL/TP PROFILE ──
        if profile is None:
            profile = _build_sltp_profile(outcomes)
            self._sltp_profile = profile

        profile_rows: list[tuple[str, str, dict]] = []
        for pair_name, sessions in profile.items():
            for sess, best in sessions.items():
                profile_rows.append((pair_name, sess, best))

        if profile_rows:
            profile_rows.sort(key=lambda x: x[2]["expectancy_r"], reverse=True)

            total_trades = sum(b["trades"] for _, _, b in profile_rows)
            total_r = sum(b["expectancy_r"] * b["trades"] for _, _, b in profile_rows)
            avg_wr = sum(b["wr"] * b["trades"] for _, _, b in profile_rows) / total_trades if total_trades else 0
            avg_exp = total_r / total_trades if total_trades else 0

            parts.append('<div class="profile-section">')
            parts.append(f'<h2>OPTIMAL SL/TP PROFILE (per Pair \u00d7 Session)</h2>')
            parts.append(f'<p>Criteria: WR \u2265 65% \u2192 best expectancy in R<br>')
            parts.append(
                f'<b>{len(profile_rows)}</b> combos &nbsp;|&nbsp; '
                f'<b>{total_trades}</b> total trades &nbsp;|&nbsp; '
                f'Weighted WR: <b>{avg_wr:.1f}%</b> &nbsp;|&nbsp; '
                f'Weighted E[R]: <b>{_clr(avg_exp, "+.2f")}R</b> &nbsp;|&nbsp; '
                f'<span class="r-total">Overall: {_clr(total_r, "+.1f")}R</span></p>'
            )
            parts.append("<table>")
            parts.append(
                "<tr><th>#</th><th>Pair</th><th>Session</th>"
                "<th>SL</th><th>TP</th><th>WR%</th><th>Trades</th><th>PF</th>"
                "<th>E[R]</th><th>MaxDD</th><th>ConsL</th><th>TotalR</th>"
                "<th>TP hit</th><th>SL hit</th><th>Signal</th>"
                '<th colspan="4" style="background:#7b3f00;text-align:center;">── Strict ──</th></tr>'
                "<tr><th></th><th></th><th></th><th></th><th></th><th></th><th></th>"
                "<th></th><th></th><th></th><th></th><th></th><th></th><th></th><th></th>"
                "<th>WR%</th><th>E[R]</th><th>TP</th><th>SL</th></tr>"
            )
            for rank, (pair_name, sess, b) in enumerate(profile_rows, 1):
                total_r_row = b["total_pnl"] / b["sl"] if b["sl"] > 0 else 0
                parts.append(
                    f'<tr><td class="rank">{rank}</td><td>{pair_name}</td><td>{sess}</td>'
                    f'<td>{b["sl"]:.1f}</td><td>{b["tp"]:.1f}</td>'
                    f'<td>{b["wr"]:.0f}%</td><td>{b["trades"]}</td>'
                    f'<td>{_pf_str(b["pf"])}</td>'
                    f'<td>{_clr(b["expectancy_r"], "+.2f")}R</td>'
                    f'<td>{b["max_dd_r"]:.1f}R</td>'
                    f'<td>{b["max_consec_loss"]}</td>'
                    f'<td>{_clr(total_r_row, "+.1f")}R</td>'
                    f'{_exit_breakdown(b)}'
                    f'{_strict_cols(b)}</tr>'
                )
            parts.append("</table>")
            parts.append('<p>\u2192 Press <b>"Save SL/TP Profile"</b> to export to optimal_sltp.json</p>')
            parts.append("</div>")
        else:
            parts.append('<div class="profile-section"><p>No pair+session combos with enough data (min 2 trades)</p></div>')
            total_r = 0.0
            total_trades = 0
            profile_rows = []

        # ── ATR-BASED OPTIMAL SL/TP PROFILE ──
        if atr_profile is None and atr_count > 0:
            atr_profile = _build_atr_sltp_profile(outcomes)
            self._atr_profile = atr_profile

        atr_profile_rows: list[tuple[str, str, dict]] = []
        if atr_profile:
            for pair_name, sessions in atr_profile.items():
                for sess, best in sessions.items():
                    atr_profile_rows.append((pair_name, sess, best))

        if atr_profile_rows:
            atr_profile_rows.sort(key=lambda x: x[2]["expectancy_r"], reverse=True)

            atr_total_trades = sum(b["trades"] for _, _, b in atr_profile_rows)
            atr_total_r = sum(b["expectancy_r"] * b["trades"] for _, _, b in atr_profile_rows)
            atr_avg_wr = sum(b["wr"] * b["trades"] for _, _, b in atr_profile_rows) / atr_total_trades if atr_total_trades else 0
            atr_avg_exp = atr_total_r / atr_total_trades if atr_total_trades else 0

            parts.append('<div class="profile-section">')
            parts.append(f'<h2>ATR-BASED OPTIMAL SL/TP PROFILE (per Pair \u00d7 Session)</h2>')
            parts.append(f'<p>Criteria: WR \u2265 65% \u2192 best expectancy in R &nbsp;|&nbsp; SL/TP adapted to each pair\'s volatility<br>')
            parts.append(
                f'<b>{len(atr_profile_rows)}</b> combos &nbsp;|&nbsp; '
                f'<b>{atr_total_trades}</b> total trades &nbsp;|&nbsp; '
                f'Weighted WR: <b>{atr_avg_wr:.1f}%</b> &nbsp;|&nbsp; '
                f'Weighted E[R]: <b>{_clr(atr_avg_exp, "+.2f")}R</b> &nbsp;|&nbsp; '
                f'<span class="r-total">Overall: {_clr(atr_total_r, "+.1f")}R</span></p>'
            )
            parts.append("<table>")
            parts.append(
                "<tr><th>#</th><th>Pair</th><th>Session</th>"
                "<th>SL (ATR)</th><th>TP (ATR)</th><th>~SL</th><th>~TP</th>"
                "<th>WR%</th><th>Trades</th><th>PF</th>"
                "<th>E[R]</th><th>MaxDD</th><th>ConsL</th><th>TotalR</th>"
                "<th>TP hit</th><th>SL hit</th><th>Signal</th>"
                '<th colspan="4" style="background:#7b3f00;text-align:center;">── Strict ──</th></tr>'
                "<tr><th></th><th></th><th></th><th></th><th></th><th></th><th></th>"
                "<th></th><th></th><th></th><th></th><th></th><th></th><th></th>"
                "<th></th><th></th><th></th>"
                "<th>WR%</th><th>E[R]</th><th>TP</th><th>SL</th></tr>"
            )
            for rank, (pair_name, sess, b) in enumerate(atr_profile_rows, 1):
                parts.append(
                    f'<tr><td class="rank">{rank}</td><td>{pair_name}</td><td>{sess}</td>'
                    f'<td>{b["sl_mult"]:.1f}</td><td>{b["tp_mult"]:.1f}</td>'
                    f'<td>{b["sl_avg_pips"]:.1f}</td><td>{b["tp_avg_pips"]:.1f}</td>'
                    f'<td>{b["wr"]:.0f}%</td><td>{b["trades"]}</td>'
                    f'<td>{_pf_str(b["pf"])}</td>'
                    f'<td>{_clr(b["expectancy_r"], "+.2f")}R</td>'
                    f'<td>{b["max_dd_r"]:.1f}R</td>'
                    f'<td>{b["max_consec_loss"]}</td>'
                    f'<td>{_clr(b["total_r"], "+.1f")}R</td>'
                    f'{_exit_breakdown(b)}'
                    f'{_strict_cols(b)}</tr>'
                )
            parts.append("</table>")
            parts.append("</div>")

        # ── EARNINGS PROJECTION ──
        capital = self._capital_spin.value()
        risk_pct = self._risk_spin.value() / 100.0

        def _fmt_money(v: float) -> str:
            av = abs(v)
            sign = "+" if v > 0 else "-" if v < 0 else ""
            if av >= 1e9:   return f'{sign}${av/1e9:,.1f}B'
            if av >= 1e6:   return f'{sign}${av/1e6:,.1f}M'
            if av >= 1e4:   return f'{sign}${av/1e3:,.1f}K'
            return f'{sign}${av:,.0f}'

        def _clr_money(v: float) -> str:
            s = _fmt_money(v)
            if v > 0: return f'<span class="good">{s}</span>'
            if v < 0: return f'<span class="bad">{s}</span>'
            return f'<span class="neutral">{s}</span>'

        def _calc_projection(s_wr, s_trades, s_total_r, rr, s_exp_r):
            """Calculate fixed and compounding projections."""
            import math
            risk_per_trade = capital * risk_pct
            fixed_profit = s_total_r * risk_per_trade
            fixed_final = capital + fixed_profit
            fixed_roi = (fixed_profit / capital) * 100

            win_mult = 1.0 + risk_pct * rr
            loss_mult = max(1.0 - risk_pct, 0.001)
            wr_frac = max(0.001, min(0.999, s_wr / 100.0))
            log_g = wr_frac * math.log(win_mult) + (1 - wr_frac) * math.log(loss_mult)
            total_log = max(-35, min(35, s_trades * log_g))
            comp_cap = capital * math.exp(total_log)
            comp_profit = comp_cap - capital
            comp_roi = (comp_profit / capital) * 100
            growth_per_trade = (math.exp(log_g) - 1) * 100
            return {
                "fixed_profit": fixed_profit, "fixed_final": fixed_final,
                "fixed_roi": fixed_roi, "comp_profit": comp_profit,
                "comp_cap": comp_cap, "comp_roi": comp_roi,
                "growth_per_trade": growth_per_trade, "total_r": s_total_r,
            }

        def _project_strategy(strat: dict, label: str) -> str:
            """Build earnings projection HTML for a single strategy (hybrid + strict)."""
            import math
            try:
                sl = strat["sl"]; tp = strat["tp"]
                s_wr = strat["wr"]; s_trades = strat["trades"]
                s_total_r = strat["total_pnl"] / sl if sl > 0 else 0
                s_exp_r = strat["expectancy_r"]

                if s_trades <= 0 or sl <= 0 or tp <= 0:
                    return f"<h3>{label}: insufficient data</h3>"

                rr = tp / sl
                h = _calc_projection(s_wr, s_trades, s_total_r, rr, s_exp_r)

                # Strict mode projection
                strict_wr = strat.get("strict_wr", s_wr)
                strict_total_r = strat.get("strict_total_r", strat.get("strict_total_pnl", 0) / sl if sl > 0 else 0)
                strict_exp_r = strat.get("strict_expectancy_r", s_exp_r)
                s = _calc_projection(strict_wr, s_trades, strict_total_r, rr, strict_exp_r)

            except Exception as ex:
                return f"<h3>{label}: projection error ({ex})</h3>"

            trading_days = max(1, days * 5 / 7)
            weeks = max(1, days / 7)
            months = max(1, days / 30.44)
            trades_per_day = s_trades / trading_days if trading_days > 0 else 0

            def _comp_roi_str(roi):
                rs = f"{roi:+,.0f}%" if roi < 1e7 else f"+{roi:.1e}%"
                return f'<span class="good">{rs}</span>' if roi > 0 else f'<span class="bad">{rs}</span>'

            if "sl_mult" in strat:
                sltp_str = (
                    f"SL {strat['sl_mult']:.1f} ATR (~{sl:.0f} pips avg) / "
                    f"TP {strat['tp_mult']:.1f} ATR (~{tp:.0f} pips avg)"
                )
            else:
                sltp_str = f"SL {sl:.0f} / TP {tp:.0f}"

            return f"""
            <h3>{label}: {sltp_str} &nbsp;|&nbsp; {s_trades} trades
                &nbsp;|&nbsp; R:R {rr:.1f} &nbsp;|&nbsp; ~{trades_per_day:.1f} trades/day</h3>
            <table>
            <tr><th></th>
                <th colspan="2" style="background:#2c6e49;text-align:center;">── Hybrid (WR {s_wr:.0f}%, E[R] {s_exp_r:+.2f}R) ──</th>
                <th colspan="2" style="background:#7b3f00;text-align:center;">── Strict (WR {strict_wr:.0f}%, E[R] {strict_exp_r:+.2f}R) ──</th></tr>
            <tr><th></th><th>Fixed Risk</th><th>Compounding</th><th>Fixed Risk</th><th>Compounding</th></tr>
            <tr><td><b>Total profit</b></td>
                <td>{_clr_money(h['fixed_profit'])}</td>
                <td>{_clr_money(h['comp_profit'])}</td>
                <td>{_clr_money(s['fixed_profit'])}</td>
                <td>{_clr_money(s['comp_profit'])}</td></tr>
            <tr><td><b>Final capital</b></td>
                <td><span class="big-number">{_fmt_money(h['fixed_final'])}</span></td>
                <td><span class="big-number">{_fmt_money(h['comp_cap'])}</span></td>
                <td><span class="big-number">{_fmt_money(s['fixed_final'])}</span></td>
                <td><span class="big-number">{_fmt_money(s['comp_cap'])}</span></td></tr>
            <tr><td><b>ROI</b></td>
                <td>{_clr(h['fixed_roi'], '+.1f')}%</td>
                <td>{_comp_roi_str(h['comp_roi'])}</td>
                <td>{_clr(s['fixed_roi'], '+.1f')}%</td>
                <td>{_comp_roi_str(s['comp_roi'])}</td></tr>
            <tr><td><b>Monthly avg</b></td>
                <td>{_clr_money(h['fixed_profit'] / months)}</td>
                <td>{_clr_money(h['comp_profit'] / months)}</td>
                <td>{_clr_money(s['fixed_profit'] / months)}</td>
                <td>{_clr_money(s['comp_profit'] / months)}</td></tr>
            <tr><td><b>Stats</b></td>
                <td colspan="2">{_clr(h['total_r'], '+.1f')}R total &nbsp;|&nbsp;
                    MaxDD {strat['max_dd_r']:.1f}R &nbsp;|&nbsp;
                    ConsL: {strat['max_consec_loss']}</td>
                <td colspan="2">{_clr(s['total_r'], '+.1f')}R total &nbsp;|&nbsp;
                    MaxDD {strat.get('strict_max_dd_r', strat['max_dd_r']):.1f}R &nbsp;|&nbsp;
                    ConsL: {strat.get('strict_max_consec_loss', strat['max_consec_loss'])}</td></tr>
            <tr><td><b>Growth/trade</b></td>
                <td colspan="2">{_clr(h['growth_per_trade'], '+.2f')}% (geometric mean)</td>
                <td colspan="2">{_clr(s['growth_per_trade'], '+.2f')}% (geometric mean)</td></tr>
            </table>
            """

        # Build projection strategies — ATR-based as PRIMARY, fixed-pip as secondary
        projection_strategies: list[tuple[str, dict]] = []

        # ATR-based projections (PRIMARY)
        if atr_recommended:
            atr_label = (
                f"[REC] ATR Recommended (WR\u226560%) &mdash; "
                f"SL {atr_recommended['sl_mult']:.1f} ATR (~{atr_recommended['sl_avg_pips']:.1f} pips avg) / "
                f"TP {atr_recommended['tp_mult']:.1f} ATR (~{atr_recommended['tp_avg_pips']:.1f} pips avg)"
            )
            projection_strategies.append((atr_label, atr_recommended))

        if atr_combos:
            atr_sniper_pool = [c for c in atr_combos if c["wr"] >= 80 and c["total_pnl"] > 0 and c["sl_avg_pips"] >= _MIN_SL]
            if atr_sniper_pool:
                atr_sniper_pool.sort(key=lambda x: x["expectancy_r"], reverse=True)
                atr_snp = atr_sniper_pool[0]
                atr_snp_label = (
                    f"[REC] ATR Sniper (WR\u226580%) &mdash; "
                    f"SL {atr_snp['sl_mult']:.1f} ATR (~{atr_snp['sl_avg_pips']:.1f} pips) / "
                    f"TP {atr_snp['tp_mult']:.1f} ATR (~{atr_snp['tp_avg_pips']:.1f} pips)"
                )
                projection_strategies.append((atr_snp_label, atr_snp))

        # Fixed-pip projections (SECONDARY)
        if recommended:
            projection_strategies.append(("[TOP] Fixed-Pip Recommended (WR\u226560%)", recommended))

        sniper_pool = [c for c in realistic_combos if c["wr"] >= 80 and c["total_pnl"] > 0]
        if sniper_pool:
            sniper_pool.sort(key=lambda x: x["expectancy_r"], reverse=True)
            projection_strategies.append(("[TOP] Fixed-Pip Sniper (WR\u226580%)", sniper_pool[0]))

        # Also add best total profit strategy
        profit_pool = [c for c in realistic_combos if c["total_pnl"] > 0]
        if profit_pool:
            profit_pool.sort(key=lambda x: x["total_pnl"], reverse=True)
            best_profit = profit_pool[0]
            if not recommended or (best_profit["sl"] != recommended["sl"] or best_profit["tp"] != recommended["tp"]):
                projection_strategies.append(("[TOP] Fixed-Pip Max Profit", best_profit))

        if projection_strategies and days > 0:
            parts.append('<div class="earnings-section">')
            parts.append('<h2>EARNINGS PROJECTION</h2>')
            parts.append(
                f'<p>Starting capital: <b>${capital:,.0f}</b> &nbsp;|&nbsp; '
                f'Risk per trade: <b>{risk_pct*100:.1f}%</b> (${capital * risk_pct:,.0f}) &nbsp;|&nbsp; '
                f'Period: <b>{days} days</b></p>'
            )

            if atr_recommended:
                parts.append(
                    '<p><b>PRIMARY:</b> ATR-based strategies (volatility-normalized, adapts SL/TP to each pair)<br>'
                    '<b>SECONDARY:</b> Fixed-pip strategies (same SL/TP for all pairs)</p>'
                )

            for label, strat in projection_strategies:
                parts.append(_project_strategy(strat, label))

            parts.append(
                '<p><i>\u26a0 Compounding uses geometric mean (order-independent, mathematically exact). '
                'Real results depend on maintaining consistent risk % and the strategy '
                'performing similarly forward. Slippage and spreads not included.</i></p>'
            )
            parts.append("</div>")

        parts.append("</body>")
        return "".join(parts)

    # ── SL/TP optimizer (delegates to module-level functions) ────

    def _save_sltp_profile(self) -> None:
        """Save both fixed-pip and ATR-based optimal SL/TP profiles to JSON."""
        import sys
        if getattr(sys, 'frozen', False):
            data_dir = Path(sys.executable).parent / "data"
        else:
            data_dir = Path(__file__).resolve().parent.parent.parent / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        if not self._sltp_profile and not self._atr_profile:
            self._status_label.setText("No profile to save \u2014 run backtest first")
            return

        saved = []
        if self._sltp_profile:
            out_file = data_dir / "optimal_sltp.json"
            out_file.write_text(
                json.dumps(self._sltp_profile, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            combos = sum(len(sessions) for sessions in self._sltp_profile.values())
            saved.append(f"{combos} fixed-pip profiles to {out_file.name}")

        if self._atr_profile:
            atr_file = data_dir / "optimal_sltp_atr.json"
            atr_file.write_text(
                json.dumps(self._atr_profile, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            atr_combos = sum(len(sessions) for sessions in self._atr_profile.values())
            saved.append(f"{atr_combos} ATR profiles to {atr_file.name}")

        self._status_label.setText(f"Saved: {' | '.join(saved)}")

    # ── View in Performance Dialog ───────────────────────────────

    def _view_performance(self) -> None:
        """Save backtest results and open the Performance dialog on the Backtest tab."""
        import sys
        if getattr(sys, 'frozen', False):
            data_dir = Path(sys.executable).parent / "data"
        else:
            data_dir = Path(__file__).resolve().parent.parent.parent / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        bt_file = data_dir / "backtest_outcomes.json"
        bt_file.write_text(
            json.dumps([asdict(o) for o in self._results], indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        from takumi_trader.ui.performance_dialog import PerformanceDialog
        live_file = data_dir / "alert_outcomes.json"
        dlg = PerformanceDialog(
            None,
            outcomes_file=live_file if live_file.exists() else None,
            active_count=0,
            backtest_file=bt_file,
        )
        dlg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        dlg._tabs.setCurrentIndex(1)
        dlg.show()
        self._perf_dialog = dlg
