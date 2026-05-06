"""Live Candle CSI systems dialog — shows performance of the 5 "-live"
paper systems that use the live-candle engine (computes on every worker
cycle with forming bars) instead of the candle-close engine used by A-E.

Systems displayed (mirrors of A-E):
  • Sv2-live          — mirrors System A (Sv2)
  • Sv2-Tun-live      — mirrors System D (A-tuned)
  • Sv2+SS-live       — mirrors System B (SS)
  • Sv2+SS-Tun-live   — mirrors System E (B-tuned)
  • Sv2+ATR-live      — mirrors System C (ATR)

Plus the AU Gold suite (XAUUSD-only paper systems, added 2026-04-24):
  • AU1 London        — Asian range → London H1 breakout
  • AU2 NY-ORB        — NY 09:30 NY-local 2-min Open Range Breakout
  • AU3 Pullback      — H1 pullback inside H4 trend regime
  • AU4 USD-Div       — XAUUSD vs USD-strength correlation break
  • AU5 MeanRev       — Asian-session RSI mean-reversion

After a few days of running, compare the live-systems' WR and P/L to their
non-live counterparts in the Alert Performance (BackT) dialog to determine
whether real-time (live) signal computation outperforms candle-close-only.

Implementation: thin subclass of PerformanceDialog with
`include_standard_tabs=False`. This gives the 5 live tabs the SAME rich
layout (pair filter, HTML summary, equity curve, full sortable trades table
with all MAE/MFE/context columns) as the standard Alert Performance tabs.
"""
from __future__ import annotations

from pathlib import Path

from PyQt6.QtWidgets import QWidget

from takumi_trader.ui.performance_dialog import PerformanceDialog


class LiveCandleDialog(PerformanceDialog):
    """Displays the 5 "-live" paper systems using the same layout as the
    Alert Performance dialog, but with only the 5 live tabs."""

    # Separate QSettings scope so geometry/splitters/active-tab don't
    # collide with the main Alert Performance dialog's saved state.
    _SETTINGS_KEY = "LiveCandleDialog"

    def __init__(
        self,
        parent: QWidget | None = None,
        *,
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
        # AU Gold suite (2026-04-24) — XAUUSD only
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
    ) -> None:
        super().__init__(
            parent=parent,
            include_standard_tabs=False,
            sv2_live_trades_file=sv2_live_trades_file,
            sv2_live_paper_trader=sv2_live_paper_trader,
            sv2_a_tuned_live_trades_file=sv2_a_tuned_live_trades_file,
            sv2_a_tuned_live_paper_trader=sv2_a_tuned_live_paper_trader,
            sv2_ss_live_trades_file=sv2_ss_live_trades_file,
            sv2_ss_live_paper_trader=sv2_ss_live_paper_trader,
            sv2_b_tuned_live_trades_file=sv2_b_tuned_live_trades_file,
            sv2_b_tuned_live_paper_trader=sv2_b_tuned_live_paper_trader,
            sv2_atr_live_trades_file=sv2_atr_live_trades_file,
            sv2_atr_live_paper_trader=sv2_atr_live_paper_trader,
            # AU Gold suite (2026-04-24)
            au1_trades_file=au1_trades_file,
            au1_paper_trader=au1_paper_trader,
            au2_trades_file=au2_trades_file,
            au2_paper_trader=au2_paper_trader,
            au3_trades_file=au3_trades_file,
            au3_paper_trader=au3_paper_trader,
            au4_trades_file=au4_trades_file,
            au4_paper_trader=au4_paper_trader,
            au5_trades_file=au5_trades_file,
            au5_paper_trader=au5_paper_trader,
        )
        self.setWindowTitle("Live Candle CSI systems + AU Gold")
