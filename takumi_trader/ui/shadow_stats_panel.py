"""ShadowStatsPanel — read-only observability for shadow logging.

Phase E adds operational visibility into the shadow logging
infrastructure (Phases A–D) so the operator can see capture rates,
gate distribution, recent calibrations, and worker health without
grepping logs or reading JSON files.

E.1 ships sections 1 + 4 only:
    Section 1: Today's capture summary (counts + filter value-add)
E.2 will add sections 2 (gate distribution) + 3 (recent calibrations).
E.3 will add section 4 (worker health + heartbeat).
E.4 integrates the panel into PerformanceDialog's Sv2 tab.

Architectural principles (per the Phase E spec):
    1. Read-only — never mutates shadow records, simulator, or worker.
    2. No new threads — refreshes from existing ShadowSimWorker signals
       plus a 30s pull timer for time-elapsed displays.
    3. Defensive — missing/malformed data shows "no data yet" rather
       than crashing. Panel failures must not affect trading.
    4. mtime-cached disk reads — only re-parse JSON when files change.
       Parsing 40K+ records every 30s would burn cycles for nothing.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from PyQt6.QtCore import QTimer, Qt
from PyQt6.QtWidgets import (
    QFrame, QLabel, QVBoxLayout, QWidget,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# "Today" semantics
# ─────────────────────────────────────────────────────────────────────

def today_start_utc() -> float:
    """Epoch seconds for the most recent UTC midnight.

    UTC midnight is the simplest "today" anchor and matches the
    Phase E spec example. If a JST-anchored variant is needed later
    (Sv2 trades on a JST schedule), this can be swapped without
    changing call sites.
    """
    now = time.time()
    return now - (now % 86400.0)


# ─────────────────────────────────────────────────────────────────────
# ShadowStatsPanel
# ─────────────────────────────────────────────────────────────────────

class ShadowStatsPanel(QWidget):
    """Read-only observability panel for shadow logging infrastructure.

    Displays today's capture summary (E.1), gate distribution (E.2),
    recent calibrations (E.2), and worker health (E.3). Updates from
    ShadowSimWorker signals and a 30-second pull timer.

    Read-only by design — never mutates shadow data or worker state.
    Construction with `sim_worker=None` is allowed (for tests or when
    the worker failed to start); signal slots simply never fire.
    """

    def __init__(
        self,
        shadow_journal_path: Path,
        calibration_log_path: Path,
        sim_worker=None,                    # ShadowSimWorker or None
        paper_journal_path: Path | None = None,  # for real-PnL lookup
        parent: QWidget | None = None,
        refresh_interval_ms: int = 30_000,
    ) -> None:
        super().__init__(parent)

        self.journal_path = Path(shadow_journal_path)
        self.calibration_path = Path(calibration_log_path)
        self.paper_journal_path = (
            Path(paper_journal_path) if paper_journal_path else None
        )
        self.sim_worker = sim_worker

        # mtime-keyed parse cache. Avoid re-reading 40+ MB JSON when
        # the file hasn't changed between refreshes. Sentinel mtime=-1
        # forces first-call refresh.
        self._journal_cache: list[dict] = []
        self._journal_mtime: float = -1.0
        self._calibration_cache: list[dict] = []
        self._calibration_mtime: float = -1.0
        self._paper_cache: list[dict] = []
        self._paper_mtime: float = -1.0

        # Push updates from worker (if wired)
        if self.sim_worker is not None:
            try:
                self.sim_worker.cycle_complete.connect(self._on_cycle_complete)
                self.sim_worker.drift_warning.connect(self._on_drift_warning)
            except Exception as exc:
                logger.warning(
                    "[SHADOW PANEL] failed to connect worker signals: %s", exc,
                )

        # Pull timer for time-elapsed displays + journal mtime check
        self._refresh_timer = QTimer(self)
        self._refresh_timer.timeout.connect(self.refresh)
        if refresh_interval_ms > 0:
            self._refresh_timer.start(refresh_interval_ms)

        self._build_ui()
        self.refresh()

    # ── UI construction ─────────────────────────────────────────────

    def _build_ui(self) -> None:
        """Build the panel's static structure. Section content is
        populated by refresh()."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 6, 8, 6)
        layout.setSpacing(8)

        self._capture_section = self._build_capture_section()
        layout.addWidget(self._capture_section)

        # E.2 will add: gate distribution, recent calibrations
        # E.3 will add: worker health
        # Placeholder spacer for future sections
        layout.addStretch(1)

    def _build_capture_section(self) -> QFrame:
        """Section 1: Today's capture summary.

        Compact 8-9 line block showing total signals, strength-reject
        / strength-pass split, executed / filtered split, and the
        filter-value-add diagnostic (real expectancy minus shadow
        expectancy on filtered trades).
        """
        frame = QFrame()
        frame.setFrameShape(QFrame.Shape.StyledPanel)
        frame.setStyleSheet(
            "QFrame { background: #f7f7f7; border: 1px solid #ddd; "
            "border-radius: 4px; padding: 4px; }"
        )
        v = QVBoxLayout(frame)
        v.setContentsMargins(6, 4, 6, 4)
        v.setSpacing(2)

        header = QLabel("─── Today's Capture ───")
        header.setStyleSheet("font-weight: bold; color: #444;")
        v.addWidget(header)

        self._capture_body = QLabel("(loading…)")
        self._capture_body.setTextFormat(Qt.TextFormat.RichText)
        self._capture_body.setStyleSheet(
            "font-family: 'Consolas', 'Courier New', monospace; "
            "font-size: 10pt; color: #222;"
        )
        self._capture_body.setWordWrap(True)
        v.addWidget(self._capture_body)
        return frame

    # ── Refresh ─────────────────────────────────────────────────────

    def refresh(self) -> None:
        """Re-read mtime-changed files and rebuild section content.

        Safe to call from main thread at any time. Per-section
        rebuilds are wrapped in try/except so one bad section
        doesn't blank the others."""
        self._refresh_journal_cache()
        self._refresh_calibration_cache()
        self._refresh_paper_cache()
        try:
            self._render_capture_section()
        except Exception as exc:
            logger.warning("[SHADOW PANEL] capture section refresh raised: %s", exc)
            self._capture_body.setText("(refresh error — see logs)")

    # ── Disk read with mtime caching ────────────────────────────────

    def _refresh_journal_cache(self) -> None:
        self._journal_cache, self._journal_mtime = self._refresh_one_cache(
            self.journal_path, self._journal_cache, self._journal_mtime,
        )

    def _refresh_calibration_cache(self) -> None:
        self._calibration_cache, self._calibration_mtime = self._refresh_one_cache(
            self.calibration_path, self._calibration_cache, self._calibration_mtime,
        )

    def _refresh_paper_cache(self) -> None:
        if self.paper_journal_path is None:
            return
        self._paper_cache, self._paper_mtime = self._refresh_one_cache(
            self.paper_journal_path, self._paper_cache, self._paper_mtime,
        )

    @staticmethod
    def _refresh_one_cache(
        path: Path,
        prev_cache: list[dict],
        prev_mtime: float,
    ) -> tuple[list[dict], float]:
        """Read a JSON-list file if its mtime advanced. Otherwise
        return the previous cache. On read or parse failure, also
        return the previous cache (defensive: don't blank a working
        panel just because the file is being written by paper_trader
        right now)."""
        try:
            mtime = path.stat().st_mtime
        except OSError:
            return prev_cache, prev_mtime
        if mtime == prev_mtime and prev_cache:
            return prev_cache, prev_mtime
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return prev_cache, prev_mtime
        if not isinstance(data, list):
            return prev_cache, prev_mtime
        return data, mtime

    # ── Section 1: Today's Capture ──────────────────────────────────

    def _render_capture_section(self) -> None:
        """Compute today's capture summary and render to the body label."""
        stats = self._compute_capture_stats()
        if stats is None:
            self._capture_body.setText(
                "<i>No shadow data yet — waiting for first M5 close.</i>"
            )
            return

        n_total = stats["total"]
        n_reject = stats["strength_rejects"]
        n_pass = stats["strength_passes"]
        n_executed = stats["executed"]
        n_filtered = stats["filtered_downstream"]

        if n_total == 0:
            self._capture_body.setText(
                "<i>No signals captured yet today.</i>"
            )
            return

        reject_pct = 100.0 * n_reject / n_total if n_total else 0.0
        pass_pct = 100.0 * n_pass / n_total if n_total else 0.0
        filter_rate = (
            100.0 * n_filtered / n_pass if n_pass else 0.0
        )
        pass_through_rate = (
            100.0 * n_executed / n_pass if n_pass else 0.0
        )

        # Filter value-add: real expectancy of executed minus shadow
        # expectancy of filtered. None when either side is empty.
        real_exp = stats["real_expectancy"]
        shadow_exp = stats["shadow_expectancy"]
        n_real = stats["real_expectancy_n"]
        n_shadow = stats["shadow_expectancy_n"]
        value_add_html = self._format_value_add(real_exp, shadow_exp)

        rows = [
            f"  Total signals captured:  {n_total:>7,}",
            f"  Strength-rejects:        {n_reject:>7,} ({reject_pct:.1f}%)",
            f"  Strength-passes:         {n_pass:>7,} ({pass_pct:.1f}%)",
            f"    → Executed:            {n_executed:>7,} (filter pass-through "
            f"{pass_through_rate:.1f}%)",
            f"    → Filtered downstream: {n_filtered:>7,} ({filter_rate:.1f}% "
            f"filter rate)",
            "",
            f"  Real expectancy (n={n_real}):    "
            f"{self._format_pip(real_exp)}",
            f"  Shadow expectancy (n={n_shadow}): "
            f"{self._format_pip(shadow_exp)}",
            f"  Filter value-add:        {value_add_html}",
        ]
        # Render as <pre>-style monospace via HTML; the label is
        # already monospace-styled, so plain joined text works.
        self._capture_body.setText("<br>".join(
            row.replace(" ", "&nbsp;") if row else "&nbsp;" for row in rows
        ))

    def _compute_capture_stats(self) -> dict[str, Any] | None:
        """Aggregate today's shadow records into the capture summary
        shape consumed by _render_capture_section.

        Returns None only if the shadow journal is missing entirely.
        Returns a dict with zero counts if the journal is loaded but
        no records fall within today.
        """
        if not self._journal_cache:
            # Not yet loaded, or file missing
            try:
                if not self.journal_path.exists():
                    return None
            except OSError:
                return None

        anchor = today_start_utc()
        today_records = [
            r for r in self._journal_cache
            if isinstance(r, dict) and float(r.get("signal_time", 0.0)) >= anchor
        ]

        n_total = len(today_records)
        n_reject = sum(
            1 for r in today_records
            if r.get("block_gate", "") == "strength_engine"
        )
        n_pass = n_total - n_reject

        # Among strength-passes, split by status. EXECUTED records
        # are the filter pass-through; BLOCKED records (with a
        # downstream gate, not strength_engine) are filtered.
        n_executed = 0
        n_filtered = 0
        executed_records: list[dict] = []
        filtered_records: list[dict] = []
        for r in today_records:
            if r.get("block_gate", "") == "strength_engine":
                continue
            status = r.get("status", "")
            if status == "EXECUTED":
                n_executed += 1
                executed_records.append(r)
            elif status == "BLOCKED":
                n_filtered += 1
                filtered_records.append(r)
            # PENDING / FAILED don't count toward either bucket; they
            # are in-flight or unsimulatable.

        # Real expectancy: mean real PnL over today's executed, looked
        # up via journal_idx in paper_trades.json.
        real_pnls = self._collect_real_pnls(executed_records)
        # Shadow expectancy: mean sim PnL over today's filtered records
        # whose simulation completed.
        shadow_pnls = self._collect_shadow_pnls(filtered_records)

        return {
            "total": n_total,
            "strength_rejects": n_reject,
            "strength_passes": n_pass,
            "executed": n_executed,
            "filtered_downstream": n_filtered,
            "real_expectancy": (
                sum(real_pnls) / len(real_pnls) if real_pnls else None
            ),
            "real_expectancy_n": len(real_pnls),
            "shadow_expectancy": (
                sum(shadow_pnls) / len(shadow_pnls) if shadow_pnls else None
            ),
            "shadow_expectancy_n": len(shadow_pnls),
        }

    def _collect_real_pnls(self, executed_records: list[dict]) -> list[float]:
        """For each EXECUTED record, look up its real PnL in
        paper_trades.json via journal_idx. Skip records whose lookup
        fails (paper journal missing, idx out of bounds, pair
        mismatch, trade still open) — defensive same as
        make_paper_trade_lookup."""
        if not self._paper_cache:
            return []
        out: list[float] = []
        for r in executed_records:
            if r.get("exec_lane", "") != "paper":
                continue
            ref_json = r.get("exec_ref_json", "")
            if not ref_json:
                continue
            try:
                ref = json.loads(ref_json)
            except (json.JSONDecodeError, TypeError):
                continue
            idx = ref.get("journal_idx") if isinstance(ref, dict) else None
            if not isinstance(idx, int) or idx < 0 or idx >= len(self._paper_cache):
                continue
            entry = self._paper_cache[idx]
            if not isinstance(entry, dict):
                continue
            if entry.get("pair", "") != r.get("pair", ""):
                continue
            close_time = entry.get("close_time", 0.0)
            try:
                if float(close_time) <= 0.0:
                    continue
                out.append(float(entry.get("pnl_pips", 0.0)))
            except (TypeError, ValueError):
                continue
        return out

    @staticmethod
    def _collect_shadow_pnls(filtered_records: list[dict]) -> list[float]:
        """For each filtered (BLOCKED) strength-pass record, harvest
        sim_pnl_pips if simulation completed and didn't fail."""
        out: list[float] = []
        for r in filtered_records:
            if not r.get("sim_completed", False):
                continue
            if r.get("sim_exit_reason", "") in ("FAILED", ""):
                continue
            try:
                out.append(float(r.get("sim_pnl_pips", 0.0)))
            except (TypeError, ValueError):
                continue
        return out

    # ── Formatting helpers ──────────────────────────────────────────

    @staticmethod
    def _format_pip(value: float | None) -> str:
        if value is None:
            return "<i>n/a</i>"
        sign = "+" if value >= 0 else ""
        return f"{sign}{value:.2f}p"

    @staticmethod
    def _format_value_add(real_exp: float | None, shadow_exp: float | None) -> str:
        """Format filter-value-add (real - shadow) with a green check
        for positive (filters helping) or red X for negative (filters
        hurting). Returns 'n/a' if either side is missing."""
        if real_exp is None or shadow_exp is None:
            return "<i>n/a</i> (need both real + shadow data)"
        diff = real_exp - shadow_exp
        if diff >= 0:
            sign = "+"
            color = "#2e7d32"
            mark = "&#10003;"  # check
        else:
            sign = ""
            color = "#c62828"
            mark = "&#10007;"  # cross
        return (
            f"<b style='color:{color}'>{sign}{diff:.2f}p/trade {mark}</b>"
        )

    # ── Worker signal slots ─────────────────────────────────────────

    def _on_cycle_complete(self, _stats: dict) -> None:
        """Worker cycled. Refresh on next event-loop tick rather than
        synchronously to avoid blocking the worker's emit() return."""
        QTimer.singleShot(0, self.refresh)

    def _on_drift_warning(self, _message: str) -> None:
        """E.2 will surface the drift banner inside the calibration
        section. For E.1 we just trigger a refresh so any newly-
        accumulated state is visible."""
        QTimer.singleShot(0, self.refresh)
