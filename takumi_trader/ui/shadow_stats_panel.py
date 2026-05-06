"""ShadowStatsPanel — read-only observability for shadow logging.

Phase E adds operational visibility into the shadow logging
infrastructure (Phases A–D) so the operator can see capture rates,
gate distribution, recent calibrations, and worker health without
grepping logs or reading JSON files.

E.1 shipped Section 1 (Today's Capture).
E.2 added Section 2 (Today's Gate Distribution) + Section 3 (Recent
    Calibrations with rolling-10 mean activation at n=10).
E.3 adds Section 4 (Worker Health + heartbeat-staleness detection).
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
# Gate distribution colors — stable per-gate so the bar chart's visual
# identity is recognizable across refreshes. Chosen for distinctness
# at small bar sizes; not part of any external contract.
# ─────────────────────────────────────────────────────────────────────

_GATE_COLORS: dict[str, str] = {
    "strength_engine":   "#9e9e9e",  # grey  — captured but excluded from "filter" view
    "divergence_spread": "#1976d2",  # blue
    "structural":        "#f57c00",  # orange
    "h1_sweep":          "#7b1fa2",  # purple
    "conviction":        "#388e3c",  # green
    "no_trade_window":   "#5d4037",  # brown
    "duplicate":         "#0097a7",  # teal
    "adr":               "#c2185b",  # pink
    "news":              "#fbc02d",  # yellow
    "internal":          "#616161",  # dark grey
}
_GATE_COLOR_DEFAULT = "#9c27b0"      # any unknown gate => violet


# ─────────────────────────────────────────────────────────────────────
# Calibration interpretation — matches the architect's framework
# stored in project_shadow_calibration_interpretation.md.
# ─────────────────────────────────────────────────────────────────────

_CAL_BAND_PIPS = 1.5            # drift detector warns at ±this
_CAL_BORDERLINE_PIPS = 3.0      # |delta| > this is "borderline" per architect
_CAL_ALARM_PIPS = 15.0          # |delta| > this is "investigate" per architect
_ROLLING_WINDOW = 10            # drift detector activation threshold


# ─────────────────────────────────────────────────────────────────────
# Worker heartbeat staleness — if no cycle_complete in this many seconds,
# something is wrong (poll_interval=300s, so 600s = two missed cycles).
# ─────────────────────────────────────────────────────────────────────

_HEARTBEAT_STALE_SECONDS = 600.0


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

        self._gate_section = self._build_gate_section()
        layout.addWidget(self._gate_section)

        self._calibration_section = self._build_calibration_section()
        layout.addWidget(self._calibration_section)

        self._worker_section = self._build_worker_section()
        layout.addWidget(self._worker_section)

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

    def _build_gate_section(self) -> QFrame:
        """Section 2: Today's gate distribution (horizontal bar chart)."""
        frame = QFrame()
        frame.setFrameShape(QFrame.Shape.StyledPanel)
        frame.setStyleSheet(
            "QFrame { background: #f7f7f7; border: 1px solid #ddd; "
            "border-radius: 4px; padding: 4px; }"
        )
        v = QVBoxLayout(frame)
        v.setContentsMargins(6, 4, 6, 4)
        v.setSpacing(2)

        header = QLabel("─── Today's Gate Distribution ───")
        header.setStyleSheet("font-weight: bold; color: #444;")
        v.addWidget(header)

        self._gate_body = QLabel("(loading…)")
        self._gate_body.setTextFormat(Qt.TextFormat.RichText)
        self._gate_body.setStyleSheet(
            "font-family: 'Consolas', 'Courier New', monospace; "
            "font-size: 10pt; color: #222;"
        )
        self._gate_body.setWordWrap(True)
        v.addWidget(self._gate_body)
        return frame

    def _build_worker_section(self) -> QFrame:
        """Section 4: Worker health (state, pending counts, heartbeat).

        Frame style mutates between healthy / stale heartbeat states
        (red border when stale)."""
        frame = QFrame()
        frame.setObjectName("worker_section")
        frame.setFrameShape(QFrame.Shape.StyledPanel)
        self._worker_frame_default_style = (
            "QFrame#worker_section { background: #f7f7f7; "
            "border: 1px solid #ddd; border-radius: 4px; padding: 4px; }"
        )
        frame.setStyleSheet(self._worker_frame_default_style)
        v = QVBoxLayout(frame)
        v.setContentsMargins(6, 4, 6, 4)
        v.setSpacing(2)

        self._worker_header = QLabel("─── Worker Health ───")
        self._worker_header.setStyleSheet("font-weight: bold; color: #444;")
        v.addWidget(self._worker_header)

        self._worker_body = QLabel("(loading…)")
        self._worker_body.setTextFormat(Qt.TextFormat.RichText)
        self._worker_body.setStyleSheet(
            "font-family: 'Consolas', 'Courier New', monospace; "
            "font-size: 10pt; color: #222;"
        )
        self._worker_body.setWordWrap(True)
        v.addWidget(self._worker_body)
        return frame

    def _build_calibration_section(self) -> QFrame:
        """Section 3: Recent calibrations + rolling-10 mean activation
        at n=10. The frame's stylesheet header is mutated by the drift
        warning slot when the rolling mean leaves the safe band."""
        frame = QFrame()
        frame.setObjectName("calibration_section")
        frame.setFrameShape(QFrame.Shape.StyledPanel)
        self._calibration_frame_default_style = (
            "QFrame#calibration_section { background: #f7f7f7; "
            "border: 1px solid #ddd; border-radius: 4px; padding: 4px; }"
        )
        frame.setStyleSheet(self._calibration_frame_default_style)
        v = QVBoxLayout(frame)
        v.setContentsMargins(6, 4, 6, 4)
        v.setSpacing(2)

        self._calibration_header = QLabel(
            "─── Recent Calibrations ───"
        )
        self._calibration_header.setStyleSheet("font-weight: bold; color: #444;")
        v.addWidget(self._calibration_header)

        self._calibration_body = QLabel("(loading…)")
        self._calibration_body.setTextFormat(Qt.TextFormat.RichText)
        self._calibration_body.setStyleSheet(
            "font-family: 'Consolas', 'Courier New', monospace; "
            "font-size: 10pt; color: #222;"
        )
        self._calibration_body.setWordWrap(True)
        v.addWidget(self._calibration_body)
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
        try:
            self._render_gate_section()
        except Exception as exc:
            logger.warning("[SHADOW PANEL] gate section refresh raised: %s", exc)
            self._gate_body.setText("(refresh error — see logs)")
        try:
            self._render_calibration_section()
        except Exception as exc:
            logger.warning("[SHADOW PANEL] calibration section refresh raised: %s", exc)
            self._calibration_body.setText("(refresh error — see logs)")
        try:
            self._render_worker_section()
        except Exception as exc:
            logger.warning("[SHADOW PANEL] worker section refresh raised: %s", exc)
            self._worker_body.setText("(refresh error — see logs)")

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

    # ── Section 2: Today's Gate Distribution ────────────────────────

    def _render_gate_section(self) -> None:
        """Render today's downstream-filter gate distribution as
        an HTML horizontal bar chart. Excludes strength_engine
        (those are already covered in Section 1's reject count)."""
        if not self._journal_cache:
            self._gate_body.setText("<i>No data yet.</i>")
            return

        anchor = today_start_utc()
        # Only include downstream gates (not strength_engine, not unblocked)
        counts: dict[str, int] = {}
        for r in self._journal_cache:
            if not isinstance(r, dict):
                continue
            if float(r.get("signal_time", 0.0)) < anchor:
                continue
            gate = r.get("block_gate", "")
            if not gate or gate == "strength_engine":
                continue
            counts[gate] = counts.get(gate, 0) + 1

        if not counts:
            self._gate_body.setText(
                "<i>No downstream filter rejections today.</i>"
            )
            return

        total = sum(counts.values())
        sorted_pairs = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)

        # Render rows as label + colored bar + count/percent
        rows: list[str] = []
        # Bar width budget — keep it visually compact and proportional
        # to terminal-style column lengths the operator is used to.
        max_bar_chars = 32
        max_count = max(counts.values())
        for gate, n in sorted_pairs:
            pct = 100.0 * n / total if total else 0.0
            color = _GATE_COLORS.get(gate, _GATE_COLOR_DEFAULT)
            # Bar width proportional to count vs the largest bucket
            bar_chars = int(round(max_bar_chars * n / max_count)) if max_count else 0
            bar_chars = max(1, bar_chars)
            # Render bar as a coloured span with non-breaking blocks
            bar_html = (
                f"<span style='background:{color};color:{color};'>"
                + ("&#9608;" * bar_chars)
                + "</span>"
            )
            label = f"{gate:<20s}"
            count_part = f"{n:>5} ({pct:>4.1f}%)"
            rows.append(
                f"&nbsp;&nbsp;{label.replace(' ', '&nbsp;')} "
                f"{bar_html} &nbsp;{count_part}"
            )
        self._gate_body.setText("<br>".join(rows))

    # ── Section 3: Recent Calibrations ──────────────────────────────

    def _render_calibration_section(self) -> None:
        """Render the most recent N calibration entries with delta
        column and arrow notation (real_exit -> sim_exit). When
        n >= _ROLLING_WINDOW, also render the rolling-window mean
        and apply a drift banner if it crosses ±_CAL_BAND_PIPS."""
        cal = self._calibration_cache
        if not cal:
            self._calibration_body.setText(
                "<i>No calibration data yet — first calibration writes "
                "when an executed trade closes.</i>"
            )
            self._reset_calibration_header()
            return

        # written_at is a monotonic-ish epoch — sort to ensure newest first
        sorted_cal = sorted(
            cal,
            key=lambda r: float(r.get("written_at", 0.0)),
            reverse=True,
        )
        n = len(sorted_cal)
        recent = sorted_cal[: max(_ROLLING_WINDOW, 5)]

        rows: list[str] = []
        for entry in recent:
            rows.append(self._format_calibration_row(entry))

        # Rolling-10 line + drift banner
        if n < _ROLLING_WINDOW:
            footer = (
                f"<br><i>Rolling-{_ROLLING_WINDOW} mean: not yet "
                f"(n={n}, need {_ROLLING_WINDOW - n} more)</i>"
            )
            self._reset_calibration_header()
        else:
            window = sorted_cal[:_ROLLING_WINDOW]
            deltas = [
                float(r.get("delta_pips", 0.0))
                for r in window
                if isinstance(r.get("delta_pips", None), (int, float))
            ]
            if not deltas:
                mean = 0.0
            else:
                mean = sum(deltas) / len(deltas)
            classification = self._classify_rolling_mean(mean)
            footer = (
                f"<br><b>Rolling-{_ROLLING_WINDOW} mean: "
                f"{self._format_pip(mean)}</b> &nbsp;{classification}"
                f"<br><i>Drift band: ±{_CAL_BAND_PIPS:.1f}p "
                f"(warns when outside)</i>"
            )
            if abs(mean) > _CAL_BAND_PIPS:
                if mean < 0:
                    self._set_calibration_banner_dangerous()
                else:
                    self._set_calibration_banner_pessimistic()
            else:
                self._reset_calibration_header()

        self._calibration_body.setText("<br>".join(rows) + footer)

    def _format_calibration_row(self, entry: dict) -> str:
        """One-line summary of a calibration entry. Format:
            PAIR  DIR  Nmin  REAL→SIM  Δ ±N.Np  [classification]
        """
        pair = entry.get("pair", "?")
        direction = entry.get("direction", "?")
        try:
            duration = float(entry.get("real_duration_minutes", 0.0))
        except (TypeError, ValueError):
            duration = 0.0
        delta = entry.get("delta_pips", None)
        try:
            delta_f = float(delta) if delta is not None else None
        except (TypeError, ValueError):
            delta_f = None
        real_exit = self._short_exit(entry.get("real_exit_reason", ""))
        sim_exit = self._short_exit(entry.get("sim_exit_reason", ""))

        delta_html = self._format_pip(delta_f)
        flag = self._classify_single_entry(delta_f)

        # Right-arrow as HTML entity for terminal-y feel
        arrow = "&rarr;"
        return (
            f"&nbsp;&nbsp;{pair:<7s} {direction:<4s} {int(duration):>4d}min "
            f"{real_exit}{arrow}{sim_exit} &nbsp;"
            f"&Delta;&nbsp;{delta_html}&nbsp;{flag}"
        ).replace(" ", "&nbsp;")

    @staticmethod
    def _short_exit(reason: str) -> str:
        """Compress exit reasons to 2-4 character tokens for the row."""
        m = {
            "tp_hit": "TP", "sl_hit": "SL",
            "TP": "TP", "SL": "SL", "TIMEOUT": "TO",
            "FAILED": "FX", "signal_exit": "SX",
            "weekend_close": "WC",
        }
        return m.get(reason, (reason[:4] or "?"))

    @staticmethod
    def _classify_single_entry(delta: float | None) -> str:
        """Per-entry visual flag; matches architect's interpretation
        framework (±3p sane, ±15p+ alarm, dangerous = optimistic
        direction)."""
        if delta is None:
            return ""
        if delta < -_CAL_BORDERLINE_PIPS:
            return "<span style='color:#c62828'>&#9888; optimistic</span>"
        if abs(delta) <= _CAL_BORDERLINE_PIPS:
            return "<span style='color:#2e7d32'>&#10003;</span>"
        if abs(delta) >= _CAL_ALARM_PIPS:
            return "<span style='color:#c62828'>&#9888; alarm</span>"
        return "<span style='color:#f57c00'>&#9888; borderline</span>"

    @staticmethod
    def _classify_rolling_mean(mean: float) -> str:
        """Classify rolling-mean direction + magnitude WITHOUT committing
        to a tuning lever (slippage vs SL-first).

        Originally I committed to "slippage-tunable" at +3-7p and
        "SL-first dominant" at +8-12p, but the rolling mean alone can't
        distinguish those cases — a +6p mean could be a constant spread
        baseline offset OR 50% of trades flipping +12p via SL-first OR
        any mix. Lever choice requires bucketing by pair × duration,
        which is decomposition work that lives in analysis scripts, not
        in operational visibility. Softened 2026-05-07 per architect
        review at Phase E close-out.
        """
        if mean < -_CAL_BAND_PIPS:
            return "<b style='color:#c62828'>TOO OPTIMISTIC — DANGEROUS</b>"
        if abs(mean) <= _CAL_BAND_PIPS:
            return "<span style='color:#2e7d32'>within band &#10003;</span>"
        # All over-pessimistic cases — investigate via decomposition.
        # Magnitude colorcoded but lever NOT named (would mislead).
        if mean <= 12.0:
            return (
                "<span style='color:#f57c00'>over-pessimistic — "
                "investigate (decompose by pair × duration)</span>"
            )
        return (
            "<b style='color:#c62828'>far over-pessimistic — "
            "investigate (decompose by pair × duration)</b>"
        )

    def _reset_calibration_header(self) -> None:
        self._calibration_header.setText("─── Recent Calibrations ───")
        self._calibration_section.setStyleSheet(self._calibration_frame_default_style)

    def _set_calibration_banner_dangerous(self) -> None:
        self._calibration_header.setText(
            "─── ⚠ DRIFT WARNING — Calibrations ───"
        )
        self._calibration_section.setStyleSheet(
            "QFrame#calibration_section { background: #ffebee; "
            "border: 2px solid #c62828; border-radius: 4px; padding: 4px; }"
        )

    def _set_calibration_banner_pessimistic(self) -> None:
        self._calibration_header.setText(
            "─── ⚠ Drift band exceeded (pessimistic) ───"
        )
        self._calibration_section.setStyleSheet(
            "QFrame#calibration_section { background: #fff3e0; "
            "border: 2px solid #f57c00; border-radius: 4px; padding: 4px; }"
        )

    # ── Section 4: Worker Health ────────────────────────────────────

    def _render_worker_section(self) -> None:
        """Worker state, pending queues, last cycle, heartbeat health."""
        if self.sim_worker is None:
            self._worker_body.setText(
                "<i>Worker not wired (panel running standalone or worker "
                "failed to construct).</i>"
            )
            self._reset_worker_header()
            return

        try:
            stats = self.sim_worker.get_stats()
        except Exception as exc:
            logger.warning("[SHADOW PANEL] get_stats raised: %s", exc)
            self._worker_body.setText(
                "<i>Worker unreachable (get_stats raised).</i>"
            )
            self._set_worker_header_stale()
            return

        last_ts = float(stats.get("last_cycle_complete_ts", 0.0) or 0.0)
        cycles = int(stats.get("cycles_completed", 0) or 0)
        first_run = bool(stats.get("first_run_active", False))
        pending_sim = int(stats.get("pending_count", -1))
        sims_total = int(stats.get("total_records_simulated", 0) or 0)
        cals_total = int(stats.get("total_calibrations_written", 0) or 0)
        permanents_total = int(stats.get("total_permanent_failed", 0) or 0)

        # pending_calibration count: derive from journal cache.
        # An EXECUTED record with sim_completed=True but
        # calibration_completed=False is awaiting calibration.
        pending_cal = 0
        for r in self._journal_cache:
            if not isinstance(r, dict):
                continue
            if r.get("status", "") != "EXECUTED":
                continue
            if not r.get("sim_completed", False):
                continue
            if r.get("calibration_completed", False):
                continue
            pending_cal += 1

        # Heartbeat: if last_cycle_complete_ts is 0, worker hasn't
        # finished its first cycle yet (still warming up). Otherwise,
        # check if it's gone stale beyond _HEARTBEAT_STALE_SECONDS.
        now = time.time()
        if last_ts <= 0.0:
            heartbeat_status = (
                "<i>Warming up — first cycle has not yet completed.</i>"
            )
            stale = False
        else:
            age_sec = now - last_ts
            stale = age_sec > _HEARTBEAT_STALE_SECONDS
            if stale:
                heartbeat_status = (
                    f"<b style='color:#c62828'>&#9888; HEARTBEAT STALE &mdash; "
                    f"last cycle {self._format_age(age_sec)} ago</b><br>"
                    f"&nbsp;&nbsp;Worker may be hung or M1 cache unreachable."
                )
            else:
                heartbeat_status = (
                    f"<span style='color:#2e7d32'>&#10003; healthy</span> "
                    f"&mdash; last cycle {self._format_age(age_sec)} ago"
                )

        if stale:
            self._set_worker_header_stale()
        else:
            self._reset_worker_header()

        # State line
        if first_run:
            state_line = (
                "FIRST-RUN &mdash; clearing permanent backlog "
                f"(cycle {cycles}, ~{permanents_total:,} marked so far)"
            )
        else:
            state_line = "STEADY"

        rows = [
            f"&nbsp;&nbsp;State: <b>{state_line}</b>",
            f"&nbsp;&nbsp;Pending simulations: {pending_sim}",
            f"&nbsp;&nbsp;Pending calibrations: {pending_cal}",
            f"&nbsp;&nbsp;Cycles completed: {cycles}",
            f"&nbsp;&nbsp;Total simulated: {sims_total:,} &nbsp; "
            f"calibrations: {cals_total:,} &nbsp; "
            f"permanent-failed: {permanents_total:,}",
            f"&nbsp;&nbsp;Heartbeat: {heartbeat_status}",
        ]
        drift = stats.get("current_drift_warning", None)
        if drift:
            rows.append(
                "&nbsp;&nbsp;<b style='color:#c62828'>"
                f"Drift warning active:</b> {drift}"
            )
        self._worker_body.setText("<br>".join(rows))

    def _reset_worker_header(self) -> None:
        self._worker_header.setText("─── Worker Health ───")
        self._worker_section.setStyleSheet(self._worker_frame_default_style)

    def _set_worker_header_stale(self) -> None:
        self._worker_header.setText("─── ⚠ Worker Health (HEARTBEAT STALE) ───")
        self._worker_section.setStyleSheet(
            "QFrame#worker_section { background: #ffebee; "
            "border: 2px solid #c62828; border-radius: 4px; padding: 4px; }"
        )

    @staticmethod
    def _format_age(seconds: float) -> str:
        if seconds < 60.0:
            return f"{int(seconds)}s"
        if seconds < 3600.0:
            return f"{int(seconds / 60)}m {int(seconds) % 60}s"
        hours = seconds / 3600.0
        return f"{hours:.1f}h"

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
