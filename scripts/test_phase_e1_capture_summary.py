"""Phase E.1 tests — ShadowStatsPanel skeleton + capture summary.

4 tests required by the E.1 review-gate spec:
  1. Panel constructs cleanly with valid synthetic data
  2. Panel handles missing journal gracefully
  3. Panel handles malformed journal gracefully
  4. Capture summary calculation correctness (counts + percentages)

Run from repo root:
    python scripts/test_phase_e1_capture_summary.py
"""
from __future__ import annotations

import json
import sys
import tempfile
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

# Qt requires a QApplication for any QWidget. Singleton — multiple
# tests share the same instance.
from PyQt6.QtCore import QObject, pyqtSignal  # noqa: E402
from PyQt6.QtWidgets import QApplication  # noqa: E402

from takumi_trader.ui.shadow_stats_panel import (  # noqa: E402
    ShadowStatsPanel, today_start_utc,
)


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


# Singleton QApplication
_APP = QApplication.instance() or QApplication(sys.argv)


class _StubWorker(QObject):
    """Minimal QObject with the three signals ShadowStatsPanel
    connects to. Lets tests construct the panel without spinning up
    a real ShadowSimWorker QThread."""
    cycle_complete = pyqtSignal(dict)
    drift_warning = pyqtSignal(str)
    fatal_error = pyqtSignal(str)


def _make_record(
    *,
    signal_time: float,
    block_gate: str = "",
    status: str = "BLOCKED",
    pair: str = "EURUSD",
    direction: str = "BUY",
    sim_completed: bool = False,
    sim_pnl_pips: float = 0.0,
    sim_exit_reason: str = "",
    exec_lane: str = "",
    exec_ref_json: str = "",
) -> dict:
    """Synthetic shadow record dict matching ShadowSignalRecord shape
    (only the fields the panel reads — sparse omission is fine)."""
    return {
        "signal_time": signal_time,
        "block_gate": block_gate,
        "status": status,
        "pair": pair,
        "direction": direction,
        "sim_completed": sim_completed,
        "sim_pnl_pips": sim_pnl_pips,
        "sim_exit_reason": sim_exit_reason,
        "exec_lane": exec_lane,
        "exec_ref_json": exec_ref_json,
    }


def _write_journal(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────
# Test 1 — Panel constructs cleanly with valid data
# ─────────────────────────────────────────────────────────────────────

def test_1_constructs_with_valid_data():
    print("\n[1] panel constructs cleanly with valid synthetic data")
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        journal = td_path / "shadow_trades_Sv2.json"
        cal = td_path / "shadow_calibration_Sv2.json"
        paper = td_path / "paper_trades.json"

        anchor = today_start_utc()
        recs = [
            _make_record(
                signal_time=anchor + 60.0,
                block_gate="strength_engine",
                status="BLOCKED",
            ),
            _make_record(
                signal_time=anchor + 120.0,
                block_gate="divergence_spread",
                status="BLOCKED",
                sim_completed=True, sim_pnl_pips=-2.5, sim_exit_reason="SL",
            ),
        ]
        _write_journal(journal, recs)
        _write_journal(cal, [])
        _write_journal(paper, [])

        worker = _StubWorker()
        panel = ShadowStatsPanel(
            shadow_journal_path=journal,
            calibration_log_path=cal,
            sim_worker=worker,
            paper_journal_path=paper,
            refresh_interval_ms=0,  # disable timer in tests
        )

        # Basic sanity — the panel exists and has the capture body widget
        if not hasattr(panel, "_capture_body"):
            _fail("panel missing _capture_body widget")
        text = panel._capture_body.text()
        # HTML-encoded — match an unambiguous substring that survives encoding
        if "Total" not in text and "loading" not in text.lower():
            _fail(f"unexpected capture body text: {text[:120]!r}")
        _ok(f"constructed; capture body length={len(text)}")


# ─────────────────────────────────────────────────────────────────────
# Test 2 — Panel handles missing journal gracefully
# ─────────────────────────────────────────────────────────────────────

def test_2_handles_missing_journal():
    print("\n[2] panel handles missing journal without crashing")
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        journal = td_path / "shadow_trades_Sv2.json"  # NOT created
        cal = td_path / "shadow_calibration_Sv2.json"
        _write_journal(cal, [])

        worker = _StubWorker()
        panel = ShadowStatsPanel(
            shadow_journal_path=journal,
            calibration_log_path=cal,
            sim_worker=worker,
            refresh_interval_ms=0,
        )
        text = panel._capture_body.text()
        # Must not raise; should show a "no data" message
        if "no shadow data" not in text.lower() and "no signals" not in text.lower():
            _fail(f"missing journal should show 'no data' message, got: {text[:120]!r}")
        _ok("missing journal -> 'no data' message, no crash")


# ─────────────────────────────────────────────────────────────────────
# Test 3 — Panel handles malformed journal gracefully
# ─────────────────────────────────────────────────────────────────────

def test_3_handles_malformed_journal():
    print("\n[3] panel handles malformed JSON gracefully")
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        journal = td_path / "shadow_trades_Sv2.json"
        cal = td_path / "shadow_calibration_Sv2.json"
        # Truncated mid-record
        journal.write_text(
            '[{"signal_time": 100, "block_gate": "stren', encoding="utf-8",
        )
        _write_journal(cal, [])

        worker = _StubWorker()
        panel = ShadowStatsPanel(
            shadow_journal_path=journal,
            calibration_log_path=cal,
            sim_worker=worker,
            refresh_interval_ms=0,
        )
        text = panel._capture_body.text()
        # Must not raise. With malformed JSON, the cache stays empty,
        # so we expect "no data" or "no signals" treatment.
        if (
            "no shadow data" not in text.lower()
            and "no signals" not in text.lower()
        ):
            _fail(
                "malformed journal should fall back to 'no data', "
                f"got: {text[:120]!r}"
            )
        _ok("malformed JSON falls back to 'no data', no crash")

        # Now write a valid file and refresh — cache should pick it up
        # despite the previous bad parse (recovery path).
        time.sleep(1.05)  # mtime resolution
        anchor = today_start_utc()
        _write_journal(journal, [
            _make_record(
                signal_time=anchor + 60.0,
                block_gate="strength_engine",
                status="BLOCKED",
            ),
        ])
        panel.refresh()
        text2 = panel._capture_body.text()
        # Recovery means we stopped showing the "no data" message and
        # are now showing the capture summary table (unambiguous token: "Total")
        if "Total" not in text2 or "no shadow data" in text2.lower():
            _fail(
                "after writing valid journal, panel did not recover "
                f"(still showing: {text2[:120]!r})"
            )
        _ok("recovered after journal becomes valid")


# ─────────────────────────────────────────────────────────────────────
# Test 4 — Capture summary calculation correctness
# ─────────────────────────────────────────────────────────────────────

def test_4_capture_summary_calculations():
    print("\n[4] capture summary counts + percentages computed correctly")
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        journal = td_path / "shadow_trades_Sv2.json"
        cal = td_path / "shadow_calibration_Sv2.json"
        paper = td_path / "paper_trades.json"

        anchor = today_start_utc()

        # Synthetic universe:
        #   100 strength-rejects (today)
        #   10  strength-passes  (today): 3 executed, 7 filtered
        #   1   YESTERDAY strength-pass — must be excluded from "today"
        recs: list[dict] = []
        for i in range(100):
            recs.append(_make_record(
                signal_time=anchor + i,
                block_gate="strength_engine",
                status="BLOCKED",
            ))
        # 3 executed (link to paper journal indices 0..2)
        for i in range(3):
            recs.append(_make_record(
                signal_time=anchor + 1000 + i,
                block_gate="",
                status="EXECUTED",
                exec_lane="paper",
                exec_ref_json=json.dumps({"system": "Sv2", "journal_idx": i}),
            ))
        # 7 filtered downstream — 5 with completed sim, 2 still pending
        for i in range(5):
            recs.append(_make_record(
                signal_time=anchor + 2000 + i,
                block_gate="divergence_spread",
                status="BLOCKED",
                sim_completed=True, sim_pnl_pips=-3.0 - i, sim_exit_reason="SL",
            ))
        for i in range(2):
            recs.append(_make_record(
                signal_time=anchor + 3000 + i,
                block_gate="conviction",
                status="BLOCKED",
                sim_completed=False,
            ))
        # Yesterday — should NOT count toward today
        recs.append(_make_record(
            signal_time=anchor - 3600.0,
            block_gate="strength_engine",
            status="BLOCKED",
        ))

        _write_journal(journal, recs)
        _write_journal(cal, [])

        # Paper journal: 3 closed trades aligning with idx 0..2 above
        _write_journal(paper, [
            {
                "pair": "EURUSD", "direction": "BUY",
                "close_time": time.time(), "close_reason": "tp_hit",
                "pnl_pips": 8.0, "duration_minutes": 12.0,
            },
            {
                "pair": "EURUSD", "direction": "BUY",
                "close_time": time.time(), "close_reason": "tp_hit",
                "pnl_pips": 10.0, "duration_minutes": 18.0,
            },
            {
                "pair": "EURUSD", "direction": "BUY",
                "close_time": time.time(), "close_reason": "sl_hit",
                "pnl_pips": -5.0, "duration_minutes": 22.0,
            },
        ])

        worker = _StubWorker()
        panel = ShadowStatsPanel(
            shadow_journal_path=journal,
            calibration_log_path=cal,
            sim_worker=worker,
            paper_journal_path=paper,
            refresh_interval_ms=0,
        )

        # Verify _compute_capture_stats output
        stats = panel._compute_capture_stats()
        if stats is None:
            _fail("stats unexpectedly None")
        assertions = [
            ("total", 110),
            ("strength_rejects", 100),
            ("strength_passes", 10),
            ("executed", 3),
            ("filtered_downstream", 7),
            ("real_expectancy_n", 3),
            ("shadow_expectancy_n", 5),
        ]
        for key, expected in assertions:
            actual = stats[key]
            if actual != expected:
                _fail(f"{key}: expected {expected}, got {actual}")
        # Real expectancy mean of 8, 10, -5 = 13/3 ≈ 4.333
        if abs(stats["real_expectancy"] - (8.0 + 10.0 - 5.0) / 3.0) > 1e-6:
            _fail(f"real_expectancy: {stats['real_expectancy']}")
        # Shadow expectancy mean of -3, -4, -5, -6, -7 = -25/5 = -5.0
        if abs(stats["shadow_expectancy"] - (-25.0 / 5.0)) > 1e-6:
            _fail(f"shadow_expectancy: {stats['shadow_expectancy']}")
        _ok(
            f"counts correct (total={stats['total']}, rej={stats['strength_rejects']}, "
            f"pass={stats['strength_passes']}, exec={stats['executed']}, "
            f"filt={stats['filtered_downstream']})"
        )
        _ok(
            f"expectancies correct (real={stats['real_expectancy']:+.2f}p, "
            f"shadow={stats['shadow_expectancy']:+.2f}p, "
            f"value-add={stats['real_expectancy']-stats['shadow_expectancy']:+.2f}p)"
        )

        # Verify rendering doesn't crash and the body shows expected values.
        # The body uses HTML &nbsp; encoding so check for tokens that
        # don't depend on whitespace.
        body = panel._capture_body.text()
        for token in ("110", "Filter", "value-add"):
            if token not in body:
                _fail(f"capture body missing token {token!r}: {body[:200]!r}")
        _ok("rendered body contains expected tokens")


# ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 64)
    print("Phase E.1 — ShadowStatsPanel skeleton + capture summary")
    print("=" * 64)
    test_1_constructs_with_valid_data()
    test_2_handles_missing_journal()
    test_3_handles_malformed_journal()
    test_4_capture_summary_calculations()
    print("\n" + "=" * 64)
    print("ALL E.1 TESTS PASSED")
    print("=" * 64)


if __name__ == "__main__":
    main()
