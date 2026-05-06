"""Phase E.4 integration smoke test — PerformanceDialog Sv2-tab wiring.

Architectural deviation from the spec acknowledged: the spec named
LiveCandleDialog, but the actual Sv2 tab lives in PerformanceDialog
(LiveCandleDialog explicitly excludes standard tabs via
include_standard_tabs=False). E.4 integrates into PerformanceDialog
instead. See conversation log for the call.

Tests:
  1. PerformanceDialog constructs cleanly with shadow paths and the
     ShadowStatsPanel attaches to the Sv2 tab's v_splitter
  2. PerformanceDialog without shadow paths constructs cleanly and
     leaves _sv2_shadow_panel as None (no regression for callers
     that don't pass shadow deps)

Run from repo root:
    python scripts/test_phase_e4_integration.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from PyQt6.QtCore import QObject, pyqtSignal  # noqa: E402
from PyQt6.QtWidgets import QApplication  # noqa: E402


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


_APP = QApplication.instance() or QApplication(sys.argv)


class _StubWorker(QObject):
    cycle_complete = pyqtSignal(dict)
    drift_warning = pyqtSignal(str)
    fatal_error = pyqtSignal(str)

    def get_stats(self):
        import time
        return {
            "last_cycle_complete_ts": time.time(),
            "pending_count": 0,
            "total_records_simulated": 0,
            "total_calibrations_written": 0,
            "total_permanent_failed": 0,
            "current_drift_warning": None,
            "cycles_completed": 1,
            "first_run_active": False,
        }


def _write(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────
# Test 1 — PerformanceDialog constructs with shadow deps and attaches panel
# ─────────────────────────────────────────────────────────────────────

def test_1_dialog_with_shadow_deps():
    print("\n[1] PerformanceDialog with shadow deps -> panel attached to Sv2 tab")
    from takumi_trader.ui.performance_dialog import PerformanceDialog
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        journal = td_path / "shadow_trades_Sv2.json"
        cal = td_path / "shadow_calibration_Sv2.json"
        outcomes = td_path / "outcomes.json"
        paper = td_path / "paper_trades.json"
        # Minimal valid contents
        _write(journal, [])
        _write(cal, [])
        _write(outcomes, {})
        _write(paper, [])

        worker = _StubWorker()
        try:
            dlg = PerformanceDialog(
                None,
                outcomes_file=outcomes,
                active_count=0,
                paper_trades_file=paper,
                shadow_journal_path=journal,
                shadow_calibration_path=cal,
                shadow_sim_worker=worker,
            )
        except Exception as exc:
            _fail(f"PerformanceDialog construction raised: {exc!r}")

        # Verify the shadow panel was attached to the Sv2 tab's splitter
        if not hasattr(dlg, "_sv2_shadow_panel"):
            _fail("PerformanceDialog missing _sv2_shadow_panel attribute")
        if dlg._sv2_shadow_panel is None:
            _fail(
                "shadow panel is None even though shadow paths were provided "
                "(check the try/except around panel construction)"
            )
        # Confirm the panel is parented inside the v_splitter
        v_splitter = getattr(dlg, "_paper_v_splitter", None)
        if v_splitter is None:
            _fail("PerformanceDialog missing _paper_v_splitter")
        # Walk splitter children — panel should be one of them
        n_children = v_splitter.count()
        found = False
        for i in range(n_children):
            if v_splitter.widget(i) is dlg._sv2_shadow_panel:
                found = True
                break
        if not found:
            _fail(
                f"shadow panel not in v_splitter children "
                f"(splitter has {n_children} widgets)"
            )
        _ok(f"shadow panel attached as splitter widget #{i+1}/{n_children}")

        # Cleanup — close the dialog
        dlg.close()


# ─────────────────────────────────────────────────────────────────────
# Test 2 — PerformanceDialog without shadow deps still works
# ─────────────────────────────────────────────────────────────────────

def test_2_dialog_without_shadow_deps():
    print("\n[2] PerformanceDialog without shadow deps -> panel is None, no regression")
    from takumi_trader.ui.performance_dialog import PerformanceDialog
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        outcomes = td_path / "outcomes.json"
        _write(outcomes, {})

        try:
            dlg = PerformanceDialog(
                None,
                outcomes_file=outcomes,
                active_count=0,
                # NO shadow_* params — defaults to None
            )
        except Exception as exc:
            _fail(f"backward-compat construction raised: {exc!r}")

        if dlg._sv2_shadow_panel is not None:
            _fail("shadow panel built without shadow paths — should be None")
        # Splitter should still be a 2-widget config (no shadow third pane)
        v_splitter = dlg._paper_v_splitter
        if v_splitter.count() != 2:
            _fail(
                f"v_splitter count is {v_splitter.count()}, expected 2 "
                "(no shadow panel attached)"
            )
        _ok("backward-compatible: panel=None, splitter has 2 widgets")
        dlg.close()


# ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 64)
    print("Phase E.4 — PerformanceDialog Sv2-tab integration smoke test")
    print("=" * 64)
    test_1_dialog_with_shadow_deps()
    test_2_dialog_without_shadow_deps()
    print("\n" + "=" * 64)
    print("ALL E.4 INTEGRATION TESTS PASSED")
    print("=" * 64)


if __name__ == "__main__":
    main()
