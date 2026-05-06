"""Phase E.3 tests — Worker health section + heartbeat staleness.

2 tests required by the E.3 review-gate spec:
  1. Worker health populates from get_stats() correctly
     (state line, pending counts, cycles, heartbeat health)
  2. Heartbeat staleness detection fires correctly when no
     cycle_complete arrives within _HEARTBEAT_STALE_SECONDS

Run from repo root:
    python scripts/test_phase_e3_worker_health.py
"""
from __future__ import annotations

import json
import sys
import tempfile
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from PyQt6.QtCore import QObject, pyqtSignal  # noqa: E402
from PyQt6.QtWidgets import QApplication  # noqa: E402

from takumi_trader.ui.shadow_stats_panel import (  # noqa: E402
    ShadowStatsPanel,
)


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


_APP = QApplication.instance() or QApplication(sys.argv)


class _StubWorker(QObject):
    """Mock worker that exposes a settable stats dict + the signals
    the panel wires to."""
    cycle_complete = pyqtSignal(dict)
    drift_warning = pyqtSignal(str)
    fatal_error = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.stats = {
            "last_cycle_complete_ts": 0.0,
            "pending_count": 0,
            "total_records_simulated": 0,
            "total_calibrations_written": 0,
            "total_permanent_failed": 0,
            "current_drift_warning": None,
            "cycles_completed": 0,
            "first_run_active": True,
        }

    def get_stats(self):
        return dict(self.stats)


def _write(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")


def _make_panel(td_path: Path, worker, journal_recs=None, cal_recs=None):
    journal = td_path / "shadow_trades_Sv2.json"
    cal = td_path / "shadow_calibration_Sv2.json"
    _write(journal, journal_recs or [])
    _write(cal, cal_recs or [])
    return ShadowStatsPanel(
        shadow_journal_path=journal, calibration_log_path=cal,
        sim_worker=worker, refresh_interval_ms=0,
    )


# ─────────────────────────────────────────────────────────────────────
# Test 1 — worker health populates correctly across state transitions
# ─────────────────────────────────────────────────────────────────────

def test_1_worker_health_population():
    print("\n[1] worker health: state, pending counts, cycles populate from get_stats()")
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        worker = _StubWorker()

        # Seed journal with a record awaiting calibration so pending_cal > 0.
        # Pending calibration = EXECUTED + sim_completed=True + calibration_completed=False
        journal_recs = [
            {
                "shadow_id": 1, "signal_time": 1000.0,
                "status": "EXECUTED", "block_gate": "",
                "pair": "EURUSD", "direction": "BUY",
                "sim_completed": True, "calibration_completed": False,
                "sim_pnl_pips": 5.0, "sim_exit_reason": "TP",
                "exec_lane": "paper", "exec_ref_json": "{}",
            },
            {
                "shadow_id": 2, "signal_time": 1100.0,
                "status": "EXECUTED", "block_gate": "",
                "pair": "EURUSD", "direction": "BUY",
                "sim_completed": True, "calibration_completed": False,
                "exec_lane": "paper", "exec_ref_json": "{}",
            },
            # This one is already calibrated — should NOT count
            {
                "shadow_id": 3, "signal_time": 1200.0,
                "status": "EXECUTED", "block_gate": "",
                "pair": "EURUSD", "direction": "BUY",
                "sim_completed": True, "calibration_completed": True,
                "exec_lane": "paper", "exec_ref_json": "{}",
            },
        ]
        panel = _make_panel(td_path, worker, journal_recs=journal_recs)

        # Initial state: first_run=True, no cycles, no last_ts
        body = panel._worker_body.text()
        if "FIRST-RUN" not in body:
            _fail(f"initial state should show FIRST-RUN, got: {body[:300]!r}")
        if "Warming up" not in body:
            _fail(
                f"initial heartbeat should show 'Warming up', got: {body[:300]!r}"
            )
        if "Pending calibrations: 2" not in body:
            _fail(
                "pending_calibration count should be 2 (sim_completed AND "
                f"NOT calibration_completed), got: {body[:300]!r}"
            )
        _ok("FIRST-RUN + warming-up heartbeat + pending_cal=2 displayed")

        # Now simulate a cycle: stats advance, heartbeat is fresh
        worker.stats.update({
            "last_cycle_complete_ts": time.time(),
            "cycles_completed": 5,
            "total_records_simulated": 250,
            "total_calibrations_written": 1,
            "total_permanent_failed": 5000,
            "first_run_active": False,
            "pending_count": 17,
        })
        panel.refresh()
        body2 = panel._worker_body.text()
        if "STEADY" not in body2:
            _fail(f"after first_run_active=False should show STEADY, got: {body2[:300]!r}")
        if "Cycles completed: 5" not in body2:
            _fail(f"cycles count missing: {body2[:300]!r}")
        if "healthy" not in body2:
            _fail(f"fresh heartbeat should show 'healthy', got: {body2[:300]!r}")
        # Header should NOT show stale banner
        header = panel._worker_header.text()
        if "STALE" in header:
            _fail(f"fresh heartbeat should not set stale header: {header!r}")
        _ok("STEADY state, 5 cycles, healthy heartbeat displayed correctly")


# ─────────────────────────────────────────────────────────────────────
# Test 2 — heartbeat staleness detection
# ─────────────────────────────────────────────────────────────────────

def test_2_heartbeat_staleness():
    print("\n[2] heartbeat staleness: stale banner when last cycle > 600s ago")
    from takumi_trader.ui.shadow_stats_panel import _HEARTBEAT_STALE_SECONDS
    with tempfile.TemporaryDirectory() as td:
        worker = _StubWorker()

        # Fresh state — last_ts is now
        worker.stats["last_cycle_complete_ts"] = time.time()
        worker.stats["cycles_completed"] = 1
        panel = _make_panel(Path(td), worker)
        header_fresh = panel._worker_header.text()
        if "STALE" in header_fresh:
            _fail(f"fresh state should not be stale: {header_fresh!r}")
        _ok(f"fresh: header='{header_fresh}'")

        # Stale state — last_ts is _HEARTBEAT_STALE_SECONDS+60 ago
        worker.stats["last_cycle_complete_ts"] = (
            time.time() - _HEARTBEAT_STALE_SECONDS - 60.0
        )
        panel.refresh()
        header_stale = panel._worker_header.text()
        body_stale = panel._worker_body.text()
        if "STALE" not in header_stale:
            _fail(f"stale state should set STALE header: {header_stale!r}")
        if "HEARTBEAT STALE" not in body_stale:
            _fail(f"stale body missing banner: {body_stale[:300]!r}")
        if "may be hung" not in body_stale:
            _fail(f"stale body missing diagnostic: {body_stale[:300]!r}")
        _ok(f"stale: header='{header_stale}'")

        # Recovery — fresh again, banner clears
        worker.stats["last_cycle_complete_ts"] = time.time()
        panel.refresh()
        header_recovered = panel._worker_header.text()
        if "STALE" in header_recovered:
            _fail(f"after recovery, banner should clear: {header_recovered!r}")
        _ok(f"recovered: header='{header_recovered}'")


# ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 64)
    print("Phase E.3 — Worker health + heartbeat staleness")
    print("=" * 64)
    test_1_worker_health_population()
    test_2_heartbeat_staleness()
    print("\n" + "=" * 64)
    print("ALL E.3 TESTS PASSED")
    print("=" * 64)


if __name__ == "__main__":
    main()
