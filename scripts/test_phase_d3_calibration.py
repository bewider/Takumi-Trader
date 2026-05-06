"""Phase D.3 tests — EXECUTED parity sim wiring + calibration drain.

4 tests required by the D.3 review-gate spec:
  1. Calibration writes correctly when real trade is closed (happy path)
  2. Calibration is deferred when real trade is still open (returns None)
  3. Calibration first-light log fires exactly once across multiple writes
  4. (Architect-required) Timing race: real trade still open at cycle 1,
     closes by cycle 2, calibration writes correctly on cycle 2

Run from repo root:
    python scripts/test_phase_d3_calibration.py
"""
from __future__ import annotations

import json
import logging
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from takumi_trader.core.shadow_logger import (  # noqa: E402
    ShadowLogger, ShadowSignalRecord, ShadowCalibrationLog,
    STATUS_BLOCKED, STATUS_EXECUTED,
    GATE_DIVERGENCE_SPREAD, LANE_PAPER,
)
from takumi_trader.core.shadow_simulator import (  # noqa: E402
    ShadowSimulator, ShadowSimulatorConfig, SimulatedOutcome,
)
from takumi_trader.core.broker_spread_model import BrokerSpreadModel  # noqa: E402
from takumi_trader.core.m1_cache import M1Cache  # noqa: E402
from takumi_trader.core.shadow_sim_worker import (  # noqa: E402
    ShadowSimWorker, RealTradeOutcome,
)


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


# ── Stub helpers ────────────────────────────────────────────────────

@dataclass
class _StubSimWithCalLog:
    """Stub simulator that owns a real ShadowCalibrationLog + scripted
    sim outcomes."""
    config: ShadowSimulatorConfig
    cal_log: ShadowCalibrationLog
    outcomes: list = None
    call_count: int = 0

    def simulate(self, record):
        self.call_count += 1
        if self.outcomes:
            return self.outcomes[(self.call_count - 1) % len(self.outcomes)]
        return SimulatedOutcome(
            sim_exit_time=2000.0, sim_exit_price=1.10,
            sim_exit_reason="TP", sim_pnl_pips=10.0,
            sim_mae_pips=2.0, sim_mfe_pips=12.0,
            sim_duration_minutes=10,
            sim_pessimism_applied="wcf+sp+slip_fx0.3+vol_synth",
        )

    def write_calibration(self, record, outcome, real_pnl_pips,
                          real_exit_reason, real_duration_minutes):
        # Mirror the real ShadowSimulator.write_calibration logic, just
        # writing to the calibration log directly.
        from takumi_trader.core.shadow_logger import ShadowCalibrationRecord, STATUS_EXECUTED
        if record.status != STATUS_EXECUTED:
            return False
        if outcome.sim_exit_reason == "FAILED":
            return False
        rec = ShadowCalibrationRecord(
            shadow_id=record.shadow_id,
            strategy_id=record.strategy_id,
            pair=record.pair, direction=record.direction,
            signal_time=record.signal_time,
            real_pnl_pips=float(real_pnl_pips),
            sim_pnl_pips=float(outcome.sim_pnl_pips),
            real_exit_reason=str(real_exit_reason),
            sim_exit_reason=str(outcome.sim_exit_reason),
            real_duration_minutes=float(real_duration_minutes),
            sim_duration_minutes=float(outcome.sim_duration_minutes),
            pessimism_applied=str(outcome.sim_pessimism_applied),
        )
        self.cal_log.append(rec)
        return True


def _seed_executed_record(log: ShadowLogger, journal_idx: int = 0) -> int:
    """Create an EXECUTED strength-pass record. Returns shadow_id."""
    sid = log.log_signal(
        pair="EURUSD", direction="BUY",
        proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
        proposed_sl_pips=10.0, proposed_tp_pips=20.0,
        input_snapshot={"composite_scores": {}},
    )
    log.mark_executed(
        sid, lane=LANE_PAPER,
        ref={"system": "Sv2", "journal_idx": journal_idx},
    )
    return sid


# ─────────────────────────────────────────────────────────────────────
# Test 1 — happy path: real trade closed, calibration writes
# ─────────────────────────────────────────────────────────────────────

def test_1_calibration_happy_path():
    """EXECUTED record + closed real trade -> calibration written, record marked completed."""
    print("\n[1] calibration writes when real trade closed (happy path)")
    with tempfile.TemporaryDirectory() as td:
        log = ShadowLogger("Sv2", Path(td) / "shadow.json")
        cal_log = ShadowCalibrationLog(Path(td) / "shadow_calibration.json")
        sid = _seed_executed_record(log, journal_idx=42)

        sim = _StubSimWithCalLog(config=ShadowSimulatorConfig(), cal_log=cal_log)

        # Closed real trade — lookup returns RealTradeOutcome
        def lookup(record):
            return RealTradeOutcome(
                pnl_pips=12.5, exit_reason="tp_hit", duration_minutes=11.0,
            )

        worker = ShadowSimWorker(
            shadow_logger=log, simulator=sim,
            poll_interval=300.0, max_per_cycle=10,
            real_trade_lookup=lookup,
        )
        worker._first_run_start_ts = time.time()

        # Cycle 1: Phase 2 simulates (write_simulation sets sim_completed=True),
        # Phase 3 then queries pending_calibration which now includes the record,
        # lookup says closed, write_calibration fires in the SAME cycle.
        # This is the happy-path low-latency case (no inter-cycle wait).
        stats1 = worker._run_cycle()
        if stats1["sim_succeeded_this_cycle"] != 1:
            _fail(f"cycle 1 sim succeeded: {stats1['sim_succeeded_this_cycle']}")
        if stats1["calibrations_written_this_cycle"] != 1:
            _fail(
                f"cycle 1 calibrations: {stats1['calibrations_written_this_cycle']} "
                "(expected 1 — Phase 3 sees the record after Phase 2's write_simulation)"
            )

        # Verify on-disk state
        log2 = ShadowLogger("Sv2", Path(td) / "shadow.json")
        rec = next(r for r in log2.all_records() if r.shadow_id == sid)
        if not rec.calibration_completed:
            _fail("record's calibration_completed not set after write")

        cal_recs = cal_log.all_records()
        if len(cal_recs) != 1:
            _fail(f"calibration log records: {len(cal_recs)}")
        cal = cal_recs[0]
        if cal.shadow_id != sid:
            _fail(f"cal shadow_id: {cal.shadow_id}")
        if cal.real_pnl_pips != 12.5 or cal.sim_pnl_pips != 10.0:
            _fail(f"pnl values: real={cal.real_pnl_pips} sim={cal.sim_pnl_pips}")
        if abs(cal.delta_pips - 2.5) > 1e-9:
            _fail(f"delta should be +2.50p (real - sim), got {cal.delta_pips}")
        _ok(
            f"calibration written: real=+12.5p sim=+10.0p delta=+2.50p; "
            f"record.calibration_completed=True"
        )


# ─────────────────────────────────────────────────────────────────────
# Test 2 — deferred when real trade still open
# ─────────────────────────────────────────────────────────────────────

def test_2_calibration_deferred_when_real_open():
    """lookup returns None -> calibration deferred, record stays pending_calibration."""
    print("\n[2] calibration deferred when real trade still open")
    with tempfile.TemporaryDirectory() as td:
        log = ShadowLogger("Sv2", Path(td) / "shadow.json")
        cal_log = ShadowCalibrationLog(Path(td) / "shadow_calibration.json")
        sid = _seed_executed_record(log)

        sim = _StubSimWithCalLog(config=ShadowSimulatorConfig(), cal_log=cal_log)

        # lookup ALWAYS returns None — real trade never closes in this test
        def lookup(record):
            return None

        worker = ShadowSimWorker(
            shadow_logger=log, simulator=sim,
            poll_interval=300.0, max_per_cycle=10,
            real_trade_lookup=lookup,
        )
        worker._first_run_start_ts = time.time()

        # Run several cycles — calibration must never write
        for cycle in range(5):
            stats = worker._run_cycle()
            if stats["calibrations_written_this_cycle"] != 0:
                _fail(f"cycle {cycle+1} wrote calibration despite open real trade")

        # Verify state
        log2 = ShadowLogger("Sv2", Path(td) / "shadow.json")
        rec = next(r for r in log2.all_records() if r.shadow_id == sid)
        if rec.calibration_completed:
            _fail("calibration_completed True despite no calibration written")
        if not rec.sim_completed:
            _fail("sim_completed False — sim should have run in cycle 1")
        # pending_calibration() should still include this record (waiting for real to close)
        pending_cal = log2.pending_calibration()
        if not any(r.shadow_id == sid for r in pending_cal):
            _fail("record dropped from pending_calibration despite open real trade")

        _ok(
            "5 cycles, zero calibrations written, record stays in pending_calibration "
            "queue (sim_completed=True, calibration_completed=False)"
        )


# ─────────────────────────────────────────────────────────────────────
# Test 3 — first-light log fires exactly once
# ─────────────────────────────────────────────────────────────────────

class _LogCapture:
    """Capture INFO-level shadow_sim_worker logs.

    NOTE: the LOGGER's level (not just the handler's) must be set to
    INFO. Python's default root logger is WARNING — INFO messages get
    filtered before reaching handlers if the logger itself doesn't allow
    them. This is the same gotcha that bit Test 5 in D.2.
    """
    def __init__(self):
        self.records: list[str] = []
        self._handler = None
        self._logger_obj = None
        self._original_level = None
    def __enter__(self):
        self._logger_obj = logging.getLogger("takumi_trader.core.shadow_sim_worker")
        self._original_level = self._logger_obj.level
        self._logger_obj.setLevel(logging.INFO)
        self._handler = logging.StreamHandler()
        self._handler.setLevel(logging.INFO)
        self._handler.emit = lambda r: self.records.append(r.getMessage())
        self._logger_obj.addHandler(self._handler)
        return self
    def __exit__(self, *args):
        self._logger_obj.removeHandler(self._handler)
        self._logger_obj.setLevel(self._original_level)
    def first_light_count(self) -> int:
        return sum(1 for r in self.records if "CALIBRATION-FIRST-LIGHT" in r)


def test_3_first_light_log_fires_once():
    """First-light log fires once across multiple calibration writes."""
    print("\n[3] CALIBRATION-FIRST-LIGHT fires exactly once")
    with tempfile.TemporaryDirectory() as td:
        log = ShadowLogger("Sv2", Path(td) / "shadow.json")
        cal_log = ShadowCalibrationLog(Path(td) / "shadow_calibration.json")

        # Seed 3 EXECUTED records
        sids = [_seed_executed_record(log, journal_idx=i) for i in range(3)]

        sim = _StubSimWithCalLog(config=ShadowSimulatorConfig(), cal_log=cal_log)

        # All real trades closed
        def lookup(record):
            return RealTradeOutcome(
                pnl_pips=10.0, exit_reason="tp_hit", duration_minutes=8.0,
            )

        worker = ShadowSimWorker(
            shadow_logger=log, simulator=sim,
            poll_interval=300.0, max_per_cycle=10,
            real_trade_lookup=lookup,
        )
        worker._first_run_start_ts = time.time()

        with _LogCapture() as cap:
            # Cycle 1: simulate all 3
            worker._run_cycle()
            # Cycle 2: drain calibration queue (3 records)
            worker._run_cycle()
            # Cycle 3: idle (nothing pending)
            worker._run_cycle()

            count = cap.first_light_count()
            if count != 1:
                _fail(f"first-light fired {count} times (expected exactly 1)")

        if len(cal_log.all_records()) != 3:
            _fail(f"3 calibrations expected, got {len(cal_log.all_records())}")
        _ok(
            "3 calibrations written across cycles, "
            "CALIBRATION-FIRST-LIGHT log fired exactly once"
        )


# ─────────────────────────────────────────────────────────────────────
# Test 4 — timing race (architect-required)
# ─────────────────────────────────────────────────────────────────────

def test_4_calibration_timing_race():
    """Real trade open at cycle 1, closes by cycle 2, calibration writes correctly.

    The crucial property: the worker must not LOSE the EXECUTED record
    when the real trade is open at sim time. The pending_calibration
    queue holds it across cycles until lookup returns a closed real trade.
    """
    print("\n[4] timing race: trade open at cycle 1, closes by cycle 2")
    with tempfile.TemporaryDirectory() as td:
        log = ShadowLogger("Sv2", Path(td) / "shadow.json")
        cal_log = ShadowCalibrationLog(Path(td) / "shadow_calibration.json")
        sid = _seed_executed_record(log, journal_idx=99)

        sim = _StubSimWithCalLog(config=ShadowSimulatorConfig(), cal_log=cal_log)

        # Stateful lookup: returns None at first, then closed trade after a flag flips
        state = {"closed": False}

        def lookup(record):
            if state["closed"]:
                return RealTradeOutcome(
                    pnl_pips=8.0, exit_reason="tp_hit", duration_minutes=7.0,
                )
            return None

        worker = ShadowSimWorker(
            shadow_logger=log, simulator=sim,
            poll_interval=300.0, max_per_cycle=10,
            real_trade_lookup=lookup,
        )
        worker._first_run_start_ts = time.time()

        # Cycle 1: real trade still open. Sim runs but no calibration.
        stats1 = worker._run_cycle()
        if stats1["sim_succeeded_this_cycle"] != 1:
            _fail(f"cycle 1 sim_succeeded: {stats1['sim_succeeded_this_cycle']}")
        if stats1["calibrations_written_this_cycle"] != 0:
            _fail("cycle 1 wrote calibration (real trade was still open)")

        # Cycle 2: real trade STILL open (lookup still returns None). Worker reads
        # pending_calibration, sees the EXECUTED record, lookup says open, defers.
        stats2 = worker._run_cycle()
        if stats2["calibrations_written_this_cycle"] != 0:
            _fail("cycle 2 wrote calibration despite real still open")
        # Verify the record is still pending_calibration
        pc = log.pending_calibration()
        if not any(r.shadow_id == sid for r in pc):
            _fail("record dropped from pending_calibration on cycle 2")

        # Trade closes between cycle 2 and cycle 3
        state["closed"] = True

        # Cycle 3: real trade now closed. Worker drains pending_calibration, writes.
        stats3 = worker._run_cycle()
        if stats3["calibrations_written_this_cycle"] != 1:
            _fail(f"cycle 3 should have written calibration: "
                  f"{stats3['calibrations_written_this_cycle']}")

        # Verify the calibration entry
        log2 = ShadowLogger("Sv2", Path(td) / "shadow.json")
        rec = next(r for r in log2.all_records() if r.shadow_id == sid)
        if not rec.calibration_completed:
            _fail("calibration_completed not set after cycle 3 write")

        cal_recs = cal_log.all_records()
        if len(cal_recs) != 1:
            _fail(f"cal records: {len(cal_recs)}")
        if cal_recs[0].real_pnl_pips != 8.0:
            _fail(f"real_pnl: {cal_recs[0].real_pnl_pips}")

        _ok(
            "cycle 1: sim done, real open, no cal. "
            "cycle 2: real still open, no cal, record stays in pending_calibration. "
            "cycle 3: real closes, cal writes correctly. Record never lost across cycles."
        )


if __name__ == "__main__":
    print("Phase D.3 tests — EXECUTED parity sim wiring + calibration drain")
    test_1_calibration_happy_path()
    test_2_calibration_deferred_when_real_open()
    test_3_first_light_log_fires_once()
    test_4_calibration_timing_race()
    print("\nALL D.3 TESTS PASSED")
