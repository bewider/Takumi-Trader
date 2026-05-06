"""Phase C.4 fixture tests — calibration log integration + drift self-monitoring.

Tests:
  1. write_calibration skips when no calibration_log attached
  2. write_calibration skips when record.status != EXECUTED
  3. write_calibration skips when outcome.sim_exit_reason == FAILED
  4. write_calibration writes a record + computes delta correctly
  5. drift check stays silent below n=10 (sample too small)
  6. drift check fires WARNING at +2.5p mean drift over 10 records
  7. drift check stays silent at +0.5p mean drift over 10 records
  8. drift check fires WARNING at -2.5p (negative — DANGEROUS direction)

Plus the Phase C end-to-end integration test:
  9. Full pipeline: simulate() -> outcome with features -> write_calibration ->
     calibration log entry -> drift check (n=1, no fire). Proves the simulator
     stack is composable end-to-end before Phase D wraps it in a worker.

Run:
    python scripts/test_phase_c4_calibration.py
"""
from __future__ import annotations

import json
import logging
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from takumi_trader.core.broker_spread_model import BrokerSpreadModel  # noqa: E402
from takumi_trader.core.m1_cache import M1Cache  # noqa: E402
from takumi_trader.core.shadow_logger import (  # noqa: E402
    ShadowSignalRecord, ShadowCalibrationLog, ShadowCalibrationRecord,
    STATUS_EXECUTED, STATUS_BLOCKED,
)
from takumi_trader.core.shadow_simulator import (  # noqa: E402
    ShadowSimulator, ShadowSimulatorConfig, SimulatedOutcome,
)
from takumi_trader.features.feature_engine import FeatureEngine  # noqa: E402


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


_M1_DTYPE = np.dtype([
    ("time", "i8"), ("open", "f8"), ("high", "f8"),
    ("low", "f8"), ("close", "f8"),
])


def _make_walk(start_dt: datetime, n: int, base_price: float = 1.10,
               step: float = 0.0001) -> np.ndarray:
    arr = np.empty(n, dtype=_M1_DTYPE)
    base = int(start_dt.timestamp())
    for i in range(n):
        o = base_price + step * i
        arr[i] = (base + i * 60, o, o + 0.0002, o - 0.0001, o + 0.0001)
    return arr


class FakeMT5:
    TIMEFRAME_M1 = 1
    def __init__(self, generator):
        self._gen = generator
    def copy_rates_range(self, pair, tf, start_dt, end_dt):
        return self._gen(pair, start_dt, end_dt)


def _build_simulator(
    bars_generator=None,
    with_calibration: bool = True,
    with_features: bool = True,
) -> tuple[ShadowSimulator, ShadowCalibrationLog | None]:
    """Spin up a sim with stub MT5, real cache, real spread model, optional cal log."""
    td = tempfile.mkdtemp()
    cfg = ShadowSimulatorConfig()
    if bars_generator is None:
        # Default: empty bars (forces FAILED outcome)
        def empty_gen(p, s, e):
            return np.empty(0, dtype=_M1_DTYPE)
        bars_generator = empty_gen
    cache = M1Cache(Path(td), mt5_module=FakeMT5(bars_generator))
    spread_model = BrokerSpreadModel(cfg)
    fe = FeatureEngine() if with_features else None
    cal_log = None
    if with_calibration:
        cal_log = ShadowCalibrationLog(Path(td) / "shadow_calibration.json")
    sim = ShadowSimulator(
        m1_cache=cache, spread_model=spread_model,
        feature_engine=fe, calibration_log=cal_log, config=cfg,
    )
    return sim, cal_log


def _make_executed_record() -> ShadowSignalRecord:
    """A real-shape EXECUTED strength-pass record."""
    return ShadowSignalRecord(
        shadow_id=42, strategy_id="Sv2",
        signal_time=datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp(),
        pair="EURUSD", direction="BUY",
        proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
        proposed_sl_pips=10.0, proposed_tp_pips=20.0,
        status=STATUS_EXECUTED,
        exec_lane="paper", exec_ref_json='{"system":"Sv2","journal_idx":594}',
        input_snapshot_json='{"composite_scores": {}}',
    )


# ─────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────

def test_1_skip_no_cal_log():
    """No calibration_log attached -> write_calibration returns False."""
    print("\n[1] write_calibration skips when no calibration_log")
    sim, _ = _build_simulator(with_calibration=False, with_features=False)
    rec = _make_executed_record()
    out = SimulatedOutcome(sim_exit_reason="TP", sim_pnl_pips=15.0)
    result = sim.write_calibration(rec, out, real_pnl_pips=18.0,
                                    real_exit_reason="tp_hit", real_duration_minutes=12)
    if result is not False:
        _fail(f"expected False, got {result}")
    _ok("returns False, no exception, no side effects")


def test_2_skip_non_executed_record():
    """Record with status != EXECUTED -> write_calibration skips."""
    print("\n[2] write_calibration skips non-EXECUTED records")
    sim, cal_log = _build_simulator(with_features=False)
    rec = _make_executed_record()
    rec.status = STATUS_BLOCKED  # not EXECUTED
    out = SimulatedOutcome(sim_exit_reason="TP", sim_pnl_pips=15.0)
    result = sim.write_calibration(rec, out, real_pnl_pips=18.0,
                                    real_exit_reason="tp_hit", real_duration_minutes=12)
    if result is not False:
        _fail(f"expected False, got {result}")
    if len(cal_log.all_records()) != 0:
        _fail("calibration log should be empty")
    _ok("BLOCKED record -> no calibration write")


def test_3_skip_failed_outcome():
    """Outcome with sim_exit_reason=FAILED -> write_calibration skips."""
    print("\n[3] write_calibration skips FAILED outcomes")
    sim, cal_log = _build_simulator(with_features=False)
    rec = _make_executed_record()
    out = SimulatedOutcome(sim_exit_reason="FAILED",
                           sim_failure_reason="no_m1_data")
    result = sim.write_calibration(rec, out, real_pnl_pips=18.0,
                                    real_exit_reason="tp_hit", real_duration_minutes=12)
    if result is not False:
        _fail(f"expected False, got {result}")
    if len(cal_log.all_records()) != 0:
        _fail("calibration log should be empty after FAILED skip")
    _ok("FAILED outcome -> no calibration write")


def test_4_writes_with_correct_delta():
    """Happy path: writes record with delta = real_pnl - sim_pnl."""
    print("\n[4] write_calibration writes record + computes delta")
    sim, cal_log = _build_simulator(with_features=False)
    rec = _make_executed_record()
    out = SimulatedOutcome(
        sim_exit_reason="TP", sim_pnl_pips=15.0,
        sim_duration_minutes=12,
        sim_pessimism_applied="wcf+sp+slip_fx0.3+vol_synth",
    )
    result = sim.write_calibration(rec, out, real_pnl_pips=18.0,
                                    real_exit_reason="tp_hit",
                                    real_duration_minutes=11.0)
    if result is not True:
        _fail(f"expected True, got {result}")

    # Reload the calibration log from disk to verify persistence
    cal_log2 = ShadowCalibrationLog(cal_log._path)
    recs = cal_log2.all_records()
    if len(recs) != 1:
        _fail(f"expected 1 record on disk, got {len(recs)}")

    r = recs[0]
    if r.shadow_id != 42:
        _fail(f"shadow_id mismatch: {r.shadow_id}")
    if r.real_pnl_pips != 18.0 or r.sim_pnl_pips != 15.0:
        _fail(f"pnl values: real={r.real_pnl_pips} sim={r.sim_pnl_pips}")
    if abs(r.delta_pips - 3.0) > 1e-9:
        _fail(f"delta should be 18.0 - 15.0 = 3.0, got {r.delta_pips}")
    if r.real_exit_reason != "tp_hit" or r.sim_exit_reason != "TP":
        _fail(f"reasons: real={r.real_exit_reason} sim={r.sim_exit_reason}")
    if "vol_synth" not in r.pessimism_applied:
        _fail(f"pessimism_applied missing vol_synth: {r.pessimism_applied}")
    _ok(f"write succeeded, delta=+3.00p, persisted to disk, all metadata correct")


# ── Drift detection tests ───────────────────────────────────────────

class _LogCapture:
    """Capture WARNING-level log messages for inspection."""
    def __init__(self):
        self.records: list[str] = []
        self._handler = None
    def __enter__(self):
        self._handler = logging.StreamHandler()
        self._handler.setLevel(logging.WARNING)
        self._handler.emit = lambda r: self.records.append(r.getMessage())
        logging.getLogger("takumi_trader.core.shadow_simulator").addHandler(self._handler)
        return self
    def __exit__(self, *args):
        logging.getLogger("takumi_trader.core.shadow_simulator").removeHandler(self._handler)
    def has_drift_warning(self) -> bool:
        return any("CALIBRATION" in r for r in self.records)
    def warning_text(self) -> str:
        return next((r for r in self.records if "CALIBRATION" in r), "")


def _seed_calibration_records(
    cal_log: ShadowCalibrationLog,
    n: int,
    real_pnl: float,
    sim_pnl: float,
) -> None:
    """Append n synthetic records with the given (real, sim) pair."""
    for i in range(n):
        cal_log.append(ShadowCalibrationRecord(
            shadow_id=i + 1, strategy_id="Sv2", pair="EURUSD", direction="BUY",
            signal_time=1.0e9 + i,
            real_pnl_pips=real_pnl, sim_pnl_pips=sim_pnl,
            real_exit_reason="tp_hit", sim_exit_reason="TP",
            real_duration_minutes=10.0, sim_duration_minutes=10.0,
            pessimism_applied="wcf+sp+slip_fx0.3",
        ))


def test_5_drift_silent_below_n():
    """Drift check stays silent below calibration_warn_after_n records."""
    print("\n[5] drift check silent at n < 10")
    sim, cal_log = _build_simulator(with_features=False)
    # Append 5 records with massive drift; should NOT warn
    _seed_calibration_records(cal_log, n=5, real_pnl=10.0, sim_pnl=0.0)  # +10p drift
    with _LogCapture() as cap:
        sim._check_calibration_drift()
    if cap.has_drift_warning():
        _fail(f"unexpected warning: {cap.warning_text()}")
    _ok("n=5 (below threshold n=10) -> no warning, even with +10p drift")


def test_6_drift_warns_at_2p5_positive():
    """Drift check fires WARNING at +2.5p mean drift over 10 records."""
    print("\n[6] drift fires WARNING at +2.5p over 10 records")
    sim, cal_log = _build_simulator(with_features=False)
    # 10 records: real=12.5, sim=10.0 -> mean delta = +2.5p (above 1.5 band)
    _seed_calibration_records(cal_log, n=10, real_pnl=12.5, sim_pnl=10.0)
    with _LogCapture() as cap:
        sim._check_calibration_drift()
    if not cap.has_drift_warning():
        _fail("expected drift warning at +2.5p, got none")
    msg = cap.warning_text()
    if "+2.50p" not in msg and "+2.5" not in msg:
        _fail(f"warning should mention +2.5p, got: {msg}")
    if "too pessimistic" not in msg:
        _fail(f"warning should classify as 'too pessimistic': {msg}")
    _ok(f"+2.5p drift -> WARNING fired: {msg.split(':',1)[1].strip()[:80]}...")


def test_7_drift_silent_at_0p5():
    """Drift check stays silent at +0.5p (within 1.5p band)."""
    print("\n[7] drift silent at +0.5p (within band)")
    sim, cal_log = _build_simulator(with_features=False)
    _seed_calibration_records(cal_log, n=10, real_pnl=10.5, sim_pnl=10.0)
    with _LogCapture() as cap:
        sim._check_calibration_drift()
    if cap.has_drift_warning():
        _fail(f"unexpected warning: {cap.warning_text()}")
    _ok("+0.5p mean delta is within ±1.5p band -> no warning")


def test_8_drift_warns_at_negative_dangerous_direction():
    """Drift check fires WARNING at -2.5p (sim beats real -> DANGEROUS)."""
    print("\n[8] drift fires WARNING at -2.5p (DANGEROUS direction)")
    sim, cal_log = _build_simulator(with_features=False)
    # 10 records: real=10.0, sim=12.5 -> mean delta = -2.5p (sim too generous)
    _seed_calibration_records(cal_log, n=10, real_pnl=10.0, sim_pnl=12.5)
    with _LogCapture() as cap:
        sim._check_calibration_drift()
    if not cap.has_drift_warning():
        _fail("expected drift warning at -2.5p, got none")
    msg = cap.warning_text()
    # Negative drift is the DANGEROUS direction (sim beats real -> illusory edge)
    if "TOO OPTIMISTIC" not in msg and "DANGEROUS" not in msg:
        _fail(f"negative drift should be flagged DANGEROUS: {msg}")
    _ok(f"-2.5p drift -> WARNING with DANGEROUS classification fired")


# ── End-to-end integration test (the C.4 review-gate addition) ─────

def test_9_phase_c_end_to_end():
    """Full pipeline: simulate() -> outcome with features -> write_calibration ->
    log entry -> drift check (n=1, no fire). Proves Phase C is composable
    end-to-end as a unit before Phase D wraps it in a worker.
    """
    print("\n[9] PHASE C END-TO-END integration test")
    signal_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)
    sig_ep = int(signal_time.timestamp())

    # Anchor-to-signal-time generator: at signal_time, price is ~1.10
    # exactly. Walking up linearly before/after. M1Cache's ±12h padding
    # means start_dt may be 12h before signal_time; without anchoring,
    # price would be 1.17 at signal_time and the test's proposed_entry
    # of 1.10 would be 70p underwater at fill -> nonsense pnl.
    def gen(pair, start_dt, end_dt):
        n_min = int((end_dt - start_dt).total_seconds() // 60)
        if n_min <= 0:
            return np.empty(0, dtype=_M1_DTYPE)
        arr = np.empty(n_min, dtype=_M1_DTYPE)
        base_ep = int(start_dt.timestamp())
        for i in range(n_min):
            bar_time = base_ep + i * 60
            offset_min = (bar_time - sig_ep) / 60.0
            # Step is 1 pip per 10 minutes upward — gentle walk so TP
            # fires within ~50 minutes after signal_time, no SL.
            o = 1.10 + 0.00001 * offset_min  # 1pip / 10min
            arr[i] = (bar_time, o, o + 0.0002, o - 0.0001, o + 0.0001)
        return arr

    sim, cal_log = _build_simulator(bars_generator=gen, with_features=True)

    # Build an EXECUTED strength-pass record (status=EXECUTED, has snapshot)
    snap = {
        "composite_scores": {"USD": 5.0, "EUR": 7.0, "GBP": 4.0, "JPY": 5.0,
                             "CAD": 5.5, "AUD": 4.5, "NZD": 5.0, "CHF": 5.5},
        "cross_pair_close_prices": {"EURUSD": 1.10, "USDJPY": 150.0, "EURJPY": 165.0},
    }
    rec = ShadowSignalRecord(
        shadow_id=999, strategy_id="Sv2",
        signal_time=signal_time.timestamp(),
        pair="EURUSD", direction="BUY",
        proposed_entry=1.1000, proposed_sl_price=1.0980, proposed_tp_price=1.1100,
        proposed_sl_pips=20.0, proposed_tp_pips=100.0,  # wide TP for the synthetic walk
        status=STATUS_EXECUTED,
        exec_lane="paper", exec_ref_json='{"system":"Sv2","journal_idx":777}',
        input_snapshot_json=json.dumps(snap),
    )

    # Step 1: simulate
    outcome = sim.simulate(rec)
    if outcome.sim_exit_reason == "FAILED":
        _fail(f"sim FAILED: {outcome.sim_failure_reason}")

    # Step 2: outcome must have features
    if outcome.features is None:
        _fail("outcome.features is None — feature recompute didn't fire")
    if len(outcome.features) < 138:
        _fail(f"feature count below 138: {len(outcome.features)}")

    # Step 3: write_calibration with synthetic real_trade values
    real_pnl = outcome.sim_pnl_pips + 1.0  # real beats sim by 1p (within band)
    written = sim.write_calibration(
        rec, outcome,
        real_pnl_pips=real_pnl,
        real_exit_reason="tp_hit",
        real_duration_minutes=outcome.sim_duration_minutes + 0.5,
    )
    if written is not True:
        _fail("write_calibration returned False on happy-path EXECUTED record")

    # Step 4: log entry persisted
    if len(cal_log.all_records()) != 1:
        _fail(f"calibration log should have 1 record, got {len(cal_log.all_records())}")
    cal_rec = cal_log.all_records()[0]
    if cal_rec.shadow_id != 999:
        _fail(f"shadow_id wrong on cal record: {cal_rec.shadow_id}")
    if abs(cal_rec.delta_pips - 1.0) > 1e-9:
        _fail(f"delta should be 1.0p, got {cal_rec.delta_pips}")

    # Step 5: drift check at n=1 should NOT fire (sample too small)
    with _LogCapture() as cap:
        sim._check_calibration_drift()
    if cap.has_drift_warning():
        _fail(f"drift fired at n=1: {cap.warning_text()}")

    _ok(
        f"end-to-end pipeline: simulate({outcome.sim_exit_reason}, "
        f"{outcome.sim_pnl_pips}p, {len(outcome.features)} features) -> "
        f"calibration delta=+1.00p -> drift silent at n=1"
    )


if __name__ == "__main__":
    print("Phase C.4 fixture tests + Phase C end-to-end integration")
    test_1_skip_no_cal_log()
    test_2_skip_non_executed_record()
    test_3_skip_failed_outcome()
    test_4_writes_with_correct_delta()
    test_5_drift_silent_below_n()
    test_6_drift_warns_at_2p5_positive()
    test_7_drift_silent_at_0p5()
    test_8_drift_warns_at_negative_dangerous_direction()
    test_9_phase_c_end_to_end()
    print("\nALL C.4 + END-TO-END TESTS PASSED")
