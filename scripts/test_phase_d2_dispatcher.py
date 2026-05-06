"""Phase D.2 tests — per-cycle dispatcher logic.

5 tests required by the D.2 review-gate spec:
  1. _is_permanent_failure_record correctly classifies all permanent shapes
  2. Batched flush during permanent-FAILED fast-path produces fewer disk
     writes than per-record (architect-required: mock _flush_atomic, count)
  3. transient_retry_count cap correctly transitions to permanent-FAILED
     after transient_retry_max retries (architect-required)
  4. Real-sim outcome classification routes success / transient / permanent
     to correct ShadowLogger methods
  5. FIRST-RUN -> STEADY mode flip fires exactly once at the right cycle
     (architect-required: explicit one-time-fire test)

Run from repo root:
    python scripts/test_phase_d2_dispatcher.py
"""
from __future__ import annotations

import logging
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from takumi_trader.core.shadow_logger import (  # noqa: E402
    ShadowLogger, ShadowSignalRecord,
    STATUS_BLOCKED, STATUS_EXECUTED, STATUS_PENDING,
    GATE_STRENGTH_ENGINE, GATE_DIVERGENCE_SPREAD,
)
from takumi_trader.core.shadow_simulator import (  # noqa: E402
    ShadowSimulatorConfig, SimulatedOutcome,
)
from takumi_trader.core.shadow_sim_worker import (  # noqa: E402
    ShadowSimWorker,
    _is_permanent_failure_record,
    _is_transient_failure_reason,
    _classify_permanent_reason,
)


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


# ── Stub simulators ─────────────────────────────────────────────────

@dataclass
class _StubSimulator:
    """Configurable stub — returns scripted outcomes per call."""
    config: ShadowSimulatorConfig
    outcomes: list = None
    call_count: int = 0

    def simulate(self, record):
        self.call_count += 1
        if self.outcomes:
            i = (self.call_count - 1) % len(self.outcomes)
            return self.outcomes[i]
        return SimulatedOutcome(sim_exit_reason="TIMEOUT", sim_pnl_pips=0.0)


class _AlwaysTransientSimulator:
    """Always returns transient FAILED — used to test the retry cap."""
    def __init__(self):
        self.config = ShadowSimulatorConfig()
        self.calls = 0
    def simulate(self, record):
        self.calls += 1
        return SimulatedOutcome(
            sim_exit_reason="FAILED",
            sim_failure_reason="no_m1_data",
        )


# ─────────────────────────────────────────────────────────────────────
# Test 1 — classifier identifies all permanent shapes
# ─────────────────────────────────────────────────────────────────────

def test_1_classifier_identifies_permanent_shapes():
    """_is_permanent_failure_record correctly identifies every permanent shape."""
    print("\n[1] permanent-failure classifier")

    # Strength-reject (no input_snapshot)
    r1 = ShadowSignalRecord(
        shadow_id=1, status=STATUS_BLOCKED, block_gate=GATE_STRENGTH_ENGINE,
        pair="EURUSD", direction="BUY",
        # No input_snapshot_json
    )
    if not _is_permanent_failure_record(r1):
        _fail("strength-reject not classified permanent")
    if _classify_permanent_reason(r1) != "strength_reject_no_snapshot":
        _fail(f"reason: {_classify_permanent_reason(r1)}")

    # Invalid prices (zero entry)
    r2 = ShadowSignalRecord(
        shadow_id=2, status=STATUS_BLOCKED, block_gate=GATE_DIVERGENCE_SPREAD,
        pair="EURUSD", direction="BUY",
        proposed_entry=0.0, proposed_sl_price=1.099, proposed_tp_price=1.102,
        input_snapshot_json='{"x":1}',
    )
    if not _is_permanent_failure_record(r2):
        _fail("invalid prices not classified permanent")
    if _classify_permanent_reason(r2) != "invalid_proposal_prices":
        _fail(f"reason: {_classify_permanent_reason(r2)}")

    # Invalid direction
    r3 = ShadowSignalRecord(
        shadow_id=3, status=STATUS_BLOCKED, block_gate=GATE_DIVERGENCE_SPREAD,
        pair="EURUSD", direction="LONG",  # invalid
        proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
        input_snapshot_json='{"x":1}',
    )
    if not _is_permanent_failure_record(r3):
        _fail("invalid direction not classified permanent")
    if not _classify_permanent_reason(r3).startswith("invalid_direction"):
        _fail(f"reason: {_classify_permanent_reason(r3)}")

    # Valid record (should NOT be classified permanent)
    r4 = ShadowSignalRecord(
        shadow_id=4, status=STATUS_BLOCKED, block_gate=GATE_DIVERGENCE_SPREAD,
        pair="EURUSD", direction="BUY",
        proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
        input_snapshot_json='{"composite_scores":{}}',
    )
    if _is_permanent_failure_record(r4):
        _fail("valid record incorrectly classified permanent")

    # transient reason classifier
    if not _is_transient_failure_reason("no_m1_data"):
        _fail("no_m1_data should be transient")
    if not _is_transient_failure_reason("data_too_recent"):
        _fail("data_too_recent should be transient")
    if _is_transient_failure_reason("strength_reject_no_snapshot"):
        _fail("strength_reject_no_snapshot should be permanent")
    if _is_transient_failure_reason("invalid_direction:'LONG'"):
        _fail("invalid_direction:* should be permanent (prefix match)")
    if not _is_transient_failure_reason(""):
        _fail("empty reason should default to transient")
    if not _is_transient_failure_reason("brand_new_unknown_failure"):
        _fail("unknown reasons should default to transient")

    _ok("strength-reject + invalid-prices + invalid-direction permanent; valid+unknown not")


# ─────────────────────────────────────────────────────────────────────
# Test 2 — batched flush during fast-path
# ─────────────────────────────────────────────────────────────────────

def test_2_batched_flush_permanent_fastpath():
    """Architect-required: count _flush_atomic calls during permanent
    fast-path; must be <<records to prove batching works."""
    print("\n[2] batched flush during permanent fast-path")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow.json"
        log = ShadowLogger("Sv2", jpath)

        # Seed 100 strength-rejects (permanent-FAILED candidates)
        N = 100
        for i in range(N):
            log.log_strength_reject(
                pair="EURUSD", direction="BUY",
                reason="threshold below",
                m5_base=5.0, m5_quote=5.0, m15_base=5.0, m15_quote=5.0,
                h1_base=5.0, h1_quote=5.0, h4_base=5.0, h4_quote=5.0,
                d1_base=5.0, d1_quote=5.0,
                spread_points=0.1, m5_atr_pips=0.0, h1_atr_pips=10.0,
                usd_score=5.0, ccy_dispersion=1.0, session="London",
            )

        # Worker with stub simulator (won't be called for fast-path)
        sim = _StubSimulator(config=ShadowSimulatorConfig())
        worker = ShadowSimWorker(
            shadow_logger=log, simulator=sim,
            poll_interval=300.0,
            max_per_cycle=10,
            max_permanent_per_cycle=N,  # process all in one cycle
        )

        # Patch _flush_atomic on the logger to count calls.
        # We measure flushes that happen DURING the fast-path itself,
        # so we patch before _run_cycle, not before record creation.
        flush_count = {"n": 0}
        original_flush = log._flush_atomic
        def counting_flush():
            flush_count["n"] += 1
            return original_flush()

        with patch.object(log, "_flush_atomic", side_effect=counting_flush):
            worker._first_run_start_ts = time.time()
            stats = worker._run_cycle()

        # Architect's spec: must be <<records. Concrete bound: at most
        # a single-digit number of flushes for 100 record mutations.
        # Throttle is 30s, so within one cycle we expect 0-2 throttled
        # flushes + 1 force_flush at end = ~1-3 total.
        if stats["permanent_failed_this_cycle"] != N:
            _fail(f"expected {N} permanent-FAILEDs processed, got "
                  f"{stats['permanent_failed_this_cycle']}")
        if flush_count["n"] >= N:
            _fail(f"flush count {flush_count['n']} >= records {N} — batching broken")
        if flush_count["n"] > 5:
            _fail(f"unexpectedly high flush count: {flush_count['n']} (target <5)")
        _ok(f"100 permanent records -> {flush_count['n']} disk flushes "
            f"(target <5; per-record would be 100)")


# ─────────────────────────────────────────────────────────────────────
# Test 3 — transient retry cap escalates to permanent
# ─────────────────────────────────────────────────────────────────────

def test_3_transient_retry_cap():
    """Record retried 12 times with no_m1_data -> escalates to permanent
    on the 12th retry (or before). Architect-required."""
    print("\n[3] transient_retry_count cap -> permanent escalation")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow.json"
        log = ShadowLogger("Sv2", jpath)

        # Seed one strength-pass record (with input_snapshot, not a fast-path)
        # Then mark it BLOCKED via a downstream gate so pending_simulation
        # returns it (PENDING records are excluded by design — they're orphans).
        sid = log.log_signal(
            pair="EURUSD", direction="BUY",
            proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
            proposed_sl_pips=10.0, proposed_tp_pips=20.0,
            input_snapshot={"composite_scores": {}},
        )
        log.mark_decision(sid, STATUS_BLOCKED, GATE_DIVERGENCE_SPREAD,
                          "test fixture: composite spread below threshold")

        sim = _AlwaysTransientSimulator()
        cap = sim.config.transient_retry_max  # 12 by default
        worker = ShadowSimWorker(
            shadow_logger=log, simulator=sim,
            poll_interval=300.0,
            max_per_cycle=1,
            # max_permanent_per_cycle defaults to 1000; no permanent
            # candidates in the test journal so this doesn't affect anything.
        )
        worker._first_run_start_ts = time.time()

        # Run cycles until escalation. The worker's bump_transient_retry
        # increments the counter; once it hits cap, _dispatch_outcome
        # calls mark_permanent_failed.
        rec = next(r for r in log.all_records() if r.shadow_id == sid)
        for cycle in range(cap + 5):  # extra cycles to ensure escalation completes
            stats = worker._run_cycle()
            rec = next(r for r in log.all_records() if r.shadow_id == sid)
            if rec.sim_completed:
                break

        if not rec.sim_completed:
            _fail(f"record never escalated after {cycle+1} cycles")
        if rec.sim_exit_reason != "FAILED":
            _fail(f"escalated record sim_exit_reason: {rec.sim_exit_reason}")
        if "transient_giveup" not in rec.sim_failure_reason:
            _fail(f"failure reason: {rec.sim_failure_reason}")
        if rec.transient_retry_count < cap:
            _fail(f"escalated at retry count {rec.transient_retry_count} "
                  f"(expected >= {cap})")

        _ok(
            f"after {rec.transient_retry_count} transient retries, escalated to "
            f"permanent-FAILED with reason={rec.sim_failure_reason!r}"
        )


# ─────────────────────────────────────────────────────────────────────
# Test 4 — outcome dispatch routes success / transient / permanent
# ─────────────────────────────────────────────────────────────────────

def test_4_real_sim_outcome_classification():
    """Worker correctly routes simulator outcomes to ShadowLogger methods.

    Three records, three different scripted outcomes:
      Record A: success (TP) -> write_simulation called, sim_completed=True
      Record B: transient FAILED (no_m1_data) -> bump_transient_retry called,
                sim_completed=False (not finalized)
      Record C: permanent FAILED (insufficient_m1_bars from sim) ->
                mark_permanent_failed called, sim_completed=True with reason
    """
    print("\n[4] outcome routing: success / transient / permanent")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow.json"
        log = ShadowLogger("Sv2", jpath)

        # Seed 3 strength-pass records, marked BLOCKED so pending_simulation sees them
        sids = []
        for i in range(3):
            sid = log.log_signal(
                pair="EURUSD", direction="BUY",
                proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
                proposed_sl_pips=10.0, proposed_tp_pips=20.0,
                input_snapshot={"composite_scores": {}},
            )
            log.mark_decision(sid, STATUS_BLOCKED, GATE_DIVERGENCE_SPREAD,
                              "test fixture")
            sids.append(sid)

        outcomes = [
            # Record A: success
            SimulatedOutcome(
                sim_exit_time=2000.0, sim_exit_price=1.102,
                sim_exit_reason="TP", sim_pnl_pips=15.0,
                sim_mae_pips=2.0, sim_mfe_pips=20.0,
                sim_duration_minutes=15, sim_pessimism_applied="wcf+sp",
            ),
            # Record B: transient
            SimulatedOutcome(sim_exit_reason="FAILED", sim_failure_reason="no_m1_data"),
            # Record C: permanent
            SimulatedOutcome(sim_exit_reason="FAILED",
                             sim_failure_reason="insufficient_m1_bars"),
        ]
        sim = _StubSimulator(config=ShadowSimulatorConfig(), outcomes=outcomes)
        worker = ShadowSimWorker(
            shadow_logger=log, simulator=sim,
            poll_interval=300.0,
            max_per_cycle=3,
            # max_permanent_per_cycle defaults to 1000; no permanent records seeded.
        )
        worker._first_run_start_ts = time.time()
        stats = worker._run_cycle()

        # Verify per-record outcomes
        log2 = ShadowLogger("Sv2", jpath)  # reload from disk
        rec_a = next(r for r in log2.all_records() if r.shadow_id == sids[0])
        rec_b = next(r for r in log2.all_records() if r.shadow_id == sids[1])
        rec_c = next(r for r in log2.all_records() if r.shadow_id == sids[2])

        # Record A: success
        if not rec_a.sim_completed:
            _fail("Record A (success) not marked sim_completed")
        if rec_a.sim_exit_reason != "TP":
            _fail(f"Record A exit_reason: {rec_a.sim_exit_reason}")
        if rec_a.sim_pnl_pips != 15.0:
            _fail(f"Record A pnl: {rec_a.sim_pnl_pips}")

        # Record B: transient -> NOT completed, retry counter incremented
        if rec_b.sim_completed:
            _fail("Record B (transient) wrongly marked sim_completed")
        if rec_b.transient_retry_count != 1:
            _fail(f"Record B retry count: {rec_b.transient_retry_count}")

        # Record C: permanent -> completed with FAILED reason
        if not rec_c.sim_completed:
            _fail("Record C (permanent) not marked sim_completed")
        if rec_c.sim_exit_reason != "FAILED":
            _fail(f"Record C exit_reason: {rec_c.sim_exit_reason}")
        if rec_c.sim_failure_reason != "insufficient_m1_bars":
            _fail(f"Record C failure_reason: {rec_c.sim_failure_reason}")

        if stats["sim_attempted_this_cycle"] != 3:
            _fail(f"attempted: {stats['sim_attempted_this_cycle']}")
        if stats["sim_succeeded_this_cycle"] != 1:
            _fail(f"succeeded: {stats['sim_succeeded_this_cycle']}")
        if stats["sim_transient_retries_this_cycle"] != 1:
            _fail(f"transient_retries: {stats['sim_transient_retries_this_cycle']}")

        _ok(
            f"3 outcomes routed correctly: "
            f"success(TP +15p, completed), "
            f"transient(no_m1, retry=1, NOT completed), "
            f"permanent(insufficient_m1, FAILED, completed)"
        )


# ─────────────────────────────────────────────────────────────────────
# Test 5 — FIRST-RUN -> STEADY mode flip fires exactly once
# ─────────────────────────────────────────────────────────────────────

class _LogCapture:
    """Capture INFO-level log messages from shadow_sim_worker.

    Requires setting the LOGGER's level (not just the handler's) because
    Python's default logger level is WARNING — INFO messages are filtered
    before they reach handlers if the logger itself doesn't allow them.
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
    def transition_log_count(self) -> int:
        return sum(1 for r in self.records if "FIRST-RUN -> STEADY" in r)


def test_5_first_run_to_steady_mode_flip():
    """FIRST-RUN -> STEADY transition log fires EXACTLY ONCE.

    Construct a journal with permanent-FAILEDs first then real-sim
    candidates. Run cycles. Verify the transition happens at the
    correct cycle and log fires only once across multiple cycles.
    """
    print("\n[5] FIRST-RUN -> STEADY mode flip fires exactly once")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow.json"
        log = ShadowLogger("Sv2", jpath)

        # 5 strength-rejects (permanent fast-path candidates)
        for _ in range(5):
            log.log_strength_reject(
                pair="EURUSD", direction="BUY", reason="below threshold",
                m5_base=5.0, m5_quote=5.0, m15_base=5.0, m15_quote=5.0,
                h1_base=5.0, h1_quote=5.0, h4_base=5.0, h4_quote=5.0,
                d1_base=5.0, d1_quote=5.0,
                spread_points=0.1, m5_atr_pips=0.0, h1_atr_pips=10.0,
                usd_score=5.0, ccy_dispersion=1.0, session="London",
            )
        # 2 real-sim candidates (strength-passes), marked BLOCKED so pending sees them
        for _ in range(2):
            sid_p = log.log_signal(
                pair="EURUSD", direction="BUY",
                proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
                proposed_sl_pips=10.0, proposed_tp_pips=20.0,
                input_snapshot={"composite_scores": {}},
            )
            log.mark_decision(sid_p, STATUS_BLOCKED, GATE_DIVERGENCE_SPREAD,
                              "test fixture")

        # Worker: max_permanent=2/cycle, max_real=2/cycle
        # Cycle 1: 5 permanents at start (>2), Phase 1 clears 2,
        #          Phase 2 attempts 2 reals -> FIRST-RUN stays (backlog still 3)
        # Cycle 2: 3 permanents at start (>2), Phase 1 clears 2, Phase 2 0 reals
        #          -> FIRST-RUN stays (backlog still 1)
        # Cycle 3: 1 permanent at start (<=2 = max), this cycle drains backlog
        #          -> FLIP to STEADY (the trigger is "backlog cleared")
        # Cycle 4: 0 permanents, queue empty -> stays STEADY (no re-fire)
        sim = _StubSimulator(
            config=ShadowSimulatorConfig(),
            outcomes=[
                SimulatedOutcome(sim_exit_reason="TP", sim_pnl_pips=10.0,
                                 sim_pessimism_applied="wcf"),
            ],
        )
        worker = ShadowSimWorker(
            shadow_logger=log, simulator=sim,
            poll_interval=300.0,
            max_per_cycle=2,
            max_permanent_per_cycle=2,
        )
        worker._first_run_start_ts = time.time()

        with _LogCapture() as cap:
            # Cycle 1: 5 permanents at start > max=2; backlog NOT cleared
            stats1 = worker._run_cycle()
            if not worker._first_run_active:
                _fail("FIRST-RUN flag flipped in cycle 1 (backlog still 3 left)")
            if stats1["permanent_failed_this_cycle"] != 2:
                _fail(f"cycle 1 permanents: {stats1['permanent_failed_this_cycle']}")
            # Phase 2 DOES run in cycle 1 — Phase 1 + Phase 2 happen in same cycle
            if stats1["sim_attempted_this_cycle"] != 2:
                _fail(f"cycle 1 sim_attempted: {stats1['sim_attempted_this_cycle']} "
                      f"(expected 2 — Phase 2 runs alongside Phase 1)")
            worker._cycles_completed += 1

            # Cycle 2: 3 permanents at start > max=2; backlog NOT cleared
            stats2 = worker._run_cycle()
            if not worker._first_run_active:
                _fail("FIRST-RUN flipped in cycle 2 (backlog still 1 left)")
            if stats2["permanent_failed_this_cycle"] != 2:
                _fail(f"cycle 2 permanents: {stats2['permanent_failed_this_cycle']}")
            worker._cycles_completed += 1

            # Cycle 3: 1 permanent at start <= max=2; this cycle CLEARS backlog
            stats3 = worker._run_cycle()
            if worker._first_run_active:
                _fail("FIRST-RUN should have flipped in cycle 3 (backlog cleared)")
            if cap.transition_log_count() != 1:
                _fail(f"transition log fired {cap.transition_log_count()} times "
                      "(expected exactly 1)")
            worker._cycles_completed += 1

            # Cycle 4 — queue empty, must NOT re-fire transition log
            stats4 = worker._run_cycle()
            if cap.transition_log_count() != 1:
                _fail(f"transition log re-fired in cycle 4 "
                      f"(total: {cap.transition_log_count()})")

        _ok(
            f"FIRST-RUN over cycles 1-2 (backlog still > max), "
            f"flipped to STEADY at cycle 3 when permanents_at_start <= max, "
            f"transition log fired exactly 1 time across 4 cycles"
        )


if __name__ == "__main__":
    print("Phase D.2 tests — per-cycle dispatcher")
    test_1_classifier_identifies_permanent_shapes()
    test_2_batched_flush_permanent_fastpath()
    test_3_transient_retry_cap()
    test_4_real_sim_outcome_classification()
    test_5_first_run_to_steady_mode_flip()
    print("\nALL D.2 TESTS PASSED")
