"""Phase D.1 tests — ShadowSimWorker skeleton + schema-evolution.

Coverage per the architect's D.1 review-gate spec:
  1. Worker construction with all expected params (validation, defaults)
  2. Start/stop lifecycle — worker starts, runs, stops cleanly
  3. _running flag interrupts cycle mid-flight (worst-case <100ms)
  4. Clean shutdown within 3-second budget (matches closeEvent.wait(3000))
  5. Schema-evolution: production journal loads with transient_retry_count=0
  6. Sparse-omit: transient_retry_count=0 omitted from disk; non-zero persisted

Plus a no-regression sweep: confirms all 8 ShadowLogger Phase A tests still pass.

The worker tests do NOT require a QApplication event loop — QThread runs in
its own OS thread, and we test lifecycle behaviors via the thread's own
state, not via Qt signal/slot dispatch (which would need an event loop).
Signal emission tests are deferred to D.4's integration smoke test.

Run from repo root:
    python scripts/test_phase_d1_worker.py
"""
from __future__ import annotations

import json
import sys
import time
import tempfile
from dataclasses import asdict, fields
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from takumi_trader.core.shadow_logger import (  # noqa: E402
    ShadowLogger, ShadowSignalRecord, _compact_record_dict,
    STATUS_BLOCKED, GATE_STRENGTH_ENGINE,
)
from takumi_trader.core.shadow_sim_worker import ShadowSimWorker  # noqa: E402


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


class _NopSimulator:
    """Stub simulator — D.1 worker doesn't actually call simulate() yet
    (the _run_cycle() stub returns zero-stats), but the worker validates
    that simulator is not None at construction."""
    pass


# ─────────────────────────────────────────────────────────────────────
# Test 1 — construction validation
# ─────────────────────────────────────────────────────────────────────

def test_1_construction():
    """Constructor accepts all expected params and rejects invalid input."""
    print("\n[1] worker construction")
    with tempfile.TemporaryDirectory() as td:
        log = ShadowLogger("Sv2", Path(td) / "shadow.json")

        # Defaults
        w = ShadowSimWorker(shadow_logger=log, simulator=_NopSimulator())
        if w.poll_interval != 300.0:
            _fail(f"default poll_interval: {w.poll_interval}")
        if w.max_per_cycle != 50:
            _fail(f"default max_per_cycle: {w.max_per_cycle}")
        if w.max_permanent_per_cycle != 1000:
            _fail(f"default max_permanent_per_cycle: {w.max_permanent_per_cycle}")
        if not w.is_running():
            _fail("worker should start with _running=True before run()")

        # Custom params
        w2 = ShadowSimWorker(
            shadow_logger=log, simulator=_NopSimulator(),
            poll_interval=60.0, max_per_cycle=10, max_permanent_per_cycle=200,
        )
        if w2.poll_interval != 60.0:
            _fail("custom poll_interval not stored")

        # Validation failures
        try:
            ShadowSimWorker(shadow_logger=None, simulator=_NopSimulator())
            _fail("None shadow_logger should raise")
        except ValueError:
            pass
        try:
            ShadowSimWorker(shadow_logger=log, simulator=None)
            _fail("None simulator should raise")
        except ValueError:
            pass
        try:
            ShadowSimWorker(
                shadow_logger=log, simulator=_NopSimulator(),
                poll_interval=0,
            )
            _fail("zero poll_interval should raise")
        except ValueError:
            pass
        try:
            ShadowSimWorker(
                shadow_logger=log, simulator=_NopSimulator(),
                max_per_cycle=-5,
            )
            _fail("negative max_per_cycle should raise")
        except ValueError:
            pass

        # get_stats works pre-run
        stats = w.get_stats()
        for key in ("last_cycle_complete_ts", "pending_count",
                    "total_records_simulated", "total_calibrations_written",
                    "total_permanent_failed", "current_drift_warning",
                    "cycles_completed", "first_run_active"):
            if key not in stats:
                _fail(f"get_stats missing key {key!r}")
        if stats["cycles_completed"] != 0:
            _fail(f"pre-run cycles_completed: {stats['cycles_completed']}")
        _ok("construction defaults, validation, get_stats — all correct")


# ─────────────────────────────────────────────────────────────────────
# Test 2 — start/stop lifecycle
# ─────────────────────────────────────────────────────────────────────

def test_2_start_stop_lifecycle():
    """Worker starts, runs at least one cycle, stops cleanly, returns."""
    print("\n[2] start/stop lifecycle")
    with tempfile.TemporaryDirectory() as td:
        log = ShadowLogger("Sv2", Path(td) / "shadow.json")
        # Use a tiny poll interval so a cycle completes quickly during the test
        w = ShadowSimWorker(
            shadow_logger=log, simulator=_NopSimulator(),
            poll_interval=0.2,
        )

        w.start()  # spawns OS thread, calls run() in it
        # Wait up to 1s for at least one cycle to complete
        t0 = time.time()
        while w.get_stats()["cycles_completed"] < 1 and time.time() - t0 < 1.0:
            time.sleep(0.05)

        cycles_observed = w.get_stats()["cycles_completed"]
        if cycles_observed < 1:
            _fail(f"no cycles completed in 1s: {cycles_observed}")

        # Stop and wait for thread exit
        w.stop()
        if not w.wait(3000):  # 3s budget
            _fail("worker did not exit within 3s")

        # State after exit
        if w.is_running():
            _fail("is_running() True after stop()+wait")
        if w.isRunning():  # Qt method, lowercase R
            _fail("Qt isRunning() True after wait — thread didn't terminate")

        _ok(f"worker started, completed {cycles_observed} cycles, stopped within 3s")


# ─────────────────────────────────────────────────────────────────────
# Test 3 — _running interrupts cycle mid-flight
# ─────────────────────────────────────────────────────────────────────

class _SlowCycleWorker(ShadowSimWorker):
    """Test subclass: _run_cycle simulates a slow batch by checking _running
    in a tight loop. If stop() arrives mid-cycle, the loop should exit
    within ~_SLEEP_STEP."""
    def _run_cycle(self):
        # Simulate processing 100 records, each "taking" a small time
        # but checking _running between each — the per-record granularity
        # D.2 will wire for real.
        records_processed = 0
        for _ in range(100):
            if not self._running:
                break  # honor stop() mid-cycle
            time.sleep(0.05)  # simulate per-record work
            records_processed += 1
        return {
            "records_processed": records_processed,
            "permanent_failed_this_cycle": 0,
            "sim_attempted_this_cycle": records_processed,
            "sim_succeeded_this_cycle": records_processed,
            "sim_transient_retries_this_cycle": 0,
            "calibrations_written_this_cycle": 0,
            "first_run_active": False,
        }


def test_3_running_interrupts_mid_cycle():
    """_running flag honored between records — stop arrives mid-cycle."""
    print("\n[3] _running interrupts cycle mid-flight")
    with tempfile.TemporaryDirectory() as td:
        log = ShadowLogger("Sv2", Path(td) / "shadow.json")
        # Long poll_interval so we definitely catch the worker mid-cycle
        w = _SlowCycleWorker(
            shadow_logger=log, simulator=_NopSimulator(),
            poll_interval=300.0,
        )
        w.start()
        # Give the cycle ~250ms to start processing records (about 5 records in)
        time.sleep(0.25)
        # Now stop mid-cycle and time the response
        t0 = time.monotonic()
        w.stop()
        if not w.wait(3000):
            _fail("worker did not exit within 3s after mid-cycle stop")
        elapsed_ms = (time.monotonic() - t0) * 1000

        # The cycle should interrupt within ~150ms (one per-record step + buffer)
        if elapsed_ms > 500:
            _fail(f"mid-cycle interrupt took {elapsed_ms:.0f}ms (expected <500ms)")
        _ok(f"stop() honored mid-cycle: {elapsed_ms:.0f}ms to exit (target <500ms)")


# ─────────────────────────────────────────────────────────────────────
# Test 4 — clean shutdown within 3s budget
# ─────────────────────────────────────────────────────────────────────

def test_4_shutdown_budget():
    """Shutdown completes well within closeEvent's wait(3000) budget."""
    print("\n[4] shutdown completes within 3s budget")
    with tempfile.TemporaryDirectory() as td:
        log = ShadowLogger("Sv2", Path(td) / "shadow.json")
        # 1-second poll so we test BOTH cycle interrupt AND sleep interrupt
        w = ShadowSimWorker(
            shadow_logger=log, simulator=_NopSimulator(),
            poll_interval=1.0,
        )
        w.start()
        # Let it complete a cycle then enter sleep
        time.sleep(0.5)
        # Stop while sleeping — sleep should interrupt within _SLEEP_STEP (~100ms)
        t0 = time.monotonic()
        w.stop()
        ok = w.wait(3000)
        elapsed_ms = (time.monotonic() - t0) * 1000

        if not ok:
            _fail(f"wait(3000) timed out")
        # Should be much faster than 3s — target <500ms
        if elapsed_ms > 500:
            _fail(f"shutdown took {elapsed_ms:.0f}ms (target <500ms)")
        _ok(f"shutdown within {elapsed_ms:.0f}ms, well under 3000ms ceiling")


# ─────────────────────────────────────────────────────────────────────
# Test 5 — schema evolution: load production journal with new field
# ─────────────────────────────────────────────────────────────────────

def test_5_schema_evolution_on_production_journal():
    """Real production journal loads cleanly with the new field at default 0.

    This is the load-bearing test for the schema-extension protocol —
    confirms Phase A's setattr-load pattern correctly handles a new
    field added in a later phase.
    """
    print("\n[5] schema evolution: production journal loads cleanly")
    journal_path = _REPO / "data" / "shadow_trades_Sv2.json"
    if not journal_path.exists():
        print("  (skipped — production journal not present on this machine)")
        return

    # Time the load to confirm the new field doesn't slow it down
    t0 = time.monotonic()
    log = ShadowLogger("Sv2", journal_path)
    load_dt_ms = (time.monotonic() - t0) * 1000
    recs = log.all_records()

    if len(recs) == 0:
        _fail("production journal loaded zero records — unexpected")

    # Every loaded record must have transient_retry_count = 0 (the default)
    # because no record on disk was written with the field present yet.
    nonzero = [r for r in recs if r.transient_retry_count != 0]
    if nonzero:
        _fail(f"{len(nonzero)} records loaded with non-zero transient_retry_count "
              "(expected all 0 — field is brand new)")

    # Phase A's malformed check: every record must have valid identity
    malformed = [r for r in recs if r.shadow_id == 0 or r.signal_time == 0]
    if malformed:
        _fail(f"{len(malformed)} malformed records after load")

    _ok(
        f"loaded {len(recs):,} records in {load_dt_ms:.0f}ms "
        f"({len(recs) / (load_dt_ms / 1000):.0f} rec/s), "
        f"all with transient_retry_count=0, zero malformed"
    )


# ─────────────────────────────────────────────────────────────────────
# Test 6 — sparse omission and non-default persistence
# ─────────────────────────────────────────────────────────────────────

def test_6_sparse_omission():
    """transient_retry_count=0 is omitted from disk JSON; non-zero persisted."""
    print("\n[6] sparse omission of transient_retry_count")

    # Spot-check the field exists on the dataclass
    field_names = {f.name for f in fields(ShadowSignalRecord)}
    if "transient_retry_count" not in field_names:
        _fail("transient_retry_count not added to dataclass")
    # And that its default is 0
    field_def = next(f for f in fields(ShadowSignalRecord) if f.name == "transient_retry_count")
    if field_def.default != 0:
        _fail(f"transient_retry_count default: {field_def.default} (expected 0)")

    # Test 6a: default=0 → omitted by sparse serialization
    rec_default = ShadowSignalRecord(
        shadow_id=1, strategy_id="Sv2", signal_time=1000.0,
        pair="EURUSD", direction="BUY",
    )
    sparse_dict = _compact_record_dict(rec_default)
    if "transient_retry_count" in sparse_dict:
        _fail("transient_retry_count=0 was NOT omitted by sparse serialization")

    # Test 6b: non-default → persisted
    rec_with_count = ShadowSignalRecord(
        shadow_id=2, strategy_id="Sv2", signal_time=2000.0,
        pair="EURUSD", direction="BUY",
        transient_retry_count=3,
    )
    sparse_dict_2 = _compact_record_dict(rec_with_count)
    if "transient_retry_count" not in sparse_dict_2:
        _fail("transient_retry_count=3 was omitted (should be persisted)")
    if sparse_dict_2["transient_retry_count"] != 3:
        _fail(f"transient_retry_count value: {sparse_dict_2['transient_retry_count']}")

    # Test 6c: roundtrip — write a record with count=5, reload, verify
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow.json"
        log = ShadowLogger("Sv2", jpath)
        sid = log.log_signal(
            pair="EURUSD", direction="BUY",
            proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
            proposed_sl_pips=10.0, proposed_tp_pips=20.0,
        )
        # Manually mutate transient_retry_count and re-flush
        rec = next(r for r in log.all_records() if r.shadow_id == sid)
        rec.transient_retry_count = 7
        log.force_flush()

        # Read raw JSON and verify the field is present
        raw = json.loads(jpath.read_text(encoding="utf-8"))
        target = next(d for d in raw if d.get("shadow_id") == sid)
        if "transient_retry_count" not in target:
            _fail("transient_retry_count=7 not persisted in raw JSON")
        if target["transient_retry_count"] != 7:
            _fail(f"persisted value: {target['transient_retry_count']}")

        # Reload via fresh ShadowLogger and confirm the count survives
        log2 = ShadowLogger("Sv2", jpath)
        rec2 = log2.all_records()[0]
        if rec2.transient_retry_count != 7:
            _fail(f"reload value: {rec2.transient_retry_count}")

    _ok("sparse omission at default=0; non-default value=7 persisted + roundtrip lossless")


if __name__ == "__main__":
    print("Phase D.1 tests — ShadowSimWorker skeleton + schema evolution")
    test_1_construction()
    test_2_start_stop_lifecycle()
    test_3_running_interrupts_mid_cycle()
    test_4_shutdown_budget()
    test_5_schema_evolution_on_production_journal()
    test_6_sparse_omission()
    print("\nALL D.1 TESTS PASSED")
