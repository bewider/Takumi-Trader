"""Phase F.1 tests — throttled flush for hot-path mutations.

Diagnosed 2026-05-14: ShadowLogger's per-call atomic flush blocked the
main thread for ~500ms once the journal grew past 90 MB. log_signal,
log_strength_reject, and mark_decision fire on the hot signal-evaluation
path. F.1 added a force_flush=False default + throttle window
(_FLUSH_THROTTLE_SEC=30s) so the hot path stays in memory and disk
flushes happen at most every 30 seconds.

Tests:
  1. log_signal with force_flush=False: many calls in <30s -> only
     first call flushes; subsequent calls leave records in memory
  2. log_signal with force_flush=True: every call flushes
  3. log_strength_reject throttles the same way
  4. mark_decision throttles
  5. mark_executed always flushes (durability for exec_ref linkage)
  6. force_flush() public method always flushes
  7. After throttle window elapses, next call DOES flush
  8. Backward-compat: existing _sim_update_last_flush alias still works
     for Phase D worker code

Run from repo root:
    python scripts/test_phase_f1_throttled_flush.py
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

from takumi_trader.core.shadow_logger import (  # noqa: E402
    ShadowLogger, ShadowSignalRecord,
    STATUS_BLOCKED, STATUS_EXECUTED,
    GATE_DIVERGENCE_SPREAD, LANE_PAPER,
)


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


def _disk_record_count(path: Path) -> int:
    """Read journal from disk and return record count."""
    if not path.exists():
        return 0
    try:
        return len(json.loads(path.read_text(encoding="utf-8")))
    except Exception:
        return 0


# ─────────────────────────────────────────────────────────────────────
# Test 1 — log_signal: hot-path throttled by default
# ─────────────────────────────────────────────────────────────────────

def test_1_log_signal_throttled():
    print("\n[1] log_signal: rapid calls -> first flushes, rest stay in memory")
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "shadow.json"
        log = ShadowLogger("Sv2", path)
        # log._last_flush starts at 0.0 — first call exceeds 30s window
        # so it WILL flush.
        log.log_signal(
            pair="EURUSD", direction="BUY",
            proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
            proposed_sl_pips=10.0, proposed_tp_pips=20.0,
        )
        if _disk_record_count(path) != 1:
            _fail(f"first log_signal should flush; disk count = {_disk_record_count(path)}")

        # Now log 5 more rapidly within 30s — none should flush
        for i in range(5):
            log.log_signal(
                pair="EURUSD", direction="BUY",
                proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
                proposed_sl_pips=10.0, proposed_tp_pips=20.0,
            )
        # In-memory has 6 records, disk still has 1
        if len(log._journal) != 6:
            _fail(f"in-memory expected 6, got {len(log._journal)}")
        if _disk_record_count(path) != 1:
            _fail(
                f"disk count after 5 throttled calls should still be 1, "
                f"got {_disk_record_count(path)} — throttle not honored"
            )
        _ok("first call flushed, next 5 within window stayed in memory only")


# ─────────────────────────────────────────────────────────────────────
# Test 2 — log_signal with force_flush=True always flushes
# ─────────────────────────────────────────────────────────────────────

def test_2_log_signal_force_flush():
    print("\n[2] log_signal force_flush=True: every call flushes immediately")
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "shadow.json"
        log = ShadowLogger("Sv2", path)
        # First call (force_flush=True) flushes
        log.log_signal(
            pair="EURUSD", direction="BUY",
            proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
            proposed_sl_pips=10.0, proposed_tp_pips=20.0,
            force_flush=True,
        )
        # 4 more, all force_flush=True
        for _ in range(4):
            log.log_signal(
                pair="EURUSD", direction="BUY",
                proposed_entry=1.10, proposed_sl_price=1.099,
                proposed_tp_price=1.102,
                proposed_sl_pips=10.0, proposed_tp_pips=20.0,
                force_flush=True,
            )
        if _disk_record_count(path) != 5:
            _fail(f"force_flush=True should flush every call; disk={_disk_record_count(path)}")
        _ok("force_flush=True flushed all 5 calls")


# ─────────────────────────────────────────────────────────────────────
# Test 3 — log_strength_reject throttles
# ─────────────────────────────────────────────────────────────────────

def test_3_log_strength_reject_throttled():
    print("\n[3] log_strength_reject: throttled by default (hot path)")
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "shadow.json"
        log = ShadowLogger("Sv2", path)

        def _reject(pair):
            log.log_strength_reject(
                pair=pair, direction="BUY",
                reason="strength M5 trend weak",
                m5_base=5.0, m5_quote=5.0,
                m15_base=5.0, m15_quote=5.0,
                h1_base=5.0, h1_quote=5.0,
                h4_base=5.0, h4_quote=5.0,
                d1_base=5.0, d1_quote=5.0,
                spread_points=0.5, m5_atr_pips=2.0, h1_atr_pips=15.0,
                usd_score=5.0, ccy_dispersion=1.5, session="normal",
            )

        # First call flushes (no prior flush)
        _reject("EURUSD")
        if _disk_record_count(path) != 1:
            _fail("first reject should flush")
        # 9 more in rapid succession — should stay in memory
        for i in range(9):
            _reject("EURUSD")
        if len(log._journal) != 10:
            _fail(f"in-memory expected 10, got {len(log._journal)}")
        if _disk_record_count(path) != 1:
            _fail(f"disk should still be 1 (throttled), got {_disk_record_count(path)}")
        _ok("first call flushed, next 9 rejects stayed in memory")


# ─────────────────────────────────────────────────────────────────────
# Test 4 — mark_decision throttles
# ─────────────────────────────────────────────────────────────────────

def test_4_mark_decision_throttled():
    print("\n[4] mark_decision: throttled by default")
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "shadow.json"
        log = ShadowLogger("Sv2", path)
        # Seed with one log_signal (force_flush=True so disk is up to date)
        sid = log.log_signal(
            pair="EURUSD", direction="BUY",
            proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
            proposed_sl_pips=10.0, proposed_tp_pips=20.0,
            force_flush=True,
            input_snapshot={"composite_scores": {}},
        )
        # On-disk: 1 record, status=PENDING
        on_disk_before = json.loads(path.read_text(encoding="utf-8"))
        if on_disk_before[0].get("status", "PENDING") != "PENDING":
            _fail(f"pre-mark status: {on_disk_before[0].get('status')}")

        # mark_decision without force_flush — in-memory updated but disk untouched
        log.mark_decision(sid, STATUS_BLOCKED, gate=GATE_DIVERGENCE_SPREAD,
                          reason="test")
        # In-memory: status updated
        if log._journal[0].status != STATUS_BLOCKED:
            _fail(f"in-memory status not updated: {log._journal[0].status}")
        # On-disk: still PENDING (throttle held)
        on_disk_after = json.loads(path.read_text(encoding="utf-8"))
        status = on_disk_after[0].get("status", "PENDING")
        if status == STATUS_BLOCKED:
            _fail(
                "disk should still show PENDING after throttled mark_decision, "
                f"got {status} — throttle not honored"
            )
        _ok("mark_decision updated in-memory; disk stayed at pre-mark state")


# ─────────────────────────────────────────────────────────────────────
# Test 5 — mark_executed always flushes (durability)
# ─────────────────────────────────────────────────────────────────────

def test_5_mark_executed_always_flushes():
    print("\n[5] mark_executed: always flushes (durability for exec_ref)")
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "shadow.json"
        log = ShadowLogger("Sv2", path)
        sid = log.log_signal(
            pair="EURUSD", direction="BUY",
            proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
            proposed_sl_pips=10.0, proposed_tp_pips=20.0,
            input_snapshot={"composite_scores": {}},
        )
        # mark_executed should flush even without force_flush param
        log.mark_executed(sid, lane=LANE_PAPER,
                          ref={"system": "Sv2", "journal_idx": 42})
        # On-disk: must show EXECUTED + exec_ref_json
        on_disk = json.loads(path.read_text(encoding="utf-8"))
        if on_disk[0].get("status", "") != STATUS_EXECUTED:
            _fail(f"disk status should be EXECUTED, got {on_disk[0].get('status')}")
        if "journal_idx" not in on_disk[0].get("exec_ref_json", ""):
            _fail(f"disk exec_ref_json not persisted: {on_disk[0]}")
        _ok("mark_executed flushed immediately — exec_ref durable on disk")


# ─────────────────────────────────────────────────────────────────────
# Test 6 — force_flush() public method
# ─────────────────────────────────────────────────────────────────────

def test_6_force_flush_public():
    print("\n[6] force_flush(): commits all pending in-memory mutations")
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "shadow.json"
        log = ShadowLogger("Sv2", path)
        # Seed with one (forced)
        log.log_signal(
            pair="EURUSD", direction="BUY",
            proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
            proposed_sl_pips=10.0, proposed_tp_pips=20.0,
            force_flush=True,
        )
        # 5 more throttled
        for _ in range(5):
            log.log_signal(
                pair="EURUSD", direction="BUY",
                proposed_entry=1.10, proposed_sl_price=1.099,
                proposed_tp_price=1.102,
                proposed_sl_pips=10.0, proposed_tp_pips=20.0,
            )
        # disk has 1, in-memory has 6
        if _disk_record_count(path) != 1:
            _fail("setup: disk should be 1")
        # Explicit force_flush -> disk now has 6
        log.force_flush()
        if _disk_record_count(path) != 6:
            _fail(f"after force_flush, disk should be 6, got {_disk_record_count(path)}")
        _ok("force_flush() committed all 6 in-memory records to disk")


# ─────────────────────────────────────────────────────────────────────
# Test 7 — throttle window elapses -> next call flushes
# ─────────────────────────────────────────────────────────────────────

def test_7_throttle_window_elapses():
    print("\n[7] throttle window elapses -> next call flushes")
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "shadow.json"
        log = ShadowLogger("Sv2", path)
        # Use a short throttle for the test by monkey-patching the class const
        original = ShadowLogger._FLUSH_THROTTLE_SEC
        ShadowLogger._FLUSH_THROTTLE_SEC = 0.5  # half-second window
        try:
            # First call flushes (initial state)
            log.log_signal(
                pair="EURUSD", direction="BUY",
                proposed_entry=1.10, proposed_sl_price=1.099,
                proposed_tp_price=1.102,
                proposed_sl_pips=10.0, proposed_tp_pips=20.0,
            )
            if _disk_record_count(path) != 1:
                _fail("first call should flush")
            # Within window, throttled
            log.log_signal(
                pair="EURUSD", direction="BUY",
                proposed_entry=1.10, proposed_sl_price=1.099,
                proposed_tp_price=1.102,
                proposed_sl_pips=10.0, proposed_tp_pips=20.0,
            )
            if _disk_record_count(path) != 1:
                _fail("within-window call should be throttled")
            # Wait past the window
            time.sleep(0.6)
            # Next call should flush
            log.log_signal(
                pair="EURUSD", direction="BUY",
                proposed_entry=1.10, proposed_sl_price=1.099,
                proposed_tp_price=1.102,
                proposed_sl_pips=10.0, proposed_tp_pips=20.0,
            )
            if _disk_record_count(path) != 3:
                _fail(
                    f"after window, next call should flush all 3; "
                    f"disk={_disk_record_count(path)}"
                )
            _ok("throttle window elapsed -> next call flushed all pending records")
        finally:
            ShadowLogger._FLUSH_THROTTLE_SEC = original


# ─────────────────────────────────────────────────────────────────────
# Test 8 — backward-compat alias _sim_update_last_flush still works
# ─────────────────────────────────────────────────────────────────────

def test_8_backward_compat_alias():
    print("\n[8] backward-compat: _sim_update_last_flush alias tracks _last_flush")
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "shadow.json"
        log = ShadowLogger("Sv2", path)
        log.log_signal(
            pair="EURUSD", direction="BUY",
            proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
            proposed_sl_pips=10.0, proposed_tp_pips=20.0,
            force_flush=True,
        )
        # After a flush, both should be ~same time
        diff = abs(log._last_flush - log._sim_update_last_flush)
        if diff > 0.1:
            _fail(
                f"alias drift: _last_flush={log._last_flush}, "
                f"_sim_update_last_flush={log._sim_update_last_flush}, diff={diff:.3f}"
            )
        if log._last_flush <= 0:
            _fail(f"_last_flush should be set, got {log._last_flush}")
        _ok(f"_last_flush and _sim_update_last_flush stay in sync (diff={diff*1000:.1f}ms)")


# ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 64)
    print("Phase F.1 — throttled flush for hot-path mutations")
    print("=" * 64)
    test_1_log_signal_throttled()
    test_2_log_signal_force_flush()
    test_3_log_strength_reject_throttled()
    test_4_mark_decision_throttled()
    test_5_mark_executed_always_flushes()
    test_6_force_flush_public()
    test_7_throttle_window_elapses()
    test_8_backward_compat_alias()
    print("\n" + "=" * 64)
    print("ALL F.1 TESTS PASSED")
    print("=" * 64)


if __name__ == "__main__":
    main()
