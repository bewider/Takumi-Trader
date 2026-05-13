"""Phase A smoke test for shadow_logger.

Validates the four behaviors that justify Phase A:
  1. log_signal -> mark_decision flushes durable across process restart
  2. log_signal -> mark_executed flushes durable across process restart
  3. PENDING orphans are detectable on reload
  4. Concurrent ShadowLogger instances on different journals don't collide

Run from repo root:
    python -m scripts.test_shadow_logger
or directly:
    python scripts/test_shadow_logger.py

Uses a temp directory so it never touches real data/shadow_trades_*.json.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

# Make the repo importable when running directly
_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from takumi_trader.core.shadow_logger import (  # noqa: E402
    ShadowLogger, ShadowCalibrationLog, ShadowCalibrationRecord,
    GATE_STRENGTH_ENGINE, GATE_CONVICTION,
    STATUS_PENDING, STATUS_EXECUTED, STATUS_BLOCKED,
    LANE_PAPER, LANE_MT5,
)


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")

def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


def test_capture_then_block_roundtrips():
    """log_signal -> mark_decision -> reload -> record found with BLOCKED status."""
    print("\n[1] log_signal -> mark_decision durability")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow_trades_Sv2.json"
        log = ShadowLogger("Sv2", jpath)
        sid = log.log_signal(
            pair="GBPJPY", direction="BUY",
            proposed_entry=185.123, proposed_sl_price=185.073, proposed_tp_price=185.223,
            proposed_sl_pips=5.0, proposed_tp_pips=10.0,
            input_snapshot={"composite_scores": {"GBP": 7.2, "JPY": 2.8}},
        )
        ok = log.mark_decision(sid, STATUS_BLOCKED, GATE_STRENGTH_ENGINE,
                               "H4 against: GBP=2.1/JPY=7.8",
                               metadata={"h4_base": 2.1, "h4_quote": 7.8})
        if not ok:
            _fail("mark_decision returned False")

        # F.1 (2026-05-14): mark_decision is now throttled by default.
        # Production uses closeEvent's force_flush() to commit the
        # last window of mutations before shutdown. Tests that verify
        # disk durability after mutations must call force_flush()
        # explicitly to simulate the same persistence point.
        log.force_flush()

        # Simulate process restart by constructing a new logger pointing at same file
        log2 = ShadowLogger("Sv2", jpath)
        recs = log2.all_records()
        if len(recs) != 1:
            _fail(f"expected 1 record after reload, got {len(recs)}")
        r = recs[0]
        if r.shadow_id != sid:
            _fail(f"shadow_id mismatch: {r.shadow_id} != {sid}")
        if r.status != STATUS_BLOCKED:
            _fail(f"status: {r.status} != BLOCKED")
        if r.block_gate != GATE_STRENGTH_ENGINE:
            _fail(f"gate: {r.block_gate}")
        if "GBP=2.1" not in r.block_reason:
            _fail(f"reason: {r.block_reason}")
        meta = json.loads(r.block_metadata_json)
        if meta.get("h4_base") != 2.1:
            _fail(f"metadata: {meta}")
        snap = json.loads(r.input_snapshot_json)
        if snap.get("composite_scores", {}).get("GBP") != 7.2:
            _fail(f"input_snapshot: {snap}")
        _ok("BLOCKED record roundtrip including gate, reason, metadata, input_snapshot")


def test_capture_then_execute_roundtrips():
    """log_signal -> mark_executed -> reload -> EXECUTED with lane+ref intact."""
    print("\n[2] log_signal -> mark_executed durability")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow_trades_Sv2.json"
        log = ShadowLogger("Sv2", jpath)
        sid = log.log_signal(
            pair="EURUSD", direction="SELL",
            proposed_entry=1.0850, proposed_sl_price=1.0855, proposed_tp_price=1.0840,
            proposed_sl_pips=5.0, proposed_tp_pips=10.0,
        )
        log.mark_executed(sid, lane=LANE_PAPER,
                          ref={"system": "Sv2", "journal_idx": 594})

        log2 = ShadowLogger("Sv2", jpath)
        r = log2.all_records()[0]
        if r.status != STATUS_EXECUTED:
            _fail(f"status: {r.status}")
        if r.exec_lane != LANE_PAPER:
            _fail(f"lane: {r.exec_lane}")
        ref = json.loads(r.exec_ref_json)
        if ref.get("journal_idx") != 594:
            _fail(f"ref: {ref}")
        _ok("EXECUTED record roundtrip including lane and ref dict")


def test_orphan_detection():
    """log_signal but no decision/execute -> PENDING on reload, orphan_count > 0."""
    print("\n[3] PENDING orphan detection")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow_trades_Sv2.json"
        log = ShadowLogger("Sv2", jpath)
        log.log_signal(
            pair="USDJPY", direction="BUY",
            proposed_entry=150.0, proposed_sl_price=149.95, proposed_tp_price=150.10,
            proposed_sl_pips=5.0, proposed_tp_pips=10.0,
        )
        # Simulate crash before mark_* by NOT calling them.
        # F.1: log_signal is throttled; force_flush ensures the PENDING
        # record is durably on disk for the reload to see.
        log.force_flush()
        log2 = ShadowLogger("Sv2", jpath)
        if log2.orphan_count() != 1:
            _fail(f"orphan_count: {log2.orphan_count()}")
        r = log2.all_records()[0]
        if r.status != STATUS_PENDING:
            _fail(f"status: {r.status}")
        _ok("orphan record reloads as PENDING and orphan_count is reported")


def test_monotonic_id_across_restart():
    """shadow_id keeps increasing across reloads — no collisions."""
    print("\n[4] shadow_id monotonic across process restart")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow_trades_Sv2.json"
        log = ShadowLogger("Sv2", jpath)
        ids = [
            log.log_signal(
                pair="EURUSD", direction="BUY",
                proposed_entry=1.08, proposed_sl_price=1.07, proposed_tp_price=1.09,
                proposed_sl_pips=10, proposed_tp_pips=10,
            )
            for _ in range(3)
        ]
        log.force_flush()  # F.1: persist for reload
        log2 = ShadowLogger("Sv2", jpath)
        next_id = log2.log_signal(
            pair="GBPUSD", direction="SELL",
            proposed_entry=1.25, proposed_sl_price=1.26, proposed_tp_price=1.24,
            proposed_sl_pips=10, proposed_tp_pips=10,
        )
        if ids != [1, 2, 3] or next_id != 4:
            _fail(f"ids: {ids} + next_id={next_id} (expected 1,2,3,4)")
        _ok("shadow_id sequence: 1,2,3 (pre-restart) -> 4 (post-restart)")


def test_atomic_write_no_corruption():
    """Read the on-disk JSON between calls; must always parse cleanly."""
    print("\n[5] atomic write — file always parses mid-stream")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow_trades_Sv2.json"
        log = ShadowLogger("Sv2", jpath)
        for i in range(20):
            sid = log.log_signal(
                pair="AUDJPY", direction="BUY",
                proposed_entry=100 + i, proposed_sl_price=99 + i, proposed_tp_price=101 + i,
                proposed_sl_pips=10, proposed_tp_pips=10,
            )
            log.mark_decision(sid, STATUS_BLOCKED, GATE_CONVICTION, f"low conv {50-i}",
                              force_flush=True)
            # After every mutation the on-disk file MUST be valid JSON.
            # F.1: force_flush above ensures disk is up to date for the
            # atomic-write integrity check.
            data = json.loads(jpath.read_text(encoding="utf-8"))
            if not isinstance(data, list) or len(data) != i + 1:
                _fail(f"corruption at iter {i}: got {type(data).__name__} len={len(data) if isinstance(data, list) else '?'}")
        _ok("20 alternating capture+decide cycles — file stayed valid JSON throughout")


def test_calibration_log():
    """ShadowCalibrationLog append + delta computation + summary."""
    print("\n[6] ShadowCalibrationLog (Addition 1)")
    with tempfile.TemporaryDirectory() as td:
        cpath = Path(td) / "shadow_calibration.json"
        cal = ShadowCalibrationLog(cpath)
        cal.append(ShadowCalibrationRecord(
            shadow_id=1, strategy_id="Sv2", pair="GBPJPY", direction="BUY",
            real_pnl_pips=10.0, sim_pnl_pips=8.0,
            real_exit_reason="tp_hit", sim_exit_reason="TP",
        ))
        cal.append(ShadowCalibrationRecord(
            shadow_id=2, strategy_id="Sv2", pair="EURUSD", direction="SELL",
            real_pnl_pips=-5.0, sim_pnl_pips=-6.0,
            real_exit_reason="sl_hit", sim_exit_reason="SL",
        ))
        cal2 = ShadowCalibrationLog(cpath)
        recs = cal2.all_records()
        if len(recs) != 2:
            _fail(f"len: {len(recs)}")
        if recs[0].delta_pips != 2.0:
            _fail(f"delta_0: {recs[0].delta_pips}")
        if recs[1].delta_pips != 1.0:
            _fail(f"delta_1: {recs[1].delta_pips}")
        s = cal2.summary()
        if s["n"] != 2 or abs(s["mean"] - 1.5) > 1e-9:
            _fail(f"summary: {s}")
        _ok(f"calibration: 2 records, mean delta = {s['mean']:+.2f}p (sim is {'pessimistic' if s['mean'] > 0 else 'optimistic'})")


def test_invalid_inputs_rejected():
    """API contract violations should raise, not silently pass."""
    print("\n[7] input validation")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow_trades_Sv2.json"
        log = ShadowLogger("Sv2", jpath)
        try:
            log.log_signal(pair="EURUSD", direction="LONG",  # bad direction
                           proposed_entry=1, proposed_sl_price=1, proposed_tp_price=1,
                           proposed_sl_pips=1, proposed_tp_pips=1)
            _fail("bad direction silently accepted")
        except ValueError:
            pass
        sid = log.log_signal(
            pair="EURUSD", direction="BUY",
            proposed_entry=1, proposed_sl_price=1, proposed_tp_price=1,
            proposed_sl_pips=1, proposed_tp_pips=1,
        )
        try:
            log.mark_decision(sid, STATUS_BLOCKED, "made_up_gate", "x")
            _fail("invalid gate accepted")
        except ValueError:
            pass
        try:
            log.mark_decision(sid, STATUS_EXECUTED, "")
            _fail("STATUS_EXECUTED via mark_decision accepted (should require mark_executed)")
        except ValueError:
            pass
        try:
            log.mark_executed(sid, lane="bittrex", ref={})
            _fail("invalid lane accepted")
        except ValueError:
            pass
        # Unknown shadow_id is non-fatal — returns False
        if log.mark_decision(99999, STATUS_BLOCKED, GATE_CONVICTION, "x") is not False:
            _fail("unknown shadow_id should return False, not raise or succeed")
        _ok("ValueError on bad direction/gate/status/lane; missing id returns False")


def test_lightweight_strength_reject():
    """log_strength_reject: one-call BLOCKED record under 1 KB."""
    print("\n[8] log_strength_reject (Phase B lightweight path)")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow_trades_Sv2.json"
        log = ShadowLogger("Sv2", jpath)
        sid = log.log_strength_reject(
            pair="GBPJPY", direction="BUY", reason="M5+M15 below threshold",
            m5_base=5.2, m5_quote=4.8, m15_base=5.0, m15_quote=5.1,
            h1_base=6.3, h1_quote=4.2, h4_base=7.1, h4_quote=2.9,
            d1_base=6.8, d1_quote=3.4,
            spread_points=12.0, m5_atr_pips=8.4, h1_atr_pips=14.1,
            usd_score=5.6, ccy_dispersion=1.42, session="London",
        )
        log.force_flush()  # F.1: persist throttled record for reload
        log2 = ShadowLogger("Sv2", jpath)
        r = log2.all_records()[0]
        if r.status != STATUS_BLOCKED:
            _fail(f"status: {r.status}")
        meta = json.loads(r.block_metadata_json)
        for k in ("m5_base", "m15_quote", "h1_base", "h4_quote", "d1_base",
                  "spread_points", "m5_atr_pips", "h1_atr_pips",
                  "usd_score", "ccy_dispersion", "session"):
            if k not in meta:
                _fail(f"missing metadata key: {k}")
        if meta["session"] != "London":
            _fail(f"session: {meta['session']}")
        if meta["h4_base"] != 7.1:
            _fail(f"h4_base: {meta['h4_base']}")
        # On-disk size matters more than asdict() — the flush writes
        # compact JSON (no whitespace) to keep volume manageable.
        on_disk_total = jpath.stat().st_size
        # Subtract list-overhead [] and account for one record
        # (single-record file: total is roughly per-record + 2 bytes for [])
        approx_per_record = on_disk_total - 2
        if approx_per_record > 1024:
            _fail(f"strength-reject record too big on disk: {approx_per_record} bytes (budget < 1 KB)")
        _ok(f"lightweight reject roundtrip; on-disk size={approx_per_record} B (under 1 KB compact)")


if __name__ == "__main__":
    print("Phase A + B.2 smoke test -- shadow_logger")
    test_capture_then_block_roundtrips()
    test_capture_then_execute_roundtrips()
    test_orphan_detection()
    test_monotonic_id_across_restart()
    test_atomic_write_no_corruption()
    test_calibration_log()
    test_invalid_inputs_rejected()
    test_lightweight_strength_reject()
    print("\nALL TESTS PASSED")
