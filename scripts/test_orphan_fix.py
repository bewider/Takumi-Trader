"""Synthetic verification of the orphan-PENDING fix.

Tests the fix's code path directly without instantiating MainWindow.
Constructs minimal mocks for the dependencies the fix touches:
  * shadow_logger (real ShadowLogger on a tempdir journal)
  * result (object with sv2_shadow_ids attr)
  * full_candidates (dict)
  * fired (set)
  * open_pairs (set)
  * trade_tracker (object exposing get_trade(pair))

5 tests per the architect's spec. All must pass for the verification gate
to close. Run from repo root:
    python scripts/test_orphan_fix.py
"""
from __future__ import annotations

import json
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from takumi_trader.core.shadow_logger import (  # noqa: E402
    ShadowLogger,
    STATUS_PENDING, STATUS_BLOCKED,
    GATE_DUPLICATE, GATE_INTERNAL,
)
from takumi_trader.core.shadow_orphan_marker import (  # noqa: E402
    mark_alert_mgr_orphans,
)


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")

def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


# ── Minimal mocks ────────────────────────────────────────────────────

@dataclass
class FakeTrade:
    """Mimics TrackedTrade's surface that mark_alert_mgr_orphans uses."""
    direction: str
    entry_price: float
    duration_minutes: float


class FakeTradeTracker:
    """Mimics TradeTracker.get_trade(). Holds a small dict of fake trades."""

    def __init__(self, trades: dict[str, FakeTrade] | None = None) -> None:
        self._trades = trades or {}

    def get_trade(self, pair: str) -> FakeTrade | None:
        return self._trades.get(pair)


class FakeResult:
    """Mimics CalculationResult — just the .sv2_shadow_ids attribute."""

    def __init__(self, sv2_shadow_ids: dict[str, int] | None = None) -> None:
        self.sv2_shadow_ids = sv2_shadow_ids or {}


def _seed_pending_record(
    log: ShadowLogger,
    pair: str,
    direction: str,
) -> int:
    """Create a strength-pass record in PENDING state. Returns shadow_id."""
    return log.log_signal(
        pair=pair, direction=direction,
        proposed_entry=1.0, proposed_sl_price=0.99, proposed_tp_price=1.02,
        proposed_sl_pips=10.0, proposed_tp_pips=20.0,
        input_snapshot={"composite_scores": {"USD": 5.0}},
    )


# ── Tests ───────────────────────────────────────────────────────────

def test_1_alert_mgr_dedup_happy_path():
    """Test 1: alert_mgr dedup against open trade → GATE_DUPLICATE with full metadata."""
    print("\n[1] alert_mgr_dedup happy path (existing open trade)")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow_trades_Sv2.json"
        log = ShadowLogger("Sv2", jpath)
        sid = _seed_pending_record(log, pair="GBPJPY", direction="BUY")

        result = FakeResult(sv2_shadow_ids={"GBPJPY": sid})
        full_candidates = {"GBPJPY": ("BUY", {"M5": 7.5})}
        fired: set[str] = set()  # alert_mgr removed it
        open_pairs = {"GBPJPY"}
        tracker = FakeTradeTracker({
            "GBPJPY": FakeTrade(
                direction="SELL",
                entry_price=180.50,
                duration_minutes=25.0,
            ),
        })

        marks = mark_alert_mgr_orphans(
            shadow_logger=log, result=result,
            full_candidates=full_candidates,
            fired=fired,
            open_pairs=open_pairs,
            trade_tracker=tracker,
        )

        if marks.get("GBPJPY") != GATE_DUPLICATE:
            _fail(f"expected GATE_DUPLICATE for GBPJPY, got {marks!r}")

        # Reload to ensure persistence
        log2 = ShadowLogger("Sv2", jpath)
        rec = next(r for r in log2.all_records() if r.shadow_id == sid)

        if rec.status != STATUS_BLOCKED:
            _fail(f"status: {rec.status} != BLOCKED (was PENDING, transition missed)")
        if rec.block_gate != GATE_DUPLICATE:
            _fail(f"gate: {rec.block_gate}")
        if "alert_mgr filtered" not in rec.block_reason:
            _fail(f"reason: {rec.block_reason}")

        meta = json.loads(rec.block_metadata_json)
        # All 6 metadata fields per spec
        if meta.get("existing_direction") != "SELL":
            _fail(f"existing_direction: {meta.get('existing_direction')}")
        if abs(meta.get("existing_trade_age_minutes", -1) - 25.0) > 0.5:
            _fail(f"existing_trade_age_minutes: {meta.get('existing_trade_age_minutes')}")
        if meta.get("existing_entry_price") != 180.50:
            _fail(f"existing_entry_price: {meta.get('existing_entry_price')}")
        if meta.get("blocked_direction") != "BUY":
            _fail(f"blocked_direction: {meta.get('blocked_direction')}")
        if meta.get("already_open") is not True:
            _fail(f"already_open: {meta.get('already_open')}")
        if meta.get("block_source") != "alert_mgr_dedup":
            _fail(f"block_source: {meta.get('block_source')}")

        _ok(f"PENDING -> BLOCKED, gate=duplicate, all 6 metadata fields populated correctly")


def test_2_alert_mgr_nondup_path():
    """Test 2: alert_mgr filtered but no open trade → GATE_INTERNAL alert_mgr_nondup."""
    print("\n[2] alert_mgr_nondup path (cooldown/debounce, no open trade)")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow_trades_Sv2.json"
        log = ShadowLogger("Sv2", jpath)
        sid = _seed_pending_record(log, pair="EURJPY", direction="BUY")

        result = FakeResult(sv2_shadow_ids={"EURJPY": sid})
        full_candidates = {"EURJPY": ("BUY", {"M5": 7.0})}
        fired: set[str] = set()  # alert_mgr removed it
        open_pairs: set[str] = set()  # NO open trades
        tracker = FakeTradeTracker({})  # tracker says no trade either

        marks = mark_alert_mgr_orphans(
            shadow_logger=log, result=result,
            full_candidates=full_candidates,
            fired=fired,
            open_pairs=open_pairs,
            trade_tracker=tracker,
        )

        if marks.get("EURJPY") != GATE_INTERNAL:
            _fail(f"expected GATE_INTERNAL for EURJPY, got {marks!r}")

        log2 = ShadowLogger("Sv2", jpath)
        rec = next(r for r in log2.all_records() if r.shadow_id == sid)

        if rec.status != STATUS_BLOCKED:
            _fail(f"status: {rec.status}")
        if rec.block_gate != GATE_INTERNAL:
            _fail(f"gate: {rec.block_gate}")

        meta = json.loads(rec.block_metadata_json)
        if meta.get("block_source") != "alert_mgr_nondup":
            _fail(f"block_source: {meta.get('block_source')}")
        if meta.get("blocked_direction") != "BUY":
            _fail(f"blocked_direction: {meta.get('blocked_direction')}")
        _ok(f"PENDING -> BLOCKED, gate=internal, block_source=alert_mgr_nondup")


def test_3_pair_in_fired_not_remarked():
    """Test 3: pair survived alert_mgr (in fired set) → record stays PENDING.

    Downstream per-pair loop has the responsibility of marking these; the
    orphan-marker MUST NOT touch them or it would steal records the
    downstream loop expected to mark.
    """
    print("\n[3] pair in fired set is NOT touched by orphan-marker")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow_trades_Sv2.json"
        log = ShadowLogger("Sv2", jpath)
        sid = _seed_pending_record(log, pair="AUDJPY", direction="BUY")

        result = FakeResult(sv2_shadow_ids={"AUDJPY": sid})
        full_candidates = {"AUDJPY": ("BUY", {"M5": 7.5})}
        fired = {"AUDJPY"}  # alert_mgr passed it through
        open_pairs: set[str] = set()
        tracker = FakeTradeTracker({})

        marks = mark_alert_mgr_orphans(
            shadow_logger=log, result=result,
            full_candidates=full_candidates,
            fired=fired,
            open_pairs=open_pairs,
            trade_tracker=tracker,
        )

        if marks:
            _fail(f"orphan-marker should not have touched AUDJPY, got {marks!r}")

        log2 = ShadowLogger("Sv2", jpath)
        rec = next(r for r in log2.all_records() if r.shadow_id == sid)

        if rec.status != STATUS_PENDING:
            _fail(
                f"AUDJPY status: {rec.status} (expected PENDING — "
                f"downstream loop has the marking responsibility)"
            )
        _ok("AUDJPY left at PENDING, downstream loop owns the marking")


def test_4_empty_intersection_noop():
    """Test 4: empty full_candidates and fired → no-op, no exceptions, no writes."""
    print("\n[4] empty intersection no-op (quiet morning)")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow_trades_Sv2.json"
        log = ShadowLogger("Sv2", jpath)

        result = FakeResult(sv2_shadow_ids={})
        full_candidates: dict = {}
        fired: set[str] = set()
        open_pairs: set[str] = set()
        tracker = FakeTradeTracker({})

        marks = mark_alert_mgr_orphans(
            shadow_logger=log, result=result,
            full_candidates=full_candidates,
            fired=fired,
            open_pairs=open_pairs,
            trade_tracker=tracker,
        )

        if marks:
            _fail(f"expected empty dict, got {marks!r}")

        log2 = ShadowLogger("Sv2", jpath)
        if log2.all_records():
            _fail(f"expected zero records on disk, got {len(log2.all_records())}")
        _ok("no-op: zero records written, zero exceptions, zero side effects")


def test_5_defensive_missing_shadow_id():
    """Test 5: full_candidates has pair but result.sv2_shadow_ids does NOT.

    Defensive: the binding could be lost (cycle ordering bug, stale result,
    etc.). Function must handle gracefully — skip pair, no crash, no write.
    """
    print("\n[5] defensive: shadow_id missing from result.sv2_shadow_ids")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow_trades_Sv2.json"
        log = ShadowLogger("Sv2", jpath)
        # Note: NO record seeded — sv2_shadow_ids will be empty

        result = FakeResult(sv2_shadow_ids={})  # empty dict — binding missing
        full_candidates = {"GBPJPY": ("BUY", {"M5": 7.5})}
        fired: set[str] = set()
        open_pairs = {"GBPJPY"}
        tracker = FakeTradeTracker({
            "GBPJPY": FakeTrade("SELL", 180.0, 10.0),
        })

        # This must NOT raise
        try:
            marks = mark_alert_mgr_orphans(
                shadow_logger=log, result=result,
                full_candidates=full_candidates,
                fired=fired,
                open_pairs=open_pairs,
                trade_tracker=tracker,
            )
        except Exception as exc:
            _fail(f"function crashed on missing shadow_id: {exc!r}")
            return

        if marks:
            _fail(f"expected no marks (no shadow_id to attach to), got {marks!r}")

        log2 = ShadowLogger("Sv2", jpath)
        if log2.all_records():
            _fail(f"expected zero records, got {len(log2.all_records())}")
        _ok("missing shadow_id handled gracefully — pair skipped, no exception, no write")


# ── Bonus: defensive trade_tracker raises ───────────────────────────

def test_6_defensive_tracker_raises():
    """Bonus: trade_tracker.get_trade raises an exception → caught + logged."""
    print("\n[6] (bonus) defensive: trade_tracker.get_trade raises")
    with tempfile.TemporaryDirectory() as td:
        jpath = Path(td) / "shadow_trades_Sv2.json"
        log = ShadowLogger("Sv2", jpath)
        sid = _seed_pending_record(log, pair="USDJPY", direction="BUY")

        class CrashingTracker:
            def get_trade(self, pair):
                raise RuntimeError("simulated tracker failure")

        result = FakeResult(sv2_shadow_ids={"USDJPY": sid})
        full_candidates = {"USDJPY": ("BUY", {"M5": 7.5})}
        fired: set[str] = set()
        open_pairs = {"USDJPY"}
        tracker = CrashingTracker()

        try:
            marks = mark_alert_mgr_orphans(
                shadow_logger=log, result=result,
                full_candidates=full_candidates,
                fired=fired,
                open_pairs=open_pairs,
                trade_tracker=tracker,
            )
        except Exception as exc:
            _fail(f"tracker crash propagated up: {exc!r}")
            return

        # Tracker raised → existing is None → falls into nondup path
        if marks.get("USDJPY") != GATE_INTERNAL:
            _fail(f"expected GATE_INTERNAL fallback after tracker crash, got {marks!r}")
        _ok("tracker exception caught, fell through to nondup path safely")


if __name__ == "__main__":
    print("Synthetic verification — orphan-PENDING fix")
    test_1_alert_mgr_dedup_happy_path()
    test_2_alert_mgr_nondup_path()
    test_3_pair_in_fired_not_remarked()
    test_4_empty_intersection_noop()
    test_5_defensive_missing_shadow_id()
    test_6_defensive_tracker_raises()
    print("\nALL ORPHAN-FIX TESTS PASSED")
