"""Phase D.4 tests — make_paper_trade_lookup factory.

The factory builds a closure that resolves EXECUTED shadow records to
closed RealTradeOutcome via paper_trader's on-disk journal. The closure
must return None gracefully on every failure mode the architect called
out: out-of-bounds idx, pair mismatch, not-yet-closed, missing journal,
malformed JSON. Also exercise the mtime cache so we don't re-parse
JSON on every cycle.

Run from repo root:
    python scripts/test_phase_d4_lookup_factory.py
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

from takumi_trader.core.shadow_sim_worker import (  # noqa: E402
    RealTradeOutcome, make_paper_trade_lookup,
)


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


# Minimal record stub matching the fields the factory reads.
@dataclass
class _Rec:
    shadow_id: int = 1
    pair: str = "EURUSD"
    exec_lane: str = "paper"
    exec_ref_json: str = ""


def _write_journal(path: Path, entries: list[dict]) -> None:
    """Write a JSON array; mimics paper_trader.save_journal."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(entries, ensure_ascii=False), encoding="utf-8")


def _closed_entry(pair="EURUSD", pnl=12.5, reason="tp_hit", dur=11.0) -> dict:
    return {
        "pair": pair,
        "direction": "BUY",
        "entry_price": 1.1000,
        "entry_time": 1700000000.0,
        "close_price": 1.1012,
        "close_time": 1700000600.0,
        "close_reason": reason,
        "pnl_pips": pnl,
        "duration_minutes": dur,
    }


def _open_entry(pair="EURUSD") -> dict:
    return {
        "pair": pair,
        "direction": "BUY",
        "entry_price": 1.1000,
        "entry_time": 1700000000.0,
        "close_price": 0.0,
        "close_time": 0.0,
        "close_reason": "",
        "pnl_pips": 0.0,
        "duration_minutes": 0.0,
    }


# ─────────────────────────────────────────────────────────────────────
# Test 1 — happy path: closed trade returns RealTradeOutcome
# ─────────────────────────────────────────────────────────────────────

def test_1_happy_path():
    print("\n[1] closed trade returns RealTradeOutcome")
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "paper_trades.json"
        _write_journal(path, [_closed_entry(pnl=15.0, reason="tp_hit", dur=22.0)])
        lookup = make_paper_trade_lookup(path)

        rec = _Rec(shadow_id=42, pair="EURUSD",
                   exec_ref_json=json.dumps({"system": "Sv2", "journal_idx": 0}))
        out = lookup(rec)

        if out is None:
            _fail("expected RealTradeOutcome, got None")
        if not isinstance(out, RealTradeOutcome):
            _fail(f"expected RealTradeOutcome, got {type(out).__name__}")
        if abs(out.pnl_pips - 15.0) > 1e-9:
            _fail(f"pnl_pips: {out.pnl_pips}")
        if out.exit_reason != "tp_hit":
            _fail(f"exit_reason: {out.exit_reason!r}")
        if abs(out.duration_minutes - 22.0) > 1e-9:
            _fail(f"duration_minutes: {out.duration_minutes}")
        _ok("RealTradeOutcome returned with correct fields")


# ─────────────────────────────────────────────────────────────────────
# Test 2 — failure modes (each returns None gracefully)
# ─────────────────────────────────────────────────────────────────────

def test_2_failure_modes():
    print("\n[2] each failure mode returns None gracefully")
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "paper_trades.json"
        _write_journal(path, [
            _closed_entry(pair="EURUSD", pnl=10.0),
            _open_entry(pair="GBPUSD"),
        ])
        lookup = make_paper_trade_lookup(path)

        # 2a) exec_lane != "paper" -> None (different lane)
        rec = _Rec(exec_lane="ctrader",
                   exec_ref_json=json.dumps({"position_id": 999}))
        if lookup(rec) is not None:
            _fail("non-paper lane should return None")
        _ok("non-paper lane -> None")

        # 2b) exec_ref_json empty -> None
        rec = _Rec(exec_lane="paper", exec_ref_json="")
        if lookup(rec) is not None:
            _fail("empty exec_ref_json should return None")
        _ok("empty exec_ref_json -> None")

        # 2c) malformed JSON in exec_ref_json -> None
        rec = _Rec(exec_lane="paper", exec_ref_json="{not json")
        if lookup(rec) is not None:
            _fail("malformed exec_ref_json should return None")
        _ok("malformed exec_ref_json -> None")

        # 2d) journal_idx missing/non-int -> None
        rec = _Rec(exec_lane="paper",
                   exec_ref_json=json.dumps({"system": "Sv2"}))
        if lookup(rec) is not None:
            _fail("missing journal_idx should return None")
        rec = _Rec(exec_lane="paper",
                   exec_ref_json=json.dumps({"journal_idx": "abc"}))
        if lookup(rec) is not None:
            _fail("non-int journal_idx should return None")
        rec = _Rec(exec_lane="paper",
                   exec_ref_json=json.dumps({"journal_idx": -1}))
        if lookup(rec) is not None:
            _fail("negative journal_idx should return None")
        _ok("missing/bad journal_idx -> None")

        # 2e) journal_idx out of bounds -> None (corruption — logged once)
        rec = _Rec(shadow_id=99, pair="EURUSD", exec_lane="paper",
                   exec_ref_json=json.dumps({"journal_idx": 9999}))
        if lookup(rec) is not None:
            _fail("out-of-bounds journal_idx should return None")
        _ok("out-of-bounds journal_idx -> None")

        # 2f) pair mismatch -> None (corruption — logged once)
        rec = _Rec(shadow_id=100, pair="USDJPY", exec_lane="paper",
                   exec_ref_json=json.dumps({"journal_idx": 0}))
        if lookup(rec) is not None:
            _fail("pair mismatch should return None")
        _ok("pair mismatch -> None")

        # 2g) trade still open (close_time == 0) -> None
        rec = _Rec(shadow_id=101, pair="GBPUSD", exec_lane="paper",
                   exec_ref_json=json.dumps({"journal_idx": 1}))
        if lookup(rec) is not None:
            _fail("open trade should return None")
        _ok("open trade -> None")


# ─────────────────────────────────────────────────────────────────────
# Test 3 — missing/unreadable journal -> None gracefully
# ─────────────────────────────────────────────────────────────────────

def test_3_missing_and_malformed_journal():
    print("\n[3] missing or malformed journal returns None gracefully")
    with tempfile.TemporaryDirectory() as td:
        # 3a) Journal file doesn't exist yet (startup race) -> None
        path = Path(td) / "paper_trades.json"
        lookup = make_paper_trade_lookup(path)
        rec = _Rec(exec_lane="paper",
                   exec_ref_json=json.dumps({"journal_idx": 0}))
        if lookup(rec) is not None:
            _fail("missing journal should return None")
        _ok("missing journal -> None")

        # 3b) Journal exists but malformed JSON -> None
        path.write_text("{not valid json", encoding="utf-8")
        if lookup(rec) is not None:
            _fail("malformed JSON should return None")
        _ok("malformed journal JSON -> None")

        # 3c) Journal exists, empty list -> None for any idx
        _write_journal(path, [])
        if lookup(rec) is not None:
            _fail("empty journal + idx=0 should return None")
        _ok("empty journal -> None")

        # 3d) Journal exists, root is dict (not list) -> None
        path.write_text(json.dumps({"oops": "shape"}), encoding="utf-8")
        if lookup(rec) is not None:
            _fail("non-list JSON root should return None")
        _ok("non-list JSON root -> None")


# ─────────────────────────────────────────────────────────────────────
# Test 4 — mtime cache: re-reads only when file changes
# ─────────────────────────────────────────────────────────────────────

def test_4_mtime_cache():
    print("\n[4] mtime cache avoids re-parsing unchanged journal")
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "paper_trades.json"
        _write_journal(path, [_closed_entry(pnl=5.0)])
        lookup = make_paper_trade_lookup(path)

        rec = _Rec(exec_lane="paper",
                   exec_ref_json=json.dumps({"journal_idx": 0}))

        # First call — populates cache
        out1 = lookup(rec)
        if out1 is None or out1.pnl_pips != 5.0:
            _fail(f"first lookup wrong: {out1}")

        # Without modifying the file, lookup should return cached data
        # even if the file goes missing momentarily (cache fallback).
        # Quick check: replace contents with same mtime won't be possible
        # cross-platform; instead, verify that subsequent calls keep
        # producing the same answer without raising.
        out2 = lookup(rec)
        if out2 is None or out2.pnl_pips != 5.0:
            _fail(f"second lookup wrong: {out2}")
        _ok("repeated lookups stable on unchanged journal")

        # Now modify journal — bump pnl. Sleep briefly to ensure mtime
        # actually advances on platforms with second-resolution mtime.
        time.sleep(1.1)
        _write_journal(path, [_closed_entry(pnl=99.0)])
        out3 = lookup(rec)
        if out3 is None or abs(out3.pnl_pips - 99.0) > 1e-9:
            _fail(
                f"after journal update, lookup returned {out3} "
                "(expected pnl=99.0 — cache did not refresh on mtime change)"
            )
        _ok("cache refreshes when mtime advances")


# ─────────────────────────────────────────────────────────────────────
# Test 5 — corruption logs only once per shadow_id
# ─────────────────────────────────────────────────────────────────────

def test_5_corruption_logs_once():
    print("\n[5] out-of-bounds + pair-mismatch warnings logged once per shadow_id")
    import logging
    log_records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, r):
            log_records.append(r)

    handler = _Capture()
    handler.setLevel(logging.WARNING)
    target = logging.getLogger("takumi_trader.core.shadow_sim_worker")
    prev = target.level
    target.setLevel(logging.WARNING)
    target.addHandler(handler)
    try:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "paper_trades.json"
            _write_journal(path, [_closed_entry(pair="EURUSD")])
            lookup = make_paper_trade_lookup(path)

            # Same shadow_id triggering OOB across many cycles —
            # should produce exactly ONE warning.
            rec_oob = _Rec(shadow_id=777, pair="EURUSD", exec_lane="paper",
                           exec_ref_json=json.dumps({"journal_idx": 5000}))
            for _ in range(10):
                lookup(rec_oob)

            # Different shadow_id, also OOB — separate warning
            rec_oob2 = _Rec(shadow_id=778, pair="EURUSD", exec_lane="paper",
                            exec_ref_json=json.dumps({"journal_idx": 5001}))
            lookup(rec_oob2)

            # Pair-mismatch on yet another shadow_id — separate warning
            rec_mismatch = _Rec(shadow_id=779, pair="USDJPY", exec_lane="paper",
                                exec_ref_json=json.dumps({"journal_idx": 0}))
            for _ in range(10):
                lookup(rec_mismatch)

            warns = [r for r in log_records if r.levelno >= logging.WARNING]
            if len(warns) != 3:
                msgs = "\n    ".join(r.getMessage() for r in warns)
                _fail(
                    f"expected exactly 3 warnings (one per shadow_id), got {len(warns)}:\n    {msgs}"
                )
            _ok(f"exactly 3 warnings for 3 distinct shadow_ids ({len(warns)})")
    finally:
        target.removeHandler(handler)
        target.setLevel(prev)


# ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 64)
    print("Phase D.4 — make_paper_trade_lookup factory tests")
    print("=" * 64)
    test_1_happy_path()
    test_2_failure_modes()
    test_3_missing_and_malformed_journal()
    test_4_mtime_cache()
    test_5_corruption_logs_once()
    print("\n" + "=" * 64)
    print("ALL TESTS PASSED")
    print("=" * 64)


if __name__ == "__main__":
    main()
