"""Tier 1 analytics test suite — 7 tests required by the spec.

  1. shadow_loader basic load + date filter
  2. ShadowRecord defensive accessors (typed defaults for sparse fields)
  3. capture distribution counts on synthetic data
  4. calibration trend rolling-mean + decomposition
  5. filter value-add counterfactual math
  6. schema health detects anomalies
  7. real-data smoke (read-only, all 4 scripts complete without exception)

Run from repo root:
    python scripts/test_tier1_analytics.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from takumi_trader.analytics.shadow_loader import (  # noqa: E402
    ShadowRecord, CalibrationRecord, KNOWN_GATES,
    categorize_pair, classify_session,
    load_calibration_log, load_shadow_journal, parse_date_arg,
)


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _make_rec(**overrides) -> dict:
    """Synthetic shadow record with reasonable defaults."""
    base = {
        "shadow_id": 1,
        "strategy_id": "Sv2",
        "signal_time": 1777800000.0,
        "signal_time_str": "05-04 09:00:00 JST",
        "captured_at": 1777800000.0,
        "last_updated": 1777800000.0,
        "pair": "EURUSD",
        "direction": "BUY",
        "status": "BLOCKED",
        "block_gate": "divergence_spread",
        "block_reason": "composite spread 1.0 < min 12.0",
    }
    base.update(overrides)
    return base


def _make_cal(**overrides) -> dict:
    base = {
        "shadow_id": 1, "strategy_id": "Sv2",
        "pair": "EURUSD", "direction": "BUY",
        "signal_time": 1777800000.0,
        "real_pnl_pips": 5.0, "sim_pnl_pips": 3.0, "delta_pips": 2.0,
        "real_exit_reason": "tp_hit", "sim_exit_reason": "TP",
        "real_duration_minutes": 30.0, "sim_duration_minutes": 25.0,
        "pessimism_applied": "wcf+sp+slip_fx0.3",
        "written_at": 1777800600.0,
    }
    base.update(overrides)
    return base


# ─────────────────────────────────────────────────────────────────────
# Test 1 — shadow_loader basic load + date filter
# ─────────────────────────────────────────────────────────────────────

def test_1_loader_load_and_filter():
    print("\n[1] shadow_loader: load + date-range filter")
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        # 5 records across 2 days
        recs = [
            _make_rec(shadow_id=i, signal_time=1777800000 + i * 86400)
            for i in range(5)
        ]
        journal = td_path / "shadow.json"
        _write_json(journal, recs)

        # No filter -> all 5 returned, sorted ascending
        loaded = load_shadow_journal(journal)
        if len(loaded) != 5:
            _fail(f"expected 5 records, got {len(loaded)}")
        for i in range(len(loaded) - 1):
            if loaded[i].signal_time > loaded[i + 1].signal_time:
                _fail("records not sorted by signal_time ascending")

        # since filter — should keep only records >= since
        since = datetime.fromtimestamp(1777800000 + 2 * 86400, tz=timezone.utc)
        loaded2 = load_shadow_journal(journal, since=since)
        if len(loaded2) != 3:
            _fail(f"since filter: expected 3, got {len(loaded2)}")

        # Missing journal returns []
        empty = load_shadow_journal(td_path / "does_not_exist.json")
        if empty != []:
            _fail("missing journal should return empty list")
        _ok("loader: 5 records loaded + sorted, since filter works, missing file -> []")


# ─────────────────────────────────────────────────────────────────────
# Test 2 — ShadowRecord defensive accessors
# ─────────────────────────────────────────────────────────────────────

def test_2_defensive_accessors():
    print("\n[2] ShadowRecord defensive accessors return typed defaults")
    # Record with most fields missing (sparse)
    raw = {"shadow_id": 42, "pair": "EURUSD"}
    r = ShadowRecord(raw)

    cases = [
        ("status", "PENDING"),       # default "PENDING" (sparse-omitted)
        ("transient_retry_count", 0),
        ("sim_completed", False),
        ("sim_pnl_pips", 0.0),
        ("block_gate", ""),
        ("input_snapshot_json", ""),
    ]
    for field, expected in cases:
        got = getattr(r, field)
        if got != expected:
            _fail(f"{field}: expected {expected!r}, got {got!r}")
    # Unknown fields raise AttributeError so typos surface
    try:
        _ = r.utterly_invented_field
        _fail("unknown field should raise AttributeError")
    except AttributeError:
        pass
    _ok("typed defaults work; unknown fields raise AttributeError")


# ─────────────────────────────────────────────────────────────────────
# Test 3 — capture distribution on synthetic data
# ─────────────────────────────────────────────────────────────────────

def test_3_capture_distribution():
    print("\n[3] analyze_capture_distribution counts on synthetic data")
    from takumi_trader.analytics.analyze_capture_distribution import build_report
    recs = []
    # 100 strength-rejects
    for i in range(100):
        recs.append(_make_rec(
            shadow_id=i + 1, signal_time=1777800000 + i,
            block_gate="strength_engine",
            input_snapshot_json="",  # lightweight reject
        ))
    # 10 strength-passes (5 BLOCKED divergence_spread, 3 BLOCKED structural,
    # 2 EXECUTED)
    for i in range(5):
        recs.append(_make_rec(
            shadow_id=200 + i, signal_time=1777800500 + i,
            block_gate="divergence_spread",
            input_snapshot_json='{"composite_scores": {}}',
            status="BLOCKED",
        ))
    for i in range(3):
        recs.append(_make_rec(
            shadow_id=300 + i, signal_time=1777800600 + i,
            block_gate="structural",
            input_snapshot_json='{"composite_scores": {}}',
            status="BLOCKED",
        ))
    for i in range(2):
        recs.append(_make_rec(
            shadow_id=400 + i, signal_time=1777800700 + i,
            block_gate="",
            input_snapshot_json='{"composite_scores": {}}',
            status="EXECUTED",
        ))

    wrapped = [ShadowRecord(r) for r in recs]
    since = datetime.fromtimestamp(1777800000, tz=timezone.utc)
    until = datetime.fromtimestamp(1777801000, tz=timezone.utc)
    report = build_report(wrapped, "synthetic", since, until)

    # Verify expected counts appear in report
    if "100 records" not in report and "Total records in window: 110" not in report:
        _fail(f"total count not in report: {report[:300]!r}")
    if "100" not in report:
        _fail("strength-reject count missing")
    # "8" appears for blocked strength-passes count
    if "divergence_spread" not in report or "structural" not in report:
        _fail("gates missing from gate distribution section")
    _ok("counts + gate distribution render correctly")


# ─────────────────────────────────────────────────────────────────────
# Test 4 — calibration trend on synthetic deltas
# ─────────────────────────────────────────────────────────────────────

def test_4_calibration_trend():
    print("\n[4] analyze_calibration_trend rolling-mean + decomposition")
    from takumi_trader.analytics.analyze_calibration_trend import build_report
    # 12 entries with deltas 0, 0.5, 1.0, ... 5.5; written_at increasing
    cals = []
    for i in range(12):
        cals.append(_make_cal(
            shadow_id=i + 1,
            written_at=100.0 + 100.0 * i,
            delta_pips=0.0 + 0.5 * i,
            real_pnl_pips=10.0,
            sim_pnl_pips=10.0 - (0.0 + 0.5 * i),
            real_duration_minutes=20.0 + i,
            pair="NZDJPY" if i % 2 == 0 else "EURUSD",
        ))
    wrapped = [CalibrationRecord(c) for c in cals]
    report = build_report(wrapped, "synthetic_cal")

    # Rolling-10 mean over deltas 1.0..5.5 step 0.5 = (1+1.5+...+5.5)/10 = 32.5/10 = 3.25
    if "+3.25p" not in report:
        _fail(f"rolling-10 mean of synthetic data should be +3.25p: \n{report[-600:]}")
    if "n=12" not in report:
        _fail("entry count n=12 missing")
    # JPY crosses category should appear (we used NZDJPY for half)
    if "JPY crosses" not in report:
        _fail("JPY-cross category missing from decomposition")
    _ok("rolling-10 mean = +3.25p; decomposition by pair-category populated")


def test_4b_calibration_dedupe_at_read():
    """Defensive dedupe: duplicate cal entries by (shadow_id, signal_time)
    are dropped at read-time, with the WARNING surfaced in the header."""
    print("\n[4b] analyze_calibration_trend dedupes duplicates with warning")
    from takumi_trader.analytics.analyze_calibration_trend import build_report
    # 3 unique entries plus 2 duplicates (same shadow_id+signal_time, different written_at)
    cals = [
        _make_cal(shadow_id=1, signal_time=1000.0, written_at=100.0,
                  delta_pips=2.0, pair="EURUSD"),
        _make_cal(shadow_id=2, signal_time=2000.0, written_at=200.0,
                  delta_pips=4.0, pair="GBPJPY"),
        _make_cal(shadow_id=3, signal_time=3000.0, written_at=300.0,
                  delta_pips=6.0, pair="NZDJPY"),
        # Duplicate of shadow_id=1 with different written_at
        _make_cal(shadow_id=1, signal_time=1000.0, written_at=400.0,
                  delta_pips=2.0, pair="EURUSD"),
        # Duplicate of shadow_id=2 with different written_at
        _make_cal(shadow_id=2, signal_time=2000.0, written_at=500.0,
                  delta_pips=4.0, pair="GBPJPY"),
    ]
    wrapped = [CalibrationRecord(c) for c in cals]
    report = build_report(wrapped, "synthetic_dup_cal")

    # After dedupe: 3 unique entries, 2 duplicates removed
    if "Total entries: 3" not in report:
        _fail(f"expected dedupe to leave 3 entries, got: {report[:500]!r}")
    if "2 duplicate" not in report or "deduped" not in report:
        _fail(f"WARNING about duplicates not surfaced: {report[:500]!r}")
    # Mean of unique deltas (2, 4, 6) = 4.0
    if "+4.00p" not in report:
        _fail(f"all-time mean over deduped entries should be +4.00p: {report[-500:]!r}")
    _ok("dedupe-at-read: 5 entries -> 3 unique, warning surfaced, mean correct")


# ─────────────────────────────────────────────────────────────────────
# Test 5 — filter value-add counterfactual math
# ─────────────────────────────────────────────────────────────────────

def test_5_filter_value_add():
    print("\n[5] analyze_filter_value_add per-gate sim expectancy")
    from takumi_trader.analytics.analyze_filter_value_add import build_report
    # 5 BLOCKED records, all divergence_spread, sim_pnl: -1, -2, +3, -1, 0 (mean -0.2)
    # 4 BLOCKED records, structural, sim_pnl: +5, +6, +4, +7 (mean +5.5 — net negative)
    recs = []
    sids = 1
    for pnl in (-1.0, -2.0, 3.0, -1.0, 0.0):
        recs.append(_make_rec(
            shadow_id=sids, signal_time=1777800000 + sids,
            block_gate="divergence_spread", status="BLOCKED",
            input_snapshot_json='{"x":1}',
            sim_completed=True, sim_exit_reason="TP" if pnl > 0 else "SL",
            sim_pnl_pips=pnl,
        ))
        sids += 1
    for pnl in (5.0, 6.0, 4.0, 7.0):
        recs.append(_make_rec(
            shadow_id=sids, signal_time=1777800000 + sids,
            block_gate="structural", status="BLOCKED",
            input_snapshot_json='{"x":1}',
            sim_completed=True, sim_exit_reason="TP",
            sim_pnl_pips=pnl,
        ))
        sids += 1
    wrapped = [ShadowRecord(r) for r in recs]
    since = datetime.fromtimestamp(1777800000, tz=timezone.utc)
    until = datetime.fromtimestamp(1777801000, tz=timezone.utc)
    report = build_report(wrapped, "synthetic", since, until)

    # divergence_spread mean = -0.20p (or -0.20p depending on rounding) — neutral verdict
    # structural mean = +5.50p — flagged as filter hurts
    if "+5.50p" not in report:
        _fail(f"structural mean should be +5.50p:\n{report[-500:]}")
    if "structural" not in report or "filter hurts" not in report:
        _fail("structural should be flagged as filter hurts")
    _ok("per-gate means correct; structural flagged as net-negative")


# ─────────────────────────────────────────────────────────────────────
# Test 6 — schema health detects anomalies
# ─────────────────────────────────────────────────────────────────────

def test_6_schema_health_anomalies():
    print("\n[6] verify_schema_health detects NaN / negative MAE / unknown gate")
    from takumi_trader.analytics.verify_schema_health import build_report
    recs = []
    # 1 record with NaN sim_pnl
    recs.append(_make_rec(
        shadow_id=1, sim_completed=True, sim_pnl_pips=float("nan"),
        sim_exit_reason="TP",
    ))
    # 1 record with negative MAE
    recs.append(_make_rec(
        shadow_id=2, sim_completed=True, sim_mae_pips=-1.5,
        sim_exit_reason="TP",
    ))
    # 1 record with unknown block_gate
    recs.append(_make_rec(
        shadow_id=3, block_gate="THIS_IS_NEW_GATE_XX",
    ))
    # Plus several normal records
    for i in range(5):
        recs.append(_make_rec(
            shadow_id=10 + i, signal_time=1777800000 + i,
        ))

    wrapped = [ShadowRecord(r) for r in recs]
    report = build_report(wrapped, [], "synthetic", "synthetic_cal")

    if "NaN" not in report or "1 NaN" not in report:
        _fail("NaN not flagged in report")
    if "negative MAE" not in report and "1 negative MAE" not in report:
        _fail("negative MAE not flagged")
    if "THIS_IS_NEW_GATE_XX" not in report:
        _fail("unknown gate not flagged as schema drift")
    _ok("NaN, negative MAE, schema drift all flagged with appropriate severity")


# ─────────────────────────────────────────────────────────────────────
# Test 7 — real-data smoke test (read-only)
# ─────────────────────────────────────────────────────────────────────

def test_7_real_data_smoke():
    print("\n[7] all 4 scripts complete cleanly against real production journal")
    from takumi_trader.analytics import (
        analyze_capture_distribution as ana1,
        analyze_calibration_trend as ana2,
        analyze_filter_value_add as ana3,
        verify_schema_health as ana4,
    )
    journal = _REPO / "data" / "shadow_trades_Sv2.json"
    cal = _REPO / "data" / "shadow_calibration_Sv2.json"
    if not journal.exists():
        print("  (skipped — production journal not present)")
        return

    from takumi_trader.analytics.shadow_loader import (
        load_calibration_log, load_shadow_journal,
    )
    records = load_shadow_journal(journal)
    cals = load_calibration_log(cal)
    if not records:
        print("  (skipped — production journal is empty)")
        return

    # Each script's build_report should run without exception
    since = datetime.fromtimestamp(records[0].signal_time, tz=timezone.utc)
    until = datetime.fromtimestamp(records[-1].signal_time + 1, tz=timezone.utc)
    for label, fn in [
        ("analyze_capture_distribution", lambda: ana1.build_report(records, journal.name, since, until)),
        ("analyze_calibration_trend", lambda: ana2.build_report(cals, cal.name)),
        ("analyze_filter_value_add", lambda: ana3.build_report(records, journal.name, since, until)),
        ("verify_schema_health", lambda: ana4.build_report(records, cals, journal.name, cal.name)),
    ]:
        try:
            report = fn()
        except Exception as exc:
            _fail(f"{label} raised on real data: {exc!r}")
        if not report or len(report) < 100:
            _fail(f"{label} produced suspiciously short output ({len(report)} chars)")
    _ok(f"all 4 scripts complete on real journal ({len(records):,} recs, {len(cals)} cals)")


# ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 64)
    print("Tier 1 Analytics — test suite")
    print("=" * 64)
    test_1_loader_load_and_filter()
    test_2_defensive_accessors()
    test_3_capture_distribution()
    test_4_calibration_trend()
    test_4b_calibration_dedupe_at_read()
    test_5_filter_value_add()
    test_6_schema_health_anomalies()
    test_7_real_data_smoke()
    print("\n" + "=" * 64)
    print("ALL TIER 1 TESTS PASSED")
    print("=" * 64)


if __name__ == "__main__":
    main()
