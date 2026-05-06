"""Phase B day-1 observation report for Sv2 shadow capture.

Reads `data/shadow_trades_Sv2.json` (and `data/shadow_calibration.json`
if present) and produces the empirical report the design review
requires before fan-out to the other 21 systems:

  1. Total candidate count (strength-rejects + strength-passes), with
     today (JST) and 24h-window slices.
  2. Gate distribution across all 10 gates as percentages.
  3. Top 10 most-blocked (pair x direction) combos.
  4. Any (pair x direction) combo that never appeared as a strength-pass
     over the 24h window.
  5. Sim-vs-real delta distribution from ShadowCalibrationLog.
  6. Trading-loop latency hint: time-between-captures sampled from
     captured_at on the most recent N records.

Run from repo root:
    python scripts/observe_shadow_day1.py

Or for a different system once we fan out:
    python scripts/observe_shadow_day1.py SystemName
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from takumi_trader.core.shadow_logger import (  # noqa: E402
    ShadowLogger, ShadowCalibrationLog, VALID_GATES,
    STATUS_BLOCKED, STATUS_EXECUTED, STATUS_PENDING, STATUS_SKIPPED_NO_DATA,
)

JST = timezone(timedelta(hours=9))


def _fmt_pct(n: int, total: int) -> str:
    if total == 0:
        return "  0.0%"
    return f"{100.0 * n / total:5.1f}%"


def _hr(char: str = "-", width: int = 70) -> str:
    return char * width


def report(system_id: str = "Sv2") -> None:
    journal_path = _REPO / "data" / f"shadow_trades_{system_id}.json"
    cal_path = _REPO / "data" / "shadow_calibration.json"

    if not journal_path.exists():
        print(f"NO JOURNAL FILE: {journal_path}")
        print("  Has the worker been instrumented + run on this machine yet?")
        sys.exit(0)

    log = ShadowLogger(system_id, journal_path)
    records = log.all_records()
    if not records:
        print(f"Journal file exists but is empty: {journal_path}")
        sys.exit(0)

    # 24h window
    now_jst = datetime.now(JST)
    cutoff = (now_jst - timedelta(hours=24)).timestamp()
    today_ord = now_jst.toordinal()
    last_24h = [r for r in records if r.signal_time >= cutoff]
    today = [
        r for r in records
        if datetime.fromtimestamp(r.signal_time, JST).toordinal() == today_ord
    ]

    print(_hr("="))
    print(f" Sv2 SHADOW CAPTURE - DAY 1 OBSERVATION  ({system_id})")
    print(f" Now: {now_jst:%Y-%m-%d %H:%M JST}   Journal: {journal_path}")
    print(_hr("="))

    # ── 1. Capture counts ──
    print()
    print("[1] CAPTURE COUNTS")
    print(_hr())
    print(f"  Total records (all-time):  {len(records):>7}")
    print(f"  Last 24 hours:             {len(last_24h):>7}")
    print(f"  Today (JST {now_jst:%Y-%m-%d}): {len(today):>7}")
    if not last_24h:
        print()
        print("  No records in the last 24h. Either TAKUMI hasn't been")
        print("  running, or the worker hasn't reached an M5-close cycle")
        print("  since startup. Re-run after at least one M5 close has")
        print("  fired on a live MT5 connection.")
        sys.exit(0)

    n_passes = sum(
        1 for r in last_24h
        if r.status == STATUS_EXECUTED or (
            r.status != STATUS_BLOCKED and r.proposed_entry > 0
        )
    )
    n_strength_pass = sum(
        1 for r in last_24h if r.proposed_entry > 0
    )
    n_strength_reject = sum(
        1 for r in last_24h if r.proposed_entry <= 0
    )
    print()
    print(f"  Strength-rejects (lightweight): {n_strength_reject:>7} "
          f"({_fmt_pct(n_strength_reject, len(last_24h))})")
    print(f"  Strength-passes  (full record): {n_strength_pass:>7} "
          f"({_fmt_pct(n_strength_pass, len(last_24h))})")
    print(f"    of which EXECUTED:            "
          f"{sum(1 for r in last_24h if r.status == STATUS_EXECUTED):>7}")
    print(f"    of which BLOCKED downstream:  "
          f"{sum(1 for r in last_24h if r.proposed_entry > 0 and r.status == STATUS_BLOCKED):>7}")
    print(f"    of which PENDING (orphaned):  "
          f"{sum(1 for r in last_24h if r.proposed_entry > 0 and r.status == STATUS_PENDING):>7}")

    # ── 2. Gate distribution ──
    print()
    print("[2] GATE DISTRIBUTION (last 24h, BLOCKED records only)")
    print(_hr())
    blocked_24h = [r for r in last_24h if r.status == STATUS_BLOCKED]
    gate_counter = Counter(r.block_gate for r in blocked_24h)
    if blocked_24h:
        # Show every known gate, even if zero (helps spot missing instrumentation)
        for gate in sorted(VALID_GATES):
            n = gate_counter.get(gate, 0)
            bar = "#" * int(40.0 * n / max(1, len(blocked_24h)))
            print(f"  {gate:<22} {n:>6}  {_fmt_pct(n, len(blocked_24h))}  {bar}")
        # Catch any unknown gate names (typos / vocabulary drift)
        unknown = set(gate_counter.keys()) - VALID_GATES - {""}
        if unknown:
            print()
            print(f"  WARNING - unknown gate names in journal: {sorted(unknown)}")
            print("  Vocabulary drift detected. Check shadow_logger.VALID_GATES.")
    else:
        print("  No BLOCKED records in window.")

    # ── 3. Top 10 most-blocked (pair x direction) ──
    print()
    print("[3] TOP 10 MOST-BLOCKED (pair x direction) - last 24h")
    print(_hr())
    pd_counter = Counter(
        (r.pair, r.direction, r.block_gate)
        for r in blocked_24h
    )
    pd_total = Counter((r.pair, r.direction) for r in blocked_24h)
    top10 = pd_total.most_common(10)
    if top10:
        print(f"  {'#':<3} {'PAIR':<8} {'DIR':<5} {'BLOCKS':>7}   "
              f"TOP REJECT GATE")
        for i, ((pair, direction), n) in enumerate(top10, 1):
            # Most common gate for this pair-direction combo
            gate_subc = Counter(
                g for (p, d, g), _n in pd_counter.items()
                for _ in range(_n) if p == pair and d == direction
            )
            top_gate = gate_subc.most_common(1)[0][0] if gate_subc else "?"
            print(f"  {i:<3} {pair:<8} {direction:<5} {n:>7}   {top_gate}")
    else:
        print("  No data.")

    # ── 4. Pairs that NEVER appeared as a strength-pass ──
    print()
    print("[4] (pair x direction) NEVER appeared as a strength-pass - last 24h")
    print(_hr())
    all_combos: set[tuple[str, str]] = set()
    pass_combos: set[tuple[str, str]] = set()
    for r in last_24h:
        all_combos.add((r.pair, r.direction))
        if r.proposed_entry > 0:  # strength-pass
            pass_combos.add((r.pair, r.direction))
    never_pass = sorted(all_combos - pass_combos)
    if never_pass:
        print(f"  {len(never_pass)} combos considered but never passed strength gate:")
        for pair, direction in never_pass[:30]:
            print(f"    {pair:<8} {direction}")
        if len(never_pass) > 30:
            print(f"    ... and {len(never_pass) - 30} more")
        print()
        print(f"  These {len(never_pass)} combos contributed only to noise. If a")
        print(f"  pair structurally never trades on Sv2, consider excluding it")
        print(f"  from capture entirely to save journal space (decide AFTER")
        print(f"  multi-day data — single-day might just be a quiet session).")
    else:
        print("  Every combo passed strength gate at least once in the window.")

    # ── 5. Calibration log ──
    print()
    print("[5] PESSIMISM CALIBRATION (sim-vs-real delta on EXECUTED parity sims)")
    print(_hr())
    if cal_path.exists():
        cal = ShadowCalibrationLog(cal_path)
        s = cal.summary()
        if s["n"] == 0:
            print("  No calibration records yet. Phase C ShadowSimWorker writes")
            print("  one record per CLOSED EXECUTED trade.")
        else:
            print(f"  N records: {s['n']}")
            print(f"  Mean delta (real - sim): {s['mean']:+.2f} pips")
            print(f"  Median delta:            {s['median']:+.2f} pips")
            print(f"  Stdev:                   {s['std']:.2f} pips")
            print()
            if s["n"] < 50:
                print("  N < 50 - NOISE. Wait for more samples before drawing conclusions.")
            elif abs(s["mean"]) < 0.5:
                print("  Mean delta near zero - simulator is well-calibrated.")
            elif s["mean"] > 0.5:
                print("  Mean delta positive - sim is too PESSIMISTIC")
                print("    (real trades beat sim by ~", round(s["mean"], 2), "pips on average).")
                print("    Consider relaxing slippage / spread assumptions in ShadowSimulator.")
            else:
                print("  Mean delta negative - sim is too OPTIMISTIC")
                print("    (sim beats real by ~", round(-s["mean"], 2), "pips on average).")
                print("    This is the dangerous direction - tighten pessimism in ShadowSimulator.")
    else:
        print(f"  No calibration file at {cal_path}.")
        print("  Phase C ShadowSimWorker will create it.")

    # ── 6. Trading-loop latency hint ──
    print()
    print("[6] CAPTURE TIMING (sampled, last 100 records)")
    print(_hr())
    sample = sorted(records, key=lambda r: r.captured_at)[-100:]
    if len(sample) >= 2:
        deltas = [
            sample[i].captured_at - sample[i - 1].captured_at
            for i in range(1, len(sample))
            if sample[i].captured_at > 0 and sample[i - 1].captured_at > 0
        ]
        if deltas:
            sorted_d = sorted(deltas)
            n = len(sorted_d)
            print(f"  Time between consecutive captures (n={n}):")
            print(f"    min:    {min(sorted_d):>7.3f} sec")
            print(f"    p50:    {sorted_d[n // 2]:>7.3f} sec")
            print(f"    p95:    {sorted_d[int(n * 0.95)]:>7.3f} sec")
            print(f"    max:    {max(sorted_d):>7.3f} sec")
            print()
            print("  Expected pattern: bursts of 50+ records within ~1 sec at each")
            print("  M5 close, then ~5 minute gap. If p50 > 0.1 sec, capture loop")
            print("  may be doing too much I/O. If p95 > 1.0 sec, investigate.")
        else:
            print("  Not enough captured_at samples to compute timing.")
    else:
        print("  Not enough records yet to sample timing.")

    print()
    print(_hr("="))
    print(" END REPORT")
    print(_hr("="))


if __name__ == "__main__":
    sys_id = sys.argv[1] if len(sys.argv) > 1 else "Sv2"
    report(sys_id)
