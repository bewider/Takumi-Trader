"""Refined quality-filter test: only the 2 features with real predictive
power in the DTC-equivalent dataset.

Findings from scripts/analyze_quality_filters.py:
  ✅ Tick Volume Ratio ≥ 1.0  : 86.2% WR (+3.5pp vs 82.6% baseline)
  ✅ ADR Consumed % ≤ 70      : 86.5% WR (+3.1pp vs 83.3% baseline)
  ❌ H1 ATR                   : no real edge (winner/loser means identical)
  ❌ Momentum Buildup         : median is 0 — metric not meaningful
  ❌ Conv Trend               : maxes at 30 — no discrimination
  ❌ Conv Isolation           : means near-identical
  ❌ Strong Top Gap           : marginal

This script tests the 2 clean signals together with SOFT semantics
(skip check if feature not measured) so volume isn't punished during
transition as more trades get the feature populated.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

DATA = Path(r"D:\Trading\TAKUMI Trader\data")
JST = timezone(timedelta(hours=9))
USD_QUOTE = {"EURUSD","GBPUSD","AUDUSD","NZDUSD","USDCAD","USDCHF","USDJPY"}


def commission(pair):
    if pair in USD_QUOTE: return 0.6
    if pair.endswith("JPY"): return 0.7
    return 0.8


FILTERS = {
    "SS":      ("paper_trades_ss.json",
                [("08:00","08:15"),("14:15","14:30"),("16:00","16:15"),
                 ("16:30","16:45"),("17:45","18:00"),("20:15","20:45"),
                 ("21:00","21:15")],
                {"AUDCAD","AUDCHF","CADCHF","EURCAD","EURCHF","EURUSD","GBPAUD"}),
    "ATR":     ("paper_trades_atr.json", [("16:00","16:15")],
                {"AUDCAD","AUDCHF","EURCAD","EURNZD","EURUSD","GBPAUD"}),
    "B-tuned": ("paper_trades_b_tuned.json",
                [("08:15","08:30"),("14:15","14:30"),("15:00","15:15"),
                 ("16:30","16:45"),("17:00","17:15"),("19:00","19:15")],
                {"AUDCAD","AUDCHF","AUDJPY","EURNZD","GBPCAD","NZDUSD"}),
}

def hm_to_min(s):
    h, m = s.split(":"); return int(h)*60 + int(m)

def in_blackout(dt, bl):
    mins = dt.hour*60 + dt.minute
    return any(hm_to_min(s) <= mins < hm_to_min(e) for s, e in bl)


def load_filtered():
    all_trades = []
    for name, (fname, tbl, pbl) in FILTERS.items():
        recs = json.loads((DATA / fname).read_text(encoding="utf-8"))
        closed = [r for r in recs if r.get("close_reason")]
        for r in closed:
            if r.get("pair") in pbl:
                continue
            dt = datetime.fromtimestamp(r["entry_time"], tz=JST)
            if in_blackout(dt, tbl):
                continue
            r = dict(r)
            r["_net_pnl"] = (r.get("pnl_pips", 0) or 0) - commission(r.get("pair", ""))
            r["_net_is_win"] = r["_net_pnl"] > 0
            all_trades.append(r)
    by_pair = defaultdict(list)
    for t in all_trades:
        by_pair[t["pair"]].append(t)
    kept = []
    for p, ts in by_pair.items():
        ts_sorted = sorted(ts, key=lambda x: x["entry_time"])
        last = None
        for t in ts_sorted:
            et = t["entry_time"]
            if last is None or (et - last) >= 120:
                kept.append(t)
                last = et
    return kept


def test_filter(trades, name, check_fn):
    kept = [t for t in trades if check_fn(t)]
    dropped = [t for t in trades if not check_fn(t)]
    if not kept:
        print(f"  {name:<45} (no trades kept)")
        return
    kn, dn = len(kept), len(dropped)
    kw = sum(1 for t in kept if t["_net_is_win"])
    dw = sum(1 for t in dropped if t["_net_is_win"]) if dn > 0 else 0
    kp = sum(t["_net_pnl"] for t in kept)
    dp = sum(t["_net_pnl"] for t in dropped) if dn > 0 else 0
    # Compound sim @ 3% risk
    bal = 1000
    peak = 1000
    mdd = 0
    for t in sorted(kept, key=lambda x: x["entry_time"]):
        sl = t.get("sl_pips", 10) or 10
        r = (t["_net_pnl"] / sl) if sl > 0 else 0
        bal += bal * 0.03 * r
        if bal > peak: peak = bal
        dd = (peak-bal)/peak*100
        if dd > mdd: mdd = dd
    dropped_avg = dp/dn if dn > 0 else 0.0
    print(f"  {name:<45} kept={kn:>3} WR={kw/kn*100:>5.1f}% "
          f"P/L={kp:>+7.1f}p DD={mdd:>4.1f}% ${bal:>6,.0f}  "
          f"| drop={dn:>3} drop_WR={dw/dn*100 if dn else 0:>5.1f}% "
          f"drop_avg={dropped_avg:+5.2f}p")


def main():
    trades = load_filtered()
    print(f"Dataset: {len(trades)} DTC-equivalent trades\n")

    print(f"{'='*92}")
    print(f"  Baseline and single/combined quality filters")
    print(f"{'='*92}\n")

    test_filter(trades, "BASELINE (no quality filter)", lambda t: True)

    # Single filters (soft — skip if feature missing)
    def tv_ok(t):
        v = t.get("entry_tick_volume_ratio")
        return v is None or v >= 1.0

    def tv_ok_strict_06(t):
        v = t.get("entry_tick_volume_ratio")
        return v is None or v >= 0.6

    def adr_ok(t):
        v = t.get("adr_consumed_pct")
        return v is None or v <= 70

    def adr_ok_strict_60(t):
        v = t.get("adr_consumed_pct")
        return v is None or v <= 60

    # Secondary — strong_top_gap shows modest edge
    def stg_ok(t):
        v = t.get("entry_strong_top_gap")
        return v is None or v >= 1.2

    print()
    print("  SINGLE FILTERS (soft: trades without measurement pass through):")
    test_filter(trades, "tick_volume_ratio >= 1.0 (tight)", tv_ok)
    test_filter(trades, "tick_volume_ratio >= 0.6 (looser)", tv_ok_strict_06)
    test_filter(trades, "adr_consumed_pct <= 70", adr_ok)
    test_filter(trades, "adr_consumed_pct <= 60 (tight)", adr_ok_strict_60)
    test_filter(trades, "strong_top_gap >= 1.2", stg_ok)

    print()
    print("  COMBINED FILTERS:")
    test_filter(trades, "tick_vol >= 1.0 AND adr <= 70",
                lambda t: tv_ok(t) and adr_ok(t))
    test_filter(trades, "tick_vol >= 0.6 AND adr <= 70",
                lambda t: tv_ok_strict_06(t) and adr_ok(t))
    test_filter(trades, "tick_vol >= 0.6 AND adr <= 80",
                lambda t: tv_ok_strict_06(t) and (t.get("adr_consumed_pct") is None or t.get("adr_consumed_pct") <= 80))

    # Strict: drop if ANY feature missing
    def tv_adr_strict(t):
        v = t.get("entry_tick_volume_ratio")
        a = t.get("adr_consumed_pct")
        if v is None or a is None:
            return False
        return v >= 0.6 and a <= 70

    test_filter(trades, "tick_vol >= 0.6 AND adr <= 70 [STRICT: require both]",
                tv_adr_strict)

    # What happens if we REJECT only the WORST quadrant? (low volume AND high ADR)
    def reject_bad(t):
        v = t.get("entry_tick_volume_ratio")
        a = t.get("adr_consumed_pct")
        # Only reject if BOTH features indicate low quality
        if v is not None and a is not None:
            if v < 0.5 and a > 80:
                return False
        return True

    test_filter(trades, "REJECT only low-vol AND high-ADR (conservative)",
                reject_bad)

    # Per-pair sanity: does the 2-feature filter help or hurt specific pairs?
    print()
    print(f"{'='*92}")
    print(f"  PER-PAIR IMPACT — tick_vol >= 0.6 AND adr <= 70 (soft)")
    print(f"{'='*92}")
    by_pair_kept = defaultdict(list)
    by_pair_dropped = defaultdict(list)
    for t in trades:
        if tv_ok_strict_06(t) and adr_ok(t):
            by_pair_kept[t["pair"]].append(t)
        else:
            by_pair_dropped[t["pair"]].append(t)
    print(f"  {'Pair':<8} {'kept':>5} {'k_WR':>6} {'k_P/L':>9}  "
          f"{'drop':>5} {'d_WR':>6} {'d_P/L':>9}")
    pairs = sorted(set(list(by_pair_kept.keys()) + list(by_pair_dropped.keys())))
    for p in pairs:
        k = by_pair_kept.get(p, [])
        d = by_pair_dropped.get(p, [])
        if len(k) + len(d) < 5:
            continue
        kn, dn = len(k), len(d)
        kw = sum(1 for t in k if t["_net_is_win"]) if kn else 0
        dw = sum(1 for t in d if t["_net_is_win"]) if dn else 0
        kp = sum(t["_net_pnl"] for t in k) if kn else 0
        dp = sum(t["_net_pnl"] for t in d) if dn else 0
        print(f"  {p:<8} {kn:>5} {kw/kn*100 if kn else 0:>5.1f}% {kp:>+8.1f}p  "
              f"{dn:>5} {dw/dn*100 if dn else 0:>5.1f}% {dp:>+8.1f}p")


if __name__ == "__main__":
    main()
