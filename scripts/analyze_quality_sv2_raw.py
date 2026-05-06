"""Test quality filters on Sv2 (System A) RAW history — full 15 days.

The DTC analysis showed filters hurt because the 83% WR baseline leaves
no room. But Sv2 raw has lower WR and 600+ trades — much better signal-
to-noise for testing whether the quality features have real predictive
power that we could harvest.

If filters work HERE, we may want to:
  1. Apply them to the Sv2 paper system itself (improve baseline stats)
  2. Add Sv2 to DTC sources with the filter as gate (4th source)
  3. Apply same filters to SS/ATR/B-tuned IF the same patterns hold
     (they all build on the same Sv2 entry foundation)
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median

sys.stdout.reconfigure(encoding="utf-8")

DATA = Path(r"D:\Trading\TAKUMI Trader\data")
JST = timezone(timedelta(hours=9))
USD_QUOTE = {"EURUSD","GBPUSD","AUDUSD","NZDUSD","USDCAD","USDCHF","USDJPY"}


def commission(pair):
    if pair in USD_QUOTE: return 0.6
    if pair.endswith("JPY"): return 0.7
    return 0.8


def load_raw(filename):
    """Load all closed trades — NO time/pair filter. Apply commission."""
    recs = json.loads((DATA / filename).read_text(encoding="utf-8"))
    closed = [r for r in recs if r.get("close_reason")]
    out = []
    for r in closed:
        r = dict(r)
        r["_net_pnl"] = (r.get("pnl_pips", 0) or 0) - commission(r.get("pair", ""))
        r["_net_is_win"] = r["_net_pnl"] > 0
        out.append(r)
    return out


def stats(trades):
    if not trades:
        return None
    n = len(trades)
    w = sum(1 for t in trades if t["_net_is_win"])
    p = sum(t["_net_pnl"] for t in trades)
    # Compound 3% sim
    bal, peak, mdd = 1000, 1000, 0
    for t in sorted(trades, key=lambda x: x["entry_time"]):
        sl = t.get("sl_pips", 10) or 10
        r = t["_net_pnl"] / sl if sl > 0 else 0
        bal += bal * 0.03 * r
        if bal > peak: peak = bal
        dd = (peak - bal) / peak * 100
        if dd > mdd: mdd = dd
    dates = set(datetime.fromtimestamp(t["entry_time"], tz=JST).date()
                for t in trades)
    return dict(n=n, wr=w/n*100, pnl=p, bal=bal, mdd=mdd,
                days=max(1, len(dates)),
                ret_pct=(bal/1000-1)*100)


def fmt(label, s):
    if s is None:
        return f"  {label:<55} (no trades)"
    return (f"  {label:<55} n={s['n']:>4} WR={s['wr']:>5.1f}% "
            f"P/L={s['pnl']:>+7.1f}p DD={s['mdd']:>5.1f}% "
            f"final=${s['bal']:>7,.0f} ({s['ret_pct']:>+6.0f}%)")


def feature_distribution(trades, field):
    """Show feature stats for winners vs losers, with population count."""
    wins = [t.get(field) for t in trades if t["_net_is_win"] and t.get(field) is not None]
    losses = [t.get(field) for t in trades if not t["_net_is_win"] and t.get(field) is not None]
    if not wins or not losses:
        return None
    return dict(
        n_w=len(wins), n_l=len(losses),
        mean_w=mean(wins), mean_l=mean(losses),
        med_w=median(wins), med_l=median(losses),
        gap_mean=mean(wins) - mean(losses),
    )


def threshold_scan(trades, field, rule, thresholds):
    """Return rows of (threshold, kept_n, kept_wr, kept_pnl, dropped_n,
    dropped_wr, dropped_pnl) for trades where field is populated."""
    sub = [t for t in trades if t.get(field) is not None and t.get(field) != 0]
    if not sub:
        return None, []
    base = stats(sub)
    rows = []
    for th in thresholds:
        if rule == "ge":
            kept = [t for t in sub if t[field] >= th]
            dropped = [t for t in sub if t[field] < th]
        else:
            kept = [t for t in sub if t[field] <= th]
            dropped = [t for t in sub if t[field] > th]
        if not kept:
            continue
        ks = stats(kept)
        ds = stats(dropped) if dropped else None
        rows.append((th, ks, ds))
    return base, rows


def analyze_system(name, filename):
    print(f"\n{'#'*92}")
    print(f"  SYSTEM: {name}  (file: {filename})")
    print(f"{'#'*92}")

    trades = load_raw(filename)
    base = stats(trades)
    print(fmt(f"BASELINE — all closed (commission applied)", base))
    print(f"  Span: {base['days']} days  ({base['n']/base['days']:.1f} trades/day)")

    # Population check
    print(f"\n  FEATURE POPULATION & WINNER-vs-LOSER MEANS:")
    print(f"  {'Feature':<32} {'pop':>5} {'W mean':>8} {'L mean':>8} {'gap':>7}")
    features = [
        ("entry_h1_atr_pips", "ge"),
        ("entry_tick_volume_ratio", "ge"),
        ("entry_momentum_buildup_sec", "ge"),
        ("entry_conv_trend", "ge"),
        ("entry_conv_isolation", "ge"),
        ("entry_strong_top_gap", "ge"),
        ("adr_consumed_pct", "le"),
    ]
    for field, rule in features:
        d = feature_distribution(trades, field)
        if d is None:
            print(f"  {field:<32} (insufficient data)")
            continue
        pop = d["n_w"] + d["n_l"]
        print(f"  {field:<32} {pop:>5} {d['mean_w']:>7.2f}  {d['mean_l']:>7.2f}  "
              f"{d['gap_mean']:>+6.2f}")

    # Threshold sweeps for the 2 strongest features
    print(f"\n  THRESHOLD SWEEPS (only trades with feature populated):")
    sweeps = [
        ("entry_h1_atr_pips", "ge", [3, 5, 7, 9, 11, 13]),
        ("entry_tick_volume_ratio", "ge", [0.3, 0.5, 0.7, 1.0, 1.3]),
        ("adr_consumed_pct", "le", [40, 50, 60, 70, 80, 90]),
        ("entry_momentum_buildup_sec", "ge", [10, 30, 60, 120]),
    ]
    for field, rule, thresholds in sweeps:
        op = ">=" if rule == "ge" else "<="
        print(f"\n  {field}  ({op}):")
        result = threshold_scan(trades, field, rule, thresholds)
        if result[0] is None:
            print(f"    (feature not populated)")
            continue
        base_sub, rows = result
        print(f"    {'thresh':>7}  {'kept_n':>6} {'k_WR':>6} {'k_P/L':>9} "
              f"{'k_$':>9}  {'drop_n':>6} {'d_WR':>6} {'d_P/L':>9}")
        print(f"    {'(none)':>7}  {base_sub['n']:>6} {base_sub['wr']:>5.1f}% "
              f"{base_sub['pnl']:>+8.1f}p ${base_sub['bal']:>7,.0f}")
        for th, ks, ds in rows:
            d_str = (f"{ds['n']:>5} {ds['wr']:>5.1f}% {ds['pnl']:>+8.1f}p"
                     if ds else "  (none)")
            wr_gain = ks['wr'] - base_sub['wr']
            ret_diff = ks['bal'] - base_sub['bal']
            flag = ""
            if wr_gain > 2 and ret_diff > 0:
                flag = " ✓ WIN"
            elif wr_gain > 2 and ret_diff < 0:
                flag = " (WR up but $ down)"
            print(f"    {th:>7}  {ks['n']:>6} {ks['wr']:>5.1f}% "
                  f"{ks['pnl']:>+8.1f}p ${ks['bal']:>7,.0f}  {d_str}{flag}")

    # Combined filter test
    print(f"\n  COMBINED FILTER TESTS:")

    def adr_ok(t, max_pct=70):
        v = t.get("adr_consumed_pct"); return v is None or v <= max_pct

    def tv_ok(t, min_ratio=0.5):
        v = t.get("entry_tick_volume_ratio"); return v is None or v >= min_ratio

    def atr_ok(t, min_pips=5):
        v = t.get("entry_h1_atr_pips"); return v is None or v >= min_pips

    def buildup_ok(t, min_sec=10):
        v = t.get("entry_momentum_buildup_sec")
        return v is None or v >= min_sec

    print(fmt("BASELINE", stats(trades)))
    print(fmt("ADR <= 70 (soft)",
              stats([t for t in trades if adr_ok(t, 70)])))
    print(fmt("ADR <= 60 (soft)",
              stats([t for t in trades if adr_ok(t, 60)])))
    print(fmt("Tick vol >= 0.5 (soft)",
              stats([t for t in trades if tv_ok(t, 0.5)])))
    print(fmt("ATR >= 7 (soft)",
              stats([t for t in trades if atr_ok(t, 7)])))
    print(fmt("Momentum >= 10s (soft)",
              stats([t for t in trades if buildup_ok(t, 10)])))
    print(fmt("ADR<=70 AND tick_vol>=0.5 (soft)",
              stats([t for t in trades if adr_ok(t, 70) and tv_ok(t, 0.5)])))
    print(fmt("ADR<=70 AND atr>=7 (soft)",
              stats([t for t in trades if adr_ok(t, 70) and atr_ok(t, 7)])))
    print(fmt("ADR<=70 AND tick_vol>=0.5 AND atr>=7 (soft)",
              stats([t for t in trades if adr_ok(t, 70) and tv_ok(t, 0.5) and atr_ok(t, 7)])))
    print(fmt("ADR<=70 AND momentum>=10s (soft)",
              stats([t for t in trades if adr_ok(t, 70) and buildup_ok(t, 10)])))
    print(fmt("All 4 features tight (soft)",
              stats([t for t in trades if adr_ok(t, 70) and tv_ok(t, 0.7)
                     and atr_ok(t, 7) and buildup_ok(t, 30)])))


def main():
    print("RAW HISTORY QUALITY-FILTER ANALYSIS")
    print("(commission applied, 3% compound risk, NO pair/time blacklists)\n")

    for name, fname in [
        ("Sv2 (A)",      "paper_trades.json"),
        ("SS (B)",       "paper_trades_ss.json"),
        ("ATR (C)",      "paper_trades_atr.json"),
        ("A-tuned (D)",  "paper_trades_a_tuned.json"),
        ("B-tuned (E)",  "paper_trades_b_tuned.json"),
    ]:
        analyze_system(name, fname)


if __name__ == "__main__":
    main()
