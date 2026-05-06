"""Detailed Sv2 (System A) filter option comparison.

Compares 4 deployments:
  1. CURRENT — ADR<=70 only (what's running today)
  2. TIME + PAIR (no ADR) — derived 15-min windows + per-pair blacklist
  3. TIME + ADR (no PAIR) — derived 15-min windows + ADR<=70
  4. ALL THREE — derived time + derived pair + ADR<=70

For each: full stats, the actual filter content, equity curve milestones,
and forward projections under 3 decay scenarios.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean

sys.stdout.reconfigure(encoding="utf-8")

DATA = Path(r"D:\Trading\TAKUMI Trader\data")
JST = timezone(timedelta(hours=9))
USD_QUOTE = {"EURUSD","GBPUSD","AUDUSD","NZDUSD","USDCAD","USDCHF","USDJPY"}
ADR_MAX = 70.0


def commission(pair):
    if pair in USD_QUOTE: return 0.6
    if pair.endswith("JPY"): return 0.7
    return 0.8


def hm_to_min(s):
    h, m = s.split(":"); return int(h)*60 + int(m)


def in_blackout(dt, bl):
    mins = dt.hour*60 + dt.minute
    return any(hm_to_min(s) <= mins < hm_to_min(e) for s, e in bl)


def load():
    recs = json.loads((DATA / "paper_trades.json").read_text(encoding="utf-8"))
    out = []
    for r in recs:
        if not r.get("close_reason"):
            continue
        r = dict(r)
        r["_net_pnl"] = (r.get("pnl_pips", 0) or 0) - commission(r.get("pair", ""))
        r["_net_is_win"] = r["_net_pnl"] > 0
        out.append(r)
    return out


def derive_time_blacklist(trades, bin_minutes=15, min_n=3, min_bad_pnl=-5, max_wr=50):
    bins = defaultdict(list)
    for t in trades:
        dt = datetime.fromtimestamp(t["entry_time"], tz=JST)
        b = (dt.hour * 60 + dt.minute) // bin_minutes
        bins[b].append(t)
    bad = []
    for b, ts in sorted(bins.items()):
        if len(ts) < min_n:
            continue
        wins = sum(1 for t in ts if t["_net_is_win"])
        pnl = sum(t["_net_pnl"] for t in ts)
        wr = wins / len(ts) * 100
        if pnl <= min_bad_pnl and wr < max_wr:
            start_m = b * bin_minutes
            end_m = start_m + bin_minutes
            bad.append((f"{start_m//60:02d}:{start_m%60:02d}",
                        f"{end_m//60:02d}:{end_m%60:02d}",
                        len(ts), wr, pnl))
    return bad


def derive_pair_blacklist(trades, min_n=3, max_pnl=0):
    by_pair = defaultdict(list)
    for t in trades:
        by_pair[t["pair"]].append(t)
    bad = []
    for pair, ts in by_pair.items():
        if len(ts) < min_n:
            continue
        pnl = sum(t["_net_pnl"] for t in ts)
        wins = sum(1 for t in ts if t["_net_is_win"])
        if pnl <= max_pnl:
            bad.append((pair, len(ts), wins/len(ts)*100, pnl))
    return sorted(bad, key=lambda x: x[3])


def apply_filters(trades, time_bl=None, pair_bl=None, adr_max=None):
    out = []
    for t in trades:
        dt = datetime.fromtimestamp(t["entry_time"], tz=JST)
        if time_bl:
            tbl_pairs = [(s, e) for s, e, *_ in time_bl] if time_bl and len(time_bl[0]) > 2 else time_bl
            if in_blackout(dt, tbl_pairs):
                continue
        if pair_bl:
            pbl_set = {p for p, *_ in pair_bl} if pair_bl and isinstance(pair_bl[0], tuple) else set(pair_bl)
            if t.get("pair") in pbl_set:
                continue
        if adr_max is not None:
            adr = t.get("adr_consumed_pct")
            if adr is not None and adr > adr_max:
                continue
        out.append(t)
    return out


def stats(trades, label):
    if not trades:
        return None
    n = len(trades)
    w = sum(1 for t in trades if t["_net_is_win"])
    p = sum(t["_net_pnl"] for t in trades)
    bal, peak, mdd = 1000, 1000, 0
    eq_curve = []
    for t in sorted(trades, key=lambda x: x["entry_time"]):
        sl = t.get("sl_pips", 10) or 10
        r = t["_net_pnl"] / sl if sl > 0 else 0
        bal += bal * 0.03 * r
        eq_curve.append(bal)
        if bal > peak: peak = bal
        dd = (peak - bal) / peak * 100
        if dd > mdd: mdd = dd
    gw = sum(t["_net_pnl"] for t in trades if t["_net_is_win"])
    gl = abs(sum(t["_net_pnl"] for t in trades if not t["_net_is_win"]))
    pf = gw / gl if gl > 0 else 99
    avg_win = gw / w if w > 0 else 0
    avg_loss = -gl / (n - w) if (n - w) > 0 else 0
    avg_sl = mean(t.get("sl_pips", 0) or 0 for t in trades)
    dates = set(datetime.fromtimestamp(t["entry_time"], tz=JST).date()
                for t in trades)
    days = max(1, len(dates))
    avg_r = (p/n)/avg_sl if avg_sl > 0 else 0
    tpd = n/days
    daily = avg_r * tpd * 0.03 * 100
    rec = (bal/1000-1)*100/mdd if mdd > 0 else 0
    return dict(
        label=label, n=n, w=w, l=n-w, wr=w/n*100, pnl=p, avg_pnl=p/n,
        avg_win=avg_win, avg_loss=avg_loss,
        bal=bal, ret_pct=(bal/1000-1)*100, mdd=mdd,
        pf=pf, days=days, tpd=tpd, daily=daily,
        recovery=rec, avg_sl=avg_sl,
    )


def proj(daily_pct, days):
    return ((1 + daily_pct/100)**days - 1) * 100


def fmt_block(s):
    if s is None:
        return "  (no trades)"
    return [
        f"  Trades        : {s['n']}  ({s['w']}W / {s['l']}L)",
        f"  Win Rate      : {s['wr']:.1f}%",
        f"  Net P/L       : {s['pnl']:+.1f}p   (avg {s['avg_pnl']:+.2f}p/trade)",
        f"  Avg Win/Loss  : {s['avg_win']:+.1f}p / {s['avg_loss']:+.1f}p",
        f"  Profit Factor : {s['pf']:.2f}",
        f"  Max Drawdown  : {s['mdd']:.1f}%",
        f"  Recovery Fact : {s['recovery']:.2f}",
        f"  Days / Vol    : {s['days']} days, {s['tpd']:.1f} trades/day",
        f"  Daily Return  : {s['daily']:+.2f}%",
        f"  Final ($1k)   : ${s['bal']:,.0f}  ({s['ret_pct']:+.1f}%)",
    ]


def main():
    print("=" * 78)
    print("  SV2 (SYSTEM A) — Detailed Filter Option Comparison")
    print("=" * 78)
    print()

    raw = load()
    print(f"  Raw history: {len(raw)} closed Sv2 paper trades")
    span_dates = sorted(set(datetime.fromtimestamp(t["entry_time"], tz=JST).date() for t in raw))
    print(f"  Period: {span_dates[0]} → {span_dates[-1]} ({len(span_dates)} trading days)")
    print(f"  Risk model: 3% compound per trade, ICMarkets Raw commission")
    print()

    # Derive the filters from this data
    derived_time = derive_time_blacklist(raw)
    derived_pair = derive_pair_blacklist(raw)

    # Print filter contents
    print("=" * 78)
    print("  AUTO-DERIVED FILTER CONTENT  (from full Sv2 history)")
    print("=" * 78)
    print()
    print(f"  TIME BLACKLIST ({len(derived_time)} 15-min windows):")
    print(f"    {'Window':<14} {'n':>4} {'WR':>6} {'P/L':>9}")
    for s, e, n, wr, pnl in derived_time:
        print(f"    {s}-{e}   {n:>4} {wr:>5.1f}% {pnl:>+8.1f}p")
    print()
    print(f"  PAIR BLACKLIST ({len(derived_pair)} pairs):")
    print(f"    {'Pair':<8} {'n':>4} {'WR':>6} {'P/L':>9}")
    for pair, n, wr, pnl in derived_pair:
        print(f"    {pair:<8} {n:>4} {wr:>5.1f}% {pnl:>+8.1f}p")
    print()
    print(f"  ADR BLACKLIST: trades with adr_consumed_pct > {ADR_MAX}%")
    adr_blocked = [t for t in raw if (t.get("adr_consumed_pct") or 0) > ADR_MAX]
    adr_w = sum(1 for t in adr_blocked if t["_net_is_win"])
    adr_p = sum(t["_net_pnl"] for t in adr_blocked)
    if adr_blocked:
        print(f"    {len(adr_blocked)} trades / WR {adr_w/len(adr_blocked)*100:.1f}% "
              f"/ P/L {adr_p:+.1f}p / avg {adr_p/len(adr_blocked):+.2f}p")
    print()

    # Run all 4 configurations
    configs = [
        ("CURRENT (ADR<=70 only)", None, None, ADR_MAX),
        ("TIME + PAIR (no ADR)",   derived_time, derived_pair, None),
        ("TIME + ADR (no PAIR)",   derived_time, None, ADR_MAX),
        ("ALL THREE (TIME + PAIR + ADR)", derived_time, derived_pair, ADR_MAX),
    ]

    all_stats = {}
    for label, t_bl, p_bl, adr in configs:
        filtered = apply_filters(raw, time_bl=t_bl, pair_bl=p_bl, adr_max=adr)
        all_stats[label] = stats(filtered, label)

    # Detailed per-config breakdown
    for label, _, _, _ in configs:
        s = all_stats[label]
        print("=" * 78)
        print(f"  {label}")
        print("=" * 78)
        for line in fmt_block(s):
            print(line)
        print()

    # Side-by-side comparison
    print("=" * 78)
    print("  SIDE-BY-SIDE COMPARISON")
    print("=" * 78)
    print()
    keys = ["n", "wr", "pnl", "pf", "mdd", "tpd", "daily", "bal"]
    headers = ["Trades", "Win Rate", "Net P/L", "Profit Factor",
               "Max DD", "Trades/Day", "Daily Return", "Final $ (from $1k)"]
    fmts = [
        lambda s: f"{s['n']}",
        lambda s: f"{s['wr']:.1f}%",
        lambda s: f"{s['pnl']:+.1f}p",
        lambda s: f"{s['pf']:.2f}",
        lambda s: f"{s['mdd']:.1f}%",
        lambda s: f"{s['tpd']:.1f}",
        lambda s: f"{s['daily']:+.2f}%",
        lambda s: f"${s['bal']:,.0f}",
    ]
    cfgs = [c[0] for c in configs]
    # Header
    print(f"  {'Metric':<20} | " + " | ".join(f"{c[:18]:>18}" for c in cfgs))
    print(f"  {'-'*20}-+-" + "-+-".join("-"*18 for _ in cfgs))
    for header, fn in zip(headers, fmts):
        row = f"  {header:<20} | "
        row += " | ".join(f"{fn(all_stats[c]):>18}" for c in cfgs)
        print(row)
    print()

    # Improvements over CURRENT
    print("=" * 78)
    print("  IMPROVEMENT OVER CURRENT (ADR<=70 only)")
    print("=" * 78)
    print()
    cur = all_stats["CURRENT (ADR<=70 only)"]
    print(f"  {'Strategy':<32} {'Final $':>10} {'Δ$':>10} {'Δ%':>8} "
          f"{'WR Δ':>7} {'PF Δ':>7} {'DD Δ':>7}")
    for label, _, _, _ in configs:
        s = all_stats[label]
        d_bal = s["bal"] - cur["bal"]
        d_pct = (s["bal"] - cur["bal"]) / cur["bal"] * 100
        d_wr = s["wr"] - cur["wr"]
        d_pf = s["pf"] - cur["pf"]
        d_dd = s["mdd"] - cur["mdd"]
        flag = " ★" if label != "CURRENT (ADR<=70 only)" and d_bal > 0 else ""
        print(f"  {label:<32} ${s['bal']:>7,.0f} "
              f"${d_bal:>+8,.0f} {d_pct:>+6.1f}% {d_wr:>+5.1f}pp "
              f"{d_pf:>+5.2f} {d_dd:>+5.1f}pp{flag}")
    print()

    # Forward projections for each
    print("=" * 78)
    print("  FORWARD PROJECTIONS — what to expect over the next month")
    print("=" * 78)
    print()
    print("  Daily compound rate × decay scenario × time horizon")
    print()
    print(f"  {'Strategy':<32} {'Daily':>7} | {'1mo realistic':>14} {'2mo realistic':>14} | "
          f"{'1mo no-decay':>13} {'1mo pessim.':>12}")
    print(f"  {'-'*32} {'-'*7}-+-{'-'*14} {'-'*14}-+-{'-'*13} {'-'*12}")
    for label, _, _, _ in configs:
        s = all_stats[label]
        d = s["daily"]
        # Realistic = 50% of measured daily
        realistic = d * 0.50
        pess = d * 0.20
        print(f"  {label:<32} {d:>+6.2f}% | "
              f"{proj(realistic, 22):>+12.0f}% {proj(realistic, 44):>+12.0f}% | "
              f"{proj(d, 22):>+11.0f}% {proj(pess, 22):>+10.0f}%")
    print()
    print("  ($1k starting balance, scale linearly for larger accounts)")
    print()

    # Concrete dollar projections
    print("=" * 78)
    print("  $$ DOLLAR PROJECTIONS (realistic 50%-of-measured decay)")
    print("=" * 78)
    print()
    starts = [1000, 10000, 100000]
    print(f"  {'Strategy':<32} | " +
          " | ".join(f"{'Start $'+str(s):>14}" for s in starts) +
          " | " + f"{'1-month':>10} | {'2-month':>10}")
    for label, _, _, _ in configs:
        s = all_stats[label]
        realistic = s["daily"] * 0.50
        for_horizon_22 = (1 + realistic/100)**22
        for_horizon_44 = (1 + realistic/100)**44
        row = f"  {label:<32} | "
        row += " | ".join(f"${st * for_horizon_22:>13,.0f}" for st in starts)
        row += f" | (1-month) | (2-month)"
        print(row)
    print()
    for label, _, _, _ in configs:
        s = all_stats[label]
        realistic = s["daily"] * 0.50
        f22 = (1 + realistic/100)**22
        f44 = (1 + realistic/100)**44
        print(f"  {label}:")
        for st in starts:
            print(f"    Start ${st:>6,} → 1mo ${st*f22:>10,.0f}  →  2mo ${st*f44:>13,.0f}")
        print()


if __name__ == "__main__":
    main()
