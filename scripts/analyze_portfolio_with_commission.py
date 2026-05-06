"""Portfolio re-analysis with 6% commission on EACH trade.

Commission model: 0.06 × sl_pips subtracted from every trade's pnl
(both winners and losers pay it). This is equivalent to 0.06R per
round-trip — how commission-based FX brokers charge.

After applying commission:
  1. Identify per-pair outcomes in each system — flag pairs that flip
     to negative or near-zero.
  2. Build NEW optimized pair blacklists.
  3. Re-run portfolio with updated filters + dedup.
  4. Give final combo recommendation.
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
COMMISSION_R = 0.06  # 6% of SL (risk) per trade
DEDUP_SECONDS = 120


def apply_commission(r):
    """Mutate trade to include commission. Returns the NET pnl in pips."""
    sl = r.get("sl_pips", 0) or 0
    gross = r.get("pnl_pips", 0) or 0
    comm = sl * COMMISSION_R
    net = gross - comm
    r["_gross_pnl"] = gross
    r["_commission"] = comm
    r["_net_pnl"] = net
    # Recalculate is_win based on NET (a 0.5p win before comm may flip to loss)
    r["_net_is_win"] = net > 0
    return net


FILTERS = {
    "SS": {
        "file": "paper_trades_ss.json",
        "time_bl": [
            ("08:00", "08:15"), ("14:15", "14:30"),
            ("16:00", "16:15"), ("16:30", "16:45"),
            ("17:45", "18:00"),
            ("20:15", "20:45"), ("21:00", "21:15"),
        ],
        "pair_bl": {"GBPAUD", "EURUSD", "AUDCAD", "AUDCHF", "EURCAD"},
    },
    "ATR": {
        "file": "paper_trades_atr.json",
        "time_bl": [("16:00", "16:15")],
        "pair_bl": {"EURUSD", "AUDCAD", "EURNZD", "GBPAUD"},
    },
    "B-tuned": {
        "file": "paper_trades_b_tuned.json",
        "time_bl": [
            ("08:15", "08:30"), ("14:15", "14:30"),
            ("15:00", "15:15"), ("16:30", "16:45"),
            ("17:00", "17:15"), ("19:00", "19:15"),
        ],
        "pair_bl": set(),
    },
}


def hm_to_min(hm):
    h, m = hm.split(":")
    return int(h) * 60 + int(m)


def in_blackout(dt, bl):
    mins = dt.hour * 60 + dt.minute
    for s, e in bl:
        if hm_to_min(s) <= mins < hm_to_min(e):
            return True
    return False


def load_filter_commission(system_name):
    cfg = FILTERS[system_name]
    recs = json.loads((DATA / cfg["file"]).read_text(encoding="utf-8"))
    closed = [r for r in recs if r.get("close_reason")]
    kept = []
    for r in closed:
        if r.get("pair") in cfg["pair_bl"]:
            continue
        dt = datetime.fromtimestamp(r["entry_time"], tz=JST)
        if in_blackout(dt, cfg["time_bl"]):
            continue
        apply_commission(r)
        r["_system"] = system_name
        kept.append(r)
    return kept


def simulate(trades, risk_pct=3.0, start_cap=1000.0, use_net=True):
    if not trades:
        return None
    sorted_t = sorted(trades, key=lambda r: r.get("entry_time", 0))
    bal = start_cap
    peak = start_cap
    mdd = 0.0
    for r in sorted_t:
        sl = r.get("sl_pips", 0) or 10
        pp = r["_net_pnl"] if use_net else (r.get("pnl_pips", 0) or 0)
        rm = pp / sl if sl > 0 else 0
        bal += bal * (risk_pct / 100) * rm
        if bal > peak:
            peak = bal
        dd = (peak - bal) / peak * 100
        if dd > mdd:
            mdd = dd
    n = len(sorted_t)
    pnl = sum(r["_net_pnl"] if use_net else (r.get("pnl_pips", 0) or 0)
              for r in sorted_t)
    wins = [r for r in sorted_t
            if (r["_net_is_win"] if use_net else r.get("is_win"))]
    gw = sum((r["_net_pnl"] if use_net else (r.get("pnl_pips", 0) or 0))
             for r in wins)
    gl = abs(pnl - gw)
    dates = set(datetime.fromtimestamp(r["entry_time"], tz=JST).date()
                for r in sorted_t)
    avg_sl = mean(r.get("sl_pips", 0) or 0 for r in sorted_t)
    days = max(1, len(dates))
    tpd = n / days
    avg = pnl / n
    avg_r = avg / avg_sl if avg_sl > 0 else 0
    daily = avg_r * tpd * (risk_pct / 100) * 100
    return dict(
        n=n, wr=len(wins) / n * 100,
        pnl=pnl, avg=avg,
        avg_win=gw / len(wins) if wins else 0,
        avg_loss=-gl / (n - len(wins)) if n > len(wins) else 0,
        pf=gw / gl if gl > 0 else float("inf"),
        final=bal, return_pct=(bal / start_cap - 1) * 100,
        mdd=mdd, days=days, tpd=tpd, daily=daily,
        pips_per_day=pnl / days,
    )


def print_stats(label, s):
    print(f"\n{'='*72}\n  {label}\n{'='*72}")
    if s is None:
        print("  (no trades)")
        return
    print(f"  Trades: {s['n']}   WR: {s['wr']:.1f}%")
    print(f"  Total P/L (net): {s['pnl']:+.1f}p   Avg: {s['avg']:+.2f}p")
    print(f"  Avg Win / Loss: {s['avg_win']:+.1f}p / {s['avg_loss']:+.1f}p")
    print(f"  Profit Factor: {s['pf']:.2f}   Max DD: {s['mdd']:.1f}%")
    print(f"  Days: {s['days']}   Trades/Day: {s['tpd']:.1f}   "
          f"Pips/Day: {s['pips_per_day']:+.1f}p")
    print(f"  Daily Compound: {s['daily']:+.2f}%")
    print(f"  Final: ${s['final']:,.0f}  ({s['return_pct']:+.1f}%)")


def pair_breakdown(trades, system_name, min_n=3):
    pair_map = defaultdict(list)
    for t in trades:
        pair_map[t["pair"]].append(t)
    rows = []
    for p, ts in pair_map.items():
        if len(ts) < min_n:
            continue
        n = len(ts)
        wins = sum(1 for t in ts if t["_net_is_win"])
        gross = sum(t["_gross_pnl"] for t in ts)
        net = sum(t["_net_pnl"] for t in ts)
        rows.append((p, n, wins / n * 100, gross, net))
    return sorted(rows, key=lambda x: x[4])


def dedup(trades, win_s=DEDUP_SECONDS):
    by_pair = defaultdict(list)
    for t in trades:
        by_pair[t["pair"]].append(t)
    kept = []
    dropped = 0
    for p, ts in by_pair.items():
        ts_sorted = sorted(ts, key=lambda x: x["entry_time"])
        last = None
        for t in ts_sorted:
            et = t["entry_time"]
            if last is None or (et - last) >= win_s:
                kept.append(t)
                last = et
            else:
                dropped += 1
    return kept, dropped


def proj(dr, days):
    return ((1 + dr / 100) ** days - 1) * 100


def main():
    print(f"PORTFOLIO WITH 6% COMMISSION (0.06R per trade)\n")

    all_systems = {}
    for name in ("SS", "ATR", "B-tuned"):
        kept = load_filter_commission(name)
        all_systems[name] = kept
        s_gross = simulate(kept, use_net=False)
        s_net = simulate(kept, use_net=True)
        print(f"\n--- {name} ---")
        print(f"  Gross: {s_gross['n']} tr, WR {s_gross['wr']:.1f}%, "
              f"{s_gross['pnl']:+.1f}p, PF {s_gross['pf']:.2f}, "
              f"final ${s_gross['final']:,.0f}")
        print(f"  NET  : {s_net['n']} tr, WR {s_net['wr']:.1f}%, "
              f"{s_net['pnl']:+.1f}p, PF {s_net['pf']:.2f}, "
              f"final ${s_net['final']:,.0f}")

    # Per-pair breakdown to find pairs flipped to negative by commission
    print(f"\n\n{'#'*72}")
    print("  PAIR BREAKDOWN AFTER COMMISSION — flag pairs to remove")
    print(f"{'#'*72}")

    recommended_blacklist_add = defaultdict(set)

    for name in ("SS", "ATR", "B-tuned"):
        rows = pair_breakdown(all_systems[name], name, min_n=3)
        print(f"\n  === {name} ===")
        print(f"    {'Pair':<8} {'n':>4} {'WR':>6} "
              f"{'Gross':>8} {'Net':>8}  flag")
        for p, n, wr, gross, net in rows:
            flag = ""
            if net < -3:
                flag = " ❌ REMOVE"
                recommended_blacklist_add[name].add(p)
            elif net < 3:
                flag = " ⚠️  near-zero"
                if net < 1:
                    recommended_blacklist_add[name].add(p)
            elif net < gross * 0.5 and gross > 5:
                flag = " 📉 comm hurt"
            print(f"    {p:<8} {n:>4} {wr:>5.1f}% "
                  f"{gross:>+7.1f}p {net:>+7.1f}p{flag}")

    print(f"\n{'#'*72}")
    print("  RECOMMENDED ADDITIONAL PAIR BLACKLISTS (from commission impact)")
    print(f"{'#'*72}")
    for name, adds in recommended_blacklist_add.items():
        existing = FILTERS[name]["pair_bl"]
        new_total = existing | adds
        print(f"  {name}:")
        print(f"    Current: {sorted(existing)}")
        print(f"    Add:     {sorted(adds)}")
        print(f"    NEW total: {sorted(new_total)}")

    # Re-run portfolio with updated blacklists
    print(f"\n\n{'#'*72}")
    print("  RE-OPTIMIZED PORTFOLIO (additional blacklists applied)")
    print(f"{'#'*72}")

    reopt = []
    for name in ("SS", "ATR", "B-tuned"):
        new_bl = FILTERS[name]["pair_bl"] | recommended_blacklist_add[name]
        kept = [t for t in all_systems[name] if t["pair"] not in new_bl]
        reopt.extend(kept)
        s = simulate(kept, use_net=True)
        print(f"\n  {name} (with new blacklist): "
              f"{s['n']} tr, WR {s['wr']:.1f}%, "
              f"{s['pnl']:+.1f}p, PF {s['pf']:.2f}, "
              f"DD {s['mdd']:.1f}%, daily {s['daily']:+.2f}%, "
              f"final ${s['final']:,.0f}")

    # Dedup combined
    merged_kept, dropped = dedup(reopt)
    print(f"\n  Merged {len(reopt)} → dedup → {len(merged_kept)} "
          f"(dropped {dropped})")

    final = simulate(merged_kept, use_net=True)
    print_stats("FINAL PORTFOLIO (6% commission + optimized blacklists + dedup)",
                final)

    # Projections
    print(f"\n  Forward projections (net-of-commission):")
    dr = final["daily"]
    print(f"    1 week   (5d):  {proj(dr,5):+.1f}%")
    print(f"    2 weeks (10d):  {proj(dr,10):+.1f}%")
    print(f"    1 month (22d):  {proj(dr,22):+.1f}%")
    print(f"    2 months(44d):  {proj(dr,44):+.1f}%")

    # Pair leaderboard final
    print(f"\n  Final pair leaderboard (n≥5, net P/L):")
    pair_map = defaultdict(list)
    for t in merged_kept:
        pair_map[t["pair"]].append(t)
    rows = []
    for p, ts in pair_map.items():
        if len(ts) < 5:
            continue
        n = len(ts)
        wins = sum(1 for t in ts if t["_net_is_win"])
        net = sum(t["_net_pnl"] for t in ts)
        rows.append((p, n, wins / n * 100, net))
    rows.sort(key=lambda x: -x[3])
    print(f"    {'Pair':<8} {'n':>4} {'WR':>6} {'Net P/L':>9}")
    for p, n, wr, net in rows:
        flag = " ✓✓" if net > 50 else " ✓" if net > 10 else \
               " ❌" if net < -5 else " ⚠️" if net < 5 else ""
        print(f"    {p:<8} {n:>4} {wr:>5.1f}% {net:>+8.1f}p{flag}")

    # Also do each system ALONE with the new blacklist, in case user
    # prefers single-system
    print(f"\n\n{'#'*72}")
    print("  EACH SYSTEM STANDALONE (6% commission + new blacklists)")
    print(f"{'#'*72}")
    for name in ("SS", "ATR", "B-tuned"):
        new_bl = FILTERS[name]["pair_bl"] | recommended_blacklist_add[name]
        kept = [t for t in all_systems[name] if t["pair"] not in new_bl]
        s = simulate(kept, use_net=True)
        print(f"\n  {name}: {s['n']} tr, WR {s['wr']:.1f}%, "
              f"{s['pnl']:+.1f}p, PF {s['pf']:.2f}, "
              f"DD {s['mdd']:.1f}%, daily {s['daily']:+.2f}%, "
              f"final ${s['final']:,.0f} ({s['return_pct']:+.1f}%)")


if __name__ == "__main__":
    main()
