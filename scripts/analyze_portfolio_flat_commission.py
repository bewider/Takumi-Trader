"""Portfolio analysis with ACCURATE ICMarkets Raw Spread cTrader commission.

ICMarkets Raw Spread cTrader commission is FIXED per trade in pip-equivalent:
  - USD-quote pairs (EURUSD, GBPUSD, AUDUSD, NZDUSD, USDCAD, USDCHF):
    $6 round-trip per lot / $10 per pip = 0.6 pips
  - JPY pairs (xxxJPY):
    ~$6 round-trip per lot / ~$8-9 per pip = 0.7 pips
  - Other crosses (EURGBP, EURAUD, GBPAUD, etc.):
    ~$6 round-trip per lot / ~$7-8 per pip = 0.8 pips

This replaces the old (0.06 × sl_pips) scaling model, which underestimated
commission on tight-SL trades and overestimated on wide-SL trades.
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
DEDUP_SECONDS = 120

# Pair-specific flat commission (pips per trade round-trip)
USD_QUOTE_PAIRS = {"EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF",
                   "USDJPY"}  # USDJPY quote is JPY but pip value close to USD


def commission_for_pair(pair: str) -> float:
    """Return round-trip commission in pips for ICMarkets Raw cTrader."""
    if pair in USD_QUOTE_PAIRS:
        return 0.6
    if pair.endswith("JPY"):
        return 0.7
    return 0.8  # other crosses


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


def apply_flat_commission(r):
    gross = r.get("pnl_pips", 0) or 0
    comm = commission_for_pair(r.get("pair", ""))
    net = gross - comm
    r["_gross_pnl"] = gross
    r["_commission"] = comm
    r["_net_pnl"] = net
    r["_net_is_win"] = net > 0
    return net


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
        apply_flat_commission(r)
        r["_system"] = system_name
        kept.append(r)
    return kept


def simulate(trades, risk_pct=3.0, start_cap=1000.0):
    if not trades:
        return None
    sorted_t = sorted(trades, key=lambda r: r.get("entry_time", 0))
    bal = start_cap
    peak = start_cap
    mdd = 0.0
    for r in sorted_t:
        sl = r.get("sl_pips", 0) or 10
        pp = r["_net_pnl"]
        rm = pp / sl if sl > 0 else 0
        bal += bal * (risk_pct / 100) * rm
        if bal > peak:
            peak = bal
        dd = (peak - bal) / peak * 100
        if dd > mdd:
            mdd = dd
    n = len(sorted_t)
    pnl = sum(r["_net_pnl"] for r in sorted_t)
    wins = [r for r in sorted_t if r["_net_is_win"]]
    gw = sum(r["_net_pnl"] for r in wins)
    gl = abs(pnl - gw)
    dates = set(datetime.fromtimestamp(r["entry_time"], tz=JST).date()
                for r in sorted_t)
    avg_sl = mean(r.get("sl_pips", 0) or 0 for r in sorted_t)
    days = max(1, len(dates))
    tpd = n / days
    avg = pnl / n
    avg_r = avg / avg_sl if avg_sl > 0 else 0
    daily = avg_r * tpd * (risk_pct / 100) * 100
    total_comm = sum(r["_commission"] for r in sorted_t)
    return dict(
        n=n, wr=len(wins) / n * 100,
        pnl=pnl, avg=avg,
        gross=sum(r["_gross_pnl"] for r in sorted_t),
        commission=total_comm,
        avg_win=gw / len(wins) if wins else 0,
        avg_loss=-gl / (n - len(wins)) if n > len(wins) else 0,
        pf=gw / gl if gl > 0 else float("inf"),
        final=bal, return_pct=(bal / start_cap - 1) * 100,
        mdd=mdd, days=days, tpd=tpd, daily=daily,
        pips_per_day=pnl / days,
    )


def dedup(trades, win_s=DEDUP_SECONDS):
    by_pair = defaultdict(list)
    for t in trades:
        by_pair[t["pair"]].append(t)
    kept = []
    for p, ts in by_pair.items():
        ts_sorted = sorted(ts, key=lambda x: x["entry_time"])
        last = None
        for t in ts_sorted:
            et = t["entry_time"]
            if last is None or (et - last) >= win_s:
                kept.append(t)
                last = et
    return kept


def proj(dr, days):
    return ((1 + dr / 100) ** days - 1) * 100


def pair_breakdown(trades, min_n=3):
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


def main():
    print("PORTFOLIO with FLAT cTrader Raw commission (0.6/0.7/0.8 pips by pair type)\n")

    all_sys = {}
    print(f"  {'System':<10} {'Trades':>6} {'Gross':>8} {'Comm':>8} "
          f"{'Net':>8} {'PF':>6} {'DD':>6} {'Daily':>7} {'Final':>10}")
    for name in ("SS", "ATR", "B-tuned"):
        kept = load_filter_commission(name)
        all_sys[name] = kept
        s = simulate(kept)
        print(f"  {name:<10} {s['n']:>6} "
              f"{s['gross']:>+7.1f}p {s['commission']:>7.1f}p "
              f"{s['pnl']:>+7.1f}p {s['pf']:>5.2f} "
              f"{s['mdd']:>5.1f}% {s['daily']:>+6.2f}% ${s['final']:>9,.0f}")

    # Per-pair with flat commission — find pairs to blacklist
    print(f"\n\n{'#'*72}")
    print("  PAIR BREAKDOWN WITH FLAT COMMISSION — find pairs to remove")
    print(f"{'#'*72}")

    new_bl = defaultdict(set)
    for name in ("SS", "ATR", "B-tuned"):
        rows = pair_breakdown(all_sys[name])
        print(f"\n  === {name} ===")
        print(f"    {'Pair':<8} {'n':>4} {'WR':>6} "
              f"{'Gross':>8} {'Comm':>6} {'Net':>8}  flag")
        for p, n, wr, gross, net in rows:
            total_comm = gross - net
            flag = ""
            if net < -3:
                flag = " ❌ REMOVE"
                new_bl[name].add(p)
            elif net < 1:
                flag = " ⚠️  near-zero"
                new_bl[name].add(p)
            elif net < gross * 0.5 and gross > 5:
                flag = " 📉 comm hurt"
            print(f"    {p:<8} {n:>4} {wr:>5.1f}% "
                  f"{gross:>+7.1f}p {total_comm:>5.1f}p {net:>+7.1f}p{flag}")

    # Re-optimized portfolio
    print(f"\n\n{'#'*72}")
    print("  RE-OPTIMIZED PORTFOLIO (flat commission + updated blacklists)")
    print(f"{'#'*72}")

    reopt = []
    for name in ("SS", "ATR", "B-tuned"):
        combined_bl = FILTERS[name]["pair_bl"] | new_bl[name]
        kept = [t for t in all_sys[name] if t["pair"] not in combined_bl]
        reopt.extend(kept)
        s = simulate(kept)
        print(f"\n  {name} (final blacklist {sorted(combined_bl)}):")
        print(f"    {s['n']} trades, WR {s['wr']:.1f}%, Net {s['pnl']:+.1f}p, "
              f"PF {s['pf']:.2f}, DD {s['mdd']:.1f}%, "
              f"daily {s['daily']:+.2f}%, final ${s['final']:,.0f}")

    merged = dedup(reopt)
    dropped = len(reopt) - len(merged)
    print(f"\n  Merged: {len(reopt)} → dedup → {len(merged)} (dropped {dropped})")

    final = simulate(merged)
    print(f"\n{'='*72}")
    print("  FINAL COMBINED PORTFOLIO")
    print(f"{'='*72}")
    print(f"  Trades: {final['n']}   WR: {final['wr']:.1f}%")
    print(f"  Gross: {final['gross']:+.1f}p   Commission: {final['commission']:.1f}p "
          f"({final['commission']/final['gross']*100:.1f}% of gross)")
    print(f"  Net P/L: {final['pnl']:+.1f}p   Avg: {final['avg']:+.2f}p")
    print(f"  Profit Factor: {final['pf']:.2f}   Max DD: {final['mdd']:.1f}%")
    print(f"  Days: {final['days']}   Trades/Day: {final['tpd']:.1f}   "
          f"Pips/Day: {final['pips_per_day']:+.1f}p")
    print(f"  Daily Compound: {final['daily']:+.2f}%")
    print(f"  Final: ${final['final']:,.0f}  ({final['return_pct']:+.1f}%)")

    # Projections table
    print(f"\n  Forward projections (flat-commission reality):")
    dr = final["daily"]
    for scale_name, scale in [("Best case (no decay)", 1.0),
                               ("Realistic (50% holds)", 0.5),
                               ("Pessimistic (20% holds)", 0.2)]:
        eff = dr * scale
        print(f"\n    {scale_name}  ({eff:+.2f}%/day)")
        for days, label in [(22, "1 month"), (44, "2 months")]:
            r = proj(eff, days)
            for start in (1000, 10000, 100000):
                end = start * (1 + eff/100) ** days
                print(f"      {label:<9} ${start:>7,} → ${end:>14,.0f}   ({r:+,.0f}%)",
                      end="" if start == 1000 else "\n" if start == 100000 else "\n                                                  ")
            print()


if __name__ == "__main__":
    main()
