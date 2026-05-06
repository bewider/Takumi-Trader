"""Test: can `adr_consumed_pct <= 70` REPLACE the per-system pair
blacklists in DTC, or do they ADD value when stacked?

Hypothesis: ADR filter on raw data improves every system. But the DTC
pair blacklist also improves performance. Are they redundant (catch the
same bad trades) or complementary (catch different bad trades)?

We test 4 configurations on the SS+ATR+B-tuned union:
  A. Time-bl ONLY                (no pair, no ADR)    — minimal
  B. Time-bl + Pair-bl           (current DTC config) — baseline
  C. Time-bl + ADR<=70           (replace pair w/ ADR)
  D. Time-bl + Pair-bl + ADR<=70 (stack both)         — most aggressive
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


SYSTEMS = {
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


def load_with_filter(use_pair_bl, use_adr_filter, adr_max=70):
    """Load union of SS+ATR+B-tuned with selectable filters."""
    out = []
    for name, (fname, tbl, pbl) in SYSTEMS.items():
        recs = json.loads((DATA / fname).read_text(encoding="utf-8"))
        closed = [r for r in recs if r.get("close_reason")]
        for r in closed:
            # Always apply time blackout (per-system) and commission
            dt = datetime.fromtimestamp(r["entry_time"], tz=JST)
            if in_blackout(dt, tbl):
                continue
            if use_pair_bl and r.get("pair") in pbl:
                continue
            if use_adr_filter:
                adr = r.get("adr_consumed_pct")
                # Hard filter: drop if ADR exceeds max OR if measurement missing
                # (soft alternative would skip the check when missing)
                if adr is None or adr > adr_max:
                    continue
            r = dict(r)
            r["_net_pnl"] = (r.get("pnl_pips", 0) or 0) - commission(r.get("pair", ""))
            r["_net_is_win"] = r["_net_pnl"] > 0
            r["_system"] = name
            out.append(r)
    # 120s same-pair dedup
    by_pair = defaultdict(list)
    for t in out:
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


def stats(trades):
    if not trades:
        return None
    n = len(trades)
    w = sum(1 for t in trades if t["_net_is_win"])
    p = sum(t["_net_pnl"] for t in trades)
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
    days = max(1, len(dates))
    return dict(n=n, wr=w/n*100, pnl=p, mdd=mdd, bal=bal,
                ret_pct=(bal/1000-1)*100, days=days,
                tpd=n/days, daily=((bal/1000)**(1/days)-1)*100)


def fmt(label, s):
    if s is None:
        return f"  {label:<60} (no trades)"
    return (f"  {label:<60} n={s['n']:>3} WR={s['wr']:>5.1f}% "
            f"P/L={s['pnl']:>+6.0f}p  PF=  DD={s['mdd']:>4.1f}%  "
            f"daily={s['daily']:>+5.2f}%  ${s['bal']:>6,.0f}")


def soft_load_with_filter(use_pair_bl, use_adr_filter, adr_max=70):
    """Same as load_with_filter but ADR filter is SOFT: trades without
    the ADR measurement still pass. Avoids killing older trades."""
    out = []
    for name, (fname, tbl, pbl) in SYSTEMS.items():
        recs = json.loads((DATA / fname).read_text(encoding="utf-8"))
        closed = [r for r in recs if r.get("close_reason")]
        for r in closed:
            dt = datetime.fromtimestamp(r["entry_time"], tz=JST)
            if in_blackout(dt, tbl):
                continue
            if use_pair_bl and r.get("pair") in pbl:
                continue
            if use_adr_filter:
                adr = r.get("adr_consumed_pct")
                # SOFT: only block if measurement exists AND exceeds threshold
                if adr is not None and adr > adr_max:
                    continue
            r = dict(r)
            r["_net_pnl"] = (r.get("pnl_pips", 0) or 0) - commission(r.get("pair", ""))
            r["_net_is_win"] = r["_net_pnl"] > 0
            r["_system"] = name
            out.append(r)
    by_pair = defaultdict(list)
    for t in out:
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


def main():
    print("ADR FILTER vs PAIR BLACKLIST — replacement / augment test\n")
    print("Dataset: SS + ATR + B-tuned union, time-blackout always applied,")
    print("120s same-pair dedup always applied, commission always applied.\n")

    # All 4 configs HARD ADR (drops if measurement missing)
    print(f"{'='*92}")
    print(f"  HARD ADR filter (drops trades with no ADR measurement)")
    print(f"{'='*92}")
    print(fmt("A. Time-bl ONLY",
              stats(load_with_filter(False, False))))
    print(fmt("B. Time-bl + Pair-bl (CURRENT DTC)",
              stats(load_with_filter(True, False))))
    print(fmt("C. Time-bl + ADR<=70 (replace pair w/ ADR)",
              stats(load_with_filter(False, True, 70))))
    print(fmt("D. Time-bl + Pair-bl + ADR<=70 (stack both)",
              stats(load_with_filter(True, True, 70))))

    print(f"\n{'='*92}")
    print(f"  SOFT ADR filter (trades without ADR measurement pass through)")
    print(f"{'='*92}")
    print(fmt("A. Time-bl ONLY",
              stats(soft_load_with_filter(False, False))))
    print(fmt("B. Time-bl + Pair-bl (CURRENT DTC)",
              stats(soft_load_with_filter(True, False))))
    print(fmt("C. Time-bl + ADR<=70 SOFT (replace pair w/ ADR)",
              stats(soft_load_with_filter(False, True, 70))))
    print(fmt("D. Time-bl + Pair-bl + ADR<=70 SOFT (stack both)",
              stats(soft_load_with_filter(True, True, 70))))

    # Also try varying ADR max
    print(f"\n{'='*92}")
    print(f"  ADR threshold sweep (SOFT, with current Pair-bl in place)")
    print(f"{'='*92}")
    for adr_max in [50, 55, 60, 65, 70, 75, 80, 85, 90]:
        print(fmt(f"Pair-bl + ADR<={adr_max} (SOFT)",
                  stats(soft_load_with_filter(True, True, adr_max))))

    print(f"\n{'='*92}")
    print(f"  ADR threshold sweep (SOFT, NO Pair-bl)")
    print(f"{'='*92}")
    for adr_max in [50, 55, 60, 65, 70, 75, 80, 85, 90]:
        print(fmt(f"Time-bl + ADR<={adr_max} (SOFT, no pair filter)",
                  stats(soft_load_with_filter(False, True, adr_max))))


if __name__ == "__main__":
    main()
