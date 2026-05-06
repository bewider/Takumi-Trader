"""Check for correlated-pair double losses opened within 15 min.

A "double loss" event = two trades on highly correlated pairs (in the
SAME direction relative to the shared currency) opened within N minutes
that BOTH ended up losers. This indicates risk concentration — the
system is doubling exposure to the same underlying market move.

We check both:
  1. The current filtered/deduped DTC stream (what actually goes live)
  2. The raw filtered SS+ATR+B-tuned union (broader picture)
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
WINDOW_SECONDS = 15 * 60  # 15-minute correlation window

USD_QUOTE = {"EURUSD","GBPUSD","AUDUSD","NZDUSD","USDCAD","USDCHF","USDJPY"}


def commission(pair):
    if pair in USD_QUOTE:
        return 0.6
    if pair.endswith("JPY"):
        return 0.7
    return 0.8


# Highly-correlated pair groups: (pair_a, pair_b, shared_currency_role)
# "shared_currency_role" describes how the pairs share an underlying move:
#   "base"  = both have same base currency (e.g., AUDUSD/AUDJPY share AUD)
#   "quote" = both have same quote currency (e.g., AUDUSD/NZDUSD share USD)
#   "anti"  = pairs share an "anti-X" structure (e.g., GBPAUD/GBPNZD = long GBP)
CORRELATION_GROUPS = [
    # Antipodean-cross groups (the highest correlation in FX)
    ("AUDUSD", "NZDUSD", "anti-USD long AUD/NZD"),
    ("AUDJPY", "NZDJPY", "anti-JPY long AUD/NZD"),
    ("AUDCAD", "NZDCAD", "anti-CAD long AUD/NZD"),
    ("AUDCHF", "NZDCHF", "anti-CHF long AUD/NZD"),
    ("GBPAUD", "GBPNZD", "long GBP short AUD/NZD"),
    ("EURAUD", "EURNZD", "long EUR short AUD/NZD"),
    # JPY-cross block
    ("EURJPY", "GBPJPY", "anti-JPY long EUR/GBP"),
    ("CADJPY", "CHFJPY", "anti-JPY long CAD/CHF"),
    ("AUDJPY", "GBPJPY", "anti-JPY long AUD/GBP"),
    ("NZDJPY", "GBPJPY", "anti-JPY long NZD/GBP"),
    # USD-cross block
    ("EURUSD", "GBPUSD", "anti-USD long EUR/GBP"),
    # Antipodean-base block (same base, different quote)
    ("AUDUSD", "AUDJPY", "long AUD vs USD/JPY"),
    ("NZDUSD", "NZDJPY", "long NZD vs USD/JPY"),
]


# Same DTC filters as production
FILTERS = {
    "SS": {
        "file": "paper_trades_ss.json",
        "time_bl": [("08:00","08:15"),("14:15","14:30"),("16:00","16:15"),
                    ("16:30","16:45"),("17:45","18:00"),("20:15","20:45"),
                    ("21:00","21:15")],
        "pair_bl": {"AUDCAD","AUDCHF","CADCHF","EURCAD","EURCHF","EURUSD","GBPAUD"},
    },
    "ATR": {
        "file": "paper_trades_atr.json",
        "time_bl": [("16:00","16:15")],
        "pair_bl": {"AUDCAD","AUDCHF","EURCAD","EURNZD","EURUSD","GBPAUD"},
    },
    "B-tuned": {
        "file": "paper_trades_b_tuned.json",
        "time_bl": [("08:15","08:30"),("14:15","14:30"),("15:00","15:15"),
                    ("16:30","16:45"),("17:00","17:15"),("19:00","19:15")],
        "pair_bl": {"AUDCAD","AUDCHF","AUDJPY","EURNZD","GBPCAD","NZDUSD"},
    },
}


def hm_to_min(s):
    h, m = s.split(":"); return int(h)*60 + int(m)


def in_blackout(dt, bl):
    mins = dt.hour*60 + dt.minute
    return any(hm_to_min(s) <= mins < hm_to_min(e) for s, e in bl)


def load_filtered(name):
    cfg = FILTERS[name]
    recs = json.loads((DATA / cfg["file"]).read_text(encoding="utf-8"))
    closed = [r for r in recs if r.get("close_reason")]
    out = []
    for r in closed:
        if r.get("pair") in cfg["pair_bl"]:
            continue
        dt = datetime.fromtimestamp(r["entry_time"], tz=JST)
        if in_blackout(dt, cfg["time_bl"]):
            continue
        r = dict(r)
        r["_system"] = name
        r["_net_pnl"] = (r.get("pnl_pips", 0) or 0) - commission(r.get("pair", ""))
        r["_net_is_win"] = r["_net_pnl"] > 0
        out.append(r)
    return out


def dedup_120s(trades):
    by_pair = defaultdict(list)
    for t in trades:
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


def directions_share_signal(t1, t2, label):
    """Determine if the two trades represent the SAME directional bet.

    For "anti-X" labels: same direction on both pairs = same bet
      (e.g. both BUY GBPAUD and BUY GBPNZD = long GBP both)
    For "long X vs Y/Z" labels: same direction = same bet
    For "long AUD vs USD/JPY" (same base): same direction = same bet
    Generally, since both pairs in our groups share the same structural
    bet, same direction means same exposure.
    """
    return t1["direction"] == t2["direction"]


def find_correlated_events(trades, window_s=WINDOW_SECONDS):
    """For each correlation group, find pairs of trades within window_s
    on those two pairs in the SAME direction. Return list of events."""
    by_pair = defaultdict(list)
    for t in trades:
        by_pair[t["pair"]].append(t)

    events = []
    for pair_a, pair_b, label in CORRELATION_GROUPS:
        ts_a = sorted(by_pair.get(pair_a, []), key=lambda x: x["entry_time"])
        ts_b = sorted(by_pair.get(pair_b, []), key=lambda x: x["entry_time"])
        if not ts_a or not ts_b:
            continue
        # For each trade in pair_a, find any in pair_b within window
        for ta in ts_a:
            for tb in ts_b:
                gap = abs(ta["entry_time"] - tb["entry_time"])
                if gap > window_s:
                    continue
                if not directions_share_signal(ta, tb, label):
                    continue
                events.append({
                    "group": (pair_a, pair_b),
                    "label": label,
                    "gap_s": gap,
                    "trade_a": ta,
                    "trade_b": tb,
                })
    return events


def categorize(event):
    a_win = event["trade_a"]["_net_is_win"]
    b_win = event["trade_b"]["_net_is_win"]
    if a_win and b_win:
        return "both_win"
    if not a_win and not b_win:
        return "both_lose"
    return "split"


def fmt_dt(ts):
    return datetime.fromtimestamp(ts, tz=JST).strftime("%m-%d %H:%M")


def report(name, trades):
    print(f"\n{'='*78}")
    print(f"  {name}  ({len(trades)} trades)")
    print(f"{'='*78}")
    events = find_correlated_events(trades)
    if not events:
        print("  No correlated-pair co-occurrences found.")
        return

    by_cat = {"both_win":[], "both_lose":[], "split":[]}
    for e in events:
        by_cat[categorize(e)].append(e)

    print(f"  Total co-occurrence events: {len(events)}")
    print(f"    Both wins:    {len(by_cat['both_win'])}")
    print(f"    Both losses:  {len(by_cat['both_lose'])} ← concentration risk")
    print(f"    Split (1W/1L): {len(by_cat['split'])}")

    # Aggregate net P/L across event types
    for cat in ["both_lose", "both_win", "split"]:
        evs = by_cat[cat]
        if not evs:
            continue
        total_pnl = sum(e["trade_a"]["_net_pnl"] + e["trade_b"]["_net_pnl"] for e in evs)
        print(f"\n  Category: {cat}  (n={len(evs)}, total net P/L on both trades: {total_pnl:+.1f}p)")
        for e in sorted(evs, key=lambda x: x["trade_a"]["_net_pnl"]+x["trade_b"]["_net_pnl"])[:15]:
            ta, tb = e["trade_a"], e["trade_b"]
            print(f"    {fmt_dt(min(ta['entry_time'], tb['entry_time']))}  "
                  f"{e['group'][0]:<7} {ta['direction']:<4} {ta['_net_pnl']:>+5.1f}p [{ta.get('_system','?'):<7}] | "
                  f"{e['group'][1]:<7} {tb['direction']:<4} {tb['_net_pnl']:>+5.1f}p [{tb.get('_system','?'):<7}]  "
                  f"gap={e['gap_s']/60:.0f}m  ({e['label']})")
        if len(evs) > 15:
            print(f"    ... and {len(evs)-15} more")

    # Group-level summary
    print(f"\n  Per-group breakdown:")
    print(f"    {'Group':<30} {'events':>7} {'BB-loss':>9} {'BB-win':>9} {'split':>7} "
          f"{'avg-loss-pair-pnl':>18}")
    grouped = defaultdict(list)
    for e in events:
        grouped[e["group"]].append(e)
    for grp, evs in sorted(grouped.items()):
        n = len(evs)
        nl = sum(1 for e in evs if categorize(e) == "both_lose")
        nw = sum(1 for e in evs if categorize(e) == "both_win")
        ns = n - nl - nw
        avg_loss_pnl = (sum(e["trade_a"]["_net_pnl"] + e["trade_b"]["_net_pnl"]
                            for e in evs if categorize(e) == "both_lose") / nl
                        if nl > 0 else 0)
        print(f"    {grp[0]+'/'+grp[1]:<30} {n:>7} {nl:>9} {nw:>9} {ns:>7}  "
              f"{avg_loss_pnl:>+15.1f}p")


def main():
    print(f"CORRELATED-PAIR DOUBLE-LOSS ANALYSIS  (window = {WINDOW_SECONDS//60} min)\n")
    print(f"Looking for trades on correlated pair groups in SAME direction,")
    print(f"opened within {WINDOW_SECONDS//60} min of each other.\n")

    # Load all 3 sources
    all_trades = {}
    for name in FILTERS:
        all_trades[name] = load_filtered(name)
        print(f"  {name}: {len(all_trades[name])} filtered trades")

    # Per-system check
    for name in FILTERS:
        report(f"{name} (alone)", all_trades[name])

    # DTC-equivalent: union + 120s same-pair dedup
    union = []
    for name in FILTERS:
        union.extend(all_trades[name])
    deduped = dedup_120s(union)
    report(f"DTC-COMBO (SS + ATR + B-tuned, 120s same-pair dedup)", deduped)


if __name__ == "__main__":
    main()
