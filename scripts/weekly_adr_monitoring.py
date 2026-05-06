"""Weekly ADR-overlap monitor.

Runs every Friday alongside the regular weekly review. Tracks how the
ADR-quality filter compares to the per-system pair blacklist over time:

  - For each system, count trades that the existing pair blacklist would
    have blocked vs trades the ADR filter would have blocked.
  - Compute the overlap (trades caught by BOTH) and the divergence (caught
    by only one).
  - If the divergence grows (ADR catches more bad trades than pair-bl), it's
    a signal to consider adding/replacing pair-bl with ADR filter.
  - If overlap stays high, the pair blacklist is doing its job.

Output:
  - Per-system table of trades-that-would-be-blocked breakdown
  - Trend indicator: STABLE / SHIFTING TOWARD ADR / SHIFTING AWAY
  - Recommended action if a shift is detected
  - Appended to data/optimization_results.md as new section
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
ADR_THRESHOLD = 70.0


def commission(pair):
    if pair in USD_QUOTE: return 0.6
    if pair.endswith("JPY"): return 0.7
    return 0.8


# Current production filters per system (mirror DTC config)
SYSTEMS = {
    "Sv2 (A)":     {"file": "paper_trades.json",         "time_bl": [], "pair_bl": set()},
    "SS (B)":      {"file": "paper_trades_ss.json",
                    "time_bl": [("08:00","08:15"),("14:15","14:30"),("16:00","16:15"),
                                ("16:30","16:45"),("17:45","18:00"),("20:15","20:45"),
                                ("21:00","21:15")],
                    "pair_bl": {"AUDCAD","AUDCHF","CADCHF","EURCAD","EURCHF","EURUSD","GBPAUD"}},
    "ATR (C)":     {"file": "paper_trades_atr.json",
                    "time_bl": [("16:00","16:15")],
                    "pair_bl": {"AUDCAD","AUDCHF","EURCAD","EURNZD","EURUSD","GBPAUD"}},
    "A-tuned (D)": {"file": "paper_trades_a_tuned.json", "time_bl": [], "pair_bl": set()},
    "B-tuned (E)": {"file": "paper_trades_b_tuned.json",
                    "time_bl": [("08:15","08:30"),("14:15","14:30"),("15:00","15:15"),
                                ("16:30","16:45"),("17:00","17:15"),("19:00","19:15")],
                    "pair_bl": {"AUDCAD","AUDCHF","AUDJPY","EURNZD","GBPCAD","NZDUSD"}},
}


def hm_to_min(s):
    h, m = s.split(":"); return int(h)*60 + int(m)


def in_blackout(dt, bl):
    mins = dt.hour*60 + dt.minute
    return any(hm_to_min(s) <= mins < hm_to_min(e) for s, e in bl)


def load_trades(filename):
    """Load all closed trades, apply commission, return list."""
    recs = json.loads((DATA / filename).read_text(encoding="utf-8"))
    out = []
    for r in recs:
        if not r.get("close_reason"):
            continue
        r = dict(r)
        r["_net_pnl"] = (r.get("pnl_pips", 0) or 0) - commission(r.get("pair", ""))
        r["_net_is_win"] = r["_net_pnl"] > 0
        out.append(r)
    return out


def stats(trades):
    if not trades:
        return dict(n=0, wr=0, pnl=0, avg=0)
    n = len(trades)
    w = sum(1 for t in trades if t["_net_is_win"])
    p = sum(t["_net_pnl"] for t in trades)
    return dict(n=n, wr=w/n*100, pnl=p, avg=p/n)


def categorize_trades(system_name, trades):
    """For each trade, determine which filters would block it.

    Returns dict with keys: 'kept', 'pair_blocked', 'adr_blocked',
    'both_blocked', 'time_blocked', 'no_data' (no ADR measurement).
    """
    cfg = SYSTEMS[system_name]
    out = {"kept": [], "pair_blocked": [], "adr_blocked": [],
           "both_blocked": [], "time_blocked": [], "no_data": []}
    for t in trades:
        # Time blackout always blocks first
        dt = datetime.fromtimestamp(t["entry_time"], tz=JST)
        if in_blackout(dt, cfg["time_bl"]):
            out["time_blocked"].append(t)
            continue
        adr = t.get("adr_consumed_pct")
        pair_blocked = t.get("pair") in cfg["pair_bl"]
        adr_blocked = (adr is not None) and (adr > ADR_THRESHOLD)
        if adr is None and pair_blocked:
            out["pair_blocked"].append(t)
        elif adr is None:
            out["no_data"].append(t)
            # No ADR data — counts as "kept" (we can't filter it)
            if not pair_blocked:
                out["kept"].append(t)
        elif pair_blocked and adr_blocked:
            out["both_blocked"].append(t)
        elif pair_blocked:
            out["pair_blocked"].append(t)
        elif adr_blocked:
            out["adr_blocked"].append(t)
        else:
            out["kept"].append(t)
    return out


def fmt_stats(label, trades):
    s = stats(trades)
    if s["n"] == 0:
        return f"  {label:<22} n=0"
    return (f"  {label:<22} n={s['n']:>3} WR={s['wr']:>5.1f}% "
            f"P/L={s['pnl']:>+7.1f}p avg={s['avg']:>+5.2f}p")


def main():
    print(f"WEEKLY ADR-OVERLAP MONITOR  (ADR threshold: {ADR_THRESHOLD}%)")
    print(f"Run date: {datetime.now().strftime('%Y-%m-%d')}\n")
    print("Tracks how the ADR filter compares to the per-system pair blacklist.")
    print("Goal: detect if ADR is starting to add value beyond what pair-bl catches.\n")

    summary_rows = []

    for name, cfg in SYSTEMS.items():
        trades = load_trades(cfg["file"])
        if not trades:
            continue
        cats = categorize_trades(name, trades)

        print(f"\n{'='*78}")
        print(f"  {name}  ({len(trades)} closed trades)")
        print(f"{'='*78}")
        # Population
        with_adr = [t for t in trades if t.get("adr_consumed_pct") is not None]
        print(f"  ADR field populated on {len(with_adr)}/{len(trades)} trades "
              f"({len(with_adr)/len(trades)*100:.0f}%)")

        print()
        print(fmt_stats("KEPT (live)", cats["kept"]))
        print(fmt_stats("Time-blackout", cats["time_blocked"]))
        if cfg["pair_bl"]:
            print(fmt_stats("Pair-bl ONLY", cats["pair_blocked"]))
        print(fmt_stats("ADR-bl ONLY", cats["adr_blocked"]))
        if cfg["pair_bl"]:
            print(fmt_stats("BOTH (pair+ADR)", cats["both_blocked"]))
        print(fmt_stats("(no ADR data)", cats["no_data"]))

        # Decision metrics
        n_pair_only = len(cats["pair_blocked"])
        n_adr_only = len(cats["adr_blocked"])
        n_both = len(cats["both_blocked"])

        # Rule-of-thumb: if ADR-only is catching more BAD trades than pair-only
        # it's a signal that ADR could be added/strengthened
        adr_only_stats = stats(cats["adr_blocked"])
        pair_only_stats = stats(cats["pair_blocked"])

        print(f"\n  ADR vs PAIR catch comparison:")
        print(f"    ADR-only catches: {n_adr_only} trades")
        if n_adr_only > 0:
            print(f"      → those trades' WR: {adr_only_stats['wr']:.1f}%, "
                  f"avg P/L: {adr_only_stats['avg']:+.2f}p")
            verdict = ("USEFUL — would block bad trades"
                       if adr_only_stats["avg"] < 0
                       else "BORDERLINE — those trades are net-positive")
            print(f"      → verdict: {verdict}")
        if cfg["pair_bl"]:
            print(f"    Pair-only catches: {n_pair_only} trades")
            if n_pair_only > 0:
                print(f"      → those trades' WR: {pair_only_stats['wr']:.1f}%, "
                      f"avg P/L: {pair_only_stats['avg']:+.2f}p")
            print(f"    Both filters agree on: {n_both} trades")

        # Per-pair ADR breakdown (top losers by avg PnL where ADR>70)
        if n_adr_only > 0 or n_both > 0:
            adr_blocked_all = cats["adr_blocked"] + cats["both_blocked"]
            by_pair_adr = defaultdict(list)
            for t in adr_blocked_all:
                by_pair_adr[t["pair"]].append(t)
            print(f"\n  Top pairs that ADR>{ADR_THRESHOLD:.0f}% would block:")
            rows = []
            for pair, ts in by_pair_adr.items():
                if len(ts) < 2:
                    continue
                s = stats(ts)
                rows.append((pair, s["n"], s["wr"], s["pnl"], s["avg"]))
            rows.sort(key=lambda x: x[4])  # worst avg first
            for p, n, wr, pnl, avg in rows[:10]:
                in_pair_bl = " (in pair-bl)" if p in cfg["pair_bl"] else " (NOT in pair-bl)"
                print(f"    {p:<8} n={n:>3} WR={wr:>5.1f}% "
                      f"P/L={pnl:>+6.1f}p avg={avg:>+5.2f}p{in_pair_bl}")

        # Save row for summary
        summary_rows.append((name, cats, cfg))

    # ── Summary recommendation ──
    print(f"\n\n{'#'*78}")
    print(f"  RECOMMENDATIONS")
    print(f"{'#'*78}\n")

    for name, cats, cfg in summary_rows:
        adr_only = cats["adr_blocked"]
        pair_only = cats["pair_blocked"]
        if not adr_only and not pair_only:
            continue

        # Decision logic:
        # 1. If ADR-only avg < -1p (consistently bad trades), recommend adding ADR
        # 2. If ADR-only avg > +1p (catches good trades), recommend NOT adding
        # 3. If pair-only avg > +1p (pair-bl too aggressive), recommend reviewing pair-bl
        recs = []
        if adr_only:
            adr_avg = stats(adr_only)["avg"]
            if adr_avg < -1.0:
                recs.append(f"ADD ADR<={ADR_THRESHOLD:.0f} filter — would block "
                            f"{len(adr_only)} trades avg {adr_avg:+.2f}p (bad)")
            elif adr_avg > 1.0:
                recs.append(f"DO NOT add ADR filter — those {len(adr_only)} "
                            f"trades avg {adr_avg:+.2f}p (good, would lose them)")

        if pair_only:
            pair_avg = stats(pair_only)["avg"]
            if pair_avg > 1.0:
                recs.append(f"REVIEW pair-bl — {len(pair_only)} blocked trades avg "
                            f"{pair_avg:+.2f}p (positive — pair-bl may be too tight)")

        if not recs:
            recs.append("STABLE — current config is optimal")

        print(f"  {name}:")
        for r in recs:
            print(f"    • {r}")
        print()

    print(f"\n{'#'*78}")
    print(f"  Append below to data/optimization_results.md for trend tracking:")
    print(f"{'#'*78}\n")
    print(f"### {datetime.now().strftime('%Y-%m-%d')} — ADR Monitor")
    for name, cats, cfg in summary_rows:
        adr_only = stats(cats["adr_blocked"])
        pair_only = stats(cats["pair_blocked"])
        print(f"- **{name}**: ADR-only blocks n={adr_only['n']} avg={adr_only['avg']:+.2f}p, "
              f"Pair-only blocks n={pair_only['n']} avg={pair_only['avg']:+.2f}p")


if __name__ == "__main__":
    main()
