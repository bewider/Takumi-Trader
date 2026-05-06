"""Compare filter strategies across all 5 currency-strength systems.

Tests each filter type alone and in combination:
  1. BASELINE         — no filter
  2. TIME-bl          — manual 15-min window blacklist
  3. ADR<=70          — universal ADR rule
  4. PAIR-bl          — manual per-system pair blacklist
  5. TIME + ADR       — combined time + ADR
  6. TIME + PAIR      — combined time + pair (current DTC config)
  7. ADR + PAIR       — combined ADR + pair
  8. ALL THREE        — time + pair + ADR

For systems without predefined time/pair filters (Sv2, A-tuned), we
auto-derive a time blacklist from the raw history for the comparison.
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


# Current production filters (what is LIVE today)
CURRENT_FILTERS = {
    "Sv2 (A)": {
        "file": "paper_trades.json",
        "time_bl": [],
        "pair_bl": set(),
        "has_adr": True,
    },
    "SS (B)": {
        "file": "paper_trades_ss.json",
        "time_bl": [("08:00","08:15"),("14:15","14:30"),("16:00","16:15"),
                    ("16:30","16:45"),("17:45","18:00"),("20:15","20:45"),
                    ("21:00","21:15")],
        "pair_bl": {"AUDCAD","AUDCHF","CADCHF","EURCAD","EURCHF","EURUSD","GBPAUD"},
        "has_adr": False,
    },
    "ATR (C)": {
        "file": "paper_trades_atr.json",
        "time_bl": [("16:00","16:15")],
        "pair_bl": {"AUDCAD","AUDCHF","EURCAD","EURNZD","EURUSD","GBPAUD"},
        "has_adr": False,
    },
    "A-tuned (D)": {
        "file": "paper_trades_a_tuned.json",
        "time_bl": [],
        "pair_bl": set(),
        "has_adr": True,
    },
    "B-tuned (E)": {
        "file": "paper_trades_b_tuned.json",
        "time_bl": [("08:15","08:30"),("14:15","14:30"),("15:00","15:15"),
                    ("16:30","16:45"),("17:00","17:15"),("19:00","19:15")],
        "pair_bl": {"AUDCAD","AUDCHF","AUDJPY","EURNZD","GBPCAD","NZDUSD"},
        "has_adr": False,
    },
}


def hm_to_min(s):
    h, m = s.split(":"); return int(h)*60 + int(m)

def in_blackout(dt, bl):
    mins = dt.hour*60 + dt.minute
    return any(hm_to_min(s) <= mins < hm_to_min(e) for s, e in bl)


def load_all(filename):
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


def derive_time_blacklist(trades, bin_minutes=15, min_n=3, min_bad_pnl=-5):
    """Auto-derive 15-min bad windows from historical data.

    A bin qualifies as blacklist-worthy if:
      - >= min_n trades in that bin
      - Total net P/L <= min_bad_pnl
      - WR < 50%
    Returns list of (start_str, end_str) tuples.
    """
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
        if pnl <= min_bad_pnl and wr < 50:
            start_m = b * bin_minutes
            end_m = start_m + bin_minutes
            bad.append((f"{start_m//60:02d}:{start_m%60:02d}",
                        f"{end_m//60:02d}:{end_m%60:02d}"))
    return bad


def derive_pair_blacklist(trades, min_n=3, max_pnl=0):
    """Auto-derive per-pair blacklist (pairs with >=min_n trades and net pnl<=max)."""
    by_pair = defaultdict(list)
    for t in trades:
        by_pair[t["pair"]].append(t)
    bad = set()
    for pair, ts in by_pair.items():
        if len(ts) < min_n:
            continue
        pnl = sum(t["_net_pnl"] for t in ts)
        if pnl <= max_pnl:
            bad.add(pair)
    return bad


def apply_filters(trades, time_bl=None, pair_bl=None, adr_max=None):
    """Return filtered trades."""
    out = []
    for t in trades:
        dt = datetime.fromtimestamp(t["entry_time"], tz=JST)
        if time_bl and in_blackout(dt, time_bl):
            continue
        if pair_bl and t.get("pair") in pair_bl:
            continue
        if adr_max is not None:
            adr = t.get("adr_consumed_pct")
            if adr is not None and adr > adr_max:
                continue
        out.append(t)
    return out


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
    # PF
    gw = sum(t["_net_pnl"] for t in trades if t["_net_is_win"])
    gl = abs(sum(t["_net_pnl"] for t in trades if not t["_net_is_win"]))
    pf = gw / gl if gl > 0 else 99
    return dict(n=n, wr=w/n*100, pnl=p, mdd=mdd, bal=bal,
                pf=pf, ret_pct=(bal/1000-1)*100)


def fmt(label, s, highlight=False):
    if s is None:
        return f"    {label:<45} (no trades)"
    prefix = "  ★ " if highlight else "    "
    return (f"{prefix}{label:<43} n={s['n']:>3} WR={s['wr']:>5.1f}% "
            f"P/L={s['pnl']:>+7.1f}p PF={s['pf']:>4.2f} "
            f"DD={s['mdd']:>4.1f}% ${s['bal']:>6,.0f}")


def main():
    print("FILTER STRATEGY COMPARISON — Time vs ADR vs Pair vs Combined\n")
    print("For each system: 8 filter configurations tested on the FULL raw history.")
    print("3% compound risk, ICMarkets commission. Best final balance wins.\n")

    summary = []

    for name, cfg in CURRENT_FILTERS.items():
        raw = load_all(cfg["file"])
        if not raw:
            continue

        print(f"{'='*92}")
        print(f"  {name}  —  {len(raw)} closed trades")
        print(f"{'='*92}")

        # Derive time-bl / pair-bl if system doesn't have production ones
        time_bl = cfg["time_bl"] or derive_time_blacklist(raw)
        pair_bl = cfg["pair_bl"] or derive_pair_blacklist(raw)
        time_bl_derived = not cfg["time_bl"]
        pair_bl_derived = not cfg["pair_bl"]

        if time_bl_derived and time_bl:
            print(f"  [Auto-derived time-bl: {len(time_bl)} windows: {time_bl[:3]}...]")
        if pair_bl_derived and pair_bl:
            print(f"  [Auto-derived pair-bl: {sorted(pair_bl)}]")

        # All 8 configs
        results = {}
        results["BASELINE"] = stats(raw)
        results["TIME-bl"] = stats(apply_filters(raw, time_bl=time_bl))
        results["ADR<=70"] = stats(apply_filters(raw, adr_max=ADR_MAX))
        results["PAIR-bl"] = stats(apply_filters(raw, pair_bl=pair_bl))
        results["TIME + ADR"] = stats(apply_filters(raw, time_bl=time_bl, adr_max=ADR_MAX))
        results["TIME + PAIR"] = stats(apply_filters(raw, time_bl=time_bl, pair_bl=pair_bl))
        results["ADR + PAIR"] = stats(apply_filters(raw, pair_bl=pair_bl, adr_max=ADR_MAX))
        results["ALL THREE"] = stats(apply_filters(raw, time_bl=time_bl, pair_bl=pair_bl, adr_max=ADR_MAX))

        # Find best
        best_label = max(results, key=lambda k: results[k]["bal"] if results[k] else 0)

        print()
        print(fmt("BASELINE (no filter)", results["BASELINE"]))
        if time_bl:
            print(fmt("TIME-bl only" + (" [derived]" if time_bl_derived else ""),
                      results["TIME-bl"], best_label == "TIME-bl"))
        print(fmt("ADR<=70 only",
                  results["ADR<=70"], best_label == "ADR<=70"))
        if pair_bl:
            print(fmt("PAIR-bl only" + (" [derived]" if pair_bl_derived else ""),
                      results["PAIR-bl"], best_label == "PAIR-bl"))
        if time_bl:
            print(fmt("TIME + ADR",
                      results["TIME + ADR"], best_label == "TIME + ADR"))
            if pair_bl:
                print(fmt("TIME + PAIR (current config)",
                          results["TIME + PAIR"], best_label == "TIME + PAIR"))
        if pair_bl:
            print(fmt("ADR + PAIR",
                      results["ADR + PAIR"], best_label == "ADR + PAIR"))
        if time_bl and pair_bl:
            print(fmt("ALL THREE (time + pair + ADR)",
                      results["ALL THREE"], best_label == "ALL THREE"))

        # Rank
        print(f"\n  → Best for {name}: {best_label}")
        baseline_bal = results["BASELINE"]["bal"]
        best_bal = results[best_label]["bal"]
        improvement = (best_bal - baseline_bal) / baseline_bal * 100
        print(f"    vs baseline: {improvement:+.1f}% "
              f"(${baseline_bal:,.0f} → ${best_bal:,.0f})")
        print()

        summary.append((name, best_label, results, time_bl_derived, pair_bl_derived))

    # ── Meta summary ──
    print(f"\n{'#'*92}")
    print(f"  SUMMARY — best filter strategy per system")
    print(f"{'#'*92}\n")
    print(f"  {'System':<14} {'Best strategy':<32} {'Final $':>10} "
          f"{'vs baseline':>12} {'vs current':>12}")
    for name, best_label, results, t_der, p_der in summary:
        bal = results[best_label]["bal"]
        base = results["BASELINE"]["bal"]
        # "Current" is what the system uses LIVE today:
        if "Sv2" in name:
            current_key = "ADR<=70"
        elif "A-tuned" in name:
            current_key = "ADR<=70"
        else:
            current_key = "TIME + PAIR"
        cur = results.get(current_key, {}).get("bal", base)
        vs_base = (bal - base) / base * 100
        vs_cur = (bal - cur) / cur * 100
        change_flag = ""
        if best_label != current_key:
            change_flag = "  ⬅ CHANGE"
        print(f"  {name:<14} {best_label:<32} "
              f"${bal:>7,.0f}  {vs_base:>+10.1f}%  {vs_cur:>+10.1f}%{change_flag}")

    print(f"\n{'#'*92}")
    print(f"  OBSERVATIONS")
    print(f"{'#'*92}")
    print("""
  Key questions to answer from the numbers above:

  1. When ADR alone beats TIME alone, it suggests the 15-min time-windows
     were over-fitted to specific loss clusters that ADR catches more
     universally.

  2. When TIME + PAIR beats TIME + ADR, the per-pair blacklist is
     capturing pair-specific dynamics (e.g. JPY vs USD crosses behave
     differently at the same ADR level).

  3. When ALL THREE is best, filters are complementary — each catches
     a different slice of bad trades. Worth keeping all of them.

  4. When ALL THREE is WORSE than TIME + PAIR, you're over-filtering —
     killing profitable trades. Stay with the 2-filter combo.

  5. For Sv2 / A-tuned (no pair-bl in production): if ADR alone beats
     a derived pair-bl, ADR is your best tool since pair behaviour in
     these systems is diffuse across many pairs.
    """)


if __name__ == "__main__":
    main()
