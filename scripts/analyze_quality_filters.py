"""Find optimal quality-filter thresholds for DTC entries.

For each entry-context feature, this script:
  1. Shows the distribution for winners vs losers
  2. Sweeps thresholds and finds the one that maximises WR-gain net of
     "winners lost" — i.e. doesn't blindly reject too many good trades
  3. Ranks features by their standalone predictive power
  4. Tests combined filters (AND logic) to quantify realistic gains

Output: recommended DTC config additions + projected improvement.
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
    if pair in USD_QUOTE:
        return 0.6
    if pair.endswith("JPY"):
        return 0.7
    return 0.8


# Same DTC filters as production
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
    """DTC-equivalent dataset: SS+ATR+B-tuned with current prod filters
    + 120s dedup. Only trades with full entry-context fields populated."""
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
            r["_system"] = name
            r["_net_pnl"] = (r.get("pnl_pips", 0) or 0) - commission(r.get("pair", ""))
            r["_net_is_win"] = r["_net_pnl"] > 0
            all_trades.append(r)
    # 120s same-pair dedup
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


def feature_available(trades, field):
    """Only keep trades with the feature populated (non-null, non-zero for
    numeric features except adr/buildup which can legitimately be 0)."""
    nonzero_required = {
        "entry_h1_atr_pips": True,
        "entry_tick_volume_ratio": True,
        "entry_momentum_buildup_sec": False,  # can be 0 legitimately
        "entry_conv_trend": True,
        "entry_conv_isolation": True,
        "entry_strong_top_gap": True,
        "adr_consumed_pct": True,
    }
    nz = nonzero_required.get(field, True)
    return [t for t in trades if t.get(field) is not None
            and (not nz or t[field] != 0)]


def distribution(trades, field):
    """Return (winner_values, loser_values) lists for a feature."""
    wins = [t[field] for t in trades if t["_net_is_win"]]
    losses = [t[field] for t in trades if not t["_net_is_win"]]
    return wins, losses


def threshold_sweep(trades, field, rule, candidate_thresholds):
    """For each candidate threshold, compute what happens if we filter.

    rule: "ge" (keep trades where feature >= threshold) or
          "le" (keep trades where feature <= threshold)

    Returns list of dicts with metrics per threshold.
    """
    results = []
    for thresh in candidate_thresholds:
        if rule == "ge":
            kept = [t for t in trades if t[field] >= thresh]
            dropped = [t for t in trades if t[field] < thresh]
        else:  # "le"
            kept = [t for t in trades if t[field] <= thresh]
            dropped = [t for t in trades if t[field] > thresh]
        if not kept:
            continue
        n = len(kept)
        wins = sum(1 for t in kept if t["_net_is_win"])
        pnl = sum(t["_net_pnl"] for t in kept)
        wr = wins / n * 100
        # Compare to what we dropped — did we drop net losers?
        d_n = len(dropped)
        d_wins = sum(1 for t in dropped if t["_net_is_win"]) if d_n > 0 else 0
        d_pnl = sum(t["_net_pnl"] for t in dropped) if d_n > 0 else 0
        d_wr = d_wins / d_n * 100 if d_n > 0 else 0
        results.append(dict(
            thresh=thresh, n=n, wr=wr, pnl=pnl,
            dropped_n=d_n, dropped_wr=d_wr, dropped_pnl=d_pnl,
        ))
    return results


def main():
    trades = load_filtered()
    print(f"Loaded {len(trades)} DTC-equivalent closed trades\n")

    # Summary: how many have ALL features
    full_ctx = [t for t in trades
                if all(t.get(f) is not None for f in
                       ["entry_h1_atr_pips","entry_tick_volume_ratio",
                        "entry_momentum_buildup_sec","entry_conv_trend",
                        "entry_conv_isolation","entry_strong_top_gap",
                        "adr_consumed_pct"])]
    print(f"Trades with FULL entry context: {len(full_ctx)}\n")

    if full_ctx:
        n = len(full_ctx)
        w = sum(1 for t in full_ctx if t["_net_is_win"])
        p = sum(t["_net_pnl"] for t in full_ctx)
        print(f"Baseline (full-context subset): n={n} WR={w/n*100:.1f}% "
              f"P/L={p:+.1f}p\n")

    # Features to analyze — with "ge" or "le" direction based on winners-vs-losers
    features = [
        ("entry_h1_atr_pips", "H1 ATR (pips)", "ge",
         [2, 3, 4, 5, 6, 7, 8, 10, 12, 15]),
        ("entry_tick_volume_ratio", "Tick Volume Ratio", "ge",
         [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.0]),
        ("entry_momentum_buildup_sec", "Momentum Buildup (sec)", "ge",
         [0, 10, 15, 20, 30, 45, 60, 90]),
        ("entry_conv_trend", "Conv Trend Score", "ge",
         [10, 12, 14, 16, 18, 20, 22, 25]),
        ("entry_conv_isolation", "Conv Isolation", "ge",
         [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]),
        ("entry_strong_top_gap", "Strong Top Gap", "ge",
         [0.5, 0.8, 1.0, 1.2, 1.4, 1.6, 2.0]),
        ("adr_consumed_pct", "ADR Consumption %", "le",
         [30, 40, 50, 60, 70, 80, 90]),
    ]

    feature_results = {}
    for field, label, rule, candidates in features:
        subset = feature_available(trades, field)
        if len(subset) < 30:
            print(f"\n{label}: only {len(subset)} trades with this feature — skipping")
            continue
        wins, losses = distribution(subset, field)
        print(f"\n{'='*72}")
        print(f"  {label}  ({'KEEP if >=' if rule=='ge' else 'KEEP if <='} X)")
        print(f"{'='*72}")
        print(f"  Winners: n={len(wins):>4}  mean={mean(wins):>6.2f}  "
              f"median={median(wins):>6.2f}")
        print(f"  Losers:  n={len(losses):>4}  mean={mean(losses):>6.2f}  "
              f"median={median(losses):>6.2f}")

        print(f"\n  {'Threshold':>10}  {'kept':>5} {'WR':>6} {'P/L':>8}  "
              f"{'drop n':>7} {'drop WR':>8} {'drop P/L':>9}")
        results = threshold_sweep(subset, field, rule, candidates)
        # Find baseline (no filter) stats from subset
        base_n = len(subset)
        base_w = sum(1 for t in subset if t["_net_is_win"])
        base_p = sum(t["_net_pnl"] for t in subset)
        print(f"  {'(no filter)':>10}  {base_n:>5} {base_w/base_n*100:>5.1f}% "
              f"{base_p:>+7.1f}p")
        for r in results:
            flag = ""
            if r["wr"] > base_w/base_n*100 + 3 and r["n"] > base_n * 0.5:
                flag = " ✓"
            if r["n"] < base_n * 0.3:
                flag = " (too narrow)"
            print(f"  {r['thresh']:>10}  {r['n']:>5} {r['wr']:>5.1f}% "
                  f"{r['pnl']:>+7.1f}p  "
                  f"{r['dropped_n']:>6} {r['dropped_wr']:>7.1f}% "
                  f"{r['dropped_pnl']:>+8.1f}p{flag}")
        feature_results[field] = (subset, results, rule)

    # ── Combined filter test ──
    print(f"\n\n{'#'*72}")
    print(f"  COMBINED FILTER TEST — AND logic across features")
    print(f"{'#'*72}\n")
    print("  Using each feature's 'sweet spot' threshold (best WR gain with")
    print("  minimum trade loss), apply all simultaneously.\n")

    # Hand-picked sweet spots based on the sweep (we'll adjust after seeing output)
    sweet_spots = {
        "entry_h1_atr_pips":         ("ge", 6),
        "entry_tick_volume_ratio":   ("ge", 0.5),
        "entry_momentum_buildup_sec":("ge", 20),
        "entry_conv_trend":          ("ge", 16),
        "entry_conv_isolation":      ("ge", 3.5),
        "entry_strong_top_gap":      ("ge", 1.0),
        "adr_consumed_pct":          ("le", 70),
    }

    # Apply all filters; if feature is missing on a trade, skip that check
    # (soft fail — don't drop trades that simply lack the measurement)
    def passes_all(t):
        for field, (rule, thresh) in sweet_spots.items():
            val = t.get(field)
            if val is None:
                continue  # can't evaluate, don't drop
            if rule == "ge" and val < thresh:
                return False
            if rule == "le" and val > thresh:
                return False
        return True

    def passes_strict(t):
        """Strict version: drop trade if ANY feature is missing."""
        for field, (rule, thresh) in sweet_spots.items():
            val = t.get(field)
            if val is None:
                return False
            if rule == "ge" and val < thresh:
                return False
            if rule == "le" and val > thresh:
                return False
        return True

    for mode, fn in [("SOFT (skip missing features)", passes_all),
                     ("STRICT (require all features)", passes_strict)]:
        kept = [t for t in trades if fn(t)]
        dropped = [t for t in trades if not fn(t)]
        if not kept:
            continue
        kn = len(kept); kw = sum(1 for t in kept if t["_net_is_win"])
        kp = sum(t["_net_pnl"] for t in kept)
        dn = len(dropped); dw = sum(1 for t in dropped if t["_net_is_win"])
        dp = sum(t["_net_pnl"] for t in dropped)

        print(f"  {mode}:")
        print(f"    Kept:    n={kn:>3} WR={kw/kn*100:>5.1f}% P/L={kp:>+7.1f}p  "
              f"(avg {kp/kn:+.2f}p)")
        if dn > 0:
            print(f"    Dropped: n={dn:>3} WR={dw/dn*100:>5.1f}% P/L={dp:>+7.1f}p  "
                  f"(avg {dp/dn:+.2f}p)")
        print()

    # Baseline for comparison
    n = len(trades); w = sum(1 for t in trades if t["_net_is_win"])
    p = sum(t["_net_pnl"] for t in trades)
    print(f"  BASELINE (no quality filter): n={n} WR={w/n*100:.1f}% P/L={p:+.1f}p")

    # Which single feature has best edge?
    print(f"\n\n{'#'*72}")
    print(f"  FEATURE RANKING — single-filter edge over baseline")
    print(f"{'#'*72}\n")
    print(f"  {'Feature':<28} {'best thresh':>12} {'keeps':>6} {'WR':>6} "
          f"{'WR gain':>8} {'P/L':>8}")
    for field, (subset, results, rule) in feature_results.items():
        bn = len(subset); bw = sum(1 for t in subset if t["_net_is_win"])
        base_wr = bw / bn * 100
        # Find threshold with highest WR while keeping >= 50% of trades
        best = None
        for r in results:
            if r["n"] < bn * 0.5:
                continue
            if best is None or r["wr"] > best["wr"]:
                best = r
        if best:
            print(f"  {field:<28} "
                  f"{'≥' if rule=='ge' else '≤'}{best['thresh']:>10.1f} "
                  f"{best['n']:>5} "
                  f"{best['wr']:>5.1f}% "
                  f"{best['wr']-base_wr:>+6.1f}pp "
                  f"{best['pnl']:>+7.1f}p")


if __name__ == "__main__":
    main()
