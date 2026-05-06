"""Deep-dive on why EURAUD and CHFJPY lose money in Sv2.

We compare losers vs winners ON THE SAME PAIR across every entry-context
field we record, looking for any factor that distinguishes them. If we
find a clear pattern, that's a candidate for a pair-specific filter.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict, Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median, stdev

sys.stdout.reconfigure(encoding="utf-8")

DATA = Path(r"D:\Trading\TAKUMI Trader\data")
JST = timezone(timedelta(hours=9))
USD_QUOTE = {"EURUSD","GBPUSD","AUDUSD","NZDUSD","USDCAD","USDCHF","USDJPY"}


def commission(pair):
    if pair in USD_QUOTE: return 0.6
    if pair.endswith("JPY"): return 0.7
    return 0.8


def load_pair(pair):
    recs = json.loads((DATA / "paper_trades.json").read_text(encoding="utf-8"))
    out = []
    for r in recs:
        if not r.get("close_reason"):
            continue
        if r.get("pair") != pair:
            continue
        r = dict(r)
        r["_net_pnl"] = (r.get("pnl_pips", 0) or 0) - commission(pair)
        r["_net_is_win"] = r["_net_pnl"] > 0
        out.append(r)
    return out


def split_by_outcome(trades):
    return ([t for t in trades if t["_net_is_win"]],
            [t for t in trades if not t["_net_is_win"]])


def fmt_dist(label, wins, losses, field, fmt=".2f"):
    """Print a feature distribution for winners vs losers."""
    w_vals = [t.get(field) for t in wins if t.get(field) is not None]
    l_vals = [t.get(field) for t in losses if t.get(field) is not None]
    if not w_vals or not l_vals:
        return
    w_m = mean(w_vals); l_m = mean(l_vals)
    w_med = median(w_vals); l_med = median(l_vals)
    gap = w_m - l_m
    flag = ""
    if abs(gap) > 0:
        # significance heuristic — gap > 0.5 std
        try:
            all_vals = w_vals + l_vals
            sd = stdev(all_vals) if len(all_vals) > 1 else 0
            if sd > 0 and abs(gap) > sd * 0.3:
                flag = " ★"
        except Exception:
            pass
    print(f"  {label:<28} W={w_m:>8{fmt}} L={l_m:>8{fmt}}  "
          f"med W={w_med:>8{fmt}} L={l_med:>8{fmt}}  gap={gap:>+8{fmt}}{flag}")


def categorical_breakdown(label, trades, key_fn):
    """Show win-rate by category."""
    buckets = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0.0})
    for t in trades:
        k = key_fn(t)
        if k is None:
            continue
        b = buckets[k]
        if t["_net_is_win"]:
            b["w"] += 1
        else:
            b["l"] += 1
        b["pnl"] += t["_net_pnl"]
    print(f"  {label}:")
    print(f"    {'Category':<22} {'n':>4} {'WR':>6} {'P/L':>8} {'avg':>7}")
    for k in sorted(buckets.keys(), key=lambda x: str(x)):
        b = buckets[k]
        n = b["w"] + b["l"]
        wr = b["w"] / n * 100 if n else 0
        avg = b["pnl"] / n if n else 0
        flag = ""
        if n >= 3 and wr < 40:
            flag = " ❌"
        elif n >= 3 and wr > 75:
            flag = " ✓"
        print(f"    {str(k):<22} {n:>4} {wr:>5.1f}% {b['pnl']:>+7.1f}p {avg:>+6.2f}p{flag}")


def hour_breakdown(trades):
    """WR by hour of day (JST)."""
    by_hour = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0.0})
    for t in trades:
        dt = datetime.fromtimestamp(t["entry_time"], tz=JST)
        h = dt.hour
        b = by_hour[h]
        if t["_net_is_win"]:
            b["w"] += 1
        else:
            b["l"] += 1
        b["pnl"] += t["_net_pnl"]
    print(f"  Hour of day (JST):")
    print(f"    {'Hour':<6} {'n':>4} {'WR':>6} {'P/L':>8} {'pattern':<30}")
    for h in sorted(by_hour.keys()):
        b = by_hour[h]
        n = b["w"] + b["l"]
        wr = b["w"] / n * 100 if n else 0
        bar = "█" * int(wr/10) + "░" * (10 - int(wr/10))
        flag = ""
        if n >= 2 and wr < 40:
            flag = " ❌ bad"
        elif n >= 2 and wr > 75:
            flag = " ✓ good"
        print(f"    {h:>2}:00  {n:>4} {wr:>5.1f}% {b['pnl']:>+7.1f}p  {bar}{flag}")


def deep_dive(pair):
    trades = load_pair(pair)
    if not trades:
        print(f"\n[{pair}] No closed trades found.")
        return

    wins, losses = split_by_outcome(trades)
    n = len(trades)
    print(f"\n{'='*78}")
    print(f"  {pair}  —  {n} trades  ({len(wins)}W / {len(losses)}L = "
          f"{len(wins)/n*100:.1f}% WR)")
    total_pnl = sum(t["_net_pnl"] for t in trades)
    print(f"  Net P/L: {total_pnl:+.1f}p (avg {total_pnl/n:+.2f}p)")
    print(f"{'='*78}")

    # Direction bias
    print()
    categorical_breakdown(
        "Direction bias",
        trades, lambda t: t.get("direction"),
    )

    # Hour-of-day
    print()
    hour_breakdown(trades)

    # Day of week
    print()
    categorical_breakdown(
        "Day-of-week",
        trades,
        lambda t: ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][
            datetime.fromtimestamp(t["entry_time"], tz=JST).weekday()],
    )

    # Session (entry_session_minutes_in)
    print()
    categorical_breakdown(
        "Session",
        trades, lambda t: t.get("session"),
    )

    # Conviction bucket
    print()
    categorical_breakdown(
        "Conviction (entry_conviction)",
        trades,
        lambda t: f"{(t.get('entry_conviction') or 0)//5*5}-{(t.get('entry_conviction') or 0)//5*5+5}",
    )

    # ADR consumption bucket
    print()
    def adr_b(t):
        a = t.get("adr_consumed_pct")
        if a is None: return None
        if a <= 30: return "0-30%"
        if a <= 50: return "30-50%"
        if a <= 70: return "50-70%"
        if a <= 90: return "70-90%"
        return ">90%"
    categorical_breakdown("ADR consumption", trades, adr_b)

    # Strong currency at entry
    print()
    categorical_breakdown(
        "Strong currency",
        trades, lambda t: t.get("entry_strong_ccy"),
    )
    print()
    categorical_breakdown(
        "Weak currency",
        trades, lambda t: t.get("entry_weak_ccy"),
    )

    # Numerical feature comparison
    print()
    print(f"  Numerical feature distributions (★ = significant gap):")
    fmt_dist("Sv2 Conviction",      wins, losses, "entry_conviction",       ".0f")
    fmt_dist("H1 ATR (pips)",       wins, losses, "entry_h1_atr_pips",      ".2f")
    fmt_dist("Conv Trend",          wins, losses, "entry_conv_trend",       ".1f")
    fmt_dist("Conv Velocity",       wins, losses, "entry_conv_velocity",    ".2f")
    fmt_dist("Conv Isolation",      wins, losses, "entry_conv_isolation",   ".2f")
    fmt_dist("Conv Structural",     wins, losses, "entry_conv_structural",  ".2f")
    fmt_dist("Tick volume ratio",   wins, losses, "entry_tick_volume_ratio",".2f")
    fmt_dist("Strong top gap",      wins, losses, "entry_strong_top_gap",   ".2f")
    fmt_dist("Weak bottom gap",     wins, losses, "entry_weak_bottom_gap",  ".2f")
    fmt_dist("Strong velocity",     wins, losses, "entry_strong_velocity",  ".2f")
    fmt_dist("Weak velocity",       wins, losses, "entry_weak_velocity",    ".2f")
    fmt_dist("ADR consumed %",      wins, losses, "adr_consumed_pct",       ".1f")
    fmt_dist("Spread at entry",     wins, losses, "entry_spread_price",     ".5f")
    fmt_dist("M1 body %",           wins, losses, "entry_m1_body_pct",      ".1f")
    fmt_dist("Cluster count",       wins, losses, "entry_cluster_count",    ".0f")
    fmt_dist("Concurrent trades",   wins, losses, "entry_concurrent_trades", ".0f")
    fmt_dist("Min since news",      wins, losses, "entry_minutes_since_news",".0f")

    # Per-TF strength scores at entry
    print()
    print(f"  Per-TF currency strength at entry:")
    for tf in ["m1","m5","m15","h1","h4","d1","w1","mn"]:
        fmt_dist(f"{tf} base",  wins, losses, f"entry_{tf}_base",  ".2f")
        fmt_dist(f"{tf} quote", wins, losses, f"entry_{tf}_quote", ".2f")

    # MAE / MFE / went-profit-first patterns
    print()
    print(f"  Trade-progression patterns:")
    fmt_dist("Peak P/L (during)",   wins, losses, "peak_pnl_pips",   ".2f")
    fmt_dist("Worst P/L (during)",  wins, losses, "worst_pnl_pips",  ".2f")
    went_w = sum(1 for t in wins if t.get("went_profit_first"))
    went_l = sum(1 for t in losses if t.get("went_profit_first"))
    print(f"  went_profit_first         W={went_w}/{len(wins)} ({went_w/len(wins)*100:.0f}%)  "
          f"L={went_l}/{len(losses)} ({went_l/len(losses)*100:.0f}%)")

    # SL/TP ratio
    sl_pips = [t.get("sl_pips") for t in trades if t.get("sl_pips")]
    tp_pips = [t.get("tp_pips") for t in trades if t.get("tp_pips")]
    if sl_pips and tp_pips:
        avg_sl = mean(sl_pips); avg_tp = mean(tp_pips)
        print(f"\n  SL/TP setup: avg SL = {avg_sl:.1f}p, avg TP = {avg_tp:.1f}p, "
              f"R:R = 1:{avg_tp/avg_sl:.2f}")

    # Loss quality breakdown
    print(f"\n  Loss quality (how losses played out):")
    deep_losers = [t for t in losses if t.get("worst_pnl_pips", 0) < -10]
    quick_losers = [t for t in losses if -10 <= t.get("worst_pnl_pips", 0) < 0]
    runners = [t for t in losses if (t.get("peak_pnl_pips") or 0) > 5]
    print(f"    Deep MAE (>10p adverse): {len(deep_losers)}/{len(losses)} losses")
    print(f"    Reverted from profit:    {len(runners)}/{len(losses)} losses had peak >+5p before turning")
    print(f"    Pure losers:             {len(losses) - len(runners)}/{len(losses)} never went into profit")


def main():
    for pair in ["EURAUD", "CHFJPY"]:
        deep_dive(pair)

    print(f"\n\n{'#'*78}")
    print(f"  CROSS-PAIR INSIGHT — what's different about EURAUD/CHFJPY")
    print(f"{'#'*78}")
    print()

    # Compare to a winning pair
    print("Compare to GBPJPY (winning pair) to see what's different:")
    deep_dive("GBPJPY")


if __name__ == "__main__":
    main()
