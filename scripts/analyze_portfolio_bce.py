"""Portfolio simulation: SS (B) + ATR (C) + B-tuned (E) combined.

Applies each system's optimized filters, then merges and deduplicates:
  - If two trades on the SAME pair fire within <2 minutes of each other,
    keep only the earlier one.
  - If the gap is >= 2 minutes, keep both (different signal, different
    market state).

Then runs chronological 3%-of-equity compounding on the combined stream.
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
DEDUP_SECONDS = 120  # <2 min = duplicate; >=2 min = separate signals

# Per-system optimized filters (from optimization_results.md)
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


def hm_to_min(hm: str) -> int:
    h, m = hm.split(":")
    return int(h) * 60 + int(m)


def in_blackout(dt: datetime, bl: list[tuple[str, str]]) -> bool:
    mins = dt.hour * 60 + dt.minute
    for s, e in bl:
        if hm_to_min(s) <= mins < hm_to_min(e):
            return True
    return False


def load_and_filter(system_name: str):
    """Load closed trades for a system and apply its optimized filters."""
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
        r["_system"] = system_name
        kept.append(r)
    return closed, kept


def dedup_within_pair(trades, window_sec: int = DEDUP_SECONDS):
    """Keep first trade per pair when another signal fires within window_sec.

    Returns (kept_list, removed_list).
    """
    by_pair = defaultdict(list)
    for t in trades:
        by_pair[t["pair"]].append(t)
    kept = []
    removed = []
    for pair, ts in by_pair.items():
        ts_sorted = sorted(ts, key=lambda x: x["entry_time"])
        last_kept_time = None
        for t in ts_sorted:
            et = t["entry_time"]
            if last_kept_time is None or (et - last_kept_time) >= window_sec:
                kept.append(t)
                last_kept_time = et
            else:
                removed.append(t)
    return kept, removed


def simulate_compound(trades, risk_pct=3.0, start_cap=1000.0):
    if not trades:
        return None
    sorted_t = sorted(trades, key=lambda r: r.get("entry_time", 0))
    balance = start_cap
    peak = start_cap
    max_dd = 0.0
    eq_curve = []
    for r in sorted_t:
        sl = r.get("sl_pips", 0) or 10
        pp = r.get("pnl_pips", 0) or 0
        r_mult = pp / sl if sl > 0 else 0
        balance += balance * (risk_pct / 100) * r_mult
        eq_curve.append(balance)
        if balance > peak:
            peak = balance
        dd = (peak - balance) / peak * 100
        if dd > max_dd:
            max_dd = dd
    n = len(sorted_t)
    wins = [t for t in sorted_t if t.get("is_win")]
    pnl = sum(t.get("pnl_pips", 0) or 0 for t in sorted_t)
    gw = sum(t.get("pnl_pips", 0) or 0 for t in wins)
    gl = abs(pnl - gw)
    dates = set(datetime.fromtimestamp(t["entry_time"], tz=JST).date() for t in sorted_t)
    avg_sl = mean(t.get("sl_pips", 0) or 0 for t in sorted_t)
    days = max(1, len(dates))
    tpd = n / days
    avg = pnl / n
    avg_r = avg / avg_sl if avg_sl > 0 else 0
    daily = avg_r * tpd * (risk_pct / 100) * 100
    return dict(
        n=n, wr=len(wins) / n * 100,
        pnl=pnl, avg=avg,
        tp=sum(1 for t in sorted_t if t.get("close_reason") == "tp_hit"),
        sl=sum(1 for t in sorted_t if t.get("close_reason") == "sl_hit"),
        avg_win=gw / len(wins) if wins else 0,
        avg_loss=-gl / (n - len(wins)) if n > len(wins) else 0,
        pf=gw / gl if gl > 0 else float("inf"),
        final=balance, return_pct=(balance / start_cap - 1) * 100,
        max_dd=max_dd,
        recovery=((balance / start_cap - 1) * 100) / max_dd if max_dd > 0 else 0,
        days=days, tpd=tpd, pips_per_day=pnl / days,
        daily_ret=daily,
    )


def print_stats(label, s):
    print(f"\n{'='*72}")
    print(f"  {label}")
    print(f"{'='*72}")
    if s is None or s["n"] == 0:
        print("  (no trades)")
        return
    print(f"  Trades: {s['n']}   WR: {s['wr']:.1f}%   ({s['tp']}TP/{s['sl']}SL)")
    print(f"  Total P/L: {s['pnl']:+.1f}p   Avg: {s['avg']:+.2f}p")
    print(f"  Avg Win / Loss: {s['avg_win']:+.1f}p / {s['avg_loss']:+.1f}p")
    print(f"  Profit Factor: {s['pf']:.2f}   Max DD: {s['max_dd']:.1f}%")
    print(f"  Days: {s['days']}   Trades/Day: {s['tpd']:.1f}   Pips/Day: {s['pips_per_day']:+.1f}p")
    print(f"  Daily Compound: {s['daily_ret']:+.2f}%")
    print(f"  Final: ${s['final']:,.0f}  ({s['return_pct']:+.1f}%)")
    print(f"  Recovery Factor: {s['recovery']:.2f}")


def proj(daily_pct, days):
    return ((1 + daily_pct / 100) ** days - 1) * 100


def main():
    print(f"PORTFOLIO SIMULATION — SS (B) + ATR (C) + B-tuned (E)")
    print(f"Dedup window: <{DEDUP_SECONDS}s on same pair = keep first only\n")

    all_filtered = []
    for name in ("SS", "ATR", "B-tuned"):
        closed, kept = load_and_filter(name)
        removed_filt = len(closed) - len(kept)
        print(f"  {name}: {len(closed)} closed → {len(kept)} after filters (-{removed_filt})")
        all_filtered.extend(kept)

    # Individual system stats (post-filter, no dedup yet)
    for name in ("SS", "ATR", "B-tuned"):
        subset = [t for t in all_filtered if t["_system"] == name]
        print_stats(f"{name} alone (filtered)", simulate_compound(subset))

    # Dedup combined
    kept, removed = dedup_within_pair(all_filtered)
    print(f"\n{'='*72}")
    print(f"  DEDUP: {len(all_filtered)} merged trades → {len(kept)} kept, {len(removed)} removed")
    print(f"{'='*72}")
    from collections import Counter
    sys_count = Counter(t["_system"] for t in kept)
    rem_count = Counter(t["_system"] for t in removed)
    print(f"  Kept per system:   {dict(sys_count)}")
    print(f"  Dropped per system: {dict(rem_count)}")

    # Show some examples of removed trades
    print(f"\n  Sample of dropped duplicates (first 10):")
    print(f"    {'Time':<20} {'Pair':<7} {'Sys':<10} {'Gap(s)':>7}")
    by_pair_all = defaultdict(list)
    for t in all_filtered:
        by_pair_all[t["pair"]].append(t)
    printed = 0
    for pair, ts in by_pair_all.items():
        if printed >= 10:
            break
        ts_sorted = sorted(ts, key=lambda x: x["entry_time"])
        for i in range(1, len(ts_sorted)):
            gap = ts_sorted[i]["entry_time"] - ts_sorted[i-1]["entry_time"]
            if gap < DEDUP_SECONDS:
                dt = datetime.fromtimestamp(ts_sorted[i]["entry_time"], tz=JST)
                print(f"    {dt.strftime('%Y-%m-%d %H:%M'):<20} {pair:<7} "
                      f"{ts_sorted[i]['_system']:<10} {gap:>7}")
                printed += 1
                if printed >= 10:
                    break

    # Combined result
    combined_stats = simulate_compound(kept)
    print_stats("COMBINED PORTFOLIO (dedup applied)", combined_stats)

    # For comparison: naive sum (no dedup)
    naive = simulate_compound(all_filtered)
    print_stats("(for reference) naive sum, no dedup", naive)

    # Projections
    if combined_stats:
        print(f"\n{'='*72}")
        print(f"  FORWARD PROJECTIONS — portfolio compounded")
        print(f"{'='*72}")
        dr = combined_stats["daily_ret"]
        print(f"  Daily compound: {dr:+.2f}%")
        print(f"  1 week   (5 days):  {proj(dr,5):+.1f}%")
        print(f"  2 weeks (10 days):  {proj(dr,10):+.1f}%")
        print(f"  1 month (22 days):  {proj(dr,22):+.1f}%")
        print(f"  2 months(44 days):  {proj(dr,44):+.1f}%")

    # Per-day breakdown
    print(f"\n{'='*72}")
    print(f"  PER-DAY BREAKDOWN (portfolio, post-dedup)")
    print(f"{'='*72}")
    by_day = defaultdict(list)
    for t in kept:
        d = datetime.fromtimestamp(t["entry_time"], tz=JST).date()
        by_day[d].append(t)
    print(f"    {'Date':<12} {'n':>4} {'WR':>6} {'P/L':>9}")
    for d in sorted(by_day):
        ts = by_day[d]
        n = len(ts)
        wins = sum(1 for t in ts if t.get("is_win"))
        pnl = sum(t.get("pnl_pips", 0) or 0 for t in ts)
        print(f"    {str(d):<12} {n:>4} {wins/n*100:>5.1f}% {pnl:>+8.1f}p")

    # Pair leaderboard
    print(f"\n{'='*72}")
    print(f"  PAIR LEADERBOARD (portfolio, post-dedup, n≥5)")
    print(f"{'='*72}")
    pair_stats = defaultdict(list)
    for t in kept:
        pair_stats[t["pair"]].append(t)
    rows = []
    for p, ts in pair_stats.items():
        n = len(ts)
        if n < 5:
            continue
        wins = sum(1 for t in ts if t.get("is_win"))
        pnl = sum(t.get("pnl_pips", 0) or 0 for t in ts)
        rows.append((p, n, wins / n * 100, pnl))
    rows.sort(key=lambda x: -x[3])
    print(f"    {'Pair':<8} {'n':>4} {'WR':>6} {'P/L':>9}")
    for p, n, wr, pnl in rows:
        flag = " ✓✓" if pnl > 50 else " ✓" if pnl > 10 else " ❌" if pnl < -5 else ""
        print(f"    {p:<8} {n:>4} {wr:>5.1f}% {pnl:>+8.1f}p{flag}")


if __name__ == "__main__":
    main()
