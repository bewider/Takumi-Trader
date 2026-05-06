"""Sv2 analysis v2 — 15-min windows over full 536-trade history.

Workflow:
  1. Bucket all 536 trades into 15-minute windows (96 bins/day)
  2. Identify bad windows (n ≥ 5 AND P/L < -5p)
  3. Apply those time blocks as filter #1
  4. Re-evaluate each pair's performance WITH the time filter
  5. Some losing pairs may flip to winners when bad hours are removed
  6. Final pair blacklist = only pairs that still lose AFTER time filter
  7. Report baseline vs optimized stats
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean

DATA = Path(r"D:\Trading\TAKUMI Trader\data")
JST = timezone(timedelta(hours=9))

BIN_MIN = 15       # 15-min window size
MIN_N_WINDOW = 5   # min trades in a window to consider
MIN_PNL_BLOCK = -5.0  # block if window P/L < this
MIN_N_PAIR = 5     # min trades per pair to consider for blacklist


def load_closed() -> list[dict]:
    recs = json.loads((DATA / "paper_trades.json").read_text(encoding="utf-8"))
    return [r for r in recs if r.get("close_reason")]


def minute_of_day(r: dict) -> int:
    dt = datetime.fromtimestamp(r.get("entry_time", 0), tz=JST)
    return dt.hour * 60 + dt.minute


def bin_of(r: dict) -> int:
    """15-min bin index (0..95)."""
    return minute_of_day(r) // BIN_MIN


def fmt_bin(bin_idx: int, is_end: bool = False) -> str:
    minutes = bin_idx * BIN_MIN + (BIN_MIN if is_end else 0)
    h, m = divmod(minutes, 60)
    return f"{h:02d}:{m:02d}"


def stats(trades: list[dict]) -> dict:
    if not trades:
        return {"n": 0, "wr": 0, "pnl": 0, "avg": 0, "tp": 0, "sl": 0,
                "avg_win": 0, "avg_loss": 0, "days": 0, "profit_factor": 0}
    n = len(trades)
    wins = [t for t in trades if t.get("is_win")]
    losses = [t for t in trades if not t.get("is_win")]
    pnl = sum(t.get("pnl_pips", 0) or 0 for t in trades)
    gross_w = sum(t.get("pnl_pips", 0) or 0 for t in wins)
    gross_l = abs(sum(t.get("pnl_pips", 0) or 0 for t in losses))
    dates = set()
    for t in trades:
        et = t.get("entry_time", 0)
        if et > 0:
            dates.add(datetime.fromtimestamp(et, tz=JST).date())
    return {
        "n": n,
        "wr": len(wins) / n * 100,
        "pnl": pnl,
        "avg": pnl / n,
        "tp": sum(1 for t in trades if t.get("close_reason") == "tp_hit"),
        "sl": sum(1 for t in trades if t.get("close_reason") == "sl_hit"),
        "avg_win": gross_w / len(wins) if wins else 0,
        "avg_loss": -gross_l / len(losses) if losses else 0,
        "days": max(1, len(dates)),
        "profit_factor": gross_w / gross_l if gross_l > 0 else float("inf"),
        "avg_sl": mean(t.get("sl_pips", 0) or 0 for t in trades),
        "avg_dur": mean(t.get("duration_minutes", 0) or 0 for t in trades),
    }


def simulate_compound(trades: list[dict], risk_pct: float = 3.0, start_cap: float = 1000.0) -> dict:
    s = stats(trades)
    sorted_t = sorted(trades, key=lambda r: r.get("entry_time", 0))
    balance = start_cap
    peak = start_cap
    max_dd = 0.0
    max_dd_abs = 0.0
    for r in sorted_t:
        sl = r.get("sl_pips", 0) or 10
        pnl = r.get("pnl_pips", 0) or 0
        r_mult = pnl / sl if sl > 0 else 0
        balance += balance * (risk_pct / 100.0) * r_mult
        if balance > peak:
            peak = balance
        dd = (peak - balance) / peak * 100
        if dd > max_dd:
            max_dd = dd
            max_dd_abs = peak - balance

    avg_r = s["avg"] / s["avg_sl"] if s["avg_sl"] > 0 else 0
    trades_per_day = s["n"] / s["days"] if s["days"] else 0
    daily_ret = avg_r * trades_per_day * (risk_pct / 100.0) * 100

    s.update({
        "final": balance,
        "return_pct": (balance / start_cap - 1) * 100,
        "max_dd": max_dd,
        "max_dd_abs": max_dd_abs,
        "recovery": ((balance / start_cap - 1) * 100) / max_dd if max_dd > 0 else 0,
        "daily_ret": daily_ret,
        "pips_per_day": s["pnl"] / s["days"],
        "trades_per_day": trades_per_day,
        "avg_r": avg_r,
    })
    return s


def find_bad_windows(trades: list[dict]) -> tuple[set[int], list[tuple]]:
    """Find 15-min bins with bad stats. Returns (blocked_bins, details)."""
    buckets = defaultdict(list)
    for r in trades:
        buckets[bin_of(r)].append(r)
    bad = []
    for b, recs in buckets.items():
        if len(recs) < MIN_N_WINDOW:
            continue
        s = stats(recs)
        if s["pnl"] < MIN_PNL_BLOCK:
            bad.append((b, s["n"], s["wr"], s["pnl"]))
    bad.sort(key=lambda x: x[3])  # worst first
    blocked = {b for b, _, _, _ in bad}
    return blocked, bad


def merge_windows(blocked: set[int]) -> list[tuple[int, int]]:
    """Merge adjacent blocked bins into contiguous ranges."""
    if not blocked:
        return []
    sorted_b = sorted(blocked)
    merged = []
    start = sorted_b[0]
    end = sorted_b[0]
    for b in sorted_b[1:]:
        if b == end + 1:
            end = b
        else:
            merged.append((start, end))
            start = b
            end = b
    merged.append((start, end))
    return merged


def print_stats_block(label: str, s: dict) -> None:
    print(f"\n{'='*76}")
    print(f"  {label}")
    print(f"{'='*76}")
    print(f"  Trades: {s['n']:>4}  WR: {s['wr']:>5.1f}%  ({s['n']-s.get('sl',0)-s.get('sig',0)}TP/{s.get('sl',0)}SL)")
    print(f"  Total P/L: {s['pnl']:>+7.1f}p   Avg: {s['avg']:>+.2f}p")
    print(f"  Avg Win: {s.get('avg_win', 0):>+.1f}p   Avg Loss: {s.get('avg_loss', 0):>+.1f}p")
    print(f"  Pips/Day: {s.get('pips_per_day', 0):>+.1f}   Trades/Day: {s.get('trades_per_day', 0):>.1f}")
    print(f"  Daily Return: {s.get('daily_ret', 0):>+.2f}%   Monthly: {s.get('daily_ret', 0) * 22:>+.1f}%")
    print(f"  Profit Factor: {s.get('profit_factor', 0):>.2f}   Max DD: {s.get('max_dd', 0):>.1f}%   Recovery: {s.get('recovery', 0):>.1f}")
    print(f"  Final balance (3% risk, $1k start): ${s.get('final', 0):>,.2f}  ({s.get('return_pct', 0):>+.1f}%)")


def main():
    trades = load_closed()
    print(f"Sv2 v2 analysis — {len(trades)} closed trades, 15-min windows, full history\n")

    # === Step 1: Baseline ===
    baseline = simulate_compound(trades)
    print_stats_block("BASELINE — all 536 trades, no filter", baseline)

    # === Step 2: Find bad 15-min windows from ALL history ===
    blocked_bins, bad_details = find_bad_windows(trades)
    print(f"\n\n{'='*76}")
    print(f"  BAD 15-MIN WINDOWS (from full 536-trade history)")
    print(f"  Criteria: n >= {MIN_N_WINDOW} AND P/L <= {MIN_PNL_BLOCK}p")
    print(f"{'='*76}")
    if not bad_details:
        print("  None found.")
    else:
        print(f"  {'Window (JST)':<14} {'n':>4} {'WR':>6} {'P/L':>9}")
        for b, n, wr, pnl in bad_details:
            flag = " !!!" if pnl < -30 else " !!" if pnl < -15 else " !"
            print(f"  {fmt_bin(b)}-{fmt_bin(b, True)}  {n:>4} {wr:>5.1f}% {pnl:>+8.1f}p{flag}")

    merged = merge_windows(blocked_bins)
    print(f"\n  Merged blocked windows ({len(merged)} ranges):")
    for s, e in merged:
        print(f"    {fmt_bin(s)} - {fmt_bin(e, True)}")

    # === Step 3: Filter by time, then re-evaluate pairs ===
    def in_block(t):
        return bin_of(t) in blocked_bins

    time_filtered = [t for t in trades if not in_block(t)]
    print(f"\n  After time filter: {len(time_filtered)}/{len(trades)} trades remain")

    # === Step 4: Pair analysis BEFORE vs AFTER time filter ===
    print(f"\n\n{'='*76}")
    print(f"  PAIR ANALYSIS — Before (all trades) vs After (time-filtered)")
    print(f"{'='*76}")
    pair_before = defaultdict(list)
    pair_after = defaultdict(list)
    for t in trades:
        pair_before[t.get("pair", "?")].append(t)
    for t in time_filtered:
        pair_after[t.get("pair", "?")].append(t)

    print(f"  {'Pair':<8} {'BEFORE':<25} {'AFTER time filter':<25} {'Verdict':<15}")
    print(f"  {'':<8} {'n':>4} {'WR':>6} {'P/L':>9}  {'n':>4} {'WR':>6} {'P/L':>9}")

    results = []
    for pair in sorted(pair_before.keys()):
        bs = stats(pair_before[pair])
        as_ = stats(pair_after.get(pair, []))
        flipped = bs["pnl"] < 0 and as_["pnl"] >= 0
        still_losing = as_["pnl"] < 0 and as_["n"] >= MIN_N_PAIR
        verdict = ""
        if flipped:
            verdict = "✓ flipped"
        elif still_losing:
            verdict = "✗ still loses"
        elif bs["pnl"] < 0 and as_["pnl"] < 0:
            verdict = "still neg (low n)"
        elif bs["pnl"] >= 0:
            verdict = "still winner"
        results.append((pair, bs, as_, verdict, still_losing))

    # Sort by AFTER P/L ascending
    results.sort(key=lambda x: x[2]["pnl"])
    for pair, bs, as_, verdict, _ in results:
        b_str = f"{bs['n']:>4} {bs['wr']:>5.1f}% {bs['pnl']:>+8.1f}p"
        if as_["n"] == 0:
            a_str = f"{'(all removed)':<25}"
        else:
            a_str = f"{as_['n']:>4} {as_['wr']:>5.1f}% {as_['pnl']:>+8.1f}p"
        print(f"  {pair:<8} {b_str}  {a_str}  {verdict}")

    # Pairs that are STILL losing after time filter (with enough sample)
    final_blacklist = [p for p, bs, as_, v, losing in results if losing]
    print(f"\n  FINAL PAIR BLACKLIST (still loses after time filter, n>={MIN_N_PAIR}): {final_blacklist}")

    # === Step 5: Full optimized simulation ===
    fully_filtered = [t for t in time_filtered if t.get("pair") not in final_blacklist]
    optimized = simulate_compound(fully_filtered)
    print_stats_block(
        f"OPTIMIZED — time filter + {len(final_blacklist)} pair blacklist",
        optimized,
    )

    # === Step 6: Comparison ===
    print(f"\n\n{'='*76}")
    print(f"  COMPARISON — Baseline vs Optimized")
    print(f"{'='*76}")
    rows = [
        ("Trades", "n", "{:d}"),
        ("Win Rate %", "wr", "{:.1f}"),
        ("Total P/L", "pnl", "{:+.1f}p"),
        ("Pips/Day", "pips_per_day", "{:+.1f}"),
        ("Avg Win", "avg_win", "{:+.1f}p"),
        ("Avg Loss", "avg_loss", "{:+.1f}p"),
        ("Profit Factor", "profit_factor", "{:.2f}"),
        ("Daily Return %", "daily_ret", "{:.2f}"),
        ("Monthly Return %", None, None),  # computed
        ("Max DD %", "max_dd", "{:.1f}"),
        ("Max DD $ (1k start)", "max_dd_abs", "${:,.0f}"),
        ("Recovery Factor", "recovery", "{:.1f}"),
        ("Final balance", "final", "${:,.0f}"),
        ("Total Return %", "return_pct", "{:.1f}"),
    ]
    print(f"  {'Metric':<22} {'Baseline':>12} {'Optimized':>12} {'Δ':>11}")
    for label, key, fmt in rows:
        if label == "Monthly Return %":
            b = baseline["daily_ret"] * 22
            o = optimized["daily_ret"] * 22
            delta = o - b
            print(f"  {label:<22} {b:>12.1f} {o:>12.1f} {delta:>+11.1f}")
            continue
        b = baseline[key]
        o = optimized[key]
        delta = o - b
        b_s = fmt.format(b) if b is not None else "-"
        o_s = fmt.format(o) if o is not None else "-"
        d_s = f"{delta:+.1f}" if isinstance(b, float) else f"{int(delta):+d}"
        print(f"  {label:<22} {b_s:>12} {o_s:>12} {d_s:>11}")

    # === Step 7: Final config export ===
    print(f"\n\n{'='*76}")
    print(f"  PROPOSED Sv2 FILTER CONFIG")
    print(f"{'='*76}")
    print(f"  Time blacklist (JST, 15-min windows):")
    for s, e in merged:
        print(f"    [\"{fmt_bin(s)}\", \"{fmt_bin(e, True)}\"]")
    print(f"  Pair blacklist: {final_blacklist}")


if __name__ == "__main__":
    main()
