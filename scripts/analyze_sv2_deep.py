"""Deep analysis of Sv2 (System A) paper trades.

Finds:
  1. Losing pairs to blacklist
  2. Losing 5-min time windows to block (last 4 days, fine-grained)
  3. Entry-context patterns that predict win/loss (when available)
  4. Best combined filter and projected P/L

Outputs a summary matching the TAKUMI performance-dialog stats format.
"""
from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean

DATA = Path(r"D:\Trading\TAKUMI Trader\data")
JST = timezone(timedelta(hours=9))


def load_closed() -> list[dict]:
    recs = json.loads((DATA / "paper_trades.json").read_text(encoding="utf-8"))
    return [r for r in recs if r.get("close_reason")]


def simulate(trades: list[dict], risk_pct: float = 3.0, start_cap: float = 1000.0) -> dict:
    """Simulate compounding 3% risk. Returns stats matching TAKUMI dialog."""
    if not trades:
        return {"n": 0, "wr": 0, "total_pnl": 0, "final": start_cap,
                "return_pct": 0, "max_dd": 0, "max_dd_abs": 0,
                "profit_factor": 0, "recovery": 0, "days": 0,
                "avg_dur": 0, "avg_sl": 0, "tp": 0, "sl": 0,
                "avg_win": 0, "avg_loss": 0, "pips_per_day": 0,
                "trades_per_day": 0, "daily_ret": 0}

    trades_sorted = sorted(trades, key=lambda r: r.get("entry_time", 0))

    # Equity curve
    balance = start_cap
    peak = start_cap
    max_dd_pct = 0.0
    max_dd_abs = 0.0
    for r in trades_sorted:
        sl = r.get("sl_pips", 0) or 0
        pnl = r.get("pnl_pips", 0) or 0
        if sl > 0:
            r_mult = pnl / sl
        else:
            r_mult = pnl / 10.0
        balance += balance * (risk_pct / 100.0) * r_mult
        if balance > peak:
            peak = balance
        dd = (peak - balance) / peak * 100
        if dd > max_dd_pct:
            max_dd_pct = dd
            max_dd_abs = peak - balance

    n = len(trades)
    wins = [r for r in trades if r.get("is_win")]
    losses = [r for r in trades if not r.get("is_win")]
    total_pnl = sum(r.get("pnl_pips", 0) or 0 for r in trades)
    avg_dur = mean(r.get("duration_minutes", 0) or 0 for r in trades)

    # Trading days
    dates = set()
    for r in trades:
        et = r.get("entry_time", 0)
        if et > 0:
            dates.add(datetime.fromtimestamp(et, tz=JST).date())
    n_days = max(1, len(dates))

    gross_win = sum(r.get("pnl_pips", 0) or 0 for r in wins)
    gross_loss = abs(sum(r.get("pnl_pips", 0) or 0 for r in losses))
    pf = gross_win / gross_loss if gross_loss > 0 else 0.0

    avg_sl = mean(r.get("sl_pips", 0) or 0 for r in trades)
    avg_pnl = total_pnl / n if n else 0
    avg_r = avg_pnl / avg_sl if avg_sl > 0 else 0
    daily_ret = avg_r * (n / n_days) * (risk_pct / 100.0) * 100

    return_pct = (balance / start_cap - 1) * 100
    recovery = return_pct / max_dd_pct if max_dd_pct > 0 else 0

    return {
        "n": n,
        "wins": len(wins),
        "losses": len(losses),
        "wr": len(wins) / n * 100 if n else 0,
        "total_pnl": total_pnl,
        "avg_dur": avg_dur,
        "tp": sum(1 for r in trades if r.get("close_reason") == "tp_hit"),
        "sl": sum(1 for r in trades if r.get("close_reason") == "sl_hit"),
        "sig": sum(1 for r in trades if r.get("close_reason") == "signal_exit"),
        "avg_win": (gross_win / len(wins)) if wins else 0,
        "avg_loss": (-gross_loss / len(losses)) if losses else 0,
        "days": n_days,
        "avg_sl": avg_sl,
        "avg_r": avg_r,
        "daily_ret": daily_ret,
        "pips_per_day": total_pnl / n_days,
        "trades_per_day": n / n_days,
        "final": balance,
        "return_pct": return_pct,
        "max_dd": max_dd_pct,
        "max_dd_abs": max_dd_abs,
        "profit_factor": pf,
        "recovery": recovery,
    }


def print_stats(label: str, s: dict) -> None:
    print(f"\n{'='*76}")
    print(f"  {label}")
    print(f"{'='*76}")
    print(f"  Summary")
    print(f"    Total trades: {s['n']:>6}   WR: {s['wr']:>5.1f}%  ({s['wins']}W / {s['losses']}L)")
    print(f"    Total P/L:    {s['total_pnl']:>+7.1f}p    Avg duration: {s['avg_dur']:>4.0f}m")
    print(f"")
    print(f"  Exit Breakdown")
    print(f"    TP Hits: {s['tp']:>4}   SL Hits: {s['sl']:>4}   Signal Exits: {s['sig']:>3}")
    print(f"    Avg Win: {s['avg_win']:>+5.1f}p    Avg Loss: {s['avg_loss']:>+5.1f}p")
    print(f"")
    print(f"  Daily Averages & Projected Returns (3% risk/trade, compound)")
    print(f"    Pips/Day: {s['pips_per_day']:>+6.1f}   Trades/Day: {s['trades_per_day']:>5.1f}")
    print(f"    Daily Return: {s['daily_ret']:>+6.2f}%   Weekly: {s['daily_ret']*5:>+6.2f}%   Monthly: {s['daily_ret']*22:>+6.2f}%")
    print(f"    Based on {s['days']} trading days, avg SL={s['avg_sl']:.1f}p, avg R={s['avg_r']:+.2f}")
    print(f"")
    print(f"  Risk Metrics")
    print(f"    Max Drawdown: {s['max_dd']:>5.1f}%   Max DD abs: ${s['max_dd_abs']:>6,.0f}")
    print(f"    Profit Factor: {s['profit_factor']:>4.2f}   Recovery Factor: {s['recovery']:>4.1f}")
    print(f"    Final balance: ${s['final']:>9,.2f}   ({s['return_pct']:+.1f}%)")


def analyze_by_pair(trades: list[dict]) -> list[tuple]:
    """Return sorted (pair, n, wr, pnl, avg, include_recommendation) list."""
    by_p = defaultdict(list)
    for r in trades:
        by_p[r.get("pair", "?")].append(r)
    rows = []
    for pair, tlist in by_p.items():
        n = len(tlist)
        w = sum(1 for t in tlist if t.get("is_win"))
        p = sum(t.get("pnl_pips", 0) or 0 for t in tlist)
        avg = p / n if n else 0
        rows.append((pair, n, w, p, avg, w / n * 100 if n else 0))
    return sorted(rows, key=lambda x: x[3])  # worst P/L first


def analyze_5min_windows(trades: list[dict], cutoff_ts: float) -> list[tuple]:
    """Find 5-min windows where P/L is negative. Only uses trades >= cutoff_ts."""
    recent = [r for r in trades if r.get("entry_time", 0) >= cutoff_ts]
    buckets = defaultdict(list)
    for r in recent:
        dt = datetime.fromtimestamp(r["entry_time"], tz=JST)
        key = dt.hour * 60 + (dt.minute // 5) * 5
        buckets[key].append(r)

    # Sliding 3-bin (15-min) clusters
    cluster_stats = []
    for center_key in range(0, 24 * 60, 5):
        recs = []
        for offset in (-5, 0, 5):
            recs.extend(buckets.get(center_key + offset, []))
        if len(recs) < 3:
            continue
        n = len(recs)
        w = sum(1 for r in recs if r.get("is_win"))
        p = sum(r.get("pnl_pips", 0) or 0 for r in recs)
        if p < 0:
            cluster_stats.append((center_key, n, w, p))
    cluster_stats.sort(key=lambda x: x[3])

    # Dedup overlapping clusters (keep worst)
    blocked_keys = set()
    merged = []
    for center, n, w, p in cluster_stats:
        if center in blocked_keys:
            continue
        # Block a 15-min window centered here
        for k in (center - 5, center, center + 5):
            blocked_keys.add(k)
        merged.append((center, n, w, p))
    return merged


def minute_to_hm(m: int) -> str:
    h, mm = divmod(m, 60)
    return f"{h:02d}:{mm:02d}"


def build_bad_windows_list(clusters: list, min_pnl: float = -5, min_n: int = 3) -> list[tuple[int, int]]:
    """Return merged list of (start_min, end_min) for bad windows."""
    bad_bins = set()
    for center, n, w, p in clusters:
        if p > min_pnl or n < min_n:
            continue
        for k in (center - 5, center, center + 5):
            bad_bins.add(k)
    if not bad_bins:
        return []
    sorted_bins = sorted(bad_bins)
    merged = []
    cur_start = sorted_bins[0]
    cur_end = sorted_bins[0] + 5
    for b in sorted_bins[1:]:
        if b <= cur_end + 5:  # adjacent or 5-min gap
            cur_end = b + 5
        else:
            merged.append((cur_start, cur_end))
            cur_start = b
            cur_end = b + 5
    merged.append((cur_start, cur_end))
    return merged


def main():
    trades = load_closed()
    print(f"Sv2 deep analysis — {len(trades)} closed trades")

    # ── Baseline ──
    baseline = simulate(trades)
    print_stats("BASELINE (current Sv2 — all trades)", baseline)

    # ── Pair analysis (all history) ──
    print(f"\n\n{'='*76}")
    print(f"  PAIR ANALYSIS — worst performers (all history)")
    print(f"{'='*76}")
    pair_rows = analyze_by_pair(trades)
    print(f"  {'Pair':<8} {'n':>4} {'WR':>6} {'P/L':>9} {'avg':>7}")
    for pair, n, w, p, avg, wr in pair_rows:
        flag = " ← LOSS" if p < 0 and n >= 5 else ""
        print(f"  {pair:<8} {n:>4} {wr:>5.1f}% {p:>+8.1f}p {avg:>+6.2f}p{flag}")

    # Identify losing pairs (net negative, n >= 5)
    losing_pairs = {pair for pair, n, w, p, avg, wr in pair_rows
                    if p < 0 and n >= 5}
    print(f"\n  Losing pairs to blacklist: {sorted(losing_pairs)}")

    # ── 5-min window analysis (last 4 days) ──
    now = datetime.now(JST)
    cutoff_4d = (now - timedelta(days=4)).timestamp()
    clusters = analyze_5min_windows(trades, cutoff_4d)
    print(f"\n\n{'='*76}")
    print(f"  5-MINUTE WINDOW ANALYSIS — last 4 days ({cutoff_4d:.0f} cutoff)")
    print(f"{'='*76}")
    if not clusters:
        print("  No persistent loss clusters found.")
    else:
        print(f"  {'Window (JST)':<14} {'n':>4} {'WR':>6} {'P/L':>9}")
        for center, n, w, p in clusters[:25]:
            wr = w / n * 100 if n else 0
            flag = ""
            if p < -15:
                flag = " !!!"
            elif p < -8:
                flag = " !!"
            elif p < -3:
                flag = " !"
            s_str = minute_to_hm(center - 5)
            e_str = minute_to_hm(center + 10)
            print(f"  {s_str}-{e_str}  {n:>4} {wr:>5.1f}% {p:>+8.1f}p{flag}")

    bad_windows = build_bad_windows_list(clusters, min_pnl=-5, min_n=3)
    print(f"\n  Merged bad windows to block (JST):")
    for s, e in bad_windows:
        print(f"    {minute_to_hm(s)} - {minute_to_hm(e)}")

    # ── Entry-context analysis (last 4 days) ──
    recent = [r for r in trades if r.get("entry_time", 0) >= cutoff_4d]
    print(f"\n\n{'='*76}")
    print(f"  ENTRY CONTEXT — last 4 days ({len(recent)} trades)")
    print(f"{'='*76}")

    def split_by_field(field, recent, numeric=True):
        """Split wins/losses by field value."""
        w_vals = [r.get(field, 0) or 0 for r in recent if r.get("is_win")]
        l_vals = [r.get(field, 0) or 0 for r in recent if not r.get("is_win")]
        if w_vals and l_vals:
            return mean(w_vals), mean(l_vals)
        return None, None

    context_fields = [
        "entry_div_spread", "entry_spread_std", "entry_h1_atr_pips",
        "entry_tick_volume_ratio", "entry_cluster_count",
        "entry_conv_trend", "entry_conv_velocity", "entry_conv_isolation", "entry_conv_structural",
        "entry_strong_rank", "entry_weak_rank",
        "entry_strong_top_gap", "entry_weak_bottom_gap",
        "entry_strong_velocity", "entry_weak_velocity",
        "entry_m5_tr_slope_ratio",
        "adr_consumed_pct", "entry_momentum_buildup_sec",
    ]
    print(f"  {'Field':<35} {'Win avg':>9} {'Loss avg':>9} {'Δ':>7}")
    for f in context_fields:
        wa, la = split_by_field(f, recent)
        if wa is None:
            continue
        if wa == 0 and la == 0:
            continue
        delta = wa - la
        print(f"  {f:<35} {wa:>+9.2f} {la:>+9.2f} {delta:>+7.2f}")

    # ── What-if simulations ──
    print(f"\n\n{'='*76}")
    print(f"  OPTIMIZATION SCENARIOS")
    print(f"{'='*76}")

    def in_bad_window(r, windows):
        et = r.get("entry_time", 0)
        dt = datetime.fromtimestamp(et, tz=JST)
        m = dt.hour * 60 + dt.minute
        for s, e in windows:
            if s <= m < e:
                return True
        return False

    # Scenario 1: drop losing pairs
    kept_pairs_only = [r for r in trades if r.get("pair") not in losing_pairs]
    s1 = simulate(kept_pairs_only)
    print_stats(
        f"SCENARIO 1 — blacklist {len(losing_pairs)} losing pairs: {sorted(losing_pairs)}",
        s1,
    )

    # Scenario 2: block bad time windows
    if bad_windows:
        kept_time = [r for r in trades if not in_bad_window(r, bad_windows)]
        s2 = simulate(kept_time)
        print_stats(
            f"SCENARIO 2 — block {len(bad_windows)} bad 5-min windows",
            s2,
        )

    # Scenario 3: BOTH filters
    kept_both = [r for r in trades
                 if r.get("pair") not in losing_pairs
                 and not in_bad_window(r, bad_windows)]
    s3 = simulate(kept_both)
    print_stats(
        "SCENARIO 3 — BOTH filters (pair blacklist + time block)",
        s3,
    )

    # Final comparison
    print(f"\n\n{'='*76}")
    print(f"  COMPARISON")
    print(f"{'='*76}")
    print(f"  {'Metric':<22} {'Baseline':>11} {'Scenario 3':>11} {'Δ':>10}")
    for key, lbl, fmt in [
        ("n", "Trades", "{:d}"),
        ("wr", "Win Rate %", "{:.1f}"),
        ("total_pnl", "Total P/L", "{:+.1f}p"),
        ("pips_per_day", "Pips/Day", "{:+.1f}"),
        ("avg_win", "Avg Win", "{:+.1f}p"),
        ("avg_loss", "Avg Loss", "{:+.1f}p"),
        ("profit_factor", "Profit Factor", "{:.2f}"),
        ("daily_ret", "Daily Return %", "{:.2f}"),
        ("max_dd", "Max DD %", "{:.1f}"),
        ("recovery", "Recovery Factor", "{:.1f}"),
        ("return_pct", "Total Return %", "{:.1f}"),
    ]:
        b_val = baseline[key]
        o_val = s3[key]
        delta = o_val - b_val
        delta_str = f"{delta:+.1f}" if isinstance(b_val, float) else f"{int(delta):+d}"
        print(f"  {lbl:<22} {fmt.format(b_val):>11} {fmt.format(o_val):>11} {delta_str:>10}")

    # Export the final config for the user
    print(f"\n\n{'='*76}")
    print(f"  PROPOSED CONFIG")
    print(f"{'='*76}")
    print(f"  Pair blacklist (Sv2): {sorted(losing_pairs)}")
    print(f"  Time blacklist (JST):")
    for s, e in bad_windows:
        print(f"    {minute_to_hm(s)} - {minute_to_hm(e)}")


if __name__ == "__main__":
    main()
