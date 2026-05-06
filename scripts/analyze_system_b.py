"""System B (SS) deep analysis — 15-min windows over full history.

Same methodology as analyze_sv2_v2.py but for paper_trades_ss.json.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean

DATA = Path(r"D:\Trading\TAKUMI Trader\data")
JST = timezone(timedelta(hours=9))

BIN_MIN = 15
MIN_N_WINDOW = 5
MIN_PNL_BLOCK = -5.0
MIN_N_PAIR = 5

SYSTEM_FILE = "paper_trades_ss.json"
SYSTEM_NAME = "System B (SS — Sv2+Spread Stability)"


def load_closed() -> list[dict]:
    recs = json.loads((DATA / SYSTEM_FILE).read_text(encoding="utf-8"))
    return [r for r in recs if r.get("close_reason")]


def bin_of(r):
    dt = datetime.fromtimestamp(r["entry_time"], tz=JST)
    return (dt.hour * 60 + dt.minute) // BIN_MIN


def fmt_bin(b, is_end=False):
    m = b * BIN_MIN + (BIN_MIN if is_end else 0)
    h, mm = divmod(m, 60)
    return f"{h:02d}:{mm:02d}"


def stats(trades):
    if not trades:
        return dict(n=0, wr=0, pnl=0, avg=0, tp=0, sl=0, avg_win=0, avg_loss=0,
                    days=0, profit_factor=0, avg_sl=0, avg_dur=0)
    n = len(trades)
    wins = [t for t in trades if t.get("is_win")]
    losses = [t for t in trades if not t.get("is_win")]
    pnl = sum(t.get("pnl_pips", 0) or 0 for t in trades)
    gw = sum(t.get("pnl_pips", 0) or 0 for t in wins)
    gl = abs(sum(t.get("pnl_pips", 0) or 0 for t in losses))
    dates = set()
    for t in trades:
        et = t.get("entry_time", 0)
        if et > 0:
            dates.add(datetime.fromtimestamp(et, tz=JST).date())
    return dict(
        n=n,
        wr=len(wins) / n * 100,
        pnl=pnl,
        avg=pnl / n,
        tp=sum(1 for t in trades if t.get("close_reason") == "tp_hit"),
        sl=sum(1 for t in trades if t.get("close_reason") == "sl_hit"),
        avg_win=gw / len(wins) if wins else 0,
        avg_loss=-gl / len(losses) if losses else 0,
        days=max(1, len(dates)),
        profit_factor=gw / gl if gl > 0 else float("inf"),
        avg_sl=mean(t.get("sl_pips", 0) or 0 for t in trades),
        avg_dur=mean(t.get("duration_minutes", 0) or 0 for t in trades),
    )


def simulate_compound(trades, risk_pct=3.0, start_cap=1000.0):
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
    tpd = s["n"] / s["days"] if s["days"] else 0
    daily_ret = avg_r * tpd * (risk_pct / 100) * 100
    s.update(
        final=balance,
        return_pct=(balance / start_cap - 1) * 100,
        max_dd=max_dd,
        max_dd_abs=max_dd_abs,
        recovery=((balance / start_cap - 1) * 100) / max_dd if max_dd > 0 else 0,
        daily_ret=daily_ret,
        pips_per_day=s["pnl"] / s["days"],
        trades_per_day=tpd,
        avg_r=avg_r,
    )
    return s


def find_bad_windows(trades):
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
    bad.sort(key=lambda x: x[3])
    return {b for b, _, _, _ in bad}, bad


def merge_windows(blocked):
    if not blocked:
        return []
    s = sorted(blocked)
    merged = []
    cur_s = s[0]
    cur_e = s[0]
    for b in s[1:]:
        if b == cur_e + 1:
            cur_e = b
        else:
            merged.append((cur_s, cur_e))
            cur_s = b
            cur_e = b
    merged.append((cur_s, cur_e))
    return merged


def print_stats_block(label, s):
    print(f"\n{'='*76}")
    print(f"  {label}")
    print(f"{'='*76}")
    print(f"  Trades: {s['n']:>4}  WR: {s['wr']:>5.1f}%  ({s['tp']}TP/{s['sl']}SL)")
    print(f"  Total P/L: {s['pnl']:>+7.1f}p   Avg: {s['avg']:>+.2f}p")
    print(f"  Avg Win: {s['avg_win']:>+.1f}p   Avg Loss: {s['avg_loss']:>+.1f}p")
    print(f"  Pips/Day: {s['pips_per_day']:>+.1f}   Trades/Day: {s['trades_per_day']:>.1f}")
    print(f"  Daily Return: {s['daily_ret']:>+.2f}%   Monthly: {s['daily_ret']*22:>+.1f}%")
    print(f"  Profit Factor: {s['profit_factor']:>.2f}   Max DD: {s['max_dd']:>.1f}%   Recovery: {s['recovery']:>.1f}")
    print(f"  Final: ${s['final']:>,.2f}  ({s['return_pct']:+.1f}%)")


def in_block(t, blocked):
    return bin_of(t) in blocked


def main():
    trades = load_closed()
    print(f"{SYSTEM_NAME} — {len(trades)} closed trades\n")

    baseline = simulate_compound(trades)
    print_stats_block(f"BASELINE — all {len(trades)} trades", baseline)

    # Step 2: bad windows
    blocked, bad_details = find_bad_windows(trades)
    print(f"\n\n{'='*76}")
    print(f"  BAD 15-MIN WINDOWS (full history, n≥{MIN_N_WINDOW}, P/L≤{MIN_PNL_BLOCK}p)")
    print(f"{'='*76}")
    if not bad_details:
        print("  None found.")
    else:
        print(f"  {'Window':<14} {'n':>4} {'WR':>6} {'P/L':>9}")
        for b, n, wr, pnl in bad_details:
            flag = " !!!" if pnl < -30 else " !!" if pnl < -15 else " !"
            print(f"  {fmt_bin(b)}-{fmt_bin(b, True)}  {n:>4} {wr:>5.1f}% {pnl:>+8.1f}p{flag}")

    merged = merge_windows(blocked)
    print(f"\n  Merged blocked windows ({len(merged)} ranges):")
    for s, e in merged:
        print(f"    {fmt_bin(s)} - {fmt_bin(e, True)}")

    # Step 3: time-filter
    time_filtered = [t for t in trades if not in_block(t, blocked)]
    print(f"\n  After time filter: {len(time_filtered)}/{len(trades)} trades remain")

    # Step 4: pair analysis before/after
    print(f"\n\n{'='*76}")
    print(f"  PAIR ANALYSIS — Before vs After time filter")
    print(f"{'='*76}")

    pair_before = defaultdict(list)
    pair_after = defaultdict(list)
    for t in trades:
        pair_before[t.get("pair", "?")].append(t)
    for t in time_filtered:
        pair_after[t.get("pair", "?")].append(t)

    print(f"  {'Pair':<8} {'n':>4} {'WR':>6} {'P/L':>9}  -> {'n':>4} {'WR':>6} {'P/L':>9}  {'Verdict'}")
    results = []
    for pair in sorted(pair_before.keys()):
        bs = stats(pair_before[pair])
        as_ = stats(pair_after.get(pair, []))
        flipped = bs["pnl"] < 0 and as_["pnl"] >= 0 and as_["n"] > 0
        still_losing = as_["pnl"] < 0 and as_["n"] >= MIN_N_PAIR
        verdict = ""
        if as_["n"] == 0:
            verdict = "all removed"
        elif flipped:
            verdict = "✓ flipped"
        elif still_losing:
            verdict = "✗ still loses"
        elif bs["pnl"] < 0:
            verdict = "still neg (low n)"
        else:
            verdict = "still winner"
        results.append((pair, bs, as_, verdict, still_losing))

    results.sort(key=lambda x: x[2]["pnl"])
    for pair, bs, as_, verdict, _ in results:
        b_str = f"{bs['n']:>4} {bs['wr']:>5.1f}% {bs['pnl']:>+8.1f}p"
        if as_["n"] == 0:
            a_str = f"{'(all)':>23}"
        else:
            a_str = f"{as_['n']:>4} {as_['wr']:>5.1f}% {as_['pnl']:>+8.1f}p"
        print(f"  {pair:<8} {b_str}  -> {a_str}  {verdict}")

    final_blacklist = [p for p, bs, as_, v, losing in results if losing]
    print(f"\n  FINAL PAIR BLACKLIST (still loses after time filter, n≥{MIN_N_PAIR}): {final_blacklist}")

    # Flipped pairs
    flipped_list = [(p, bs["pnl"], as_["pnl"]) for p, bs, as_, v, _ in results
                    if v == "✓ flipped"]
    if flipped_list:
        print(f"\n  FLIPPED from loser to winner after time filter:")
        for p, before, after in flipped_list:
            print(f"    {p}: {before:+.1f}p → {after:+.1f}p")

    # Step 5: optimized
    fully = [t for t in time_filtered if t.get("pair") not in final_blacklist]
    optimized = simulate_compound(fully)
    print_stats_block(
        f"OPTIMIZED — time filter + {len(final_blacklist)} pair blacklist",
        optimized,
    )

    # Step 6: comparison
    print(f"\n\n{'='*76}")
    print(f"  COMPARISON")
    print(f"{'='*76}")
    print(f"  {'Metric':<22} {'Baseline':>12} {'Optimized':>12} {'Δ':>12}")
    labeled = [
        ("Trades", "n", "{:d}"),
        ("Win Rate %", "wr", "{:.1f}"),
        ("Total P/L", "pnl", "{:+.1f}p"),
        ("Pips/Day", "pips_per_day", "{:+.1f}"),
        ("Avg Win", "avg_win", "{:+.1f}p"),
        ("Avg Loss", "avg_loss", "{:+.1f}p"),
        ("Profit Factor", "profit_factor", "{:.2f}"),
        ("Daily Return %", "daily_ret", "{:.2f}"),
        ("Monthly Return %", None, None),
        ("Max DD %", "max_dd", "{:.1f}"),
        ("Max DD $", "max_dd_abs", "${:,.0f}"),
        ("Recovery Factor", "recovery", "{:.1f}"),
        ("Final balance", "final", "${:,.0f}"),
        ("Total Return %", "return_pct", "{:.1f}"),
    ]
    for label, key, fmt in labeled:
        if label == "Monthly Return %":
            b = baseline["daily_ret"] * 22
            o = optimized["daily_ret"] * 22
            print(f"  {label:<22} {b:>12.1f} {o:>12.1f} {o-b:>+12.1f}")
            continue
        b = baseline[key]
        o = optimized[key]
        b_s = fmt.format(b)
        o_s = fmt.format(o)
        delta = o - b
        d_s = f"{delta:+.1f}" if isinstance(b, float) else f"{int(delta):+d}"
        print(f"  {label:<22} {b_s:>12} {o_s:>12} {d_s:>12}")

    # Step 7: projections
    print(f"\n\n{'='*76}")
    print(f"  PROJECTIONS — Compound 3% risk starting $1,000")
    print(f"{'='*76}")
    daily_mult = (optimized["final"] / 1000) ** (1 / optimized["days"])
    print(f"  Observed daily compound rate: {(daily_mult-1)*100:+.2f}%")
    for h_days, label in [(22, "1 month"), (44, "2 months"), (66, "3 months")]:
        bal = 1000 * daily_mult ** h_days
        print(f"    {label} ({h_days}d): ${bal:>,.0f}  (+{(bal/1000-1)*100:.1f}%)")

    # Final config export
    print(f"\n\n{'='*76}")
    print(f"  PROPOSED {SYSTEM_NAME} CONFIG")
    print(f"{'='*76}")
    print(f"  Time blacklist:")
    for s, e in merged:
        print(f'    ["{fmt_bin(s)}", "{fmt_bin(e, True)}"]')
    print(f"  Pair blacklist: {final_blacklist}")


if __name__ == "__main__":
    main()
