"""QM4 (System F) deep analysis — find any profitable subset.

QM4 has been the worst-performing system overall. This script explores:
  - By alert type (MTF, MTFC, HTF, CUM, PAIR/*)
  - By pair
  - By 15-min windows
  - By alignment count
  - By ADR bucket
  - By direction
  - Combined filters to find ANY positive edge
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


def load_closed():
    recs = json.loads((DATA / "paper_trades_qm4.json").read_text(encoding="utf-8"))
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
    if s["n"] == 0:
        return s
    sorted_t = sorted(trades, key=lambda r: r.get("entry_time", 0))
    balance = start_cap
    peak = start_cap
    max_dd = 0.0
    max_dd_abs = 0.0
    for r in sorted_t:
        sl = r.get("sl_pips", 0) or 10
        pnl = r.get("pnl_pips", 0) or 0
        r_mult = pnl / sl if sl > 0 else 0
        balance += balance * (risk_pct / 100) * r_mult
        if balance > peak:
            peak = balance
        dd = (peak - balance) / peak * 100
        if dd > max_dd:
            max_dd = dd
            max_dd_abs = peak - balance
    avg_r = s["avg"] / s["avg_sl"] if s["avg_sl"] > 0 else 0
    tpd = s["n"] / s["days"]
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


def group_stats(trades, key_fn):
    groups = defaultdict(list)
    for r in trades:
        k = key_fn(r)
        if k is None or k == "":
            continue
        groups[k].append(r)
    rows = []
    for k, grp in groups.items():
        s = stats(grp)
        rows.append((k, s["n"], s["wr"], s["pnl"], s["avg"]))
    return sorted(rows, key=lambda x: x[3])


def print_grp(label, rows, min_n=3):
    print(f"\n  {label}:")
    print(f"    {'Key':<22} {'n':>4} {'WR':>6} {'P/L':>9} {'avg':>7}")
    for k, n, wr, pnl, avg in rows:
        if n < min_n:
            continue
        flag = ""
        if pnl < -20:
            flag = " !!!"
        elif pnl < -5:
            flag = " !!"
        elif pnl > 20:
            flag = " ✓✓"
        elif pnl > 5:
            flag = " ✓"
        print(f"    {str(k):<22} {n:>4} {wr:>5.1f}% {pnl:>+8.1f}p {avg:>+6.2f}p{flag}")


def main():
    trades = load_closed()
    print(f"QM4 (System F) deep analysis — {len(trades)} closed trades")

    # Baseline
    baseline = simulate_compound(trades)
    print(f"\n{'='*80}")
    print(f"  BASELINE — all {len(trades)} trades")
    print(f"{'='*80}")
    print(f"  Trades: {baseline['n']}  WR: {baseline['wr']:.1f}%  ({baseline['tp']}TP/{baseline['sl']}SL)")
    print(f"  Total P/L: {baseline['pnl']:+.1f}p  Avg: {baseline['avg']:+.2f}p")
    print(f"  Avg Win/Loss: {baseline['avg_win']:+.1f}p / {baseline['avg_loss']:+.1f}p")
    print(f"  Profit Factor: {baseline['profit_factor']:.2f}  Max DD: {baseline['max_dd']:.1f}%")
    print(f"  Final: ${baseline['final']:,.0f}  ({baseline['return_pct']:+.1f}%)")
    print(f"  Days: {baseline['days']}  Trades/Day: {baseline['trades_per_day']:.1f}")

    # By alert type
    print(f"\n\n{'='*80}")
    print(f"  SLICING BY ALERT TYPE")
    print(f"{'='*80}")
    by_type = group_stats(trades, lambda r: r.get("qm4_alert_type", "(unset)") or "(unset)")
    print_grp("Alert types", sorted(by_type, key=lambda x: -x[3]), min_n=3)

    # By direction
    print(f"\n\n{'='*80}")
    print(f"  SLICING BY DIRECTION")
    print(f"{'='*80}")
    by_dir = group_stats(trades, lambda r: r.get("direction", ""))
    print_grp("Direction", sorted(by_dir, key=lambda x: -x[3]))

    # By alignment count
    print(f"\n\n{'='*80}")
    print(f"  SLICING BY ALIGNMENT COUNT")
    print(f"{'='*80}")
    by_align = group_stats(trades, lambda r: r.get("entry_alignment_count", 0))
    print_grp("Alignment", sorted(by_align, key=lambda x: x[0]))

    # By ADR bucket
    print(f"\n\n{'='*80}")
    print(f"  SLICING BY ADR BUCKET")
    print(f"{'='*80}")
    def adr_bucket(r):
        a = r.get("adr_consumed_pct", 0)
        if a < 30: return "0-30%"
        elif a < 50: return "30-50%"
        elif a < 70: return "50-70%"
        elif a < 90: return "70-90%"
        elif a < 110: return "90-110%"
        elif a < 130: return "110-130%"
        else: return ">130%"
    by_adr = group_stats(trades, adr_bucket)
    print_grp("ADR bucket", sorted(by_adr, key=lambda x: x[0]))

    # By pair
    print(f"\n\n{'='*80}")
    print(f"  SLICING BY PAIR")
    print(f"{'='*80}")
    by_pair = group_stats(trades, lambda r: r.get("pair", "?"))
    print_grp("Pair", by_pair, min_n=5)

    # By 15-min window
    print(f"\n\n{'='*80}")
    print(f"  BAD 15-MIN WINDOWS (full history)")
    print(f"{'='*80}")
    by_win = group_stats(trades, bin_of)
    bad_windows = [(k, n, wr, pnl) for k, n, wr, pnl, _ in by_win
                   if n >= MIN_N_WINDOW and pnl <= MIN_PNL_BLOCK]
    print(f"    {'Window':<14} {'n':>4} {'WR':>6} {'P/L':>9}")
    for k, n, wr, pnl in sorted(bad_windows, key=lambda x: x[3]):
        flag = " !!!" if pnl < -20 else " !!" if pnl < -10 else " !"
        print(f"    {fmt_bin(k)}-{fmt_bin(k, True)}  {n:>4} {wr:>5.1f}% {pnl:>+8.1f}p{flag}")
    print(f"\n  Good 15-min windows (n≥{MIN_N_WINDOW}, P/L ≥ +5p):")
    good_windows = [(k, n, wr, pnl) for k, n, wr, pnl, _ in by_win
                    if n >= MIN_N_WINDOW and pnl >= 5]
    print(f"    {'Window':<14} {'n':>4} {'WR':>6} {'P/L':>9}")
    for k, n, wr, pnl in sorted(good_windows, key=lambda x: -x[3]):
        print(f"    {fmt_bin(k)}-{fmt_bin(k, True)}  {n:>4} {wr:>5.1f}% {pnl:>+8.1f}p")

    # === Intersection analyses: find profitable subsets ===
    print(f"\n\n{'='*80}")
    print(f"  INTERSECTION: ALERT TYPE × DIRECTION")
    print(f"{'='*80}")
    groups = defaultdict(list)
    for r in trades:
        k = f"{r.get('qm4_alert_type', '?') or '?'} / {r.get('direction', '?')}"
        groups[k].append(r)
    rows = []
    for k, grp in groups.items():
        s = stats(grp)
        rows.append((k, s["n"], s["wr"], s["pnl"], s["avg"]))
    rows.sort(key=lambda x: -x[3])
    print_grp("Type × Direction", rows, min_n=3)

    # === Best combined filter ===
    print(f"\n\n{'='*80}")
    print(f"  OPTIMIZATION — best combined filter")
    print(f"{'='*80}")

    # Build filters based on the analysis above
    # Kill blocked time windows (15-min)
    blocked_bins = set()
    for k, n, wr, pnl in bad_windows:
        blocked_bins.add(k)

    # Kill losing pairs (n >= 5)
    losing_pairs = {pair for pair, n, wr, pnl, _ in by_pair
                    if pnl < 0 and n >= MIN_N_PAIR}

    # Kill losing alert types (n >= 5)
    losing_types = {atype for atype, n, wr, pnl, _ in by_type
                    if pnl < 0 and n >= 5}

    # Kill losing ADR buckets
    losing_adr = {bkt for bkt, n, wr, pnl, _ in by_adr
                  if pnl < 0 and n >= 5}

    def adr_bucket_of(r):
        a = r.get("adr_consumed_pct", 0)
        if a < 30: return "0-30%"
        elif a < 50: return "30-50%"
        elif a < 70: return "50-70%"
        elif a < 90: return "70-90%"
        elif a < 110: return "90-110%"
        elif a < 130: return "110-130%"
        else: return ">130%"

    def passes(r):
        if bin_of(r) in blocked_bins:
            return False
        if r.get("pair") in losing_pairs:
            return False
        if (r.get("qm4_alert_type") or "(unset)") in losing_types:
            return False
        if adr_bucket_of(r) in losing_adr:
            return False
        return True

    optimized = [r for r in trades if passes(r)]
    opt_stats = simulate_compound(optimized)

    print(f"  Applied filters:")
    print(f"    Time blacklist bins: {len(blocked_bins)} windows")
    print(f"    Pair blacklist: {sorted(losing_pairs)}")
    print(f"    Alert type blacklist: {sorted(losing_types)}")
    print(f"    ADR bucket blacklist: {sorted(losing_adr)}")
    print()
    print(f"  Baseline: {baseline['n']} trades, {baseline['wr']:.1f}% WR, {baseline['pnl']:+.1f}p, DD {baseline['max_dd']:.1f}%")
    print(f"  Optimized: {opt_stats['n']} trades, {opt_stats['wr']:.1f}% WR, {opt_stats['pnl']:+.1f}p, DD {opt_stats['max_dd']:.1f}%")
    print(f"  Improvement: {opt_stats['pnl'] - baseline['pnl']:+.1f}p")

    # Now recheck pair/alert-type with filters applied
    print(f"\n  PAIR PERFORMANCE after all filters:")
    pair_after = group_stats(optimized, lambda r: r.get("pair", "?"))
    print_grp("Pair", pair_after, min_n=3)

    # === Pair blacklist AFTER time + alert filters (not just pair-only) ===
    # Re-check which pairs still lose once we filter out bad bins/types
    filtered = [r for r in trades
                if bin_of(r) not in blocked_bins
                and (r.get("qm4_alert_type") or "(unset)") not in losing_types]
    pair_after_time = group_stats(filtered, lambda r: r.get("pair", "?"))
    print(f"\n  PAIR PERFORMANCE after only time + alert-type filters (no pair filter):")
    print_grp("Pair", pair_after_time, min_n=3)

    # Final optimized stats with compound projection
    print(f"\n\n{'='*80}")
    print(f"  FINAL OPTIMIZED STATS")
    print(f"{'='*80}")
    print(f"  Trades: {opt_stats['n']}  WR: {opt_stats['wr']:.1f}%  ({opt_stats['tp']}TP/{opt_stats['sl']}SL)")
    print(f"  Total P/L: {opt_stats['pnl']:+.1f}p  Avg: {opt_stats['avg']:+.2f}p")
    print(f"  Avg Win/Loss: {opt_stats['avg_win']:+.1f}p / {opt_stats['avg_loss']:+.1f}p")
    print(f"  Profit Factor: {opt_stats['profit_factor']:.2f}  Max DD: {opt_stats['max_dd']:.1f}%")
    print(f"  Daily Return: {opt_stats['daily_ret']:+.2f}%  Monthly: {opt_stats['daily_ret']*22:+.1f}%")
    print(f"  Final: ${opt_stats['final']:,.0f}  ({opt_stats['return_pct']:+.1f}%)")


if __name__ == "__main__":
    main()
