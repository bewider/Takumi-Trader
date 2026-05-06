"""QM4 optimal subset test — CUM/SELL + (unset)/BUY combined.

These two slices alone had the best WR and P/L. Test if the combined
subset produces a sustainable, tradeable system.
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


def load_closed():
    recs = json.loads((DATA / "paper_trades_qm4.json").read_text(encoding="utf-8"))
    return [r for r in recs if r.get("close_reason")]


def stats(trades):
    if not trades:
        return dict(n=0, wr=0, pnl=0, avg=0, tp=0, sl=0, avg_win=0, avg_loss=0,
                    days=0, profit_factor=0, avg_sl=0)
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
    )


def simulate_compound(trades, risk_pct=3.0, start_cap=1000.0):
    s = stats(trades)
    if s["n"] == 0:
        return s
    sorted_t = sorted(trades, key=lambda r: r.get("entry_time", 0))
    balance = start_cap
    peak = start_cap
    max_dd = 0.0
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
    avg_r = s["avg"] / s["avg_sl"] if s["avg_sl"] > 0 else 0
    tpd = s["n"] / s["days"]
    daily_ret = avg_r * tpd * (risk_pct / 100) * 100
    s.update(
        final=balance,
        return_pct=(balance / start_cap - 1) * 100,
        max_dd=max_dd,
        recovery=((balance / start_cap - 1) * 100) / max_dd if max_dd > 0 else 0,
        daily_ret=daily_ret,
        pips_per_day=s["pnl"] / s["days"],
        trades_per_day=tpd,
        avg_r=avg_r,
    )
    return s


def print_stats(label, s):
    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"{'='*70}")
    if s["n"] == 0:
        print("  (no trades)")
        return
    print(f"  Trades: {s['n']}   WR: {s['wr']:.1f}%   ({s['tp']}TP/{s['sl']}SL)")
    print(f"  Total P/L: {s['pnl']:+.1f}p   Avg: {s['avg']:+.2f}p")
    print(f"  Avg Win/Loss: {s['avg_win']:+.1f}p / {s['avg_loss']:+.1f}p")
    print(f"  Profit Factor: {s['profit_factor']:.2f}   Max DD: {s['max_dd']:.1f}%")
    print(f"  Trades/Day: {s['trades_per_day']:.1f}   Pips/Day: {s['pips_per_day']:+.1f}p")
    print(f"  Daily Return: {s['daily_ret']:+.2f}%")
    print(f"  Final: ${s['final']:,.0f}  ({s['return_pct']:+.1f}%)")
    print(f"  Recovery Factor: {s['recovery']:.2f}")


def group_print(label, trades, key_fn, min_n=3):
    groups = defaultdict(list)
    for t in trades:
        k = key_fn(t)
        if k is None:
            continue
        groups[k].append(t)
    rows = []
    for k, grp in groups.items():
        s = stats(grp)
        rows.append((k, s["n"], s["wr"], s["pnl"], s["avg"]))
    rows.sort(key=lambda x: x[3])
    print(f"\n  {label}:")
    print(f"    {'Key':<22} {'n':>4} {'WR':>6} {'P/L':>9} {'avg':>7}")
    for k, n, wr, pnl, avg in rows:
        if n < min_n:
            continue
        flag = ""
        if pnl < -10:
            flag = " !!!"
        elif pnl < 0:
            flag = " !"
        elif pnl > 20:
            flag = " ✓✓"
        elif pnl > 5:
            flag = " ✓"
        print(f"    {str(k):<22} {n:>4} {wr:>5.1f}% {pnl:>+8.1f}p {avg:>+6.2f}p{flag}")


def projection(daily_ret_pct, days):
    """Compounded total return across n trading days."""
    return ((1 + daily_ret_pct / 100) ** days - 1) * 100


def main():
    trades = load_closed()
    print(f"QM4 optimal subset — {len(trades)} closed trades\n")

    # Filter: CUM/SELL + (unset)/BUY only
    def is_keep(r):
        atype = r.get("qm4_alert_type") or "(unset)"
        dir_ = r.get("direction") or ""
        if atype == "CUM" and dir_ == "SELL":
            return True
        if atype == "(unset)" and dir_ == "BUY":
            return True
        return False

    subset = [r for r in trades if is_keep(r)]

    # Baseline (all QM4)
    base = simulate_compound(trades)
    print_stats("BASELINE — all 508 QM4 trades", base)

    # Subset 1: CUM/SELL
    cum_sell = [r for r in trades if (r.get("qm4_alert_type") or "(unset)") == "CUM" and r.get("direction") == "SELL"]
    print_stats("CUM / SELL only", simulate_compound(cum_sell))

    # Subset 2: (unset)/BUY
    unset_buy = [r for r in trades if (r.get("qm4_alert_type") or "(unset)") == "(unset)" and r.get("direction") == "BUY"]
    print_stats("(unset) / BUY only", simulate_compound(unset_buy))

    # Combined
    combined = simulate_compound(subset)
    print_stats("COMBINED — CUM/SELL + (unset)/BUY", combined)

    # Pair breakdown of combined
    group_print("Pair breakdown of combined subset", subset, lambda r: r.get("pair"))

    # Time breakdown of combined
    def bin_of(r):
        dt = datetime.fromtimestamp(r["entry_time"], tz=JST)
        return (dt.hour * 60 + dt.minute) // 15

    def fmt_bin(b):
        m = b * 15
        h, mm = divmod(m, 60)
        h2, mm2 = divmod(m + 15, 60)
        return f"{h:02d}:{mm:02d}-{h2:02d}:{mm2:02d}"

    group_print(
        "15-min window breakdown (bad windows only)",
        subset,
        lambda r: fmt_bin(bin_of(r)),
        min_n=3,
    )

    # Projections (assume combined subset keeps its ~2.6p avg)
    print(f"\n{'='*70}")
    print(f"  FORWARD PROJECTIONS (combined subset, compounded)")
    print(f"{'='*70}")
    dr = combined["daily_ret"]
    print(f"  Daily compound: {dr:+.2f}%")
    print(f"  Trades/day: {combined['trades_per_day']:.1f}")
    print(f"  1 week   (5 days):  {projection(dr, 5):+.1f}%")
    print(f"  2 weeks (10 days):  {projection(dr, 10):+.1f}%")
    print(f"  1 month (22 days):  {projection(dr, 22):+.1f}%")
    print(f"  2 months(44 days):  {projection(dr, 44):+.1f}%")
    print(f"  3 months(66 days):  {projection(dr, 66):+.1f}%")

    # Also test with aggressive pair filter (drop the tiny losers)
    print(f"\n{'='*70}")
    print(f"  REFINED SUBSET — with losing-pair filter")
    print(f"{'='*70}")

    # Compute pair stats first to find losers (n >= 3 AND pnl < 0)
    pair_groups = defaultdict(list)
    for t in subset:
        pair_groups[t.get("pair")].append(t)
    bad_pairs = [p for p, tr in pair_groups.items()
                 if len(tr) >= 3 and sum(x.get("pnl_pips", 0) or 0 for x in tr) < 0]
    print(f"  Removing pairs with n≥3 and negative P/L: {sorted(bad_pairs)}")

    refined = [t for t in subset if t.get("pair") not in bad_pairs]
    refined_s = simulate_compound(refined)
    print_stats("REFINED — combined + pair filter", refined_s)

    dr2 = refined_s["daily_ret"]
    print(f"\n  Refined projections:")
    print(f"  Daily compound: {dr2:+.2f}%")
    print(f"  1 month (22 days): {projection(dr2, 22):+.1f}%")
    print(f"  2 months(44 days): {projection(dr2, 44):+.1f}%")


if __name__ == "__main__":
    main()
