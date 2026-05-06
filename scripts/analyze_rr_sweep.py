"""Simulate per-trade outcomes under different TP:SL ratios.

Current: TP = 0.50 × SL  (1:0.5 R:R, high WR low DD)
Test:    TP = 0.75 × SL  (1:0.75)
Test:    TP = 1.00 × SL  (1:1)

Re-simulation logic per trade:
  effective_MFE = max(peak_pnl_pips, post_close_max_mfe_pips)
  effective_MAE = max(|worst_pnl_pips|, post_close_max_mae_pips)
  new_TP = ratio × sl_pips

  if MFE >= new_TP and MAE < sl:  → WIN at +new_TP
  if MAE >= sl and MFE < new_TP:  → LOSS at -sl
  if both reached: use went_profit_first to break tie
    True  → TP hit first → WIN
    False → SL hit first → LOSS (conservative)
  if neither: trade times out → use original pnl

Then: subtract pair-aware commission, run 3% compound, compare metrics.
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


def commission_for_pair(pair: str) -> float:
    if pair in USD_QUOTE:
        return 0.6
    if pair.endswith("JPY"):
        return 0.7
    return 0.8


FILTERS = {
    "Sv2 (A)":      {"file":"paper_trades.json",         "time_bl":[], "pair_bl":set()},
    "SS (B)":       {"file":"paper_trades_ss.json",      "time_bl":[("08:00","08:15"),("14:15","14:30"),("16:00","16:15"),("16:30","16:45"),("17:45","18:00"),("20:15","20:45"),("21:00","21:15")], "pair_bl":{"AUDCAD","AUDCHF","CADCHF","EURCAD","EURCHF","EURUSD","GBPAUD"}},
    "ATR (C)":      {"file":"paper_trades_atr.json",     "time_bl":[("16:00","16:15")],                    "pair_bl":{"AUDCAD","AUDCHF","EURCAD","EURNZD","EURUSD","GBPAUD"}},
    "A-tuned (D)":  {"file":"paper_trades_a_tuned.json", "time_bl":[], "pair_bl":set()},
    "B-tuned (E)":  {"file":"paper_trades_b_tuned.json", "time_bl":[("08:15","08:30"),("14:15","14:30"),("15:00","15:15"),("16:30","16:45"),("17:00","17:15"),("19:00","19:15")], "pair_bl":{"AUDCAD","AUDCHF","AUDJPY","EURNZD","GBPCAD","NZDUSD"}},
}


def hm_to_min(s):
    h, m = s.split(":"); return int(h)*60 + int(m)


def in_blackout(dt, bl):
    mins = dt.hour*60 + dt.minute
    return any(hm_to_min(s) <= mins < hm_to_min(e) for s, e in bl)


def load_filtered(name):
    cfg = FILTERS[name]
    recs = json.loads((DATA / cfg["file"]).read_text(encoding="utf-8"))
    closed = [r for r in recs if r.get("close_reason")]
    out = []
    for r in closed:
        if r.get("pair") in cfg["pair_bl"]:
            continue
        dt = datetime.fromtimestamp(r["entry_time"], tz=JST)
        if in_blackout(dt, cfg["time_bl"]):
            continue
        out.append(r)
    return out


def simulate_trade(trade, tp_ratio):
    """Re-simulate one trade with a different TP:SL ratio.

    Returns (new_pnl_pips, outcome) where outcome is one of:
      'tp_new' — would hit new TP
      'sl_new' — would hit SL
      'timeout' — neither, use original pnl
    """
    sl = trade.get("sl_pips", 0) or 0
    if sl <= 0:
        return trade.get("pnl_pips", 0) or 0, "timeout"

    new_tp = sl * tp_ratio
    peak = trade.get("peak_pnl_pips", 0) or 0
    worst = abs(trade.get("worst_pnl_pips", 0) or 0)
    pc_mfe = trade.get("post_close_max_mfe_pips", 0) or 0
    pc_mae = trade.get("post_close_max_mae_pips", 0) or 0

    eff_mfe = max(peak, pc_mfe)
    eff_mae = max(worst, pc_mae)

    tp_hit = eff_mfe >= new_tp
    sl_hit = eff_mae >= sl
    went_first = bool(trade.get("went_profit_first", False))

    if tp_hit and not sl_hit:
        return new_tp, "tp_new"
    if sl_hit and not tp_hit:
        return -sl, "sl_new"
    if tp_hit and sl_hit:
        # Both reached at some point — use went_profit_first to break tie
        if went_first:
            return new_tp, "tp_new"
        else:
            # If original close was tp_hit, profit came first even if went_first
            # was False (e.g., trade reached +3p without crossing +5p threshold)
            if trade.get("close_reason") == "tp_hit":
                return new_tp, "tp_new"
            return -sl, "sl_new"
    # Neither — keep original
    return trade.get("pnl_pips", 0) or 0, "timeout"


def stats_for_ratio(trades, tp_ratio, apply_commission=True):
    if not trades:
        return None
    sorted_t = sorted(trades, key=lambda r: r["entry_time"])
    bal = 1000.0
    peak_b = bal
    mdd = 0.0
    n_tp = n_sl = n_to = 0
    pnl_total = 0.0
    gw = 0.0
    gl = 0.0
    wins = 0
    for r in sorted_t:
        sl = r.get("sl_pips", 0) or 10
        new_pnl, outcome = simulate_trade(r, tp_ratio)
        if outcome == "tp_new":
            n_tp += 1
        elif outcome == "sl_new":
            n_sl += 1
        else:
            n_to += 1

        net = new_pnl
        if apply_commission:
            net -= commission_for_pair(r.get("pair", ""))

        rm = net / sl if sl > 0 else 0
        bal += bal * 0.03 * rm
        if bal > peak_b:
            peak_b = bal
        dd = (peak_b - bal) / peak_b * 100
        if dd > mdd:
            mdd = dd

        pnl_total += net
        if net > 0:
            gw += net
            wins += 1
        else:
            gl += abs(net)

    n = len(sorted_t)
    pf = gw / gl if gl > 0 else float("inf")
    dates = set(datetime.fromtimestamp(r["entry_time"], tz=JST).date()
                for r in sorted_t)
    days = max(1, len(dates))
    avg_sl = mean(r.get("sl_pips", 0) or 0 for r in sorted_t)
    avg = pnl_total / n
    avg_r = avg / avg_sl if avg_sl > 0 else 0
    tpd = n / days
    daily = avg_r * tpd * 0.03 * 100
    return dict(
        n=n, wr=wins/n*100, pnl=pnl_total, pf=pf, avg=avg,
        final=bal, ret_pct=(bal/1000-1)*100, mdd=mdd,
        days=days, tpd=tpd, daily=daily,
        n_tp=n_tp, n_sl=n_sl, n_to=n_to,
    )


def fmt_row(label, s, baseline=None):
    if s is None or s["n"] == 0:
        return f"  {label:<28}  (no trades)"
    extra = ""
    if baseline is not None:
        d_pnl = s["pnl"] - baseline["pnl"]
        d_pf = s["pf"] - baseline["pf"]
        d_dd = s["mdd"] - baseline["mdd"]
        d_final = s["final"] - baseline["final"]
        extra = (f"   Δ pnl={d_pnl:+6.1f}p  Δ PF={d_pf:+5.2f}  "
                 f"Δ DD={d_dd:+5.1f}%  Δ$={d_final:+6.0f}")
    return (f"  {label:<28} n={s['n']:>3} WR={s['wr']:>5.1f}% "
            f"P/L={s['pnl']:>+7.1f}p PF={s['pf']:>4.2f} "
            f"DD={s['mdd']:>5.1f}% daily={s['daily']:>+6.2f}% "
            f"${s['final']:>6,.0f}{extra}")


def main():
    print("R:R RATIO SWEEP — historical re-simulation per trade\n")
    print("Ratios tested: 0.50 (current), 0.75, 1.00")
    print("Commission applied (ICMarkets Raw cTrader: 0.6/0.7/0.8 pips by pair type)\n")

    all_trades = {}
    for name in FILTERS:
        all_trades[name] = load_filtered(name)

    # Per-system table
    for name in FILTERS:
        trades = all_trades[name]
        if not trades:
            continue
        s_50 = stats_for_ratio(trades, 0.50)
        s_75 = stats_for_ratio(trades, 0.75)
        s_100 = stats_for_ratio(trades, 1.00)

        print(f"\n=== {name} ({len(trades)} filtered trades) ===")
        print(fmt_row("0.50 × SL (current)", s_50))
        print(fmt_row("0.75 × SL", s_75, s_50))
        print(fmt_row("1.00 × SL", s_100, s_50))
        print(f"  → 0.50 outcomes: {s_50['n_tp']:>3} TP_new / "
              f"{s_50['n_sl']:>3} SL_new / {s_50['n_to']:>3} timeout")
        print(f"  → 0.75 outcomes: {s_75['n_tp']:>3} TP_new / "
              f"{s_75['n_sl']:>3} SL_new / {s_75['n_to']:>3} timeout")
        print(f"  → 1.00 outcomes: {s_100['n_tp']:>3} TP_new / "
              f"{s_100['n_sl']:>3} SL_new / {s_100['n_to']:>3} timeout")

    # Combined portfolio (DTC-combo equivalent: SS + ATR + B-tuned + dedup)
    print(f"\n\n{'='*72}")
    print(f"  PORTFOLIO: SS + ATR + B-tuned (current DTC-combo sources)")
    print(f"{'='*72}")
    combined = []
    for name in ("SS (B)", "ATR (C)", "B-tuned (E)"):
        for t in all_trades[name]:
            t = dict(t)
            t["_system"] = name
            combined.append(t)
    by_pair = defaultdict(list)
    for t in combined:
        by_pair[t["pair"]].append(t)
    deduped = []
    for p, ts in by_pair.items():
        ts_sorted = sorted(ts, key=lambda x: x["entry_time"])
        last = None
        for t in ts_sorted:
            et = t["entry_time"]
            if last is None or (et - last) >= 120:
                deduped.append(t)
                last = et

    s_50 = stats_for_ratio(deduped, 0.50)
    s_75 = stats_for_ratio(deduped, 0.75)
    s_100 = stats_for_ratio(deduped, 1.00)
    print(fmt_row("0.50 × SL (current)", s_50))
    print(fmt_row("0.75 × SL", s_75, s_50))
    print(fmt_row("1.00 × SL", s_100, s_50))
    print(f"\n  Outcome distribution (of {len(deduped)} trades):")
    print(f"    0.50 × SL : {s_50['n_tp']:>3} TP / {s_50['n_sl']:>3} SL / {s_50['n_to']:>3} timeout  → WR {s_50['wr']:.1f}%")
    print(f"    0.75 × SL : {s_75['n_tp']:>3} TP / {s_75['n_sl']:>3} SL / {s_75['n_to']:>3} timeout  → WR {s_75['wr']:.1f}%")
    print(f"    1.00 × SL : {s_100['n_tp']:>3} TP / {s_100['n_sl']:>3} SL / {s_100['n_to']:>3} timeout  → WR {s_100['wr']:.1f}%")

    # Fine sweep on portfolio
    print(f"\n\n{'='*72}")
    print(f"  FULL R:R SWEEP — portfolio")
    print(f"{'='*72}")
    print(f"  {'TP/SL':>6}  {'WR':>6} {'TP':>4} {'SL':>4} {'TO':>3} "
          f"{'P/L':>8} {'PF':>5} {'DD':>5} {'Daily':>7} {'Final':>9}")
    for ratio in [0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0, 1.1, 1.25, 1.5, 1.75, 2.0]:
        s = stats_for_ratio(deduped, ratio)
        if s is None:
            continue
        print(f"  {ratio:>5.2f}   "
              f"{s['wr']:>5.1f}% {s['n_tp']:>3} {s['n_sl']:>3} {s['n_to']:>3} "
              f"{s['pnl']:>+7.1f}p {s['pf']:>4.2f} "
              f"{s['mdd']:>4.1f}% {s['daily']:>+6.2f}% "
              f"${s['final']:>8,.0f}")


if __name__ == "__main__":
    main()
