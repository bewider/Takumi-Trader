"""Analyze: what if we enter with a limit order at 8% of SL further
from market (in the favorable direction)?

For a BUY signal at price E with SL distance = sl_pips:
  Limit price L = E - 0.08 × sl_pips  (BUY: 0.08*SL below market)
  For SELL: L = E + 0.08 × sl_pips  (above market)

A trade FILLS at L only if price pulled back at least 8% of SL against
the original direction. We use `worst_pnl_pips` (max adverse excursion)
to detect this:
  fills if abs(worst_pnl_pips) >= 0.08 × sl_pips

Two interpretations:
  A) SL/TP relative to FILL price (standard broker behavior).
     Effect: only filters trades — pnl unchanged for filled trades.
  B) SL/TP price levels FIXED at signal-time (some traders).
     Effect: filtered + each filled trade pnl shifted by +0.08*sl_pips
     (better entry adds to win, reduces loss).

Reports per-system and combined portfolio for both interpretations.
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
LIMIT_PCT = 0.08  # 8% of SL distance

# Pair-aware commission (ICMarkets Raw cTrader)
USD_QUOTE = {"EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY"}


def commission_for_pair(pair: str) -> float:
    if pair in USD_QUOTE:
        return 0.6
    if pair.endswith("JPY"):
        return 0.7
    return 0.8


# Per-system optimized filters (current production config)
FILTERS = {
    "Sv2 (A)": {
        "file": "paper_trades.json",
        "time_bl": [],
        "pair_bl": set(),
    },
    "SS (B)": {
        "file": "paper_trades_ss.json",
        "time_bl": [
            ("08:00","08:15"), ("14:15","14:30"),
            ("16:00","16:15"), ("16:30","16:45"),
            ("17:45","18:00"),
            ("20:15","20:45"), ("21:00","21:15"),
        ],
        "pair_bl": {"AUDCAD","AUDCHF","CADCHF","EURCAD","EURCHF","EURUSD","GBPAUD"},
    },
    "ATR (C)": {
        "file": "paper_trades_atr.json",
        "time_bl": [("16:00","16:15")],
        "pair_bl": {"AUDCAD","AUDCHF","EURCAD","EURNZD","EURUSD","GBPAUD"},
    },
    "A-tuned (D)": {
        "file": "paper_trades_a_tuned.json",
        "time_bl": [],
        "pair_bl": set(),
    },
    "B-tuned (E)": {
        "file": "paper_trades_b_tuned.json",
        "time_bl": [
            ("08:15","08:30"), ("14:15","14:30"),
            ("15:00","15:15"), ("16:30","16:45"),
            ("17:00","17:15"), ("19:00","19:15"),
        ],
        "pair_bl": {"AUDCAD","AUDCHF","AUDJPY","EURNZD","GBPCAD","NZDUSD"},
    },
}


def hm_to_min(s):
    h, m = s.split(":")
    return int(h)*60 + int(m)


def in_blackout(dt, bl):
    mins = dt.hour*60 + dt.minute
    for s, e in bl:
        if hm_to_min(s) <= mins < hm_to_min(e):
            return True
    return False


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


def classify_limit_fill(trade):
    """Return (filled: bool, threshold: float).

    Limit fills if the trade pulled back at least 8% of SL against the
    direction (i.e., worst_pnl_pips <= -0.08 * sl_pips).
    """
    sl = trade.get("sl_pips", 0) or 0
    if sl <= 0:
        return False, 0.0
    threshold = LIMIT_PCT * sl
    worst = trade.get("worst_pnl_pips", 0) or 0
    # worst_pnl_pips is negative for adverse excursion
    filled = (worst <= -threshold)
    return filled, threshold


def adjust_pnl(trade, interp: str):
    """Compute net pnl under chosen interpretation.

    A: SL/TP relative to fill — pnl unchanged.
    B: SL/TP fixed at signal — pnl shifted by +0.08*sl_pips.
    """
    pnl = trade.get("pnl_pips", 0) or 0
    if interp == "B":
        sl = trade.get("sl_pips", 0) or 0
        pnl = pnl + LIMIT_PCT * sl
    return pnl


def stats(trades, interp, apply_commission=True):
    """Return stats for the trades that would have FILLED under limit logic."""
    if not trades:
        return None
    filled_trades = []
    for t in trades:
        fill, _ = classify_limit_fill(t)
        if not fill:
            continue
        net_pnl = adjust_pnl(t, interp)
        if apply_commission:
            net_pnl -= commission_for_pair(t.get("pair", ""))
        filled_trades.append((t, net_pnl))

    if not filled_trades:
        return dict(n=0, n_orig=len(trades), wr=0, pnl=0, mdd=0, daily=0,
                    final=1000, ret_pct=0, fill_rate=0, pf=0,
                    days=1, tpd=0, missed=len(trades),
                    missed_winners=0, missed_losers=0,
                    missed_pnl=0)

    # Counter-factual: what did we miss?
    missed = [t for t in trades if not classify_limit_fill(t)[0]]
    missed_w = sum(1 for t in missed if t.get("is_win"))
    missed_l = len(missed) - missed_w
    missed_pnl = sum((t.get("pnl_pips", 0) or 0) for t in missed)
    if apply_commission:
        missed_pnl -= sum(commission_for_pair(t.get("pair", "")) for t in missed)

    sorted_t = sorted(filled_trades, key=lambda x: x[0]["entry_time"])
    bal = 1000.0
    peak = bal
    mdd = 0.0
    for t, net in sorted_t:
        sl = t.get("sl_pips", 0) or 10
        # New "effective SL" under interp B is 0.92*SL (smaller risk)
        if interp == "B":
            sl_eff = sl * (1 - LIMIT_PCT)
        else:
            sl_eff = sl
        rm = net / sl_eff if sl_eff > 0 else 0
        bal += bal * 0.03 * rm
        if bal > peak:
            peak = bal
        dd = (peak - bal) / peak * 100
        if dd > mdd:
            mdd = dd

    n = len(filled_trades)
    pnl = sum(net for _, net in filled_trades)
    wins = [(t, p) for t, p in filled_trades if p > 0]
    gw = sum(p for _, p in wins)
    gl = abs(sum(p for _, p in filled_trades if p <= 0))
    pf = gw / gl if gl > 0 else float("inf")
    dates = set(datetime.fromtimestamp(t["entry_time"], tz=JST).date()
                for t, _ in filled_trades)
    days = max(1, len(dates))
    avg_sl = mean(t.get("sl_pips", 0) or 0 for t, _ in filled_trades)
    if interp == "B":
        avg_sl_eff = avg_sl * (1 - LIMIT_PCT)
    else:
        avg_sl_eff = avg_sl
    avg = pnl / n
    avg_r = avg / avg_sl_eff if avg_sl_eff > 0 else 0
    tpd = n / days
    daily = avg_r * tpd * 0.03 * 100
    return dict(
        n=n, n_orig=len(trades), wr=len(wins) / n * 100,
        pnl=pnl, avg=avg, pf=pf,
        final=bal, ret_pct=(bal / 1000 - 1) * 100, mdd=mdd,
        days=days, tpd=tpd, daily=daily,
        fill_rate=n / len(trades) * 100,
        missed=len(missed), missed_winners=missed_w, missed_losers=missed_l,
        missed_pnl=missed_pnl,
    )


def baseline_stats(trades, apply_commission=True):
    """Stats using the ORIGINAL market-entry trades (no limit logic)."""
    if not trades:
        return None
    sorted_t = sorted(trades, key=lambda r: r["entry_time"])
    bal = 1000.0
    peak = bal
    mdd = 0.0
    for r in sorted_t:
        sl = r.get("sl_pips", 0) or 10
        pnl = r.get("pnl_pips", 0) or 0
        if apply_commission:
            pnl -= commission_for_pair(r.get("pair", ""))
        rm = pnl / sl if sl > 0 else 0
        bal += bal * 0.03 * rm
        if bal > peak:
            peak = bal
        dd = (peak - bal) / peak * 100
        if dd > mdd:
            mdd = dd
    n = len(sorted_t)
    pnl_total = 0.0
    gw = 0.0
    gl = 0.0
    wins = 0
    for r in sorted_t:
        pp = (r.get("pnl_pips", 0) or 0)
        if apply_commission:
            pp -= commission_for_pair(r.get("pair", ""))
        pnl_total += pp
        if pp > 0:
            gw += pp
            wins += 1
        else:
            gl += abs(pp)
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
    )


def fmt_row(label, s, baseline=None):
    if s is None or s["n"] == 0:
        return f"  {label:<22}  (no trades)"
    extra = ""
    if baseline is not None and baseline["n"] > 0:
        d_pnl = s["pnl"] - baseline["pnl"]
        d_pf = s["pf"] - baseline["pf"]
        d_dd = s["mdd"] - baseline["mdd"]
        d_final = s["final"] - baseline["final"]
        extra = (f"   Δ pnl={d_pnl:+6.1f}p  Δ PF={d_pf:+5.2f}  "
                 f"Δ DD={d_dd:+5.1f}%  Δ$={d_final:+6.0f}")
    return (f"  {label:<22} n={s['n']:>3}({s.get('fill_rate',100):.0f}%) "
            f"WR={s['wr']:>5.1f}% P/L={s['pnl']:>+7.1f}p "
            f"PF={s['pf']:>4.2f} DD={s['mdd']:>5.1f}% "
            f"daily={s['daily']:>+6.2f}% ${s['final']:>6,.0f}{extra}")


def main():
    global LIMIT_PCT
    print(f"LIMIT-ORDER ENTRY ANALYSIS — {LIMIT_PCT*100:.0f}% of SL "
          f"closer to favorable side\n")
    print("Each trade's entry threshold = 0.08 × sl_pips. Trade fills only")
    print("if price pulled back at least that much (uses worst_pnl_pips).\n")
    print("Two interpretations tested:")
    print("  A: SL/TP relative to FILL → pnl unchanged for filled, missed dropped")
    print("  B: SL/TP fixed at signal → filled pnl boosted by 0.08×SL\n")

    all_trades = {}
    for name in FILTERS:
        trades = load_filtered(name)
        all_trades[name] = trades
        if not trades:
            print(f"\n=== {name}: NO trades after filters ===")
            continue

        base = baseline_stats(trades)
        s_a = stats(trades, "A")
        s_b = stats(trades, "B")

        print(f"\n=== {name} ===")
        print(fmt_row("BASELINE (market)", base))
        print(fmt_row("LIMIT (interp A)", s_a, base))
        print(fmt_row("LIMIT (interp B)", s_b, base))
        print(f"  → Missed {s_a['missed']} trades "
              f"({s_a['missed_winners']} winners / {s_a['missed_losers']} losers, "
              f"net {s_a['missed_pnl']:+.1f}p)")
        if s_a['missed'] > 0:
            miss_wr = s_a['missed_winners'] / s_a['missed'] * 100
            print(f"  → Missed-trade WR = {miss_wr:.1f}% "
                  f"(if > overall WR, the limit HURTS — we're missing winners)")

    # Combined: SS+ATR+B-tuned (current DTC-combo equivalent)
    print(f"\n\n{'='*72}")
    print(f"  PORTFOLIO: SS + ATR + B-tuned (DTC-combo current sources)")
    print(f"{'='*72}")
    combined = []
    for name in ("SS (B)", "ATR (C)", "B-tuned (E)"):
        for t in all_trades[name]:
            t = dict(t)
            t["_system"] = name
            combined.append(t)
    # Dedup same-pair within 120s
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

    base = baseline_stats(deduped)
    s_a = stats(deduped, "A")
    s_b = stats(deduped, "B")
    print(fmt_row("BASELINE (market)", base))
    print(fmt_row("LIMIT (interp A)", s_a, base))
    print(fmt_row("LIMIT (interp B)", s_b, base))
    print(f"  → Missed {s_a['missed']}/{len(deduped)} trades "
          f"({s_a['missed_winners']} winners / {s_a['missed_losers']} losers)")

    # Sweep: try different limit % to find the sweet spot
    print(f"\n\n{'='*72}")
    print(f"  SWEEP — what % of SL gives the best portfolio outcome?")
    print(f"{'='*72}")
    print(f"  {'Pct':>5}  {'fills':>6} {'WR':>6} {'P/L':>8} {'PF':>5} {'DD':>5} {'Daily':>7} {'Final':>8}")

    saved_pct = LIMIT_PCT
    for pct in [0.0, 0.04, 0.06, 0.08, 0.10, 0.12, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50]:
        LIMIT_PCT = pct
        s = stats(deduped, "A")
        if s is None or s["n"] == 0:
            print(f"  {pct*100:>4.0f}%  (no fills)")
            continue
        print(f"  {pct*100:>4.0f}%  "
              f"{s['fill_rate']:>5.1f}% {s['wr']:>5.1f}% "
              f"{s['pnl']:>+7.1f}p {s['pf']:>4.2f} "
              f"{s['mdd']:>4.1f}% {s['daily']:>+6.2f}% "
              f"${s['final']:>7,.0f}")
    LIMIT_PCT = saved_pct


if __name__ == "__main__":
    main()
