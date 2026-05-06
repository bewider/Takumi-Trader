"""Per-pair deep-dive report generator.

Implements PAIR_ANALYSIS_PROMPT.md spec. Usage:
    python scripts/pair_deep_dive.py AUDCAD
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
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


# System registry with current blacklists
SYSTEMS = [
    ("Sv2",      "paper_trades.json",          set()),
    ("A-tuned",  "paper_trades_a_tuned.json",  set()),
    ("SS",       "paper_trades_ss.json",       {"AUDCAD","AUDCHF","CADCHF","EURCAD","EURCHF","EURUSD","GBPAUD"}),
    ("B-tuned",  "paper_trades_b_tuned.json",  {"AUDCAD","AUDCHF","AUDJPY","EURNZD","GBPCAD","NZDUSD"}),
    ("ATR",      "paper_trades_atr.json",      {"AUDCAD","AUDCHF","EURCAD","EURNZD","EURUSD","GBPAUD"}),
    ("QM4",      "paper_trades_qm4.json",      set()),
]


def load_pair_all(pair):
    """Return dict system_name → list of net-adjusted trades for this pair."""
    out = {}
    for sys_name, file, _bl in SYSTEMS:
        try:
            recs = json.loads((DATA / file).read_text(encoding="utf-8"))
        except Exception:
            continue
        keep = []
        for r in recs:
            if not r.get("close_reason") or r.get("pair") != pair:
                continue
            if sys_name == "QM4":
                # Production QM4 only takes CUM/SELL
                if (r.get("qm4_alert_type") or "") != "CUM" or r.get("direction") != "SELL":
                    continue
            r = dict(r)
            r["_net_pnl"] = (r.get("pnl_pips", 0) or 0) - commission(pair)
            r["_net_is_win"] = r["_net_pnl"] > 0
            r["_system"] = sys_name
            keep.append(r)
        if keep:
            out[sys_name] = keep
    return out


def stats(trades):
    if not trades:
        return None
    n = len(trades)
    w = sum(1 for t in trades if t["_net_is_win"])
    p = sum(t["_net_pnl"] for t in trades)
    bal, peak, mdd = 1000, 1000, 0
    for t in sorted(trades, key=lambda x: x["entry_time"]):
        sl = t.get("sl_pips", 10) or 10
        r = t["_net_pnl"] / sl if sl > 0 else 0
        bal += bal * 0.03 * r
        if bal > peak: peak = bal
        dd = (peak - bal) / peak * 100
        if dd > mdd: mdd = dd
    gw = sum(t["_net_pnl"] for t in trades if t["_net_is_win"])
    gl = abs(sum(t["_net_pnl"] for t in trades if not t["_net_is_win"]))
    pf = gw / gl if gl > 0 else 99
    dates = set(datetime.fromtimestamp(t["entry_time"], tz=JST).date() for t in trades)
    days = max(1, len(dates))
    avg_sl = mean(t.get("sl_pips", 0) or 0 for t in trades)
    avg_r = (p/n) / avg_sl if avg_sl > 0 else 0
    daily = avg_r * (n/days) * 0.03 * 100
    return dict(
        n=n, wins=w, wr=w/n*100, pnl=p, avg_pnl=p/n,
        pf=pf, mdd=mdd, bal=bal, ret=(bal/1000-1)*100,
        days=days, tpd=n/days, daily=daily,
        avg_sl=avg_sl, avg_tp=mean(t.get("tp_pips", 0) or 0 for t in trades),
        avg_win=gw/w if w else 0,
        avg_loss=-gl/(n-w) if (n-w) else 0,
    )


def fmt_cat(label, buckets, min_n=3):
    """Print a categorical breakdown with flags."""
    print(f"  {label}:")
    print(f"    {'Category':<22} {'n':>4} {'WR':>6} {'P/L':>8} {'flag'}")
    rows = []
    for k, trades in buckets.items():
        if not trades or len(trades) < 1:
            continue
        n = len(trades)
        w = sum(1 for t in trades if t["_net_is_win"])
        p = sum(t["_net_pnl"] for t in trades)
        wr = w/n*100
        flag = ""
        if n >= min_n:
            if wr < 40:
                flag = "❌ block"
            elif wr > 75:
                flag = "✓ keep"
        rows.append((k, n, wr, p, flag))
    rows.sort(key=lambda x: x[3])
    for k, n, wr, p, flag in rows:
        print(f"    {str(k)[:22]:<22} {n:>4} {wr:>5.1f}% {p:>+7.1f}p  {flag}")


def hour_breakdown(trades):
    print(f"  Hour of day (JST):")
    print(f"    {'Hour':<7} {'n':>4} {'WR':>6} {'P/L':>8}  bars                flag")
    by_hour = defaultdict(list)
    for t in trades:
        dt = datetime.fromtimestamp(t["entry_time"], tz=JST)
        by_hour[dt.hour].append(t)
    for h in sorted(by_hour):
        ts = by_hour[h]
        n = len(ts); w = sum(1 for t in ts if t["_net_is_win"])
        p = sum(t["_net_pnl"] for t in ts)
        wr = w/n*100
        bar = "█" * int(wr/10) + "░" * (10 - int(wr/10))
        flag = ""
        if n >= 3:
            if wr < 40: flag = "❌ block"
            elif wr > 75: flag = "✓ keep"
        print(f"    {h:>2}:00   {n:>4} {wr:>5.1f}% {p:>+7.1f}p  {bar}  {flag}")


def bin15_breakdown(trades):
    bins = defaultdict(list)
    for t in trades:
        dt = datetime.fromtimestamp(t["entry_time"], tz=JST)
        b = (dt.hour * 60 + dt.minute) // 15
        bins[b].append(t)
    print(f"  15-min bins (only n>=3):")
    print(f"    {'Window':<14} {'n':>4} {'WR':>6} {'P/L':>8} flag")
    rows = []
    for b, ts in sorted(bins.items()):
        if len(ts) < 3:
            continue
        n = len(ts); w = sum(1 for t in ts if t["_net_is_win"])
        p = sum(t["_net_pnl"] for t in ts)
        wr = w/n*100
        sm = b*15; em = sm+15
        win = f"{sm//60:02d}:{sm%60:02d}-{em//60:02d}:{em%60:02d}"
        flag = ""
        if wr < 40: flag = "❌ block"
        elif wr > 75: flag = "✓ keep"
        rows.append((win, n, wr, p, flag))
    rows.sort(key=lambda x: x[3])
    for win, n, wr, p, flag in rows:
        print(f"    {win:<14} {n:>4} {wr:>5.1f}% {p:>+7.1f}p  {flag}")


def feature_compare(wins, losses, field, fmt=".2f"):
    wv = [t.get(field) for t in wins if isinstance(t.get(field), (int, float))]
    lv = [t.get(field) for t in losses if isinstance(t.get(field), (int, float))]
    if not wv or not lv:
        return None, None, None, ""
    wm, lm = mean(wv), mean(lv)
    gap = wm - lm
    flag = ""
    if abs(gap) > 0:
        try:
            all_vals = wv + lv
            sd = stdev(all_vals) if len(all_vals) > 1 else 0
            if sd > 0 and abs(gap) / sd > 0.3:
                flag = " ★"
            elif max(abs(wm), abs(lm)) > 0 and abs(gap) / max(abs(wm), abs(lm)) > 0.25:
                flag = " ★"
        except Exception:
            pass
    return wm, lm, gap, flag


def bool_compare(wins, losses, field):
    """% TRUE for boolean field."""
    wt = sum(1 for t in wins if t.get(field))
    lt = sum(1 for t in losses if t.get(field))
    wn, ln = len(wins), len(losses)
    if not wn or not ln:
        return None, None, None, ""
    wp = wt/wn*100; lp = lt/ln*100
    gap = wp - lp
    flag = " ★" if abs(gap) >= 20 else ""
    return wp, lp, gap, flag


def format_ctx(t):
    """Format chart-context line for a trade."""
    bits = []
    if t.get("entry_ctx_h1_trend_slope_pips_per_bar") is not None:
        v = t["entry_ctx_h1_trend_slope_pips_per_bar"]
        if v != 0:
            bits.append(f"H1slope={v:+.2f}")
    if t.get("entry_ctx_h1_atr_ratio"):
        bits.append(f"H1atr_r={t['entry_ctx_h1_atr_ratio']:.2f}")
    if t.get("entry_ctx_h1_trend_aligned"):
        bits.append("H1align=T")
    if t.get("entry_ctx_h4_trend_aligned"):
        bits.append("H4align=T")
    if t.get("entry_ctx_d1_range_consumed_pct"):
        bits.append(f"D1range={t['entry_ctx_d1_range_consumed_pct']:.0f}%")
    if t.get("entry_ctx_d1_dist_to_today_open_pips"):
        bits.append(f"D1dist={t['entry_ctx_d1_dist_to_today_open_pips']:+.1f}p")
    if t.get("entry_ctx_entering_into_resistance"):
        bits.append("into_R")
    if t.get("entry_ctx_entering_into_support"):
        bits.append("into_S")
    return " | ".join(bits) if bits else "(no context data)"


def narrative(t, is_loss=True):
    """Plain-English interpretation of what happened."""
    parts = []
    d = t.get("direction", "?")
    sl = t.get("sl_pips", 0)
    mae = t.get("worst_pnl_pips", 0)
    mfe = t.get("peak_pnl_pips", 0)
    wpf = t.get("went_profit_first", False)
    pnl = t["_net_pnl"]

    if is_loss:
        if abs(mae) > sl * 1.2:
            parts.append(f"stop blew through by {abs(mae)-sl:.1f}p (slippage/gap)")
        if wpf:
            parts.append(f"went +{mfe:.1f}p before reverting")
        elif mfe > 3:
            parts.append(f"touched +{mfe:.1f}p briefly")
        else:
            parts.append("never went into profit")
        # Chart context cues
        adr = t.get("adr_consumed_pct")
        if adr and adr > 70:
            parts.append(f"ADR exhausted ({adr:.0f}%)")
        if t.get("entry_ctx_h1_trend_aligned") is False:
            parts.append("traded against H1 trend")
        if t.get("entry_ctx_d1_range_consumed_pct") and t["entry_ctx_d1_range_consumed_pct"] < 50:
            parts.append("chop day (narrow D1 range)")
    else:
        if not wpf and mae < -3:
            parts.append(f"survived {abs(mae):.1f}p drawdown before winning")
        if mfe > t.get("tp_pips", 10) * 1.5:
            parts.append(f"big runner (+{mfe:.1f}p peak)")
        if t.get("entry_ctx_h1_trend_aligned") and t.get("entry_ctx_h4_trend_aligned"):
            parts.append("aligned with H1+H4 trend")

    return " — ".join(parts) if parts else f"pnl {pnl:+.1f}p"


def session_of(t):
    return t.get("session") or "(unknown)"


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/pair_deep_dive.py <PAIR>")
        return 1
    pair = sys.argv[1].upper()

    print(f"{'='*82}")
    print(f"  PAIR DEEP-DIVE: {pair}")
    print(f"  Run date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*82}\n")

    by_system = load_pair_all(pair)
    if not by_system:
        print(f"  No trades found for {pair}. Exiting.")
        return 1

    # All trades combined
    all_trades = []
    for ts in by_system.values():
        all_trades.extend(ts)

    total_n = len(all_trades)
    if total_n < 5:
        print(f"  ABORT: only {total_n} closed trades. Not enough data.")
        return 0

    dates = sorted(set(datetime.fromtimestamp(t["entry_time"], tz=JST).date()
                       for t in all_trades))
    print(f"  Total trades across all systems: {total_n}")
    print(f"  Date range: {dates[0]} → {dates[-1]} ({len(dates)} trading days)")
    print()

    # ────────────────────────────────────────────────────────────────
    # 1. CROSS-SYSTEM SCORECARD
    # ────────────────────────────────────────────────────────────────
    print(f"{'#'*82}")
    print(f"  1. CROSS-SYSTEM SCORECARD")
    print(f"{'#'*82}\n")

    bl_map = {s[0]: s[2] for s in SYSTEMS}
    rows = []
    for sys_name, trades in by_system.items():
        s = stats(trades)
        is_bl = pair in bl_map.get(sys_name, set())
        rows.append((sys_name, s, is_bl))
    rows.sort(key=lambda x: -x[1]["pnl"])

    print(f"  {'System':<10} {'n':>4} {'WR':>6} {'P/L':>9} {'PF':>5} "
          f"{'MDD':>6} {'daily':>7} {'Final$':>9}  flag")
    for sys_name, s, is_bl in rows:
        bl_flag = "[BL]" if is_bl else ""
        print(f"  {sys_name:<10} {s['n']:>4} {s['wr']:>5.1f}% {s['pnl']:>+7.1f}p "
              f"{s['pf']:>4.2f} {s['mdd']:>5.1f}% {s['daily']:>+6.2f}% "
              f"${s['bal']:>7,.0f}  {bl_flag}")

    # ────────────────────────────────────────────────────────────────
    # 2. DIRECTION + TIME PROFILE
    # ────────────────────────────────────────────────────────────────
    print(f"\n\n{'#'*82}")
    print(f"  2. DIRECTION + TIME PROFILE  (all systems combined)")
    print(f"{'#'*82}\n")

    by_dir = defaultdict(list)
    for t in all_trades:
        by_dir[t.get("direction", "?")].append(t)
    fmt_cat("Direction", by_dir, min_n=3)

    print()
    hour_breakdown(all_trades)

    print()
    bin15_breakdown(all_trades)

    print()
    by_dow = defaultdict(list)
    dnames = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    for t in all_trades:
        dow = dnames[datetime.fromtimestamp(t["entry_time"], tz=JST).weekday()]
        by_dow[dow].append(t)
    fmt_cat("Day of week", by_dow)

    print()
    by_sess = defaultdict(list)
    for t in all_trades:
        by_sess[session_of(t)].append(t)
    fmt_cat("Session", by_sess)

    # ────────────────────────────────────────────────────────────────
    # 3. SL/TP & R:R DIAGNOSIS
    # ────────────────────────────────────────────────────────────────
    print(f"\n\n{'#'*82}")
    print(f"  3. SL/TP & R:R DIAGNOSIS")
    print(f"{'#'*82}\n")

    overall = stats(all_trades)
    tpsl = overall["avg_tp"] / overall["avg_sl"] if overall["avg_sl"] > 0 else 0
    # Break-even WR: probability needed where expected value = 0
    # WR * avg_win - (1-WR) * |avg_loss| - commission = 0
    # (considering commission already subtracted from both)
    be_wr = overall["avg_sl"] / (overall["avg_sl"] + overall["avg_tp"]) * 100 if overall["avg_tp"] > 0 else 50
    edge = overall["wr"] - be_wr

    print(f"  Average SL:       {overall['avg_sl']:.1f}p")
    print(f"  Average TP:       {overall['avg_tp']:.1f}p")
    print(f"  R:R ratio:        1:{tpsl:.2f}")
    print(f"  Break-even WR:    {be_wr:.1f}%  (given this R:R)")
    print(f"  Actual WR:        {overall['wr']:.1f}%")
    print(f"  Edge gap:         {edge:+.1f}pp  "
          f"{'(DEFICIT — pair loses money)' if edge < 0 else '(SURPLUS)'}")
    print()
    print(f"  Avg win:          {overall['avg_win']:+.1f}p")
    print(f"  Avg loss:         {overall['avg_loss']:+.1f}p")
    ratio = abs(overall['avg_loss']) / overall['avg_win'] if overall['avg_win'] > 0 else 0
    print(f"  Loss/Win ratio:   {ratio:.2f}x  "
          f"{'(⚠️ losses much bigger than wins — stops may be slipping)' if ratio > 2.0 else '(OK)'}")

    # ────────────────────────────────────────────────────────────────
    # 4. CHART-CONTEXT PATTERN ANALYSIS
    # ────────────────────────────────────────────────────────────────
    print(f"\n\n{'#'*82}")
    print(f"  4. CHART-CONTEXT PATTERN ANALYSIS")
    print(f"{'#'*82}\n")

    wins = [t for t in all_trades if t["_net_is_win"]]
    losses = [t for t in all_trades if not t["_net_is_win"]]

    # Check population
    ctx_fields = [
        "entry_ctx_h1_trend_slope_pips_per_bar",
        "entry_ctx_h1_atr_ratio",
        "entry_ctx_h4_trend_slope_pips_per_bar",
        "entry_ctx_d1_range_consumed_pct",
        "entry_ctx_d1_dist_to_today_open_pips",
        "entry_ctx_m15_range_expansion_ratio",
        "entry_ctx_m15_last_bar_body_ratio",
    ]
    populated = sum(1 for t in all_trades if any(t.get(f) for f in ctx_fields))
    pct_pop = populated/total_n*100 if total_n else 0
    print(f"  Chart-context populated on {populated}/{total_n} trades ({pct_pop:.0f}%)")
    if pct_pop < 30:
        print(f"  ⚠️  Most trades predate this feature. Results are from trades after 2026-04-18.")
    print()

    print(f"  Numerical feature distributions (★ = significant gap):")
    numerical_fields = [
        ("entry_ctx_h1_trend_slope_pips_per_bar", "H1 trend slope"),
        ("entry_ctx_h1_atr_ratio",                "H1 ATR ratio (expand>1)"),
        ("entry_ctx_h4_trend_slope_pips_per_bar", "H4 trend slope"),
        ("entry_ctx_d1_range_consumed_pct",       "D1 range consumed %"),
        ("entry_ctx_d1_dist_to_today_open_pips",  "D1 dist from open"),
        ("entry_ctx_m15_range_expansion_ratio",   "M15 range expansion"),
        ("entry_ctx_m15_last_bar_body_ratio",     "M15 last bar body"),
        # legacy fields too
        ("entry_h1_atr_pips",                     "H1 ATR pips (legacy)"),
        ("entry_tick_volume_ratio",               "Tick volume ratio"),
        ("adr_consumed_pct",                      "ADR consumed %"),
        ("entry_conv_trend",                      "Conv Trend score"),
        ("entry_momentum_buildup_sec",            "Momentum buildup (s)"),
    ]
    for field, label in numerical_fields:
        wm, lm, gap, flag = feature_compare(wins, losses, field)
        if wm is None:
            continue
        print(f"    {label:<28} W={wm:>7.2f}  L={lm:>7.2f}  gap={gap:>+7.2f}{flag}")

    print(f"\n  Boolean features (% TRUE):")
    for field, label in [
        ("entry_ctx_h1_trend_aligned",      "H1 trend aligned"),
        ("entry_ctx_h4_trend_aligned",      "H4 trend aligned"),
        ("entry_ctx_m15_last_bar_aligned",  "M15 last bar aligned"),
        ("entry_ctx_entering_into_resistance", "Into resistance (BUY)"),
        ("entry_ctx_entering_into_support",    "Into support (SELL)"),
        ("went_profit_first",               "Went profit first"),
    ]:
        wp, lp, gap, flag = bool_compare(wins, losses, field)
        if wp is None:
            continue
        print(f"    {label:<28} W={wp:>5.1f}%  L={lp:>5.1f}%  gap={gap:>+5.1f}pp{flag}")

    # ────────────────────────────────────────────────────────────────
    # 5. WORST 5 LOSSES — DETAILED
    # ────────────────────────────────────────────────────────────────
    print(f"\n\n{'#'*82}")
    print(f"  5. WORST 5 LOSSES — DETAILED")
    print(f"{'#'*82}\n")

    worst = sorted(losses, key=lambda t: t["_net_pnl"])[:5]
    for t in worst:
        dt = datetime.fromtimestamp(t["entry_time"], tz=JST).strftime("%m-%d %H:%M")
        print(f"  {dt}  {t.get('direction'):<4} {t.get('_system'):<8} "
              f"pnl={t['_net_pnl']:>+6.1f}p  (MAE={t.get('worst_pnl_pips', 0):.1f}, "
              f"MFE={t.get('peak_pnl_pips', 0):.1f}, "
              f"went_pf={t.get('went_profit_first', False)})")
        print(f"      Ctx: {format_ctx(t)}")
        print(f"      → {narrative(t, is_loss=True)}")
        print()

    # ────────────────────────────────────────────────────────────────
    # 6. BEST 5 WINS — DETAILED
    # ────────────────────────────────────────────────────────────────
    print(f"\n{'#'*82}")
    print(f"  6. BEST 5 WINS — DETAILED")
    print(f"{'#'*82}\n")

    best = sorted(wins, key=lambda t: -t["_net_pnl"])[:5]
    for t in best:
        dt = datetime.fromtimestamp(t["entry_time"], tz=JST).strftime("%m-%d %H:%M")
        print(f"  {dt}  {t.get('direction'):<4} {t.get('_system'):<8} "
              f"pnl={t['_net_pnl']:>+6.1f}p  (MAE={t.get('worst_pnl_pips', 0):.1f}, "
              f"MFE={t.get('peak_pnl_pips', 0):.1f}, "
              f"went_pf={t.get('went_profit_first', False)})")
        print(f"      Ctx: {format_ctx(t)}")
        print(f"      → {narrative(t, is_loss=False)}")
        print()


if __name__ == "__main__":
    sys.exit(main())
