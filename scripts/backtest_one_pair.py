"""Validate Sv2 backtester against live paper history for ONE pair.

Runs the existing BacktestEngine on Dukascopy M1 data with settings that
match LIVE TAKUMI exactly. Then loads the live paper_trades.json for the
same pair + date range and compares trade-by-trade to verify the backtest
reproduces what live trading actually did.

This is a regression test: if the backtest output is close to live, we
trust the backtester for what-if scenarios. If they diverge significantly,
either the backtester is misconfigured or the live system has hidden
inputs we haven't accounted for.

Usage:
    python scripts/backtest_one_pair.py GBPCAD
"""
from __future__ import annotations

import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

# Add repo root to import path so `takumi_trader` resolves regardless of CWD
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Quiet the backtester's noisy info logs
logging.basicConfig(level=logging.WARNING, format="%(message)s")

from takumi_trader.core.backtester import BacktestConfig, BacktestEngine

DATA = Path(r"D:\Trading\TAKUMI Trader\data")
JST = timezone(timedelta(hours=9))


def load_live_trades(pair: str, start_ts: float, end_ts: float):
    """Load Sv2 live paper trades for the pair within the date range."""
    recs = json.loads((DATA / "paper_trades.json").read_text(encoding="utf-8"))
    out = []
    for r in recs:
        if r.get("pair") != pair:
            continue
        if not r.get("close_reason"):
            continue
        et = r.get("entry_time", 0)
        if not (start_ts <= et < end_ts):
            continue
        out.append(r)
    return sorted(out, key=lambda r: r["entry_time"])


def fmt_dt(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=JST).strftime("%m-%d %H:%M JST")


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/backtest_one_pair.py <PAIR>")
        return 1
    pair = sys.argv[1].upper()

    # ── Choose data source: env var DATA_SRC=mt5 uses MT5 history ──
    import os
    data_src = os.environ.get("DATA_SRC", "dukascopy").lower()

    if data_src == "mt5":
        # Probe MT5 history to determine the achievable date range
        import MetaTrader5 as mt5
        if not mt5.initialize():
            print(f"MT5 init failed: {mt5.last_error()}")
            return 1
        mt5.symbol_select(pair, True)
        import time as _t; _t.sleep(0.3)
        bars = mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_M1, 0, 30000)
        mt5.shutdown()
        if bars is None or len(bars) == 0:
            print(f"MT5 has no M1 data for {pair}")
            return 1
        duk_start = datetime.fromtimestamp(int(bars[0]["time"]), tz=timezone.utc)
        duk_end = datetime.fromtimestamp(int(bars[-1]["time"]), tz=timezone.utc)
        print(f"MT5 {pair}: {len(bars):,} M1 bars  span {duk_start} → {duk_end}")
    else:
        from takumi_trader.core.dukascopy_downloader import DukascopyDownloader
        dl = DukascopyDownloader(DATA / "dukascopy")
        arr = dl.load_pair(pair)
        if arr is None or len(arr) == 0:
            print(f"No Dukascopy data for {pair}")
            return 1
        duk_start = datetime.fromtimestamp(int(arr["time"][0]), tz=timezone.utc)
        duk_end = datetime.fromtimestamp(int(arr["time"][-1]), tz=timezone.utc)
        print(f"Dukascopy {pair}: {len(arr):,} M1 bars  span {duk_start} → {duk_end}")
    print()

    # ── Configure backtester to match LIVE TAKUMI ──
    # Settings PULLED FROM LIVE QSETTINGS (the real live values, not defaults):
    #   filters/trend_regime = False   ← user disabled HTF
    #   filters/velocity     = True
    #   filters/isolation    = True
    #   filters/structural   = True
    #   filters/conv_full    = 50      ← user lowered from default 70
    #   filters/conv_dimmed  = 25
    # Plus the post-filter chain that lives in main_window.py:
    #   - MIN_DIVERGENCE_SPREAD = 12.0 (composite spread filter)
    #   - alert_mgr cooldown = 60s
    #   - 7:58–22:00 JST trading window
    cfg = BacktestConfig(
        start_date=duk_start.strftime("%Y-%m-%d"),
        use_dukascopy=(data_src != "mt5"),
        single_pair=pair,
        simulate_sltp=True,
        filter_no_trade_session=True,
        filter_news=True,
        filter_htf=False,           # ← live has trend_regime DISABLED
        filter_vel=True,
        filter_isol=True,
        filter_structural=True,
        conviction_threshold=50,    # ← matches live's actual conv_full
        allow_session_reentry=True,
        use_accel_entry=False,
    )

    # ── Run the backtest ──
    print(f"Running BacktestEngine for {pair} with live-equivalent settings...")
    engine = BacktestEngine(config=cfg, progress_callback=None)
    bt_outcomes_raw = engine.run()
    print(f"  → backtest completed: {len(bt_outcomes_raw)} raw outcomes")

    # ── Post-filter chain mirroring live main_window.py ──
    # Live applies these AFTER conviction passes; the BT engine doesn't
    # apply them itself, so we replicate them here.

    # Allow runtime override via env var for diagnostic testing
    import os
    MIN_DIVERGENCE_SPREAD = float(os.environ.get("BT_MIN_SPREAD", "12.0"))
    ALERT_COOLDOWN_SEC = int(os.environ.get("BT_COOLDOWN", "60"))

    # Step 1: trading-window filter (7:58–22:00 JST)
    after_window = []
    for o in bt_outcomes_raw:
        dt = datetime.fromtimestamp(o.entry_time, tz=JST)
        mins = dt.hour * 60 + dt.minute
        if 478 <= mins < 1320:
            after_window.append(o)
    print(f"  → trading window 7:58-22:00 JST: "
          f"kept {len(after_window)}/{len(bt_outcomes_raw)}")

    # Step 2: composite-spread filter (>= 12.0)
    after_spread = [o for o in after_window
                    if (o.strength_spread or 0) >= MIN_DIVERGENCE_SPREAD]
    print(f"  → spread filter (>={MIN_DIVERGENCE_SPREAD}): "
          f"kept {len(after_spread)}/{len(after_window)}")

    # Step 3: alert-manager 60-second per-pair cooldown
    # (BT doesn't apply this; live's AlertManager debounces re-firing
    # on same pair within 60s)
    by_time = sorted(after_spread, key=lambda o: o.entry_time)
    last_fire: dict[str, float] = {}
    after_cooldown = []
    for o in by_time:
        last = last_fire.get(o.pair, 0)
        if o.entry_time - last >= ALERT_COOLDOWN_SEC:
            after_cooldown.append(o)
            last_fire[o.pair] = o.entry_time
    print(f"  → 60s cooldown dedup: kept {len(after_cooldown)}/{len(after_spread)}")

    bt_outcomes = after_cooldown
    print(f"  → FINAL backtest signals after live-equivalent filters: "
          f"{len(bt_outcomes)}")
    print()

    # ── Load live trades from the same window ──
    live = load_live_trades(pair, duk_start.timestamp(), duk_end.timestamp() + 86400)
    print(f"Live Sv2 paper trades for {pair} in the same window: {len(live)}")
    print()

    # ── Compare ──
    print(f"{'='*82}")
    print(f"  HEAD-TO-HEAD: Backtest vs Live for {pair}")
    print(f"{'='*82}")

    # Stats overview
    bt_n = len(bt_outcomes)
    live_n = len(live)
    bt_wins = sum(1 for o in bt_outcomes if (o.final_pnl_pips or 0) > 0)
    live_wins = sum(1 for r in live if r.get("is_win"))
    bt_pnl = sum(o.final_pnl_pips for o in bt_outcomes)
    live_pnl = sum(r.get("pnl_pips", 0) for r in live)
    bt_buys = sum(1 for o in bt_outcomes if o.direction == "BUY")
    live_buys = sum(1 for r in live if r.get("direction") == "BUY")

    print(f"\n  Trade count:    backtest={bt_n}  live={live_n}  Δ={bt_n-live_n:+d}")
    print(f"  Wins:           backtest={bt_wins}  live={live_wins}")
    print(f"  WR:             backtest={bt_wins/bt_n*100 if bt_n else 0:.1f}%  "
          f"live={live_wins/live_n*100 if live_n else 0:.1f}%")
    print(f"  Total P/L:      backtest={bt_pnl:+.1f}p  live={live_pnl:+.1f}p")
    print(f"  Direction split: backtest BUY={bt_buys}/SELL={bt_n-bt_buys}  "
          f"live BUY={live_buys}/SELL={live_n-live_buys}")

    # Per-day comparison
    print(f"\n  Trades per day (JST):")
    print(f"    {'Date':<12} {'BT':>4} {'Live':>5} {'Δ':>4}")
    bt_per_day = defaultdict(int)
    live_per_day = defaultdict(int)
    for o in bt_outcomes:
        d = datetime.fromtimestamp(o.entry_time, tz=JST).date()
        bt_per_day[d] += 1
    for r in live:
        d = datetime.fromtimestamp(r["entry_time"], tz=JST).date()
        live_per_day[d] += 1
    all_days = sorted(set(list(bt_per_day) + list(live_per_day)))
    for d in all_days:
        b = bt_per_day.get(d, 0); l = live_per_day.get(d, 0)
        flag = "" if b == l else (" ← BT extra" if b > l else " ← live extra")
        print(f"    {str(d):<12} {b:>4} {l:>5} {b-l:>+4}{flag}")

    # ── Trade-level matching ──
    # For each LIVE trade, find the closest backtest entry within ±5 min
    # on the same direction. Report match / no-match.
    print(f"\n  Trade-level matching (live → nearest BT entry, ±5 min, same direction):")
    print(f"    {'Live entry':<22} {'Live dir':<5} {'BT match':<22} {'gap':>7}")
    matches = 0
    for r in live:
        live_ts = r["entry_time"]
        live_dir = r.get("direction")
        # Find closest BT outcome
        candidates = [
            o for o in bt_outcomes
            if o.direction == live_dir
            and abs(o.entry_time - live_ts) <= 300  # 5 min window
        ]
        if candidates:
            closest = min(candidates, key=lambda o: abs(o.entry_time - live_ts))
            gap = closest.entry_time - live_ts
            print(f"    {fmt_dt(live_ts):<22} {live_dir:<5} "
                  f"{fmt_dt(closest.entry_time):<22} {gap:>+5.0f}s ✓")
            matches += 1
        else:
            print(f"    {fmt_dt(live_ts):<22} {live_dir:<5} "
                  f"{'(no BT entry within 5m)':<22}        ❌")

    print(f"\n  Match rate: {matches}/{live_n} ({matches/live_n*100 if live_n else 0:.0f}%)")

    # Backtest-only entries (BT fired but live didn't)
    bt_only = []
    for o in bt_outcomes:
        live_match = [r for r in live
                      if r.get("direction") == o.direction
                      and abs(r["entry_time"] - o.entry_time) <= 300]
        if not live_match:
            bt_only.append(o)
    print(f"\n  Backtest-only entries (BT fired, no live counterpart): {len(bt_only)}")
    for o in bt_only[:10]:
        print(f"    {fmt_dt(o.entry_time):<22} {o.direction:<5} "
              f"final_pnl={o.final_pnl_pips:+.1f}p  conviction={o.conviction_score}")
    if len(bt_only) > 10:
        print(f"    ... and {len(bt_only)-10} more")

    # ── Diagnosis ──
    print(f"\n{'='*82}")
    print(f"  DIAGNOSIS")
    print(f"{'='*82}")
    if live_n == 0:
        print("  No live trades for this pair in the window — comparison N/A.")
    else:
        match_pct = matches / live_n * 100
        if match_pct >= 80:
            print(f"  ✓ STRONG MATCH ({match_pct:.0f}%) — backtester reproduces live signals reliably.")
        elif match_pct >= 50:
            print(f"  ↔ PARTIAL MATCH ({match_pct:.0f}%) — many live trades found in BT,")
            print(f"    but timing or filtering differs. Check filter settings.")
        else:
            print(f"  ✗ WEAK MATCH ({match_pct:.0f}%) — backtester is firing different signals.")
            print(f"    Likely causes:")
            print(f"      - Different conviction threshold or filter toggles")
            print(f"      - Live used MT5 data, backtest uses Dukascopy (slight price diffs)")
            print(f"      - Time-zone offset in NO_TRADE filter")
            print(f"      - Backtester's _resample_ohlc differs from live MT5 bars")

    if bt_n > live_n * 1.5:
        print(f"\n  Note: backtest fired {bt_n-live_n} extra trades. Likely the live")
        print(f"  system had additional filters not yet matched in BacktestConfig.")
    elif bt_n < live_n * 0.5:
        print(f"\n  Note: backtest missed {live_n-bt_n} live trades. Likely a stricter")
        print(f"  filter is on in BT or warmup period excluded early signals.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
