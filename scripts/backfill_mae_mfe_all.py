"""Backfill Peak/Worst + 4H MAX-MFE/MAX-MAE across ALL 9 systems.

Unlike the older `backfill_paper_trades.py`, this one:
  - Covers all 9 paper trade journals (A-tuned and B-tuned were missed)
  - FORCE-overwrites bogus zero-valued post_close data for closed trades
    that were marked complete prematurely (the bug that just got fixed)
  - Only updates trades where the 4H post-close window has fully elapsed

Run while TAKUMI is stopped.
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import MetaTrader5 as mt5

DATA = Path(r"D:\Trading\TAKUMI Trader\data")

JOURNALS = [
    ("Sv2",        "paper_trades.json"),
    ("SS",         "paper_trades_ss.json"),
    ("ATR",        "paper_trades_atr.json"),
    ("QM4",        "paper_trades_qm4.json"),
    ("A-tuned",    "paper_trades_a_tuned.json"),
    ("B-tuned",    "paper_trades_b_tuned.json"),
    ("Breakout",   "paper_trades_breakout.json"),
    ("Squeeze",    "paper_trades_squeeze.json"),
    ("Divergence", "paper_trades_divergence.json"),
]


def pip_value(pair: str) -> float:
    return 0.01 if "JPY" in pair else 0.0001


def needs_backfill(trade: dict) -> bool:
    """Return True if this closed trade has suspicious zero 4H data."""
    if not trade.get("close_reason"):
        return False  # still open
    close_time = trade.get("close_time", 0)
    if close_time <= 0:
        return False  # no close_time known
    elapsed_h = (time.time() - close_time) / 3600.0
    if elapsed_h < 4.0:
        return False  # 4h window not elapsed yet

    # Check if any of the long-term metrics are zero (suspicious)
    mfe = trade.get("post_close_max_mfe_pips", 0)
    mae = trade.get("post_close_max_mae_pips", 0)
    end = trade.get("post_close_final_pips", 0)
    # If all 3 are exactly 0, almost certainly incomplete tracking
    if mfe == 0 and mae == 0 and end == 0:
        return True
    return False


def needs_peak_worst(trade: dict) -> bool:
    """Check if peak/worst are missing (both exactly 0 is suspicious)."""
    if not trade.get("close_reason"):
        return False
    peak = trade.get("peak_pnl_pips", 0)
    worst = trade.get("worst_pnl_pips", 0)
    return peak == 0 and worst == 0


def backfill_trade(trade: dict) -> bool:
    """Compute missing values from MT5 historical candles. Returns True if updated."""
    pair = trade.get("pair", "")
    direction = trade.get("direction", "")
    entry_price = trade.get("entry_price", 0.0)
    entry_time = trade.get("entry_time", 0.0)
    close_time = trade.get("close_time", 0.0)

    if not pair or not direction or entry_price <= 0 or entry_time <= 0:
        return False

    pip = pip_value(pair)

    # Fetch M1 bars from entry through 4h post-close
    start_dt = datetime.fromtimestamp(entry_time, tz=timezone.utc) - timedelta(minutes=1)
    end_dt = datetime.fromtimestamp(close_time, tz=timezone.utc) + timedelta(hours=4, minutes=5)

    bars = mt5.copy_rates_range(pair, mt5.TIMEFRAME_M1, start_dt, end_dt)
    if bars is None or len(bars) < 2:
        return False

    # Phase 1: entry → close (peak/worst)
    peak = 0.0
    worst = 0.0
    close_bar_time = int(close_time)
    for bar in bars:
        bt = int(bar["time"])
        if bt < int(entry_time):
            continue
        if bt > close_bar_time:
            break
        h = float(bar["high"])
        l = float(bar["low"])
        if direction == "BUY":
            best = (h - entry_price) / pip
            worst_p = (l - entry_price) / pip
        else:
            best = (entry_price - l) / pip
            worst_p = (entry_price - h) / pip
        if best > peak:
            peak = best
        if worst_p < worst:
            worst = worst_p

    # Phase 2: close → close+4h (post-close MFE/MAE/END)
    post_mfe = 0.0
    post_mae = 0.0
    post_final = 0.0
    post_complete = False
    post_end_ts = close_time + 4 * 3600
    exit_pnl = trade.get("pnl_pips", 0.0)

    for bar in bars:
        bt = int(bar["time"])
        if bt <= close_bar_time:
            continue
        if bt > post_end_ts:
            break
        h = float(bar["high"])
        l = float(bar["low"])
        c = float(bar["close"])
        if direction == "BUY":
            cur = (c - entry_price) / pip
            best = (h - entry_price) / pip
            worst_p = (l - entry_price) / pip
        else:
            cur = (entry_price - c) / pip
            best = (entry_price - l) / pip
            worst_p = (entry_price - h) / pip
        # Post-close MFE/MAE relative to exit
        post_move = best - exit_pnl
        post_adv = worst_p - exit_pnl
        if post_move > post_mfe:
            post_mfe = post_move
        if post_adv < -post_mae:
            post_mae = abs(post_adv)
        post_final = cur

    # Did our bar range actually cover the full 4h window?
    if len(bars) > 0:
        last_bar_time = int(bars[-1]["time"])
        if last_bar_time >= post_end_ts - 60:
            post_complete = True

    # Update the trade record — always overwrite
    changed = False
    if abs(peak) > 0.01 or abs(worst) > 0.01:
        trade["peak_pnl_pips"] = round(peak, 1)
        trade["worst_pnl_pips"] = round(worst, 1)
        changed = True
    if post_complete:
        trade["post_close_max_mfe_pips"] = round(post_mfe, 1)
        trade["post_close_max_mae_pips"] = round(post_mae, 1)
        trade["post_close_final_pips"] = round(post_final, 1)
        trade["post_close_complete"] = True
        trade["post_close_minutes"] = round((time.time() - close_time) / 60.0, 1)
        changed = True

    return changed


def main() -> int:
    print("Backfilling Peak/Worst + 4H MFE/MAE across all 9 systems")
    print("=" * 70)

    if not mt5.initialize():
        print("ERROR: MT5 not connected")
        return 1

    info = mt5.account_info()
    print(f"MT5 connected: {info.server if info else 'unknown'}")
    print()

    total_updated = 0
    total_scanned = 0
    for name, fname in JOURNALS:
        path = DATA / fname
        if not path.exists():
            print(f"  [{name:<11}] skipping — file not found")
            continue

        data = json.loads(path.read_text(encoding="utf-8"))
        n_total = len(data)
        n_need = 0
        n_updated = 0
        for i, trade in enumerate(data):
            needs_p = needs_peak_worst(trade)
            needs_4h = needs_backfill(trade)
            if not needs_p and not needs_4h:
                continue
            n_need += 1
            if backfill_trade(trade):
                n_updated += 1
            if n_need % 25 == 0:
                print(f"    {name:<11} processed {n_need}...")

        if n_updated > 0:
            path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

        total_scanned += n_total
        total_updated += n_updated
        print(f"  [{name:<11}] {n_total:>4} trades, {n_need:>3} needed backfill, {n_updated:>3} updated")

    mt5.shutdown()
    print()
    print("=" * 70)
    print(f"DONE — {total_updated}/{total_scanned} trades updated across all systems")
    return 0


if __name__ == "__main__":
    sys.exit(main())
