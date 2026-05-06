"""Backfill Peak, Worst, 4H MFE, 4H MAE, 4H END for past paper trades.

Reads all paper trade journals, fetches M1 bars from MT5, and computes
the missing values from actual price history.

Usage: python scripts/backfill_paper_trades.py
"""

import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import MetaTrader5 as mt5


def pip_value(pair: str) -> float:
    return 0.01 if "JPY" in pair else 0.0001


def backfill_journal(journal_path: Path) -> int:
    """Backfill a single journal file. Returns number of trades updated."""
    if not journal_path.exists():
        print(f"  {journal_path.name}: not found")
        return 0

    data = json.loads(journal_path.read_text(encoding="utf-8"))
    print(f"  {journal_path.name}: {len(data)} trades")

    updated = 0
    for i, trade in enumerate(data):
        pair = trade.get("pair", "")
        direction = trade.get("direction", "")
        entry_price = trade.get("entry_price", 0.0)
        entry_time = trade.get("entry_time", 0.0)
        close_time = trade.get("close_time", 0.0)

        if not pair or not direction or entry_price <= 0 or entry_time <= 0:
            continue

        pip = pip_value(pair)

        # Fetch M1 bars from entry to close + 4 hours
        from datetime import timezone
        start_dt = datetime.fromtimestamp(entry_time, tz=timezone.utc) - timedelta(minutes=1)
        end_time = close_time if close_time > 0 else entry_time + 3600
        end_dt = datetime.fromtimestamp(end_time, tz=timezone.utc) + timedelta(hours=4, minutes=5)

        bars = mt5.copy_rates_range(pair, mt5.TIMEFRAME_M1, start_dt, end_dt)
        if bars is None or len(bars) < 2:
            continue

        # Phase 1: Entry → Close (Peak/Worst)
        close_bar_time = int(close_time) if close_time > 0 else int(entry_time + 3600)
        peak = 0.0
        worst = 0.0

        for bar in bars:
            bar_time = int(bar["time"])
            if bar_time < int(entry_time):
                continue
            if bar_time > close_bar_time:
                break

            h = float(bar["high"])
            l = float(bar["low"])

            if direction == "BUY":
                best_pnl = (h - entry_price) / pip
                worst_pnl = (l - entry_price) / pip
            else:
                best_pnl = (entry_price - l) / pip
                worst_pnl = (entry_price - h) / pip

            if best_pnl > peak:
                peak = best_pnl
            if worst_pnl < worst:
                worst = worst_pnl

        # Phase 2: Post-close 4H observation
        post_mfe = 0.0
        post_mae = 0.0
        post_final = 0.0
        post_complete = False

        if close_time > 0:
            post_end = close_time + 4 * 3600
            exit_pnl = trade.get("pnl_pips", 0.0)

            for bar in bars:
                bar_time = int(bar["time"])
                if bar_time <= close_bar_time:
                    continue
                if bar_time > post_end:
                    break

                h = float(bar["high"])
                l = float(bar["low"])
                c = float(bar["close"])

                if direction == "BUY":
                    current_pnl = (c - entry_price) / pip
                    best_pnl = (h - entry_price) / pip
                    worst_pnl = (l - entry_price) / pip
                else:
                    current_pnl = (entry_price - c) / pip
                    best_pnl = (entry_price - l) / pip
                    worst_pnl = (entry_price - h) / pip

                # Post-exit MFE/MAE (relative to exit, in original direction)
                post_move = best_pnl - exit_pnl
                post_adverse = worst_pnl - exit_pnl
                if post_move > post_mfe:
                    post_mfe = post_move
                if post_adverse < -post_mae:
                    post_mae = abs(post_adverse)
                post_final = current_pnl

            # Check if we have enough bars to cover 4h
            if bars is not None and len(bars) > 0:
                last_bar_time = int(bars[-1]["time"])
                if last_bar_time >= post_end - 60:
                    post_complete = True

        # Update trade record — always overwrite with computed values
        changed = False
        if abs(peak) > 0.01 or abs(worst) > 0.01:
            trade["peak_pnl_pips"] = round(peak, 1)
            trade["worst_pnl_pips"] = round(worst, 1)
            changed = True
        if post_complete and not trade.get("post_close_complete", False):
            trade["post_close_max_mfe_pips"] = round(post_mfe, 1)
            trade["post_close_max_mae_pips"] = round(post_mae, 1)
            trade["post_close_final_pips"] = round(post_final, 1)
            trade["post_close_complete"] = True
            changed = True

        if changed:
            updated += 1

        # Progress
        if (i + 1) % 50 == 0:
            print(f"    processed {i + 1}/{len(data)}...")

    # Save
    journal_path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"    Updated {updated}/{len(data)} trades")
    return updated


def main():
    print("Backfilling paper trade Peak/Worst/4H data from MT5...")

    if not mt5.initialize():
        print("ERROR: MT5 not connected")
        sys.exit(1)

    info = mt5.account_info()
    print(f"MT5 connected: {info.server if info else 'unknown'}")

    base_dir = Path(__file__).resolve().parent.parent / "data"

    journals = [
        base_dir / "paper_trades.json",
        base_dir / "paper_trades_ss.json",
        base_dir / "paper_trades_atr.json",
    ]

    total_updated = 0
    for journal in journals:
        total_updated += backfill_journal(journal)

    mt5.shutdown()
    print(f"\nDone. {total_updated} trades updated total.")


if __name__ == "__main__":
    main()
