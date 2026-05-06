"""Close orphan OPEN records by replaying M1 history.

An "orphan OPEN" is a journal record whose pair+entry_time is NOT in the
corresponding tracker file — meaning the tracker has no active trade to
drive it through `_check_sl_tp`. These rows show "OPEN" forever.

Strategy:
  For each orphan, pull M1 candles since entry_time and check if a bar's
  high/low crossed the TP or SL level. If so, close the record with the
  correct reason and P/L. If neither was hit, leave it OPEN (still a live
  trade, just not tracked — user can decide what to do).

Run while TAKUMI is stopped.
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import MetaTrader5 as mt5

DATA = Path(r"D:\Trading\TAKUMI Trader\data")

# (tracker_file, journal_file, system_label)
PAIRS = [
    ("tracked_trades.json",          "paper_trades.json",          "Sv2"),
    ("tracked_trades_ss.json",       "paper_trades_ss.json",       "SS"),
    ("tracked_trades_atr.json",      "paper_trades_atr.json",      "ATR"),
    ("tracked_trades_qm4.json",      "paper_trades_qm4.json",      "QM4"),
    ("tracked_trades_a_tuned.json",  "paper_trades_a_tuned.json",  "A-tuned"),
    ("tracked_trades_b_tuned.json",  "paper_trades_b_tuned.json",  "B-tuned"),
]


def pip(pair: str) -> float:
    return 0.01 if "JPY" in pair else 0.0001


def tracker_keys(path: Path) -> set[tuple[str, int]]:
    if not path.exists():
        return set()
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return set()
    if isinstance(d, dict):
        d = d.get("active_trades", [])
    keys = set()
    for t in d or []:
        p = t.get("pair", "")
        et = t.get("entry_time", 0)
        if p and et:
            keys.add((p, int(et)))
    return keys


def replay_close(rec: dict) -> dict | None:
    """Replay M1 bars since entry; return close dict if SL/TP hit, else None."""
    pair = rec.get("pair", "")
    direction = rec.get("direction", "")
    entry_price = rec.get("entry_price", 0.0)
    entry_time = rec.get("entry_time", 0.0)
    sl_price = rec.get("sl_price", 0.0)
    tp_price = rec.get("tp_price", 0.0)

    if not (pair and direction and entry_price > 0 and entry_time > 0
            and sl_price > 0 and tp_price > 0):
        return None

    start = datetime.fromtimestamp(entry_time, tz=timezone.utc) - timedelta(minutes=1)
    end = datetime.now(timezone.utc) + timedelta(minutes=1)
    bars = mt5.copy_rates_range(pair, mt5.TIMEFRAME_M1, start, end)
    if bars is None or len(bars) < 2:
        return None

    _pip = pip(pair)
    peak = 0.0
    worst = 0.0
    for bar in bars:
        bt = int(bar["time"])
        if bt < int(entry_time):
            continue
        h = float(bar["high"])
        l = float(bar["low"])

        # Update peak/worst
        if direction == "BUY":
            bp = (h - entry_price) / _pip
            wp = (l - entry_price) / _pip
        else:
            bp = (entry_price - l) / _pip
            wp = (entry_price - h) / _pip
        if bp > peak:
            peak = bp
        if wp < worst:
            worst = wp

        # Check SL/TP hit
        hit_reason = None
        hit_price = None
        if direction == "BUY":
            if l <= sl_price:
                hit_reason = "stop_loss"
                hit_price = sl_price
            elif h >= tp_price:
                hit_reason = "take_profit"
                hit_price = tp_price
        else:  # SELL
            if h >= sl_price:
                hit_reason = "stop_loss"
                hit_price = sl_price
            elif l <= tp_price:
                hit_reason = "take_profit"
                hit_price = tp_price

        if hit_reason:
            close_time = float(bt)
            if direction == "BUY":
                pnl = (hit_price - entry_price) / _pip
            else:
                pnl = (entry_price - hit_price) / _pip
            dur_min = (close_time - entry_time) / 60.0
            close_str = datetime.fromtimestamp(close_time, tz=timezone.utc).astimezone(
                timezone(timedelta(hours=9))
            ).strftime("%Y-%m-%d %H:%M:%S")
            return {
                "close_price": float(hit_price),
                "close_time": close_time,
                "close_time_str": close_str,
                "close_reason": hit_reason,
                "pnl_pips": round(float(pnl), 1),
                "peak_pnl_pips": round(float(peak), 1),
                "worst_pnl_pips": round(float(worst), 1),
                "duration_minutes": round(float(dur_min), 1),
                "is_win": pnl > 0,
            }

    return None


def main() -> int:
    if not mt5.initialize():
        print("ERROR: MT5 not connected")
        return 1
    info = mt5.account_info()
    print(f"MT5 connected: {info.server if info else 'unknown'}")
    print()

    grand_orphans = 0
    grand_closed = 0

    for tracker_name, journal_name, label in PAIRS:
        tpath = DATA / tracker_name
        jpath = DATA / journal_name
        if not jpath.exists():
            continue

        active_keys = tracker_keys(tpath)
        journal_raw = json.loads(jpath.read_text(encoding="utf-8"))
        if isinstance(journal_raw, dict):
            recs = journal_raw.get("trades", [])
        else:
            recs = journal_raw

        orphans = 0
        closed = 0
        for r in recs:
            if r.get("close_reason"):
                continue  # already closed
            pair = r.get("pair", "")
            et = r.get("entry_time", 0)
            key = (pair, int(et)) if (pair and et) else None
            if key is None:
                continue
            if key in active_keys:
                continue  # real active trade — leave it alone

            orphans += 1
            result = replay_close(r)
            if result:
                r.update(result)
                closed += 1

        if isinstance(journal_raw, dict):
            journal_raw["trades"] = recs
            out = journal_raw
        else:
            out = recs
        jpath.write_text(
            json.dumps(out, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        print(f"  [{label:<8}] orphans={orphans}, closed via replay={closed}")
        grand_orphans += orphans
        grand_closed += closed

    print()
    print(f"Total orphans: {grand_orphans}")
    print(f"Closed by replay: {grand_closed}")
    print(f"Still OPEN (SL/TP never hit): {grand_orphans - grand_closed}")

    mt5.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
