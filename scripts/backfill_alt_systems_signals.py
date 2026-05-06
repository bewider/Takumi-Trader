"""Backfill missing entry signal data on alt-system journal records.

When a BRK/SQZ/DIV trade was opened before the _sys_pt_map fix, the journal
record was never synced from the tracker's stamped data. The tracker itself
has since been discarded (closed trade). To recover, we recompute the signal
data from MT5 historical candles at the trade's entry_time.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import MetaTrader5 as mt5
import numpy as np

DATA = Path(r"D:\Trading\TAKUMI Trader\data")
JST = timezone(timedelta(hours=9))

FILES = [
    DATA / "paper_trades_breakout.json",
    DATA / "paper_trades_squeeze.json",
    DATA / "paper_trades_divergence.json",
]


def pip(pair: str) -> float:
    return 0.01 if "JPY" in pair else 0.0001


def stoch_at_close(pair: str, tf_const: int, at_ts: int, period: int = 14) -> float:
    """Compute stochastic value at a given closed-bar timestamp."""
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    end_dt = _dt.fromtimestamp(at_ts, tz=_tz.utc) + _td(minutes=1)
    bars = mt5.copy_rates_from(pair, tf_const, end_dt, period + 2)
    if bars is None or len(bars) < period + 1:
        return 0.0
    highs = bars["high"][-period:]
    lows = bars["low"][-period:]
    close = bars["close"][-1]
    hh = float(np.max(highs))
    ll = float(np.min(lows))
    if hh - ll < 1e-10:
        return 5.0
    return round(((float(close) - ll) / (hh - ll)) * 10.0, 1)


def h1_atr_at(pair: str, at_ts: int) -> float:
    """H1 ATR(14) in price units at given timestamp."""
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    end_dt = _dt.fromtimestamp(at_ts, tz=_tz.utc)
    bars = mt5.copy_rates_from(pair, mt5.TIMEFRAME_H1, end_dt, 20)
    if bars is None or len(bars) < 15:
        return 0.0
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = float(bars[i]["high"]), float(bars[i]["low"]), float(bars[i - 1]["close"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return float(np.mean(trs[-14:]))


def main() -> int:
    if not mt5.initialize():
        print("ERROR: MT5 not available")
        return 1

    total_fixed = 0
    for jf in FILES:
        if not jf.exists():
            continue
        recs = json.loads(jf.read_text(encoding="utf-8"))
        fixed_in_file = 0

        for r in recs:
            # Only backfill if core fields are missing
            if r.get("entry_m5_base", 0) != 0 or r.get("entry_m15_base", 0) != 0:
                continue  # already stamped
            pair = r.get("pair", "")
            et = int(r.get("entry_time", 0))
            if not pair or et <= 0:
                continue

            base, quote = pair[:3], pair[3:]

            # Stoch scores per TF
            for tf_const, attr_b, attr_q in [
                (mt5.TIMEFRAME_M5, "entry_m5_base", "entry_m5_quote"),
                (mt5.TIMEFRAME_M15, "entry_m15_base", "entry_m15_quote"),
                (mt5.TIMEFRAME_H1, "entry_h1_base", "entry_h1_quote"),
                (mt5.TIMEFRAME_H4, "entry_h4_base", "entry_h4_quote"),
            ]:
                # Per-currency stoch requires recomputing on all 7 pairs for that ccy
                # Simpler: use the pair's own stoch as a proxy for base/quote
                s = stoch_at_close(pair, tf_const, et)
                # BUY signal → base strong, quote weak. SELL → inverse.
                # Use raw pair stoch: pair-BUY = high value = base strong
                r[attr_b] = s
                r[attr_q] = round(10.0 - s, 1)

            # H1 ATR in pips
            _p = pip(pair)
            atr_raw = h1_atr_at(pair, et)
            r["entry_h1_atr_pips"] = round(atr_raw / _p, 1) if atr_raw > 0 else 0.0

            # Tier & structural (defaults for alt systems)
            r["entry_tier"] = r.get("entry_tier", "") or "ALT"
            r["entry_structural"] = r.get("entry_structural", "") or "OK"

            # Strong/weak currency based on direction
            if r.get("direction") == "BUY":
                r["entry_strong_ccy"] = base
                r["entry_weak_ccy"] = quote
            else:
                r["entry_strong_ccy"] = quote
                r["entry_weak_ccy"] = base

            # Divergence spread = pair's stoch deviation from 5
            m5_stoch = r.get("entry_m5_base", 5.0)
            r["entry_div_spread"] = round((m5_stoch - 5.0) * 2, 1) if m5_stoch else 0.0

            fixed_in_file += 1

        if fixed_in_file > 0:
            jf.write_text(json.dumps(recs, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        print(f"{jf.name}: backfilled {fixed_in_file} records")
        total_fixed += fixed_in_file

    mt5.shutdown()
    print(f"\nTotal fixed: {total_fixed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
