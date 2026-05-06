"""Backtest structural filters (Key Levels, TP Clearance, ADR, Range Position)
against the A+B+C baseline on all pairs."""

import numpy as np
import time
import json
import sys
sys.path.insert(0, r"D:\Trading\TAKUMI Trader")

from takumi_trader.core.backtester import BacktestEngine, BacktestConfig, CalcParams
from takumi_trader.core.strength import DISPLAY_PAIRS


with open(r"D:\Trading\TAKUMI Trader\DATA\pair_algo_settings.json") as f:
    settings = json.load(f)


def pip_value(pair):
    return 0.01 if "JPY" in pair else 0.0001


def compute_key_levels(data, pair, m1_idx):
    m1 = data.get(pair, {}).get("M1")
    if m1 is None or m1_idx < 1440:
        return None

    pip = pip_value(pair)
    current_price = float(m1[m1_idx]["close"])

    day_start = max(0, m1_idx - 1440)
    day_slice = m1[day_start:m1_idx]
    if len(day_slice) < 100:
        return None

    prev_day_high = float(np.max(day_slice["high"]))
    prev_day_low = float(np.min(day_slice["low"]))

    week_start = max(0, m1_idx - 7200)
    week_slice = m1[week_start:m1_idx]
    prev_week_high = float(np.max(week_slice["high"]))
    prev_week_low = float(np.min(week_slice["low"]))

    # ADR
    adr_pips = 0
    for d in range(5):
        d_start = max(0, m1_idx - (d + 1) * 1440)
        d_end = max(0, m1_idx - d * 1440)
        if d_end > d_start:
            d_slice = m1[d_start:d_end]
            if len(d_slice) > 0:
                adr_pips += (float(np.max(d_slice["high"])) - float(np.min(d_slice["low"]))) / pip
    adr_pips /= 5

    today_start = max(0, m1_idx - 1440)
    today_slice = m1[today_start:m1_idx + 1]
    today_high = float(np.max(today_slice["high"]))
    today_low = float(np.min(today_slice["low"]))
    today_range = (today_high - today_low) / pip
    adr_consumed = (today_range / adr_pips * 100) if adr_pips > 0 else 0

    week_range = prev_week_high - prev_week_low
    range_position = ((current_price - prev_week_low) / week_range * 100) if week_range > 0 else 50

    return {
        "price": current_price,
        "prev_day_high": prev_day_high, "prev_day_low": prev_day_low,
        "prev_week_high": prev_week_high, "prev_week_low": prev_week_low,
        "adr_consumed": adr_consumed, "adr_pips": adr_pips,
        "range_position": range_position,
        "pip": pip,
    }


def apply_filters(trade, levels, flags):
    if levels is None:
        return True, "no_levels"

    pip = levels["pip"]
    price = trade.entry_price
    direction = trade.direction

    c = settings.get(trade.pair, {}).get("current", {})
    sl_atr = c.get("sl_atr", 0.5)
    tp_atr = c.get("tp_atr", 0.5)
    if trade.entry_atr_pips > 0:
        tp_pips = tp_atr * trade.entry_atr_pips
    else:
        tp_pips = c.get("tp_pips", 10)

    if direction == "BUY":
        tp_price = price + tp_pips * pip
    else:
        tp_price = price - tp_pips * pip

    proximity_pips = 10

    if flags.get("key_levels"):
        if direction == "BUY":
            dist_day = (levels["prev_day_high"] - price) / pip
            dist_week = (levels["prev_week_high"] - price) / pip
            if 0 < dist_day < proximity_pips:
                return False, "near_day_high"
            if 0 < dist_week < proximity_pips:
                return False, "near_week_high"
        else:
            dist_day = (price - levels["prev_day_low"]) / pip
            dist_week = (price - levels["prev_week_low"]) / pip
            if 0 < dist_day < proximity_pips:
                return False, "near_day_low"
            if 0 < dist_week < proximity_pips:
                return False, "near_week_low"

    if flags.get("tp_clearance"):
        if direction == "BUY":
            if tp_price > levels["prev_day_high"]:
                return False, "tp_above_day_high"
        else:
            if tp_price < levels["prev_day_low"]:
                return False, "tp_below_day_low"

    if flags.get("adr"):
        if levels["adr_consumed"] > 75:
            return False, "adr_exhausted"

    if flags.get("range_position"):
        rp = levels["range_position"]
        if direction == "BUY" and rp > 85:
            return False, "buying_at_top"
        if direction == "SELL" and rp < 15:
            return False, "selling_at_bottom"

    return True, "passed"


# ── Run backtest ──
cp = CalcParams()
cp.ema_period = 4
cp.roc_decay = 0.2
cp.threshold_m1 = 5.5
cp.threshold_m5 = 5.0
cp.threshold_m15 = 4.5
cp.threshold_h1 = 4.0

config = BacktestConfig(
    days_back=0, start_date="2026-01-05", use_dukascopy=True,
    calc_params=cp, allow_session_reentry=True, use_accel_entry=True,
    accel_min_velocity=1.5, accel_min_spread=6.0, accel_min_htf_agree=2,
)

print("Running full backtest (A+B+C: EMA4, Z50, Accel) all pairs...")
t0 = time.time()
engine = BacktestEngine(config)
all_trades = engine.run()
elapsed = time.time() - t0
print(f"Done: {len(all_trades)} trades in {elapsed:.0f}s\n")

# Fetch data for level computation
print("Computing key levels for each trade...")
data = engine._fetch_dukascopy_data()
trade_levels = []
for trade in all_trades:
    m1 = data.get(trade.pair, {}).get("M1")
    if m1 is None:
        trade_levels.append(None)
        continue
    times = m1["time"]
    idx = int(np.searchsorted(times, trade.entry_time))
    idx = min(idx, len(times) - 1)
    trade_levels.append(compute_key_levels(data, trade.pair, idx))

print("Done\n")

# ── Test all configs ──
test_configs = [
    ("BASELINE: A+B+C (no structural)", {}),
    ("1. Key Level Proximity", {"key_levels": True}),
    ("2. TP Clearance (day)", {"tp_clearance": True}),
    ("3. ADR Exhaustion (>75%)", {"adr": True}),
    ("4. Range Position", {"range_position": True}),
    ("1+2: Levels + TP Clear", {"key_levels": True, "tp_clearance": True}),
    ("1+3: Levels + ADR", {"key_levels": True, "adr": True}),
    ("2+3: TP Clear + ADR", {"tp_clearance": True, "adr": True}),
    ("2+4: TP Clear + Range", {"tp_clearance": True, "range_position": True}),
    ("1+2+3: Levels+TP+ADR", {"key_levels": True, "tp_clearance": True, "adr": True}),
    ("3+4: ADR + Range", {"adr": True, "range_position": True}),
    ("ALL: 1+2+3+4", {"key_levels": True, "tp_clearance": True, "adr": True, "range_position": True}),
]

print(f"{'Method':<40s} | {'Total':>5s} | {'Kept':>5s} | {'Skip':>5s} | {'WR%':>6s} | {'Avg P/L':>8s} | {'Total PnL':>10s} | {'vs Base':>8s}")
print("=" * 110)

baseline_pnl = None
for label, flags in test_configs:
    kept = []
    skipped = 0
    for i, trade in enumerate(all_trades):
        if not flags:
            kept.append(trade)
        else:
            passed, _ = apply_filters(trade, trade_levels[i], flags)
            if passed:
                kept.append(trade)
            else:
                skipped += 1

    total = len(kept)
    wins = sum(1 for t in kept if t.final_pnl_pips > 0)
    wr = wins / total * 100 if total else 0
    total_pnl = sum(t.final_pnl_pips for t in kept)
    avg_pnl = total_pnl / total if total else 0

    if baseline_pnl is None:
        baseline_pnl = total_pnl
        vs = ""
    else:
        diff = (total_pnl - baseline_pnl) / abs(baseline_pnl) * 100 if baseline_pnl else 0
        vs = f"{diff:>+6.0f}%"

    print(f"{label:<40s} | {len(all_trades):>5d} | {total:>5d} | {skipped:>5d} | {wr:>5.1f}% | {avg_pnl:>+7.1f}p | {total_pnl:>+9.0f}p | {vs:>8s}")

print("=" * 110)

# Quality check
print("\nQUALITY CHECK: Kept vs Skipped trade quality")
print(f"{'Filter':<30s} | {'Kept WR':>7s} | {'Skip WR':>7s} | {'Kept Avg':>9s} | {'Skip Avg':>9s} | {'Verdict':>8s}")
print("-" * 85)
for label, flags in test_configs[1:5]:
    kept_t = []
    skip_t = []
    for i, trade in enumerate(all_trades):
        passed, _ = apply_filters(trade, trade_levels[i], flags)
        if passed:
            kept_t.append(trade)
        else:
            skip_t.append(trade)

    kept_wr = sum(1 for t in kept_t if t.final_pnl_pips > 0) / len(kept_t) * 100 if kept_t else 0
    skip_wr = sum(1 for t in skip_t if t.final_pnl_pips > 0) / len(skip_t) * 100 if skip_t else 0
    kept_avg = sum(t.final_pnl_pips for t in kept_t) / len(kept_t) if kept_t else 0
    skip_avg = sum(t.final_pnl_pips for t in skip_t) / len(skip_t) if skip_t else 0

    verdict = "GOOD!" if kept_avg > skip_avg else "BAD"
    short = label.split(".")[1].strip() if "." in label else label
    print(f"  {short:<28s} | {kept_wr:>6.1f}% | {skip_wr:>6.1f}% | {kept_avg:>+8.1f}p | {skip_avg:>+8.1f}p | {verdict:>8s}")
