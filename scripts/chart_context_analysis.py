"""Chart-context analyzer for losing trades.

For each trade in the target pair(s), fetches MT5 OHLC bars at the
entry time and computes structural features:
  - Trend slope on H1 / H4
  - ATR expansion vs contraction
  - Distance to recent swing high / low (would the trade be entering INTO S/R?)
  - Daily/weekly extreme proximity
  - Bar-pattern at entry (body %, wick direction)
  - Range expansion ratio

Then aggregates across winners vs losers to find patterns the entry-context
fields didn't capture (because they're snapshot-only, not multi-bar).

Usage:
    python scripts/chart_context_analysis.py [PAIR1] [PAIR2] ...
    (defaults to EURAUD CHFJPY GBPJPY for comparison)
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median

sys.stdout.reconfigure(encoding="utf-8")

import MetaTrader5 as mt5

DATA = Path(r"D:\Trading\TAKUMI Trader\data")
JST = timezone(timedelta(hours=9))
USD_QUOTE = {"EURUSD","GBPUSD","AUDUSD","NZDUSD","USDCAD","USDCHF","USDJPY"}


def commission(pair):
    if pair in USD_QUOTE: return 0.6
    if pair.endswith("JPY"): return 0.7
    return 0.8


def pip_value(pair):
    return 0.01 if "JPY" in pair else 0.0001


def load_pair(pair):
    """Load Sv2 trades for the pair, with net P/L."""
    recs = json.loads((DATA / "paper_trades.json").read_text(encoding="utf-8"))
    out = []
    for r in recs:
        if not r.get("close_reason") or r.get("pair") != pair:
            continue
        r = dict(r)
        r["_net_pnl"] = (r.get("pnl_pips", 0) or 0) - commission(pair)
        r["_net_is_win"] = r["_net_pnl"] > 0
        out.append(r)
    return out


def fetch_bars(pair, entry_ts, tf_label, lookback_bars):
    """Fetch the last N bars CLOSING AT OR BEFORE entry_ts.

    Returns list of dicts with open/high/low/close/time, or None.
    """
    tf_map = {
        "M5": mt5.TIMEFRAME_M5,
        "M15": mt5.TIMEFRAME_M15,
        "H1": mt5.TIMEFRAME_H1,
        "H4": mt5.TIMEFRAME_H4,
        "D1": mt5.TIMEFRAME_D1,
    }
    tf = tf_map.get(tf_label)
    if tf is None:
        return None
    # MT5 expects datetime in BROKER server time. Most brokers use UTC+2/+3.
    # We pass entry_ts as a UTC datetime; MT5 returns whatever bars closed
    # at or before that wall time. Use copy_rates_from for "from this time
    # going back".
    dt_utc = datetime.fromtimestamp(entry_ts, tz=timezone.utc)
    rates = mt5.copy_rates_from(pair, tf, dt_utc, lookback_bars)
    if rates is None or len(rates) == 0:
        return None
    bars = []
    for r in rates:
        bars.append({
            "time": r["time"],
            "open": float(r["open"]),
            "high": float(r["high"]),
            "low":  float(r["low"]),
            "close": float(r["close"]),
            "volume": int(r["tick_volume"]),
        })
    return bars


def trend_slope(bars, n=20):
    """Linear regression slope of close prices over last n bars,
    normalized to pips per bar."""
    if not bars or len(bars) < n:
        return 0.0
    closes = [b["close"] for b in bars[-n:]]
    pip = pip_value(bars[0].get("_pair", "EURUSD"))
    # Simple slope: (last - first) / n in pips
    return (closes[-1] - closes[0]) / pip / n


def compute_atr(bars, n=14):
    """Wilder's ATR over last n bars."""
    if not bars or len(bars) < n + 1:
        return 0.0
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i]["high"], bars[i]["low"], bars[i-1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    return sum(trs[-n:]) / n if len(trs) >= n else 0.0


def features_for_trade(pair, entry_ts, direction):
    """Return a dict of structural features from MT5 bars at entry time."""
    out = {}
    pip = pip_value(pair)

    # Multi-TF bar fetches
    h1_bars = fetch_bars(pair, entry_ts, "H1", 50)
    h4_bars = fetch_bars(pair, entry_ts, "H4", 30)
    d1_bars = fetch_bars(pair, entry_ts, "D1", 10)
    m15_bars = fetch_bars(pair, entry_ts, "M15", 100)

    if not h1_bars or not h4_bars or not m15_bars:
        return None  # MT5 didn't return data

    entry_price = h1_bars[-1]["close"]
    out["entry_price"] = entry_price

    # ── H1 features ──
    h1_atr_now = compute_atr(h1_bars[-15:], 14)
    h1_atr_prior = compute_atr(h1_bars[-30:-15], 14) if len(h1_bars) >= 30 else h1_atr_now
    out["h1_atr_pips"] = h1_atr_now / pip
    out["h1_atr_ratio"] = (h1_atr_now / h1_atr_prior) if h1_atr_prior > 0 else 1.0
    # >1.0 = expanding (good for momentum), <1.0 = contracting (chop)

    # H1 trend: slope of last 20 H1 closes
    if len(h1_bars) >= 20:
        h1_slope = (h1_bars[-1]["close"] - h1_bars[-20]["close"]) / pip / 20
        out["h1_trend_slope_pips_per_bar"] = h1_slope
        # Aligned with direction?
        if direction == "BUY":
            out["h1_trend_aligned"] = h1_slope > 0
        else:
            out["h1_trend_aligned"] = h1_slope < 0

    # H1 swing high/low in last 24 bars
    if len(h1_bars) >= 24:
        recent_h1 = h1_bars[-24:]
        h1_swing_high = max(b["high"] for b in recent_h1)
        h1_swing_low  = min(b["low"] for b in recent_h1)
        out["h1_dist_to_swing_high_pips"] = (h1_swing_high - entry_price) / pip
        out["h1_dist_to_swing_low_pips"]  = (entry_price - h1_swing_low) / pip
        # If BUYING very close to swing high → buying into resistance
        # If SELLING very close to swing low → selling into support
        if direction == "BUY":
            out["entering_into_resistance"] = out["h1_dist_to_swing_high_pips"] < 5
        else:
            out["entering_into_support"] = out["h1_dist_to_swing_low_pips"] < 5

    # ── H4 features ──
    if len(h4_bars) >= 10:
        h4_slope = (h4_bars[-1]["close"] - h4_bars[-10]["close"]) / pip / 10
        out["h4_trend_slope_pips_per_bar"] = h4_slope
        if direction == "BUY":
            out["h4_trend_aligned"] = h4_slope > 0
        else:
            out["h4_trend_aligned"] = h4_slope < 0
        # H4 ATR
        h4_atr = compute_atr(h4_bars[-15:], 14)
        out["h4_atr_pips"] = h4_atr / pip

    # ── D1 features ──
    if d1_bars and len(d1_bars) >= 2:
        # Today's range so far / yesterday's full range
        today = d1_bars[-1]
        yesterday = d1_bars[-2]
        today_range = (today["high"] - today["low"]) / pip
        yesterday_range = (yesterday["high"] - yesterday["low"]) / pip
        out["d1_range_consumed_pct"] = (today_range / yesterday_range * 100) if yesterday_range > 0 else 0
        out["d1_yesterday_range_pips"] = yesterday_range
        # Distance to today's open (proxy for "where in the day are we")
        out["d1_dist_to_today_open_pips"] = (entry_price - today["open"]) / pip

    # ── M15 micro structure ──
    if len(m15_bars) >= 8:
        last_8 = m15_bars[-8:]
        # Range expansion: last 4 bars range vs prior 4 bars range
        late_range = max(b["high"] for b in last_8[-4:]) - min(b["low"] for b in last_8[-4:])
        early_range = max(b["high"] for b in last_8[:4]) - min(b["low"] for b in last_8[:4])
        out["m15_range_expansion"] = (late_range / early_range) if early_range > 0 else 1.0
        # Last M15 bar character
        last = last_8[-1]
        body = abs(last["close"] - last["open"])
        full = last["high"] - last["low"]
        out["m15_body_ratio"] = (body / full) if full > 0 else 0
        # Direction of last bar
        out["m15_last_bar_direction"] = "UP" if last["close"] > last["open"] else "DOWN"
        out["m15_last_bar_aligned"] = (
            (out["m15_last_bar_direction"] == "UP" and direction == "BUY")
            or (out["m15_last_bar_direction"] == "DOWN" and direction == "SELL")
        )

    # ── Time-of-day for the bar ──
    dt_jst = datetime.fromtimestamp(entry_ts, tz=JST)
    out["jst_hour"] = dt_jst.hour
    out["jst_minute"] = dt_jst.minute
    out["weekday"] = dt_jst.strftime("%a")

    return out


def compare_features(feature_dicts_w, feature_dicts_l, pair):
    """Aggregate winner vs loser features and report differences."""
    print(f"\n{'='*78}")
    print(f"  {pair} — chart-context comparison ({len(feature_dicts_w)}W vs {len(feature_dicts_l)}L)")
    print(f"{'='*78}\n")

    if not feature_dicts_w or not feature_dicts_l:
        print("  Insufficient data.")
        return

    # Numerical comparison
    numeric_keys = [k for k in feature_dicts_w[0]
                    if isinstance(feature_dicts_w[0].get(k), (int, float))
                    and not isinstance(feature_dicts_w[0].get(k), bool)]

    print(f"  Numerical features (★ = significant gap):")
    for key in numeric_keys:
        w_vals = [d.get(key) for d in feature_dicts_w if isinstance(d.get(key), (int, float))]
        l_vals = [d.get(key) for d in feature_dicts_l if isinstance(d.get(key), (int, float))]
        if not w_vals or not l_vals:
            continue
        w_mean = mean(w_vals)
        l_mean = mean(l_vals)
        w_med = median(w_vals)
        l_med = median(l_vals)
        gap = w_mean - l_mean
        flag = ""
        # Significance: gap > 25% of either mean
        if abs(gap) > 0 and (abs(w_mean) > 0 or abs(l_mean) > 0):
            base = max(abs(w_mean), abs(l_mean))
            if base > 0 and abs(gap) / base > 0.25:
                flag = " ★"
        print(f"    {key:<35} W={w_mean:>7.2f} L={l_mean:>7.2f}  "
              f"med W={w_med:>7.2f} L={l_med:>7.2f}  gap={gap:>+7.2f}{flag}")

    # Boolean / categorical comparison
    print(f"\n  Boolean / categorical features (% TRUE):")
    bool_keys = [k for k in feature_dicts_w[0]
                 if isinstance(feature_dicts_w[0].get(k), bool)]
    for key in bool_keys:
        w_true = sum(1 for d in feature_dicts_w if d.get(key))
        l_true = sum(1 for d in feature_dicts_l if d.get(key))
        wn, ln = len(feature_dicts_w), len(feature_dicts_l)
        wp = w_true / wn * 100
        lp = l_true / ln * 100
        gap = wp - lp
        flag = " ★" if abs(gap) >= 20 else ""
        print(f"    {key:<35} W={wp:>5.1f}% ({w_true}/{wn})  "
              f"L={lp:>5.1f}% ({l_true}/{ln})  gap={gap:>+5.1f}pp{flag}")


def analyze_pair(pair):
    trades = load_pair(pair)
    if not trades:
        print(f"\n[{pair}] No trades found")
        return

    print(f"\nProcessing {pair}: {len(trades)} trades")
    feats_w, feats_l = [], []
    failed = 0
    for t in trades:
        feats = features_for_trade(pair, t["entry_time"], t.get("direction", "BUY"))
        if feats is None:
            failed += 1
            continue
        feats["_pnl"] = t["_net_pnl"]
        feats["_entry_time_str"] = datetime.fromtimestamp(
            t["entry_time"], tz=JST).strftime("%m-%d %H:%M")
        feats["_direction"] = t.get("direction")
        if t["_net_is_win"]:
            feats_w.append(feats)
        else:
            feats_l.append(feats)
    if failed:
        print(f"  ⚠️  {failed} trades skipped (MT5 didn't return bars)")
    compare_features(feats_w, feats_l, pair)

    # Print the 5 worst losses with their features for visual scan
    print(f"\n  TOP 5 worst losses on {pair}:")
    worst = sorted(feats_l, key=lambda x: x["_pnl"])[:5]
    for t in worst:
        print(f"    {t['_entry_time_str']} {t['_direction']} pnl={t['_pnl']:+.1f}p:")
        notable = []
        if "h1_trend_aligned" in t:
            notable.append(f"H1_aligned={t['h1_trend_aligned']}")
        if "h4_trend_aligned" in t:
            notable.append(f"H4_aligned={t['h4_trend_aligned']}")
        if "h1_atr_ratio" in t:
            notable.append(f"H1_ATR_ratio={t['h1_atr_ratio']:.2f}")
        if "entering_into_resistance" in t:
            notable.append(f"into_R={t['entering_into_resistance']}")
        if "entering_into_support" in t:
            notable.append(f"into_S={t['entering_into_support']}")
        if "m15_range_expansion" in t:
            notable.append(f"M15_exp={t['m15_range_expansion']:.2f}")
        if "d1_range_consumed_pct" in t:
            notable.append(f"D1_used={t['d1_range_consumed_pct']:.0f}%")
        print(f"      {' | '.join(notable)}")


def main():
    if not mt5.initialize():
        print(f"MT5 initialize failed: {mt5.last_error()}")
        return 1
    try:
        pairs = sys.argv[1:] if len(sys.argv) > 1 else ["EURAUD", "CHFJPY", "GBPJPY"]
        for pair in pairs:
            analyze_pair(pair)
    finally:
        mt5.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
