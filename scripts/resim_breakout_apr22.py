"""One-off: resimulate Apr 22 Breakout trades that ran under the bug-formula
R:R 1.26 (range-based SL=range, TP=1.5*range) and rewrite the journal with
the outcomes they WOULD have had under the proven per-pair Sv2 formula
(sl_atr * H1_ATR, tp_atr * H1_ATR — typically R:R 0.5).

Originals are preserved as `_orig_<field>`. A flag `_outcome_resimulated=True`
is added so future code (or a re-run of this script) can detect what was
already touched.
"""
import json, shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta
JST = timezone(timedelta(hours=9))
UTC = timezone.utc

import MetaTrader5 as mt5
from takumi_trader.core.pair_algo_settings import get_pair_settings


def pip(pair: str) -> float:
    return 0.01 if "JPY" in pair else 0.0001


def simulate_one(r: dict):
    """Walk M1 bars from entry forward, find first SL or TP touch under the
    per-pair formula. Return a dict of new field values, or None if data
    unavailable."""
    pair = r["pair"]
    direction = r["direction"]
    entry_price = r["entry_price"]
    entry_time = r["entry_time"]
    h1_atr_pips = r.get("entry_h1_atr_pips", 0) or 0
    if h1_atr_pips <= 0:
        return None

    p_pip = pip(pair)
    s = get_pair_settings(pair) or {}
    sl_atr_mult = s.get("sl_atr", 0.3)
    tp_atr_mult = s.get("tp_atr", 1.0)
    new_sl_pips = round(sl_atr_mult * h1_atr_pips, 1)
    new_tp_pips = round(tp_atr_mult * h1_atr_pips, 1)
    if direction == "BUY":
        new_sl_price = round(entry_price - sl_atr_mult * h1_atr_pips * p_pip, 5)
        new_tp_price = round(entry_price + tp_atr_mult * h1_atr_pips * p_pip, 5)
    else:
        new_sl_price = round(entry_price + sl_atr_mult * h1_atr_pips * p_pip, 5)
        new_tp_price = round(entry_price - tp_atr_mult * h1_atr_pips * p_pip, 5)

    # MT5 broker uses GMT+3 — bar timestamps are 3h ahead of real UTC, so we
    # query with entry_time + 3h to land on the right bars.
    from_dt = datetime.fromtimestamp(entry_time + 3 * 3600, tz=UTC)
    to_dt = from_dt + timedelta(hours=8)
    bars = mt5.copy_rates_range(pair, mt5.TIMEFRAME_M1, from_dt, to_dt)
    if bars is None or len(bars) == 0:
        return None

    outcome = None
    new_pnl = 0.0
    close_idx = -1
    peak_pips = 0.0
    worst_pips = 0.0
    near_sl_count = 0
    near_tp_count = 0
    bars_to_close = 0
    time_to_5p = -1.0
    went_profit_first = False

    for i, b in enumerate(bars):
        bars_to_close = i + 1
        h, l = float(b["high"]), float(b["low"])
        o, c = float(b["open"]), float(b["close"])

        if direction == "BUY":
            best = (h - entry_price) / p_pip
            worst = (l - entry_price) / p_pip
        else:
            best = (entry_price - l) / p_pip
            worst = (entry_price - h) / p_pip
        if best > peak_pips:
            peak_pips = best
        if worst < worst_pips:
            worst_pips = worst

        if time_to_5p < 0 and best >= 5.0:
            # Bar time is in broker's "fake-UTC" — subtract 3h for real UTC
            time_to_5p = round((int(b["time"]) - 3 * 3600 - entry_time) / 60.0, 1)
            if worst_pips > -5.0:
                went_profit_first = True

        if direction == "BUY":
            dist_to_sl = (l - new_sl_price) / p_pip
            dist_to_tp = (new_tp_price - h) / p_pip
        else:
            dist_to_sl = (new_sl_price - h) / p_pip
            dist_to_tp = (l - new_tp_price) / p_pip
        if 0 < dist_to_sl < 2.0:
            near_sl_count += 1
        if 0 < dist_to_tp < 2.0:
            near_tp_count += 1

        if direction == "BUY":
            sl_h = l <= new_sl_price
            tp_h = h >= new_tp_price
            if sl_h and tp_h:
                if c > o:
                    outcome = "tp_hit"; new_pnl = +new_tp_pips
                else:
                    outcome = "sl_hit"; new_pnl = -new_sl_pips
                close_idx = i; break
            elif sl_h:
                outcome = "sl_hit"; new_pnl = -new_sl_pips; close_idx = i; break
            elif tp_h:
                outcome = "tp_hit"; new_pnl = +new_tp_pips; close_idx = i; break
        else:
            sl_h = h >= new_sl_price
            tp_h = l <= new_tp_price
            if sl_h and tp_h:
                if c < o:
                    outcome = "tp_hit"; new_pnl = +new_tp_pips
                else:
                    outcome = "sl_hit"; new_pnl = -new_sl_pips
                close_idx = i; break
            elif sl_h:
                outcome = "sl_hit"; new_pnl = -new_sl_pips; close_idx = i; break
            elif tp_h:
                outcome = "tp_hit"; new_pnl = +new_tp_pips; close_idx = i; break

    if outcome is None:
        last = bars[-1]
        last_close_price = float(last["close"])
        if direction == "BUY":
            new_pnl = round((last_close_price - entry_price) / p_pip, 1)
        else:
            new_pnl = round((entry_price - last_close_price) / p_pip, 1)
        outcome = "signal_exit"
        close_idx = len(bars) - 1
        close_price = last_close_price
        close_time = int(last["time"]) - 3 * 3600
    elif outcome == "tp_hit":
        close_price = new_tp_price
        close_time = int(bars[close_idx]["time"]) - 3 * 3600
    else:
        close_price = new_sl_price
        close_time = int(bars[close_idx]["time"]) - 3 * 3600

    duration_min = round((close_time - entry_time) / 60.0, 1)
    is_win = new_pnl > 0

    # ── Post-close 4h MAE/MFE ──
    pc_from = datetime.fromtimestamp(close_time + 3 * 3600, tz=UTC)
    pc_to = pc_from + timedelta(hours=4)
    pc_bars = mt5.copy_rates_range(pair, mt5.TIMEFRAME_M1, pc_from, pc_to)
    pc_max_mfe = 0.0
    pc_max_mae = 0.0
    pc_final = 0.0
    pc_minutes = 240.0
    pc_complete = True
    if pc_bars is not None and len(pc_bars) > 0:
        for b in pc_bars:
            h, l = float(b["high"]), float(b["low"])
            if direction == "BUY":
                fav = (h - close_price) / p_pip
                adv = (l - close_price) / p_pip
            else:
                fav = (close_price - l) / p_pip
                adv = (close_price - h) / p_pip
            if fav > pc_max_mfe:
                pc_max_mfe = fav
            if adv < pc_max_mae:
                pc_max_mae = adv
        last_px = float(pc_bars[-1]["close"])
        pc_final = ((last_px - close_price) / p_pip) if direction == "BUY" \
                   else ((close_price - last_px) / p_pip)

    return {
        "sl_pips": new_sl_pips,
        "tp_pips": new_tp_pips,
        "sl_price": new_sl_price,
        "tp_price": new_tp_price,
        "close_price": round(close_price, 5),
        "close_time": close_time,
        "close_time_str": datetime.fromtimestamp(close_time, tz=JST).strftime("%Y-%m-%d %H:%M:%S"),
        "close_reason": outcome,
        "pnl_pips": round(new_pnl, 1),
        "peak_pnl_pips": round(peak_pips, 1),
        "worst_pnl_pips": round(worst_pips, 1),
        "duration_minutes": duration_min,
        "is_win": is_win,
        "time_to_5p_profit_min": round(time_to_5p, 1) if time_to_5p > 0 else -1.0,
        "went_profit_first": went_profit_first,
        "near_sl_count": near_sl_count,
        "near_tp_count": near_tp_count,
        "bars_to_close": bars_to_close,
        "post_close_max_mfe_pips": round(pc_max_mfe, 1),
        "post_close_max_mae_pips": round(pc_max_mae, 1),
        "post_close_final_pips": round(pc_final, 1),
        "post_close_minutes": pc_minutes,
        "post_close_complete": pc_complete,
    }


def main():
    src = Path("data/paper_trades_breakout.json")
    if not mt5.initialize():
        print("MT5 init failed:", mt5.last_error())
        return

    # Backup
    ts = datetime.now(JST).strftime("%Y%m%d_%H%M%S")
    bak = src.with_suffix(f".json.pre_resim_{ts}.bak")
    shutil.copy(src, bak)
    print(f"Backup: {bak}")

    data = json.loads(src.read_text())
    print(f"Total records: {len(data)}")

    # Filter: closed Apr 22 trades with R:R > 1.0 (the bug formula)
    to_resim = [
        r for r in data
        if r.get("close_reason")
        and r.get("entry_time_str", "").startswith("2026-04-22")
        and r.get("sl_pips", 0) > 0 and r.get("tp_pips", 0) > 0
        and r["tp_pips"] / r["sl_pips"] > 1.0
        and not r.get("_outcome_resimulated")
    ]
    print(f"Will resimulate {len(to_resim)} closed Apr 22 trades")

    results = {}
    for r in to_resim:
        sim = simulate_one(r)
        if sim is None:
            print(f"  [SKIP] {r['pair']} {r['direction']} {r['entry_time_str']}")
            continue
        key = (r["pair"], r["direction"], r["entry_time"])
        results[key] = sim

    print(f"Simulated {len(results)} trades successfully")

    ORIG_FIELDS = [
        "sl_pips", "tp_pips", "sl_price", "tp_price",
        "close_price", "close_time", "close_time_str", "close_reason",
        "pnl_pips", "peak_pnl_pips", "worst_pnl_pips", "duration_minutes", "is_win",
        "time_to_5p_profit_min", "went_profit_first",
        "near_sl_count", "near_tp_count", "bars_to_close",
        "post_close_max_mfe_pips", "post_close_max_mae_pips",
        "post_close_final_pips", "post_close_minutes", "post_close_complete",
    ]
    modified = 0
    for r in data:
        key = (r["pair"], r["direction"], r["entry_time"])
        sim = results.get(key)
        if sim is None:
            continue
        if r.get("_outcome_resimulated"):
            continue  # already done (idempotent)
        # Preserve originals
        for f in ORIG_FIELDS:
            if f in r:
                r[f"_orig_{f}"] = r[f]
        # Overwrite with simulated
        for f in ORIG_FIELDS:
            if f in sim:
                r[f] = sim[f]
        r["_outcome_resimulated"] = True
        r["_resim_timestamp"] = datetime.now(JST).isoformat()
        r["_resim_reason"] = (
            "Apr 22 trades reverted from bug-formula R:R 1.26 to per-pair "
            "Sv2 R:R 0.5 (proven-profitable config)"
        )
        modified += 1

    print(f"Modified {modified} records")

    # Pretty-print before/after for confirmation
    print()
    print("=== Before/After ===")
    print(f"{'pair':<8} {'dir':<5} {'orig_pnl':<10} {'new_pnl':<10} {'orig_close':<12} {'new_close'}")
    for r in data:
        if r.get("_outcome_resimulated"):
            print(f"{r['pair']:<8} {r['direction']:<5} "
                  f"{r['_orig_pnl_pips']:<+9.1f}p {r['pnl_pips']:<+9.1f}p "
                  f"{r['_orig_close_reason']:<12} {r['close_reason']}")

    src.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    print()
    print(f"Wrote {src} ({src.stat().st_size:,} bytes)")
    mt5.shutdown()


if __name__ == "__main__":
    main()
