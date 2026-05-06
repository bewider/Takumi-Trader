"""Live-faithful backtester (v2).

Goal: reproduce live Sv2 trades EXACTLY by reusing the same engine
instances live uses, just feeding them historical Dukascopy bars instead
of polling MT5.

Key fidelity points (matching live mt5_worker.py):
  • Update stoch_engine ONLY on M5/M15/H1 candle CLOSES (not every M1)
  • Use the SAME StochStrengthEngine, CalculationEngine, FilterEngine,
    HTFRegimeTracker, AlertManager that live uses (not BT's reimplementation)
  • Apply filters in the SAME order: check_entry → divergence_spread →
    conviction tier == FULL → alert_mgr cooldown
  • Read filter settings from QSettings (matches what live actually uses)

This file does NOT reuse the old BacktestEngine. It walks bar-by-bar
through history, simulating exactly what mt5_worker would have polled
at each minute.

Usage:
    python scripts/backtester_v2.py GBPCAD 2026-04-08 2026-04-08
"""
from __future__ import annotations

import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import numpy as np

sys.stdout.reconfigure(encoding="utf-8")

# Repo on path
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

logging.basicConfig(level=logging.WARNING, format="%(message)s")
logger = logging.getLogger("backtester_v2")

# Live engines — IMPORT THE SAME ONES LIVE USES
from takumi_trader.core.strength import CalculationEngine, TIMEFRAME_LABELS, ALL_28_PAIRS, WARMUP_BARS
from takumi_trader.core.stoch_engine import StochStrengthEngine
from takumi_trader.core.filter_engine import FilterEngine, FilterSettings
from takumi_trader.core.htf_regime import HTFRegimeTracker
from takumi_trader.core.alerts import AlertManager
from takumi_trader.core.dukascopy_downloader import DukascopyDownloader

DATA = Path(r"D:\Trading\TAKUMI Trader\data")
JST = timezone(timedelta(hours=9))

# From main_window.py — these are NOT in BacktestConfig
MIN_DIVERGENCE_SPREAD = 12.0
NO_TRADE_START_MIN = 5 * 60        # 05:00 JST
NO_TRADE_END_MIN = 7 * 60 + 57     # 07:57 JST
TRADE_WINDOW_START = 7 * 60 + 58   # 07:58 JST
TRADE_WINDOW_END = 22 * 60         # 22:00 JST


def load_filter_settings_from_qsettings() -> FilterSettings:
    """Pull live's actual filter values from QSettings."""
    from PyQt6.QtCore import QSettings
    s = QSettings("TAKUMITrader", "TAKUMITrader")
    fs = FilterSettings()
    fs.trend_regime_enabled = s.value("filters/trend_regime", True, type=bool)
    fs.strength_velocity_enabled = s.value("filters/velocity", True, type=bool)
    fs.isolation_enabled = s.value("filters/isolation", True, type=bool)
    fs.structural_enabled = s.value("filters/structural", True, type=bool)
    fs.conviction_full_threshold = s.value("filters/conv_full", 70, type=int)
    fs.conviction_dimmed_threshold = s.value("filters/conv_dimmed", 45, type=int)
    return fs


def load_all_pair_bars(start_dt: datetime, end_dt: datetime) -> dict[str, np.ndarray]:
    """Load ALL available M1 bars (we walk through everything before
    start_dt to warm the engine state to live-equivalent values)."""
    dl = DukascopyDownloader(DATA / "dukascopy")
    bars_by_pair = {}
    for pair in ALL_28_PAIRS:
        arr = dl.load_pair(pair)
        if arr is None or len(arr) == 0:
            continue
        # Keep ALL bars up to end_dt (no early clipping)
        mask = arr["time"] <= int(end_dt.timestamp())
        clipped = arr[mask]
        if len(clipped) > 0:
            bars_by_pair[pair] = clipped
    return bars_by_pair


def resample_m1_to_tf(m1: np.ndarray, period_seconds: int) -> np.ndarray:
    """Resample M1 to higher TF, aligned to broker boundaries (XX:00, XX:05...)."""
    if len(m1) == 0:
        return m1
    aligned = (m1["time"] // period_seconds) * period_seconds
    unique_periods = np.unique(aligned)
    out = np.zeros(len(unique_periods), dtype=m1.dtype)
    for i, p in enumerate(unique_periods):
        mask = aligned == p
        group = m1[mask]
        out[i]["time"] = p
        out[i]["open"] = group["open"][0]
        out[i]["high"] = np.max(group["high"])
        out[i]["low"] = np.min(group["low"])
        out[i]["close"] = group["close"][-1]
        out[i]["tick_volume"] = np.sum(group["tick_volume"])
    return out


def slice_bars_up_to(bars: np.ndarray, t_now: int, n: int,
                     strict: bool = False) -> np.ndarray:
    """Return last n bars whose time <= t_now (or < if strict=True).

    For higher-TF bars (M5/M15/H1/H4/D1) we MUST use strict=True when
    feeding the stoch engine: at time T, the bar with timestamp==T is
    still FORMING (only contains the current M1 minute's data). Including
    it would use its FULLY-FORMED-LATER close as if it were available
    now — that's data leakage from the future.
    """
    if len(bars) == 0:
        return bars
    if strict:
        mask = bars["time"] < t_now
    else:
        mask = bars["time"] <= t_now
    eligible = bars[mask]
    if len(eligible) <= n:
        return eligible
    return eligible[-n:]


def compute_structural_data(pair: str, h1_bars: np.ndarray, t_now: int) -> dict | None:
    """Replicate live's structural_levels logic from mt5_worker.py:476-547."""
    if h1_bars is None or len(h1_bars) < 24:
        return None
    pip = 0.01 if "JPY" in pair else 0.0001

    # Filter out H1 bars from UTC hour 21 (junk wicks at IC Markets server reset)
    times = h1_bars["time"]
    utc_hours = (times % 86400) // 3600
    h1_clean = h1_bars[utc_hours != 21]
    if len(h1_clean) < 12:
        h1_clean = h1_bars

    # Server day resets at 21:00 UTC
    today_21 = (t_now // 86400) * 86400 + 21 * 3600
    if today_21 > t_now:
        today_21 -= 86400
    yesterday_21 = today_21 - 86400

    prev_day_mask = (h1_clean["time"] >= yesterday_21) & (h1_clean["time"] < today_21)
    prev_day = h1_clean[prev_day_mask]
    if len(prev_day) < 6:
        if len(h1_clean) >= 48:
            prev_day = h1_clean[-48:-24]
        else:
            return None

    prev_week_mask = h1_clean["time"] < today_21
    prev_week = h1_clean[prev_week_mask][-120:]
    if len(prev_week) < 24:
        prev_week = prev_day

    prev_month_mask = h1_clean["time"] < today_21
    prev_month = h1_clean[prev_month_mask][-720:]
    if len(prev_month) < 24:
        prev_month = prev_week

    return {
        "prev_day_high": float(np.max(prev_day["high"])),
        "prev_day_low": float(np.min(prev_day["low"])),
        "prev_week_high": float(np.max(prev_week["high"])),
        "prev_week_low": float(np.min(prev_week["low"])),
        "prev_month_high": float(np.max(prev_month["high"])),
        "prev_month_low": float(np.min(prev_month["low"])),
        "pip": pip,
    }


def main():
    if len(sys.argv) < 4:
        print("Usage: python scripts/backtester_v2.py <PAIR> <YYYY-MM-DD start> <YYYY-MM-DD end>")
        return 1
    pair = sys.argv[1].upper()
    start_str = sys.argv[2]
    end_str = sys.argv[3]
    start_dt = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = (datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
              + timedelta(days=1) - timedelta(seconds=1))

    print(f"Live-faithful backtester v2")
    print(f"  Pair: {pair}")
    print(f"  Period: {start_str} → {end_str} (UTC)")
    print()

    # ── Settings (FROM LIVE QSETTINGS, not defaults) ──
    fs = load_filter_settings_from_qsettings()
    print(f"Filter settings (from QSettings):")
    print(f"  trend_regime_enabled = {fs.trend_regime_enabled}")
    print(f"  velocity_enabled     = {fs.strength_velocity_enabled}")
    print(f"  isolation_enabled    = {fs.isolation_enabled}")
    print(f"  structural_enabled   = {fs.structural_enabled}")
    print(f"  conviction_full      = {fs.conviction_full_threshold}")
    print(f"  conviction_dimmed    = {fs.conviction_dimmed_threshold}")
    print()

    # ── Load all pair M1 bars + resample to higher TFs ──
    print(f"Loading bars for all 28 pairs from Dukascopy...")
    m1_by_pair = load_all_pair_bars(start_dt, end_dt)
    print(f"  Loaded M1 for {len(m1_by_pair)} pairs")

    if pair not in m1_by_pair:
        print(f"  ERROR: target pair {pair} has no data")
        return 1

    bars_by_pair_tf: dict[str, dict[str, np.ndarray]] = {}
    for p, m1 in m1_by_pair.items():
        bars_by_pair_tf[p] = {
            "M1": m1,
            "M5": resample_m1_to_tf(m1, 300),
            "M15": resample_m1_to_tf(m1, 900),
            "H1": resample_m1_to_tf(m1, 3600),
            "H4": resample_m1_to_tf(m1, 14400),
            "D1": resample_m1_to_tf(m1, 86400),
        }

    # ── Initialize engines (SAME CLASSES AS LIVE) ──
    engine = CalculationEngine()
    stoch_engine = StochStrengthEngine()
    htf_regime = HTFRegimeTracker()
    filter_engine = FilterEngine(settings=fs)
    alert_mgr = AlertManager(cooldown_seconds=60)

    # ── Bootstrap engines with warmup data (200 bars before start_dt) ──
    print(f"Bootstrapping engines with warmup data...")
    warmup_start_ts = int(start_dt.timestamp())
    warmup_data: dict[str, dict[str, np.ndarray]] = {}
    for p in ALL_28_PAIRS:
        if p not in bars_by_pair_tf:
            continue
        warmup_data[p] = {}
        for tf in TIMEFRAME_LABELS:  # M1, M5, M15, H1
            arr = bars_by_pair_tf[p][tf]
            mask = arr["time"] < warmup_start_ts
            warmup_data[p][tf] = arr[mask][-WARMUP_BARS:] if (arr[mask].size >= WARMUP_BARS) else arr[mask]
    engine.bootstrap(warmup_data)

    # Bootstrap HTF regime
    htf_warmup: dict[str, dict[str, np.ndarray]] = {}
    for p in ALL_28_PAIRS:
        if p not in bars_by_pair_tf:
            continue
        htf_warmup[p] = {}
        for tf in ("H4", "D1"):
            arr = bars_by_pair_tf[p][tf]
            mask = arr["time"] < warmup_start_ts
            htf_warmup[p][tf] = arr[mask]
    htf_regime.bootstrap(htf_warmup)

    # Bootstrap stoch engine on M5/M15
    for tf in ("M5", "M15", "H1"):
        stoch_warmup: dict[str, np.ndarray] = {}
        for p in ALL_28_PAIRS:
            if p not in bars_by_pair_tf:
                continue
            arr = bars_by_pair_tf[p][tf]
            mask = arr["time"] < warmup_start_ts
            sliced = arr[mask][-30:]
            if len(sliced) > 3:
                stoch_warmup[p] = sliced
        if stoch_warmup:
            stoch_engine.compute_tf(stoch_warmup, tf)

    # Compute initial stoch velocity
    composite = stoch_engine.get_composite(["M5", "M15"])
    stoch_engine.update_velocity(composite)

    print(f"  Bootstrap complete.")
    print()

    # ── Pre-warm engine state by walking through ALL pre-window bars ──
    # The CalculationEngine maintains rolling z-score buffers that depend
    # on accumulated history. To match live's state (which has been running
    # for weeks), we walk through ALL pre-start_dt bars feeding the engine
    # but NOT checking entries.
    print(f"Pre-warming engine state through pre-window history...")
    target_m1 = bars_by_pair_tf[pair]["M1"]
    pre_mask = target_m1["time"] < int(start_dt.timestamp())
    pre_bars = target_m1[pre_mask]
    print(f"  Walking through {len(pre_bars)} pre-window M1 bars to warm state")

    # Initialise last_candle_time using the data slice end of bootstrap
    last_candle_time: dict[tuple, int] = {}
    for p in ALL_28_PAIRS:
        if p not in bars_by_pair_tf:
            continue
        for tf in ("M1", "M5", "M15", "H1"):
            arr = bars_by_pair_tf[p][tf]
            mask = arr["time"] < warmup_start_ts
            if mask.any():
                last_candle_time[(p, tf)] = int(arr[mask][-1]["time"])

    # Walk through pre-window bars (state-only; no entries checked)
    for pre_idx in range(0, len(pre_bars), 1):
        m1_time = int(pre_bars[pre_idx]["time"])
        if m1_time < warmup_start_ts:
            continue
        # Lightweight cycle: just feed engines, no entry check
        m1_data = {}; m5_data = {}; m15_data = {}; h1_data = {}
        m5_new = m15_new = h1_new = False
        for p in ALL_28_PAIRS:
            if p not in bars_by_pair_tf:
                continue
            for tf, td, strict_use in [
                ("M1", m1_data, False), ("M5", m5_data, True),
                ("M15", m15_data, True), ("H1", h1_data, True),
            ]:
                sliced = slice_bars_up_to(bars_by_pair_tf[p][tf], m1_time, 60,
                                          strict=strict_use)
                if len(sliced) > 0:
                    td[p] = sliced
                    t_last = int(sliced[-1]["time"])
                    key = (p, tf)
                    prev_t = last_candle_time.get(key)
                    if prev_t is not None and t_last != prev_t:
                        if tf == "M5": m5_new = True
                        elif tf == "M15": m15_new = True
                        elif tf == "H1": h1_new = True
                    last_candle_time[key] = t_last
        for tf, td in [("M1", m1_data), ("M5", m5_data),
                       ("M15", m15_data), ("H1", h1_data)]:
            if td:
                engine.compute(td, tf, update_zscore=True)
        # Feed stoch on closes (with forming bar)
        for tf, did_close, period_sec in [
            ("M5", m5_new, 300), ("M15", m15_new, 900), ("H1", h1_new, 3600),
        ]:
            if not did_close:
                continue
            stoch_pair_data = {}
            period_start = (m1_time // period_sec) * period_sec
            for p in ALL_28_PAIRS:
                if p not in bars_by_pair_tf:
                    continue
                closed = slice_bars_up_to(bars_by_pair_tf[p][tf],
                                          period_start - 1, 30, strict=False)
                m1_arr = bars_by_pair_tf[p]["M1"]
                form_mask = (m1_arr["time"] >= period_start) & (m1_arr["time"] <= m1_time)
                form_bars = m1_arr[form_mask]
                if len(form_bars) > 0:
                    forming = np.zeros(1, dtype=closed.dtype)
                    forming[0]["time"] = period_start
                    forming[0]["open"] = form_bars["open"][0]
                    forming[0]["high"] = np.max(form_bars["high"])
                    forming[0]["low"] = np.min(form_bars["low"])
                    forming[0]["close"] = form_bars["close"][-1]
                    if "tick_volume" in closed.dtype.names:
                        forming[0]["tick_volume"] = np.sum(form_bars["tick_volume"])
                    combined = np.concatenate([closed, forming])
                else:
                    combined = closed
                if len(combined) > 3:
                    stoch_pair_data[p] = combined
            if stoch_pair_data:
                stoch_engine.compute_tf(stoch_pair_data, tf)
        if m5_new:
            comp = stoch_engine.get_composite(["M5", "M15"])
            stoch_engine.update_velocity(comp)
    print(f"  Pre-warm complete. Engine state should now resemble live.")
    print()

    # ── Walk forward minute by minute (entry checking) ──
    print(f"Walking forward minute-by-minute...")
    sim_mask = (target_m1["time"] >= int(start_dt.timestamp())) & \
               (target_m1["time"] <= int(end_dt.timestamp()))
    sim_bars = target_m1[sim_mask]
    print(f"  Simulating {len(sim_bars)} M1 bars in target window")
    print()

    # last_candle_time was initialised + advanced during pre-warm phase

    entries_fired = []  # list of (time, pair, direction, conviction, tier, spread)
    diagnostics: dict = {}
    stage_stats = {"stoch": 0, "spread": 0, "conv": 0, "tier": 0, "cooldown": 0}
    last_reject: dict = {}  # (HH:MM, dir) → (stage, reason)
    cycle_count = 0
    close_event_count = 0

    for sim_idx in range(len(sim_bars)):
        m1_time = int(sim_bars[sim_idx]["time"])

        # Build current bar snapshots for all pairs (live equivalent of mt5.copy_rates_from_pos)
        m1_data: dict[str, np.ndarray] = {}
        m5_data: dict[str, np.ndarray] = {}
        m15_data: dict[str, np.ndarray] = {}
        h1_data: dict[str, np.ndarray] = {}
        m5_new_candle = False
        m15_new_candle = False
        h1_new_candle = False

        for p in ALL_28_PAIRS:
            if p not in bars_by_pair_tf:
                continue
            for tf, n_bars, target_dict, use_strict in [
                ("M1", 60, m1_data, False),  # M1 = the bar that just closed
                ("M5", 60, m5_data, True),   # higher TFs must exclude forming bar
                ("M15", 60, m15_data, True),
                ("H1", 60, h1_data, True),
            ]:
                sliced = slice_bars_up_to(bars_by_pair_tf[p][tf], m1_time,
                                          n_bars, strict=use_strict)
                if len(sliced) > 0:
                    target_dict[p] = sliced
                    # Detect new candle close
                    t_last = int(sliced[-1]["time"])
                    key = (p, tf)
                    prev_t = last_candle_time.get(key)
                    if prev_t is not None and t_last != prev_t:
                        if tf == "M5": m5_new_candle = True
                        elif tf == "M15": m15_new_candle = True
                        elif tf == "H1": h1_new_candle = True
                    last_candle_time[key] = t_last

        # ── Update CalculationEngine for all TFs (mirrors live) ──
        cached_results_live = {}
        for tf, tf_data in [("M1", m1_data), ("M5", m5_data),
                             ("M15", m15_data), ("H1", h1_data)]:
            if tf_data:
                tf_result = engine.compute(tf_data, tf, update_zscore=True)
                cached_results_live[tf] = tf_result

        # ── Update HTF regime on H4/D1 closes (use closed bars only) ──
        if h1_new_candle:
            h4_data = {}
            d1_data = {}
            for p in ALL_28_PAIRS:
                if p not in bars_by_pair_tf:
                    continue
                h4 = slice_bars_up_to(bars_by_pair_tf[p]["H4"], m1_time, 50, strict=True)
                d1 = slice_bars_up_to(bars_by_pair_tf[p]["D1"], m1_time, 20, strict=True)
                if len(h4) > 0: h4_data[p] = h4
                if len(d1) > 0: d1_data[p] = d1
            if h4_data: htf_regime.update(h4_data, "H4")
            if d1_data: htf_regime.update(d1_data, "D1")

        # ── Update stoch engine ONLY ON CLOSES (matching live) ──
        # Live's behaviour: includes a FORMING bar (timestamp = current period
        # start, content = whatever ticks have arrived since). To replicate,
        # we append a synthetic forming bar built from the M1 bar just closed,
        # but ONLY if its M1-time is past the period boundary already.
        for tf, did_close, period_sec in [
            ("M5", m5_new_candle, 300),
            ("M15", m15_new_candle, 900),
            ("H1", h1_new_candle, 3600),
        ]:
            if not did_close:
                continue
            stoch_pair_data = {}
            # Forming bar starts at the period containing m1_time
            period_start = (m1_time // period_sec) * period_sec
            for p in ALL_28_PAIRS:
                if p not in bars_by_pair_tf:
                    continue
                # Closed bars: time < period_start
                closed = slice_bars_up_to(bars_by_pair_tf[p][tf],
                                          period_start - 1, 30, strict=False)
                # Forming bar: M1 bars in current period_start..m1_time
                m1_arr = bars_by_pair_tf[p]["M1"]
                form_mask = (m1_arr["time"] >= period_start) & \
                            (m1_arr["time"] <= m1_time)
                form_bars = m1_arr[form_mask]
                if len(form_bars) > 0:
                    # Build synthetic forming HTF bar
                    forming = np.zeros(1, dtype=closed.dtype)
                    forming[0]["time"] = period_start
                    forming[0]["open"] = form_bars["open"][0]
                    forming[0]["high"] = np.max(form_bars["high"])
                    forming[0]["low"] = np.min(form_bars["low"])
                    forming[0]["close"] = form_bars["close"][-1]
                    if "tick_volume" in closed.dtype.names:
                        forming[0]["tick_volume"] = np.sum(form_bars["tick_volume"])
                    combined = np.concatenate([closed, forming])
                else:
                    combined = closed
                if len(combined) > 3:
                    stoch_pair_data[p] = combined
            if stoch_pair_data:
                stoch_engine.compute_tf(stoch_pair_data, tf)

        # Update velocity on M5 close
        if m5_new_candle:
            comp = stoch_engine.get_composite(["M5", "M15"])
            stoch_engine.update_velocity(comp)

        cycle_count += 1
        # ── Skip if no new HTF close (no point checking entries) ──
        if not (m5_new_candle or m15_new_candle or h1_new_candle):
            continue
        close_event_count += 1

        # ── Trading window check ──
        dt_jst = datetime.fromtimestamp(m1_time, tz=JST)
        mins_jst = dt_jst.hour * 60 + dt_jst.minute
        if not (TRADE_WINDOW_START <= mins_jst < TRADE_WINDOW_END):
            continue

        # ── Check entry for target pair ──
        base, quote = pair[:3], pair[3:]

        # Diagnostic dump: show stoch scores at specific minutes when live had trades
        import os as _os
        if _os.environ.get("DUMP_SCORES") == "1":
            target_minutes = [(19, 24), (19, 56), (20, 40)]
            if (dt_jst.hour, dt_jst.minute) in target_minutes:
                print(f"\n  [SCORES at {dt_jst.strftime('%m-%d %H:%M')} JST]")
                for tf in ["M5", "M15", "H1", "H4"]:
                    sc = stoch_engine._cached_scores.get(tf, {})
                    bv = sc.get(base); qv = sc.get(quote)
                    bs = f"{bv:>5.2f}" if isinstance(bv, (int, float)) else "  ? "
                    qs = f"{qv:>5.2f}" if isinstance(qv, (int, float)) else "  ? "
                    print(f"    {tf:<4} {base}={bs}  {quote}={qs}")

        for direction in ("BUY", "SELL"):
            ok, reason = stoch_engine.check_entry(base, quote, direction)
            stage_key = (dt_jst.strftime("%H:%M"), direction)
            if not ok:
                # Track rejection at stoch stage
                stage_stats["stoch"] += 1
                last_reject[stage_key] = ("stoch", reason)
                continue

            # MIN_DIVERGENCE_SPREAD check — uses CalculationEngine composite,
            # NOT stoch engine. Live builds composite by averaging
            # cached_results_live across M1/M5/M15/H1 (mt5_worker.py:565-575)
            calc_composite: dict[str, float] = {}
            from takumi_trader.core.strength import CURRENCIES as _CURRENCIES
            for ccy in _CURRENCIES:
                total = 0.0; cnt = 0
                for tf in TIMEFRAME_LABELS:
                    tr = cached_results_live.get(tf)
                    if tr and ccy in tr.currency_scores:
                        total += tr.currency_scores[ccy]
                        cnt += 1
                if cnt > 0:
                    calc_composite[ccy] = total / cnt
            base_comp = calc_composite.get(base, 5.0)
            quote_comp = calc_composite.get(quote, 5.0)
            spread = abs(base_comp - quote_comp)
            if spread < MIN_DIVERGENCE_SPREAD:
                stage_stats["spread"] += 1
                last_reject[stage_key] = ("spread", f"{spread:.2f}<{MIN_DIVERGENCE_SPREAD}")
                continue

            # Conviction filter
            try:
                # Compute structural data for this pair (matching live)
                structural_data = compute_structural_data(
                    pair, bars_by_pair_tf[pair]["H1"], m1_time,
                )
                # H1 ATR for TP clearance
                h1_arr = bars_by_pair_tf[pair]["H1"]
                h1_atr = 0.0
                if len(h1_arr) >= 14:
                    h1_recent = slice_bars_up_to(h1_arr, m1_time, 14)
                    if len(h1_recent) >= 14:
                        trs = []
                        for i in range(1, len(h1_recent)):
                            h = h1_recent[i]["high"]; l = h1_recent[i]["low"]
                            pc = h1_recent[i-1]["close"]
                            trs.append(max(h-l, abs(h-pc), abs(l-pc)))
                        h1_atr = sum(trs) / len(trs)
                pip = 0.01 if "JPY" in pair else 0.0001
                tp_pips = (h1_atr * 0.5) / pip if h1_atr > 0 else 10  # 0.5 ATR

                # entry price
                entry_price = float(m1_data[pair][-1]["close"]) if pair in m1_data else 0.0

                # velocity_data + composite_scores for FilterEngine
                velocity_data = {}
                for c in ['EUR','GBP','USD','JPY','CHF','AUD','CAD','NZD']:
                    velocity_data[c] = htf_regime.get_velocity(c)

                strong_ccy = base if direction == "BUY" else quote
                weak_ccy = quote if direction == "BUY" else base

                conv_result = filter_engine.evaluate(
                    strong_ccy=strong_ccy,
                    weak_ccy=weak_ccy,
                    pair=pair,
                    direction=direction,
                    htf_regimes=htf_regime.get_all_regimes(),
                    velocity_data=velocity_data,
                    composite_scores=calc_composite,  # use CalculationEngine, matching live
                    structural_data=structural_data,
                    entry_price=entry_price,
                    tp_pips=tp_pips,
                )
            except Exception as exc:
                logger.debug("conviction calc failed: %s", exc)
                stage_stats["conv"] += 1
                last_reject[stage_key] = ("conv_calc", str(exc)[:50])
                continue

            # Tier == FULL filter
            if conv_result.tier != "FULL":
                stage_stats["tier"] += 1
                last_reject[stage_key] = ("tier",
                    f"conv={conv_result.conviction} tier={conv_result.tier}")
                continue

            # Alert manager cooldown (60s per pair)
            # Note: we use m1_time (in seconds) directly
            last_fire = alert_mgr._last_alert_times.get(pair, 0) if hasattr(alert_mgr, '_last_alert_times') else 0
            if last_fire and (m1_time - last_fire) < alert_mgr.cooldown_seconds:
                stage_stats["cooldown"] += 1
                last_reject[stage_key] = ("cooldown", f"{m1_time-last_fire}s ago")
                continue
            # Manually update last_alert_times since check_and_fire uses time.time()
            if not hasattr(alert_mgr, '_last_alert_times'):
                alert_mgr._last_alert_times = {}
            alert_mgr._last_alert_times[pair] = m1_time

            # ── Entry FIRED ──
            entries_fired.append({
                "time": m1_time,
                "pair": pair,
                "direction": direction,
                "conviction": conv_result.conviction,
                "tier": conv_result.tier,
                "spread": spread,
                "reason": reason,
            })
            break  # one direction per cycle

    # ── Report ──
    print(f"\nWalked {cycle_count} M1 cycles; {close_event_count} had a HTF close event")
    print(f"\nRejection stats per stage:")
    for stage, n in stage_stats.items():
        print(f"  {stage:<10} {n:>5}")
    # Show last-reject for the exact minutes live had trades
    print(f"\nLast rejection at minutes when live had trades:")
    live_minutes = ["19:24", "19:56", "20:40", "21:30", "08:35"]
    for m in live_minutes:
        for d in ("BUY", "SELL"):
            lr = last_reject.get((m, d))
            if lr:
                print(f"  {m} {d:<5}: {lr}")
    print(f"\nBacktester v2 fired {len(entries_fired)} entries:")
    print(f"  {'Entry JST':<22} {'Dir':<5} {'Conv':>5} {'Tier':<6} {'Spread':>7}")
    for e in entries_fired:
        dt = datetime.fromtimestamp(e["time"], tz=JST).strftime("%m-%d %H:%M JST")
        print(f"  {dt:<22} {e['direction']:<5} {e['conviction']:>4}  "
              f"{e['tier']:<6} {e['spread']:>6.2f}")

    # ── Compare to live ──
    import json
    recs = json.loads((DATA / "paper_trades.json").read_text(encoding="utf-8"))
    live = [r for r in recs
            if r.get("pair") == pair and r.get("close_reason")
            and start_dt.timestamp() <= r.get("entry_time", 0) <= end_dt.timestamp()]
    live.sort(key=lambda r: r["entry_time"])

    print(f"\nLive Sv2 trades on {pair} in same window: {len(live)}")
    print(f"  {'Entry JST':<22} {'Dir':<5} {'Conv':>5} {'Tier':<6}")
    for r in live:
        dt = datetime.fromtimestamp(r["entry_time"], tz=JST).strftime("%m-%d %H:%M JST")
        tier = r.get("entry_tier") or "(?)"
        print(f"  {dt:<22} {r['direction']:<5} {r.get('entry_conviction',0):>4}  {tier:<6}")

    # Match analysis: ±5 minutes, same direction
    matches = 0
    for r in live:
        cands = [e for e in entries_fired
                 if e["direction"] == r.get("direction")
                 and abs(e["time"] - r["entry_time"]) <= 300]
        if cands:
            matches += 1
    print(f"\nMatch rate: {matches}/{len(live)} "
          f"({matches/len(live)*100 if live else 0:.0f}%)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
