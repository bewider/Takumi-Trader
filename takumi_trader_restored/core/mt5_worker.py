"""QThread worker for MT5 connection, data fetching, and strength calculations.

Lifecycle:
  1. Connect to MT5 terminal.
  2. Warmup: fetch 200 bars for all 28 pairs × 4 TFs + D1 for ADR, bootstrap.
  3. Live loop (1s cycle):
     - Always update M1 (with tick velocity).
     - Update M5 every 3s, M15 every 10s, H1 every 30s.
     - Detect candle closes to update Z-score buffers.
     - On M1 close: run range detection for all pairs.
     - On H1 close: refresh ADR cache.
     - Emit CalculationResult via signal.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from PyQt6.QtCore import QThread, pyqtSignal

from takumi_trader.core.htf_regime import HTFRegimeTracker
from takumi_trader.core.range_engine import RangeEngine
from takumi_trader.core.session_manager import get_current_session, get_session_label
from takumi_trader.core.session_range import SessionRangeTracker
from takumi_trader.core.strength import (
    ALL_28_PAIRS,
    CURRENCIES,
    DISPLAY_PAIRS,
    LIVE_FETCH_BARS,
    TIMEFRAME_LABELS,
    WARMUP_BARS,
    CalculationEngine,
    CalculationResult,
    TimeframeResult,
    compute_atr,
)
from takumi_trader.core.tick_flow_tracker import TickFlowTracker

logger = logging.getLogger(__name__)

# MT5 timeframe constants — resolved lazily after import
_MT5_TIMEFRAMES: dict[str, int] | None = None


def _get_mt5_timeframes() -> dict[str, int]:
    global _MT5_TIMEFRAMES
    if _MT5_TIMEFRAMES is None:
        import MetaTrader5 as mt5

        _MT5_TIMEFRAMES = {
            "M1": mt5.TIMEFRAME_M1,
            "M5": mt5.TIMEFRAME_M5,
            "M15": mt5.TIMEFRAME_M15,
            "H1": mt5.TIMEFRAME_H1,
        }
    return _MT5_TIMEFRAMES


class MT5Worker(QThread):
    """Background worker that polls MT5 and emits calculation results."""

    data_ready = pyqtSignal(object)  # emits CalculationResult
    connection_status = pyqtSignal(bool, str)  # (connected, message)

    def __init__(self, poll_interval: float = 1.0) -> None:
        super().__init__()
        self.poll_interval = poll_interval
        self._running = True
        self._connected = False

    def stop(self) -> None:
        self._running = False

    def _try_connect(self) -> bool:
        import MetaTrader5 as mt5

        if mt5.initialize():
            self._connected = True
            info = mt5.terminal_info()
            broker = info.company if info else "Unknown"
            self.connection_status.emit(True, f"Connected to {broker}")
            logger.info("MT5 connected: %s", broker)
            return True
        else:
            error = mt5.last_error()
            msg = f"MT5 init failed: {error}"
            self.connection_status.emit(False, msg)
            logger.warning(msg)
            return False

    def run(self) -> None:  # noqa: C901
        import MetaTrader5 as mt5

        # ── 1. Connect ───────────────────────────────────────────
        while self._running and not self._connected:
            if self._try_connect():
                break
            self.connection_status.emit(False, "MT5 not connected — retrying…")
            for _ in range(50):
                if not self._running:
                    return
                time.sleep(0.1)

        tf_map = _get_mt5_timeframes()

        # ── 2. Warmup phase ──────────────────────────────────────
        self.connection_status.emit(True, "Connected — warming up indicators…")

        # Use optimized params — EMA4 for faster reaction (A+B+C combo)
        engine = CalculationEngine(sensitivity=1.0, ema_period=4, roc_decay=0.2)
        range_engine = RangeEngine()
        flow_tracker = TickFlowTracker()
        session_range_tracker = SessionRangeTracker()
        htf_regime = HTFRegimeTracker()
        warmup_data: dict[str, dict[str, Any]] = {}
        htf_warmup: dict[str, dict[str, Any]] = {}  # pair -> {H4/D1 -> candles}

        for pair in ALL_28_PAIRS:
            if not self._running:
                return
            warmup_data[pair] = {}
            for tf_label, tf_const in tf_map.items():
                try:
                    candles = mt5.copy_rates_from_pos(pair, tf_const, 0, WARMUP_BARS)
                    if candles is not None and len(candles) > 0:
                        warmup_data[pair][tf_label] = candles
                except Exception:
                    logger.warning("Warmup fetch failed: %s %s", pair, tf_label)

            # Fetch D1 data for ADR calculation
            try:
                d1_candles = mt5.copy_rates_from_pos(
                    pair, mt5.TIMEFRAME_D1, 0, 15
                )
                if d1_candles is not None and len(d1_candles) > 0:
                    range_engine.update_adr(pair, d1_candles)
            except Exception:
                logger.warning("D1 fetch failed for ADR: %s", pair)

            # Fetch 2 weeks of H1 data for session-filtered ADR
            try:
                h1_2w = mt5.copy_rates_from_pos(
                    pair, tf_map["H1"], 0, 340
                )
                if h1_2w is not None and len(h1_2w) > 0:
                    session_range_tracker.update_adr(pair, h1_2w)
                    session_range_tracker.update_today(
                        pair, h1_2w,
                        warmup_data.get(pair, {}).get("M1"),
                    )
            except Exception:
                logger.warning("H1 session range fetch failed: %s", pair)

            # Fetch H4 and D1 for HTF regime
            htf_warmup[pair] = {}
            try:
                h4_candles = mt5.copy_rates_from_pos(
                    pair, mt5.TIMEFRAME_H4, 0, 50
                )
                if h4_candles is not None and len(h4_candles) > 0:
                    htf_warmup[pair]["H4"] = h4_candles
            except Exception:
                logger.warning("H4 warmup fetch failed: %s", pair)
            try:
                d1_candles_htf = mt5.copy_rates_from_pos(
                    pair, mt5.TIMEFRAME_D1, 0, 20
                )
                if d1_candles_htf is not None and len(d1_candles_htf) > 0:
                    htf_warmup[pair]["D1"] = d1_candles_htf
            except Exception:
                logger.warning("D1 HTF warmup fetch failed: %s", pair)

        engine.bootstrap(warmup_data)
        htf_regime.bootstrap(htf_warmup)

        # Track candle timestamps for close detection
        last_candle_time: dict[tuple[str, str], int] = {}
        for pair in ALL_28_PAIRS:
            for tf in TIMEFRAME_LABELS:
                candles = warmup_data.get(pair, {}).get(tf)
                if candles is not None and len(candles) > 0:
                    last_candle_time[(pair, tf)] = int(candles[-1]["time"])

        # Signal ready
        info = mt5.terminal_info()
        broker = info.company if info else "Unknown"
        self.connection_status.emit(True, f"Connected to {broker}")

        # Cache last results per TF — seed from warmup so HTF scores
        # are available immediately (not empty until first candle close)
        cached_results: dict[str, TimeframeResult] = {}
        for tf_label in TIMEFRAME_LABELS:
            warmup_candles: dict[str, Any] = {}
            for pair in ALL_28_PAIRS:
                c = warmup_data.get(pair, {}).get(tf_label)
                if c is not None and len(c) > 0:
                    warmup_candles[pair] = c
            if warmup_candles:
                cached_results[tf_label] = engine.compute(
                    warmup_candles, tf_label, update_zscore=False
                )
        logger.info("HTF cache seeded: %s", list(cached_results.keys()))

        # Cache M1 candle data for range detection
        cached_m1_data: dict[str, Any] = {}
        # Cache H1 candle data for session range
        cached_h1_data: dict[str, Any] = {}

        # ── 3. Live loop ─────────────────────────────────────────
        cycle = 0

        while self._running:
            cycle_start = time.monotonic()

            # Check connection
            if not mt5.terminal_info():
                self._connected = False
                self.connection_status.emit(False, "MT5 connection lost — retrying…")
                while self._running and not self._try_connect():
                    for _ in range(50):
                        if not self._running:
                            return
                        time.sleep(0.1)
                if not self._running:
                    return

            result = CalculationResult(connected=True)

            # Decide which TFs to process this cycle
            tfs_this_cycle = ["M1"]
            if cycle % 3 == 0:
                tfs_this_cycle.append("M5")
            if cycle % 10 == 0:
                tfs_this_cycle.append("M15")
            if cycle % 30 == 0:
                tfs_this_cycle.append("H1")

            m1_new_candle = False
            h1_new_candle = False

            for tf_label in tfs_this_cycle:
                tf_const = tf_map[tf_label]
                candle_data: dict[str, Any] = {}
                new_candle = False
                fetch_bars = LIVE_FETCH_BARS.get(tf_label, 50)

                for pair in ALL_28_PAIRS:
                    try:
                        candles = mt5.copy_rates_from_pos(
                            pair, tf_const, 0, fetch_bars
                        )
                    except Exception:
                        logger.debug("Fetch failed: %s %s", pair, tf_label)
                        continue

                    if candles is not None and len(candles) > 0:
                        candle_data[pair] = candles

                        # Detect candle close
                        t = int(candles[-1]["time"])
                        prev = last_candle_time.get((pair, tf_label))
                        if prev is not None and t != prev:
                            new_candle = True
                        last_candle_time[(pair, tf_label)] = t

                if candle_data:
                    tf_result = engine.compute(
                        candle_data, tf_label, update_zscore=new_candle
                    )

                    # M1: always update (need real-time entry detection)
                    # M5/M15/H1: ONLY update on candle close to match
                    # backtest behavior — prevents premature spread-collapse
                    # exits from forming-candle noise
                    if tf_label == "M1":
                        cached_results[tf_label] = tf_result
                        cached_m1_data = candle_data
                        m1_new_candle = new_candle
                    elif new_candle:
                        # HTF: only update cached result on candle close
                        cached_results[tf_label] = tf_result
                        if tf_label == "H1":
                            cached_h1_data = candle_data
                            h1_new_candle = True
                    else:
                        # HTF forming candle: compute for internal state
                        # but DON'T update cached_results — keep last close
                        if tf_label == "H1":
                            h1_new_candle = False

            # ── H4/D1 regime update (every 120s check for H4, 300s for D1) ──
            if cycle % 120 == 0 or h1_new_candle:
                h4_data: dict[str, Any] = {}
                for pair in ALL_28_PAIRS:
                    try:
                        h4c = mt5.copy_rates_from_pos(
                            pair, mt5.TIMEFRAME_H4, 0, 50
                        )
                        if h4c is not None and len(h4c) > 0:
                            h4_data[pair] = h4c
                    except Exception:
                        pass
                if h4_data:
                    htf_regime.update(h4_data, "H4")

            if cycle % 300 == 0 or h1_new_candle:
                d1_data: dict[str, Any] = {}
                for pair in ALL_28_PAIRS:
                    try:
                        d1c = mt5.copy_rates_from_pos(
                            pair, mt5.TIMEFRAME_D1, 0, 20
                        )
                        if d1c is not None and len(d1c) > 0:
                            d1_data[pair] = d1c
                    except Exception:
                        pass
                if d1_data:
                    htf_regime.update(d1_data, "D1")

            # ── Refresh ADR on H1 close ──
            if h1_new_candle:
                for pair in ALL_28_PAIRS:
                    try:
                        d1 = mt5.copy_rates_from_pos(
                            pair, mt5.TIMEFRAME_D1, 0, 15
                        )
                        if d1 is not None and len(d1) > 0:
                            range_engine.update_adr(pair, d1)
                    except Exception:
                        pass

            # ── Session range: refresh ADR on H1 close, update today on every cycle ──
            if h1_new_candle:
                for pair in ALL_28_PAIRS:
                    h1c = cached_h1_data.get(pair)
                    if h1c is not None:
                        session_range_tracker.update_adr(pair, h1c)

            # Update today's session high/low from H1 + M1
            for pair in ALL_28_PAIRS:
                h1c = cached_h1_data.get(pair)
                m1c = cached_m1_data.get(pair)
                if h1c is not None or m1c is not None:
                    session_range_tracker.update_today(pair, h1c, m1c)

            # ── Range detection on M1 close ──
            if m1_new_candle and cached_m1_data:
                ccy_per_tf: dict[str, dict[str, float]] = {}
                for tf in TIMEFRAME_LABELS:
                    if tf in cached_results:
                        ccy_per_tf[tf] = cached_results[tf].currency_scores

                # Current prices for breakout detection
                tick_prices: dict[str, float] = {}
                for pair, candles in cached_m1_data.items():
                    if len(candles) > 0:
                        tick_prices[pair] = float(candles[-1]["close"])

                range_states = range_engine.detect_all(
                    cached_m1_data, ccy_per_tf, tick_prices
                )
                result.range_states = range_states

            # ── Tick flow update on M1 data ──
            if cached_m1_data:
                for pair, candles in cached_m1_data.items():
                    flow_tracker.update_from_candles(pair, candles)

            # Build full result from cached values
            for tf in TIMEFRAME_LABELS:
                if tf in cached_results:
                    result.timeframes[tf] = cached_results[tf]

            # ── Close prices for trade tracking ──
            if cached_m1_data:
                for pair, candles in cached_m1_data.items():
                    if len(candles) > 0:
                        result.close_prices[pair] = float(candles[-1]["close"])
                        result.high_prices[pair] = float(candles[-1]["high"])
                        result.low_prices[pair] = float(candles[-1]["low"])
                        # M1 bar timestamp (same for all pairs — use first found)
                        if result.m1_bar_time == 0:
                            result.m1_bar_time = int(candles[-1]["time"])

            # ── H1 ATR(14) per pair for dynamic SL/TP ──
            if cached_h1_data:
                import numpy as np
                for pair, h1c in cached_h1_data.items():
                    if h1c is not None and len(h1c) >= 15:
                        try:
                            high = h1c["high"].astype(np.float64)
                            low = h1c["low"].astype(np.float64)
                            close = h1c["close"].astype(np.float64)
                            atr_arr = compute_atr(high, low, close, period=14)
                            result.h1_atr[pair] = float(atr_arr[-1])
                        except Exception:
                            pass

            # ── Structural levels (key levels for filter) ──
            if cached_h1_data:
                import numpy as np
                for pair, h1c in cached_h1_data.items():
                    if h1c is not None and len(h1c) >= 24:
                        try:
                            _pip = 0.01 if "JPY" in pair else 0.0001
                            result.structural_levels[pair] = {
                                "prev_day_high": float(np.max(h1c[-24:]["high"])),
                                "prev_day_low": float(np.min(h1c[-24:]["low"])),
                                "prev_week_high": float(np.max(h1c[-120:]["high"])) if len(h1c) >= 120 else float(np.max(h1c["high"])),
                                "prev_week_low": float(np.min(h1c[-120:]["low"])) if len(h1c) >= 120 else float(np.min(h1c["low"])),
                                "pip": _pip,
                            }
                        except Exception:
                            pass

            # ── Session range consumed percentages ──
            result.session_range_pct = session_range_tracker.get_all_consumed_pct()

            # ── Session info ──
            result.session_label = get_session_label()

            # ── Flow states ──
            result.flow_states = flow_tracker.get_all_states()

            # ── Momentum acceleration tracking ──
            composite_scores: dict[str, float] = {}
            htf_composite_scores: dict[str, float] = {}
            _HTF_ONLY = {"M5", "M15", "H1"}
            for ccy in CURRENCIES:
                total = 0.0
                count = 0
                htf_total = 0.0
                htf_count = 0
                for tf in TIMEFRAME_LABELS:
                    tr = cached_results.get(tf)
                    if tr and ccy in tr.currency_scores:
                        score = tr.currency_scores[ccy]
                        total += score
                        count += 1
                        if tf in _HTF_ONLY:
                            htf_total += score
                            htf_count += 1
                if count > 0:
                    composite_scores[ccy] = total / count
                if htf_count > 0:
                    htf_composite_scores[ccy] = htf_total / htf_count

            if composite_scores:
                # Build per-TF scores dict for acceleration tracking
                _tf_scores: dict[str, dict[str, float]] = {}
                for tf in TIMEFRAME_LABELS:
                    tr = result.timeframes.get(tf)
                    if tr and tr.currency_scores:
                        _tf_scores[tf] = tr.currency_scores
                result.momentum_phases = engine.update_momentum(
                    composite_scores, _tf_scores
                )
                # Check acceleration entry candidates for all display pairs
                accel_cands: dict[str, tuple[str, str]] = {}
                for pair in DISPLAY_PAIRS:
                    base, quote = pair[:3], pair[3:]
                    # Check both directions
                    for direction in ("BUY", "SELL"):
                        ok, reason = engine.check_acceleration_entry(
                            base, quote, direction,
                        )
                        if ok:
                            accel_cands[pair] = (direction, reason)
                            break  # only one direction per pair
                result.accel_candidates = accel_cands
                # Update velocity tracking for filter engine
                htf_regime.update_velocity(composite_scores)
                result.composite_scores = composite_scores
                result.htf_composite_scores = htf_composite_scores

            # ── HTF regime + velocity data for filter engine ──
            result.htf_regimes = htf_regime.get_all_regimes()
            velocity_data: dict[str, tuple[float, bool]] = {}
            for ccy in CURRENCIES:
                velocity_data[ccy] = htf_regime.get_velocity(ccy)
            result.velocity_data = velocity_data

            self.data_ready.emit(result)
            cycle += 1

            # Sleep remainder
            elapsed = time.monotonic() - cycle_start
            sleep_time = max(0.0, self.poll_interval - elapsed)
            slept = 0.0
            while slept < sleep_time and self._running:
                step = min(0.1, sleep_time - slept)
                time.sleep(step)
                slept += step

        # Cleanup
        try:
            mt5.shutdown()
        except Exception:
            pass
