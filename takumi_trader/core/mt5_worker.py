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


def _m5_close_this_cycle(m5_new_candle: bool) -> bool:
    """Single source of truth: did this cycle observe an M5 candle close?

    Two consumers MUST consult this function rather than re-deriving from
    cycle counters or independent timestamp checks:

      1. The strength-engine compute_tf gate that builds
         _stoch_tfs_this_cycle (~line 776). Updates Stoch %K scores only
         on candle close — this is the existing TAKUMI invariant.

      2. The shadow-trade capture for Sv2 (~line 1010, added 2026-05-03).
         Captures every (pair, direction) the strength engine considered
         this M5 close, populating the unfiltered universe Edge Miner
         needs.

    Drift between the two consumers would silently corrupt shadow data
    integrity: a strength refresh without a paired shadow capture means
    the journal misses signals; a shadow capture without a strength
    refresh means duplicate or stale captures. Wrapping the boolean
    in this named function makes the contract searchable and auditable.

    Argument is the cycle-local `m5_new_candle` flag derived inside the
    main loop's per-pair candle-fetch block (~lines 432-461). Don't
    inline-recompute; consult this helper.
    """
    return bool(m5_new_candle)


class MT5Worker(QThread):
    """Background worker that polls MT5 and emits calculation results."""

    data_ready = pyqtSignal(object)  # emits CalculationResult
    connection_status = pyqtSignal(bool, str)  # (connected, message)

    def __init__(
        self,
        poll_interval: float = 1.0,
        shadow_logger_sv2=None,
    ) -> None:
        super().__init__()
        self.poll_interval = poll_interval
        self._running = True
        self._connected = False
        # Shadow capture (Phase B, 2026-05-03). Optional dependency — if
        # None, the worker behaves exactly as before (no capture). When
        # provided, every (pair, direction) the standard stoch engine
        # considers at M5 close is logged. Only the standard engine is
        # instrumented in the Sv2 vertical slice; tuned + live engines
        # come during fan-out after Phase F validation.
        self._shadow_logger_sv2 = shadow_logger_sv2

    def stop(self) -> None:
        self._running = False

    # ── Phase B shadow capture (Sv2 only, vertical slice) ─────────────

    def _capture_sv2_shadow(
        self,
        stoch_engine,
        result,
        mt5,
        sweep_veto_cache: dict[str, str],
    ) -> dict[str, int]:
        """At M5 close, capture every (pair, direction) Sv2 considered.

        Iterates BOTH directions per pair (no short-circuit) so the journal
        has the full unfiltered universe — the entire premise of the build.
        Strength-rejects (~99% of evaluations) get lightweight records via
        log_strength_reject. Strength-passes (~30/day) get full records
        with input_snapshot for lazy feature recompute downstream.

        H1 sweep handling (Site 1): the sweep veto in the existing trade-
        decision loop fires AFTER strength-pass but BEFORE the pair enters
        stoch_entry_candidates. So a sweep-vetoed pair would otherwise
        slip into the journal as a STATUS_PENDING strength-pass with no
        downstream gate ever marking it. To keep the gate distribution
        accurate, we consult the same _sweep_veto_cache the trade-decision
        loop populated this cycle; if a strength-pass is sweep-vetoed, we
        log_signal it (full record so Edge Miner can ask sweep questions
        symmetrically) AND immediately mark_decision(BLOCKED, GATE_H1_SWEEP).
        Cache passed by reference so we observe the same state the trade
        loop saw — never a stale snapshot.

        Mutual exclusion: BUY passes when base≥7.0 AND quote≤3.0; SELL
        when base≤3.0 AND quote≥7.0 — disjoint conditions, so at most one
        direction per pair becomes a strength-pass.

        Returns: {pair: shadow_id} for strength-passes that survived the
        sweep check (rejects + sweep-blocks don't need downstream marks;
        they're already terminal at log time). Main_window uses the
        returned dict to call mark_decision / mark_executed at the 8
        downstream gate sites.
        """
        from takumi_trader.core.pair_algo_settings import get_pair_settings
        from takumi_trader.core.trade_tracker import pip_value
        from takumi_trader.core.strength import DISPLAY_PAIRS as _DP
        from datetime import datetime, timezone, timedelta

        passes: dict[str, int] = {}

        # Per-cycle shared metadata (computed once, used in every record)
        composite = result.composite_scores or {}
        usd_score = float(composite.get("USD", 5.0))
        ccy_vals = list(composite.values())
        if len(ccy_vals) >= 2:
            mean = sum(ccy_vals) / len(ccy_vals)
            ccy_dispersion = (
                sum((v - mean) ** 2 for v in ccy_vals) / len(ccy_vals)
            ) ** 0.5
        else:
            ccy_dispersion = 0.0
        session = result.session_label or ""
        jst = timezone(timedelta(hours=9))
        signal_time = time.time()
        signal_time_str = datetime.fromtimestamp(signal_time, jst).strftime(
            "%m-%d %H:%M:%S JST"
        )

        # Per-pair spread cache (one tick lookup per pair, not per direction)
        _spread_cache: dict[str, float] = {}

        def _spread_pips(pair: str) -> float:
            if pair in _spread_cache:
                return _spread_cache[pair]
            try:
                tick = mt5.symbol_info_tick(pair)
                if tick is None:
                    _spread_cache[pair] = 0.0
                else:
                    pip = pip_value(pair)
                    _spread_cache[pair] = max(
                        0.0, (tick.ask - tick.bid) / pip if pip > 0 else 0.0
                    )
            except Exception:
                _spread_cache[pair] = 0.0
            return _spread_cache[pair]

        def _sc(tf: str, ccy: str) -> float:
            return float(stoch_engine._cached_scores.get(tf, {}).get(ccy, 5.0))

        for pair in _DP:
            base, quote = pair[:3], pair[3:]
            for direction in ("BUY", "SELL"):
                # Re-call check_entry for shadow purposes. Cheap (cached
                # score lookups + comparisons) and only happens at M5
                # close — 27 × 2 × 288 = ~15K extra calls/day, trivial.
                ok, reason = stoch_engine.check_entry(base, quote, direction)

                if not ok:
                    # Strength-reject — lightweight record, terminal.
                    try:
                        self._shadow_logger_sv2.log_strength_reject(
                            pair=pair, direction=direction, reason=reason,
                            m5_base=_sc("M5", base), m5_quote=_sc("M5", quote),
                            m15_base=_sc("M15", base), m15_quote=_sc("M15", quote),
                            h1_base=_sc("H1", base), h1_quote=_sc("H1", quote),
                            h4_base=_sc("H4", base), h4_quote=_sc("H4", quote),
                            d1_base=_sc("D1", base), d1_quote=_sc("D1", quote),
                            spread_points=_spread_pips(pair),
                            # TODO(Phase-F): populate from result.m5_atr once
                            # added to CalculationResult. Currently 0.0 means
                            # UNMEASURED, NOT zero ATR — Edge Miner queries
                            # MUST filter records where m5_atr_pips == 0.0
                            # before doing any ATR-aware analysis. Without
                            # this filter, zero-imputed values pollute
                            # volatility-bucketed expectancy comparisons.
                            m5_atr_pips=0.0,
                            h1_atr_pips=float(result.h1_atr.get(pair, 0.0)),
                            usd_score=usd_score,
                            ccy_dispersion=ccy_dispersion,
                            session=session,
                            signal_time=signal_time,
                            signal_time_str=signal_time_str,
                        )
                    except Exception as exc:
                        # Shadow capture must never break trading. Log once
                        # per cycle on first failure, then move on.
                        logger.warning(
                            "[SHADOW] log_strength_reject failed for %s %s: %s",
                            pair, direction, exc,
                        )
                    continue

                # Strength-pass — full record. Compute proposed entry/SL/TP
                # the same way paper_trader.open_paper_trade would.
                entry = float(result.close_prices.get(pair, 0.0))
                if entry <= 0:
                    continue  # M1 close missing — defensive; M5 close should always have one
                pip = pip_value(pair)
                settings = get_pair_settings(pair)
                h1_atr = float(result.h1_atr.get(pair, 0.0))
                if settings and h1_atr > 0:
                    sl_pips = round(settings.get("sl_atr", 0.3) * h1_atr / pip, 1)
                    tp_pips = round(settings.get("tp_atr", 1.0) * h1_atr / pip, 1)
                elif settings:
                    sl_pips = float(settings.get("sl_pips", 10.0))
                    tp_pips = float(settings.get("tp_pips", 20.0))
                else:
                    sl_pips, tp_pips = 10.0, 20.0
                if direction == "BUY":
                    sl_price = entry - sl_pips * pip
                    tp_price = entry + tp_pips * pip
                else:
                    sl_price = entry + sl_pips * pip
                    tp_price = entry - tp_pips * pip

                # Input snapshot: only the live in-memory state that can't
                # be reproduced from disk-cached MT5 history at sim time.
                # M1/M15/H1 bars deliberately NOT included (sim re-fetches
                # via (pair, signal_time)).
                input_snapshot = {
                    "composite_scores": dict(composite),
                    "usd_score": usd_score,
                    "ccy_dispersion": ccy_dispersion,
                    "session": session,
                    "spread_points": _spread_pips(pair),
                    "h1_atr": h1_atr,
                    "cross_pair_close_prices": dict(result.close_prices),
                    "stoch_scores_snapshot": {
                        tf: dict(stoch_engine._cached_scores.get(tf, {}))
                        for tf in ("M5", "M15", "H1", "H4", "D1")
                    },
                }

                try:
                    sid = self._shadow_logger_sv2.log_signal(
                        pair=pair, direction=direction,
                        proposed_entry=entry,
                        proposed_sl_price=sl_price,
                        proposed_tp_price=tp_price,
                        proposed_sl_pips=sl_pips,
                        proposed_tp_pips=tp_pips,
                        input_snapshot=input_snapshot,
                        signal_time=signal_time,
                        signal_time_str=signal_time_str,
                    )

                    # ── Site 1: H1 sweep veto (worker-side gate) ──
                    # Consult the same cache the trade-decision loop
                    # populated this cycle (passed by reference). If
                    # sweep-vetoed, mark BLOCKED here so the record
                    # doesn't reach main_window as a sweep-blocked
                    # PENDING that nothing ever marks.
                    sweep_type = sweep_veto_cache.get(pair, "none")
                    sweep_blocked = (
                        (direction == "BUY" and sweep_type == "bearish_sweep")
                        or (direction == "SELL" and sweep_type == "bullish_sweep")
                    )
                    if sweep_blocked:
                        sweep_reason = (
                            "H1 bearish sweep — falling knife"
                            if direction == "BUY"
                            else "H1 bullish sweep — short squeeze"
                        )
                        from takumi_trader.core.shadow_logger import (
                            STATUS_BLOCKED, GATE_H1_SWEEP,
                        )
                        self._shadow_logger_sv2.mark_decision(
                            sid, STATUS_BLOCKED, GATE_H1_SWEEP,
                            reason=sweep_reason,
                            metadata={
                                "sweep_type": sweep_type,
                                "lookback_swings": 20,
                                "sweep_threshold_pips": 2.0,
                            },
                        )
                        # Don't return shadow_id — main_window has nothing
                        # left to mark for this pair. Record is terminal.
                    else:
                        passes[pair] = sid
                except Exception as exc:
                    logger.warning(
                        "[SHADOW] log_signal failed for %s %s: %s",
                        pair, direction, exc,
                    )

        return passes

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

        # Stoch engine v2 — QM4-style currency strength for entries/exits
        from takumi_trader.core.stoch_engine import StochStrengthEngine, TF_CONFIG as STOCH_TF_CONFIG
        stoch_engine = StochStrengthEngine()

        # ── Second engine instance for LIVE-candle scoring (2026-04-21) ──
        # Same class, different feeding policy: the worker updates this engine
        # EVERY cycle (~1s) using the latest bar data INCLUDING the currently-
        # forming bar (no rates[:-1] drop). Powers the 5 "-live" paper systems
        # (Sv2-live / Sv2-Tun-live / Sv2+SS-live / Sv2+SS-Tun-live / Sv2+ATR-live)
        # visible under the LiveCan UI button. `stoch_engine` above stays on the
        # candle-close-only feed for systems A/B/C/D/E.
        stoch_engine_live = StochStrengthEngine()

        # ── AU Gold suite data channel (2026-04-24) ──
        # Completely separate from the forex strength engine. Resolve the
        # broker's actual gold symbol ONCE (may be "XAUUSD", "XAUUSDm",
        # "XAUUSD.raw", "XAUUSD.cash" depending on account type). If no
        # variant exists on this broker, leave resolved_gold_symbol=None
        # and all AU strategies silently skip.
        _GOLD_CANDIDATE_SYMBOLS = ("XAUUSD", "XAUUSD.raw", "XAUUSDm",
                                    "XAUUSD.cash", "XAUUSD.s", "XAUUSD.z")
        resolved_gold_symbol: str | None = None
        for _candidate in _GOLD_CANDIDATE_SYMBOLS:
            try:
                _info = mt5.symbol_info(_candidate)
                if _info is not None and _info.visible is False:
                    mt5.symbol_select(_candidate, True)
                    _info = mt5.symbol_info(_candidate)
                if _info is not None:
                    resolved_gold_symbol = _candidate
                    logger.info("[AU GOLD] Resolved broker gold symbol: %s", _candidate)
                    break
            except Exception as _gex:
                logger.debug("[AU GOLD] %s not available: %s", _candidate, _gex)
        if resolved_gold_symbol is None:
            logger.warning(
                "[AU GOLD] No XAUUSD symbol variant found on this broker "
                "(tried %s). AU1-5 strategies will skip — forex systems "
                "continue normally.", list(_GOLD_CANDIDATE_SYMBOLS),
            )

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
        cached_results: dict[str, TimeframeResult] = {}      # CLOSED candle (exits)
        cached_results_live: dict[str, TimeframeResult] = {}  # FORMING candle (entries)
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
        # Seed live cache with same warmup data
        for tf_label in TIMEFRAME_LABELS:
            if tf_label in cached_results:
                cached_results_live[tf_label] = cached_results[tf_label]
        logger.info("HTF cache seeded: %s", list(cached_results.keys()))

        # ── Stoch v2 warmup: pre-compute scores + velocity history ──
        # Replay last 10 bars of M5 data to build velocity history
        _stoch_tfs = [
            ("M5", mt5.TIMEFRAME_M5),
            ("M15", mt5.TIMEFRAME_M15),
            ("H1", mt5.TIMEFRAME_H1),
            ("H4", mt5.TIMEFRAME_H4),
            ("D1", mt5.TIMEFRAME_D1),
            ("W1", mt5.TIMEFRAME_W1),
        ]
        # First compute current scores for all TFs.
        # BUG #3 FIX (2026-04-21): for higher TFs (H4/D1/W1) drop the forming
        # bar so warmup Stoch uses only closed bars — same rule the main
        # loop now enforces. M5/M15/H1 also benefit from this for warmup,
        # since the current bar hasn't closed yet at startup.
        _drop_forming_tfs = {"H4", "D1", "W1", "H1", "M15", "M5"}
        for stf_label, stf_const in _stoch_tfs:
            _stoch_warmup: dict[str, Any] = {}
            for pair in ALL_28_PAIRS:
                try:
                    r = mt5.copy_rates_from_pos(pair, stf_const, 0, 30)
                    if r is not None and len(r) > 4:  # need period+buffer after dropping forming
                        if stf_label in _drop_forming_tfs:
                            _stoch_warmup[pair] = r[:-1]  # drop forming bar
                        else:
                            _stoch_warmup[pair] = r
                except Exception:
                    pass
            if _stoch_warmup:
                stoch_engine.compute_tf(_stoch_warmup, stf_label)
                # Also seed the live engine from the same warmup bars so its
                # first few cycles aren't empty (it will start recomputing
                # with live data on the first main-loop iteration).
                stoch_engine_live.compute_tf(_stoch_warmup, stf_label)

        # Replay M5 + M15 history to build velocity (simulate last 10 M5 closes)
        #
        # BUG #2 FIX (2026-04-21): previously this loop called
        #   stoch_engine.compute_tf(_m5_data, "M5")
        #   stoch_engine.compute_tf(_m5_data, "M15")   ← SAME M5 data as M15
        # which mislabeled M5 bars as M15, producing identical scores and
        # corrupting the composite that feeds velocity history. Now fetches
        # M15 bars separately and slices them proportionally (1 M15 bar per
        # 3 M5 bars) so the composite at each replay step reflects actual
        # M5 AND M15 historical context.
        _m5_warmup: dict[str, Any] = {}
        _m15_warmup: dict[str, Any] = {}
        for pair in ALL_28_PAIRS:
            try:
                r5 = mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_M5, 0, 30)
                if r5 is not None and len(r5) > 12:
                    _m5_warmup[pair] = r5
            except Exception:
                pass
            try:
                # M15 bars: 10 M5 closes = ~50 min = ~3.5 M15 bars, so 20 is
                # plenty of history. Fetch 30 to be safe.
                r15 = mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_M15, 0, 30)
                if r15 is not None and len(r15) > 5:
                    _m15_warmup[pair] = r15
            except Exception:
                pass

        if _m5_warmup:
            for replay_offset in range(10, 0, -1):
                # Slice M5 data to simulate each historical M5 close
                _m5_replay: dict[str, Any] = {}
                for pair, rates in _m5_warmup.items():
                    if len(rates) > replay_offset + 3:
                        _m5_replay[pair] = rates[:len(rates) - replay_offset]

                # M15 is slower — 3 M5 bars per 1 M15 bar. So for a given
                # M5 replay_offset, the equivalent M15 offset is ~offset/3.
                # Using integer division to stay conservative.
                _m15_replay_offset = max(0, replay_offset // 3)
                _m15_replay: dict[str, Any] = {}
                for pair, rates in _m15_warmup.items():
                    if _m15_replay_offset == 0:
                        _m15_replay[pair] = rates
                    elif len(rates) > _m15_replay_offset + 3:
                        _m15_replay[pair] = rates[:len(rates) - _m15_replay_offset]

                if _m5_replay:
                    stoch_engine.compute_tf(_m5_replay, "M5")
                    stoch_engine_live.compute_tf(_m5_replay, "M5")
                if _m15_replay:
                    stoch_engine.compute_tf(_m15_replay, "M15")
                    stoch_engine_live.compute_tf(_m15_replay, "M15")
                if _m5_replay or _m15_replay:
                    composite = stoch_engine.get_composite(["M5", "M15"])
                    stoch_engine.update_velocity(composite)
                    # Live engine gets its own velocity history seeded too
                    live_composite = stoch_engine_live.get_composite(["M5", "M15"])
                    stoch_engine_live.update_velocity(live_composite)

        _vel_sample = {c: f"{stoch_engine.get_velocity(c):+.2f}" for c in CURRENCIES}
        logger.info("Stoch v2 warmup done — velocities: %s", _vel_sample)

        # ── Warmup diagnostic: which TFs actually got scores? ──
        # (added 2026-04-21 to debug the "grid frozen after MT5 restart" issue.)
        # If this log line shows missing TFs, the self-healing retry in the
        # main loop will populate them on subsequent cycles once MT5's bar
        # history becomes available.
        _warmup_coverage = {
            tf: len(stoch_engine._cached_scores.get(tf, {}))
            for tf in ("M5", "M15", "H1", "H4", "D1", "W1")
        }
        _missing = [tf for tf, n in _warmup_coverage.items() if n == 0]
        if _missing:
            logger.warning(
                "Stoch v2 warmup INCOMPLETE — missing scores for %s "
                "(coverage: %s). Self-healing retry will populate on next "
                "cycles once MT5 returns bar history for these TFs.",
                _missing, _warmup_coverage,
            )
        else:
            logger.info("Stoch v2 warmup coverage: %s — all TFs populated",
                        _warmup_coverage)

        # Cache M1 candle data for range detection
        cached_m1_data: dict[str, Any] = {}
        # Cache H1 candle data for ATR + structural levels
        # Seed from warmup so H1 ATR is available immediately (no 60min wait)
        cached_h1_data: dict[str, Any] = {}
        for pair in ALL_28_PAIRS:
            h1w = warmup_data.get(pair, {}).get("H1")
            if h1w is not None and len(h1w) >= 15:
                cached_h1_data[pair] = h1w
        logger.info("H1 ATR cache seeded: %d pairs", len(cached_h1_data))

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
            m5_new_candle = False
            m15_new_candle = False
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

                    # LIVE results (forming candle) — used for ENTRY detection
                    # Always update so entries react to real-time momentum
                    cached_results_live[tf_label] = tf_result

                    # CLOSED results — used for spread-collapse EXIT only
                    # M1: always update (real-time SL/TP + entry)
                    # M5/M15/H1: ONLY update on candle close (stable exits)
                    if tf_label == "M1":
                        cached_results[tf_label] = tf_result
                        cached_m1_data = candle_data
                        m1_new_candle = new_candle
                    elif new_candle:
                        cached_results[tf_label] = tf_result
                        if tf_label == "M5":
                            m5_new_candle = True
                        elif tf_label == "M15":
                            m15_new_candle = True
                        elif tf_label == "H1":
                            cached_h1_data = candle_data
                            h1_new_candle = True
                    else:
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

            # Build full result — use LIVE (forming candle) for UI + entries
            for tf in TIMEFRAME_LABELS:
                if tf in cached_results_live:
                    result.timeframes[tf] = cached_results_live[tf]

            # ── Close prices for trade tracking ──
            if cached_m1_data:
                for pair, candles in cached_m1_data.items():
                    if len(candles) > 0:
                        result.close_prices[pair] = float(candles[-1]["close"])
                        result.high_prices[pair] = float(candles[-1]["high"])
                        result.low_prices[pair] = float(candles[-1]["low"])
                        try:
                            result.tick_volumes[pair] = int(candles[-1]["tick_volume"])
                        except (KeyError, ValueError):
                            pass
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

                            # Filter out H1 bars from 6:00-6:59 JST (UTC hour 21)
                            # IC Markets has junk wicks at session open that
                            # don't represent real support/resistance.
                            _times = h1c["time"]
                            _utc_hours = (_times % 86400) // 3600
                            _clean = _utc_hours != 21  # 21:00 UTC = 6:00 JST
                            _h1_clean = h1c[_clean]

                            if len(_h1_clean) < 12:
                                _h1_clean = h1c  # fallback if too few bars left

                            # Separate PREVIOUS DAY from today.
                            # IC Markets server day resets at 21:00 UTC (00:00 UTC+3).
                            # "Today" = bars from last 21:00 UTC to now.
                            # "Yesterday" = bars from 21:00 UTC (2 days ago) to 21:00 UTC (yesterday).
                            _now_utc = int(time.time())
                            _hour_utc = (_now_utc % 86400) // 3600
                            # Find the most recent 21:00 UTC boundary
                            _today_21 = (_now_utc // 86400) * 86400 + 21 * 3600
                            if _today_21 > _now_utc:
                                _today_21 -= 86400  # 21:00 UTC yesterday
                            _yesterday_21 = _today_21 - 86400

                            # Previous day = bars from yesterday's 21:00 to today's 21:00
                            _prev_day_mask = (_h1_clean["time"] >= _yesterday_21) & (_h1_clean["time"] < _today_21)
                            _prev_day = _h1_clean[_prev_day_mask]

                            if len(_prev_day) < 6:
                                # Fallback: use bars 24-48 hours ago (strictly previous day)
                                # NEVER use _h1_clean[-24:] as it includes today's data
                                if len(_h1_clean) >= 48:
                                    _prev_day = _h1_clean[-48:-24]
                                else:
                                    # Not enough history — skip this pair to avoid
                                    # corrupting the structural filter with today's data
                                    logger.warning(
                                        "Insufficient H1 history for %s prev_day levels "
                                        "(only %d bars) — skipping",
                                        pair, len(_h1_clean),
                                    )
                                    continue

                            # Previous week: use bars from before today's 21:00 boundary
                            _prev_week_mask = _h1_clean["time"] < _today_21
                            _prev_week = _h1_clean[_prev_week_mask][-120:]
                            if len(_prev_week) < 24:
                                _prev_week = _prev_day  # fallback to prev day

                            # Previous month: same treatment
                            _prev_month_mask = _h1_clean["time"] < _today_21
                            _prev_month = _h1_clean[_prev_month_mask][-720:]
                            if len(_prev_month) < 24:
                                _prev_month = _prev_week

                            result.structural_levels[pair] = {
                                "prev_day_high": float(np.max(_prev_day["high"])),
                                "prev_day_low": float(np.min(_prev_day["low"])),
                                "prev_week_high": float(np.max(_prev_week["high"])),
                                "prev_week_low": float(np.min(_prev_week["low"])),
                                "prev_month_high": float(np.max(_prev_month["high"])),
                                "prev_month_low": float(np.min(_prev_month["low"])),
                                "pip": _pip,
                            }
                        except Exception as _sl_exc:
                            logger.warning("Structural level calc failed for %s: %s", pair, _sl_exc)

            # ── Session range consumed percentages ──
            result.session_range_pct = session_range_tracker.get_all_consumed_pct()

            # ── Session info ──
            result.session_label = get_session_label()

            # ── Flow states ──
            result.flow_states = flow_tracker.get_all_states()

            # ── Composite scores ──
            # ENTRIES use LIVE (forming candle) — fast, reactive
            # EXITS use CLOSED candle + confirmation — stable, gives room
            composite_scores: dict[str, float] = {}
            htf_composite_scores: dict[str, float] = {}
            _HTF_ONLY = {"M5", "M15", "H1"}

            # LIVE composite for entries + momentum (forming candle)
            for ccy in CURRENCIES:
                total = 0.0
                count = 0
                for tf in TIMEFRAME_LABELS:
                    tr = cached_results_live.get(tf)
                    if tr and ccy in tr.currency_scores:
                        total += tr.currency_scores[ccy]
                        count += 1
                if count > 0:
                    composite_scores[ccy] = total / count

            # CLOSED HTF composite for spread-collapse exits
            for ccy in CURRENCIES:
                htf_total = 0.0
                htf_count = 0
                for tf in _HTF_ONLY:
                    tr = cached_results.get(tf)  # CLOSED candle data
                    if tr and ccy in tr.currency_scores:
                        htf_total += tr.currency_scores[ccy]
                        htf_count += 1
                if htf_count > 0:
                    htf_composite_scores[ccy] = htf_total / htf_count

            if composite_scores:
                # Build per-TF scores dict for acceleration tracking
                # Use HTF only (M5+M15+H1, no M1) — M1 is too noisy
                _HTF_TFS = ("M5", "M15", "H1")
                _tf_scores: dict[str, dict[str, float]] = {}
                for tf in _HTF_TFS:
                    tr = result.timeframes.get(tf)
                    if tr and tr.currency_scores:
                        _tf_scores[tf] = tr.currency_scores
                result.momentum_phases = engine.update_momentum(
                    htf_composite_scores, _tf_scores
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

            # ── SELF-HEALING retry for primary-TF scores (2026-04-21 fix) ──
            # If warmup ran before MT5 had bar history ready (common after an
            # MT5 restart), `_cached_scores` can stay empty for hours because
            # the normal "compute only on candle close" logic below won't
            # trigger H4/D1/W1 until those bars close (up to 4+ hours).
            # This retry block re-fetches bars for any missing primary TF
            # and populates scores — so the grid auto-heals within seconds
            # of MT5's bar history becoming available.
            _primary_tfs = [
                ("M5", mt5.TIMEFRAME_M5),
                ("M15", mt5.TIMEFRAME_M15),
                ("H1", mt5.TIMEFRAME_H1),
                ("H4", mt5.TIMEFRAME_H4),
            ]
            for _rtf_label, _rtf_const in _primary_tfs:
                _existing = stoch_engine._cached_scores.get(_rtf_label)
                if _existing:
                    continue  # already populated — skip
                # Try to fetch bars and recompute this TF
                # BUG #3 FIX: drop forming bar so retry matches warmup + main-loop semantics.
                _retry_data: dict[str, Any] = {}
                for _rp in ALL_28_PAIRS:
                    try:
                        _rrates = mt5.copy_rates_from_pos(_rp, _rtf_const, 0, 30)
                        if _rrates is not None and len(_rrates) > 4:
                            _retry_data[_rp] = _rrates[:-1]  # drop forming bar
                    except Exception:
                        pass
                if _retry_data:
                    stoch_engine.compute_tf(_retry_data, _rtf_label)
                    logger.info(
                        "[STOCH RETRY] Populated missing %s scores for %d pairs "
                        "(warmup had missed this TF — auto-healed)",
                        _rtf_label, len(_retry_data),
                    )
                # If _retry_data is still empty (MT5 not ready), next cycle retries.

            # ── Stoch Engine v2: QM4-style currency strength ──
            # CRITICAL: Only update Stoch scores on CANDLE CLOSE — matches
            # backtest exactly. Forming candle data causes false entries
            # that the backtest never sees (the desync problem).
            _stoch_tfs_this_cycle = []
            if m5_new_candle:
                _stoch_tfs_this_cycle.append("M5")
            if m15_new_candle:
                _stoch_tfs_this_cycle.append("M15")
            if h1_new_candle:
                _stoch_tfs_this_cycle.append("H1")

            # Also fetch H4/D1/W1 periodically for Stoch (every 120/300/600 cycles)
            #
            # BUG #3 FIX (2026-04-21): previously used the full 30-bar result which
            # INCLUDES the currently-forming H4/D1/W1 bar. compute_tf then did
            # highs[-period:] using that partial bar, causing H4 Stoch to flip
            # back and forth as the forming bar's high/low evolved mid-bar —
            # contradicting the engine's "only on candle close" design and
            # diverging live behaviour from backtest. Fix: drop the last bar
            # (`rates[:-1]`) so Stoch uses only closed bars, matching the
            # treatment of M5/M15/H1.
            _stoch_htf_data: dict[str, dict[str, Any]] = {}
            if cycle % 120 == 0:
                for pair in ALL_28_PAIRS:
                    try:
                        h4c = mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_H4, 0, 30)
                        if h4c is not None and len(h4c) > 6:  # need period+buffer after dropping forming
                            if "H4" not in _stoch_htf_data:
                                _stoch_htf_data["H4"] = {}
                            _stoch_htf_data["H4"][pair] = h4c[:-1]  # drop forming bar
                    except Exception:
                        pass
            if cycle % 300 == 0:
                for pair in ALL_28_PAIRS:
                    try:
                        d1c = mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_D1, 0, 30)
                        if d1c is not None and len(d1c) > 6:
                            if "D1" not in _stoch_htf_data:
                                _stoch_htf_data["D1"] = {}
                            _stoch_htf_data["D1"][pair] = d1c[:-1]  # drop forming bar
                    except Exception:
                        pass
            if cycle % 600 == 0:
                for pair in ALL_28_PAIRS:
                    try:
                        w1c = mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_W1, 0, 30)
                        if w1c is not None and len(w1c) > 6:
                            if "W1" not in _stoch_htf_data:
                                _stoch_htf_data["W1"] = {}
                            _stoch_htf_data["W1"][pair] = w1c[:-1]  # drop forming bar
                    except Exception:
                        pass

            # Compute Stoch scores for fast TFs using existing candle_data
            for stf in _stoch_tfs_this_cycle:
                # Re-use pair data from the main loop (already fetched)
                _stoch_pair_data: dict[str, Any] = {}
                tf_const_stoch = tf_map.get(stf)
                if tf_const_stoch is None:
                    continue
                for pair in ALL_28_PAIRS:
                    try:
                        _rates = mt5.copy_rates_from_pos(pair, tf_const_stoch, 0, 30)
                        if _rates is not None and len(_rates) > 3:
                            _stoch_pair_data[pair] = _rates
                    except Exception:
                        continue
                if _stoch_pair_data:
                    stoch_engine.compute_tf(_stoch_pair_data, stf)

            # Compute Stoch scores for HTF (H4/D1/W1)
            for htf_label, htf_pair_data in _stoch_htf_data.items():
                if htf_pair_data:
                    stoch_engine.compute_tf(htf_pair_data, htf_label)

            # Update velocity on M5 close (most reactive fast TF)
            if "M5" in _stoch_tfs_this_cycle:
                stoch_composite = stoch_engine.get_composite(["M5", "M15"])
                stoch_engine.update_velocity(stoch_composite)

            # ── H1 sweep veto (added 2026-04-30) ─────────────────────
            # Reject entries that fire while the H1 candle just put in a
            # liquidity sweep against the trade direction. A "bearish_sweep"
            # (high broke above swing high then closed below) on a BUY = we'd
            # be entering as bears just took out longs at the high. Classic
            # falling-knife / pump-before-dump trap. Likewise bullish_sweep
            # vetos any SELL.
            #
            # Lazy on-demand H1 fetch — only runs for the 0–2 pairs that
            # pass `check_entry` in this cycle, not all 28.
            from takumi_trader.features import adversarial as _adv
            from takumi_trader.core.trade_tracker import pip_value as _pip_value
            _sweep_veto_cache: dict[str, str] = {}  # pair -> sweep_type

            def _h1_sweep_blocks(pair: str, direction: str) -> tuple[bool, str]:
                """Returns (blocked, reason). Cached per cycle per pair."""
                if pair in _sweep_veto_cache:
                    sweep_type = _sweep_veto_cache[pair]
                else:
                    try:
                        h1_rates = mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_H1, 0, 30)
                        if h1_rates is None or len(h1_rates) < 26:
                            _sweep_veto_cache[pair] = "none"
                            return False, ""
                        import numpy as _np
                        highs = _np.asarray(h1_rates["high"], dtype=_np.float64)
                        lows = _np.asarray(h1_rates["low"], dtype=_np.float64)
                        closes = _np.asarray(h1_rates["close"], dtype=_np.float64)
                        sweep = _adv.liquidity_sweep_pattern(
                            highs, lows, closes,
                            lookback_swings=20, sweep_threshold_pips=2.0,
                            pip_size=_pip_value(pair),
                        )
                        sweep_type = sweep.get("sweep_type", "none")
                        _sweep_veto_cache[pair] = sweep_type
                    except Exception:
                        _sweep_veto_cache[pair] = "none"
                        return False, ""
                if direction == "BUY" and sweep_type == "bearish_sweep":
                    return True, "H1 bearish sweep — falling knife"
                if direction == "SELL" and sweep_type == "bullish_sweep":
                    return True, "H1 bullish sweep — short squeeze"
                return False, ""

            # Check Stoch entry candidates (standard thresholds)
            stoch_entries: dict[str, tuple[str, str]] = {}
            for pair in DISPLAY_PAIRS:
                base, quote = pair[:3], pair[3:]
                for direction in ("BUY", "SELL"):
                    ok, reason = stoch_engine.check_entry(base, quote, direction)
                    if ok:
                        blocked, sweep_reason = _h1_sweep_blocks(pair, direction)
                        if blocked:
                            logger.info("[STOCH] %s %s blocked: %s", direction, pair, sweep_reason)
                            break
                        stoch_entries[pair] = (direction, reason)
                        break

            # ── Shadow capture (Sv2 only, M5-close trigger) ──────────
            # Captures the unfiltered signal universe for Edge Miner.
            # Single source of truth for "is this an M5-close cycle?":
            # _m5_close_this_cycle(m5_new_candle). Any future code that
            # also gates on M5 close MUST consult that helper or risk
            # silently drifting from the strength-engine's score-refresh
            # cadence.
            if (self._shadow_logger_sv2 is not None
                    and _m5_close_this_cycle(m5_new_candle)):
                try:
                    # Pass _sweep_veto_cache by reference so shadow sees
                    # the same cache state the trade-decision loop saw.
                    # Cache is populated above (line ~1127) for pairs
                    # whose strength gate passed; shadow capture
                    # consults it inside the strength-pass branch.
                    sv2_shadow_ids = self._capture_sv2_shadow(
                        stoch_engine, result, mt5, _sweep_veto_cache,
                    )
                    result.sv2_shadow_ids = sv2_shadow_ids
                except Exception as _shadow_exc:
                    # Shadow path must never abort trading.
                    logger.warning(
                        "[SHADOW] _capture_sv2_shadow raised: %s",
                        _shadow_exc, exc_info=True,
                    )
                    result.sv2_shadow_ids = {}

            # Check Stoch entry candidates (TUNED: looser thresholds for earlier entry)
            # BUG #1 FIX (2026-04-21): now also passes d1_block_strong/weak so D1
            # matches the rest of the loosened chain instead of silently using
            # the strict 3.0/7.0 hard-coded defaults.
            stoch_entries_tuned: dict[str, tuple[str, str]] = {}
            for pair in DISPLAY_PAIRS:
                base, quote = pair[:3], pair[3:]
                for direction in ("BUY", "SELL"):
                    ok, reason = stoch_engine.check_entry(
                        base, quote, direction,
                        min_strong=6.5, min_weak=3.5,
                        h1_block_strong=6.5, h1_block_weak=3.5,
                        h4_block_strong=6.5, h4_block_weak=3.5,
                        d1_block_strong=6.5, d1_block_weak=3.5,
                    )
                    if ok:
                        blocked, sweep_reason = _h1_sweep_blocks(pair, direction)
                        if blocked:
                            logger.info("[STOCH-TUNED] %s %s blocked: %s", direction, pair, sweep_reason)
                            break
                        stoch_entries_tuned[pair] = (direction, reason)
                        break

            # ═══════════════════════════════════════════════════════════
            # LIVE-CANDLE ENGINE (2026-04-21): fresh fetch every cycle
            # ═══════════════════════════════════════════════════════════
            # Unlike the main `stoch_engine` (candle-close only, matches
            # backtest), this engine computes on EVERY cycle using bars
            # that INCLUDE the currently-forming bar. Reacts to price
            # movement in real time. Powers the 5 "-live" paper systems.
            #
            # Cadence:
            #   M5, M15, H1:  refetch every cycle (live reactivity)
            #   H4:           refetch every 30 cycles (~30s — H4 only
            #                 meaningfully changes every 4h anyway)
            #   D1:           refetch every 300 cycles (~5 min)
            _live_fetch_schedule = [
                ("M5",  mt5.TIMEFRAME_M5,  1),     # every cycle
                ("M15", mt5.TIMEFRAME_M15, 1),     # every cycle
                ("H1",  mt5.TIMEFRAME_H1,  1),     # every cycle
                ("H4",  mt5.TIMEFRAME_H4,  30),    # every 30 cycles
                ("D1",  mt5.TIMEFRAME_D1,  300),   # every 300 cycles
            ]
            for _live_tf, _live_const, _refresh_cycles in _live_fetch_schedule:
                if cycle % _refresh_cycles != 0:
                    continue  # not time to refresh this TF yet
                _live_data: dict[str, Any] = {}
                for pair in ALL_28_PAIRS:
                    try:
                        _rates = mt5.copy_rates_from_pos(pair, _live_const, 0, 30)
                        if _rates is not None and len(_rates) > 3:
                            # LIVE engine includes the forming bar — no slicing
                            _live_data[pair] = _rates
                    except Exception:
                        continue
                if _live_data:
                    stoch_engine_live.compute_tf(_live_data, _live_tf)

            # Update live velocity every cycle (vs. main engine which only
            # updates on M5 close)
            if stoch_engine_live._cached_scores.get("M5") and stoch_engine_live._cached_scores.get("M15"):
                _live_composite = stoch_engine_live.get_composite(["M5", "M15"])
                stoch_engine_live.update_velocity(_live_composite)

            # Live engine entry candidates — standard + tuned
            # H1 sweep veto applies here too (uses the same _sweep_veto_cache
            # populated above so we don't refetch H1 bars).
            stoch_entries_live: dict[str, tuple[str, str]] = {}
            stoch_entries_tuned_live: dict[str, tuple[str, str]] = {}
            for pair in DISPLAY_PAIRS:
                base, quote = pair[:3], pair[3:]
                for direction in ("BUY", "SELL"):
                    ok, reason = stoch_engine_live.check_entry(base, quote, direction)
                    if ok:
                        blocked, sweep_reason = _h1_sweep_blocks(pair, direction)
                        if blocked:
                            logger.info("[STOCH-LIVE] %s %s blocked: %s", direction, pair, sweep_reason)
                            break
                        stoch_entries_live[pair] = (direction, reason)
                        break
                for direction in ("BUY", "SELL"):
                    ok, reason = stoch_engine_live.check_entry(
                        base, quote, direction,
                        min_strong=6.5, min_weak=3.5,
                        h1_block_strong=6.5, h1_block_weak=3.5,
                        h4_block_strong=6.5, h4_block_weak=3.5,
                        d1_block_strong=6.5, d1_block_weak=3.5,
                    )
                    if ok:
                        blocked, sweep_reason = _h1_sweep_blocks(pair, direction)
                        if blocked:
                            logger.info("[STOCH-TUNED-LIVE] %s %s blocked: %s", direction, pair, sweep_reason)
                            break
                        stoch_entries_tuned_live[pair] = (direction, reason)
                        break

            # Populate result with Stoch data (both engines)
            result.stoch_scores = dict(stoch_engine._cached_scores)
            result.stoch_entry_candidates = stoch_entries
            result.stoch_entry_candidates_tuned = stoch_entries_tuned
            # Live-engine outputs
            result.stoch_scores_live = dict(stoch_engine_live._cached_scores)
            result.stoch_entry_candidates_live = stoch_entries_live
            result.stoch_entry_candidates_tuned_live = stoch_entries_tuned_live
            result.stoch_velocities = {
                ccy: stoch_engine.get_velocity(ccy) for ccy in CURRENCIES
            }

            # ── AU Gold suite: fetch XAUUSD on separate channel (2026-04-24) ──
            # Isolated from forex pipeline — reads/writes ONLY xau_* fields on
            # result. Fetches M1/M5 every cycle, M15/H1 every 5 cycles, H4/D1
            # every 30 cycles. Matches cadence of the forex live-engine so we
            # pay roughly the same MT5 overhead. Silent no-op if broker has
            # no gold symbol (resolved_gold_symbol is None).
            if resolved_gold_symbol is not None:
                try:
                    result.xau_symbol = resolved_gold_symbol
                    _xau_fetch_schedule = [
                        ("M1",  mt5.TIMEFRAME_M1,  1,   200),
                        ("M5",  mt5.TIMEFRAME_M5,  1,   120),
                        ("M15", mt5.TIMEFRAME_M15, 5,   100),
                        ("H1",  mt5.TIMEFRAME_H1,  5,   100),
                        ("H4",  mt5.TIMEFRAME_H4,  30,  80),
                        ("D1",  mt5.TIMEFRAME_D1,  300, 60),
                    ]
                    for _xtf, _xconst, _xcycles, _xcount in _xau_fetch_schedule:
                        if cycle % _xcycles != 0 and _xtf in result.xau_candles:
                            continue  # keep last fetched bars until refresh
                        try:
                            _xbars = mt5.copy_rates_from_pos(
                                resolved_gold_symbol, _xconst, 0, _xcount,
                            )
                            if _xbars is not None and len(_xbars) > 0:
                                result.xau_candles[_xtf] = _xbars
                        except Exception as _xe:
                            logger.debug("[AU GOLD] %s %s fetch failed: %s",
                                         resolved_gold_symbol, _xtf, _xe)
                    # Latest M1 tick info
                    if "M1" in result.xau_candles and len(result.xau_candles["M1"]) > 0:
                        _last_m1 = result.xau_candles["M1"][-1]
                        result.xau_price = float(_last_m1["close"])
                        result.xau_high = float(_last_m1["high"])
                        result.xau_low = float(_last_m1["low"])
                    # Current bid/ask spread (for spread filter)
                    try:
                        _tick = mt5.symbol_info_tick(resolved_gold_symbol)
                        if _tick is not None:
                            _sp_price = float(_tick.ask) - float(_tick.bid)
                            result.xau_spread_points = _sp_price / 0.01  # pips = price/0.01
                    except Exception:
                        pass
                except Exception as _gold_err:
                    logger.debug("[AU GOLD] Fetch loop error: %s", _gold_err)

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
