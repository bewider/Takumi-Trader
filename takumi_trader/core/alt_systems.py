"""Alternative trading systems — BREAKOUT, SQUEEZE, DIVERGENCE.

Three independent signal engines that use price action / volatility /
correlation — fundamentally different from the currency-strength approach.

All 3 are paper-only, NO cTrader execution.
NO_TRADE window: 05:00–07:57 JST (enforced by the caller in main_window).
"""
from __future__ import annotations

import logging
import math
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import numpy as np

logger = logging.getLogger(__name__)

_JST = timezone(timedelta(hours=9))

# ── All 28 FX pairs ──────────────────────────────────────────────
ALL_28 = [
    "EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
    "EURGBP", "EURAUD", "EURNZD", "EURCAD", "EURCHF", "EURJPY",
    "GBPAUD", "GBPNZD", "GBPCAD", "GBPCHF", "GBPJPY",
    "AUDNZD", "AUDCAD", "AUDCHF", "AUDJPY",
    "NZDCAD", "NZDCHF", "NZDJPY",
    "CADCHF", "CADJPY",
    "CHFJPY",
]

# Correlated pair groups for DIVERGENCE system
CORR_GROUPS: list[tuple[str, str]] = [
    ("AUDUSD", "NZDUSD"),
    ("AUDJPY", "NZDJPY"),
    ("EURUSD", "GBPUSD"),
    ("EURJPY", "GBPJPY"),
    ("EURCHF", "GBPCHF"),
    ("AUDCAD", "NZDCAD"),
]


def _pip(pair: str) -> float:
    return 0.01 if "JPY" in pair else 0.0001


@dataclass
class Signal:
    """A trade signal emitted by one of the alt systems."""
    pair: str
    direction: str        # "BUY" or "SELL"
    entry_price: float
    sl_pips: float
    tp_pips: float
    system_type: str      # "breakout", "squeeze", "divergence"
    # System-specific signal data for analysis:
    #   BRK: sig1=range_pips, sig2=breakout_dist_pips, sig3="", sig4=asian_high
    #   SQZ: sig1=squeeze_bars, sig2=bb/kc_ratio, sig3="", sig4=sma_dist_pips
    #   DIV: sig1=z_score, sig2=ratio_vs_mean, sig3=pair_group, sig4=std
    alt_signal_1: float = 0.0
    alt_signal_2: float = 0.0
    alt_signal_3: str = ""
    alt_signal_4: float = 0.0

    # ── Squeeze-specific extended context (added 2026-04-20) ──
    # Populated ONLY for system_type=="squeeze"; other systems leave at defaults.
    # Purpose: accumulate richer per-trade context for future what-if analysis.
    sqz_bb_kc_ratio_min: float = 0.0        # tightest BB/KC ratio seen during squeeze
    sqz_bb_width_pips_release: float = 0.0  # BB width in pips at the release moment
    sqz_bb_width_min_pips: float = 0.0      # tightest BB width (pips) seen during squeeze
    sqz_real_age_bars: int = 0              # M15 bars from squeeze start to release (clean count)
    sqz_dist_to_upper_bb_pips: float = 0.0  # signed: positive = upper BB is above close
    sqz_dist_to_lower_bb_pips: float = 0.0  # signed: positive = close is above lower BB
    sqz_close_pos_in_kc: float = -1.0       # 0..1 when measured; -1 = not measured (non-squeeze trade or old record). (Fix BUG #9: 0.5 was ambiguous with "mid-channel" real value.)
    sqz_atr_ratio_during: float = 0.0       # ATR at release / ATR at squeeze start
    sqz_touches_count: int = 0              # # of M15 bars during squeeze where ratio >= 0.9 (BB near KC edge = shallow); low = deep squeeze, high = oscillating near release threshold
    sqz_concurrent_count: int = 0           # # of OTHER pairs in squeeze at time of release


class AltSystemEngine:
    """Manages state and signal detection for 3 alternative systems.

    Called every M1 cycle from main_window._on_data(). Accumulates
    internal state (ranges, histories, squeeze flags) and emits
    Signal objects when entry conditions are met.
    """

    def __init__(self) -> None:
        # ── BREAKOUT state ──
        self._asian_high: dict[str, float] = {}  # pair -> running high accumulated TODAY
        self._asian_low: dict[str, float] = {}
        # 2026-04-23: yesterday's snapshot. At the 00:00 JST daily reset we copy
        # today's accumulated range here, then breakouts ALL DAY trade against
        # this stable snapshot — letting the strategy fire any time except the
        # explicit no-trade window (05:00-07:57 JST). Pre-warmed at startup
        # from MT5 historical M1 bars so day 1 after deploy still works.
        self._yesterday_high: dict[str, float] = {}
        self._yesterday_low: dict[str, float] = {}
        self._asian_day: int = -1                 # JST day-of-year for daily reset
        self._breakout_fired: set[str] = set()    # pairs that already fired today

        # ── SQUEEZE state (M15 timeframe) ──
        self._sqz_closes: dict[str, deque] = {}    # pair -> deque(maxlen=25)
        self._sqz_highs: dict[str, deque] = {}
        self._sqz_lows: dict[str, deque] = {}
        self._last_sqz_key: int = 0
        self._squeeze_active: dict[str, bool] = {}  # pair -> currently in squeeze?
        self._squeeze_fired: dict[str, bool] = {}   # pair -> already fired this squeeze?
        self._squeeze_bars: dict[str, int] = {}      # pair -> bars in current squeeze
        self._squeeze_last_fire_bar: dict[str, int] = {}  # pair -> M15 bar key of last fire (cooldown)
        # ── Extended per-squeeze tracking (added 2026-04-20) ──
        # All reset when a new squeeze BEGINS (not when it ends or fires).
        # Used to populate the sqz_* context fields on the Signal at release.
        self._squeeze_start_bar: dict[str, int] = {}       # M15 bar key when squeeze began
        self._squeeze_atr_at_start: dict[str, float] = {}  # M15 ATR when squeeze began
        self._squeeze_ratio_min: dict[str, float] = {}     # min BB/KC ratio during squeeze
        self._squeeze_bb_width_min: dict[str, float] = {}  # min BB width (price units) during squeeze
        self._squeeze_touches: dict[str, int] = {}          # count of M15 bars squeezed (= touches)

        # ── DIVERGENCE state ──
        self._m15_closes: dict[str, deque] = {}  # pair -> deque(maxlen=60)
        self._last_m15_key: int = 0
        self._last_m5_key: int = 0   # z-score checked every M5 for fast detection
        self._div_fired: dict[str, bool] = {}   # group_key -> already in trade?

        # Warm up from MT5 history so systems are ready immediately
        self._warmup_done = False

    # ══════════════════════════════════════════════════════════════
    # Warmup: load historical candles from MT5 on first cycle
    # ══════════════════════════════════════════════════════════════

    def _warmup(self) -> None:
        """Pre-fill H1 and M15 histories from MT5 so SQUEEZE and DIVERGENCE
        can trade immediately on restart (no waiting for 20+ bars to accumulate).
        """
        if self._warmup_done:
            return
        self._warmup_done = True
        try:
            import MetaTrader5 as mt5
            if not mt5.terminal_info():
                logger.warning("[ALT] MT5 not ready for warmup — will accumulate live")
                return

            logger.info("[ALT] Warming up M15 history for SQUEEZE & DIVERGENCE...")

            # Set _last_sqz_key BEFORE the per-pair loop so warmup-seeded
            # _squeeze_start_bar values reference the correct current M15 bar key.
            # (Originally this was set at the END of warmup, which left it at 0
            # during pair seeding — making _squeeze_start_bar negative for any
            # pair already in a squeeze at warmup time.)
            now_ts_warmup = int(datetime.now(_JST).timestamp())
            self._last_sqz_key = now_ts_warmup // 900

            for pair in ALL_28:
                # ── M15 warmup for SQUEEZE (25 bars) ──
                sqz_bars = mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_M15, 0, 26)
                if sqz_bars is not None and len(sqz_bars) >= 22:
                    self._sqz_closes[pair] = deque(maxlen=25)
                    self._sqz_highs[pair] = deque(maxlen=25)
                    self._sqz_lows[pair] = deque(maxlen=25)
                    for bar in sqz_bars[:-1]:  # skip the still-forming bar
                        self._sqz_closes[pair].append(float(bar["close"]))
                        self._sqz_highs[pair].append(float(bar["high"]))
                        self._sqz_lows[pair].append(float(bar["low"]))

                    # Pre-compute squeeze state so first bar close can detect a release.
                    #
                    # BUG #6 FIX (2026-04-20): if a pair is currently squeezed at warmup
                    # time, the old code set _squeeze_active=True but did NOT initialize
                    # the extended tracking state (_squeeze_start_bar, _squeeze_ratio_min,
                    # etc.). On the first live M15 bar the code went to the "continuing
                    # squeeze" branch (not "fresh squeeze"), and tracking state was
                    # .get()ed with defaults — producing wrong age/ratio_min/etc at release.
                    # Fix: initialize tracking state from the warmup bars themselves.
                    if len(self._sqz_closes[pair]) >= 20:
                        is_sqz = self._is_squeezed(pair)
                        self._squeeze_active[pair] = is_sqz
                        if is_sqz:
                            # Walk BACKWARDS from the latest bar to find when this
                            # squeeze started. Approximates `real_age_bars` correctly
                            # even for squeezes already in progress.
                            import numpy as _np_sz
                            closes = list(self._sqz_closes[pair])
                            highs = list(self._sqz_highs[pair])
                            lows = list(self._sqz_lows[pair])
                            start_offset = 0  # bars back from latest where squeeze started
                            min_ratio_seen = 1.0
                            min_bb_width_seen = 999.0
                            touches = 0
                            atr_at_start = 0.0
                            for back in range(len(closes) - self._BB_PERIOD + 1):
                                # Offset from end: 0 = latest bar, back = older
                                if back >= len(closes) - self._BB_PERIOD:
                                    break
                                end = len(closes) - back
                                if end < self._BB_PERIOD:
                                    break
                                bb_slice = closes[end - self._BB_PERIOD:end]
                                sma = float(_np_sz.mean(bb_slice))
                                std = float(_np_sz.std(bb_slice, ddof=1))
                                if std < 1e-10:
                                    break
                                bb_u = sma + self._BB_MULT * std
                                bb_l = sma - self._BB_MULT * std
                                # Approx KC via simple ATR over last 10 bars
                                atr_end = end
                                atr_start = max(1, end - self._KC_ATR_PERIOD)
                                tr_vals = []
                                for i in range(atr_start, atr_end):
                                    tr_vals.append(max(
                                        highs[i] - lows[i],
                                        abs(highs[i] - closes[i-1]),
                                        abs(lows[i] - closes[i-1]),
                                    ))
                                if not tr_vals:
                                    break
                                m15_atr_b = float(_np_sz.mean(tr_vals))
                                if m15_atr_b < 1e-10:
                                    break
                                alpha = 2.0 / (self._KC_PERIOD + 1)
                                ema = float(bb_slice[0])
                                for v in bb_slice[1:]:
                                    ema = alpha * v + (1 - alpha) * ema
                                kc_u = ema + self._KC_MULT * m15_atr_b
                                kc_l = ema - self._KC_MULT * m15_atr_b
                                was_sq_back = (bb_u < kc_u) and (bb_l > kc_l)
                                if not was_sq_back:
                                    break  # found the bar before squeeze started
                                # Still squeezed at this offset — update tracking
                                start_offset = back
                                ratio_b = (bb_u - bb_l) / (kc_u - kc_l) if (kc_u - kc_l) > 0 else 1.0
                                width_b = bb_u - bb_l
                                if ratio_b < min_ratio_seen:
                                    min_ratio_seen = ratio_b
                                if width_b < min_bb_width_seen:
                                    min_bb_width_seen = width_b
                                # Only count shallow bars (ratio >= 0.9) for touches
                                if ratio_b >= 0.9:
                                    touches += 1
                                atr_at_start = m15_atr_b  # ATR at the oldest-still-squeezed bar
                            # Seed the tracking dicts so the first live rollover sees
                            # a properly-initialized "continuing squeeze" state.
                            total_squeezed_bars = start_offset + 1  # inclusive of latest
                            self._squeeze_start_bar[pair] = self._last_sqz_key - start_offset
                            self._squeeze_atr_at_start[pair] = atr_at_start
                            self._squeeze_ratio_min[pair] = min_ratio_seen
                            self._squeeze_bb_width_min[pair] = min_bb_width_seen
                            self._squeeze_touches[pair] = touches           # shallow-bar count
                            self._squeeze_bars[pair] = total_squeezed_bars  # total bars for alt_signal_1

                # ── M15 warmup for DIVERGENCE (60 bars) ──
                div_bars = mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_M15, 0, 62)
                if div_bars is not None and len(div_bars) >= 32:
                    self._m15_closes[pair] = deque(maxlen=60)
                    for bar in div_bars[:-1]:  # skip still-forming bar
                        self._m15_closes[pair].append(float(bar["close"]))

                # ── BREAKOUT warmup: load YESTERDAY's range from H1 bars ──
                # 2026-04-23: needed because the redesigned breakout strategy
                # trades against yesterday's range any time of day. Without
                # warmup, the first day after deploy would have no yesterday
                # data and never fire a single trade.
                # 26 H1 bars = ~24h yesterday + a few hours of today's
                # already-formed bars. Filter to the JST calendar day BEFORE
                # today using bar timestamps. Note: MT5 bar timestamps are
                # broker time = GMT+3, so we subtract 3h to get real UTC.
                try:
                    h1_bars = mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_H1, 0, 50)
                    if h1_bars is not None and len(h1_bars) >= 24:
                        now_jst = datetime.now(_JST)
                        today_date = now_jst.date()
                        from datetime import timedelta as _td_brk
                        y_high = None; y_low = None
                        for b in h1_bars:
                            # Bar time is broker GMT+3 — shift to real UTC, then to JST
                            bar_dt_utc = datetime.fromtimestamp(int(b["time"]) - 3*3600, tz=timezone.utc)
                            bar_jst = bar_dt_utc.astimezone(_JST).date()
                            if bar_jst == today_date - _td_brk(days=1):
                                bh, bl = float(b["high"]), float(b["low"])
                                y_high = bh if y_high is None else max(y_high, bh)
                                y_low  = bl if y_low  is None else min(y_low,  bl)
                        if y_high is not None and y_low is not None:
                            self._yesterday_high[pair] = y_high
                            self._yesterday_low[pair]  = y_low
                except Exception as _bk_warm_exc:
                    logger.debug("[BREAKOUT] yesterday-range warmup skipped for %s: %s",
                                 pair, _bk_warm_exc)

            # Set remaining last-bar keys so the first live bar triggers a check.
            # (_last_sqz_key was already set above, before the per-pair loop.)
            now_ts = int(datetime.now(_JST).timestamp())
            self._last_m15_key = now_ts // 900
            self._last_m5_key = now_ts // 300

            sqz_ready = sum(1 for d in self._sqz_closes.values() if len(d) >= 20)
            m15_ready = sum(1 for d in self._m15_closes.values() if len(d) >= 30)
            brk_ready = len(self._yesterday_high)
            logger.info(
                "[ALT] Warmup complete: SQZ(M15) ready for %d/%d pairs, "
                "DIV(M15) ready for %d/%d pairs, BREAKOUT yesterday-range "
                "ready for %d/%d pairs",
                sqz_ready, len(ALL_28), m15_ready, len(ALL_28),
                brk_ready, len(ALL_28),
            )
        except Exception as exc:
            logger.warning("[ALT] Warmup failed: %s — will accumulate live", exc)

    def _is_squeezed(self, pair: str) -> bool:
        """Check if a pair is currently in BB-inside-KC squeeze state (M15)."""
        ch = self._sqz_closes.get(pair)
        hh = self._sqz_highs.get(pair)
        ll = self._sqz_lows.get(pair)
        if not ch or len(ch) < 20 or not hh or not ll:
            return False
        arr = np.array(ch)
        sma = float(np.mean(arr[-20:]))
        std = float(np.std(arr[-20:], ddof=1))
        if std < 1e-10:
            return False
        bb_upper = sma + self._BB_MULT * std
        bb_lower = sma - self._BB_MULT * std
        # Keltner ATR
        c_arr, h_arr, l_arr = list(ch), list(hh), list(ll)
        tr_list = []
        for i in range(max(1, len(c_arr) - self._KC_ATR_PERIOD), len(c_arr)):
            tr = max(h_arr[i] - l_arr[i],
                     abs(h_arr[i] - c_arr[i - 1]) if i > 0 else 0,
                     abs(l_arr[i] - c_arr[i - 1]) if i > 0 else 0)
            tr_list.append(tr)
        atr = float(np.mean(tr_list)) if tr_list else 0.0
        if atr < 1e-10:
            return False
        alpha = 2.0 / (self._KC_PERIOD + 1)
        ema = float(arr[0])
        for v in arr[1:]:
            ema = alpha * v + (1 - alpha) * ema
        kc_upper = ema + self._KC_MULT * atr
        kc_lower = ema - self._KC_MULT * atr
        return (bb_upper < kc_upper) and (bb_lower > kc_lower)

    # ══════════════════════════════════════════════════════════════
    # Public API
    # ══════════════════════════════════════════════════════════════

    def update(
        self,
        high_prices: dict[str, float],
        low_prices: dict[str, float],
        close_prices: dict[str, float],
        h1_atr: dict[str, float],
        m1_bar_time: int,
    ) -> list[Signal]:
        """Run all 3 systems for the current M1 cycle.

        Returns a list of Signal objects to open. The caller is responsible
        for time-of-day filtering and opening the paper trades.
        """
        if not close_prices or m1_bar_time <= 0:
            return []

        # First cycle: load historical candles so SQUEEZE + DIVERGENCE are
        # ready to trade immediately (no 20-bar warmup wait)
        if not self._warmup_done:
            self._warmup()

        jst_now = datetime.fromtimestamp(m1_bar_time, tz=_JST)
        signals: list[Signal] = []

        # 1. BREAKOUT: accumulate Asian range + check breakout
        self._update_breakout(high_prices, low_prices, close_prices, h1_atr, jst_now, signals)

        # 2. SQUEEZE: update M15 history + check squeeze release
        sqz_key = m1_bar_time // 900  # M15 bar key
        if sqz_key != self._last_sqz_key and self._last_sqz_key > 0:
            self._update_sqz(close_prices, high_prices, low_prices)
            self._check_squeeze(close_prices, h1_atr, signals)
        if self._last_sqz_key == 0:
            self._update_sqz(close_prices, high_prices, low_prices)
        self._last_sqz_key = sqz_key

        # 3. DIVERGENCE: M15 history updated on M15 bars, but z-score
        #    checked every M5 bar for faster detection
        m15_key = m1_bar_time // 900
        if m15_key != self._last_m15_key and self._last_m15_key > 0:
            self._update_m15(close_prices)
        if self._last_m15_key == 0:
            self._update_m15(close_prices)
        self._last_m15_key = m15_key

        m5_key = m1_bar_time // 300
        if m5_key != self._last_m5_key and self._last_m5_key > 0:
            self._check_divergence(close_prices, h1_atr, signals)
        self._last_m5_key = m5_key

        return signals

    # ══════════════════════════════════════════════════════════════
    # SYSTEM 1: SESSION RANGE BREAKOUT
    # ══════════════════════════════════════════════════════════════

    # 2026-04-23: REWORKED time-window logic per user request — trade any time
    # of day EXCEPT the early-morning no-trade window (05:00-07:57 JST). The
    # strategy now breaks out of YESTERDAY'S full-day range (snapshot at the
    # 00:00 JST reset) instead of today's still-accumulating Asian range.
    # Today's range continues to accumulate in the background as a snapshot
    # source for tomorrow.
    _NO_TRADE_START_HM = 5 * 60        # 05:00 JST  (inclusive)
    _NO_TRADE_END_HM   = 7 * 60 + 57   # 07:57 JST  (inclusive)
    _MIN_RANGE_PIPS = 15.0             # skip if yesterday's range is too narrow

    def _update_breakout(
        self,
        highs: dict[str, float],
        lows: dict[str, float],
        closes: dict[str, float],
        h1_atr: dict[str, float],
        jst_now: datetime,
        signals: list[Signal],
    ) -> None:
        hm = jst_now.hour * 60 + jst_now.minute
        day_key = jst_now.timetuple().tm_yday

        # ── Daily reset at 00:00 JST ──
        # 2026-04-23: also snapshot today's accumulated range as YESTERDAY's
        # range before clearing. The breakout strategy now trades against this
        # stable yesterday-range any time of day (except the no-trade window),
        # rather than waiting until the Asian session closes at 15:44.
        if day_key != self._asian_day:
            if self._asian_high:  # only snapshot if we actually accumulated something
                self._yesterday_high = dict(self._asian_high)
                self._yesterday_low = dict(self._asian_low)
            self._asian_high.clear()
            self._asian_low.clear()
            self._breakout_fired.clear()
            self._asian_day = day_key

        # ── Always accumulate today's range in the background ──
        # This becomes "yesterday's range" tomorrow at the 00:00 reset.
        for pair in ALL_28:
            h = highs.get(pair)
            l = lows.get(pair)
            if h is None or l is None:
                continue
            if pair in self._asian_high:
                self._asian_high[pair] = max(self._asian_high[pair], h)
                self._asian_low[pair] = min(self._asian_low[pair], l)
            else:
                self._asian_high[pair] = h
                self._asian_low[pair] = l

        # ── No-trade window: 05:00 - 07:57 JST (per user spec 2026-04-23) ──
        if self._NO_TRADE_START_HM <= hm <= self._NO_TRADE_END_HM:
            return

        # ── Breakout check against YESTERDAY'S range (any time outside no-trade window) ──
        for pair in ALL_28:
            if pair in self._breakout_fired:
                continue
            ah = self._yesterday_high.get(pair)
            al = self._yesterday_low.get(pair)
            if ah is None or al is None:
                # No yesterday data yet (first day after deploy, or warmup
                # didn't load this pair). Skip — tomorrow we'll have it.
                continue

            pip = _pip(pair)
            range_pips = (ah - al) / pip
            if range_pips < self._MIN_RANGE_PIPS:
                continue

            close = closes.get(pair, 0.0)
            if close <= 0:
                continue

            direction = ""
            if close > ah:
                direction = "BUY"
            elif close < al:
                direction = "SELL"

                if direction:
                    # REVERTED 2026-04-22 per user request after Apr 22 -126p
                    # day. The previous range-based formula
                    #   sl_pips = min(range_pips, 25)
                    #   tp_pips = min(range_pips * 1.5, 30)
                    # gave R:R ≈ 1.26 and collapsed WR from 80% (Apr 16-17,
                    # 25 trades, +57p) to 29% (Apr 22, 17 trades, -126p).
                    #
                    # On Apr 16-17 the breakout signals were *unintentionally*
                    # using each pair's Sv2 sl_atr/tp_atr settings (the
                    # override args weren't being passed back then), giving
                    # R:R ≈ 0.5 — a high-WR scalp config that suited this
                    # strategy in real markets. That accidental config is
                    # the proven-profitable state.
                    #
                    # Pass sl_pips=0 and tp_pips=0 so paper_trader's
                    # `sl_pips_override > 0 and tp_pips_override > 0` check
                    # fails and it falls back to per-pair settings exactly
                    # like the pre-fix behavior.
                    _brk_dist = (close - ah) / pip if direction == "BUY" else (al - close) / pip
                    signals.append(Signal(
                        pair=pair, direction=direction,
                        entry_price=close,
                        sl_pips=0.0, tp_pips=0.0,  # → per-pair Sv2 settings via fallback
                        system_type="breakout",
                        alt_signal_1=round(range_pips, 1),     # range width
                        alt_signal_2=round(_brk_dist, 1),      # breakout distance past range
                        alt_signal_4=round(ah / pip, 1) if ah else 0,  # asian high in pips scale
                    ))
                    self._breakout_fired.add(pair)
                    logger.info(
                        "[BREAKOUT] %s %s @ %.5f  range=%.1fp  SL/TP from per-pair Sv2 settings",
                        direction, pair, close, range_pips,
                    )

    # ══════════════════════════════════════════════════════════════
    # SYSTEM 2: BOLLINGER + KELTNER SQUEEZE
    # ══════════════════════════════════════════════════════════════

    _BB_PERIOD = 20
    _BB_MULT = 2.0
    _KC_PERIOD = 20
    _KC_ATR_PERIOD = 10
    _KC_MULT = 1.5

    # ── Phase A settings (added 2026-04-20 after historical analysis) ──
    # Analysis of 74 closed trades showed:
    #   • 6 identical EURAUD SELLs fired within hours — ghost-fire bug from
    #     BB/KC oscillation repeatedly crossing back into squeeze and releasing.
    #   • BB/KC ratio 0.9–1.05 at release = marginal zone (29 trades, 44.8% WR,
    #     −157.8p). Outside this zone = 45 trades, 65% WR, +47.9p.
    # Two remedies, both applied here:
    _SQUEEZE_COOLDOWN_BARS = 4           # M15 bars (≈ 1 hour) min gap between fires per pair
    _SQUEEZE_DEADZONE_LO = 0.90          # skip if release ratio ≥ LO AND ≤ HI
    _SQUEEZE_DEADZONE_HI = 1.05          # (0.9–1.05 = indecisive crossover zone)

    def _update_sqz(
        self,
        closes: dict[str, float],
        highs: dict[str, float],
        lows: dict[str, float],
    ) -> None:
        """Append latest M15 bar data to squeeze rolling histories.

        BUG FIX (2026-04-21): previously used the caller-supplied M1 high/low
        as "M15 high/low" at each M15 boundary crossing. That meant the KC
        Keltner-ATR collapsed to ~1/15 of its true M15 size as live data
        overwrote the proper warmup bars — making "BB inside KC" (the squeeze
        test) fire on random M1 micro-compressions rather than real M15
        volatility contractions. This root-caused the losing signal quality.

        Now: pull the just-closed M15 bar directly from MT5 so the deques
        hold true M15 OHLC. Fallback to caller-supplied (M1) data only if
        MT5 is unreachable, keeping the system alive during transient MT5
        outages (degraded quality) rather than silently stopping.
        """
        try:
            import MetaTrader5 as mt5
            if not mt5.terminal_info():
                self._update_sqz_fallback(closes, highs, lows)
                return
            for pair in ALL_28:
                # Index 1 = last CLOSED M15 bar (0 would be the forming one)
                bars = mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_M15, 1, 1)
                if bars is None or len(bars) == 0:
                    continue
                b = bars[0]
                if pair not in self._sqz_closes:
                    self._sqz_closes[pair] = deque(maxlen=25)
                    self._sqz_highs[pair] = deque(maxlen=25)
                    self._sqz_lows[pair] = deque(maxlen=25)
                self._sqz_closes[pair].append(float(b["close"]))
                self._sqz_highs[pair].append(float(b["high"]))
                self._sqz_lows[pair].append(float(b["low"]))
        except Exception as exc:
            logger.warning(
                "[SQUEEZE] M15 fetch failed in _update_sqz: %s — "
                "falling back to M1 data (degraded)", exc,
            )
            self._update_sqz_fallback(closes, highs, lows)

    def _update_sqz_fallback(
        self,
        closes: dict[str, float],
        highs: dict[str, float],
        lows: dict[str, float],
    ) -> None:
        """Emergency fallback: use caller-supplied M1 data when MT5 M15
        fetch is unavailable. Degraded quality (see _update_sqz docstring)
        but keeps the system functional rather than going silent."""
        for pair in ALL_28:
            c = closes.get(pair)
            h = highs.get(pair)
            l = lows.get(pair)
            if c is None or h is None or l is None:
                continue
            if pair not in self._sqz_closes:
                self._sqz_closes[pair] = deque(maxlen=25)
                self._sqz_highs[pair] = deque(maxlen=25)
                self._sqz_lows[pair] = deque(maxlen=25)
            self._sqz_closes[pair].append(c)
            self._sqz_highs[pair].append(h)
            self._sqz_lows[pair].append(l)

    def _check_squeeze(
        self,
        closes: dict[str, float],
        h1_atr: dict[str, float],
        signals: list[Signal],
    ) -> None:
        """Check Bollinger/Keltner squeeze release on M15 for each pair."""
        for pair in ALL_28:
            ch = self._sqz_closes.get(pair)
            if ch is None or len(ch) < self._BB_PERIOD:
                continue

            arr = np.array(ch)

            # Bollinger Bands on M15 closes
            sma = float(np.mean(arr[-self._BB_PERIOD:]))
            std = float(np.std(arr[-self._BB_PERIOD:], ddof=1))
            if std < 1e-10:
                continue
            bb_upper = sma + self._BB_MULT * std
            bb_lower = sma - self._BB_MULT * std

            # Keltner Channels (ATR from M15 high/low/close)
            hh = self._sqz_highs.get(pair)
            ll = self._sqz_lows.get(pair)
            if hh is None or ll is None or len(hh) < self._KC_ATR_PERIOD:
                continue
            tr_list = []
            h_arr = list(hh)
            l_arr = list(ll)
            c_arr = list(ch)
            for i in range(max(1, len(c_arr) - self._KC_ATR_PERIOD), len(c_arr)):
                tr = max(
                    h_arr[i] - l_arr[i],
                    abs(h_arr[i] - c_arr[i - 1]) if i > 0 else 0,
                    abs(l_arr[i] - c_arr[i - 1]) if i > 0 else 0,
                )
                tr_list.append(tr)
            m15_atr = float(np.mean(tr_list)) if tr_list else 0.0
            if m15_atr < 1e-10:
                continue

            # EMA for Keltner center
            alpha = 2.0 / (self._KC_PERIOD + 1)
            ema = float(arr[0])
            for v in arr[1:]:
                ema = alpha * v + (1 - alpha) * ema

            kc_upper = ema + self._KC_MULT * m15_atr
            kc_lower = ema - self._KC_MULT * m15_atr

            # Squeeze detection: BB inside KC
            currently_squeezed = (bb_upper < kc_upper) and (bb_lower > kc_lower)
            was_squeezed = self._squeeze_active.get(pair, False)

            # Pre-compute widths + ratio (used for tracking EVERY bar, not just release)
            bb_width = bb_upper - bb_lower
            kc_width = kc_upper - kc_lower
            ratio_now = (bb_width / kc_width) if kc_width > 0 else 0.0

            if currently_squeezed:
                self._squeeze_active[pair] = True
                self._squeeze_fired[pair] = False  # reset fire flag during squeeze
                self._squeeze_bars[pair] = self._squeeze_bars.get(pair, 0) + 1

                # ── Extended tracking per M15 bar during squeeze ──
                if not was_squeezed:
                    # Fresh squeeze start — initialize tracking
                    self._squeeze_start_bar[pair] = self._last_sqz_key
                    self._squeeze_atr_at_start[pair] = m15_atr
                    self._squeeze_ratio_min[pair] = ratio_now
                    self._squeeze_bb_width_min[pair] = bb_width
                    # BUG #8 FIX: _squeeze_touches counted the same thing as
                    # sqz_real_age_bars (total squeezed bar count). Redefine to
                    # count only SHALLOW bars (ratio >= 0.9, BB near KC edge).
                    # Distinguishes deep squeezes (touches=0, stayed tight) from
                    # shallow ones (touches=many, kept flirting with release).
                    self._squeeze_touches[pair] = 1 if ratio_now >= 0.9 else 0
                else:
                    # Continuing squeeze — update running min + conditional count
                    prev_min_ratio = self._squeeze_ratio_min.get(pair, ratio_now)
                    if ratio_now < prev_min_ratio:
                        self._squeeze_ratio_min[pair] = ratio_now
                    prev_min_w = self._squeeze_bb_width_min.get(pair, bb_width)
                    if bb_width < prev_min_w:
                        self._squeeze_bb_width_min[pair] = bb_width
                    if ratio_now >= 0.9:
                        self._squeeze_touches[pair] = self._squeeze_touches.get(pair, 0) + 1
            else:
                self._squeeze_active[pair] = False
                _sqz_dur = self._squeeze_bars.get(pair, 0)

                # Squeeze just released!
                if was_squeezed and not self._squeeze_fired.get(pair, False):
                    # ─────────── Phase A filter 1: per-pair cooldown ───────────
                    _last_fire_bar = self._squeeze_last_fire_bar.get(pair, -999)
                    _bars_since_fire = self._last_sqz_key - _last_fire_bar
                    if _bars_since_fire < self._SQUEEZE_COOLDOWN_BARS:
                        logger.info(
                            "[SQUEEZE] %s release SKIPPED — cooldown (%d/%d bars since last fire)",
                            pair, _bars_since_fire, self._SQUEEZE_COOLDOWN_BARS,
                        )
                        self._squeeze_fired[pair] = True
                        self._reset_squeeze_tracking(pair)
                        continue

                    _bbkc_ratio = round(ratio_now, 3)

                    # ─────────── Phase A filter 2: dead-zone skip ───────────
                    if self._SQUEEZE_DEADZONE_LO <= _bbkc_ratio <= self._SQUEEZE_DEADZONE_HI:
                        logger.info(
                            "[SQUEEZE] %s release SKIPPED — BB/KC ratio %.3f in dead zone [%.2f, %.2f]",
                            pair, _bbkc_ratio,
                            self._SQUEEZE_DEADZONE_LO, self._SQUEEZE_DEADZONE_HI,
                        )
                        self._squeeze_fired[pair] = True
                        self._reset_squeeze_tracking(pair)
                        continue

                    latest_close = float(arr[-1])
                    pip = _pip(pair)

                    # ─────────── Phase A filter 3: require real band-break ───────────
                    # BUG FIX (2026-04-21): previously direction was decided by
                    # close-vs-SMA (mid-line). Since a squeeze-release always
                    # happens with close very near the SMA (that's what "in
                    # squeeze" means), direction was essentially coin-flip
                    # noise — and the 113-trade journal showed a consistent
                    # money-losing pattern regardless of direction. Now we
                    # require the close to actually CLOSE OUTSIDE the BB —
                    # a true directional break — or we skip the signal.
                    if latest_close > bb_upper:
                        direction = "BUY"
                    elif latest_close < bb_lower:
                        direction = "SELL"
                    else:
                        logger.info(
                            "[SQUEEZE] %s release SKIPPED — close %.5f inside bands "
                            "[%.5f, %.5f] (no directional break)",
                            pair, latest_close, bb_lower, bb_upper,
                        )
                        self._squeeze_fired[pair] = True
                        self._squeeze_last_fire_bar[pair] = self._last_sqz_key
                        self._reset_squeeze_tracking(pair)
                        continue

                    # ── Extract extended tracking state before computing fields ──
                    sqz_start = self._squeeze_start_bar.get(pair, self._last_sqz_key)
                    real_age = max(0, self._last_sqz_key - sqz_start)
                    atr_at_start = self._squeeze_atr_at_start.get(pair, m15_atr)
                    ratio_min_stored = self._squeeze_ratio_min.get(pair, ratio_now)
                    bb_width_min_price = self._squeeze_bb_width_min.get(pair, bb_width)
                    touches = self._squeeze_touches.get(pair, 0)

                    # Market-wide concurrent squeeze count (OTHER pairs still in squeeze)
                    concurrent_other = sum(
                        1 for p, active in self._squeeze_active.items()
                        if p != pair and active
                    )

                    # SL/TP scaled for M15 timeframe using H1 ATR.
                    # BUG FIX (2026-04-21): previous 0.3×/0.6× was far too tight
                    # for a volatility-expansion strategy — normal M15 post-
                    # squeeze noise ran through the 4-pip SL floor before the
                    # 6-pip TP could trigger. Restored to the original design
                    # spec (SL=1×ATR, TP=2×ATR, R:R=2:1) with wider clamps.
                    _atr_raw = h1_atr.get(pair, 0.0)
                    if _atr_raw <= 0:
                        _atr_raw = m15_atr * 4
                    sl_pips = round(1.0 * _atr_raw / pip, 1)
                    tp_pips = round(2.0 * _atr_raw / pip, 1)
                    sl_pips = max(8.0, min(sl_pips, 40.0))
                    tp_pips = max(16.0, min(tp_pips, 80.0))

                    _sma_dist = round((latest_close - sma) / pip, 1)

                    # ── Compute the 10 new extended fields ──
                    _bb_width_pips_release = round(bb_width / pip, 1)
                    _bb_width_min_pips = round(bb_width_min_price / pip, 1)
                    _dist_upper_bb = round((bb_upper - latest_close) / pip, 1)
                    _dist_lower_bb = round((latest_close - bb_lower) / pip, 1)
                    if kc_width > 0:
                        _close_pos_kc = (latest_close - kc_lower) / kc_width
                        # Don't clamp; negative/>1 values communicate "broke out" magnitude.
                        _close_pos_kc = round(_close_pos_kc, 3)
                    else:
                        _close_pos_kc = -1.0  # degenerate KC; mark as not measured
                    _atr_ratio_during = (
                        round(m15_atr / atr_at_start, 3) if atr_at_start > 0 else 1.0
                    )

                    signals.append(Signal(
                        pair=pair, direction=direction,
                        entry_price=latest_close,
                        sl_pips=sl_pips, tp_pips=tp_pips,
                        system_type="squeeze",
                        alt_signal_1=float(_sqz_dur),
                        alt_signal_2=_bbkc_ratio,
                        alt_signal_4=_sma_dist,
                        # ── 10 new squeeze-specific fields ──
                        sqz_bb_kc_ratio_min=round(ratio_min_stored, 3),
                        sqz_bb_width_pips_release=_bb_width_pips_release,
                        sqz_bb_width_min_pips=_bb_width_min_pips,
                        sqz_real_age_bars=real_age,
                        sqz_dist_to_upper_bb_pips=_dist_upper_bb,
                        sqz_dist_to_lower_bb_pips=_dist_lower_bb,
                        sqz_close_pos_in_kc=_close_pos_kc,
                        sqz_atr_ratio_during=_atr_ratio_during,
                        sqz_touches_count=touches,
                        sqz_concurrent_count=concurrent_other,
                    ))
                    self._squeeze_fired[pair] = True
                    self._squeeze_last_fire_bar[pair] = self._last_sqz_key
                    self._reset_squeeze_tracking(pair)
                    logger.info(
                        "[SQUEEZE] %s %s @ %.5f  BB=%.5f/%.5f  KC=%.5f/%.5f  SL=%.1fp TP=%.1fp  "
                        "ratio=%.3f  min_ratio=%.3f  age=%d  touches=%d  concurrent=%d",
                        direction, pair, latest_close,
                        bb_lower, bb_upper, kc_lower, kc_upper,
                        sl_pips, tp_pips, _bbkc_ratio,
                        ratio_min_stored, real_age, touches, concurrent_other,
                    )

    def _reset_squeeze_tracking(self, pair: str) -> None:
        """Clear extended squeeze tracking dicts for a pair. Called when
        the squeeze ENDS — whether via fire, cooldown-skip, or dead-zone-skip.
        Next fresh squeeze will re-initialize these in the `not was_squeezed`
        branch of _check_squeeze.
        """
        self._squeeze_start_bar.pop(pair, None)
        self._squeeze_atr_at_start.pop(pair, None)
        self._squeeze_ratio_min.pop(pair, None)
        self._squeeze_bb_width_min.pop(pair, None)
        self._squeeze_touches.pop(pair, None)
        # Also reset _squeeze_bars so alt_signal_1 reflects THIS squeeze only
        self._squeeze_bars.pop(pair, None)

    # ══════════════════════════════════════════════════════════════
    # SYSTEM 3: CORRELATION DIVERGENCE
    # ══════════════════════════════════════════════════════════════

    _DIV_WINDOW = 60     # M15 bars (= 15 hours)
    _DIV_Z_ENTRY = 2.0   # z-score threshold to enter
    _DIV_Z_RESET = 1.0   # z-score threshold to allow re-entry

    def _update_m15(self, closes: dict[str, float]) -> None:
        """Append latest M15 close to rolling history."""
        for pair in ALL_28:
            c = closes.get(pair)
            if c is None or c <= 0:
                continue
            if pair not in self._m15_closes:
                self._m15_closes[pair] = deque(maxlen=self._DIV_WINDOW)
            self._m15_closes[pair].append(c)

    def _check_divergence(
        self,
        closes: dict[str, float],
        h1_atr: dict[str, float],
        signals: list[Signal],
    ) -> None:
        """Check correlation divergence for each pair group.

        The reference distribution (mean/std) comes from M15 history (stable).
        The CURRENT ratio uses live M5 closes (fast detection).
        """
        for pair_a, pair_b in CORR_GROUPS:
            hist_a = self._m15_closes.get(pair_a)
            hist_b = self._m15_closes.get(pair_b)
            if (hist_a is None or hist_b is None
                    or len(hist_a) < 30 or len(hist_b) < 30):
                continue

            # Reference distribution from M15 history
            n = min(len(hist_a), len(hist_b))
            a = np.array(list(hist_a)[-n:])
            b = np.array(list(hist_b)[-n:])

            # Mask-based zero/negative-safe log ratio (2026-05-07 fix).
            # Previously used `with np.errstate(divide="ignore", invalid=
            # "ignore"):` to suppress division warnings, but NumPy 2.4
            # made errstate non-reentrant in a way that occasionally
            # raised "Cannot enter np.errstate twice" in production
            # under PyQt's QThread model. The mask-based approach avoids
            # the suppression entirely — we only compute log on rows
            # where a > 0 AND b > 0 (the only cases where log(a/b) is
            # finite), then check finiteness as before.
            mask = (a > 0) & (b > 0)
            if not mask.all():
                # Any zero or negative in the reference window — skip
                # this pair-group rather than introducing NaN positions.
                continue
            ratio_hist = np.log(a / b)

            if not np.all(np.isfinite(ratio_hist)):
                continue

            mean = float(np.mean(ratio_hist))
            std = float(np.std(ratio_hist, ddof=1))
            if std < 1e-10:
                continue

            # Current ratio from live M5 close prices (fast detection)
            cur_a = closes.get(pair_a, 0.0)
            cur_b = closes.get(pair_b, 0.0)
            if cur_a <= 0 or cur_b <= 0:
                continue
            cur_ratio = math.log(cur_a / cur_b)
            z = (cur_ratio - mean) / std
            group_key = f"{pair_a}_{pair_b}"

            # Reset fire flag when z reverts
            if abs(z) < self._DIV_Z_RESET:
                self._div_fired[group_key] = False

            # Check for entry
            if abs(z) >= self._DIV_Z_ENTRY and not self._div_fired.get(group_key, False):
                # z > 0 means A is outperforming B → trade B (underperformer) BUY
                # z < 0 means B is outperforming A → trade A (underperformer) BUY
                if z > 0:
                    trade_pair = pair_b
                    direction = "BUY"
                else:
                    trade_pair = pair_a
                    direction = "BUY"

                close = closes.get(trade_pair, 0.0)
                if close <= 0:
                    continue

                pip = _pip(trade_pair)
                _atr_raw = h1_atr.get(trade_pair, 0.0)
                if _atr_raw <= 0:
                    _atr_raw = 20.0 * pip  # fallback: ~20 pips

                # SL/TP sized for 8-25 pip targets: 0.3× H1 ATR SL, 0.4× TP
                sl_pips = round(0.3 * _atr_raw / pip, 1)
                tp_pips = round(0.4 * _atr_raw / pip, 1)
                sl_pips = max(5.0, min(sl_pips, 20.0))
                tp_pips = max(5.0, min(tp_pips, 25.0))

                if sl_pips >= 3 and tp_pips >= 3:
                    signals.append(Signal(
                        pair=trade_pair, direction=direction,
                        entry_price=close,
                        sl_pips=sl_pips, tp_pips=tp_pips,
                        system_type="divergence",
                        alt_signal_1=round(z, 2),                  # z-score at entry
                        alt_signal_2=round(cur_ratio - mean, 6),   # ratio deviation from mean
                        alt_signal_3=f"{pair_a}/{pair_b}",         # pair group
                        alt_signal_4=round(std, 6),                # std of historical ratio
                    ))
                    self._div_fired[group_key] = True
                    logger.info(
                        "[DIVERGENCE] %s %s @ %.5f  z=%.2f  group=%s/%s",
                        direction, trade_pair, close, z, pair_a, pair_b,
                    )
