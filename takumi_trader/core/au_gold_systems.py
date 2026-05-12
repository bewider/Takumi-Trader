"""AU Gold suite — 5 XAUUSD trading strategies.

Strategies:
  AU1  London Breakout       — Asian range → London-session H1 breakout
  AU2  NY ORB                — M1 breakout of NY-Open 2-minute range
  AU3  H1 Trend Pullback     — H1 pullback within H4-trend regime
  AU4  USD Divergence        — XAUUSD vs USD-strength correlation break
  AU5  Asian Mean Reversion  — M15 RSI extremes in low-vol Asian session

All 5 are paper-only (no cTrader, no MT5 auto-trade). Data source is
`result.xau_candles` (isolated from the forex strength engine — no reach-
back into ALL_28_PAIRS / CURRENCIES / _CCY_PAIRS). USD strength for
filters AU1/AU2/AU4 is read from `result.composite_scores['USD']` on the
existing 0-10 scale (3 = weak USD favouring LONG gold; 7 = strong USD
favouring SHORT gold).

Times throughout are JST unless stated otherwise. NY session bounds are
converted from NY-local to JST via `session_manager.ny_session_to_jst_minutes`
(DST-aware).

Phase A deliverable: this file ships with the engine wired but all 5
strategies are SKELETONS returning empty signal lists. Phase B1-B5 will
implement each strategy body individually.
"""
from __future__ import annotations

import json
import logging
import math
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import numpy as np

from takumi_trader.core.session_manager import (
    _JST, ny_session_to_jst_minutes,
)

logger = logging.getLogger(__name__)


# ── USD-strength thresholds on the existing 0-10 scale ──
# Spec's "-3.0 / +3.0" translated to TAKUMI's 0-10 composite scale:
#   LONG gold:  USD weak   → composite_scores['USD'] <= 3.0
#   SHORT gold: USD strong → composite_scores['USD'] >= 7.0
USD_LONG_MAX = 3.0
USD_SHORT_MIN = 7.0


@dataclass
class GoldSignal:
    """A trade signal emitted by an AU gold strategy.

    Consumed by main_window.py which calls paper_trader.open_paper_trade.
    Mirrors the shape of alt_systems.Signal so the two systems can be
    processed by similar harnesses in main_window.
    """
    strategy_id: str             # "au1_london_breakout" etc.
    direction: str               # "BUY" or "SELL"
    entry_price: float
    sl_pips: float               # 1 pip = 0.01 for gold
    tp_pips: float
    entry_reason: str = ""       # Human-readable trigger description
    # Per-strategy metadata stashed into PaperTradeRecord.au_metadata_json:
    metadata: dict = field(default_factory=dict)
    # Convenience fields that map directly to au_* columns on the record:
    asian_range_high: float = 0.0     # AU1
    asian_range_low: float = 0.0      # AU1
    correlation_xau_usd: float = 0.0  # AU4
    rsi_at_entry: float = 0.0         # AU5


class AuGoldSystemEngine:
    """Owns the rolling state for all 5 AU gold strategies.

    Called once per M1 cycle from main_window._on_data() with the latest
    `CalculationResult`. Emits `GoldSignal` objects; main_window decides
    whether to open paper trades.

    State notes:
      * AU1: Asian H/L accumulated 00:00-07:00 GMT (= 09:00-16:00 JST-ish);
             one trade per day per direction.
      * AU2: NY 2-min range (09:30-09:32 NY local) stored per trading day.
      * AU3: H4/H1 regime + pullback state; trend change clears pullback.
      * AU4: Rolling 30-period Pearson on (XAUUSD H1 close, USD score H1).
      * AU5: RSI(14) M15 rolling deque.
    """

    # ══════════════════════════════════════════════════════════════════
    # Initialisation
    # ══════════════════════════════════════════════════════════════════

    def __init__(self) -> None:
        # AU1 — Asian range
        self._au1_day: int = -1                       # JST day-of-year (last reset)
        self._au1_asian_high: float | None = None
        self._au1_asian_low: float | None = None
        self._au1_fired_today: set[str] = set()       # {"BUY","SELL"} fired today
        self._au1_break_direction: str | None = None  # which side broke (for retest)
        self._au1_break_price: float | None = None    # broken level (anchor for retest)
        self._au1_range_logged_today: bool = False    # one-shot range-lock log flag

        # AU2 — NY ORB
        self._au2_day: int = -1
        self._au2_orb_high: float | None = None
        self._au2_orb_low: float | None = None
        self._au2_fired_today: bool = False

        # AU3 — Trend pullback
        self._au3_regime: str = "NEUTRAL"   # "BULL"/"BEAR"/"NEUTRAL"
        self._au3_pullback_bars: int = 0
        self._au3_pullback_extreme: float | None = None
        self._au3_last_h1_key: int = 0

        # AU4 — USD divergence
        self._au4_xau_h1_closes: deque = deque(maxlen=50)
        self._au4_usd_h1_scores: deque = deque(maxlen=50)
        self._au4_last_h1_key: int = 0
        self._au4_cooldown_until: float = 0.0         # epoch seconds; avoid back-to-back

        # AU5 — Mean reversion
        self._au5_m15_closes: deque = deque(maxlen=30)
        self._au5_last_m15_key: int = 0
        self._au5_last_rsi: float = 50.0
        self._au5_cooldown_until: float = 0.0

        # Per-strategy warmup flags (loads MT5 history when each timeframe
        # first appears in result.xau_candles). Per-strategy because the
        # MT5 worker fetches M15/H1/H4 on staggered schedules — the first
        # update() call may have ONLY M1 populated, so we need to retry
        # warmup for individual strategies on later cycles as their
        # required timeframes arrive.
        self._au3_warmup_done = False
        self._au5_warmup_done = False

    # ══════════════════════════════════════════════════════════════════
    # Public API
    # ══════════════════════════════════════════════════════════════════

    def update(self, result) -> list[GoldSignal]:
        """Process one M1 cycle. Returns list of GoldSignal to open.

        Silently returns [] if:
          - No gold symbol resolved on this broker (result.xau_symbol == "")
          - No M1 bar data for XAUUSD yet (result.xau_candles empty)
          - System-level no-trade window active (caller handles that)
        """
        # Early skip: no gold data channel
        if not result.xau_symbol or not result.xau_candles:
            return []
        if result.xau_price <= 0:
            return []

        # Per-strategy warmup from MT5 history. AU3 needs H1+H4; AU5 needs
        # M15. The MT5 worker fetches H1/M15 every 5 cycles and H4 every 30,
        # so the first cycle after a fresh connection often has ONLY M1
        # populated. We retry each strategy's warmup until ITS data arrives.
        # AU1/AU2 are time-window strategies (warmup-skipped — they re-build
        # naturally in the next session). AU4 cannot be warmed up (no
        # historical composite_scores['USD'] snapshots exist).
        if not (self._au3_warmup_done and self._au5_warmup_done):
            try:
                self._warmup(result)
            except Exception as wexc:
                logger.warning("[AU GOLD] warmup raised (will retry next cycle): %s", wexc)

        signals: list[GoldSignal] = []

        # Per-strategy isolation — one strategy's exception MUST NOT
        # take down the others. Each catch surfaces in logs at WARNING
        # so the operator can see a recurring fault without the engine
        # silently going dark.
        for tag, fn in (
            ("AU1", self._run_au1),
            ("AU2", self._run_au2),
            ("AU3", self._run_au3),
            ("AU4", self._run_au4),
            ("AU5", self._run_au5),
        ):
            try:
                signals.extend(fn(result))
            except Exception as exc:
                # exc_info=True captures full traceback so we can pinpoint
                # which line raised. Previously the error message alone
                # ("'>' not supported between instances of 'float' and
                # 'NoneType'") didn't tell us which comparison was at fault.
                logger.warning("[%s] strategy raised %s: %s",
                               tag, type(exc).__name__, exc, exc_info=True)

        return signals

    # ══════════════════════════════════════════════════════════════════
    # Warmup + introspection
    # ══════════════════════════════════════════════════════════════════

    def _warmup(self, result) -> None:
        """Seed AU3 + AU5 state from existing MT5 candle history.

        Called repeatedly until both `_au3_warmup_done` and `_au5_warmup_done`
        flip True. Each strategy's section sets its own flag only when its
        required timeframe was actually present in `xau_candles` (the MT5
        worker fetches M15/H1/H4 on staggered schedules — first cycle after
        connection often only has M1).

        AU1/AU2 are time-window strategies — their state naturally
        re-builds during the next eligible session window. Pre-seeding
        them from M1 history would require shifting broker timestamps
        to JST, which is fragile across brokers; we skip warmup for them.

        AU4 cannot be warmed up because no historical USD-strength
        scores exist (composite_scores['USD'] is computed live only).
        It will be active 30 H1-bars after first connection.
        """
        h1_bars = result.xau_candles.get("H1")
        h4_bars = result.xau_candles.get("H4")
        m15_bars = result.xau_candles.get("M15")

        # ── AU3 warmup: needs BOTH H1 (≥2 bars) AND H4 (≥27 for EMA+slope) ──
        # H4 minimum mirrors the strategy's own gate (see _run_au3) so the
        # warmup-computed regime matches what the strategy would compute.
        if not self._au3_warmup_done:
            ok_h1 = h1_bars is not None and len(h1_bars) >= 2
            ok_h4 = h4_bars is not None and len(h4_bars) >= 27
            if ok_h1:
                # Anchor last_h1_key to most recent CLOSED H1 — first live
                # H1-close trigger fires on a NEW bar (no double-process).
                self._au3_last_h1_key = int(h1_bars[-2]["time"])
                logger.info("[AU GOLD] AU3 warmup: anchored last_h1_key=%d",
                            self._au3_last_h1_key)
            if ok_h4:
                # Compute initial regime so get_status() shows it before
                # the first H1 close arrives.
                try:
                    h4_closed = h4_bars[:-1]
                    arr = np.array([float(b["close"]) for b in h4_closed[-50:]],
                                   dtype=np.float64)
                    ema = self._ema_iter(arr, 20)
                    if ema is not None and len(ema) >= 6:
                        h4_close = float(h4_closed[-1]["close"])
                        if h4_close > float(ema[-1]) and ema[-1] > ema[-6]:
                            self._au3_regime = "BULL"
                        elif h4_close < float(ema[-1]) and ema[-1] < ema[-6]:
                            self._au3_regime = "BEAR"
                        else:
                            self._au3_regime = "NEUTRAL"
                        logger.info("[AU GOLD] AU3 warmup: H4 regime=%s",
                                    self._au3_regime)
                except Exception as exc:
                    logger.debug("[AU GOLD] AU3 regime warmup skipped: %s", exc)
            # Mark done only when BOTH timeframes were present. Avoids
            # leaving AU3 partially-warmed if H1 arrives before H4.
            if ok_h1 and ok_h4:
                self._au3_warmup_done = True

        # ── AU5 warmup: needs M15 (≥17 bars: 15 closed for RSI + forming) ──
        if not self._au5_warmup_done:
            if m15_bars is not None and len(m15_bars) >= 17:
                closed_m15 = list(m15_bars[:-1])
                recent = closed_m15[-30:]
                self._au5_m15_closes.clear()
                for bar in recent:
                    self._au5_m15_closes.append(float(bar["close"]))
                self._au5_last_m15_key = int(recent[-1]["time"])
                rsi = self._rsi_sma(list(self._au5_m15_closes), 14)
                if rsi is not None:
                    self._au5_last_rsi = rsi
                logger.info(
                    "[AU GOLD] AU5 warmup: %d M15 closes loaded, RSI(14)=%.1f",
                    len(self._au5_m15_closes), self._au5_last_rsi,
                )
                self._au5_warmup_done = True

        # Final summary — only emit when BOTH strategies are warmed up.
        if (self._au3_warmup_done and self._au5_warmup_done):
            logger.info(
                "[AU GOLD] Warmup COMPLETE — AU3 regime=%s, AU5 deque=%d/30 "
                "(RSI=%.1f), AU1/AU2 will accumulate live, AU4 needs 30 H1 cycles",
                self._au3_regime, len(self._au5_m15_closes), self._au5_last_rsi,
            )

    def get_status(self) -> dict:
        """Snapshot of engine internals for UI / debug display.

        Returns a flat dict with keys per strategy. Read-only — calling
        this never mutates engine state. Safe to call from the UI thread.
        """
        return {
            "warmup_done": self._au3_warmup_done and self._au5_warmup_done,
            "au3_warmup_done": self._au3_warmup_done,
            "au5_warmup_done": self._au5_warmup_done,
            "au1": {
                "asian_high": self._au1_asian_high,
                "asian_low": self._au1_asian_low,
                "asian_range_pips": (
                    (self._au1_asian_high - self._au1_asian_low) / 0.01
                    if (self._au1_asian_high is not None
                        and self._au1_asian_low is not None) else None
                ),
                "fired_today": sorted(self._au1_fired_today),
                "day_ordinal": self._au1_day,
            },
            "au2": {
                "orb_high": self._au2_orb_high,
                "orb_low": self._au2_orb_low,
                "orb_range_pips": (
                    (self._au2_orb_high - self._au2_orb_low) / 0.01
                    if (self._au2_orb_high is not None
                        and self._au2_orb_low is not None) else None
                ),
                "fired_today": self._au2_fired_today,
                "session_day_ordinal": self._au2_day,
            },
            "au3": {
                "regime": self._au3_regime,
                "pullback_bars": self._au3_pullback_bars,
                "pullback_extreme": self._au3_pullback_extreme,
                "last_h1_key": self._au3_last_h1_key,
            },
            "au4": {
                "samples": len(self._au4_xau_h1_closes),
                "samples_needed": 30,
                "ready": len(self._au4_xau_h1_closes) >= 30,
                "cooldown_remaining_sec": max(
                    0.0, self._au4_cooldown_until - time.time()
                ) if self._au4_cooldown_until else 0.0,
            },
            "au5": {
                "samples": len(self._au5_m15_closes),
                "samples_needed": 15,
                "ready": len(self._au5_m15_closes) >= 15,
                "last_rsi": round(self._au5_last_rsi, 2),
                "cooldown_remaining_sec": max(
                    0.0, self._au5_cooldown_until - time.time()
                ) if self._au5_cooldown_until else 0.0,
            },
        }

    # ══════════════════════════════════════════════════════════════════
    # Strategy bodies — all SKELETON in Phase A. Each returns [] pending
    # Phase B implementation. Structure is in place so wiring can be
    # tested end-to-end (OPEN TRADES panel / LiveCandle tab / journal)
    # without real trades firing yet.
    # ══════════════════════════════════════════════════════════════════

    def _run_au1(self, result) -> list[GoldSignal]:
        """AU1 — Asian Range → London Breakout.

        Timing (JST):
          08:00-15:00   Accumulate Asian HIGH / LOW of XAUUSD M1 bars
          15:45-20:00   Breakout window — fire on M1 close piercing the range
          >= 20:00      Stop firing new trades for today
          00:00         Day reset (clears range + fired set)

        Entry rules:
          * BUY  if xau_price > asian_high AND USD score <= 3.0 (USD_LONG_MAX)
          * SELL if xau_price < asian_low  AND USD score >= 7.0 (USD_SHORT_MIN)
          * One trade per direction per day

        SL/TP (in gold-pips, 1 pip = 0.01):
          * SL = full Asian range width (1× range)
          * TP = 2× Asian range width (2:1 R:R)

        Guards:
          * Skip if Asian range < 30 pips — too tight, whipsaw prone
          * Skip if current spread > 30 pips — bad fill environment (news)
          * Skip if xau_price missing
        """
        signals: list[GoldSignal] = []

        now_jst = datetime.now(_JST)
        jst_day = now_jst.toordinal()
        jst_min = now_jst.hour * 60 + now_jst.minute

        # Daily reset at JST midnight
        if jst_day != self._au1_day:
            if self._au1_day != -1:  # not the very first call
                logger.info("[AU1] Day reset (JST midnight) — Asian range cleared")
            self._au1_day = jst_day
            self._au1_asian_high = None
            self._au1_asian_low = None
            self._au1_fired_today = set()
            self._au1_break_direction = None
            self._au1_break_price = None
            self._au1_range_logged_today = False

        # Window bounds (JST minute-of-day)
        ASIAN_START = 8 * 60        # 08:00
        ASIAN_END   = 15 * 60       # 15:00
        BREAK_START = 15 * 60 + 45  # 15:45 (London open)
        BREAK_END   = 20 * 60       # 20:00 (end of London peak volatility)

        # ── Phase 1: accumulate Asian range ──
        if ASIAN_START <= jst_min < ASIAN_END:
            hi = result.xau_high
            lo = result.xau_low
            # Defensive type/None check (2026-05-07 hardening).
            # Dataclass default is 0.0 so these SHOULD always be float,
            # but production saw TypeError on comparisons here that we
            # couldn't reproduce in isolation. Belt-and-suspenders.
            if not (isinstance(hi, (int, float)) and isinstance(lo, (int, float))):
                return signals
            if hi > 0 and lo > 0:
                if self._au1_asian_high is None or hi > self._au1_asian_high:
                    self._au1_asian_high = hi
                if self._au1_asian_low is None or lo < self._au1_asian_low:
                    self._au1_asian_low = lo
            return signals

        # ── Phase 2: outside breakout window → idle ──
        if not (BREAK_START <= jst_min < BREAK_END):
            # Log range-lock ONCE per day, on the first cycle past 15:00 JST.
            # `jst_min == ASIAN_END` would re-fire ~60 times across the 15:00
            # minute, so guard with a one-shot flag that resets at midnight.
            if (jst_min >= ASIAN_END
                    and not self._au1_range_logged_today
                    and self._au1_asian_high is not None
                    and self._au1_asian_low is not None):
                rng_pips = (self._au1_asian_high - self._au1_asian_low) / 0.01
                logger.info(
                    "[AU1] Asian range LOCKED: high=%.2f low=%.2f range=%.0fp",
                    self._au1_asian_high, self._au1_asian_low, rng_pips,
                )
                self._au1_range_logged_today = True
            return signals

        # ── Phase 3: breakout checks ──
        if self._au1_asian_high is None or self._au1_asian_low is None:
            return signals  # didn't observe full Asian session
        # Defensive type check on state (2026-05-07 hardening). The
        # state SHOULD be float per Phase 1's assignments, but production
        # saw TypeError on subsequent comparisons that this guards.
        if not (isinstance(self._au1_asian_high, (int, float))
                and isinstance(self._au1_asian_low, (int, float))):
            return signals

        rng_price = self._au1_asian_high - self._au1_asian_low
        rng_pips = rng_price / 0.01  # XAU pip = 0.01
        if rng_pips < 30.0:
            return signals  # too tight

        # Spread guard — avoid trading into news/illiquid ticks
        sp = result.xau_spread_points
        if isinstance(sp, (int, float)) and sp > 30.0:
            return signals

        price = result.xau_price
        if not isinstance(price, (int, float)) or price <= 0:
            return signals

        sl_pips = rng_pips        # 1× range
        tp_pips = rng_pips * 2.0  # 2× range → 2:1 R:R
        usd_score = result.composite_scores.get("USD", 0.0) or 0.0

        # BUY breakout (above Asian high, USD weak)
        if (price > self._au1_asian_high
                and "BUY" not in self._au1_fired_today
                and AuGoldSystemEngine.usd_long_ok(result)):
            self._au1_fired_today.add("BUY")
            self._au1_break_direction = "BUY"
            self._au1_break_price = self._au1_asian_high
            signals.append(GoldSignal(
                strategy_id="au1_london_breakout",
                direction="BUY",
                entry_price=price,
                sl_pips=sl_pips,
                tp_pips=tp_pips,
                entry_reason=(
                    f"London breakout BUY above Asian high {self._au1_asian_high:.2f} "
                    f"(range={rng_pips:.1f}p, USD={usd_score:.1f})"
                ),
                asian_range_high=self._au1_asian_high,
                asian_range_low=self._au1_asian_low,
                metadata={
                    "asian_range_pips": round(rng_pips, 2),
                    "asian_high": self._au1_asian_high,
                    "asian_low": self._au1_asian_low,
                    "usd_strength_at_entry": round(usd_score, 2),
                    "spread_points_at_entry": round(result.xau_spread_points or 0.0, 2),
                    "jst_minute_at_entry": jst_min,
                    "break_price": self._au1_asian_high,
                },
            ))
            return signals  # only one fire per cycle

        # SELL breakout (below Asian low, USD strong)
        if (price < self._au1_asian_low
                and "SELL" not in self._au1_fired_today
                and AuGoldSystemEngine.usd_short_ok(result)):
            self._au1_fired_today.add("SELL")
            self._au1_break_direction = "SELL"
            self._au1_break_price = self._au1_asian_low
            signals.append(GoldSignal(
                strategy_id="au1_london_breakout",
                direction="SELL",
                entry_price=price,
                sl_pips=sl_pips,
                tp_pips=tp_pips,
                entry_reason=(
                    f"London breakout SELL below Asian low {self._au1_asian_low:.2f} "
                    f"(range={rng_pips:.1f}p, USD={usd_score:.1f})"
                ),
                asian_range_high=self._au1_asian_high,
                asian_range_low=self._au1_asian_low,
                metadata={
                    "asian_range_pips": round(rng_pips, 2),
                    "asian_high": self._au1_asian_high,
                    "asian_low": self._au1_asian_low,
                    "usd_strength_at_entry": round(usd_score, 2),
                    "spread_points_at_entry": round(result.xau_spread_points or 0.0, 2),
                    "jst_minute_at_entry": jst_min,
                    "break_price": self._au1_asian_low,
                },
            ))

        return signals

    def _run_au2(self, result) -> list[GoldSignal]:
        """AU2 — NY Open Range Breakout (DST-aware).

        NY local 09:30 → JST 22:30 (summer EDT) or 23:30 (winter EST).
        The 3-hour breakout window extends past JST midnight, so the daily
        reset is anchored 6h before JST midnight (i.e. at 06:00 JST) so
        the entire NY session lives inside one "session day".

        Timing:
          09:30-09:32 NY   Capture 2-min Opening Range (ORB H/L)
          09:32-12:30 NY   Breakout window — fire on M1 close beyond ORB
          (after)          Idle until next 09:30 NY

        Entry rules (single trade per session day, EITHER direction):
          * BUY  if xau_price > orb_high AND USD <= 3.0
          * SELL if xau_price < orb_low  AND USD >= 7.0

        Risk:
          * SL = 1× ORB width
          * TP = 2× ORB width (2:1 R:R)

        Guards:
          * Skip if ORB < 5 pips (0.05) — useless R:R
          * Skip if spread > 30 pips
        """
        signals: list[GoldSignal] = []

        now_jst = datetime.now(_JST)
        jst_min = now_jst.hour * 60 + now_jst.minute

        # NY-session day anchor: shift JST by -6h so reset boundary sits at
        # 06:00 JST (between end of one NY session and start of next).
        session_day = (now_jst - timedelta(hours=6)).toordinal()
        if session_day != self._au2_day:
            if self._au2_day != -1:
                logger.info("[AU2] NY-session day reset — ORB cleared")
            self._au2_day = session_day
            self._au2_orb_high = None
            self._au2_orb_low = None
            self._au2_fired_today = False

        # DST-aware JST minute-of-day bounds for THIS NY session
        orb_start_min, orb_end_min = ny_session_to_jst_minutes(
            (9, 30), (9, 32), when=now_jst,
        )
        break_start_min, break_end_min = ny_session_to_jst_minutes(
            (9, 32), (12, 30), when=now_jst,
        )

        def _in_window(m: int, start: int, end: int) -> bool:
            """Wrap-aware window check (start may be > end if window crosses midnight)."""
            if start <= end:
                return start <= m < end
            return m >= start or m < end

        # ── Phase 1: ORB capture (09:30-09:32 NY) ──
        if _in_window(jst_min, orb_start_min, orb_end_min):
            hi = result.xau_high
            lo = result.xau_low
            if hi > 0 and lo > 0:
                if self._au2_orb_high is None or hi > self._au2_orb_high:
                    self._au2_orb_high = hi
                if self._au2_orb_low is None or lo < self._au2_orb_low:
                    self._au2_orb_low = lo
            return signals

        # ── Phase 2: outside breakout window → idle ──
        if not _in_window(jst_min, break_start_min, break_end_min):
            return signals

        # ── Phase 3: breakout checks ──
        if self._au2_orb_high is None or self._au2_orb_low is None:
            return signals
        if self._au2_fired_today:
            return signals  # one trade per session day

        rng_price = self._au2_orb_high - self._au2_orb_low
        rng_pips = rng_price / 0.01
        if rng_pips < 5.0:
            return signals  # too tight for meaningful R:R

        if result.xau_spread_points and result.xau_spread_points > 30.0:
            return signals

        price = result.xau_price
        if price <= 0:
            return signals

        sl_pips = rng_pips
        tp_pips = rng_pips * 2.0
        usd_score = result.composite_scores.get("USD", 0.0) or 0.0

        meta = {
            "orb_high": self._au2_orb_high,
            "orb_low": self._au2_orb_low,
            "orb_range_pips": round(rng_pips, 2),
            "usd_strength_at_entry": round(usd_score, 2),
            "spread_points_at_entry": round(result.xau_spread_points or 0.0, 2),
            "jst_minute_at_entry": jst_min,
            "ny_orb_start_jst": orb_start_min,
            "ny_orb_end_jst": orb_end_min,
        }

        # BUY breakout
        if price > self._au2_orb_high and AuGoldSystemEngine.usd_long_ok(result):
            self._au2_fired_today = True
            signals.append(GoldSignal(
                strategy_id="au2_ny_orb",
                direction="BUY",
                entry_price=price,
                sl_pips=sl_pips,
                tp_pips=tp_pips,
                entry_reason=(
                    f"NY ORB breakout BUY above {self._au2_orb_high:.2f} "
                    f"(ORB={rng_pips:.1f}p, USD={usd_score:.1f})"
                ),
                metadata=dict(meta, break_price=self._au2_orb_high),
            ))
            return signals

        # SELL breakout
        if price < self._au2_orb_low and AuGoldSystemEngine.usd_short_ok(result):
            self._au2_fired_today = True
            signals.append(GoldSignal(
                strategy_id="au2_ny_orb",
                direction="SELL",
                entry_price=price,
                sl_pips=sl_pips,
                tp_pips=tp_pips,
                entry_reason=(
                    f"NY ORB breakout SELL below {self._au2_orb_low:.2f} "
                    f"(ORB={rng_pips:.1f}p, USD={usd_score:.1f})"
                ),
                metadata=dict(meta, break_price=self._au2_orb_low),
            ))

        return signals

    def _run_au3(self, result) -> list[GoldSignal]:
        """AU3 — H1 Trend Pullback within H4 trend regime.

        Triggers once per fully-closed H1 bar. Uses CLOSED bars only —
        i.e. `xau_candles["H1"][-2]` (the bar at [-1] is the still-forming
        one returned by mt5.copy_rates_from_pos(start_pos=0)).

        Workflow on each new H1 close:
          1. Compute H4 regime from 20-EMA on H4 closed bars
             - BULL: close > EMA AND EMA[-1] > EMA[-6] (slope up)
             - BEAR: close < EMA AND EMA[-1] < EMA[-6] (slope down)
             - else NEUTRAL → no trade
          2. Compute H1 EMA(20) on H1 closed bars
          3. Pullback state machine:
             BULL example —
             - H1 close < EMA → "pullback bar"; track lowest low as extreme
             - H1 close > EMA AND >=2 prior pullback bars → ENTRY
             - >8 pullback bars without resumption → abandon
             BEAR is mirrored
          4. Regime change resets pullback state.

        Risk:
          * SL = pullback_extreme ± 5-pip buffer (BUY: low − 5p; SELL: high + 5p)
          * TP = 2× SL distance from entry → 2:1 R:R

        Filters:
          * Pullback depth must be ≥ 30 pips
          * USD score: ≤3 for BUY, ≥7 for SELL
          * Spread > 30 pips → skip
        """
        signals: list[GoldSignal] = []

        h1_bars = result.xau_candles.get("H1")
        h4_bars = result.xau_candles.get("H4")
        if h1_bars is None or h4_bars is None:
            return signals
        # Need 26 closed bars (= 27 total with forming) so that ema[-6] is
        # PAST the SMA seed (which fills indices 0..19 for period=20). Without
        # this, slope_up/down compares a real EMA value to the flat seed →
        # spurious BULL/BEAR detection during the engine's first 5 H1 closes.
        if len(h1_bars) < 27 or len(h4_bars) < 27:
            return signals

        # Most recently CLOSED bars
        last_h1 = h1_bars[-2]
        last_h4 = h4_bars[-2]
        h1_key = int(last_h1["time"])

        # One trigger per new closed H1 bar
        if h1_key == self._au3_last_h1_key:
            return signals
        self._au3_last_h1_key = h1_key

        # ── H4 regime via 20-EMA + slope ──
        h4_closed = h4_bars[:-1]   # exclude forming
        h4_arr = np.array([float(b["close"]) for b in h4_closed[-50:]], dtype=np.float64)
        h4_ema = self._ema_iter(h4_arr, 20)
        if h4_ema is None or len(h4_ema) < 6:
            return signals

        h4_close = float(last_h4["close"])
        ema_now = float(h4_ema[-1])
        ema_5_ago = float(h4_ema[-6])
        slope_up = ema_now > ema_5_ago
        slope_down = ema_now < ema_5_ago

        if h4_close > ema_now and slope_up:
            regime = "BULL"
        elif h4_close < ema_now and slope_down:
            regime = "BEAR"
        else:
            regime = "NEUTRAL"

        # Regime change → reset pullback state
        if regime != self._au3_regime:
            logger.info("[AU3] H4 regime change: %s -> %s (h4_close=%.2f, ema=%.2f)",
                        self._au3_regime, regime, h4_close, ema_now)
            self._au3_regime = regime
            self._au3_pullback_bars = 0
            self._au3_pullback_extreme = None

        if regime == "NEUTRAL":
            return signals

        # ── H1 EMA(20) on closed bars ──
        h1_closed = h1_bars[:-1]
        h1_arr = np.array([float(b["close"]) for b in h1_closed[-50:]], dtype=np.float64)
        h1_ema = self._ema_iter(h1_arr, 20)
        if h1_ema is None or len(h1_ema) < 1:
            return signals

        h1_close = float(last_h1["close"])
        h1_low = float(last_h1["low"])
        h1_high = float(last_h1["high"])
        h1_ema_now = float(h1_ema[-1])

        # NOTE: spread guard is applied inside the entry-firing branches
        # below, NOT at the top of this section. We MUST keep tracking
        # pullback bars even during high-spread periods (e.g. NFP), or the
        # state machine corrupts: a pullback bar during news would silently
        # not be counted, and the next normal-spread bar would look like a
        # fresh pullback start instead of bar N+1 of an existing pullback.

        spread_ok = (
            not result.xau_spread_points
            or result.xau_spread_points <= 30.0
        )
        usd_score = result.composite_scores.get("USD", 0.0) or 0.0

        if regime == "BULL":
            if h1_close < h1_ema_now:
                # Pullback bar — track lowest low as extreme
                self._au3_pullback_bars += 1
                if self._au3_pullback_extreme is None or h1_low < self._au3_pullback_extreme:
                    self._au3_pullback_extreme = h1_low
                # Abandon if pullback drags too long
                if self._au3_pullback_bars > 8:
                    self._au3_pullback_bars = 0
                    self._au3_pullback_extreme = None
            else:
                # Close back above EMA — possible resumption
                if (self._au3_pullback_bars >= 2
                        and self._au3_pullback_extreme is not None):
                    pullback_depth_pips = (h1_close - self._au3_pullback_extreme) / 0.01
                    if (pullback_depth_pips >= 30.0
                            and spread_ok
                            and AuGoldSystemEngine.usd_long_ok(result)):
                        sl_price = self._au3_pullback_extreme - 0.05  # 5-pip buffer
                        sl_distance = h1_close - sl_price
                        sl_pips = sl_distance / 0.01
                        tp_pips = sl_pips * 2.0

                        signals.append(GoldSignal(
                            strategy_id="au3_trend_pullback",
                            direction="BUY",
                            entry_price=h1_close,
                            sl_pips=sl_pips,
                            tp_pips=tp_pips,
                            entry_reason=(
                                f"BULL pullback BUY ({self._au3_pullback_bars} pb-bars, "
                                f"depth={pullback_depth_pips:.0f}p, USD={usd_score:.1f})"
                            ),
                            metadata={
                                "h4_regime": "BULL",
                                "pullback_bars": self._au3_pullback_bars,
                                "pullback_low": self._au3_pullback_extreme,
                                "pullback_depth_pips": round(pullback_depth_pips, 2),
                                "h1_ema20": round(h1_ema_now, 2),
                                "h4_ema20": round(ema_now, 2),
                                "h4_ema_slope": round(ema_now - ema_5_ago, 4),
                                "usd_strength_at_entry": round(usd_score, 2),
                                "spread_points_at_entry": round(result.xau_spread_points or 0.0, 2),
                            },
                        ))
                # Reset state — one trade per pullback (or aborted pullback)
                self._au3_pullback_bars = 0
                self._au3_pullback_extreme = None

        elif regime == "BEAR":
            if h1_close > h1_ema_now:
                # Pullback bar — track highest high as extreme
                self._au3_pullback_bars += 1
                if self._au3_pullback_extreme is None or h1_high > self._au3_pullback_extreme:
                    self._au3_pullback_extreme = h1_high
                if self._au3_pullback_bars > 8:
                    self._au3_pullback_bars = 0
                    self._au3_pullback_extreme = None
            else:
                if (self._au3_pullback_bars >= 2
                        and self._au3_pullback_extreme is not None):
                    pullback_depth_pips = (self._au3_pullback_extreme - h1_close) / 0.01
                    if (pullback_depth_pips >= 30.0
                            and spread_ok
                            and AuGoldSystemEngine.usd_short_ok(result)):
                        sl_price = self._au3_pullback_extreme + 0.05
                        sl_distance = sl_price - h1_close
                        sl_pips = sl_distance / 0.01
                        tp_pips = sl_pips * 2.0

                        signals.append(GoldSignal(
                            strategy_id="au3_trend_pullback",
                            direction="SELL",
                            entry_price=h1_close,
                            sl_pips=sl_pips,
                            tp_pips=tp_pips,
                            entry_reason=(
                                f"BEAR pullback SELL ({self._au3_pullback_bars} pb-bars, "
                                f"depth={pullback_depth_pips:.0f}p, USD={usd_score:.1f})"
                            ),
                            metadata={
                                "h4_regime": "BEAR",
                                "pullback_bars": self._au3_pullback_bars,
                                "pullback_high": self._au3_pullback_extreme,
                                "pullback_depth_pips": round(pullback_depth_pips, 2),
                                "h1_ema20": round(h1_ema_now, 2),
                                "h4_ema20": round(ema_now, 2),
                                "h4_ema_slope": round(ema_now - ema_5_ago, 4),
                                "usd_strength_at_entry": round(usd_score, 2),
                                "spread_points_at_entry": round(result.xau_spread_points or 0.0, 2),
                            },
                        ))
                self._au3_pullback_bars = 0
                self._au3_pullback_extreme = None

        return signals

    def _run_au4(self, result) -> list[GoldSignal]:
        """AU4 — USD Strength Divergence (mean-reversion).

        XAU and USD are normally INVERSELY correlated (USD up → gold down).
        When the rolling 30-bar Pearson correlation weakens above -0.20
        AND both XAU and USD have moved meaningfully in the SAME direction
        recently, the divergence usually mean-reverts — gold tends to snap
        back toward the direction USD strength implies.

        Workflow on each new closed H1 bar:
          1. Append H1 close + composite_scores['USD'] to 30-deep rolling deques
          2. Once 30 samples available, compute Pearson r and z-scores
          3. If r >= -0.20 (correlation no longer reliably inverse — could
             be near-zero OR strongly positive, both = "broken") AND z_xau
             and z_usd same sign with |z| > 0.5:
             * BOTH up   (z_xau > 0.5, z_usd > 0.5) → SELL gold
             * BOTH down (z_xau < -0.5, z_usd < -0.5) → BUY gold
          4. 6-hour cooldown after entry (real epoch seconds)

        USD filter:
          * SELL gold needs USD ≥ 7.0 (already strong → confirms mean-rev DOWN)
          * BUY gold needs USD ≤ 3.0 (already weak → confirms mean-rev UP)

        Risk (uses recent volatility, not fixed pips):
          * SL = 1.5 × H1 ATR(14)
          * TP = 1.5 × H1 ATR(14) → 1:1 R:R (mean-reversion convention)

        Guards: spread > 30 pips → skip; ATR < 0.10 ($/oz) → skip (no edge).
        """
        signals: list[GoldSignal] = []

        h1_bars = result.xau_candles.get("H1")
        if h1_bars is None or len(h1_bars) < 16:
            return signals

        # USD score must exist + be a real value (default 0 means engine
        # not yet warmed up — skip rather than seeding bad samples).
        usd_score = result.composite_scores.get("USD")
        if usd_score is None or usd_score <= 0:
            return signals

        # Most recently closed H1 bar — one sample per new bar
        last_h1 = h1_bars[-2]
        h1_key = int(last_h1["time"])
        if h1_key == self._au4_last_h1_key:
            return signals
        self._au4_last_h1_key = h1_key

        self._au4_xau_h1_closes.append(float(last_h1["close"]))
        self._au4_usd_h1_scores.append(float(usd_score))

        if len(self._au4_xau_h1_closes) < 30:
            return signals

        # Cooldown (real epoch seconds — independent of broker time skew)
        if self._au4_cooldown_until and time.time() < self._au4_cooldown_until:
            return signals

        # ── Pearson correlation over last 30 bars ──
        xau_arr = np.asarray(list(self._au4_xau_h1_closes)[-30:], dtype=np.float64)
        usd_arr = np.asarray(list(self._au4_usd_h1_scores)[-30:], dtype=np.float64)

        sx = float(xau_arr.std())
        sy = float(usd_arr.std())
        # Realistic noise floors — guard against degenerate near-flat data
        # that would cause z-scores to explode (e.g. std = 1e-8 → z = 10^7+).
        # XAU std < $0.05 (5 pips) over 30 H1 bars = essentially flat → no edge.
        # USD std < 0.05 on the 0-10 composite scale = engine warmup or stuck.
        if sx < 0.05 or sy < 0.05:
            return signals

        r = float(np.corrcoef(xau_arr, usd_arr)[0, 1])

        # Only act when correlation is broken (not negative enough)
        if r >= -0.20:
            mx = float(xau_arr.mean())
            my = float(usd_arr.mean())
            z_xau = (float(xau_arr[-1]) - mx) / sx
            z_usd = (float(usd_arr[-1]) - my) / sy

            if result.xau_spread_points and result.xau_spread_points > 30.0:
                return signals

            # H1 ATR(14) on closed bars
            h1_closed = list(h1_bars[:-1])
            atr = self._h1_atr(h1_closed[-15:], 14)
            if atr is None or atr < 0.10:  # < $0.10/oz = no edge
                return signals

            sl_pips = 1.5 * atr / 0.01
            tp_pips = 1.5 * atr / 0.01
            price = result.xau_price
            if price <= 0:
                return signals

            base_meta = {
                "pearson_r_30bar": round(r, 4),
                "z_xau": round(z_xau, 3),
                "z_usd": round(z_usd, 3),
                "h1_atr14_price": round(atr, 4),
                "h1_atr14_pips": round(atr / 0.01, 2),
                "usd_strength_at_entry": round(usd_score, 2),
                "spread_points_at_entry": round(result.xau_spread_points or 0.0, 2),
                "sample_count": len(self._au4_xau_h1_closes),
            }

            logger.info(
                "[AU4] Correlation break detected: r=%+.2f z_xau=%+.2f z_usd=%+.2f USD=%.1f",
                r, z_xau, z_usd, usd_score,
            )

            # BOTH UP → SELL gold (expect USD strength to drag price down)
            if (z_xau > 0.5 and z_usd > 0.5
                    and AuGoldSystemEngine.usd_short_ok(result)):
                self._au4_cooldown_until = time.time() + 6 * 3600
                signals.append(GoldSignal(
                    strategy_id="au4_usd_divergence",
                    direction="SELL",
                    entry_price=price,
                    sl_pips=sl_pips,
                    tp_pips=tp_pips,
                    entry_reason=(
                        f"USD-divergence SELL (corr={r:+.2f}, "
                        f"z_xau={z_xau:+.2f}, z_usd={z_usd:+.2f}, USD={usd_score:.1f})"
                    ),
                    correlation_xau_usd=r,
                    metadata=dict(base_meta, divergence_type="both_up"),
                ))

            # BOTH DOWN → BUY gold (expect USD weakness to lift price)
            elif (z_xau < -0.5 and z_usd < -0.5
                    and AuGoldSystemEngine.usd_long_ok(result)):
                self._au4_cooldown_until = time.time() + 6 * 3600
                signals.append(GoldSignal(
                    strategy_id="au4_usd_divergence",
                    direction="BUY",
                    entry_price=price,
                    sl_pips=sl_pips,
                    tp_pips=tp_pips,
                    entry_reason=(
                        f"USD-divergence BUY (corr={r:+.2f}, "
                        f"z_xau={z_xau:+.2f}, z_usd={z_usd:+.2f}, USD={usd_score:.1f})"
                    ),
                    correlation_xau_usd=r,
                    metadata=dict(base_meta, divergence_type="both_down"),
                ))

        return signals

    def _run_au5(self, result) -> list[GoldSignal]:
        """AU5 — Asian Session Mean Reversion (M15 RSI extremes).

        During the Asian session (08:00-15:00 JST) XAUUSD volatility is
        typically lowest and the price tends to mean-revert inside a range.
        This strategy takes countertrend entries when M15 RSI(14) reaches
        classical oversold/overbought extremes.

        Workflow on each new closed M15 bar (dedup via _au5_last_m15_key):
          1. Gate: current JST must be 08:00-15:00
          2. Append close to rolling 30-deep deque
          3. Once 15 samples available, compute RSI(14) (SMA variant)
          4. Entry (with 2-hour cooldown):
             * RSI < 30 AND USD <= 3.0 → BUY  (bounce)
             * RSI > 70 AND USD >= 7.0 → SELL (pullback)

        Risk (H1 ATR-scaled, symmetric):
          * SL = 1.0 × H1 ATR(14)
          * TP = 1.0 × H1 ATR(14) → 1:1 R:R

        Guards:
          * ATR < $0.10/oz → skip (no edge)
          * Spread > 30 pips → skip
        """
        signals: list[GoldSignal] = []

        m15_bars = result.xau_candles.get("M15")
        h1_bars = result.xau_candles.get("H1")
        if m15_bars is None or h1_bars is None:
            return signals
        if len(m15_bars) < 17 or len(h1_bars) < 16:
            return signals  # need 15 closed M15 + 15 closed H1

        # Session window gate — Asian session only
        now_jst = datetime.now(_JST)
        jst_min = now_jst.hour * 60 + now_jst.minute
        ASIAN_START = 8 * 60   # 08:00
        ASIAN_END = 15 * 60    # 15:00
        if not (ASIAN_START <= jst_min < ASIAN_END):
            return signals

        # Latest CLOSED M15 bar — one sample per new bar
        # IMPORTANT: sample BEFORE the cooldown check so the rolling deque
        # stays fresh during the 2-hour suppression. Otherwise we'd skip
        # ~8 M15 samples per cooldown and the RSI on resume would be
        # computed on stale data.
        last_m15 = m15_bars[-2]
        m15_key = int(last_m15["time"])
        if m15_key == self._au5_last_m15_key:
            return signals
        self._au5_last_m15_key = m15_key

        self._au5_m15_closes.append(float(last_m15["close"]))

        # Need at least 15 closes for RSI(14)
        if len(self._au5_m15_closes) < 15:
            return signals

        rsi = self._rsi_sma(list(self._au5_m15_closes), 14)
        if rsi is None:
            return signals
        self._au5_last_rsi = rsi

        # Cooldown (real epoch seconds) — checked AFTER sampling so the
        # deque keeps accumulating during the suppression window.
        if self._au5_cooldown_until and time.time() < self._au5_cooldown_until:
            return signals

        # Extreme zones only
        if 30.0 <= rsi <= 70.0:
            return signals

        # Spread guard
        if result.xau_spread_points and result.xau_spread_points > 30.0:
            return signals

        # H1 ATR for risk sizing
        h1_closed = list(h1_bars[:-1])
        atr = self._h1_atr(h1_closed[-15:], 14)
        if atr is None or atr < 0.10:
            return signals

        sl_pips = 1.0 * atr / 0.01
        tp_pips = 1.0 * atr / 0.01

        price = result.xau_price
        if price <= 0:
            return signals

        usd_score = result.composite_scores.get("USD", 0.0) or 0.0

        base_meta = {
            "rsi_m15_14": round(rsi, 2),
            "h1_atr14_price": round(atr, 4),
            "h1_atr14_pips": round(atr / 0.01, 2),
            "usd_strength_at_entry": round(usd_score, 2),
            "spread_points_at_entry": round(result.xau_spread_points or 0.0, 2),
            "jst_minute_at_entry": jst_min,
            "m15_sample_count": len(self._au5_m15_closes),
        }

        # Oversold → BUY (mean-revert up)
        if rsi < 30.0 and AuGoldSystemEngine.usd_long_ok(result):
            self._au5_cooldown_until = time.time() + 2 * 3600
            signals.append(GoldSignal(
                strategy_id="au5_asian_mean_rev",
                direction="BUY",
                entry_price=price,
                sl_pips=sl_pips,
                tp_pips=tp_pips,
                entry_reason=(
                    f"Asian mean-rev BUY (M15 RSI={rsi:.1f} oversold, "
                    f"USD={usd_score:.1f})"
                ),
                rsi_at_entry=rsi,
                metadata=dict(base_meta, rsi_signal="oversold"),
            ))
            return signals

        # Overbought → SELL (mean-revert down)
        if rsi > 70.0 and AuGoldSystemEngine.usd_short_ok(result):
            self._au5_cooldown_until = time.time() + 2 * 3600
            signals.append(GoldSignal(
                strategy_id="au5_asian_mean_rev",
                direction="SELL",
                entry_price=price,
                sl_pips=sl_pips,
                tp_pips=tp_pips,
                entry_reason=(
                    f"Asian mean-rev SELL (M15 RSI={rsi:.1f} overbought, "
                    f"USD={usd_score:.1f})"
                ),
                rsi_at_entry=rsi,
                metadata=dict(base_meta, rsi_signal="overbought"),
            ))

        return signals

    # ══════════════════════════════════════════════════════════════════
    # Helpers (shared between strategies)
    # ══════════════════════════════════════════════════════════════════

    @staticmethod
    def usd_long_ok(result) -> bool:
        """LONG gold filter: USD weak (composite_scores['USD'] <= 3.0)."""
        usd = result.composite_scores.get("USD")
        return usd is not None and usd <= USD_LONG_MAX

    @staticmethod
    def usd_short_ok(result) -> bool:
        """SHORT gold filter: USD strong (composite_scores['USD'] >= 7.0)."""
        usd = result.composite_scores.get("USD")
        return usd is not None and usd >= USD_SHORT_MIN

    @staticmethod
    def now_jst_minutes() -> int:
        """Current wall-clock JST minute-of-day (0-1439)."""
        now = datetime.now(_JST)
        return now.hour * 60 + now.minute

    @staticmethod
    def _ema_iter(arr: np.ndarray, period: int):
        """Iterative EMA over a 1D numpy array.

        Seeds with the SMA of the first `period` values, then applies the
        standard alpha = 2/(period+1) recursion. Returns an array the same
        length as the input (first `period`-1 entries equal to the seed).
        Returns None if input shorter than `period`.

        Same convention used elsewhere in the codebase (alt_systems._update_sqz),
        so AU3/AU4/AU5 give identical numbers to a manual cross-check.
        """
        n = len(arr)
        if n < period:
            return None
        alpha = 2.0 / (period + 1)
        ema = np.empty(n, dtype=np.float64)
        seed = float(arr[:period].mean())
        ema[:period] = seed
        for i in range(period, n):
            ema[i] = alpha * float(arr[i]) + (1.0 - alpha) * ema[i - 1]
        return ema

    @staticmethod
    def _rsi_sma(closes, period: int = 14):
        """Cutler's RSI — SMA of gains/losses over `period` bars.

        Uses the simple-moving-average variant (not Wilder's smoothing) for
        predictability and easier manual verification. For a series flat
        then rising, the classic RSI → 100 and when dropping it → 0.

        Returns None if fewer than `period + 1` closes. Returns 100.0 when
        avg_loss == 0 (all gains) to avoid div-by-zero.
        """
        if closes is None or len(closes) < period + 1:
            return None
        diffs = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains = [d if d > 0 else 0.0 for d in diffs]
        losses = [-d if d < 0 else 0.0 for d in diffs]
        if len(gains) < period:
            return None
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        if avg_loss == 0.0:
            return 100.0 if avg_gain > 0 else 50.0
        rs = avg_gain / avg_loss
        return float(100.0 - (100.0 / (1.0 + rs)))

    @staticmethod
    def _h1_atr(bars, period: int = 14):
        """Compute ATR(period) on a list of bars (dict-shaped MT5 records).

        Uses Wilder's True Range definition:
          TR = max(high-low, |high-prev_close|, |low-prev_close|)
        and a simple SMA over the last `period` TR values.

        Returns price-units ATR (e.g. 1.5 means $1.50/oz). Caller divides by
        the gold pip size (0.01) to convert to pips. Returns None if the
        input has fewer than period+1 bars (need 1 prior close for TR[0]).
        """
        if bars is None or len(bars) < period + 1:
            return None
        trs = []
        for i in range(1, len(bars)):
            h = float(bars[i]["high"])
            lo = float(bars[i]["low"])
            pc = float(bars[i - 1]["close"])
            trs.append(max(h - lo, abs(h - pc), abs(lo - pc)))
        if len(trs) < period:
            return None
        return float(sum(trs[-period:]) / period)
