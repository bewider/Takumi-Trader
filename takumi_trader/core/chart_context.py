"""Chart-context feature computation for trade journaling.

At trade-entry time, fetches multi-timeframe OHLC bars from MT5 and
computes structural features (H1/H4 trend, ATR ratios, D1 range
consumption, swing proximity, M15 micro-structure) so they're captured
alongside every paper trade. These features are USED ONLY FOR ANALYSIS
— they do not gate any trade. Going forward they power weekly reviews
to detect new edges (e.g. "losers happen during narrow-range days").

Performance:
    One call costs ~4 MT5 `copy_rates_from()` queries (~30-40ms total).
    A per-minute module-level cache prevents duplicate queries when
    multiple systems fire on the same pair within the same minute.

Error handling:
    Returns None on any MT5 failure (disconnected, symbol missing, etc.).
    Caller should stamp default zero values on the record in that case —
    never crash an entry path just because chart context failed.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ChartContext:
    """Multi-timeframe structural features at trade-entry time."""

    # H1 features
    h1_trend_slope_pips_per_bar: float = 0.0  # Signed: + = uptrend
    h1_atr_pips: float = 0.0
    h1_atr_ratio: float = 0.0                  # current / prior 14-bar ATR
    h1_dist_to_swing_high_pips: float = 0.0
    h1_dist_to_swing_low_pips: float = 0.0
    h1_trend_aligned: bool = False             # slope sign matches direction

    # H4 features
    h4_trend_slope_pips_per_bar: float = 0.0
    h4_atr_pips: float = 0.0
    h4_trend_aligned: bool = False

    # D1 features
    d1_range_consumed_pct: float = 0.0         # today_range / yesterday_range × 100
    d1_yesterday_range_pips: float = 0.0
    d1_dist_to_today_open_pips: float = 0.0    # signed

    # M15 micro-structure
    m15_range_expansion_ratio: float = 0.0     # last 4 bars range / prior 4
    m15_last_bar_body_ratio: float = 0.0       # body / full range
    m15_last_bar_aligned: bool = False         # last bar direction matches trade

    # Derived flags
    entering_into_resistance: bool = False     # BUY within 5p of H1 swing high
    entering_into_support: bool = False        # SELL within 5p of H1 swing low


def _pip_value(pair: str) -> float:
    return 0.01 if "JPY" in pair else 0.0001


def _wilder_atr(bars, n: int = 14) -> float:
    """Classic Wilder-style ATR (simple average of true ranges)."""
    if not bars or len(bars) < n + 1:
        return 0.0
    trs = []
    for i in range(1, len(bars)):
        h = bars[i]["high"]; l = bars[i]["low"]; pc = bars[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-n:]) / n if len(trs) >= n else 0.0


def _fetch_bars(mt5, pair: str, entry_ts: float, tf_const, count: int):
    """Fetch `count` bars ending at entry_ts (UTC). Returns list of dicts or None."""
    try:
        dt_utc = datetime.fromtimestamp(entry_ts, tz=timezone.utc)
        rates = mt5.copy_rates_from(pair, tf_const, dt_utc, count)
    except Exception as exc:
        logger.debug("[chart_ctx] copy_rates_from failed for %s: %s", pair, exc)
        return None
    if rates is None or len(rates) == 0:
        return None
    return [
        {"time": int(r["time"]),
         "open": float(r["open"]), "high": float(r["high"]),
         "low":  float(r["low"]),  "close": float(r["close"])}
        for r in rates
    ]


def _compute_from_bars(pair: str, direction: str, h1, h4, d1, m15) -> ChartContext:
    """Compute all features from the fetched bar sets."""
    ctx = ChartContext()
    pip = _pip_value(pair)

    # ── H1 ──
    if h1 and len(h1) >= 30:
        entry_price = h1[-1]["close"]
        # Trend slope over last 20 closes
        slope = (h1[-1]["close"] - h1[-20]["close"]) / pip / 20
        ctx.h1_trend_slope_pips_per_bar = round(slope, 3)
        # ATR ratios
        atr_now = _wilder_atr(h1[-15:], 14)
        atr_prior = _wilder_atr(h1[-30:-15], 14)
        ctx.h1_atr_pips = round(atr_now / pip, 2)
        ctx.h1_atr_ratio = round((atr_now / atr_prior) if atr_prior > 0 else 1.0, 3)
        # Swing high/low in last 24 bars
        recent = h1[-24:] if len(h1) >= 24 else h1
        swing_h = max(b["high"] for b in recent)
        swing_l = min(b["low"] for b in recent)
        ctx.h1_dist_to_swing_high_pips = round((swing_h - entry_price) / pip, 2)
        ctx.h1_dist_to_swing_low_pips  = round((entry_price - swing_l) / pip, 2)
        # Alignment
        if direction == "BUY":
            ctx.h1_trend_aligned = slope > 0
            ctx.entering_into_resistance = ctx.h1_dist_to_swing_high_pips < 5
        elif direction == "SELL":
            ctx.h1_trend_aligned = slope < 0
            ctx.entering_into_support = ctx.h1_dist_to_swing_low_pips < 5

    # ── H4 ──
    if h4 and len(h4) >= 15:
        slope_h4 = (h4[-1]["close"] - h4[-10]["close"]) / pip / 10
        ctx.h4_trend_slope_pips_per_bar = round(slope_h4, 3)
        ctx.h4_atr_pips = round(_wilder_atr(h4[-15:], 14) / pip, 2)
        if direction == "BUY":
            ctx.h4_trend_aligned = slope_h4 > 0
        elif direction == "SELL":
            ctx.h4_trend_aligned = slope_h4 < 0

    # ── D1 ──
    if d1 and len(d1) >= 2:
        today = d1[-1]; yesterday = d1[-2]
        today_range_pips = (today["high"] - today["low"]) / pip
        yest_range_pips = (yesterday["high"] - yesterday["low"]) / pip
        ctx.d1_range_consumed_pct = round(
            (today_range_pips / yest_range_pips * 100) if yest_range_pips > 0 else 0,
            1,
        )
        ctx.d1_yesterday_range_pips = round(yest_range_pips, 2)
        # Distance from today's open (signed: + = above open)
        entry_price = (h1[-1]["close"] if h1 else today["close"])
        ctx.d1_dist_to_today_open_pips = round(
            (entry_price - today["open"]) / pip, 2
        )

    # ── M15 ──
    if m15 and len(m15) >= 8:
        last_8 = m15[-8:]
        late = max(b["high"] for b in last_8[-4:]) - min(b["low"] for b in last_8[-4:])
        early = max(b["high"] for b in last_8[:4]) - min(b["low"] for b in last_8[:4])
        ctx.m15_range_expansion_ratio = round(
            (late / early) if early > 0 else 1.0, 3,
        )
        last = last_8[-1]
        body = abs(last["close"] - last["open"])
        full = last["high"] - last["low"]
        ctx.m15_last_bar_body_ratio = round((body / full) if full > 0 else 0, 3)
        last_up = last["close"] > last["open"]
        ctx.m15_last_bar_aligned = (
            (last_up and direction == "BUY")
            or (not last_up and direction == "SELL")
        )

    return ctx


# ───────────────────────────────────────────────────────────────────────────
# Per-minute cache
# ───────────────────────────────────────────────────────────────────────────
# If two systems fire on the same pair+direction within the same clock
# minute, the features will be essentially identical — so cache to avoid
# redundant MT5 queries. Cache keyed by (pair, direction); flushed when
# the minute rolls over.

_CACHE: dict[tuple[str, str], ChartContext | None] = {}
_CACHE_MINUTE: int | None = None


def _clear_cache_if_new_minute(now_ts: float) -> None:
    global _CACHE_MINUTE
    minute = int(now_ts) // 60
    if _CACHE_MINUTE != minute:
        _CACHE.clear()
        _CACHE_MINUTE = minute


def compute_chart_context(
    pair: str,
    direction: str,
    entry_ts: Optional[float] = None,
) -> Optional[ChartContext]:
    """Return ChartContext for this pair+direction at entry_ts (default=now).

    Returns None on any MT5 failure — caller must handle gracefully.
    Thread-safe for the tick-rate of TAKUMI (single-threaded _on_data).
    """
    if entry_ts is None:
        entry_ts = time.time()

    # Check cache first
    _clear_cache_if_new_minute(entry_ts)
    key = (pair, direction)
    if key in _CACHE:
        return _CACHE[key]

    # Import MT5 lazily so this module is importable without MT5 installed
    try:
        import MetaTrader5 as _mt5
    except Exception as exc:
        logger.debug("[chart_ctx] MT5 import failed: %s", exc)
        _CACHE[key] = None
        return None

    try:
        h1 = _fetch_bars(_mt5, pair, entry_ts, _mt5.TIMEFRAME_H1, 50)
        h4 = _fetch_bars(_mt5, pair, entry_ts, _mt5.TIMEFRAME_H4, 30)
        d1 = _fetch_bars(_mt5, pair, entry_ts, _mt5.TIMEFRAME_D1, 10)
        m15 = _fetch_bars(_mt5, pair, entry_ts, _mt5.TIMEFRAME_M15, 100)
    except Exception as exc:
        logger.debug("[chart_ctx] Bar fetch failed for %s: %s", pair, exc)
        _CACHE[key] = None
        return None

    if not h1 or not h4 or not m15:
        _CACHE[key] = None
        return None

    try:
        ctx = _compute_from_bars(pair, direction, h1, h4, d1 or [], m15)
    except Exception as exc:
        logger.debug("[chart_ctx] Feature compute failed for %s: %s", pair, exc)
        _CACHE[key] = None
        return None

    _CACHE[key] = ctx
    return ctx


def apply_to_record(record, ctx: Optional[ChartContext]) -> None:
    """Stamp every ChartContext field onto a PaperTradeRecord.

    If ctx is None, record fields stay at their default zero/False values.
    Silent on missing attributes so the record schema can evolve
    independently of this module.
    """
    if ctx is None:
        return
    for name, value in asdict(ctx).items():
        attr = "entry_ctx_" + name
        if hasattr(record, attr):
            try:
                setattr(record, attr, value)
            except Exception:
                pass
