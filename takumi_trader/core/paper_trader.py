"""Paper Trader — local trade simulation with SL/TP management.

Automatically opens paper trades on fired FULL alerts, monitors them
against SL/TP price levels using **M1 bar-close confirmation**, and closes on:
  - SL hit (checked at M1 bar close — matches backtest logic exactly)
  - TP hit (checked at M1 bar close)
  - Spread-collapse signal exit (from main_window)

SL/TP is ONLY checked when the M1 bar closes (every 60 seconds), using the
completed bar's high/low. This exactly matches the backtester's logic.

Uses per-pair optimized SL/TP from pair_algo_settings.json.
Journals all completed trades to data/paper_trades.json.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from takumi_trader.core.pair_algo_settings import get_pair_settings
from takumi_trader.core.trade_tracker import TrackedTrade, TradeTracker, pip_value

logger = logging.getLogger(__name__)

_JST_FALLBACK = timezone(timedelta(hours=9))
_JST_CACHE = None


def _jst():
    global _JST_CACHE
    if _JST_CACHE is None:
        try:
            from zoneinfo import ZoneInfo
            _JST_CACHE = ZoneInfo("Asia/Tokyo")
        except Exception:
            _JST_CACHE = _JST_FALLBACK
    return _JST_CACHE


# Default SL/TP if pair has no optimized settings
_DEFAULT_SL_PIPS = 10.0
_DEFAULT_TP_PIPS = 20.0


@dataclass
class PaperTradeRecord:
    """Completed paper trade record for the journal."""

    pair: str = ""
    direction: str = ""
    entry_price: float = 0.0
    entry_time: float = 0.0
    entry_time_str: str = ""
    close_price: float = 0.0
    close_time: float = 0.0
    close_time_str: str = ""
    close_reason: str = ""         # "sl_hit", "tp_hit", "signal_exit", "weekend_close"
    sl_pips: float = 0.0
    tp_pips: float = 0.0
    sl_price: float = 0.0
    tp_price: float = 0.0
    pnl_pips: float = 0.0
    peak_pnl_pips: float = 0.0
    worst_pnl_pips: float = 0.0
    duration_minutes: float = 0.0
    entry_conviction: int = 0
    session: str = ""
    is_win: bool = False
    entry_type: str = "stoch_v2"   # "stoch_v2", "sv2_ss", or "sv2_atr"
    adr_consumed_pct: float = 0.0  # How much of ADR(14) was consumed at entry (0-100+%)
    qm4_alert_type: str = ""       # For System D: "MTF", "MTFC", "CUM", "PAIR/MTF" etc.
    # For DTC-combo trades: which source system signaled this trade
    # ("sv2_ss", "sv2_atr", or "sv2_b_tuned"). Empty for non-DTC trades.
    dtc_source_system: str = ""

    # Entry signal data (captured at trade open for diagnostics)
    entry_m5_base: float = 0.0
    entry_m5_quote: float = 0.0
    entry_m15_base: float = 0.0
    entry_m15_quote: float = 0.0
    entry_h1_base: float = 0.0
    entry_h1_quote: float = 0.0
    entry_h4_base: float = 0.0
    entry_h4_quote: float = 0.0
    # QM4 high-timeframe scores
    entry_d1_base: float = 0.0
    entry_d1_quote: float = 0.0
    entry_w1_base: float = 0.0
    entry_w1_quote: float = 0.0
    entry_mn_base: float = 0.0
    entry_mn_quote: float = 0.0
    entry_alignment_count: int = 0
    entry_div_spread: float = 0.0
    entry_spread_std: float = 0.0
    entry_h1_atr_pips: float = 0.0
    entry_structural: str = ""
    entry_tier: str = ""

    # Deep analytics context
    entry_tick_volume_ratio: float = 0.0
    entry_momentum_buildup_sec: int = 0
    entry_dist_day_high_pips: float = 0.0
    entry_dist_day_low_pips: float = 0.0
    entry_dist_week_high_pips: float = 0.0
    entry_dist_week_low_pips: float = 0.0
    entry_dist_month_high_pips: float = 0.0
    entry_dist_month_low_pips: float = 0.0
    entry_cluster_count: int = 0
    entry_dist_00_pips: float = 0.0
    entry_dist_000_pips: float = 0.0
    entry_session_minutes_in: int = 0
    entry_day_of_week: int = 0
    entry_prev_trade_result: str = ""
    entry_concurrent_trades: int = 0
    entry_m1_body_pct: float = 0.0
    entry_m1_direction: str = ""
    entry_atr_ratio: float = 0.0

    # Conviction breakdown
    entry_conv_trend: int = 0
    entry_conv_velocity: int = 0
    entry_conv_isolation: int = 0
    entry_conv_structural: int = 0

    # Pair-specific SL/TP ATR multipliers
    entry_sl_atr_mult: float = 0.0
    entry_tp_atr_mult: float = 0.0

    # Currency-specific context
    entry_strong_ccy: str = ""
    entry_weak_ccy: str = ""
    entry_strong_rank: int = 0
    entry_weak_rank: int = 0
    entry_strong_top_gap: float = 0.0
    entry_weak_bottom_gap: float = 0.0
    entry_strong_velocity: float = 0.0
    entry_weak_velocity: float = 0.0

    # ATR slope (Sv2+ATR signal)
    entry_m5_tr_slope_ratio: float = 0.0

    # News timing
    entry_minutes_since_news: float = -1.0

    # Alt system signal data
    entry_alt_signal_1: float = 0.0
    entry_alt_signal_2: float = 0.0
    entry_alt_signal_3: str = ""
    entry_alt_signal_4: float = 0.0

    # ── Extended Squeeze-specific context (2026-04-20) ──
    # Populated only for entry_type=="squeeze"; other systems leave at defaults.
    sqz_bb_kc_ratio_min: float = 0.0
    sqz_bb_width_pips_release: float = 0.0
    sqz_bb_width_min_pips: float = 0.0
    sqz_real_age_bars: int = 0
    sqz_dist_to_upper_bb_pips: float = 0.0
    sqz_dist_to_lower_bb_pips: float = 0.0
    sqz_close_pos_in_kc: float = -1.0  # -1 sentinel = not measured; 0..1 = real reading. (Fix BUG #9)
    sqz_atr_ratio_during: float = 0.0
    sqz_touches_count: int = 0
    sqz_concurrent_count: int = 0

    # Broker spread cost (in price units)
    entry_spread_price: float = 0.0

    # ── NEW momentum / trend-start signals (added 2026-04-20) ──
    # Captured ONLY for analysis — never used as a trade gate yet. Powers
    # weekly reviews: e.g. "85% WR when composite_vel_90s >= 0.4".
    # Defaults mean "not measured" (history too shallow, lookup missed, or
    # record imported from before these fields existed).
    entry_m1_consec_aligned: int = 0
    entry_composite_vel_90s: float = 0.0
    entry_m5_higher_highs: bool = False
    entry_m5_higher_lows: bool = False
    entry_vwap_dist_pips: float = 0.0
    entry_adx_h1: float = 0.0
    entry_bb_position_m15: float = 0.5
    entry_bb_width_ratio_m15: float = 0.0
    entry_tick_flow_bias: float = 0.0
    entry_volume_ramp_5m: float = 0.0
    entry_range_compression: float = 0.0
    entry_cross_pair_confirm: int = 0
    entry_session_vol_pct: float = 0.0
    entry_m5_close_strength: float = 0.5

    # Trade journey (tracked during trade, stamped at close)
    time_to_5p_profit_min: float = -1.0
    went_profit_first: bool = False
    near_sl_count: int = 0
    near_tp_count: int = 0
    bars_to_close: int = 0

    # Post-close observation (4h window after trade closes)
    post_close_max_mfe_pips: float = 0.0   # Best the trade could have done after close
    post_close_max_mae_pips: float = 0.0   # Worst excursion after close (confirms exit quality)
    post_close_final_pips: float = 0.0     # P/L at end of 4h window (from entry)
    post_close_minutes: float = 0.0        # How long we observed after close
    post_close_complete: bool = False       # True when 4h observation is done

    # ── AU Gold suite fields (2026-04-24) ──
    # Populated ONLY for entry_type starting with "au"; other systems leave at
    # defaults. Captures gold-specific context for later what-if analysis.
    au_entry_reason: str = ""          # Human-readable: "London breakout +1.4× ATR, USD weak 2.3"
    au_metadata_json: str = ""         # Free-form JSON: Asian range bounds / correlation / RSI / etc.
    au_usd_strength_at_entry: float = 0.0  # composite_scores['USD'] at entry (0-10 scale)
    au_spread_points_at_entry: float = 0.0  # bid-ask spread in pips at entry
    au_asian_range_high: float = 0.0   # AU1: Asian session high (anchor for breakout)
    au_asian_range_low: float = 0.0    # AU1: Asian session low
    au_correlation_xau_usd: float = 0.0  # AU4: rolling Pearson correlation at entry
    au_rsi_at_entry: float = 0.0       # AU5: RSI(14) M15 at entry

    # ── Chart-context features (computed from MT5 bars at entry) ──
    # Captured ONLY for analysis — never used as a trade gate. Powers
    # weekly reviews: e.g. "losers mostly had h1_atr_ratio < 0.9".
    # Default zero/False means "not measured" (MT5 lookup failed, or a
    # trade record imported from before this feature was added).
    entry_ctx_h1_trend_slope_pips_per_bar: float = 0.0
    entry_ctx_h1_atr_pips: float = 0.0
    entry_ctx_h1_atr_ratio: float = 0.0
    entry_ctx_h1_dist_to_swing_high_pips: float = 0.0
    entry_ctx_h1_dist_to_swing_low_pips: float = 0.0
    entry_ctx_h1_trend_aligned: bool = False
    entry_ctx_h4_trend_slope_pips_per_bar: float = 0.0
    entry_ctx_h4_atr_pips: float = 0.0
    entry_ctx_h4_trend_aligned: bool = False
    entry_ctx_d1_range_consumed_pct: float = 0.0
    entry_ctx_d1_yesterday_range_pips: float = 0.0
    entry_ctx_d1_dist_to_today_open_pips: float = 0.0
    entry_ctx_m15_range_expansion_ratio: float = 0.0
    entry_ctx_m15_last_bar_body_ratio: float = 0.0
    entry_ctx_m15_last_bar_aligned: bool = False
    entry_ctx_entering_into_resistance: bool = False
    entry_ctx_entering_into_support: bool = False

    # ─── Tier-1 features (added 2026-04-29 from features library) ───────
    # All prefixed `feat_` to avoid collision with existing TAKUMI fields.
    # Default values are non-informative (0/False/"") so old journal
    # records auto-fill cleanly via the generic load path.
    # Source: takumi_trader/features/feature_engine.py compute_for_entry()
    #
    # Microstructure (5):
    feat_cvd_30m: float = 0.0                  # cumulative volume delta (30 M1)
    feat_cvd_divergent: bool = False           # price-vs-CVD divergence flag
    feat_cvd_price_move_pips: float = 0.0      # net price move during CVD window
    feat_amihud_illiq_60m: float = 0.0         # |return|/volume × 1e6
    feat_kyle_lambda_60m: float = 0.0          # price impact per signed volume
    # Volatility (6):
    feat_m15_atr14_pct_rank: float = 0.0       # current ATR's percentile (200-bar)
    feat_h1_atr14_pct_rank: float = 0.0
    feat_m15_jump_detected: bool = False       # Lee-Mykland jump test
    feat_m15_yang_zhang: float = 0.0           # most efficient OHLC vol estimator
    feat_m15_realized_skew: float = 0.0        # return distribution asymmetry
    feat_m15_realized_kurt: float = 0.0        # fat-tail measure
    # Regimes (7):
    feat_h1_adx: float = 0.0                   # trend strength
    feat_h1_choppiness: float = 0.0            # 0-100, >61 = chop
    feat_h1_hurst: float = 0.5                 # >0.5 trend, <0.5 mean-revert
    feat_h1_kaufman_er: float = 0.0            # net move / total move
    feat_h1_regime: str = ""                   # TREND_UP/TREND_DOWN/RANGE/CHOP/MIXED
    feat_h1_donchian_pos: float = 0.5          # 0=at low, 1=at high (20-bar)
    feat_h1_aroon_osc: float = 0.0             # Aroon Up - Aroon Down
    # Statistics (2):
    feat_h1_acf_lag_1: float = 0.0             # return serial dependence
    feat_h1_half_life_bars: float = 0.0        # mean-reversion half-life
    # CSI metrics (3):
    feat_csi_dispersion: float = 0.0           # max - min of 8 currency scores
    feat_csi_strong_count: int = 0             # n currencies with score ≥ 7
    feat_csi_weak_count: int = 0               # n currencies with score ≤ 3
    # Cross-market (1):
    feat_dxy_synthetic: float = 0.0            # synthetic DXY from existing pairs
    # Levels (3):
    feat_dist_to_50_pips: float = 0.0          # pips from nearest 50-pip round
    feat_dist_to_big_figure_pips: float = 0.0  # pips from nearest 100-pip
    feat_h1_poc: float = 0.0                   # 100-bar volume profile POC price
    # Adversarial (2):
    feat_h1_stop_hunt_score: float = 0.0       # 0-1, recent stop-hunt intensity
    feat_h1_sweep_type: str = ""               # bullish_sweep / bearish_sweep / none
    # FX-specific (2):
    feat_is_month_end: bool = False            # last 2 days of month
    feat_in_london_fix: bool = False           # within 15:55-16:05 UTC
    feat_in_ecb_fix: bool = False              # within 13:10-13:20 UTC
    # ─── Tier-2 features (added 2026-04-30) ────────────────────────────
    # Volatility variants (M15) — beyond ATR/YZ already in Tier 1
    feat_m15_realized_var: float = 0.0       # sum of squared M1 log-rets
    feat_m15_parkinson: float = 0.0          # H-L based
    feat_m15_garman_klass: float = 0.0       # OHLC-based
    feat_m15_rogers_satchell: float = 0.0    # drift-robust OHLC
    feat_m15_bipower: float = 0.0            # jump-robust
    feat_m15_vol_ratio: float = 0.0          # short/long vol
    feat_m15_vol_of_vol: float = 0.0
    feat_m15_bb_upper: float = 0.0
    feat_m15_bb_lower: float = 0.0
    feat_m15_bb_width_pips: float = 0.0
    feat_m15_kc_upper: float = 0.0
    feat_m15_kc_lower: float = 0.0
    feat_m15_bbkc_ratio: float = 1.0         # BB-width / KC-width
    # Volatility (H1)
    feat_h1_realized_var: float = 0.0
    feat_h1_parkinson: float = 0.0
    feat_h1_yang_zhang: float = 0.0
    feat_h1_atr14_pips: float = 0.0
    feat_h1_vol_ratio: float = 0.0

    # Regimes additions (alongside ADX/Choppiness/Hurst/Aroon-osc already in Tier 1)
    feat_h1_plus_di: float = 0.0
    feat_h1_minus_di: float = 0.0
    feat_h1_aroon_up: float = 0.0
    feat_h1_aroon_down: float = 0.0
    feat_h1_vortex_plus: float = 0.0
    feat_h1_vortex_minus: float = 0.0
    feat_h1_kama: float = 0.0
    feat_h1_supertrend_value: float = 0.0
    feat_h1_supertrend_dir: int = 0          # +1 up, -1 down, 0 unknown
    feat_h1_ichimoku_tenkan: float = 0.0
    feat_h1_ichimoku_kijun: float = 0.0
    feat_h1_ichimoku_senkou_a: float = 0.0
    feat_h1_ichimoku_senkou_b: float = 0.0
    feat_h1_ichimoku_above_cloud: bool = False
    feat_h1_ichimoku_in_cloud: bool = False
    feat_h1_ichimoku_below_cloud: bool = False
    feat_h1_lr_slope: float = 0.0            # linear regression slope
    feat_h1_lr_r2: float = 0.0               # regression R²
    feat_h1_dfa: float = 0.5                 # detrended-fluctuation alpha
    feat_h1_trend_persistence: float = 0.0
    feat_h1_mr_zscore: float = 0.0           # mean-reversion z-score

    # Statistics additions
    feat_h1_acf_lag_5: float = 0.0
    feat_h1_acf_lag_15: float = 0.0
    feat_h1_pacf_lag_1: float = 0.0
    feat_h1_pacf_lag_5: float = 0.0
    feat_h1_skew_60: float = 0.0
    feat_h1_kurt_60: float = 0.0
    feat_h1_fft_period_min: float = 0.0
    feat_h1_fft_amplitude_ratio: float = 0.0

    # CSI per-currency deltas (rate of change vs prev cycle)
    feat_dUSD: float = 0.0
    feat_dEUR: float = 0.0
    feat_dGBP: float = 0.0
    feat_dJPY: float = 0.0
    feat_dCAD: float = 0.0
    feat_dAUD: float = 0.0
    feat_dNZD: float = 0.0
    feat_dCHF: float = 0.0

    # Cross-market currency baskets (synthetic)
    feat_eur_index: float = 0.0
    feat_jpy_index: float = 0.0
    feat_gbp_index: float = 0.0
    feat_aud_index: float = 0.0
    feat_carry_pips_per_day: float = 0.0

    # Levels — additional round numbers + prior period OHLC
    feat_dist_to_25_pips: float = 0.0
    feat_dist_to_century_pips: float = 0.0
    feat_prev_day_open: float = 0.0
    feat_prev_day_high: float = 0.0
    feat_prev_day_low: float = 0.0
    feat_prev_day_close: float = 0.0
    feat_prev_week_high: float = 0.0
    feat_prev_week_low: float = 0.0
    feat_prev_month_high: float = 0.0
    feat_prev_month_low: float = 0.0
    feat_year_high: float = 0.0
    feat_year_low: float = 0.0
    # Session ranges (within today, JST)
    feat_asian_session_high: float = 0.0
    feat_asian_session_low: float = 0.0
    feat_asian_session_range_pips: float = 0.0
    feat_london_session_high: float = 0.0
    feat_london_session_low: float = 0.0
    # Pivot points (from prior day OHLC)
    feat_pivot_pp: float = 0.0
    feat_pivot_r1: float = 0.0
    feat_pivot_r2: float = 0.0
    feat_pivot_s1: float = 0.0
    feat_pivot_s2: float = 0.0
    feat_fib_pp: float = 0.0
    feat_fib_r1: float = 0.0
    feat_fib_s1: float = 0.0
    feat_cam_r3: float = 0.0
    feat_cam_s3: float = 0.0
    # VWAP variants
    feat_h1_vwap: float = 0.0
    feat_h1_session_vwap: float = 0.0
    # Volume profile (Value Area)
    feat_h1_vah: float = 0.0
    feat_h1_val: float = 0.0

    # Patterns additions (alongside FVG/order-block counters not yet in Tier 1)
    feat_fvg_count: int = 0
    feat_order_block_count: int = 0
    feat_h1_equal_highs: bool = False
    feat_h1_equal_lows: bool = False
    feat_h1_trendline_break: str = ""
    feat_h1_candle_pattern: str = ""

    # Adversarial additions
    feat_h1_round_magnetism: float = 0.0
    feat_h1_liquidity_void_count: int = 0
    feat_h1_tick_burst_z: float = 0.0

    # FX-specific additions
    feat_in_tokyo_fix: bool = False
    feat_triangular_arb_pips: float = 0.0
    feat_dst_active_uk: bool = False
    feat_dst_active_us: bool = False
    feat_holiday_label: str = ""
    feat_session_label: str = ""

    # Behavioral additions
    feat_friday_late: bool = False
    feat_sunday_open: bool = False
    feat_lunch_hour: str = ""
    feat_days_into_quarter: int = 0

    # ─── Tier-3 NETWORK features (added 2026-04-30) ──────────────────────
    # Refreshed every 30 min via FeatureEngine network cache.
    # Free sources: Yahoo Finance + FRED + ForexFactory + CFTC + RSS feeds.
    # All values default 0/False/"" if network unavailable — graceful.

    # Yahoo Finance (free, no key)
    feat_vix: float = 0.0                    # CBOE VIX
    feat_vvix: float = 0.0
    feat_skew: float = 0.0                   # CBOE SKEW
    feat_move: float = 0.0                   # ICE MOVE Treasury vol
    feat_gold_close: float = 0.0
    feat_wti_close: float = 0.0
    feat_brent_close: float = 0.0
    feat_copper_close: float = 0.0
    feat_natgas_close: float = 0.0
    feat_sp500_close: float = 0.0
    feat_nasdaq_close: float = 0.0
    feat_nikkei_close: float = 0.0
    feat_dax_close: float = 0.0
    feat_ftse_close: float = 0.0
    feat_hang_seng_close: float = 0.0
    feat_btc_close: float = 0.0

    # FRED yields (free, requires API key)
    feat_fred_us_10y: float = 0.0
    feat_fred_us_2y: float = 0.0
    feat_fred_us_3m: float = 0.0
    feat_fred_yield_curve_2_10: float = 0.0  # 10Y - 2Y
    feat_fred_real_10y: float = 0.0          # TIPS
    feat_fred_hy_oas: float = 0.0
    feat_fred_ig_oas: float = 0.0
    feat_fred_ted_spread: float = 0.0

    # Calendar (ForexFactory, free)
    feat_minutes_to_next_high_event: float = -1.0
    feat_news_blackout: bool = False
    feat_events_today_count: int = 0
    feat_next_event_title: str = ""

    # Sentiment (RSS + Reddit, free)
    feat_news_sent_base: float = 0.0
    feat_news_sent_quote: float = 0.0
    feat_news_flow_rate: float = 0.0         # headlines/minute last 60 min
    feat_reddit_forex_sentiment: float = 0.0

    # COT (CFTC, free, weekly)
    feat_cot_base_net: int = 0
    feat_cot_quote_net: int = 0

    # Schema version — bump on each new batch
    feat_schema_version: int = 2


# ── Cached dataclass-field name sets (2026-04-21) ────────────────────────
# Introspected once on first use. Used by the generic trade↔record copy
# path in _close_and_journal so every shared field auto-transfers. Adding
# new fields to either dataclass in future will persist automatically.
_PAPER_TRADER_RECORD_FIELDS: set[str] | None = None
_PAPER_TRADER_TRADE_FIELDS: set[str] | None = None


def _paper_shared_fields_record() -> set[str]:
    global _PAPER_TRADER_RECORD_FIELDS
    if _PAPER_TRADER_RECORD_FIELDS is None:
        _PAPER_TRADER_RECORD_FIELDS = {f.name for f in fields(PaperTradeRecord)}
    return _PAPER_TRADER_RECORD_FIELDS


def _paper_shared_fields_trade() -> set[str]:
    global _PAPER_TRADER_TRADE_FIELDS
    if _PAPER_TRADER_TRADE_FIELDS is None:
        # Import locally to avoid circular import at module load
        from takumi_trader.core.trade_tracker import TrackedTrade
        _PAPER_TRADER_TRADE_FIELDS = {f.name for f in fields(TrackedTrade)}
    return _PAPER_TRADER_TRADE_FIELDS


class PaperTrader:
    """Manages paper trades with SL/TP monitoring."""

    # Post-close observation window (matches backtest post_exit_hours)
    POST_CLOSE_HOURS = 4.0

    def __init__(
        self,
        trade_tracker: TradeTracker,
        journal_path: Path,
    ) -> None:
        self._tracker = trade_tracker
        self._journal_path = journal_path
        self._journal: list[PaperTradeRecord] = []
        # Records still being observed post-close (journal_index -> True)
        # Using index-based tracking so multiple trades per pair are all observed
        self._post_close_watching: dict[int, bool] = {}
        self._last_journal_save: float = 0.0  # throttle saves to every 30s
        # ── M1 bar-close confirmation state ──
        # Track the current M1 bar timestamp so we detect bar changes
        self._last_m1_bar_time: int = 0
        # Accumulate each bar's high/low per pair for the completed bar
        self._bar_highs: dict[str, float] = {}  # pair -> running high within current bar
        self._bar_lows: dict[str, float] = {}   # pair -> running low within current bar

    # ── Open ────────────────────────────────────────────────────────

    def open_paper_trade(
        self,
        pair: str,
        direction: str,
        entry_price: float,
        composite_scores: dict[str, float] | None = None,
        conviction: int = 0,
        session: str = "",
        h1_atr: float = 0.0,
        entry_type: str = "stoch_v2",
        adr_consumed_pct: float = 0.0,
        tp_ratio_override: float | None = None,
        sl_pips_override: float | None = None,
        tp_pips_override: float | None = None,
    ) -> TrackedTrade | None:
        """Open a paper trade using dynamic ATR-based SL/TP.

        Only stoch_v2 entries are allowed. Standard entries are rejected.
        SL/TP is computed as: sl_atr × H1_ATR / pip (same as backtester).
        Falls back to static sl_pips/tp_pips if H1 ATR is unavailable.

        sl_pips_override / tp_pips_override: If BOTH are supplied (and > 0),
        bypass pair-settings lookup and use these values directly. Used by
        alt systems (squeeze/breakout/divergence) whose signal engines
        compute their own SL/TP specific to the strategy (fixes BUG #4 —
        before this override, alt-system SL/TP was silently overridden by
        stoch_v2's per-pair settings).

        Returns the TrackedTrade or None if already tracking this pair.
        """
        # Hard block: only known entry types allowed
        _VALID_ENTRY_TYPES = (
            "stoch_v2", "sv2_ss", "sv2_atr", "sv2_qm4", "sv2_a_tuned", "sv2_b_tuned",
            "breakout", "squeeze", "divergence",
            # Squeeze-REV (2026-04-29): mirrors every Squeeze signal with the
            # opposite direction. User hypothesis: if Squeeze loses with PF<1,
            # the inverse trades collectively profit (minus 2x spread cost).
            "squeeze_rev",
            "dtc_combo",
            # Live-candle engine systems (2026-04-21)
            "sv2_live", "sv2_a_tuned_live", "sv2_ss_live", "sv2_b_tuned_live", "sv2_atr_live",
            # Sv2-upgraded (2026-04-23): live-candle engine + conv≥65 + revenge
            # cooldown + BE-stop at +7p peak. Paper-only parallel to Sv2.
            "sv2_upgraded",
            # AU Gold suite (2026-04-24): 5 XAUUSD strategies. Paper-only,
            # completely isolated from the forex strength engine — they read
            # USD score from composite_scores['USD'] but DO NOT feed gold data
            # back into that engine. See takumi_trader/core/au_gold_systems.py.
            "au1_london_breakout",   # Asian range -> London breakout
            "au2_ny_orb",             # NY open range breakout
            "au3_trend_pullback",     # H1 pullback within H4 trend
            "au4_usd_divergence",     # XAUUSD vs USD-strength divergence
            "au5_asian_mean_rev",     # Asian-session RSI mean reversion
        )
        if entry_type not in _VALID_ENTRY_TYPES:
            logger.warning(
                "BLOCKED non-sv2 trade: %s %s entry_type=%s — investigate caller",
                pair, direction, entry_type,
            )
            import traceback
            traceback.print_stack()
            return None

        if self._tracker.has_trade(pair):
            return None

        pip = pip_value(pair)

        # ── Explicit SL/TP override (alt systems: squeeze/breakout/divergence) ──
        # When set, skips pair-settings lookup entirely — the caller has
        # already computed strategy-specific values from its own ATR scaling.
        if (sl_pips_override is not None and sl_pips_override > 0
                and tp_pips_override is not None and tp_pips_override > 0):
            sl_pips = float(sl_pips_override)
            tp_pips = float(tp_pips_override)
            logger.info(
                "[PAPER] %s using caller-supplied SL/TP override: "
                "SL=%.1fp TP=%.1fp (entry_type=%s)",
                pair, sl_pips, tp_pips, entry_type,
            )
        else:
            # Look up optimized ATR multipliers from pair settings
            settings = get_pair_settings(pair)
            if settings and h1_atr > 0:
                # Dynamic: same formula as backtester
                sl_atr_mult = settings.get("sl_atr", 0.3)
                tp_atr_mult = settings.get("tp_atr", 1.0)
                sl_pips = round(sl_atr_mult * h1_atr / pip, 1)
                tp_pips = round(tp_atr_mult * h1_atr / pip, 1)
                logger.info(
                    "[PAPER] %s ATR-based SL/TP: H1_ATR=%.5f  "
                    "SL=%.1f×ATR=%.1fp  TP=%.1f×ATR=%.1fp",
                    pair, h1_atr, sl_atr_mult, sl_pips, tp_atr_mult, tp_pips,
                )
            elif settings:
                # Fallback to static averages if no ATR available
                sl_pips = settings.get("sl_pips", _DEFAULT_SL_PIPS)
                tp_pips = settings.get("tp_pips", _DEFAULT_TP_PIPS)
                logger.warning(
                    "[PAPER] %s no H1 ATR available, using static SL=%.1fp TP=%.1fp",
                    pair, sl_pips, tp_pips,
                )
            else:
                sl_pips = _DEFAULT_SL_PIPS
                tp_pips = _DEFAULT_TP_PIPS

        # ── Optional TP ratio override (e.g., DTC-combo uses 0.75 × SL) ──
        # When set, replaces the pair-settings tp_atr-derived TP with a flat
        # ratio of the computed SL. Lets a single config value tune R:R for
        # a specific entry path without affecting the underlying systems.
        if tp_ratio_override is not None and tp_ratio_override > 0:
            new_tp = round(sl_pips * tp_ratio_override, 1)
            logger.info(
                "[PAPER] %s TP overridden: %.1fp → %.1fp  "
                "(ratio=%.2f × SL=%.1fp, entry_type=%s)",
                pair, tp_pips, new_tp, tp_ratio_override, sl_pips, entry_type,
            )
            tp_pips = new_tp

        # ── Spread-realistic SL/TP placement ──
        # MT5 OHLC bars are BID prices. The caller passes entry_price as
        # the latest M1 BID close. But real broker execution costs a
        # spread per round-trip:
        #   BUY:  enter at ASK = BID + spread, exit at BID
        #         → TP fires when BID >= entry_BID + spread + tp_pips
        #         → SL fires when BID <= entry_BID + spread - sl_pips
        #   SELL: enter at BID, exit at ASK = BID + spread
        #         → TP fires when BID <= entry_BID - tp_pips - spread
        #         → SL fires when BID >= entry_BID + sl_pips - spread
        # In both cases, both SL and TP fire one spread "earlier" in the
        # adverse direction than the naive BID-based math suggests.
        # We bake the spread into the SL/TP levels so the existing
        # `_check_sl_tp` (which compares against bar high/low BID) matches
        # what a real broker would do.
        # The same spread is subtracted from PnL on close (in
        # `_close_and_journal`) to charge the round-trip cost.
        try:
            import MetaTrader5 as _mt5
            _tick = _mt5.symbol_info_tick(pair)
            _spread_price = max(0.0, _tick.ask - _tick.bid) if _tick else 0.0
        except Exception:
            _spread_price = 0.0

        if direction == "BUY":
            # entry_BID + spread = ASK; SL/TP set relative to ASK, in BID terms
            tp_price = entry_price + _spread_price + (tp_pips * pip)
            sl_price = entry_price + _spread_price - (sl_pips * pip)
        else:  # SELL
            # entry at BID, exit at ASK = bar_low + spread (or bar_high + spread)
            # TP/SL placed in BID terms (lowered by spread to compensate)
            tp_price = entry_price - (tp_pips * pip) - _spread_price
            sl_price = entry_price + (sl_pips * pip) - _spread_price

        # ── Wrong-side SL/TP safety check (added 2026-05-17) ──
        # When `_spread_price > sl_pips * pip` (i.e., broker spread is
        # wider than the intended stop distance), the spread-compensation
        # math above inverts SL/TP to the WRONG side of entry:
        #   * BUY with SL above entry → any upward tick triggers SL at
        #     near-break-even, masking what should have been a real loss
        #   * SELL with SL below entry → symmetric
        # Audit on 2026-05-17 found 21.6% of Sv2-upgraded trades hit this
        # path, hiding ~504p of losses. Universal fix here gates every
        # paper-trader open. When detected, abort the trade entirely —
        # spread conditions are bad enough that entering is unwise even
        # with a correctly-placed stop.
        if direction == "BUY":
            sl_on_wrong_side = sl_price >= entry_price
            tp_on_wrong_side = tp_price <= entry_price
        else:  # SELL
            sl_on_wrong_side = sl_price <= entry_price
            tp_on_wrong_side = tp_price >= entry_price
        if sl_on_wrong_side or tp_on_wrong_side:
            spread_pips = _spread_price / pip if pip > 0 else 0.0
            logger.warning(
                "[PAPER %s] Aborting %s %s entry — spread %.1fp >= "
                "sl_pips %.1fp would invert SL/TP to wrong side of entry "
                "(entry=%.5f sl=%.5f tp=%.5f). Returning None.",
                entry_type or "?", direction, pair, spread_pips,
                sl_pips, entry_price, sl_price, tp_price,
            )
            return None

        trade = self._tracker.open_trade(
            pair=pair,
            direction=direction,
            entry_price=entry_price,
            currency_scores=composite_scores,
            target_pips=tp_pips,
            is_paper=True,
            sl_pips=sl_pips,
            tp_pips=tp_pips,
            sl_price=sl_price,
            tp_price=tp_price,
        )
        if trade:
            trade.entry_conviction = conviction
            trade.session = session
            trade.entry_type = entry_type
            trade.adr_consumed_pct = adr_consumed_pct
            trade.entry_spread_price = _spread_price

            # Create journal record at ENTRY time (not close) so it appears
            # in the table immediately with signal data
            from datetime import datetime
            entry_str = datetime.fromtimestamp(
                trade.entry_time, tz=_jst()
            ).strftime("%Y-%m-%d %H:%M:%S")
            record = PaperTradeRecord(
                pair=pair,
                direction=direction,
                entry_price=entry_price,
                entry_time=trade.entry_time,
                entry_time_str=entry_str,
                sl_pips=sl_pips,
                tp_pips=tp_pips,
                sl_price=sl_price,
                tp_price=tp_price,
                entry_conviction=conviction,
                session=session,
                entry_type=entry_type,
                adr_consumed_pct=adr_consumed_pct,
                # Exit fields left at defaults (0/empty) — filled on close
            )

            # ── Chart-context features (analysis only, not a trade gate) ──
            # Fetch multi-TF OHLC bars from MT5 and compute structural
            # features (H1/H4 trend, ATR ratios, D1 range consumption,
            # swing proximity, M15 micro-structure). Per-minute cache
            # inside chart_context avoids duplicate queries when multiple
            # systems fire on the same pair simultaneously.
            # Failures are silent — the record's defaults (zero/False)
            # remain if MT5 returns nothing. Never crashes entry path.
            try:
                from takumi_trader.core.chart_context import (
                    compute_chart_context, apply_to_record,
                )
                _ctx = compute_chart_context(pair, direction, trade.entry_time)
                apply_to_record(record, _ctx)
            except Exception as _ctx_exc:
                logger.debug("[PAPER] chart_context stamp failed for %s: %s",
                             pair, _ctx_exc)

            # ── Full feature stamping (Tier 1 + 2 + 3, added 2026-04-30) ──
            # Compute ~177 feat_* columns: 140 local (CVD, ADX, Choppiness,
            # Hurst, regime, ATR percentile, jump detection, DXY, fix-windows,
            # full Ichimoku, BB/KC, Vortex, KAMA, SuperTrend, pivot systems,
            # VWAP, volume profile, candle patterns, etc.) plus ~30 network
            # features (VIX, gold, oil, indices, FRED yields, COT, news
            # blackout, sentiment) when enable_network is set.
            # Local: ~50-100ms per trade. Network: 0ms via shared cache.
            # Failures silent — defaults remain if MT5/network unavailable.
            try:
                from takumi_trader.features import FeatureEngine
                import os as _os
                if not hasattr(self, "_feature_engine"):
                    fred_key = _os.environ.get("FRED_API_KEY", "")
                    # Network features enabled by DEFAULT — uses only free,
                    # no-signup sources (Yahoo Finance, ForexFactory, CFTC,
                    # RSS feeds, Reddit, Wikipedia). FRED is opt-in via the
                    # API key — without it, FRED fields default to 0 but
                    # the other ~22 network features still populate.
                    # To disable entirely (offline mode), set:
                    #   TAKUMI_DISABLE_NETWORK_FEATURES=1
                    enable_net = not bool(_os.environ.get("TAKUMI_DISABLE_NETWORK_FEATURES", ""))
                    self._feature_engine = FeatureEngine(
                        fred_api_key=fred_key or None,
                        enable_network=enable_net,
                    )
                # Prior CSI snapshot for d(strength) deltas (feat_dUSD/dEUR/...).
                # Cached on the paper_trader instance — main_window pushes the
                # latest snapshot via _cross_pair_close_cache push loop, but
                # for CSI deltas we keep our own previous snapshot here.
                _prev_csi = getattr(self, "_prev_composite_scores", None)
                feats = self._feature_engine.compute_entry_features(
                    pair=pair,
                    timestamp_utc=int(trade.entry_time),
                    composite_scores=composite_scores,
                    composite_scores_prev=_prev_csi,
                    cross_pair_data=getattr(self, "_cross_pair_close_cache", None),
                )
                for k, v in feats.items():
                    if hasattr(record, k):
                        setattr(record, k, v)
                # Update prior CSI cache AFTER computing deltas, so the next
                # trade sees this trade's CSI as the "previous" snapshot.
                try:
                    if composite_scores:
                        self._prev_composite_scores = dict(composite_scores)
                except Exception:
                    pass
            except Exception as _feat_exc:
                logger.debug("[PAPER] feature engine stamp failed for %s: %s",
                             pair, _feat_exc)

            self._journal.append(record)
            trade._journal_idx = len(self._journal) - 1
            self.save_journal()

            logger.info(
                "[PAPER] Opened %s %s @ %.5f  SL=%.5f (%.1fp)  TP=%.5f (%.1fp)  conv=%d  session=%s",
                direction, pair, entry_price, sl_price, sl_pips, tp_price, tp_pips, conviction, session,
            )
        return trade

    # ── SL/TP Monitoring ────────────────────────────────────────────

    def _check_sl_tp(
        self, trade: TrackedTrade, high: float, low: float
    ) -> str | None:
        """Check if SL or TP was hit using M1 candle high/low.

        Returns "sl_hit", "tp_hit", or None.
        TP is checked first (optimistic fill assumption).
        """
        if trade.sl_price <= 0 or trade.tp_price <= 0:
            return None

        if trade.direction == "BUY":
            # TP hit if high reaches TP level
            if high >= trade.tp_price:
                return "tp_hit"
            # SL hit if low reaches SL level
            if low <= trade.sl_price:
                return "sl_hit"
        else:  # SELL
            # TP hit if low reaches TP level
            if low <= trade.tp_price:
                return "tp_hit"
            # SL hit if high reaches SL level
            if high >= trade.sl_price:
                return "sl_hit"

        return None

    def update_cycle(
        self,
        high_prices: dict[str, float],
        low_prices: dict[str, float],
        close_prices: dict[str, float],
        m1_bar_time: int = 0,
    ) -> list[PaperTradeRecord]:
        """Check all active paper trades for SL/TP hits.

        SL/TP is checked **every cycle** (~1 second) using the running
        high/low accumulated within the current M1 bar.  This ensures
        trades exit immediately when price reaches the TP/SL level
        instead of waiting for the bar to close.

        Called every cycle (~1 second) from main_window.
        Returns list of newly closed paper trade records.
        """
        closed: list[PaperTradeRecord] = []
        active = self._tracker.active_trades
        if not active:
            return closed

        # Detect M1 bar close
        bar_just_closed = False
        if m1_bar_time > 0 and m1_bar_time != self._last_m1_bar_time:
            if self._last_m1_bar_time > 0:
                bar_just_closed = True
            self._last_m1_bar_time = m1_bar_time

        # Update running high/low for the current bar (accumulate intra-bar)
        for pair in list(active.keys()):
            h = high_prices.get(pair)
            l = low_prices.get(pair)
            if h is None or l is None:
                continue
            if pair in self._bar_highs:
                self._bar_highs[pair] = max(self._bar_highs[pair], h)
                self._bar_lows[pair] = min(self._bar_lows[pair], l)
            else:
                self._bar_highs[pair] = h
                self._bar_lows[pair] = l

        # ── SL/TP: checked every cycle using running high/low ──
        # Sv2-upgraded BE-stop opt-out pairs (2026-04-23): these pairs
        # historically hit TP cleanly without intra-trade wobble — BE stops
        # on them truncate real winners. Computed once at module level would
        # be cleaner, but keeping it inline for locality of the rule.
        _SV2_UPGRADED_BE_OPT_OUTS = {"EURUSD", "GBPUSD", "NZDUSD", "GBPCAD"}

        for pair, trade in list(active.items()):
            if not trade.is_paper:
                continue

            bar_h = self._bar_highs.get(pair)
            bar_l = self._bar_lows.get(pair)
            if bar_h is None or bar_l is None:
                continue

            # ── Sv2-upgraded: move SL to break-even at +7p peak ──
            # Triggers once per trade (guarded by trade.be_moved). Opt-out
            # pairs keep their original SL placement because they historically
            # run cleanly to TP without pullback. All other pairs get BE
            # protection once the trade has shown ≥7p favorable excursion.
            if (trade.entry_type == "sv2_upgraded"
                    and not trade.be_moved
                    and pair not in _SV2_UPGRADED_BE_OPT_OUTS):
                _pip_be = pip_value(pair)
                if trade.direction == "BUY":
                    peak_pips = (bar_h - trade.entry_price) / _pip_be
                else:
                    peak_pips = (trade.entry_price - bar_l) / _pip_be
                if peak_pips >= 7.0:
                    # Move SL to BE, adjusted for spread cost so we truly
                    # break even (not -spread). entry_spread_price was
                    # stamped at open time.
                    sp = getattr(trade, "entry_spread_price", 0.0) or 0.0
                    if trade.direction == "BUY":
                        trade.sl_price = trade.entry_price + sp
                    else:
                        trade.sl_price = trade.entry_price - sp
                    trade.be_moved = True
                    logger.info(
                        "[sv2_upgraded] %s %s BE-move: peak=%.1fp  SL %.5f -> %.5f",
                        trade.direction, pair, peak_pips,
                        self._bar_highs.get(pair, 0) if trade.direction == "SELL" else 0,
                        trade.sl_price,
                    )

            # ── Trade journey tracking (for analytics) — wrapped in try so
            # SL/TP always runs even if analytics code has a bug ──
            try:
                _pip = pip_value(pair)
                if trade.direction == "BUY":
                    _cur_best = (bar_h - trade.entry_price) / _pip
                    _dist_to_sl = (bar_l - trade.sl_price) / _pip
                    _dist_to_tp = (trade.tp_price - bar_h) / _pip
                else:
                    _cur_best = (trade.entry_price - bar_l) / _pip
                    _dist_to_sl = (trade.sl_price - bar_h) / _pip
                    _dist_to_tp = (bar_l - trade.tp_price) / _pip
                # Time to +5p profit (only set once)
                if getattr(trade, 'time_to_5p_profit_min', -1.0) < 0 and _cur_best >= 5.0:
                    trade.time_to_5p_profit_min = round((time.time() - trade.entry_time) / 60.0, 1)
                    if trade.worst_pnl_pips > -5.0:
                        trade.went_profit_first = True
                # Near-miss counters (once per bar)
                if bar_just_closed:
                    if 0 < _dist_to_sl < 2.0:
                        trade.near_sl_count = getattr(trade, 'near_sl_count', 0) + 1
                    if 0 < _dist_to_tp < 2.0:
                        trade.near_tp_count = getattr(trade, 'near_tp_count', 0) + 1
                    trade.bars_to_close = getattr(trade, 'bars_to_close', 0) + 1
            except Exception as _jex:
                logger.warning("[PAPER] Journey tracking error on %s: %s", pair, _jex)

            hit = self._check_sl_tp(trade, bar_h, bar_l)
            if hit:
                close_price = trade.tp_price if hit == "tp_hit" else trade.sl_price
                logger.info(
                    "[PAPER] %s %s %s — price %.5f",
                    trade.direction, pair, hit, close_price,
                )
                record = self._close_and_journal(pair, hit, close_price)
                if record:
                    closed.append(record)

        # Reset bar accumulators when a new bar starts
        if bar_just_closed:
            self._bar_highs.clear()
            self._bar_lows.clear()

        return closed

    # ── Post-Close Observation ─────────────────────────────────────

    def post_close_cycle(
        self,
        high_prices: dict[str, float],
        low_prices: dict[str, float],
        close_prices: dict[str, float],
    ) -> None:
        """Track MAX-MFE / MAX-MAE for 4h after each closed paper trade.

        Called every cycle from main_window (same cadence as update_cycle).
        Uses M1 high/low for realistic extremes, matching backtester behavior.
        """
        if not self._post_close_watching:
            return

        now = time.time()
        completed_indices: list[int] = []

        for idx in list(self._post_close_watching.keys()):
            record = self._journal[idx]
            pair = record.pair

            # Check if 4h observation window has elapsed
            elapsed_min = (now - record.close_time) / 60.0
            record.post_close_minutes = round(elapsed_min, 1)

            high = high_prices.get(pair)
            low = low_prices.get(pair)
            close = close_prices.get(pair)
            if high is None or low is None:
                continue

            pip = pip_value(pair)

            # Compute excursion from entry price using M1 high/low
            if record.direction == "BUY":
                best_pnl = (high - record.entry_price) / pip
                worst_pnl = (low - record.entry_price) / pip
            else:  # SELL
                best_pnl = (record.entry_price - low) / pip
                worst_pnl = (record.entry_price - high) / pip

            # Update MAX-MFE (best possible from entry)
            if best_pnl > record.post_close_max_mfe_pips:
                record.post_close_max_mfe_pips = round(best_pnl, 1)

            # Update MAX-MAE (worst excursion from entry)
            if worst_pnl < -record.post_close_max_mae_pips:
                record.post_close_max_mae_pips = round(abs(worst_pnl), 1)

            # Final P/L at current close
            if close is not None:
                if record.direction == "BUY":
                    record.post_close_final_pips = round(
                        (close - record.entry_price) / pip, 1
                    )
                else:
                    record.post_close_final_pips = round(
                        (record.entry_price - close) / pip, 1
                    )

            # Check if observation window is complete
            if elapsed_min >= self.POST_CLOSE_HOURS * 60:
                record.post_close_complete = True
                completed_indices.append(idx)
                logger.info(
                    "[PAPER] Post-close complete %s %s — "
                    "MAX-MFE=%.1fp  MAX-MAE=%.1fp  Final=%.1fp  "
                    "(closed at %.1fp)",
                    record.direction, pair,
                    record.post_close_max_mfe_pips,
                    record.post_close_max_mae_pips,
                    record.post_close_final_pips,
                    record.pnl_pips,
                )

        # Remove completed observations
        for idx in completed_indices:
            del self._post_close_watching[idx]

        # Save journal — immediately on completion, throttled otherwise
        if completed_indices:
            self.save_journal()
            self._last_journal_save = now
        elif self._post_close_watching and (now - self._last_journal_save) >= 30.0:
            self.save_journal()
            self._last_journal_save = now

    @property
    def post_close_count(self) -> int:
        """Number of trades currently being observed post-close."""
        return len(self._post_close_watching)

    # ── Signal Exit ─────────────────────────────────────────────────

    def handle_exit_signal(
        self, pair: str, urgency: str, close_price: float
    ) -> PaperTradeRecord | None:
        """Close a paper trade on exit engine URGENT signal.

        Returns PaperTradeRecord if closed, None otherwise.
        """
        trade = self._tracker.get_trade(pair)
        if not trade or not trade.is_paper:
            return None

        if urgency not in ("URGENT", "CLOSE"):
            return None

        return self._close_and_journal(pair, "signal_exit", close_price)

    # ── Bulk close (weekend / shutdown) ─────────────────────────────

    def close_all_open(
        self,
        reason: str,
        get_close_price,
    ) -> list[PaperTradeRecord]:
        """Force-close every active paper trade in this trader's tracker.

        Used by the Saturday 04:00 JST weekend close-all routine to flatten
        all paper exposure before the market gap. Caller supplies the
        close-price lookup so each trader doesn't need a CalculationResult
        reference.

        Args:
            reason: close_reason to stamp on the journal record (e.g.
                "weekend_close"). Must be a stable identifier so the
                statistics tab can filter by it.
            get_close_price: callable (pair: str) -> float. Returns the
                latest M1 close (BID) for the pair, or 0.0 if unavailable.

        Trades whose close-price lookup returns <= 0 are skipped (data not
        ready yet) — the caller should re-run on the next cycle.
        """
        closed: list[PaperTradeRecord] = []
        # Snapshot the dict so _close_and_journal's mutation doesn't break
        # iteration. active_trades returns a fresh dict each call but be
        # defensive in case that changes.
        for pair in list(self._tracker.active_trades.keys()):
            price = float(get_close_price(pair) or 0.0)
            if price <= 0:
                logger.warning(
                    "[PAPER] %s skip weekend-close — no close price",
                    pair,
                )
                continue
            rec = self._close_and_journal(pair, reason, price)
            if rec is not None:
                closed.append(rec)
        return closed

    # ── Close & Journal ─────────────────────────────────────────────

    def _close_and_journal(
        self, pair: str, reason: str, close_price: float
    ) -> PaperTradeRecord | None:
        """Close a paper trade and update the existing journal record.

        The journal record was created at entry time (in open_paper_trade).
        This method finds it and fills in the exit data.
        """
        trade = self._tracker.get_trade(pair)
        if not trade or not trade.is_paper:
            return None

        # NUCLEAR BLOCK: never journal a "standard" trade
        if trade.entry_type == "standard":
            logger.warning("BLOCKED standard trade from journal: %s %s", pair, trade.direction)
            self._tracker.close_trade(pair, reason=reason, close_price=close_price)
            return None

        # Compute final P/L at the actual close price.
        # Both directions: subtract one round-trip spread (charged once).
        # See open_paper_trade for the SL/TP placement that already shifted
        # the trigger BID levels to match real-broker behavior; this
        # subtraction completes the round-trip cost so paper P/L matches
        # what the live broker would book.
        pip = pip_value(pair)
        _spread_pips = getattr(trade, 'entry_spread_price', 0.0) / pip
        if trade.direction == "BUY":
            pnl = (close_price - trade.entry_price) / pip - _spread_pips
        else:
            pnl = (trade.entry_price - close_price) / pip - _spread_pips

        duration = (time.time() - trade.entry_time) / 60.0
        now_str = datetime.now(_jst()).strftime("%Y-%m-%d %H:%M:%S")

        # Find the existing journal record created at entry time
        journal_idx = getattr(trade, '_journal_idx', -1)
        record = None
        if 0 <= journal_idx < len(self._journal):
            candidate = self._journal[journal_idx]
            if candidate.pair == pair and abs(candidate.entry_time - trade.entry_time) < 1.0:
                record = candidate
        # Fallback: search by pair + entry_time
        if record is None:
            for idx, existing in enumerate(self._journal):
                if existing.pair == pair and abs(existing.entry_time - trade.entry_time) < 1.0:
                    record = existing
                    journal_idx = idx
                    break

        if record is None:
            # No entry record found — create one retroactively. Uses the
            # same generic field-transfer as the close path below so every
            # field shared with TrackedTrade copies automatically (no more
            # hand-maintained list).
            entry_str = datetime.fromtimestamp(
                trade.entry_time, tz=_jst()
            ).strftime("%Y-%m-%d %H:%M:%S")
            record = PaperTradeRecord(
                pair=pair, direction=trade.direction,
                entry_price=trade.entry_price, entry_time=trade.entry_time,
                entry_time_str=entry_str,
            )
            _trade_fs = _paper_shared_fields_trade()
            _record_fs = _paper_shared_fields_record()
            _skip_retro = {"pair", "direction", "entry_price", "entry_time"}
            for _f in (_trade_fs & _record_fs) - _skip_retro:
                try:
                    setattr(record, _f, getattr(trade, _f))
                except Exception:
                    pass
            self._journal.append(record)
            journal_idx = len(self._journal) - 1
            logger.info("[PAPER] No entry record found for %s — created at close", pair)

        # ── Generic trade → record transfer (2026-04-21 refactor) ──
        # Previously this was a ~100-line explicit list: every new field on
        # TrackedTrade/PaperTradeRecord required manually adding a line here,
        # and any missed field meant silently losing data at close time.
        # Now: copy every field that exists on BOTH dataclasses, except the
        # close-managed ones handled explicitly below (computed fresh, or
        # intentionally rounded). The identity fields (pair/direction/
        # entry_price/entry_time) are immutable so re-setting them is a no-op.
        _copy_skip = {
            "close_price", "close_time", "close_reason",
            "pnl_pips", "peak_pnl_pips", "worst_pnl_pips",
            "duration_minutes",
        }
        _trade_field_names = _paper_shared_fields_trade()
        _record_field_names = _paper_shared_fields_record()
        for _f in (_trade_field_names & _record_field_names) - _copy_skip:
            try:
                setattr(record, _f, getattr(trade, _f))
            except Exception as _cex:
                logger.debug("[PAPER] field copy %s skipped: %s", _f, _cex)
        # Close-managed fields (computed here, rounded for display):
        record.close_price = close_price
        record.close_time = time.time()
        record.close_time_str = now_str
        record.close_reason = reason
        record.pnl_pips = round(pnl, 1)
        record.peak_pnl_pips = round(trade.peak_pnl_pips, 1)
        record.worst_pnl_pips = round(trade.worst_pnl_pips, 1)
        record.duration_minutes = round(duration, 1)
        record.is_win = pnl > 0

        # Close the trade in the tracker
        self._tracker.close_trade(pair, reason=reason, close_price=close_price)

        # Start post-close observation (4h window)
        self._post_close_watching[journal_idx] = True

        # Clear per-bar high/low accumulator so a new trade on the same pair
        # doesn't inherit pre-close price extremes
        self._bar_highs.pop(pair, None)
        self._bar_lows.pop(pair, None)

        # Auto-save journal immediately so trades are never lost
        self.save_journal()

        logger.info(
            "[PAPER] Closed %s %s — reason=%s  P/L=%.1f pips  duration=%.0f min  "
            "peak=%.1f  worst=%.1f  (watching 4h post-close)",
            trade.direction, pair, reason, pnl, duration,
            trade.peak_pnl_pips, trade.worst_pnl_pips,
        )
        return record

    # ── Active Paper Trades ─────────────────────────────────────────

    def get_active_paper_trades(self) -> dict[str, TrackedTrade]:
        """Return all active paper trades."""
        return {
            p: t for p, t in self._tracker.active_trades.items()
            if t.is_paper
        }

    @property
    def active_count(self) -> int:
        return len(self.get_active_paper_trades())

    # ── Stats ───────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """Compute summary statistics from journal.

        Only includes CLOSED trades (those with close_reason set). OPEN
        records — created at entry time so they show in the table — are
        excluded so they don't pollute win-rate / avg-pnl / etc.
        """
        closed = [r for r in self._journal if r.close_reason]
        if not closed:
            return {
                "total": 0, "wins": 0, "losses": 0,
                "wr": 0.0, "total_pnl": 0.0, "avg_pnl": 0.0,
                "avg_win": 0.0, "avg_loss": 0.0,
                "avg_duration": 0.0,
                "sl_hits": 0, "tp_hits": 0, "signal_exits": 0,
            }

        wins = [r for r in closed if r.is_win]
        losses = [r for r in closed if not r.is_win]
        total = len(closed)

        return {
            "total": total,
            "wins": len(wins),
            "losses": len(losses),
            "wr": round(len(wins) / total * 100, 1) if total else 0.0,
            "total_pnl": round(sum(r.pnl_pips for r in closed), 1),
            "avg_pnl": round(sum(r.pnl_pips for r in closed) / total, 1),
            "avg_win": round(sum(r.pnl_pips for r in wins) / len(wins), 1) if wins else 0.0,
            "avg_loss": round(sum(r.pnl_pips for r in losses) / len(losses), 1) if losses else 0.0,
            "avg_duration": round(
                sum(r.duration_minutes for r in closed) / total, 1
            ),
            "sl_hits": sum(1 for r in closed if r.close_reason == "sl_hit"),
            "tp_hits": sum(1 for r in closed if r.close_reason == "tp_hit"),
            "signal_exits": sum(1 for r in closed if r.close_reason == "signal_exit"),
        }

    def retroactive_sl_tp_check(self) -> int:
        """Check active trades against historical M1 candles since entry.

        Called on startup to close trades whose SL/TP was hit while the
        app was offline. Returns number of trades closed.

        Requires MetaTrader5 to be initialized.
        """
        active = self.get_active_paper_trades()
        if not active:
            return 0

        try:
            import MetaTrader5 as mt5
        except ImportError:
            logger.warning("MT5 not available for retroactive SL/TP check")
            return 0

        closed = 0
        for pair, trade in list(active.items()):
            if trade.sl_price <= 0 or trade.tp_price <= 0:
                continue
            # How many M1 bars since entry?
            elapsed_min = max(1, int((time.time() - trade.entry_time) / 60))
            bars_needed = min(elapsed_min + 10, 10_000)  # cap at ~7 days

            m1 = mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_M1, 0, bars_needed)
            if m1 is None or len(m1) == 0:
                continue

            # Filter to bars AFTER entry
            entry_ts = trade.entry_time
            hit_reason = None
            hit_price = 0.0

            for bar in m1:
                # Skip entry bar itself (m1 timestamp is bar OPEN time, so bars
                # where bar["time"] <= entry_ts < bar["time"]+60 contain the entry)
                if bar["time"] + 60 <= entry_ts:
                    continue
                if bar["time"] < entry_ts:
                    continue  # entry bar — skip (wick data would be pre-entry)
                h = bar["high"]
                l = bar["low"]

                # Update peak/worst
                _pip = 0.01 if "JPY" in pair else 0.0001
                if trade.direction == "BUY":
                    bar_pnl_hi = (h - trade.entry_price) / _pip
                    bar_pnl_lo = (l - trade.entry_price) / _pip
                else:
                    bar_pnl_hi = (trade.entry_price - l) / _pip
                    bar_pnl_lo = (trade.entry_price - h) / _pip
                trade.peak_pnl_pips = max(trade.peak_pnl_pips, bar_pnl_hi)
                trade.worst_pnl_pips = min(trade.worst_pnl_pips, bar_pnl_lo)

                # Check SL/TP
                result = self._check_sl_tp(trade, h, l)
                if result:
                    hit_reason = result
                    if result == "tp_hit":
                        hit_price = trade.tp_price
                    else:
                        hit_price = trade.sl_price
                    break

            if hit_reason:
                logger.info(
                    "[PAPER] Retroactive %s: %s %s @ %.5f (SL=%.5f TP=%.5f)",
                    hit_reason, trade.direction, pair, hit_price,
                    trade.sl_price, trade.tp_price,
                )
                self._close_and_journal(pair, hit_reason, hit_price)
                closed += 1
            else:
                logger.info(
                    "[PAPER] Retroactive check OK: %s %s — still open "
                    "(peak=%.1f worst=%.1f)",
                    trade.direction, pair,
                    trade.peak_pnl_pips, trade.worst_pnl_pips,
                )

        if closed:
            self.save_journal()
        return closed

    # ── Persistence ─────────────────────────────────────────────────

    def sync_trade_to_journal(self, trade, save: bool = True) -> bool:
        """Copy all fields shared between TrackedTrade and PaperTradeRecord
        from the (stamped) trade object to its journal record, then save.

        Call this after any code path that stamps/mutates fields on a live
        TrackedTrade (notably _stamp_entry_signals in main_window), so the
        journal record on disk reflects the trade's full context AT ENTRY
        TIME rather than only at close. This is the restart-safety
        guarantee: if the process crashes after a stamp, the JSON record
        already contains the stamped fields.

        Returns True if sync happened, False if the trade has no journal
        index (stray trade not linked to journal) or the index is stale.
        """
        idx = getattr(trade, "_journal_idx", -1)
        if not (0 <= idx < len(self._journal)):
            return False
        record = self._journal[idx]
        # Sanity: journal slot should still belong to this trade
        if record.pair != trade.pair or abs(record.entry_time - trade.entry_time) > 1.0:
            return False
        _trade_fs = _paper_shared_fields_trade()
        _record_fs = _paper_shared_fields_record()
        # Same exclusion list as the close path — these are computed fresh
        # on close and should not be touched by an entry-time sync.
        _skip = {"close_price", "close_time", "close_reason",
                 "pnl_pips", "peak_pnl_pips", "worst_pnl_pips",
                 "duration_minutes"}
        for _f in (_trade_fs & _record_fs) - _skip:
            try:
                setattr(record, _f, getattr(trade, _f))
            except Exception:
                pass
        if save:
            self.save_journal()
        return True

    def save_journal(self) -> None:
        """Save the complete journal to disk."""
        self._journal_path.parent.mkdir(parents=True, exist_ok=True)
        data = [asdict(r) for r in self._journal]
        try:
            self._journal_path.write_text(
                json.dumps(data, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            logger.info("Paper journal saved: %d trades", len(data))
        except OSError as e:
            logger.warning("Failed to save paper journal: %s", e)

    def load_journal(self) -> int:
        """Load journal from disk on startup.

        Returns number of records loaded.
        """
        if not self._journal_path.exists():
            return 0
        try:
            data = json.loads(
                self._journal_path.read_text(encoding="utf-8")
            )
            for d in data:
                r = PaperTradeRecord()
                for k, v in d.items():
                    if hasattr(r, k):
                        setattr(r, k, v)
                self._journal.append(r)

            # Backfill missing session from entry_time
            from takumi_trader.core.session_manager import get_session_for_timestamp
            _backfilled = 0
            for r in self._journal:
                if not r.session and r.entry_time > 0:
                    r.session = get_session_for_timestamp(r.entry_time)
                    _backfilled += 1
            if _backfilled:
                logger.info("[PAPER] Backfilled session for %d trades", _backfilled)

            # Restore post-close watching for incomplete observations
            now = time.time()
            for idx, r in enumerate(self._journal):
                if not r.post_close_complete and r.close_time > 0:
                    elapsed_h = (now - r.close_time) / 3600.0
                    if elapsed_h < self.POST_CLOSE_HOURS:
                        # Still within 4h window — keep watching
                        self._post_close_watching[idx] = True
                    else:
                        # Past 4h window — mark complete with whatever we have
                        r.post_close_complete = True
                        r.post_close_minutes = round(elapsed_h * 60, 1)
                        logger.info(
                            "[PAPER] Post-close expired on reload: %s %s "
                            "(closed %.1fh ago, marking complete)",
                            r.direction, r.pair, elapsed_h,
                        )

            watching = len(self._post_close_watching)
            logger.info(
                "Paper journal loaded: %d trades (%d still observing post-close)",
                len(self._journal), watching,
            )
            return len(self._journal)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to load paper journal: %s", e)
            return 0

    def clear_journal(self) -> None:
        """Delete all closed trade records from memory and disk."""
        self._journal.clear()
        self._post_close_watching.clear()
        try:
            if self._journal_path.exists():
                self._journal_path.unlink()
        except OSError:
            pass
        logger.info("Paper trade journal cleared")

    @property
    def journal(self) -> list[PaperTradeRecord]:
        return list(self._journal)
