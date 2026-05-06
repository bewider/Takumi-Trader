"""Master feature engine — aggregates all feature modules into a single
unified API for entry-time enrichment of TAKUMI signals.

Usage:
    from takumi_trader.features import FeatureEngine
    engine = FeatureEngine(fred_api_key="optional")
    features = engine.compute_for_entry(
        pair="EURUSD",
        timestamp_utc=1714339200,
        m1_bars=...,        # numpy structured array from MT5
        m15_bars=...,
        h1_bars=...,
        composite_scores={"USD": 5.0, "EUR": 6.0, ...},
    )
    # `features` is a flat dict of ~100+ feature columns

Categories computed:
    - Microstructure (CVD, divergence, tick aggression if ticks provided)
    - Volatility (Parkinson, GK, YZ, ATR pct rank, jump detection, etc.)
    - Levels (round numbers, prior OHLC, pivots, VWAP)
    - Regimes (ADX, Choppiness, Hurst, Aroon, Vortex, KAMA, regime label)
    - Statistics (autocorr, FFT, skew, kurt)
    - CSI metrics (dispersion, breadth, RoC)
    - FX-specific (carry, fix windows, month-end)
    - Patterns (FVG, order blocks, equal highs/lows)
    - Adversarial (stop-hunt, liquidity sweep, round-number magnetism)
    - Behavioral (post-news, day-after-FOMC, lunch hour)
    - Cross-market (DXY computed from existing pairs)

Network-fetched features (yields, calendar, sentiment, COT) are OPT-IN
via flags so offline analysis doesn't block waiting for HTTP.
"""
from __future__ import annotations

from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta

import numpy as np

from . import (
    microstructure as ms,
    volatility as vol,
    levels as lvl,
    regimes as reg,
    statistics as stat,
    csi_metrics as csi,
    portfolio as port,
    fx_specific as fx,
    patterns as pat,
    adversarial as adv,
    behavioral as beh,
    cross_market as cm,
)

UTC = timezone.utc

JST = timezone(timedelta(hours=9))


def _pip(pair: str) -> float:
    return 0.01 if pair.endswith("JPY") else 0.0001


class FeatureEngine:
    """Unified feature computation interface."""

    def __init__(
        self,
        fred_api_key: Optional[str] = None,
        enable_network: bool = False,
        network_cache_ttl_sec: float = 1800.0,  # 30 min — refresh interval
    ) -> None:
        self.fred_api_key = fred_api_key
        self.enable_network = enable_network
        self._network_cache_ttl_sec = network_cache_ttl_sec
        # Lazy-loaded network module references
        self._yields = None
        self._market_data = None
        self._calendar = None
        self._sentiment = None
        self._positioning = None
        # Network features cache (refreshed every TTL seconds; shared across trades)
        self._network_cache: Dict[str, Any] = {}
        self._network_cache_time: float = 0.0

    # ──────────────────────────────────────────────────────────────
    # Network cache — refreshed periodically; shared across trades
    # ──────────────────────────────────────────────────────────────

    def refresh_network_cache(self, force: bool = False) -> Dict[str, Any]:
        """Refresh global network features (Yahoo, FRED, calendar, sentiment, COT).

        Returns the full cache dict. Skipped if cache is fresh AND not forced.
        Failures per source are isolated — partial cache is better than none.
        """
        import time as _t
        now = _t.time()
        if (not force
                and self._network_cache
                and (now - self._network_cache_time) < self._network_cache_ttl_sec):
            return self._network_cache

        cache: Dict[str, Any] = {}

        # ── Yahoo Finance (free, no auth) ──
        try:
            from . import market_data
            cache.update({
                "feat_vix": market_data.vix_close(),
                "feat_vvix": market_data.vvix_close(),
                "feat_skew": market_data.skew_close(),
                "feat_move": market_data.move_close(),
                "feat_gold_close": market_data.gold_close(),
                "feat_wti_close": market_data.crude_oil_close(),
                "feat_brent_close": market_data.brent_close(),
                "feat_copper_close": market_data.copper_close(),
                "feat_natgas_close": market_data.nat_gas_close(),
                "feat_sp500_close": market_data.sp500_close(),
                "feat_nasdaq_close": market_data.nasdaq_close(),
                "feat_nikkei_close": market_data.nikkei_close(),
                "feat_dax_close": market_data.dax_close(),
                "feat_ftse_close": market_data.ftse_close(),
                "feat_hang_seng_close": market_data.hang_seng_close(),
                "feat_btc_close": market_data.btc_close(),
            })
        except Exception:
            pass

        # ── FRED (requires free API key) ──
        if self.fred_api_key:
            try:
                from . import yields
                curve = yields.yield_curve_slope(self.fred_api_key)
                cache.update({
                    "feat_fred_us_10y": curve.get("us_10y", 0.0),
                    "feat_fred_us_2y": curve.get("us_2y", 0.0),
                    "feat_fred_us_3m": curve.get("us_3m", 0.0),
                    "feat_fred_yield_curve_2_10": curve.get("spread_10y_2y", 0.0),
                })
                cache["feat_fred_real_10y"] = yields.real_yields(self.fred_api_key).get("us_real_10y", 0.0)
                cs = yields.credit_spreads(self.fred_api_key)
                cache["feat_fred_hy_oas"] = cs.get("hy_oas_pct", 0.0)
                cache["feat_fred_ig_oas"] = cs.get("ig_oas_pct", 0.0)
                cache["feat_fred_ted_spread"] = yields.ted_spread(self.fred_api_key)
            except Exception:
                pass

        # ── COT (CFTC, free) — weekly snapshot ──
        try:
            from . import positioning
            cache["_cot_snapshot"] = positioning.get_latest_cot_snapshot()
        except Exception:
            cache["_cot_snapshot"] = {}

        # ── Sentiment (RSS + Reddit, free) — global aggregate ──
        try:
            from . import sentiment
            cache["feat_news_flow_rate"] = sentiment.news_flow_rate()
            cache["feat_reddit_forex_sentiment"] = (
                sentiment.reddit_forex_sentiment().get("avg_sentiment", 0.0)
            )
            # Per-currency sentiment cached so per-pair calls reuse it
            cache["_sentiment_per_ccy"] = {}
            for ccy in ("USD", "EUR", "GBP", "JPY", "CAD", "AUD", "NZD", "CHF"):
                cache["_sentiment_per_ccy"][ccy] = (
                    sentiment.aggregate_news_sentiment(ccy).get("avg_sentiment", 0.0)
                )
        except Exception:
            cache["_sentiment_per_ccy"] = {}

        # ── Calendar — kept "live" rather than cached because per-pair queries ──
        # are quick once the underlying weekly JSON is cached on disk.
        # Just verify the events list is fresh.
        try:
            from . import calendar as cal
            cal.get_all_events()  # primes disk cache
        except Exception:
            pass

        self._network_cache = cache
        self._network_cache_time = now
        return cache

    # ──────────────────────────────────────────────────────────────
    # Per-pair feature computation
    # ──────────────────────────────────────────────────────────────

    def compute_for_entry(
        self,
        pair: str,
        timestamp_utc: int,
        m1_bars: Optional[Any] = None,
        m15_bars: Optional[Any] = None,
        h1_bars: Optional[Any] = None,
        h4_bars: Optional[Any] = None,
        d1_bars: Optional[Any] = None,
        composite_scores: Optional[Dict[str, float]] = None,
        composite_scores_prev: Optional[Dict[str, float]] = None,
        cross_pair_data: Optional[Dict[str, Any]] = None,
        active_trades: Optional[list] = None,
    ) -> Dict[str, Any]:
        """Compute all available features for a single entry decision."""
        f: Dict[str, Any] = {}
        pip = _pip(pair)

        # ── Time features ──
        dt = datetime.fromtimestamp(timestamp_utc, tz=UTC)
        f["timestamp_utc"] = timestamp_utc
        f["session_jst"] = fx.session_label_jst(timestamp_utc)
        f["dst_active_uk"] = fx.is_dst_active("UK", dt)
        f["dst_active_us"] = fx.is_dst_active("US", dt)
        f["is_month_end"] = fx.is_month_end(timestamp_utc)
        f["is_quarter_end"] = fx.is_quarter_end(timestamp_utc)
        f["holiday_label"] = fx.major_holiday(timestamp_utc) or ""
        f.update(fx.all_fix_windows(timestamp_utc))
        f["is_friday_late"] = beh.is_friday_late(timestamp_utc)
        f["is_sunday_open"] = beh.is_sunday_open(timestamp_utc)
        f["lunch_hour"] = beh.lunch_hour_drift(timestamp_utc)
        f["days_into_quarter"] = beh.days_into_quarter(timestamp_utc)

        # ── Microstructure (M1) ──
        if m1_bars is not None and len(m1_bars) > 30:
            closes_m1 = np.asarray(m1_bars["close"], dtype=np.float64)
            vols_m1 = np.asarray(m1_bars.get("tick_volume", np.ones(len(closes_m1))), dtype=np.float64) \
                if hasattr(m1_bars, 'get') else np.asarray(m1_bars["tick_volume"], dtype=np.float64) \
                if "tick_volume" in m1_bars.dtype.names else np.ones(len(closes_m1))
            f["cvd_30m_m1"] = float(ms.cumulative_volume_delta_m1(closes_m1[-30:], vols_m1[-30:])[-1])
            div_flag, price_move, cvd_change = ms.cvd_divergence(
                closes_m1, vols_m1, window=30,
                price_move_threshold_pips=5.0, pip_size=pip,
            )
            f["cvd_divergent_30m"] = div_flag
            f["cvd_30m_price_move_pips"] = price_move
            f["cvd_30m_change"] = cvd_change
            f["amihud_illiq_60m"] = ms.amihud_illiquidity(closes_m1[-60:], vols_m1[-60:])
            signed_v = vols_m1 * ms.tick_rule_classify(closes_m1)
            f["kyle_lambda_60m"] = ms.kyle_lambda(closes_m1[-60:], signed_v[-60:])
            # Tick burst
            f.update({"tick_burst_" + k: v for k, v in adv.tick_burst_detection(vols_m1).items()})

        # ── Volatility (M15 + H1) ──
        if m15_bars is not None and len(m15_bars) >= 30:
            o15 = np.asarray(m15_bars["open"], dtype=np.float64)
            h15 = np.asarray(m15_bars["high"], dtype=np.float64)
            l15 = np.asarray(m15_bars["low"], dtype=np.float64)
            c15 = np.asarray(m15_bars["close"], dtype=np.float64)
            f["m15_realized_var"] = vol.realized_variance(c15[-30:])
            f["m15_parkinson"] = vol.parkinson(h15[-30:], l15[-30:])
            f["m15_garman_klass"] = vol.garman_klass(o15[-30:], h15[-30:], l15[-30:], c15[-30:])
            f["m15_rogers_satchell"] = vol.rogers_satchell(o15[-30:], h15[-30:], l15[-30:], c15[-30:])
            f["m15_yang_zhang"] = vol.yang_zhang(o15[-30:], h15[-30:], l15[-30:], c15[-30:])
            f["m15_bipower"] = vol.bipower_variation(c15[-30:])
            f["m15_realized_skew"] = vol.realized_skew(c15[-60:])
            f["m15_realized_kurt"] = vol.realized_kurtosis(c15[-60:])
            jump, stat_val = vol.lee_mykland_jump(c15)
            f["m15_jump_detected"] = jump
            f["m15_jump_stat"] = stat_val
            f["m15_vol_ratio"] = vol.volatility_ratio(c15)
            f["m15_vol_of_vol"] = vol.vol_of_vol(c15)
            f["m15_atr14_pips"] = vol.atr(h15, l15, c15, period=14) / pip
            f["m15_atr14_pct_rank"] = vol.atr_percentile_rank(h15, l15, c15, period=14, lookback_bars=200)
            bb_u, bb_m, bb_l = vol.bollinger_bands(c15, period=20, num_std=2.0)
            f["m15_bb_upper"] = bb_u
            f["m15_bb_lower"] = bb_l
            f["m15_bb_width_pips"] = (bb_u - bb_l) / pip if bb_u > 0 else 0
            kc_u, kc_m, kc_l = vol.keltner_channels(h15, l15, c15)
            f["m15_kc_upper"] = kc_u
            f["m15_kc_lower"] = kc_l
            if kc_u > kc_l:
                f["m15_bbkc_ratio"] = (bb_u - bb_l) / (kc_u - kc_l)
            else:
                f["m15_bbkc_ratio"] = 1.0

        if h1_bars is not None and len(h1_bars) >= 30:
            h1 = np.asarray(h1_bars["high"], dtype=np.float64)
            l1 = np.asarray(h1_bars["low"], dtype=np.float64)
            c1 = np.asarray(h1_bars["close"], dtype=np.float64)
            o1 = np.asarray(h1_bars["open"], dtype=np.float64)
            f["h1_atr14_pips"] = vol.atr(h1, l1, c1, period=14) / pip
            f["h1_atr14_pct_rank"] = vol.atr_percentile_rank(h1, l1, c1, period=14, lookback_bars=100)
            f["h1_yang_zhang"] = vol.yang_zhang(o1[-30:], h1[-30:], l1[-30:], c1[-30:])
            f["h1_realized_var"] = vol.realized_variance(c1[-30:])
            f["h1_parkinson"] = vol.parkinson(h1[-30:], l1[-30:])
            f["h1_vol_ratio"] = vol.volatility_ratio(c1)
            # Regime classifiers on H1
            adx_val, plus_di, minus_di = reg.adx(h1, l1, c1, period=14)
            f["h1_adx"] = adx_val
            f["h1_plus_di"] = plus_di
            f["h1_minus_di"] = minus_di
            f["h1_choppiness"] = reg.choppiness_index(h1, l1, c1, period=14)
            aroon_u, aroon_d, aroon_osc = reg.aroon(h1, l1, period=14)
            f["h1_aroon_up"] = aroon_u
            f["h1_aroon_down"] = aroon_d
            f["h1_aroon_osc"] = aroon_osc
            vi_p, vi_m = reg.vortex(h1, l1, c1, period=14)
            f["h1_vortex_plus"] = vi_p
            f["h1_vortex_minus"] = vi_m
            f["h1_kama"] = reg.kama(c1)
            st_val, st_dir = reg.supertrend(h1, l1, c1)
            f["h1_supertrend_value"] = st_val
            f["h1_supertrend_dir"] = st_dir
            ichi = reg.ichimoku(h1, l1, c1)
            for k, v in ichi.items():
                f[f"h1_ichimoku_{k}"] = v
            f["h1_donchian_pos"] = reg.donchian_position(h1, l1, c1)
            slope, r2 = reg.linear_regression(c1)
            f["h1_lr_slope"] = slope
            f["h1_lr_r2"] = r2
            f["h1_hurst"] = reg.hurst_exponent(c1, max_lag=20)
            f["h1_dfa"] = reg.detrended_fluctuation(c1, max_window=30)
            f["h1_kaufman_er"] = reg.kaufman_efficiency_ratio(c1)
            f["h1_trend_persistence"] = reg.trend_persistence(c1)
            f["h1_mr_zscore"] = reg.mean_reversion_zscore(c1)
            f["h1_regime"] = reg.regime_classify(h1, l1, c1)
            # Statistics
            f.update({f"h1_{k}": v for k, v in stat.autocorrelation(c1).items()})
            f.update({f"h1_{k}": v for k, v in stat.partial_autocorrelation(c1).items()})
            f["h1_skew_60"] = stat.rolling_skew(c1, window=60)
            f["h1_kurt_60"] = stat.rolling_kurtosis(c1, window=60)
            f.update({f"h1_{k}": v for k, v in stat.fft_dominant_frequency(c1).items()})
            f["h1_half_life"] = stat.half_life_mean_reversion(c1)
            # Adversarial
            f.update({f"h1_{k}": v for k, v in adv.round_number_magnetism(h1, l1, c1, pip_size=pip).items()})
            sweep = adv.liquidity_sweep_pattern(h1, l1, c1, pip_size=pip)
            f["h1_sweep_type"] = sweep.get("sweep_type", "none")
            # Patterns
            pat_label = pat.candlestick_pattern(o1, h1, l1, c1)
            f["h1_candle_pattern"] = pat_label
            equal = pat.equal_highs_lows(h1, l1, pip_size=pip)
            f["h1_equal_highs"] = equal["equal_highs"]
            f["h1_equal_lows"] = equal["equal_lows"]
            tlb = pat.trendline_break(c1)
            f["h1_trendline_break"] = tlb["break_type"]
            # Fair Value Gap count + Order Block count over last 50 H1 bars
            try:
                o1 = np.asarray(h1_bars["open"], dtype=np.float64)
                fvgs = pat.fair_value_gaps(
                    o1[-50:], h1[-50:], l1[-50:], c1[-50:],
                    pip_size=pip, min_gap_pips=2.0,
                )
                f["fvg_count"] = len(fvgs)
                obs = pat.order_blocks(o1[-50:], h1[-50:], l1[-50:], c1[-50:])
                f["order_block_count"] = len(obs)
            except Exception:
                f["fvg_count"] = 0
                f["order_block_count"] = 0
            # Liquidity void count
            try:
                voids = adv.liquidity_void_detection(h1, l1, c1, pip_size=pip)
                f["h1_liquidity_void_count"] = len(voids.get("voids", []))
            except Exception:
                f["h1_liquidity_void_count"] = 0

        # ── Levels (uses full history available) ──
        if h1_bars is not None and len(h1_bars) >= 50:
            t1 = np.asarray(h1_bars["time"], dtype=np.int64)
            o1 = np.asarray(h1_bars["open"], dtype=np.float64)
            h1 = np.asarray(h1_bars["high"], dtype=np.float64)
            l1 = np.asarray(h1_bars["low"], dtype=np.float64)
            c1 = np.asarray(h1_bars["close"], dtype=np.float64)
            current_price = float(c1[-1])
            f.update(lvl.round_number_distance_pips(current_price, pip_size=pip))
            f.update(lvl.prior_day_ohlc(t1, o1, h1, l1, c1, timestamp_utc))
            f.update(lvl.prior_week_ohlc(t1, o1, h1, l1, c1, timestamp_utc))
            f.update(lvl.prior_month_ohlc(t1, o1, h1, l1, c1, timestamp_utc))
            f.update(lvl.year_high_low(h1, l1))
            f.update({f"asian_{k}": v for k, v in lvl.asian_session_range(t1, h1, l1, timestamp_utc).items()})
            f.update({f"london_{k}": v for k, v in lvl.london_session_range(t1, h1, l1, timestamp_utc).items()})
            # Pivot points (using prior day OHLC if available)
            pd_h = f.get("prev_day_high", 0)
            pd_l = f.get("prev_day_low", 0)
            pd_c = f.get("prev_day_close", 0)
            if pd_h > 0 and pd_l > 0 and pd_c > 0:
                f.update(lvl.classic_pivots(pd_h, pd_l, pd_c))
                f.update(lvl.fibonacci_pivots(pd_h, pd_l, pd_c))
                f.update(lvl.camarilla_pivots(pd_h, pd_l, pd_c))
            # VWAP
            vols_h1 = np.asarray(h1_bars["tick_volume"], dtype=np.float64) if "tick_volume" in h1_bars.dtype.names else np.ones(len(c1))
            f["h1_vwap"] = lvl.vwap(c1[-50:], vols_h1[-50:], highs=h1[-50:], lows=l1[-50:])
            f["h1_session_vwap"] = lvl.session_vwap(t1, c1, vols_h1, timestamp_utc)
            # Volume profile
            vp = lvl.volume_profile(c1[-100:], vols_h1[-100:], highs=h1[-100:], lows=l1[-100:], pip_size=pip)
            f["h1_poc"] = vp["poc"]
            f["h1_vah"] = vp["vah"]
            f["h1_val"] = vp["val"]
            # Stop-hunt detection vs nearest round number
            nearest_50 = f.get("nearest_50_level", 0)
            if nearest_50 > 0:
                hunt = adv.stop_hunt_score(h1, l1, c1, nearest_50, pip_size=pip)
                f["h1_stop_hunt_score"] = hunt["hunt_score"]
                f["h1_stop_hunt_dir"] = hunt["direction"]

        # ── CSI metrics ──
        if composite_scores:
            f["csi_dispersion"] = csi.csi_dispersion(composite_scores)
            f.update(csi.csi_breadth(composite_scores))
            if composite_scores_prev:
                f.update(csi.csi_rate_of_change(composite_scores, composite_scores_prev))

        # ── FX-specific ──
        f["carry_pips_per_day"] = fx.carry_differential(pair) / 365 / pip * 10000  # rough conversion

        # ── Cross-market (DXY) — needs cross-pair data ──
        if cross_pair_data:
            try:
                f["dxy_synthetic"] = cm.dxy_approximate(
                    cross_pair_data.get("EURUSD", 0),
                    cross_pair_data.get("USDJPY", 0),
                    cross_pair_data.get("GBPUSD", 0),
                    cross_pair_data.get("USDCAD", 0),
                    cross_pair_data.get("USDCHF", 0),
                )
            except Exception:
                pass

        # ── Portfolio-level ──
        if active_trades:
            f["portfolio_delta_pips"] = port.portfolio_delta_pips(active_trades)
            f.update({f"port_{k}": v for k, v in port.concentration_risk(active_trades).items()})
            f.update({f"expo_{k}": v for k, v in port.currency_exposure(active_trades).items()})

        # ── Network features (optional) ──
        if self.enable_network:
            f.update(self._fetch_network_features(pair, timestamp_utc))

        return f

    # ──────────────────────────────────────────────────────────────
    # Auto-fetch helper — does MT5 calls for callers without bars
    # ──────────────────────────────────────────────────────────────

    def compute_entry_features(
        self,
        pair: str,
        timestamp_utc: int,
        composite_scores: Optional[Dict[str, float]] = None,
        composite_scores_prev: Optional[Dict[str, float]] = None,
        cross_pair_data: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """Compute the FULL ~177 feat_* superset by auto-fetching MT5 bars
        and merging in cached network features.

        Local features:   ~140 keys, computed per-call (~50-100ms latency)
        Network features: ~30 keys, pulled from self._network_cache
                           (refreshed every TTL, default 30 min)

        Returns empty dict if MT5 unreachable. Maps everything to
        PaperTradeRecord's `feat_` schema.
        """
        try:
            import MetaTrader5 as mt5
        except ImportError:
            return {}

        try:
            m1_bars = mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_M1, 0, 200)
            m15_bars = mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_M15, 0, 200)
            # Fetch ~6 weeks of H1 (1000 bars × 1h ≈ 42 days) so prior_month_ohlc
            # and year_high_low have enough history to look back. Without this,
            # the prior-month/year features default to 0.0.
            h1_bars = mt5.copy_rates_from_pos(pair, mt5.TIMEFRAME_H1, 0, 1000)
        except Exception:
            return {}
        if m1_bars is None or m15_bars is None or h1_bars is None:
            return {}

        full = self.compute_for_entry(
            pair=pair,
            timestamp_utc=timestamp_utc,
            m1_bars=m1_bars,
            m15_bars=m15_bars,
            h1_bars=h1_bars,
            composite_scores=composite_scores,
            composite_scores_prev=composite_scores_prev,
            cross_pair_data=cross_pair_data,
        )

        # ── DUPLICATION NOTICE (added 2026-05-05 for Phase C.3) ──
        # The feat_* mapping below is duplicated by extract_feat_dict()
        # at the bottom of this module, which the shadow-simulator path
        # uses to apply the same canonical schema to compute_for_entry
        # results computed against historical bars. The two mappings
        # MUST stay in sync — when adding a new feat_* key here, also
        # add it to extract_feat_dict.
        # A future refactor (Phase F.9) could collapse to a single source
        # by calling extract_feat_dict from here, but that change affects
        # live PaperTrader behavior and requires its own validation cycle.
        #
        # ── Sync verification command ──
        # After editing either location, verify key counts match by running:
        #   python -c "from takumi_trader.features.feature_engine import extract_feat_dict; \
        #              print('extract_feat_dict keys:', len(extract_feat_dict({})))"
        # Then count "feat_" lines in the inline mapping below (lines 481–650-ish):
        #   grep -c '^\s*\"feat_' takumi_trader/features/feature_engine.py
        # Both numbers should match (modulo intentional omissions like
        # extract_feat_dict's deliberate exclusion of network features).
        # Build complete feat_* mapping (Tier 1 + Tier 2 local features)
        out: Dict[str, Any] = {
            # ─── Tier 1 (already shipped) ────────────────────────────
            "feat_cvd_30m": full.get("cvd_30m_m1", 0.0),
            "feat_cvd_divergent": full.get("cvd_divergent_30m", False),
            "feat_cvd_price_move_pips": full.get("cvd_30m_price_move_pips", 0.0),
            "feat_amihud_illiq_60m": full.get("amihud_illiq_60m", 0.0),
            "feat_kyle_lambda_60m": full.get("kyle_lambda_60m", 0.0),
            "feat_m15_atr14_pct_rank": full.get("m15_atr14_pct_rank", 0.0),
            "feat_h1_atr14_pct_rank": full.get("h1_atr14_pct_rank", 0.0),
            "feat_m15_jump_detected": full.get("m15_jump_detected", False),
            "feat_m15_yang_zhang": full.get("m15_yang_zhang", 0.0),
            "feat_m15_realized_skew": full.get("m15_realized_skew", 0.0),
            "feat_m15_realized_kurt": full.get("m15_realized_kurt", 0.0),
            "feat_h1_adx": full.get("h1_adx", 0.0),
            "feat_h1_choppiness": full.get("h1_choppiness", 0.0),
            "feat_h1_hurst": full.get("h1_hurst", 0.5),
            "feat_h1_kaufman_er": full.get("h1_kaufman_er", 0.0),
            "feat_h1_regime": full.get("h1_regime", ""),
            "feat_h1_donchian_pos": full.get("h1_donchian_pos", 0.5),
            "feat_h1_aroon_osc": full.get("h1_aroon_osc", 0.0),
            "feat_h1_acf_lag_1": full.get("h1_acf_lag_1", 0.0),
            "feat_h1_half_life_bars": full.get("h1_half_life", 0.0),
            "feat_csi_dispersion": full.get("csi_dispersion", 0.0),
            "feat_csi_strong_count": full.get("csi_strong_count", 0),
            "feat_csi_weak_count": full.get("csi_weak_count", 0),
            "feat_dxy_synthetic": full.get("dxy_synthetic", 0.0),
            "feat_dist_to_50_pips": full.get("dist_to_nearest_50_pips", 0.0),
            "feat_dist_to_big_figure_pips": full.get("dist_to_big_figure_pips", 0.0),
            "feat_h1_poc": full.get("h1_poc", 0.0),
            "feat_h1_stop_hunt_score": full.get("h1_stop_hunt_score", 0.0),
            "feat_h1_sweep_type": full.get("h1_sweep_type", ""),
            "feat_is_month_end": full.get("is_month_end", False),
            "feat_in_london_fix": full.get("in_london_fix", False),
            "feat_in_ecb_fix": full.get("in_ecb_fix", False),

            # ─── Tier 2 — Volatility additions (M15) ─────────────────
            "feat_m15_realized_var": full.get("m15_realized_var", 0.0),
            "feat_m15_parkinson": full.get("m15_parkinson", 0.0),
            "feat_m15_garman_klass": full.get("m15_garman_klass", 0.0),
            "feat_m15_rogers_satchell": full.get("m15_rogers_satchell", 0.0),
            "feat_m15_bipower": full.get("m15_bipower", 0.0),
            "feat_m15_vol_ratio": full.get("m15_vol_ratio", 0.0),
            "feat_m15_vol_of_vol": full.get("m15_vol_of_vol", 0.0),
            "feat_m15_bb_upper": full.get("m15_bb_upper", 0.0),
            "feat_m15_bb_lower": full.get("m15_bb_lower", 0.0),
            "feat_m15_bb_width_pips": full.get("m15_bb_width_pips", 0.0),
            "feat_m15_kc_upper": full.get("m15_kc_upper", 0.0),
            "feat_m15_kc_lower": full.get("m15_kc_lower", 0.0),
            "feat_m15_bbkc_ratio": full.get("m15_bbkc_ratio", 1.0),

            # Volatility additions (H1)
            "feat_h1_realized_var": full.get("h1_realized_var", 0.0),
            "feat_h1_parkinson": full.get("h1_parkinson", 0.0),
            "feat_h1_yang_zhang": full.get("h1_yang_zhang", 0.0),
            "feat_h1_atr14_pips": full.get("h1_atr14_pips", 0.0),
            "feat_h1_vol_ratio": full.get("h1_vol_ratio", 0.0),

            # Regimes additions
            "feat_h1_plus_di": full.get("h1_plus_di", 0.0),
            "feat_h1_minus_di": full.get("h1_minus_di", 0.0),
            "feat_h1_aroon_up": full.get("h1_aroon_up", 0.0),
            "feat_h1_aroon_down": full.get("h1_aroon_down", 0.0),
            "feat_h1_vortex_plus": full.get("h1_vortex_plus", 0.0),
            "feat_h1_vortex_minus": full.get("h1_vortex_minus", 0.0),
            "feat_h1_kama": full.get("h1_kama", 0.0),
            "feat_h1_supertrend_value": full.get("h1_supertrend_value", 0.0),
            "feat_h1_supertrend_dir": full.get("h1_supertrend_dir", 0),
            "feat_h1_ichimoku_tenkan": full.get("h1_ichimoku_tenkan", 0.0),
            "feat_h1_ichimoku_kijun": full.get("h1_ichimoku_kijun", 0.0),
            "feat_h1_ichimoku_senkou_a": full.get("h1_ichimoku_senkou_a", 0.0),
            "feat_h1_ichimoku_senkou_b": full.get("h1_ichimoku_senkou_b", 0.0),
            "feat_h1_ichimoku_above_cloud": full.get("h1_ichimoku_above_cloud", False),
            "feat_h1_ichimoku_in_cloud": full.get("h1_ichimoku_in_cloud", False),
            "feat_h1_ichimoku_below_cloud": full.get("h1_ichimoku_below_cloud", False),
            "feat_h1_lr_slope": full.get("h1_lr_slope", 0.0),
            "feat_h1_lr_r2": full.get("h1_lr_r2", 0.0),
            "feat_h1_dfa": full.get("h1_dfa", 0.5),
            "feat_h1_trend_persistence": full.get("h1_trend_persistence", 0.0),
            "feat_h1_mr_zscore": full.get("h1_mr_zscore", 0.0),

            # Statistics additions
            "feat_h1_acf_lag_5": full.get("h1_acf_lag_5", 0.0),
            "feat_h1_acf_lag_15": full.get("h1_acf_lag_15", 0.0),
            "feat_h1_pacf_lag_1": full.get("h1_pacf_lag_1", 0.0),
            "feat_h1_pacf_lag_5": full.get("h1_pacf_lag_5", 0.0),
            "feat_h1_skew_60": full.get("h1_skew_60", 0.0),
            "feat_h1_kurt_60": full.get("h1_kurt_60", 0.0),
            "feat_h1_fft_period_min": full.get("h1_dominant_period_min", 0.0),
            "feat_h1_fft_amplitude_ratio": full.get("h1_amplitude_ratio", 0.0),

            # CSI deltas
            "feat_dUSD": full.get("dUSD", 0.0),
            "feat_dEUR": full.get("dEUR", 0.0),
            "feat_dGBP": full.get("dGBP", 0.0),
            "feat_dJPY": full.get("dJPY", 0.0),
            "feat_dCAD": full.get("dCAD", 0.0),
            "feat_dAUD": full.get("dAUD", 0.0),
            "feat_dNZD": full.get("dNZD", 0.0),
            "feat_dCHF": full.get("dCHF", 0.0),

            # Cross-market additions
            "feat_carry_pips_per_day": full.get("carry_pips_per_day", 0.0),

            # Levels additions
            "feat_dist_to_25_pips": full.get("dist_to_nearest_25_pips", 0.0),
            "feat_dist_to_century_pips": full.get("dist_to_century_pips", 0.0),
            "feat_prev_day_open": full.get("prev_day_open", 0.0),
            "feat_prev_day_high": full.get("prev_day_high", 0.0),
            "feat_prev_day_low": full.get("prev_day_low", 0.0),
            "feat_prev_day_close": full.get("prev_day_close", 0.0),
            "feat_prev_week_high": full.get("prev_week_high", 0.0),
            "feat_prev_week_low": full.get("prev_week_low", 0.0),
            "feat_prev_month_high": full.get("prev_month_high", 0.0),
            "feat_prev_month_low": full.get("prev_month_low", 0.0),
            "feat_year_high": full.get("year_high", 0.0),
            "feat_year_low": full.get("year_low", 0.0),
            "feat_asian_session_high": full.get("asian_session_high", 0.0),
            "feat_asian_session_low": full.get("asian_session_low", 0.0),
            "feat_asian_session_range_pips": full.get("asian_session_range_pips", 0.0),
            "feat_london_session_high": full.get("london_session_high", 0.0),
            "feat_london_session_low": full.get("london_session_low", 0.0),
            "feat_pivot_pp": full.get("pivot_pp", 0.0),
            "feat_pivot_r1": full.get("pivot_r1", 0.0),
            "feat_pivot_r2": full.get("pivot_r2", 0.0),
            "feat_pivot_s1": full.get("pivot_s1", 0.0),
            "feat_pivot_s2": full.get("pivot_s2", 0.0),
            "feat_fib_pp": full.get("fib_pp", 0.0),
            "feat_fib_r1": full.get("fib_r1", 0.0),
            "feat_fib_s1": full.get("fib_s1", 0.0),
            "feat_cam_r3": full.get("cam_r3", 0.0),
            "feat_cam_s3": full.get("cam_s3", 0.0),
            "feat_h1_vwap": full.get("h1_vwap", 0.0),
            "feat_h1_session_vwap": full.get("h1_session_vwap", 0.0),
            "feat_h1_vah": full.get("h1_vah", 0.0),
            "feat_h1_val": full.get("h1_val", 0.0),

            # Patterns
            "feat_h1_equal_highs": full.get("h1_equal_highs", False),
            "feat_h1_equal_lows": full.get("h1_equal_lows", False),
            "feat_h1_trendline_break": full.get("h1_trendline_break", ""),
            "feat_h1_candle_pattern": full.get("h1_candle_pattern", ""),
            "feat_fvg_count": full.get("fvg_count", 0),
            "feat_order_block_count": full.get("order_block_count", 0),
            "feat_h1_liquidity_void_count": full.get("h1_liquidity_void_count", 0),

            # Adversarial additions
            "feat_h1_round_magnetism": full.get("h1_magnet_score", 0.0),
            "feat_h1_tick_burst_z": full.get("tick_burst_z_score", 0.0),

            # FX-specific additions
            "feat_in_tokyo_fix": full.get("in_tokyo_fix", False),
            "feat_dst_active_uk": full.get("dst_active_uk", False),
            "feat_dst_active_us": full.get("dst_active_us", False),
            "feat_holiday_label": full.get("holiday_label", ""),
            "feat_session_label": full.get("session_jst", ""),

            # Behavioral
            "feat_friday_late": full.get("is_friday_late", False),
            "feat_sunday_open": full.get("is_sunday_open", False),
            "feat_lunch_hour": full.get("lunch_hour", ""),
            "feat_days_into_quarter": full.get("days_into_quarter", 0),

            # Schema
            "feat_schema_version": 2,
        }

        # Synthetic currency baskets (require cross-pair data — best computed
        # by main_window which has the live close_prices dict). Pass via the
        # `cross_pair_data` arg if you want them populated.
        if cross_pair_data:
            try:
                from . import cross_market as cm
                out["feat_eur_index"] = cm.eur_index(
                    cross_pair_data.get("EURUSD", 0),
                    cross_pair_data.get("EURJPY", 0),
                    cross_pair_data.get("EURGBP", 0),
                    cross_pair_data.get("EURAUD", 0),
                    cross_pair_data.get("EURCAD", 0),
                    cross_pair_data.get("EURCHF", 0),
                )
                out["feat_jpy_index"] = cm.jpy_index(
                    cross_pair_data.get("USDJPY", 0),
                    cross_pair_data.get("EURJPY", 0),
                    cross_pair_data.get("GBPJPY", 0),
                    cross_pair_data.get("AUDJPY", 0),
                    cross_pair_data.get("CADJPY", 0),
                    cross_pair_data.get("NZDJPY", 0),
                    cross_pair_data.get("CHFJPY", 0),
                )
                out["feat_gbp_index"] = cm.gbp_index(
                    cross_pair_data.get("GBPUSD", 0),
                    cross_pair_data.get("EURGBP", 0),
                    cross_pair_data.get("GBPJPY", 0),
                    cross_pair_data.get("GBPAUD", 0),
                    cross_pair_data.get("GBPCAD", 0),
                    cross_pair_data.get("GBPCHF", 0),
                    cross_pair_data.get("GBPNZD", 0),
                )
                out["feat_aud_index"] = cm.aud_index(
                    cross_pair_data.get("AUDUSD", 0),
                    cross_pair_data.get("AUDJPY", 0),
                    cross_pair_data.get("EURAUD", 0),
                    cross_pair_data.get("GBPAUD", 0),
                    cross_pair_data.get("AUDCAD", 0),
                    cross_pair_data.get("AUDNZD", 0),
                    cross_pair_data.get("AUDCHF", 0),
                )
                # Triangular arbitrage drift on the canonical EURJPY triangle
                pip = 0.01 if pair.endswith("JPY") else 0.0001
                out["feat_triangular_arb_pips"] = cm.cross_correlation_window(
                    [], [], 0,
                ) and 0.0  # stub; recomputed below
                eu = cross_pair_data.get("EURUSD", 0)
                uj = cross_pair_data.get("USDJPY", 0)
                ej = cross_pair_data.get("EURJPY", 0)
                if eu > 0 and uj > 0 and ej > 0:
                    from . import fx_specific as fxs
                    out["feat_triangular_arb_pips"] = fxs.triangular_arb_drift(eu, uj, ej, pip_size=0.01)
            except Exception:
                pass

        # ─── Tier 3 — NETWORK features (from cache) ──────────────────
        if self.enable_network:
            cache = self.refresh_network_cache(force=False)
            # Direct copy of all feat_* keys in the cache
            for k, v in cache.items():
                if k.startswith("feat_"):
                    out[k] = v
            # Per-pair COT lookups
            cot = cache.get("_cot_snapshot", {})
            base, quote = pair[:3], pair[3:]
            if base in cot:
                out["feat_cot_base_net"] = cot[base].get("noncomm_net", 0)
            if quote in cot:
                out["feat_cot_quote_net"] = cot[quote].get("noncomm_net", 0)
            # Per-pair sentiment
            sent_per = cache.get("_sentiment_per_ccy", {})
            out["feat_news_sent_base"] = sent_per.get(base, 0.0)
            out["feat_news_sent_quote"] = sent_per.get(quote, 0.0)
            # Per-pair calendar
            try:
                from . import calendar as cal
                sec_to_next, next_event = cal.time_to_next_event(timestamp_utc, [base, quote], "High")
                out["feat_minutes_to_next_high_event"] = (
                    sec_to_next / 60.0 if sec_to_next > 0 else -1.0
                )
                out["feat_next_event_title"] = next_event["title"] if next_event else ""
                blackout, _ev = cal.is_news_blackout(timestamp_utc, [base, quote])
                out["feat_news_blackout"] = blackout
                out["feat_events_today_count"] = len(cal.events_today(timestamp_utc, [base, quote]))
            except Exception:
                pass

        return out

    # Backward-compat alias for the prior method name
    def compute_tier1_for_entry(self, *args, **kwargs):
        return self.compute_entry_features(*args, **kwargs)

    # ──────────────────────────────────────────────────────────────
    # Network features (lazy-loaded)
    # ──────────────────────────────────────────────────────────────

    def _fetch_network_features(self, pair: str, timestamp_utc: int) -> Dict[str, Any]:
        """Fetch yields, market data, calendar, sentiment, COT. Catches exceptions."""
        out: Dict[str, Any] = {}
        # Yields
        try:
            from . import yields
            curve = yields.yield_curve_slope(self.fred_api_key)
            out.update({f"yield_{k}": v for k, v in curve.items()})
            out["us_real_10y"] = yields.real_yields(self.fred_api_key).get("us_real_10y", 0)
            credit = yields.credit_spreads(self.fred_api_key)
            out.update(credit)
        except Exception:
            pass
        # Market data (Yahoo)
        try:
            from . import market_data
            out["macro_vix"] = market_data.vix_close()
            out["macro_gold"] = market_data.gold_close()
            out["macro_wti"] = market_data.crude_oil_close()
            out["macro_sp500"] = market_data.sp500_close()
            out["macro_us_10y"] = market_data.us_10y_yield()
        except Exception:
            pass
        # Calendar
        try:
            from . import calendar as cal
            base, quote = pair[:3], pair[3:]
            sec_to_next, next_event = cal.time_to_next_event(timestamp_utc, [base, quote], "High")
            out["minutes_to_next_high_event"] = sec_to_next / 60 if sec_to_next > 0 else -1
            out["next_event_title"] = next_event["title"] if next_event else ""
            blackout, ev = cal.is_news_blackout(timestamp_utc, [base, quote])
            out["news_blackout"] = blackout
            out["events_today_count"] = len(cal.events_today(timestamp_utc, [base, quote]))
        except Exception:
            pass
        # Sentiment
        try:
            from . import sentiment
            sent_b = sentiment.aggregate_news_sentiment(pair[:3])
            sent_q = sentiment.aggregate_news_sentiment(pair[3:])
            out["news_sent_base"] = sent_b.get("avg_sentiment", 0)
            out["news_sent_quote"] = sent_q.get("avg_sentiment", 0)
            out["news_flow_rate"] = sentiment.news_flow_rate()
        except Exception:
            pass
        # COT
        try:
            from . import positioning
            cot = positioning.get_latest_cot_snapshot()
            base = pair[:3]
            quote = pair[3:]
            if base in cot:
                out[f"cot_{base}_net"] = cot[base].get("noncomm_net", 0)
            if quote in cot:
                out[f"cot_{quote}_net"] = cot[quote].get("noncomm_net", 0)
        except Exception:
            pass
        return out


# ─────────────────────────────────────────────────────────────────────
# extract_feat_dict — module-level helper for Phase C.3 shadow simulator
# (added 2026-05-05; do NOT modify compute_entry_features to use this)
# ─────────────────────────────────────────────────────────────────────

def extract_feat_dict(
    full_result: Dict[str, Any],
    cross_pair_data: Optional[Dict[str, Any]] = None,
    pair: str = "",
    timestamp_utc: int = 0,
) -> Dict[str, Any]:
    """Apply the canonical feat_* mapping to a compute_for_entry result.

    Pure function: takes the dict returned by FeatureEngine.compute_for_entry
    plus optional cross-pair context and returns the feat_*-prefixed dict
    that PaperTradeRecord and ShadowSignalRecord both consume.

    Used by ShadowSimulator's lazy feature recompute path (Phase C.3) where
    `compute_entry_features` cannot be called directly because it always
    fetches CURRENT MT5 bars via `mt5.copy_rates_from_pos(0, N)` — that
    would compute features against today's market for a yesterday's signal,
    poisoning Edge Miner historical analysis.

    The shadow path:
        1. Fetch HISTORICAL m1_bars from M1Cache (24h lookback before signal_time)
        2. Resample to m15 + h1
        3. Call FeatureEngine.compute_for_entry(...) directly with these bars
        4. Apply this function to the result -> feat_* dict

    ── DUPLICATION NOTICE ──
    The body of this function is a duplicate of the inline mapping inside
    FeatureEngine.compute_entry_features (search there for the matching
    "DUPLICATION NOTICE"). The two MUST stay in sync — when adding a new
    feat_* key in either location, mirror it in the other.

    ── Sync verification command ──
    After editing either location, verify key counts match:
        python -c "from takumi_trader.features.feature_engine import extract_feat_dict; \
                   print('extract_feat_dict keys:', len(extract_feat_dict({})))"
    Then count "feat_" lines in the inline mapping inside
    compute_entry_features:
        grep -c '^\\s*\"feat_' takumi_trader/features/feature_engine.py
    Both should match (modulo extract_feat_dict's deliberate exclusion of
    network features and per-pair COT/sentiment lookups).

    Why duplicated: live PaperTrader code calls compute_entry_features
    directly and has been running cleanly in production for months. The
    architect's "do NOT modify compute_entry_features" rule preserves
    behavioral stability of live trading. A Phase F task tracks the
    eventual unification refactor (move both call-sites to use this
    helper), which requires its own validation cycle since it touches
    live trading code.

    ── Network features (Tier 3) NOT included ──
    Network features in compute_entry_features come from
    `self.refresh_network_cache()` which fetches Yahoo/FRED/RSS data NOW.
    For historical recompute, those values would also be wrong (they'd
    reflect today's VIX, today's news sentiment etc., not signal_time).
    Phase F may add a historical network-feature snapshot mechanism;
    until then, shadow records have empty network features.

    ── Volume-derived feature caveat ──
    M1Cache parquet schema is (time, open, high, low, close) — NO
    tick_volume. compute_for_entry's defensive np.ones(len) fallback
    means CVD, Amihud illiquidity, and Kyle's lambda compute against
    synthetic constant-volume data. Edge Miner queries should filter
    these out (or treat them as low-fidelity) for shadow-only records.
    """
    full = full_result
    out: Dict[str, Any] = {
        # ─── Tier 1 (already shipped) ────────────────────────────
        "feat_cvd_30m": full.get("cvd_30m_m1", 0.0),
        "feat_cvd_divergent": full.get("cvd_divergent_30m", False),
        "feat_cvd_price_move_pips": full.get("cvd_30m_price_move_pips", 0.0),
        "feat_amihud_illiq_60m": full.get("amihud_illiq_60m", 0.0),
        "feat_kyle_lambda_60m": full.get("kyle_lambda_60m", 0.0),
        "feat_m15_atr14_pct_rank": full.get("m15_atr14_pct_rank", 0.0),
        "feat_h1_atr14_pct_rank": full.get("h1_atr14_pct_rank", 0.0),
        "feat_m15_jump_detected": full.get("m15_jump_detected", False),
        "feat_m15_yang_zhang": full.get("m15_yang_zhang", 0.0),
        "feat_m15_realized_skew": full.get("m15_realized_skew", 0.0),
        "feat_m15_realized_kurt": full.get("m15_realized_kurt", 0.0),
        "feat_h1_adx": full.get("h1_adx", 0.0),
        "feat_h1_choppiness": full.get("h1_choppiness", 0.0),
        "feat_h1_hurst": full.get("h1_hurst", 0.5),
        "feat_h1_kaufman_er": full.get("h1_kaufman_er", 0.0),
        "feat_h1_regime": full.get("h1_regime", ""),
        "feat_h1_donchian_pos": full.get("h1_donchian_pos", 0.5),
        "feat_h1_aroon_osc": full.get("h1_aroon_osc", 0.0),
        "feat_h1_acf_lag_1": full.get("h1_acf_lag_1", 0.0),
        "feat_h1_half_life_bars": full.get("h1_half_life", 0.0),
        "feat_csi_dispersion": full.get("csi_dispersion", 0.0),
        "feat_csi_strong_count": full.get("csi_strong_count", 0),
        "feat_csi_weak_count": full.get("csi_weak_count", 0),
        "feat_dxy_synthetic": full.get("dxy_synthetic", 0.0),
        "feat_dist_to_50_pips": full.get("dist_to_nearest_50_pips", 0.0),
        "feat_dist_to_big_figure_pips": full.get("dist_to_big_figure_pips", 0.0),
        "feat_h1_poc": full.get("h1_poc", 0.0),
        "feat_h1_stop_hunt_score": full.get("h1_stop_hunt_score", 0.0),
        "feat_h1_sweep_type": full.get("h1_sweep_type", ""),
        "feat_is_month_end": full.get("is_month_end", False),
        "feat_in_london_fix": full.get("in_london_fix", False),
        "feat_in_ecb_fix": full.get("in_ecb_fix", False),
        # ─── Tier 2 — Volatility additions (M15) ─────────────────
        "feat_m15_realized_var": full.get("m15_realized_var", 0.0),
        "feat_m15_parkinson": full.get("m15_parkinson", 0.0),
        "feat_m15_garman_klass": full.get("m15_garman_klass", 0.0),
        "feat_m15_rogers_satchell": full.get("m15_rogers_satchell", 0.0),
        "feat_m15_bipower": full.get("m15_bipower", 0.0),
        "feat_m15_vol_ratio": full.get("m15_vol_ratio", 0.0),
        "feat_m15_vol_of_vol": full.get("m15_vol_of_vol", 0.0),
        "feat_m15_bb_upper": full.get("m15_bb_upper", 0.0),
        "feat_m15_bb_lower": full.get("m15_bb_lower", 0.0),
        "feat_m15_bb_width_pips": full.get("m15_bb_width_pips", 0.0),
        "feat_m15_kc_upper": full.get("m15_kc_upper", 0.0),
        "feat_m15_kc_lower": full.get("m15_kc_lower", 0.0),
        "feat_m15_bbkc_ratio": full.get("m15_bbkc_ratio", 1.0),
        # Volatility additions (H1)
        "feat_h1_realized_var": full.get("h1_realized_var", 0.0),
        "feat_h1_parkinson": full.get("h1_parkinson", 0.0),
        "feat_h1_yang_zhang": full.get("h1_yang_zhang", 0.0),
        "feat_h1_atr14_pips": full.get("h1_atr14_pips", 0.0),
        "feat_h1_vol_ratio": full.get("h1_vol_ratio", 0.0),
        # Regimes additions
        "feat_h1_plus_di": full.get("h1_plus_di", 0.0),
        "feat_h1_minus_di": full.get("h1_minus_di", 0.0),
        "feat_h1_aroon_up": full.get("h1_aroon_up", 0.0),
        "feat_h1_aroon_down": full.get("h1_aroon_down", 0.0),
        "feat_h1_vortex_plus": full.get("h1_vortex_plus", 0.0),
        "feat_h1_vortex_minus": full.get("h1_vortex_minus", 0.0),
        "feat_h1_kama": full.get("h1_kama", 0.0),
        "feat_h1_supertrend_value": full.get("h1_supertrend_value", 0.0),
        "feat_h1_supertrend_dir": full.get("h1_supertrend_dir", 0),
        "feat_h1_ichimoku_tenkan": full.get("h1_ichimoku_tenkan", 0.0),
        "feat_h1_ichimoku_kijun": full.get("h1_ichimoku_kijun", 0.0),
        "feat_h1_ichimoku_senkou_a": full.get("h1_ichimoku_senkou_a", 0.0),
        "feat_h1_ichimoku_senkou_b": full.get("h1_ichimoku_senkou_b", 0.0),
        "feat_h1_ichimoku_above_cloud": full.get("h1_ichimoku_above_cloud", False),
        "feat_h1_ichimoku_in_cloud": full.get("h1_ichimoku_in_cloud", False),
        "feat_h1_ichimoku_below_cloud": full.get("h1_ichimoku_below_cloud", False),
        "feat_h1_lr_slope": full.get("h1_lr_slope", 0.0),
        "feat_h1_lr_r2": full.get("h1_lr_r2", 0.0),
        "feat_h1_dfa": full.get("h1_dfa", 0.5),
        "feat_h1_trend_persistence": full.get("h1_trend_persistence", 0.0),
        "feat_h1_mr_zscore": full.get("h1_mr_zscore", 0.0),
        # Statistics additions
        "feat_h1_acf_lag_5": full.get("h1_acf_lag_5", 0.0),
        "feat_h1_acf_lag_15": full.get("h1_acf_lag_15", 0.0),
        "feat_h1_pacf_lag_1": full.get("h1_pacf_lag_1", 0.0),
        "feat_h1_pacf_lag_5": full.get("h1_pacf_lag_5", 0.0),
        "feat_h1_skew_60": full.get("h1_skew_60", 0.0),
        "feat_h1_kurt_60": full.get("h1_kurt_60", 0.0),
        "feat_h1_fft_period_min": full.get("h1_dominant_period_min", 0.0),
        "feat_h1_fft_amplitude_ratio": full.get("h1_amplitude_ratio", 0.0),
        # CSI deltas
        "feat_dUSD": full.get("dUSD", 0.0),
        "feat_dEUR": full.get("dEUR", 0.0),
        "feat_dGBP": full.get("dGBP", 0.0),
        "feat_dJPY": full.get("dJPY", 0.0),
        "feat_dCAD": full.get("dCAD", 0.0),
        "feat_dAUD": full.get("dAUD", 0.0),
        "feat_dNZD": full.get("dNZD", 0.0),
        "feat_dCHF": full.get("dCHF", 0.0),
        # Cross-market additions
        "feat_carry_pips_per_day": full.get("carry_pips_per_day", 0.0),
        # Levels additions
        "feat_dist_to_25_pips": full.get("dist_to_nearest_25_pips", 0.0),
        "feat_dist_to_century_pips": full.get("dist_to_century_pips", 0.0),
        "feat_prev_day_open": full.get("prev_day_open", 0.0),
        "feat_prev_day_high": full.get("prev_day_high", 0.0),
        "feat_prev_day_low": full.get("prev_day_low", 0.0),
        "feat_prev_day_close": full.get("prev_day_close", 0.0),
        "feat_prev_week_high": full.get("prev_week_high", 0.0),
        "feat_prev_week_low": full.get("prev_week_low", 0.0),
        "feat_prev_month_high": full.get("prev_month_high", 0.0),
        "feat_prev_month_low": full.get("prev_month_low", 0.0),
        "feat_year_high": full.get("year_high", 0.0),
        "feat_year_low": full.get("year_low", 0.0),
        "feat_asian_session_high": full.get("asian_session_high", 0.0),
        "feat_asian_session_low": full.get("asian_session_low", 0.0),
        "feat_asian_session_range_pips": full.get("asian_session_range_pips", 0.0),
        "feat_london_session_high": full.get("london_session_high", 0.0),
        "feat_london_session_low": full.get("london_session_low", 0.0),
        "feat_pivot_pp": full.get("pivot_pp", 0.0),
        "feat_pivot_r1": full.get("pivot_r1", 0.0),
        "feat_pivot_r2": full.get("pivot_r2", 0.0),
        "feat_pivot_s1": full.get("pivot_s1", 0.0),
        "feat_pivot_s2": full.get("pivot_s2", 0.0),
        "feat_fib_pp": full.get("fib_pp", 0.0),
        "feat_fib_r1": full.get("fib_r1", 0.0),
        "feat_fib_s1": full.get("fib_s1", 0.0),
        "feat_cam_r3": full.get("cam_r3", 0.0),
        "feat_cam_s3": full.get("cam_s3", 0.0),
        "feat_h1_vwap": full.get("h1_vwap", 0.0),
        "feat_h1_session_vwap": full.get("h1_session_vwap", 0.0),
        "feat_h1_vah": full.get("h1_vah", 0.0),
        "feat_h1_val": full.get("h1_val", 0.0),
        # Patterns
        "feat_h1_equal_highs": full.get("h1_equal_highs", False),
        "feat_h1_equal_lows": full.get("h1_equal_lows", False),
        "feat_h1_trendline_break": full.get("h1_trendline_break", ""),
        "feat_h1_candle_pattern": full.get("h1_candle_pattern", ""),
        "feat_fvg_count": full.get("fvg_count", 0),
        "feat_order_block_count": full.get("order_block_count", 0),
        "feat_h1_liquidity_void_count": full.get("h1_liquidity_void_count", 0),
        # Adversarial additions
        "feat_h1_round_magnetism": full.get("h1_magnet_score", 0.0),
        "feat_h1_tick_burst_z": full.get("tick_burst_z_score", 0.0),
        # FX-specific additions
        "feat_in_tokyo_fix": full.get("in_tokyo_fix", False),
        "feat_dst_active_uk": full.get("dst_active_uk", False),
        "feat_dst_active_us": full.get("dst_active_us", False),
        "feat_holiday_label": full.get("holiday_label", ""),
        "feat_session_label": full.get("session_jst", ""),
        # Behavioral
        "feat_friday_late": full.get("is_friday_late", False),
        "feat_sunday_open": full.get("is_sunday_open", False),
        "feat_lunch_hour": full.get("lunch_hour", ""),
        "feat_days_into_quarter": full.get("days_into_quarter", 0),
        # Schema
        "feat_schema_version": 2,
    }

    # Synthetic currency baskets (require cross-pair data — same as
    # compute_entry_features post-processing).
    if cross_pair_data:
        try:
            from . import cross_market as cm
            out["feat_eur_index"] = cm.eur_index(
                cross_pair_data.get("EURUSD", 0),
                cross_pair_data.get("EURJPY", 0),
                cross_pair_data.get("EURGBP", 0),
                cross_pair_data.get("EURAUD", 0),
                cross_pair_data.get("EURCAD", 0),
                cross_pair_data.get("EURCHF", 0),
            )
            out["feat_jpy_index"] = cm.jpy_index(
                cross_pair_data.get("USDJPY", 0),
                cross_pair_data.get("EURJPY", 0),
                cross_pair_data.get("GBPJPY", 0),
                cross_pair_data.get("AUDJPY", 0),
                cross_pair_data.get("CADJPY", 0),
                cross_pair_data.get("NZDJPY", 0),
                cross_pair_data.get("CHFJPY", 0),
            )
            out["feat_gbp_index"] = cm.gbp_index(
                cross_pair_data.get("GBPUSD", 0),
                cross_pair_data.get("EURGBP", 0),
                cross_pair_data.get("GBPJPY", 0),
                cross_pair_data.get("GBPAUD", 0),
                cross_pair_data.get("GBPCAD", 0),
                cross_pair_data.get("GBPCHF", 0),
                cross_pair_data.get("GBPNZD", 0),
            )
            out["feat_aud_index"] = cm.aud_index(
                cross_pair_data.get("AUDUSD", 0),
                cross_pair_data.get("AUDJPY", 0),
                cross_pair_data.get("EURAUD", 0),
                cross_pair_data.get("GBPAUD", 0),
                cross_pair_data.get("AUDCAD", 0),
                cross_pair_data.get("AUDNZD", 0),
                cross_pair_data.get("AUDCHF", 0),
            )
            # Triangular arb on EURJPY canonical triangle
            eu = cross_pair_data.get("EURUSD", 0)
            uj = cross_pair_data.get("USDJPY", 0)
            ej = cross_pair_data.get("EURJPY", 0)
            if eu > 0 and uj > 0 and ej > 0:
                from . import fx_specific as fxs
                out["feat_triangular_arb_pips"] = fxs.triangular_arb_drift(
                    eu, uj, ej, pip_size=0.01,
                )
            else:
                out["feat_triangular_arb_pips"] = 0.0
        except Exception:
            # Defensive — cross-pair computation must never break the
            # feat_* dict assembly. If a basket calc fails, leave the
            # basket fields absent (caller filters or imputes).
            pass

    return out
