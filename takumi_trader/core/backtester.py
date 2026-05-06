"""Historical backtesting engine for the Forex Currency Strength Scanner.

Replays historical M1/M5/M15/H1 candle data from MT5 and simulates the live
alert + exit system. Walks forward through M1 candles bar-by-bar, computing
currency strengths using the same CalculationEngine as live mode, checking
alert conditions, and tracking simulated trades with entry/exit logic.

Usage:
    from takumi_trader.core.backtester import BacktestEngine, BacktestConfig

    config = BacktestConfig(days_back=30)
    engine = BacktestEngine(config, progress_callback=my_callback)
    outcomes = engine.run()
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable

import numpy as np

from takumi_trader.core.strength import (
    ALL_28_PAIRS,
    CURRENCIES,
    TIMEFRAME_LABELS,
    DISPLAY_PAIRS,
    CalculationEngine,
    compute_ema,
    compute_atr,
    ROC_LOOKBACK,
    WARMUP_BARS,
)
from takumi_trader.core.alert_performance import AlertOutcome
from takumi_trader.core.filter_engine import FilterEngine, FilterSettings
from takumi_trader.core.trade_tracker import pip_value

logger = logging.getLogger(__name__)

# ── Alert Thresholds (same as main_window.py) ────────────────────────

_ALERT_THRESHOLDS: dict[str, float] = {
    "M1": 6.5,
    "M5": 6.0,
    "M15": 5.5,
    "H1": 5.0,
}

_MIN_DIVERGENCE_SPREAD: float = 12.0
_EXIT_SPREAD_THRESHOLD: float = 4.0

_NUM_TF: int = len(TIMEFRAME_LABELS)

# Higher-timeframe set used for HTF composite scores
_HTF_ONLY: set[str] = {"M5", "M15", "H1"}

# ── Session Mapping (JST times) ─────────────────────────────────────

# DST-aware sessions: automatically adjusts based on date
# European DST: last Sunday of March → last Sunday of October
# US DST: 2nd Sunday of March → 1st Sunday of November

_SESSIONS_WINTER: list[tuple[tuple[int, int], tuple[int, int], str]] = [
    ((8, 0), (8, 44), "Australia"),
    ((8, 45), (9, 35), "Tokyo_open"),
    ((9, 36), (12, 8), "Morning"),
    ((12, 9), (15, 44), "Afternoon"),
    ((15, 45), (16, 25), "Frankfurt_open"),
    ((16, 26), (16, 44), "EU"),
    ((16, 45), (17, 35), "London_open"),
    ((17, 36), (20, 44), "London"),
    ((20, 45), (21, 35), "US_open"),
    ((21, 36), (23, 59), "US"),
    ((0, 0), (5, 0), "US"),
    ((5, 1), (7, 59), "NO_TRADE"),
]

_SESSIONS_SUMMER: list[tuple[tuple[int, int], tuple[int, int], str]] = [
    ((8, 0), (8, 44), "Australia"),
    ((8, 45), (9, 35), "Tokyo_open"),
    ((9, 36), (12, 8), "Morning"),
    ((12, 9), (14, 44), "Afternoon"),
    ((14, 45), (15, 25), "Frankfurt_open"),
    ((15, 26), (15, 44), "EU"),
    ((15, 45), (16, 35), "London_open"),
    ((16, 36), (20, 44), "London"),
    ((20, 45), (21, 35), "US_open"),
    ((21, 36), (23, 59), "US"),
    ((0, 0), (5, 0), "US"),
    ((5, 1), (7, 59), "NO_TRADE"),
]

def _is_european_summer(dt: datetime) -> bool:
    """Check if date falls in European Summer Time (last Sun Mar → last Sun Oct)."""
    year = dt.year
    # Last Sunday of March
    mar31 = datetime(year, 3, 31)
    dst_start = mar31 - timedelta(days=mar31.weekday() + 1) if mar31.weekday() != 6 else mar31
    # Last Sunday of October
    oct31 = datetime(year, 10, 31)
    dst_end = oct31 - timedelta(days=oct31.weekday() + 1) if oct31.weekday() != 6 else oct31
    return dst_start <= dt.replace(tzinfo=None) < dst_end

def _get_sessions_for_date(dt: datetime) -> list:
    """Return the correct session table based on European DST."""
    if _is_european_summer(dt):
        return _SESSIONS_SUMMER
    return _SESSIONS_WINTER

# Default for live system (updated manually via session_manager.py)
_SESSIONS = _SESSIONS_WINTER

_JST = timezone(timedelta(hours=9))

# ── Timeframe Constants ──────────────────────────────────────────────

# MT5 timeframe enum values
_MT5_TIMEFRAMES: dict[str, int] = {
    "M1": 1,       # mt5.TIMEFRAME_M1
    "M5": 5,       # mt5.TIMEFRAME_M5
    "M15": 15,     # mt5.TIMEFRAME_M15
    "H1": 16385,   # mt5.TIMEFRAME_H1
}

# Seconds per candle for each timeframe
_TF_SECONDS: dict[str, int] = {
    "M1": 60,
    "M5": 300,
    "M15": 900,
    "H1": 3600,
}

# Minutes per candle for calculating fetch bar counts
_TF_MINUTES: dict[str, int] = {
    "M1": 1,
    "M5": 5,
    "M15": 15,
    "H1": 60,
}

# How many bars to pass to compute() for each TF
_COMPUTE_FETCH_BARS: dict[str, int] = {
    "M1": 150,
    "M5": 50,
    "M15": 50,
    "H1": 50,
}

# Post-exit observation period in M1 bars (4 hours)
_POST_EXIT_M1_BARS: int = 240

# Safety cap: force exit after 8 hours (in M1 bars)
_MAX_TRACKING_M1_BARS: int = 480


# ── Configuration ────────────────────────────────────────────────────

@dataclass
class CalcParams:
    """Tunable calculation parameters for strategy optimization.

    These override the defaults in strength.py when passed to the backtester.
    """
    # EMA / ATR periods — defaults match live system (A+B+C combo)
    ema_period: int = 4              # EMA4 for faster reaction
    atr_period: int = 14

    # Weighted Micro-ROC
    roc_lookback_m1: int = 10
    roc_lookback_m5: int = 8
    roc_lookback_m15: int = 6
    roc_lookback_h1: int = 5
    roc_decay: float = 0.2           # exp(-decay * i) — matches live

    # Composite score weights (displacement, roc, tick_velocity)
    weight_disp_m1: float = 0.35
    weight_roc_m1: float = 0.35
    weight_tick_m1: float = 0.30

    # Z-score sensitivity (tanh scaling)
    sensitivity: float = 1.0

    # Alert thresholds per timeframe
    threshold_m1: float = 6.5
    threshold_m5: float = 6.0
    threshold_m15: float = 5.5
    threshold_h1: float = 5.0

    # Min divergence spread (base - quote) to trigger alert
    min_divergence_spread: float = 12.0

    # Momentum acceleration threshold
    accel_threshold: float = 0.1

    # Require momentum accelerating at entry (anti-late-entry filter)
    require_acceleration: bool = False


@dataclass
class BacktestConfig:
    """Configuration for the backtest engine."""

    days_back: int = 30
    start_date: str = ""           # Optional "YYYY-MM-DD" start date (overrides days_back)
    use_dukascopy: bool = False    # Use local Dukascopy data instead of MT5
    post_exit_hours: float = 4.0
    exit_spread_threshold: float = 4.0
    use_htf_exit: bool = True  # Use M5+M15+H1 for exit detection instead of M1

    # Conviction filter toggles (match FilterSettings)
    filter_htf: bool = False       # HTF Trend Regime (H4/D1 alignment)
    filter_vel: bool = True        # Strength Velocity
    filter_isol: bool = True       # Isolation Score
    filter_structural: bool = True  # Structural: Key Level Proximity + TP Clearance
    conviction_threshold: int = 50 # Minimum conviction to take a trade

    # RED news hard block (pre-30min / post-60min around HIGH impact events)
    filter_news: bool = True   # ON by default (matches live)

    # Block entries during NO_TRADE session (5:01-7:57 JST, matches live)
    filter_no_trade_session: bool = True

    # Simulate SL/TP exits during backtest (matches live paper_trader)
    # OFF during optimization (need full MAE/MFE data for grid search)
    # ON during validation runs (results match live reality)
    simulate_sltp: bool = False

    # Allow re-entry on same pair+direction within the same session after trade closes
    allow_session_reentry: bool = True

    # Acceleration-based early entries (lower threshold, momentum-driven)
    use_accel_entry: bool = False
    accel_min_velocity: float = 1.5    # min velocity (1st derivative) per currency
    accel_min_spread: float = 6.0      # min composite spread for accel entries
    accel_min_htf_agree: int = 3       # min HTFs (M5/M15/H1) with matching velocity (3 = ALL must agree)

    # Calculation parameter overrides
    calc_params: CalcParams | None = None

    # Single-pair mode (None = all pairs)
    single_pair: str | None = None


# ── Active Trade State ───────────────────────────────────────────────

@dataclass
class _ActiveTrade:
    """Internal state for a trade being tracked during backtest."""

    outcome: AlertOutcome
    entry_bar_idx: int
    exit_bar_idx: int | None = None       # M1 bar index when exit fired
    post_exit_started: bool = False
    force_completed: bool = False


# ── Backtest Engine ──────────────────────────────────────────────────

class BacktestEngine:
    """Historical backtesting engine.

    Replays M1 candle data bar-by-bar, computing currency strengths and
    checking alert/exit conditions using the same logic as the live system.

    Args:
        config: Backtest configuration parameters.
        progress_callback: Optional callback(current_bar, total_bars, trades_found)
            for UI progress updates. Called every 100 bars.
    """

    def __init__(
        self,
        config: BacktestConfig,
        progress_callback: Callable[[int, int, int], None] | None = None,
        prefetched_data: dict[str, dict[str, np.ndarray]] | None = None,
    ) -> None:
        self.config = config
        self._progress_callback = progress_callback
        self._prefetched_data = prefetched_data
        self._cp = config.calc_params or CalcParams()
        self._engine = CalculationEngine(
            sensitivity=self._cp.sensitivity,
            ema_period=self._cp.ema_period,
            atr_period=self._cp.atr_period,
            roc_decay=self._cp.roc_decay,
            roc_lookbacks={
                "M1": self._cp.roc_lookback_m1,
                "M5": self._cp.roc_lookback_m5,
                "M15": self._cp.roc_lookback_m15,
                "H1": self._cp.roc_lookback_h1,
            },
            weights_m1=(self._cp.weight_disp_m1, self._cp.weight_roc_m1, self._cp.weight_tick_m1),
        )

        # Set up conviction filter engine
        filter_settings = FilterSettings(
            trend_regime_enabled=config.filter_htf,
            strength_velocity_enabled=config.filter_vel,
            isolation_enabled=config.filter_isol,
            structural_enabled=config.filter_structural,
            conviction_full_threshold=config.conviction_threshold,
            conviction_dimmed_threshold=max(30, config.conviction_threshold - 20),
        )
        self._filter_engine = FilterEngine(filter_settings)

        # RED news hard block
        self._news_filter = None
        if config.filter_news:
            from takumi_trader.core.news_filter import NewsFilter
            nf = NewsFilter()
            if nf.load_cache():
                self._news_filter = nf
                logger.info("News filter active: %d RED events cached", nf.event_count)
            else:
                logger.warning("News filter enabled but no cache found — filter skipped")

        # Velocity tracking for filter engine
        self._velocity_history: dict[str, list[float]] = {ccy: [] for ccy in CURRENCIES}
        self._velocity_data: dict[str, tuple[float, bool]] = {}

        # Cached TF results between updates
        self._cached_tf_results: dict[str, Any] = {}

        # Stoch v2 engine (QM4-style currency strength)
        from takumi_trader.core.stoch_engine import StochStrengthEngine
        self._stoch_engine = StochStrengthEngine()
        self._stoch_cached: dict[str, dict[str, np.ndarray]] = {}  # tf -> {pair: candles}

        # Active trades being tracked
        self._active_trades: list[_ActiveTrade] = []
        self._completed: list[AlertOutcome] = []
        # Counter-momentum exit confirmation counts per pair
        self._exit_confirm_counts: dict[str, int] = {}

        # Session keys to prevent duplicate alerts (pair+direction+session)
        self._session_keys: set[str] = set()
        self._last_session_date: int = 0  # track date for session key reset

    def fetch_data(self) -> dict[str, dict[str, np.ndarray]]:
        """Public method to fetch data once for reuse across multiple runs."""
        return self._fetch_data()

    def run(self) -> list[AlertOutcome]:
        """Run the full backtest.

        Returns:
            List of completed AlertOutcome objects with all fields populated.
        """
        logger.info(
            "Starting backtest: %d days back, use_htf_exit=%s",
            self.config.days_back, self.config.use_htf_exit,
        )

        # 1. Fetch historical data (or reuse prefetched)
        if self._prefetched_data is not None:
            data = self._prefetched_data
        else:
            data = self._fetch_data()
        if not data:
            raise RuntimeError(
                "No data fetched. Check your data source:\n"
                "• MT5: Make sure MetaTrader 5 is running and connected\n"
                "• Dukascopy: Click 'Download' or 'Update All' first"
            )

        # Get the M1 candle arrays — use the first available pair to determine length
        m1_ref_pair = next(
            (p for p in ALL_28_PAIRS if "M1" in data.get(p, {})), None
        )
        if m1_ref_pair is None:
            raise RuntimeError(
                "No M1 candle data available for any pair.\n"
                "If using Dukascopy, make sure data files exist in data/dukascopy/"
            )

        # Check which pairs have data and which are missing
        pairs_with_data = [p for p in ALL_28_PAIRS if "M1" in data.get(p, {})]
        pairs_missing = [p for p in ALL_28_PAIRS if p not in pairs_with_data]
        if pairs_missing:
            logger.warning(
                "Missing M1 data for %d pairs: %s",
                len(pairs_missing), ", ".join(pairs_missing[:5]),
            )

        m1_length = len(data[m1_ref_pair]["M1"])
        if m1_length <= WARMUP_BARS:
            raise RuntimeError(
                f"Not enough M1 bars for backtest: {m1_length} bars found, "
                f"need at least {WARMUP_BARS + 1}.\n"
                f"Try a shorter period or download more data."
            )

        # 2. Bootstrap the CalculationEngine with first WARMUP_BARS
        warmup_data: dict[str, dict[str, np.ndarray]] = {}
        for pair in ALL_28_PAIRS:
            warmup_data[pair] = {}
            for tf in TIMEFRAME_LABELS:
                candles = data.get(pair, {}).get(tf)
                if candles is not None and len(candles) >= WARMUP_BARS:
                    warmup_data[pair][tf] = candles[:WARMUP_BARS]
                elif candles is not None:
                    warmup_data[pair][tf] = candles

        self._engine.bootstrap(warmup_data)
        logger.info("Bootstrap complete, starting simulation at bar %d", WARMUP_BARS)

        # 3. Walk forward through remaining M1 bars
        total_bars = m1_length - WARMUP_BARS
        simulation_start = WARMUP_BARS

        for bar_offset in range(total_bars):
            m1_idx = simulation_start + bar_offset

            self._process_bar(m1_idx, data)

            # Progress callback every 100 bars + cancellation check
            if self._progress_callback and bar_offset % 100 == 0:
                trades_found = len(self._completed) + len(self._active_trades)
                self._progress_callback(bar_offset, total_bars, trades_found)
                # Check if cancelled by UI
                if hasattr(self, '_cancel_flag') and self._cancel_flag._cancelled:
                    logger.info("Backtest cancelled by user at bar %d/%d", bar_offset, total_bars)
                    return self._completed

        # Force-complete any remaining active trades
        for trade in self._active_trades:
            self._force_complete_trade(trade, data, m1_length - 1)

        self._completed.extend(
            t.outcome for t in self._active_trades if t.outcome.completed
        )

        # Final progress callback
        if self._progress_callback:
            self._progress_callback(total_bars, total_bars, len(self._completed))

        logger.info("Backtest complete: %d trades found", len(self._completed))
        return self._completed

    # ── Data Fetching ────────────────────────────────────────────

    def _fetch_data(self) -> dict[str, dict[str, np.ndarray]]:
        """Fetch historical candles for all pairs and timeframes.

        Uses either MT5 live data or local Dukascopy Parquet files
        depending on config.use_dukascopy.

        Returns:
            Nested dict: {pair: {tf_label: structured_numpy_array}}.
        """
        if self.config.use_dukascopy:
            return self._fetch_dukascopy_data()
        return self._fetch_mt5_data()

    def _fetch_dukascopy_data(self) -> dict[str, dict[str, np.ndarray]]:
        """Fetch data from local Dukascopy Parquet files.

        M1 data comes from Dukascopy. M5/M15/H1 are resampled from M1.
        """
        import sys
        from takumi_trader.core.dukascopy_downloader import DukascopyDownloader

        if getattr(sys, 'frozen', False):
            data_dir = Path(sys.executable).parent / "data" / "dukascopy"
        else:
            data_dir = Path(__file__).resolve().parent.parent.parent / "data" / "dukascopy"

        dl = DukascopyDownloader(data_dir)
        result: dict[str, dict[str, np.ndarray]] = {}

        for pair in ALL_28_PAIRS:
            m1 = dl.load_pair(pair)
            if m1 is None or len(m1) == 0:
                logger.warning("No Dukascopy data for %s", pair)
                continue

            # Filter by date range if specified
            if self.config.start_date:
                start_dt = datetime.strptime(self.config.start_date, "%Y-%m-%d")
                start_ts = int(start_dt.timestamp())
                # Keep warmup bars before start
                warmup_seconds = WARMUP_BARS * 60  # M1 = 60 seconds each
                mask = m1["time"] >= (start_ts - warmup_seconds)
                m1 = m1[mask]
            elif self.config.days_back > 0:
                cutoff_ts = int((datetime.now() - timedelta(days=self.config.days_back + 1)).timestamp())
                cutoff_ts -= WARMUP_BARS * 60
                mask = m1["time"] >= cutoff_ts
                m1 = m1[mask]

            if len(m1) == 0:
                continue

            result[pair] = {"M1": m1}

            # Resample M1 → M5, M15, H1
            for tf_label, tf_seconds in [("M5", 300), ("M15", 900), ("H1", 3600)]:
                resampled = self._resample_ohlc(m1, tf_seconds)
                if resampled is not None and len(resampled) > 0:
                    result[pair][tf_label] = resampled

        logger.info(
            "Dukascopy data loaded: %d pairs with data",
            sum(1 for p in result if result[p]),
        )
        return result

    @staticmethod
    def _resample_ohlc(m1_data: np.ndarray, period_seconds: int) -> np.ndarray | None:
        """Resample M1 OHLCV data to a higher timeframe."""
        if len(m1_data) == 0:
            return None

        times = m1_data["time"]
        # Align to period boundaries
        aligned = (times // period_seconds) * period_seconds

        # Find unique period starts
        unique_periods = np.unique(aligned)
        n = len(unique_periods)
        if n == 0:
            return None

        dt = m1_data.dtype
        out = np.zeros(n, dtype=dt)

        for i, period_start in enumerate(unique_periods):
            mask = aligned == period_start
            group = m1_data[mask]
            out[i]["time"] = period_start
            out[i]["open"] = group["open"][0]
            out[i]["high"] = np.max(group["high"])
            out[i]["low"] = np.min(group["low"])
            out[i]["close"] = group["close"][-1]
            out[i]["tick_volume"] = np.sum(group["tick_volume"])

        return out

    def _fetch_mt5_data(self) -> dict[str, dict[str, np.ndarray]]:
        """Fetch data from MT5 live broker connection.

        MT5 brokers typically keep limited M1 history (1-3 months).
        Falls back to maximum available if requested range is too old.

        Returns:
            Nested dict: {pair: {tf_label: structured_numpy_array}}.
        """
        import MetaTrader5 as mt5

        if not mt5.initialize():
            logger.error("Failed to initialize MT5")
            return {}

        # Determine date range
        use_date_range = bool(self.config.start_date)
        if use_date_range:
            date_from = datetime.strptime(self.config.start_date, "%Y-%m-%d")
            date_to = datetime.now()
            logger.info("Fetching data from %s to now", self.config.start_date)

        result: dict[str, dict[str, np.ndarray]] = {}

        for pair in ALL_28_PAIRS:
            result[pair] = {}
            for tf_label, mt5_tf in _MT5_TIMEFRAMES.items():
                minutes_per_bar = _TF_MINUTES[tf_label]
                rates = None

                if use_date_range:
                    # Add warmup period before the start date
                    warmup_delta = timedelta(minutes=minutes_per_bar * WARMUP_BARS)
                    fetch_from = date_from - warmup_delta
                    rates = mt5.copy_rates_range(pair, mt5_tf, fetch_from, date_to)

                    # Fallback for M1: if date range returned nothing,
                    # fetch maximum available bars
                    if (rates is None or len(rates) == 0) and tf_label == "M1":
                        # Try fetching all available M1 data (max ~100k bars)
                        rates = mt5.copy_rates_from_pos(pair, mt5_tf, 0, 100_000)
                        if rates is not None and len(rates) > 0:
                            first_ts = datetime.utcfromtimestamp(rates[0]["time"])
                            logger.info(
                                "M1 fallback for %s: got %d bars from %s",
                                pair, len(rates), first_ts.strftime("%Y-%m-%d"),
                            )
                else:
                    bars_per_day = (24 * 60) // minutes_per_bar
                    num_bars = self.config.days_back * bars_per_day + WARMUP_BARS
                    rates = mt5.copy_rates_from_pos(pair, mt5_tf, 0, num_bars)

                if rates is not None and len(rates) > 0:
                    result[pair][tf_label] = rates
                else:
                    logger.warning("No %s data for %s", tf_label, pair)

        # Log actual M1 date range
        m1_starts = []
        for pair in ALL_28_PAIRS:
            m1 = result.get(pair, {}).get("M1")
            if m1 is not None and len(m1) > 0:
                m1_starts.append(datetime.utcfromtimestamp(m1[0]["time"]))
        if m1_starts:
            earliest = min(m1_starts)
            latest_start = max(m1_starts)
            logger.info(
                "M1 data range: earliest=%s, latest_start=%s",
                earliest.strftime("%Y-%m-%d"), latest_start.strftime("%Y-%m-%d"),
            )
            # Update days_back to reflect actual data if custom date was too far
            if use_date_range:
                actual_days = (datetime.now() - earliest).days
                if actual_days < self.config.days_back:
                    logger.warning(
                        "Requested %d days but M1 only available for %d days (from %s)",
                        self.config.days_back, actual_days,
                        earliest.strftime("%Y-%m-%d"),
                    )
                    self.config.days_back = actual_days

        logger.info(
            "Data fetch complete: %d pairs with data",
            sum(1 for p in result if result[p]),
        )
        return result

    # ── Main Simulation Step ─────────────────────────────────────

    def _process_bar(
        self,
        m1_idx: int,
        data: dict[str, dict[str, np.ndarray]],
    ) -> None:
        """Process a single M1 bar: compute strengths, check alerts, update trades."""

        # Get the M1 timestamp from a reference pair
        m1_time = self._get_m1_time(m1_idx, data)
        if m1_time is None:
            return

        # Reset session keys at the start of each new trading day (JST midnight)
        day_key = (m1_time + 9 * 3600) // 86400  # JST day number
        if day_key != self._last_session_date:
            self._session_keys.clear()
            self._last_session_date = day_key

        # Determine which timeframes have a new candle close at this M1 bar
        tfs_with_new_close = self._get_closed_timeframes(m1_time)

        # Compute strength for each TF with a new close
        for tf in tfs_with_new_close:
            candle_data = self._build_candle_data(tf, m1_time, m1_idx, data)
            if candle_data:
                tf_result = self._engine.compute(
                    candle_data, tf, update_zscore=True,
                )
                self._cached_tf_results[tf] = tf_result

        # Compute composite currency scores from all cached TF results
        composite_scores = self._compute_composite_scores()
        htf_composite_scores = self._compute_htf_composite_scores()

        if not composite_scores:
            return

        # Update velocity tracking for filter engine
        self._update_velocity(composite_scores)

        # Update momentum/acceleration tracking with per-TF scores
        tf_scores = {}
        for tf in _HTF_ONLY:
            tr = self._cached_tf_results.get(tf)
            if tr:
                tf_scores[tf] = tr.currency_scores
        self._engine.update_momentum(htf_composite_scores, tf_scores)

        # ── Stoch v2: compute scores on candle close for relevant TFs ──
        for tf in tfs_with_new_close:
            if tf in ("M1",):
                continue  # Skip M1 for stoch — too noisy
            # Build pair data for this TF at current bar
            stoch_pair_data: dict[str, np.ndarray] = {}
            for pair in ALL_28_PAIRS:
                tf_candles = data.get(pair, {}).get(tf)
                if tf_candles is None:
                    continue
                # Find HTF index for current M1 time
                htf_idx = self._m1_to_htf_index(m1_time, tf_candles)
                end = htf_idx + 1
                start = max(0, end - 30)  # last 30 bars
                if end > start:
                    stoch_pair_data[pair] = tf_candles[start:end]
            if stoch_pair_data:
                self._stoch_engine.compute_tf(stoch_pair_data, tf)

        # Update stoch velocity on M5 close
        if m1_time % 300 == 0:  # M5 close
            stoch_composite = self._stoch_engine.get_composite(["M5", "M15"])
            self._stoch_engine.update_velocity(stoch_composite)

        # Also compute H4/D1/W1 for stoch on their closes
        if m1_time % 14400 == 0:  # H4 close
            h4_data: dict[str, np.ndarray] = {}
            for pair in ALL_28_PAIRS:
                h4c = data.get(pair, {}).get("H4")
                if h4c is not None:
                    idx = self._m1_to_htf_index(m1_time, h4c)
                    start = max(0, idx - 29)
                    h4_data[pair] = h4c[start:idx + 1]
            if h4_data:
                self._stoch_engine.compute_tf(h4_data, "H4")

        if m1_time % 86400 == 0:  # D1 close
            d1_data: dict[str, np.ndarray] = {}
            for pair in ALL_28_PAIRS:
                d1c = data.get(pair, {}).get("D1")
                if d1c is not None:
                    idx = self._m1_to_htf_index(m1_time, d1c)
                    start = max(0, idx - 29)
                    d1_data[pair] = d1c[start:idx + 1]
            if d1_data:
                self._stoch_engine.compute_tf(d1_data, "D1")

        # Check alert conditions
        self._check_alerts(m1_idx, m1_time, data, composite_scores, htf_composite_scores)

        # Update active trades
        self._update_active_trades(
            m1_idx, m1_time, data, composite_scores, htf_composite_scores,
        )

    def _get_m1_time(
        self, m1_idx: int, data: dict[str, dict[str, np.ndarray]]
    ) -> int | None:
        """Get the unix timestamp of the M1 candle at the given index."""
        for pair in ALL_28_PAIRS:
            m1_candles = data.get(pair, {}).get("M1")
            if m1_candles is not None and m1_idx < len(m1_candles):
                return int(m1_candles[m1_idx]["time"])
        return None

    def _get_closed_timeframes(self, m1_time: int) -> list[str]:
        """Determine which TFs have a candle close at this M1 timestamp.

        A higher TF candle closes when the M1 timestamp is divisible by
        the TF's period in seconds.
        """
        closed: list[str] = ["M1"]  # M1 always closes
        for tf in ("M5", "M15", "H1"):
            if m1_time % _TF_SECONDS[tf] == 0:
                closed.append(tf)
        return closed

    def _build_candle_data(
        self,
        tf: str,
        m1_time: int,
        m1_idx: int,
        data: dict[str, dict[str, np.ndarray]],
    ) -> dict[str, np.ndarray]:
        """Build the candle_data dict for a single TF compute() call.

        For M1, slice directly using m1_idx.
        For higher TFs, find the corresponding index in the HTF array.

        Returns:
            Dict of {pair: candle_slice} suitable for CalculationEngine.compute().
        """
        fetch_bars = _COMPUTE_FETCH_BARS[tf]
        candle_data: dict[str, np.ndarray] = {}

        for pair in ALL_28_PAIRS:
            candles = data.get(pair, {}).get(tf)
            if candles is None or len(candles) == 0:
                continue

            if tf == "M1":
                # Slice directly using the M1 index
                end = m1_idx + 1
                start = max(0, end - fetch_bars)
                candle_data[pair] = candles[start:end]
            else:
                # Find the HTF index corresponding to the current M1 time
                htf_idx = self._m1_to_htf_index(m1_time, candles)
                end = htf_idx + 1
                start = max(0, end - fetch_bars)
                candle_data[pair] = candles[start:end]

        return candle_data

    def _m1_to_htf_index(self, m1_time: int, htf_candles: np.ndarray) -> int:
        """Find the index in htf_candles whose time <= m1_time.

        Uses binary search for efficiency.
        """
        times = htf_candles["time"]
        # Binary search: find rightmost index where time <= m1_time
        lo, hi = 0, len(times) - 1
        result = 0
        while lo <= hi:
            mid = (lo + hi) // 2
            if int(times[mid]) <= m1_time:
                result = mid
                lo = mid + 1
            else:
                hi = mid - 1
        return result

    # ── Composite Score Computation ──────────────────────────────

    def _compute_composite_scores(self) -> dict[str, float]:
        """Compute composite currency scores across all cached TF results.

        Composite = average of per-TF currency scores across all 4 timeframes.
        """
        composite: dict[str, float] = {}
        for ccy in CURRENCIES:
            total = 0.0
            count = 0
            for tf in TIMEFRAME_LABELS:
                tr = self._cached_tf_results.get(tf)
                if tr and ccy in tr.currency_scores:
                    total += tr.currency_scores[ccy]
                    count += 1
            if count > 0:
                composite[ccy] = total / count
        return composite

    def _compute_htf_composite_scores(self) -> dict[str, float]:
        """Compute HTF composite scores (M5+M15+H1 only)."""
        htf_composite: dict[str, float] = {}
        for ccy in CURRENCIES:
            total = 0.0
            count = 0
            for tf in _HTF_ONLY:
                tr = self._cached_tf_results.get(tf)
                if tr and ccy in tr.currency_scores:
                    total += tr.currency_scores[ccy]
                    count += 1
            if count > 0:
                htf_composite[ccy] = total / count
        return htf_composite

    # ── Velocity Tracking ─────────────────────────────────────────

    def _update_velocity(self, composite_scores: dict[str, float]) -> None:
        """Track velocity of composite scores for VEL filter."""
        for ccy in CURRENCIES:
            score = composite_scores.get(ccy, 0.0)
            history = self._velocity_history[ccy]
            history.append(score)
            if len(history) > 10:
                history.pop(0)

            if len(history) >= 3:
                # Velocity = average change per bar
                deltas = [history[i] - history[i - 1] for i in range(1, len(history))]
                velocity = sum(deltas) / len(deltas)
                is_fast = abs(velocity) > 0.6
                self._velocity_data[ccy] = (velocity, is_fast)
            else:
                self._velocity_data[ccy] = (0.0, False)

    # ── ATR Computation ────────────────────────────────────────────

    def _compute_h1_atr(
        self,
        pair: str,
        m1_time: int,
        data: dict[str, dict[str, np.ndarray]],
    ) -> float:
        """Compute H1 ATR(14) for a pair at the given M1 timestamp.

        Returns the raw ATR value (not in pips). Returns 0.0 if insufficient data.
        """
        h1_candles = data.get(pair, {}).get("H1")
        if h1_candles is None or len(h1_candles) < 15:
            return 0.0

        # Find the H1 candle index corresponding to this M1 time
        htf_idx = self._m1_to_htf_index(m1_time, h1_candles)
        if htf_idx < 14:
            return 0.0

        # Slice 14+1 candles ending at the current H1 bar
        start = max(0, htf_idx - 14)
        end = htf_idx + 1
        chunk = h1_candles[start:end]

        if len(chunk) < 15:
            return 0.0

        high = chunk["high"].astype(np.float64)
        low = chunk["low"].astype(np.float64)
        close = chunk["close"].astype(np.float64)

        atr_arr = compute_atr(high, low, close, period=14)
        return float(atr_arr[-1])

    def _get_h1_atr_pips(
        self, pair: str, m1_idx: int, data: dict[str, dict[str, np.ndarray]]
    ) -> float:
        """Get H1 ATR in pips at the current M1 bar."""
        m1 = data.get(pair, {}).get("M1")
        if m1 is None or m1_idx >= len(m1):
            return 0.0
        m1_time = int(m1[m1_idx]["time"])
        raw_atr = self._compute_h1_atr(pair, m1_time, data)
        if raw_atr <= 0:
            return 0.0
        pip = 0.01 if "JPY" in pair else 0.0001
        return raw_atr / pip

    def _compute_structural_data(
        self, pair: str, m1_idx: int, data: dict[str, dict[str, np.ndarray]]
    ) -> dict | None:
        """Compute key level data for the structural filter.

        Returns dict with prev_day_high/low, prev_week_high/low, pip value.
        Returns None if insufficient data.
        """
        m1 = data.get(pair, {}).get("M1")
        if m1 is None or m1_idx < 1440:
            return None

        pip = 0.01 if "JPY" in pair else 0.0001

        # Previous 24h high/low (look back 1440 M1 bars)
        day_start = max(0, m1_idx - 1440)
        day_slice = m1[day_start:m1_idx]
        if len(day_slice) < 100:
            return None

        prev_day_high = float(np.max(day_slice["high"]))
        prev_day_low = float(np.min(day_slice["low"]))

        # Previous 5 trading days high/low
        week_start = max(0, m1_idx - 7200)
        week_slice = m1[week_start:m1_idx]
        prev_week_high = float(np.max(week_slice["high"]))
        prev_week_low = float(np.min(week_slice["low"]))

        # Previous 20 trading days high/low (month)
        month_start = max(0, m1_idx - 28800)
        month_slice = m1[month_start:m1_idx]
        prev_month_high = float(np.max(month_slice["high"]))
        prev_month_low = float(np.min(month_slice["low"]))

        return {
            "prev_day_high": prev_day_high,
            "prev_day_low": prev_day_low,
            "prev_week_high": prev_week_high,
            "prev_week_low": prev_week_low,
            "prev_month_high": prev_month_high,
            "prev_month_low": prev_month_low,
            "pip": pip,
        }

    # ── Alert Condition Checking ─────────────────────────────────

    def _check_alerts(
        self,
        m1_idx: int,
        m1_time: int,
        data: dict[str, dict[str, np.ndarray]],
        composite_scores: dict[str, float],
        htf_composite_scores: dict[str, float],
    ) -> None:
        """Check all DISPLAY_PAIRS for alert conditions at the current bar."""

        # Skip if in NO_TRADE session window (5:01-7:57 JST, matches live)
        session = self._get_session(m1_time)
        if self.config.filter_no_trade_session and session == "NO_TRADE":
            return

        # Build per-currency per-TF scores from cached results
        ccy_per_tf: dict[str, dict[str, float]] = {ccy: {} for ccy in CURRENCIES}
        for tf in TIMEFRAME_LABELS:
            tr = self._cached_tf_results.get(tf)
            if tr:
                for ccy in CURRENCIES:
                    if ccy in tr.currency_scores:
                        ccy_per_tf[ccy][tf] = tr.currency_scores[ccy]

        pairs_to_check = [self.config.single_pair] if self.config.single_pair else DISPLAY_PAIRS
        for pair in pairs_to_check:
            # RED news hard block (currency-aware)
            if self._news_filter and self._news_filter.is_blackout(pair, m1_time):
                continue

            base, quote = pair[:3], pair[3:]
            base_scores = ccy_per_tf.get(base, {})
            quote_scores = ccy_per_tf.get(quote, {})

            if len(base_scores) < _NUM_TF or len(quote_scores) < _NUM_TF:
                continue

            # Composite spread
            base_composite = sum(base_scores.values()) / _NUM_TF
            quote_composite = sum(quote_scores.values()) / _NUM_TF
            spread = base_composite - quote_composite

            # Per-TF thresholds from CalcParams
            _thresholds = {
                "M1": self._cp.threshold_m1,
                "M5": self._cp.threshold_m5,
                "M15": self._cp.threshold_m15,
                "H1": self._cp.threshold_h1,
            }

            # BUY: base strong on all TFs, quote weak on all TFs
            base_strong_all = all(
                base_scores[tf] >= _thresholds[tf] for tf in TIMEFRAME_LABELS
            )
            quote_weak_all = all(
                quote_scores[tf] <= -_thresholds[tf] for tf in TIMEFRAME_LABELS
            )

            # SELL: base weak on all TFs, quote strong on all TFs
            base_weak_all = all(
                base_scores[tf] <= -_thresholds[tf] for tf in TIMEFRAME_LABELS
            )
            quote_strong_all = all(
                quote_scores[tf] >= _thresholds[tf] for tf in TIMEFRAME_LABELS
            )

            direction: str = ""
            entry_type: str = "stoch_v2"

            # ── Stoch v2 entry: QM4-style currency strength ──
            for try_dir in ("BUY", "SELL"):
                ok, reason = self._stoch_engine.check_entry(base, quote, try_dir)
                if ok:
                    direction = try_dir
                    break

            if not direction:
                continue

            # Anti-late-entry filter: require momentum still accelerating
            if self._cp.require_acceleration:
                strong_ccy_accel = base if direction == "BUY" else quote
                mom = self._engine._momentum_phases.get(strong_ccy_accel)
                if mom and mom.phase == "decelerating":
                    continue

            # Skip if already have an active trade on same pair+direction
            active_key = f"{pair}_{direction}"
            if any(
                f"{t.outcome.pair}_{t.outcome.direction}" == active_key
                for t in self._active_trades
            ):
                continue

            # Skip if already completed this pair+direction in this session
            # (disabled when allow_session_reentry=True)
            session_key = f"{pair}_{direction}_{session}"
            if not self.config.allow_session_reentry and session_key in self._session_keys:
                continue

            # Run conviction filter
            strong_ccy = base if direction == "BUY" else quote
            weak_ccy = quote if direction == "BUY" else base

            # Compute structural data (key levels) for this pair
            structural_data = self._compute_structural_data(pair, m1_idx, data)

            # Compute TP in pips for clearance check
            _pair_settings = None
            try:
                from takumi_trader.core.pair_algo_settings import get_pair_settings
                _pair_settings = get_pair_settings(pair)
            except Exception:
                pass
            _tp_pips = 0.0
            if _pair_settings:
                _tp_atr = _pair_settings.get("tp_atr", 0.5)
                _h1_atr_pips = self._get_h1_atr_pips(pair, m1_idx, data)
                if _h1_atr_pips > 0:
                    _tp_pips = _tp_atr * _h1_atr_pips

            entry_price_for_filter = self._get_close_price(pair, m1_idx, data) or 0.0

            conv_result = self._filter_engine.evaluate(
                strong_ccy=strong_ccy,
                weak_ccy=weak_ccy,
                pair=pair,
                direction=direction,
                htf_regimes=None,
                velocity_data=self._velocity_data if self._velocity_data else None,
                composite_scores=composite_scores,
                structural_data=structural_data,
                entry_price=entry_price_for_filter,
                tp_pips=_tp_pips,
            )

            # Skip if below conviction threshold
            if conv_result.conviction < self.config.conviction_threshold:
                continue

            # Fire alert: create AlertOutcome
            entry_price = self._get_close_price(pair, m1_idx, data)
            if entry_price is None:
                continue

            base_score = composite_scores.get(base, 0.0)
            quote_score = composite_scores.get(quote, 0.0)

            dt_jst = datetime.fromtimestamp(m1_time, tz=_JST)

            outcome = AlertOutcome(
                alert_id=f"{pair}_{m1_time}",
                pair=pair,
                direction=direction,
                entry_price=entry_price,
                entry_time=float(m1_time),
                entry_time_str=dt_jst.strftime("%Y-%m-%d %H:%M:%S"),
                conviction_score=conv_result.conviction,
                conviction_tier=conv_result.tier,
                session=session,
                base_score=round(base_score, 2),
                quote_score=round(quote_score, 2),
                strength_spread=round(abs(base_score - quote_score), 2),
                best_price=entry_price,
                worst_price=entry_price,
                last_update_time=float(m1_time),
                entry_type=entry_type,
            )

            # Compute H1 ATR(14) at entry time
            entry_atr = self._compute_h1_atr(pair, m1_time, data)
            pip = pip_value(pair)
            if entry_atr > 0:
                outcome.entry_atr_pips = round(entry_atr / pip, 1)

            # Compute SL/TP from pair_algo_settings (same as paper_trader)
            if _pair_settings and entry_atr > 0:
                _sl_atr = _pair_settings.get("sl_atr", 0.3)
                _tp_atr_m = _pair_settings.get("tp_atr", 1.0)
                outcome.sl_pips = round(_sl_atr * entry_atr / pip, 1)
                outcome.tp_pips = round(_tp_atr_m * entry_atr / pip, 1)
            elif _pair_settings:
                outcome.sl_pips = _pair_settings.get("sl_pips", 10.0)
                outcome.tp_pips = _pair_settings.get("tp_pips", 20.0)
            else:
                outcome.sl_pips = 10.0
                outcome.tp_pips = 20.0

            if outcome.sl_pips > 0 and outcome.tp_pips > 0:
                if direction == "BUY":
                    outcome.sl_price = entry_price - outcome.sl_pips * pip
                    outcome.tp_price = entry_price + outcome.tp_pips * pip
                else:
                    outcome.sl_price = entry_price + outcome.sl_pips * pip
                    outcome.tp_price = entry_price - outcome.tp_pips * pip

            trade = _ActiveTrade(outcome=outcome, entry_bar_idx=m1_idx)
            self._active_trades.append(trade)
            self._session_keys.add(session_key)

            logger.debug(
                "Alert fired: %s %s @ %.5f (bar %d, session=%s, spread=%.2f, ATR=%.1f pips)",
                direction, pair, entry_price, m1_idx, session,
                abs(base_score - quote_score), outcome.entry_atr_pips,
            )

    # ── Active Trade Updates ─────────────────────────────────────

    def _update_active_trades(
        self,
        m1_idx: int,
        m1_time: int,
        data: dict[str, dict[str, np.ndarray]],
        composite_scores: dict[str, float],
        htf_composite_scores: dict[str, float],
    ) -> None:
        """Update P/L, MFE/MAE, and check exit conditions for all active trades."""

        completed_indices: list[int] = []

        for idx, trade in enumerate(self._active_trades):
            outcome = trade.outcome
            if outcome.completed:
                completed_indices.append(idx)
                continue

            current_price = self._get_close_price(outcome.pair, m1_idx, data)
            if current_price is None:
                continue

            pip_val = pip_value(outcome.pair)
            bars_since_entry = m1_idx - trade.entry_bar_idx
            minutes_elapsed = bars_since_entry  # 1 M1 bar = 1 minute

            # Calculate current P/L in pips (close price for exit logic)
            if outcome.direction == "BUY":
                current_pnl = (current_price - outcome.entry_price) / pip_val
            else:
                current_pnl = (outcome.entry_price - current_price) / pip_val

            # ── Phase 1: Entry -> Exit signal ──
            if not outcome.exit_signal_fired:
                # Update MFE/MAE using M1 HIGH/LOW for realistic extremes
                hl = self._get_high_low(outcome.pair, m1_idx, data)
                if hl is not None:
                    bar_high, bar_low = hl
                    if outcome.direction == "BUY":
                        # Best case: bar high; worst case: bar low
                        best_pnl = (bar_high - outcome.entry_price) / pip_val
                        worst_pnl = (bar_low - outcome.entry_price) / pip_val
                    else:
                        # Best case: bar low; worst case: bar high
                        best_pnl = (outcome.entry_price - bar_low) / pip_val
                        worst_pnl = (outcome.entry_price - bar_high) / pip_val

                    if best_pnl > outcome.mfe_pips:
                        outcome.mfe_pips = round(best_pnl, 1)
                        outcome.best_price = bar_high if outcome.direction == "BUY" else bar_low
                        outcome.time_to_mfe_minutes = float(minutes_elapsed)

                    if worst_pnl < -outcome.mae_pips:
                        outcome.mae_pips = round(abs(worst_pnl), 1)
                        outcome.worst_price = bar_low if outcome.direction == "BUY" else bar_high
                        outcome.time_to_mae_minutes = float(minutes_elapsed)

                    # Record running MFE/MAE for bar-by-bar SL/TP simulation
                    outcome.bar_running_mfe.append(round(outcome.mfe_pips, 1))
                    outcome.bar_running_mae.append(round(outcome.mae_pips, 1))

                # ── SL/TP check (every bar, matches live paper_trader) ──
                # Only active when simulate_sltp=True (validation mode)
                # OFF during optimization so full MAE/MFE data is collected
                exit_reason = ""
                if self.config.simulate_sltp and outcome.sl_price > 0 and outcome.tp_price > 0 and hl is not None:
                    if outcome.direction == "BUY":
                        if bar_high >= outcome.tp_price:
                            exit_reason = "tp_hit"
                            current_pnl = outcome.tp_pips
                            current_price = outcome.tp_price
                        elif bar_low <= outcome.sl_price:
                            exit_reason = "sl_hit"
                            current_pnl = -outcome.sl_pips
                            current_price = outcome.sl_price
                    else:  # SELL
                        if bar_low <= outcome.tp_price:
                            exit_reason = "tp_hit"
                            current_pnl = outcome.tp_pips
                            current_price = outcome.tp_price
                        elif bar_high >= outcome.sl_price:
                            exit_reason = "sl_hit"
                            current_pnl = -outcome.sl_pips
                            current_price = outcome.sl_price

                # ── Counter-momentum exit (only if SL/TP didn't fire) ──
                if not exit_reason and bars_since_entry >= 2:
                    exit_reason = self._check_exit(
                        outcome, composite_scores, htf_composite_scores,
                    )

                if exit_reason:
                    outcome.exit_signal_fired = True
                    outcome.exit_signal_time = float(m1_time)
                    dt_jst = datetime.fromtimestamp(m1_time, tz=_JST)
                    outcome.exit_signal_time_str = dt_jst.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    outcome.exit_signal_pnl_pips = round(current_pnl, 1)
                    outcome.exit_signal_price = current_price
                    outcome.time_to_exit_minutes = float(minutes_elapsed)
                    outcome.exit_reason = exit_reason
                    outcome.close_reason = exit_reason
                    trade.exit_bar_idx = m1_idx
                    trade.post_exit_started = True

                    logger.debug(
                        "Exit signal: %s %s reason=%s pnl=%.1f pips (bar %d)",
                        outcome.direction, outcome.pair, exit_reason,
                        current_pnl, m1_idx,
                    )

                # Safety cap: force exit after 8 hours
                elif bars_since_entry >= _MAX_TRACKING_M1_BARS:
                    outcome.exit_signal_fired = True
                    outcome.exit_signal_time = float(m1_time)
                    dt_jst = datetime.fromtimestamp(m1_time, tz=_JST)
                    outcome.exit_signal_time_str = dt_jst.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    outcome.exit_signal_pnl_pips = round(current_pnl, 1)
                    outcome.exit_signal_price = current_price
                    outcome.time_to_exit_minutes = float(minutes_elapsed)
                    outcome.exit_reason = "timeout"
                    outcome.close_reason = "timeout"
                    trade.exit_bar_idx = m1_idx
                    trade.post_exit_started = True

            # ── Phase 2: Post-exit observation ──
            elif trade.post_exit_started:
                post_exit_bars = m1_idx - trade.exit_bar_idx

                # Track post-exit MFE/MAE (favorable = original direction)
                post_exit_pnl = current_pnl - outcome.exit_signal_pnl_pips
                if outcome.direction == "BUY":
                    fav = max(0.0, post_exit_pnl)
                    adv = max(0.0, -post_exit_pnl)
                else:
                    fav = max(0.0, post_exit_pnl)
                    adv = max(0.0, -post_exit_pnl)

                if fav > outcome.post_exit_mfe_pips:
                    outcome.post_exit_mfe_pips = round(fav, 1)
                if adv > outcome.post_exit_mae_pips:
                    outcome.post_exit_mae_pips = round(adv, 1)

                # Post-exit window complete?
                post_exit_limit = int(self.config.post_exit_hours * 60)
                if post_exit_bars >= post_exit_limit:
                    outcome.post_exit_final_pnl_pips = round(current_pnl, 1)
                    self._complete_trade(trade, current_pnl, minutes_elapsed)
                    completed_indices.append(idx)

            # ── Overall MAX-MFE / MAX-MAE using high/low ──
            hl_max = self._get_high_low(outcome.pair, m1_idx, data)
            if hl_max is not None:
                bar_h, bar_l = hl_max
                if outcome.direction == "BUY":
                    max_best = (bar_h - outcome.entry_price) / pip_val
                    max_worst = (bar_l - outcome.entry_price) / pip_val
                else:
                    max_best = (outcome.entry_price - bar_l) / pip_val
                    max_worst = (outcome.entry_price - bar_h) / pip_val
                if max_best > outcome.max_mfe_pips:
                    outcome.max_mfe_pips = round(max_best, 1)
                if max_worst < -outcome.max_mae_pips:
                    outcome.max_mae_pips = round(abs(max_worst), 1)

            outcome.last_update_time = float(m1_time)

        # Remove completed trades from active list
        for idx in sorted(completed_indices, reverse=True):
            trade = self._active_trades.pop(idx)
            if trade.outcome.completed:
                self._completed.append(trade.outcome)

    def _complete_trade(
        self,
        trade: _ActiveTrade,
        final_pnl: float,
        total_minutes: float,
    ) -> None:
        """Mark a trade as completed with final stats."""
        outcome = trade.outcome
        outcome.final_pnl_pips = round(final_pnl, 1)
        outcome.total_tracking_minutes = total_minutes
        outcome.completed = True
        outcome.completion_time = outcome.last_update_time

    def _force_complete_trade(
        self,
        trade: _ActiveTrade,
        data: dict[str, dict[str, np.ndarray]],
        last_m1_idx: int,
    ) -> None:
        """Force-complete a trade that is still active at end of backtest."""
        outcome = trade.outcome
        if outcome.completed:
            return

        current_price = self._get_close_price(outcome.pair, last_m1_idx, data)
        if current_price is None:
            current_price = outcome.entry_price

        pip_val = pip_value(outcome.pair)
        if outcome.direction == "BUY":
            final_pnl = (current_price - outcome.entry_price) / pip_val
        else:
            final_pnl = (outcome.entry_price - current_price) / pip_val

        bars_since_entry = last_m1_idx - trade.entry_bar_idx

        if not outcome.exit_signal_fired:
            m1_time = self._get_m1_time_at(last_m1_idx, data)
            outcome.exit_signal_fired = True
            outcome.exit_signal_time = float(m1_time) if m1_time else 0.0
            if m1_time:
                dt_jst = datetime.fromtimestamp(m1_time, tz=_JST)
                outcome.exit_signal_time_str = dt_jst.strftime("%Y-%m-%d %H:%M:%S")
            outcome.exit_signal_pnl_pips = round(final_pnl, 1)
            outcome.exit_signal_price = current_price
            outcome.time_to_exit_minutes = float(bars_since_entry)
            outcome.exit_reason = "backtest_end"
            outcome.close_reason = "backtest_end"

        self._complete_trade(trade, final_pnl, float(bars_since_entry))

    # ── Exit Signal Check ────────────────────────────────────────

    def _check_exit(
        self,
        alert: AlertOutcome,
        composite_scores: dict[str, float],
        htf_composite_scores: dict[str, float] | None = None,
    ) -> str:
        """Check whether exit conditions are met for a trade.

        Uses counter-momentum detection: exit when strong/explosive
        acceleration is detected AGAINST the trade direction.
        This matches the live system's exit logic exactly.

        Falls back to spread-collapse if momentum data unavailable.

        Returns:
            Exit reason string ("counter_momentum", "momentum_fading",
            "direction_flipped", "spread_collapsed"), or empty string.
        """
        base_ccy = alert.pair[:3]
        quote_ccy = alert.pair[3:]

        # ── Primary: Stoch v2 counter-momentum exit ──
        should_exit, stoch_reason = self._stoch_engine.check_exit(
            base_ccy, quote_ccy, alert.direction
        )
        if should_exit:
            return stoch_reason

        # ── Fallback: Legacy momentum phases ──
        phases = self._engine.get_momentum_phases()
        base_phase = phases.get(base_ccy)
        quote_phase = phases.get(quote_ccy)

        if base_phase and quote_phase:
            _COUNTER_MAGS = ("explosive",)
            _MIN_VEL = 1.5

            if alert.direction == "BUY":
                base_reversing = (
                    base_phase.velocity < -_MIN_VEL
                    and base_phase.accel_magnitude in _COUNTER_MAGS
                )
                quote_reversing = (
                    quote_phase.velocity > _MIN_VEL
                    and quote_phase.accel_magnitude in _COUNTER_MAGS
                )
            else:
                base_reversing = (
                    base_phase.velocity > _MIN_VEL
                    and base_phase.accel_magnitude in _COUNTER_MAGS
                )
                quote_reversing = (
                    quote_phase.velocity < -_MIN_VEL
                    and quote_phase.accel_magnitude in _COUNTER_MAGS
                )

            if base_reversing and quote_reversing:
                return "counter_momentum"

        return ""

    # ── Session Detection ────────────────────────────────────────

    def _get_session(self, unix_ts: float) -> str:
        """Get the trading session label for a given unix timestamp.

        Converts to JST and matches against the DST-aware session schedule.
        Automatically uses summer/winter sessions based on the date.
        """
        dt = datetime.fromtimestamp(unix_ts, tz=_JST)
        hm = dt.hour * 60 + dt.minute

        # Use DST-aware session table
        sessions = _get_sessions_for_date(dt)

        for (sh, sm), (eh, em), label in sessions:
            start = sh * 60 + sm
            end = eh * 60 + em
            if start <= hm <= end:
                return label

        return "Unknown"

    # ── Utility Methods ──────────────────────────────────────────

    def _get_close_price(
        self,
        pair: str,
        m1_idx: int,
        data: dict[str, dict[str, np.ndarray]],
    ) -> float | None:
        """Get the M1 close price for a pair at a given index."""
        m1_candles = data.get(pair, {}).get("M1")
        if m1_candles is None or m1_idx >= len(m1_candles):
            return None
        return float(m1_candles[m1_idx]["close"])

    def _get_high_low(
        self,
        pair: str,
        m1_idx: int,
        data: dict[str, dict[str, np.ndarray]],
    ) -> tuple[float, float] | None:
        """Get the M1 high and low prices for a pair at a given index."""
        m1_candles = data.get(pair, {}).get("M1")
        if m1_candles is None or m1_idx >= len(m1_candles):
            return None
        return float(m1_candles[m1_idx]["high"]), float(m1_candles[m1_idx]["low"])

    def _get_m1_time_at(
        self,
        m1_idx: int,
        data: dict[str, dict[str, np.ndarray]],
    ) -> int | None:
        """Get the unix timestamp of the M1 candle at a given index."""
        for pair in ALL_28_PAIRS:
            m1_candles = data.get(pair, {}).get("M1")
            if m1_candles is not None and m1_idx < len(m1_candles):
                return int(m1_candles[m1_idx]["time"])
        return None
