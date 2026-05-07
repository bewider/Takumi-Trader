"""ShadowSimulator — pessimistic M1 replay for shadow-trade records (Phase C).

Takes a strength-pass ShadowSignalRecord and computes a worst-case-realistic
simulated outcome by replaying the trade against M1 candle data. Outcome
fields populate `sim_*` on the record via Phase D's worker.

C.1 ships ONLY the dataclasses, M1 fetch helper, and constructor. The
actual simulation algorithm is C.2 — `simulate()` raises NotImplementedError
until then. This file's docstring + the simulate() docstring contain the
algorithm sketch so a reviewer can confirm the architecture before code.

Pessimism asymmetry vs PaperTrader:

    Real PaperTrader (paper_trader.py:_check_sl_tp):
        OPTIMISTIC — TP checked first when both SL/TP fire in same M1 bar.
    ShadowSimulator (this module):
        PESSIMISTIC — SL checked first in the ambiguous case. Plus
        worst-case fill within entry candle, plus IC Markets spread,
        plus configurable entry+SL slippage.

That intentional gap is what `ShadowCalibrationLog` measures: the
mean(real_pnl - sim_pnl) over EXECUTED parity sims. A consistent small
positive bias means pessimism is well-calibrated; either direction of
drift is actionable.

Pure-function design: simulate(record) -> SimulatedOutcome. NO record
mutation, NO disk writes, NO Qt. Phase D wraps this in a QThread and
handles persistence. Phase C is fully unit-testable with synthetic M1.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from takumi_trader.core.shadow_logger import ShadowSignalRecord

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────

@dataclass
class ShadowSimulatorConfig:
    """Pessimism + simulation configuration.

    Defaults are calibrated for IC Markets ECN raw account conditions
    (per Ryosuke's broker reality-check 2026-05-05). Numbers tuned for
    a different broker DO NOT APPLY here — over-pessimism kills profitable
    rules during validation. ShadowCalibrationLog empirically tightens
    these from observed (real - sim) deltas over time.
    """

    # ── Architectural pessimism (broker-independent) ─────────────────
    # These model the genuine uncertainty about WHERE within an M1 bar
    # a fill occurred — they're not broker-related and should never be
    # disabled in production simulation.
    worst_case_fill: bool = True
    """Use bar high (BUY) or bar low (SELL) of the entry candle as the
    fill price, not the M1 close. Models that we can't know precisely
    where in the minute the fill happened, only that it was somewhere."""

    include_spread: bool = True
    """Add the entry-side spread to the fill price (cross the spread
    cost on entry, exit at the bid/ask appropriate to the direction)."""

    ambiguous_candle_assume_sl_first: bool = True
    """When a single M1 bar has high+low both touching SL and TP levels,
    assume SL fired first. PaperTrader assumes TP first (optimistic);
    this is the deliberate inversion that exposes optimism bias in the
    real journal's edge claims."""

    # ── IC Markets-calibrated forex slippage (in price points) ───────
    slippage_points_forex_normal: float = 0.3
    """Entry slippage on forex in normal conditions. IC Markets ECN raw
    typical 0.0-0.5 spread; +0.3 is realistic-worst-case."""

    slippage_points_forex_news: float = 3.0
    """Entry slippage during NFP/FOMC/CPI windows. Spreads + slippage
    spike legitimately."""

    sl_slippage_points_forex: float = 0.5
    """Stop-out slippage on forex. IC Markets stop fills are typically
    clean; +0.5 is realistic-worst-case."""

    # ── IC Markets-calibrated gold slippage (in price points) ────────
    slippage_points_gold_normal: float = 1.0
    """Entry slippage on XAUUSD in normal conditions. ~10 cents on gold
    via IC Markets."""

    slippage_points_gold_news: float = 10.0
    """Entry slippage on XAUUSD during news windows."""

    sl_slippage_points_gold: float = 2.0
    """Stop-out slippage on XAUUSD."""

    # ── Window ───────────────────────────────────────────────────────
    max_hold_minutes: int = 240
    """Beyond this, the simulator returns sim_exit_reason='TIMEOUT'.
    240 (4h) covers all legitimate Sv2 holds with margin; longer holds
    typically indicate signal_exit / vote-based-close territory the
    simulator doesn't try to reproduce."""

    # ── Self-monitoring drift detection ──────────────────────────────
    calibration_warn_band_pips: float = 1.5
    """If mean(real_pnl - sim_pnl) over recent calibration records drifts
    beyond ±this many pips, the simulator emits a WARNING. Either
    direction of drift is actionable: positive => simulator too
    pessimistic; negative => too optimistic (the dangerous direction)."""

    calibration_warn_after_n: int = 10
    """Drift check runs every N calibration appends. Below N records,
    sample is pure noise."""

    # ── Phase D worker retry policy ──────────────────────────────────
    transient_retry_max: int = 12
    """ShadowSimWorker gives up on transient failures (no_m1_data,
    data_too_recent, empty_m1) after this many retries and marks the
    record permanent-FAILED. At the default 5-min cycle interval, 12
    retries ≈ 1 hour of attempts. Configurable here so we can tune
    after observing real M1 cache fill behavior — gold during news
    or thin-liquidity crosses may legitimately take longer than 1h.
    Bump this rather than hardcoding a magic number in the worker."""

    def serialize(self) -> str:
        """Compact string representation stamped on each sim outcome.

        Format: 'wcf+sp+slip_fx0.3_au1.0+sl_first+sl_slip_fx0.5_au2.0+news_stub'

        Stamped on every record via SimulatedOutcome.sim_pessimism_applied
        so historical sim outputs remain interpretable when config evolves
        (e.g., post-empirical-calibration tightening in Phase F).

        The trailing 'news_stub' tag indicates that BrokerSpreadModel's
        HARDCODED_NEWS_WINDOWS is empty — the simulator is using
        normal/overlap/tokyo slippage even during NFP/FOMC/CPI windows.
        This will UNDERESTIMATE friction on news days. When Phase F
        integrates news_filter.is_blackout, this tag flips to 'news_live',
        making pre-Phase-F vs post-Phase-F records distinguishable forever
        for forensic calibration analysis.
        """
        parts: list[str] = []
        if self.worst_case_fill:
            parts.append("wcf")
        if self.include_spread:
            parts.append("sp")
        parts.append(
            f"slip_fx{self.slippage_points_forex_normal}_"
            f"au{self.slippage_points_gold_normal}"
        )
        if self.ambiguous_candle_assume_sl_first:
            parts.append("sl_first")
        parts.append(
            f"sl_slip_fx{self.sl_slippage_points_forex}_"
            f"au{self.sl_slippage_points_gold}"
        )
        # News-integration phase indicator. Phase F flips to 'news_live'.
        parts.append("news_stub")
        # Volume-data state indicator. M1Cache lacks tick_volume so the
        # feature engine's defensive np.ones() fallback fires for
        # CVD / Amihud / Kyle's lambda. Phase F.8 (add tick_volume to
        # M1Cache schema) flips this to 'vol_real'. Until then, Edge
        # Miner queries should filter or de-prioritize volume-derived
        # features on records stamped 'vol_synth'.
        parts.append("vol_synth")
        return "+".join(parts)


@dataclass
class SimulatedOutcome:
    """Result of one ShadowSimulator.simulate() call. Pure data, no I/O.

    Phase D's worker reads these fields and writes them back to the
    journal record via ShadowLogger.write_simulation. C.3 populates
    `features` via lazy feature recompute.
    """

    sim_exit_time: float = 0.0          # epoch seconds; 0.0 if FAILED
    sim_exit_price: float = 0.0          # 0.0 if FAILED
    sim_exit_reason: str = ""            # "TP" | "SL" | "TIMEOUT" | "FAILED"
    sim_pnl_pips: float = 0.0            # signed; INCLUDES spread + slippage costs
    sim_pnl_account_ccy: float = 0.0     # estimated; uses proposed_lot_size
    sim_mae_pips: float = 0.0            # max adverse excursion (always >= 0)
    sim_mfe_pips: float = 0.0            # max favorable excursion (always >= 0)
    sim_duration_minutes: int = 0
    sim_pessimism_applied: str = ""      # config.serialize() at sim time
    sim_failure_reason: str = ""         # populated only when sim_exit_reason == "FAILED"
    features: dict | None = None         # populated by C.3 lazy recompute; None until then


# ─────────────────────────────────────────────────────────────────────
# Simulator
# ─────────────────────────────────────────────────────────────────────

class ShadowSimulator:
    """Pessimistic M1 replay simulator for shadow-trade records.

    C.1: constructor + fetch_m1 helper. C.2 implements simulate().
    C.3 wires lazy feature recompute. C.4 hooks ShadowCalibrationLog.

    Pure function semantics: simulate(record) -> SimulatedOutcome.
    Does NOT mutate the record or write to disk. Phase D's QThread
    worker handles persistence.
    """

    def __init__(
        self,
        m1_cache,                          # M1Cache instance
        spread_model,                      # BrokerSpreadModel instance
        feature_engine: Any | None = None,  # FeatureEngine; populated in C.3
        calibration_log: Any | None = None,  # ShadowCalibrationLog; populated in C.4
        config: ShadowSimulatorConfig | None = None,
    ) -> None:
        self.cache = m1_cache
        self.spread_model = spread_model
        self.feature_engine = feature_engine
        self.calibration_log = calibration_log
        self.config = config or ShadowSimulatorConfig()

    def fetch_m1(
        self,
        pair: str,
        signal_time: float,
        lookforward_min: int | None = None,
    ):
        """Fetch the simulation window from M1Cache.

        Window: [signal_time, signal_time + lookforward * 60].
        lookforward defaults to config.max_hold_minutes. Returns
        whatever M1Cache.fetch returns — typically a numpy structured
        array; None if data unavailable (signal too recent, MT5 down,
        or window beyond available history)."""
        lookforward = lookforward_min or self.config.max_hold_minutes
        return self.cache.fetch(
            pair=pair,
            start_epoch=signal_time,
            end_epoch=signal_time + lookforward * 60,
        )

    def simulate(self, record: "ShadowSignalRecord") -> SimulatedOutcome:
        """Run pessimistic M1 replay for a strength-pass record.

        Implementation of the C.2 algorithm. See SimulatedOutcome and
        ShadowSimulatorConfig for the data shape this returns.

        Pure function: does NOT mutate `record`, does NOT write to disk.
        Phase D's worker handles persistence.

        Failure modes (all return FAILED outcome, no exception raised):
            * strength-reject record (no input_snapshot, no entry to sim)
            * no M1 data available (signal too recent, MT5 down, history gap)
            * malformed record (zero SL/TP, zero entry price)
            * record with proposed_sl_price == proposed_tp_price (defensive)
        """
        from takumi_trader.core.shadow_logger import GATE_STRENGTH_ENGINE
        from takumi_trader.core.trade_tracker import pip_value

        # ── Step 1: strength-reject guard ────────────────────────────
        # Lightweight rejects have no input_snapshot AND no proposed entry.
        # Skip — they're terminal and don't need simulation.
        if record.block_gate == GATE_STRENGTH_ENGINE and not record.input_snapshot_json:
            return self._failed("strength_reject_no_snapshot")
        if record.proposed_entry <= 0 or record.proposed_sl_price <= 0 or record.proposed_tp_price <= 0:
            return self._failed("invalid_proposal_prices")
        if record.direction not in ("BUY", "SELL"):
            return self._failed(f"invalid_direction:{record.direction!r}")

        # ── Step 2: fetch M1 bars ────────────────────────────────────
        bars = self.fetch_m1(record.pair, record.signal_time)
        if bars is None:
            return self._failed("no_m1_data")
        if len(bars) == 0:
            return self._failed("empty_m1")
        if len(bars) < 2:
            # Need at least entry candle + one forward bar to walk
            return self._failed("insufficient_m1_bars")

        # ── Step 3: spread + slippage lookup ─────────────────────────
        spread_info = self.spread_model.lookup(record.pair, record.signal_time)
        pip = pip_value(record.pair)

        # ── Step 4: pessimistic entry price ─────────────────────────
        sim_entry = self._compute_pessimistic_entry(
            entry_bar=bars[0],
            direction=record.direction,
            spread_info=spread_info,
            pip=pip,
        )

        # ── Step 4.5: stale-proposed-levels guard (2026-05-07 fix) ─
        # The pessimistic-entry mechanism assumes sim_entry stays on the
        # "correct side" of the proposed SL/TP levels. When there's
        # significant price movement between signal_time and bars[0]
        # (typically Asia-Pacific session pairs with high per-bar
        # range), entry slippage can push sim_entry past one of the
        # proposed levels. In that state the trade's R:R framing is
        # broken: SL/TP are no longer in the directions they were
        # designed to monitor, and producing pnl yields structurally-
        # impossible outcomes (e.g., SL hit at profit).
        #
        # Fail-fast with reason "stale_proposed_levels" so the worker
        # marks the record permanent-FAILED rather than retrying 12x
        # (the failure reason is added to _PERMANENT_FAILURE_PREFIXES
        # in shadow_sim_worker.py). Edge Miner queries can identify
        # and exclude these records by reason.
        #
        # Diagnosed via Tier 1 due-diligence on calibration entry
        # shadow_id=29686 EURCHF SELL: proposed_entry=0.91555, sl=0.91604,
        # but bars[0] LOW=0.91638 → sim_entry=0.9163 (8p past proposed).
        # Blast radius scan: 210 of 60,359 records (0.35%) hit this
        # before the guard, dominated by Asia-Pacific SELL signals.
        # Fix B (root-cause bar-alignment) deferred to a future session.
        if record.direction == "BUY":
            if sim_entry <= record.proposed_sl_price or sim_entry >= record.proposed_tp_price:
                return self._failed("stale_proposed_levels")
        else:  # SELL
            if sim_entry >= record.proposed_sl_price or sim_entry <= record.proposed_tp_price:
                return self._failed("stale_proposed_levels")

        # ── Step 5: SL/TP stay at proposed PRICES (Decision (a)) ─────
        # The broker would have placed SL/TP at these levels regardless
        # of where we got filled. Pessimistic entry naturally degrades
        # R:R — that's the realism we want.
        sl_price = record.proposed_sl_price
        tp_price = record.proposed_tp_price

        # Direction-specific SL slippage (in price space, applied adverse)
        sl_slip_price = spread_info.sl_slippage_points * pip

        # ── Step 6: walk forward bar-by-bar ─────────────────────────
        exit_price: float | None = None
        exit_reason = ""
        exit_bar_time: int = 0
        mae_pips = 0.0
        mfe_pips = 0.0

        # Start at bars[1], skipping the entry bar (bars[0]).
        # Rationale: pessimistic entry already assumes worst-case fill
        # within bars[0]'s high-low range. Checking SL/TP in the same
        # bar would double-stack pessimism — we'd be asking "after a
        # worst-case fill, did SL/TP also hit in the same minute?"
        # which is incoherent because the post-fill intra-bar price
        # path is unknown. The correct semantic is: pessimistic entry
        # happens during bars[0], then SL/TP monitoring begins on
        # bars[1] onward, matching how a real broker handles it. Also
        # matches paper_trader._check_sl_tp convention.
        for bar in bars[1:]:
            high = float(bar["high"])
            low = float(bar["low"])
            close = float(bar["close"])

            # Track MAE/MFE before checking exit conditions
            if record.direction == "BUY":
                # MAE = how far adverse (down from entry); MFE = how far favorable (up)
                mae_pips = max(mae_pips, (sim_entry - low) / pip)
                mfe_pips = max(mfe_pips, (high - sim_entry) / pip)
                sl_hit = low <= sl_price
                tp_hit = high >= tp_price
            else:  # SELL
                mae_pips = max(mae_pips, (high - sim_entry) / pip)
                mfe_pips = max(mfe_pips, (sim_entry - low) / pip)
                sl_hit = high >= sl_price
                tp_hit = low <= tp_price

            # Ambiguous candle: both SL and TP within this bar's range
            if sl_hit and tp_hit:
                if self.config.ambiguous_candle_assume_sl_first:
                    if record.direction == "BUY":
                        exit_price = sl_price - sl_slip_price
                    else:
                        exit_price = sl_price + sl_slip_price
                    exit_reason = "SL"
                else:
                    exit_price = tp_price
                    exit_reason = "TP"
                exit_bar_time = int(bar["time"])
                break
            elif sl_hit:
                if record.direction == "BUY":
                    exit_price = sl_price - sl_slip_price
                else:
                    exit_price = sl_price + sl_slip_price
                exit_reason = "SL"
                exit_bar_time = int(bar["time"])
                break
            elif tp_hit:
                exit_price = tp_price  # no TP slippage by design (favorable side)
                exit_reason = "TP"
                exit_bar_time = int(bar["time"])
                break

        # ── Step 7: TIMEOUT if loop completed without break ─────────
        if exit_price is None:
            exit_price = float(bars[-1]["close"])
            exit_reason = "TIMEOUT"
            exit_bar_time = int(bars[-1]["time"])

        # MAE/MFE clamps — should already be non-negative from max() but
        # defensive against float weirdness in pathological synthetic data
        mae_pips = max(0.0, mae_pips)
        mfe_pips = max(0.0, mfe_pips)

        # ── Step 8: pnl_pips ────────────────────────────────────────
        if record.direction == "BUY":
            pnl_price = exit_price - sim_entry
        else:
            pnl_price = sim_entry - exit_price
        pnl_pips = pnl_price / pip

        # ── Step 9: pnl_account_ccy ──────────────────────────────────
        # TODO(Phase-F): compute proper account-currency PnL via
        # broker symbol_info + lot_size. For C.2, leaving at 0.0 because:
        # 1. Edge Miner queries on shadow records use pnl_pips, not
        #    account_ccy (currency conversion depends on live FX rates).
        # 2. Computing it accurately requires symbol_info lookup which
        #    isn't pure-function in the simulator's design.
        # 3. The 0.0 is unambiguous — Edge Miner can compute account_ccy
        #    on demand from pnl_pips + lot_size when needed.
        pnl_account_ccy = 0.0

        # ── Step 10: assemble SimulatedOutcome ──────────────────────
        duration_min = max(0, (exit_bar_time - int(record.signal_time)) // 60)
        outcome = SimulatedOutcome(
            sim_exit_time=float(exit_bar_time),
            sim_exit_price=exit_price,
            sim_exit_reason=exit_reason,
            sim_pnl_pips=round(pnl_pips, 2),
            sim_pnl_account_ccy=pnl_account_ccy,
            sim_mae_pips=round(mae_pips, 2),
            sim_mfe_pips=round(mfe_pips, 2),
            sim_duration_minutes=int(duration_min),
            sim_pessimism_applied=self.config.serialize(),
        )

        # ── Step 11 (Phase C.3): lazy feature recompute ─────────────
        # Compute the 138-key feat_* panel from input_snapshot + historical
        # M1Cache bars. Failures don't propagate — outcome.features stays
        # None and Edge Miner treats that as missing.
        if self.feature_engine is not None:
            try:
                outcome.features = self._recompute_features(record)
            except Exception as exc:
                logger.warning(
                    "[SHADOW SIM] feature recompute failed for shadow_id=%s: %s",
                    record.shadow_id, exc,
                )
                outcome.features = None

        return outcome

    # ── Phase C.3: lazy feature recompute ───────────────────────────

    def _recompute_features(self, record: "ShadowSignalRecord") -> dict | None:
        """Lazy recompute of the 138-key feat_* panel at signal_time.

        Inputs:
            * record.input_snapshot_json — composite_scores + cross_pair_close_prices
              snapshotted at capture time (see mt5_worker._capture_sv2_shadow)
            * M1Cache historical fetch — 24h lookback from signal_time
            * M15/H1 bars resampled from the M1 lookback in-memory

        Returns dict of 138 feat_* keys, or None if M1 history unavailable.

        ── Volume-derived feature caveat ──
        M1Cache schema lacks tick_volume. compute_for_entry's defensive
        np.ones() fallback means CVD, Amihud illiquidity, Kyle's lambda
        compute against synthetic constant-volume data. Edge Miner queries
        on shadow records should filter these out (Phase F adds tick_volume
        to M1Cache schema).

        ── Network features (Tier 3) NOT included ──
        compute_entry_features pulls VIX/yield-curve/sentiment from a live
        cache fetched NOW, which would be wrong for historical recompute.
        extract_feat_dict deliberately omits the network section. Phase F
        could add a historical network snapshot mechanism.

        ── composite_scores_prev gap ──
        Existing journal records (captured before this Phase F note) lack
        composite_scores_prev in their input_snapshot. Without it, the
        delta features (feat_dUSD, feat_dEUR, ...) compute from None ->
        default 0.0. Forward records can have it added; historical records
        permanently lack it. Edge Miner queries on shadow records should
        filter feat_dXXX to non-zero before trusting them.
        """
        import json
        import numpy as np

        if not record.input_snapshot_json:
            return None

        try:
            snap = json.loads(record.input_snapshot_json)
        except Exception:
            return None

        signal_time = record.signal_time
        pair = record.pair

        # ── Fetch historical M1 lookback window (24h before signal_time) ──
        # Reuses M1Cache.fetch with a lookback window. Different from
        # self.fetch_m1 (which fetches FORWARD for sim replay).
        lookback_seconds = 24 * 3600
        m1_bars = self.cache.fetch(
            pair=pair,
            start_epoch=signal_time - lookback_seconds,
            end_epoch=signal_time,
        )
        if m1_bars is None or len(m1_bars) == 0:
            # No historical M1 — feature recompute can't proceed. Return
            # None; outcome.features stays absent. Phase D worker may
            # retry on next cycle if cache fills lazily.
            return None

        # ── Resample to M15 + H1 ─────────────────────────────────────
        # Determine the calendar month of the signal for resample cache keying.
        from datetime import datetime, timezone
        sig_dt = datetime.fromtimestamp(signal_time, tz=timezone.utc)
        ym = f"{sig_dt.year:04d}-{sig_dt.month:02d}"
        m15_bars = self.cache.resample(pair, ym, m1_bars, target_minutes=15)
        h1_bars = self.cache.resample(pair, ym, m1_bars, target_minutes=60)

        # ── Call compute_for_entry directly with HISTORICAL bars ────
        # NOT compute_entry_features, which would silently re-fetch
        # CURRENT bars from MT5 and poison Edge Miner historical analysis.
        try:
            full = self.feature_engine.compute_for_entry(
                pair=pair,
                timestamp_utc=int(signal_time),
                m1_bars=m1_bars,
                m15_bars=m15_bars,
                h1_bars=h1_bars,
                composite_scores=snap.get("composite_scores"),
                composite_scores_prev=snap.get("composite_scores_prev"),  # may be None
                cross_pair_data=snap.get("cross_pair_close_prices"),
            )
        except Exception as exc:
            logger.warning(
                "[SHADOW SIM] compute_for_entry raised on shadow_id=%s: %s",
                record.shadow_id, exc,
            )
            return None

        # ── Apply canonical feat_* mapping via extract_feat_dict ────
        from takumi_trader.features.feature_engine import extract_feat_dict
        feat = extract_feat_dict(
            full_result=full,
            cross_pair_data=snap.get("cross_pair_close_prices"),
            pair=pair,
            timestamp_utc=int(signal_time),
        )
        return feat

    # ── Phase C.4: calibration log integration ──────────────────────

    def write_calibration(
        self,
        record: "ShadowSignalRecord",
        outcome: SimulatedOutcome,
        real_pnl_pips: float,
        real_exit_reason: str,
        real_duration_minutes: float,
    ) -> bool:
        """Write a sim-vs-real calibration record for an EXECUTED parity sim.

        Called by Phase D's worker after each simulation of an EXECUTED
        record where the linked real trade is closed. The delta_pips
        (real - sim) accumulates over time and feeds the drift-detection
        WARNING that surfaces when pessimism calibration goes off-target.

        Args:
            record: the shadow signal record (must have status=EXECUTED).
            outcome: the SimulatedOutcome from simulate(record).
            real_pnl_pips: pnl_pips from the linked real trade.
            real_exit_reason: real trade close_reason (e.g., "tp_hit", "sl_hit").
            real_duration_minutes: real trade duration in minutes.

        Returns:
            True if a record was written; False if skipped (no calibration_log
            attached, record not EXECUTED, sim FAILED, etc.). Skipping is
            silent and non-error — calibration is best-effort by design.

        Side effects:
            * Appends to ShadowCalibrationLog if attached
            * Triggers _check_calibration_drift after every append
              (which may emit a WARNING if drift exceeds the configured band)
        """
        from takumi_trader.core.shadow_logger import (
            STATUS_EXECUTED, ShadowCalibrationRecord,
        )

        if self.calibration_log is None:
            return False
        if record.status != STATUS_EXECUTED:
            return False
        if outcome.sim_exit_reason == "FAILED":
            return False  # FAILED sims have no comparable pnl

        try:
            cal = ShadowCalibrationRecord(
                shadow_id=record.shadow_id,
                strategy_id=record.strategy_id,
                pair=record.pair,
                direction=record.direction,
                signal_time=record.signal_time,
                real_pnl_pips=float(real_pnl_pips),
                sim_pnl_pips=float(outcome.sim_pnl_pips),
                # delta_pips computed by ShadowCalibrationLog.append
                real_exit_reason=str(real_exit_reason),
                sim_exit_reason=str(outcome.sim_exit_reason),
                real_duration_minutes=float(real_duration_minutes),
                sim_duration_minutes=float(outcome.sim_duration_minutes),
                pessimism_applied=str(outcome.sim_pessimism_applied),
            )
            self.calibration_log.append(cal)
        except Exception as exc:
            logger.warning(
                "[SHADOW SIM] write_calibration failed shadow_id=%s: %s",
                record.shadow_id, exc,
            )
            return False

        # Self-monitoring: warn on drift after every N appends
        try:
            self._check_calibration_drift()
        except Exception as exc:
            # Drift check failures are non-fatal — log only, don't fail
            # the calibration write itself.
            logger.warning("[SHADOW SIM] drift check failed: %s", exc)
        return True

    def _check_calibration_drift(self) -> None:
        """Emit WARNING if mean(real - sim) over the last N records drifts
        beyond ±config.calibration_warn_band_pips.

        Either direction of drift is actionable:
            mean > +band  → simulator too pessimistic (real beats sim
                              consistently — consider tightening
                              slippage/spread settings)
            mean < -band  → simulator too optimistic (sim beats real
                              consistently — DANGEROUS, real edge is
                              illusory and pessimism must be loosened)

        Below `calibration_warn_after_n` records, the sample is pure
        noise and the check stays silent. The threshold should match
        the architect-confirmed config defaults: warn_band=1.5p,
        warn_after=10.
        """
        if self.calibration_log is None:
            return

        n_required = int(self.config.calibration_warn_after_n)
        band = float(self.config.calibration_warn_band_pips)

        records = self.calibration_log.all_records()
        if len(records) < n_required:
            return

        recent = records[-n_required:]
        deltas = [r.delta_pips for r in recent]
        mean_delta = sum(deltas) / len(deltas)

        if abs(mean_delta) <= band:
            return  # within tolerance; no warning

        direction = "too pessimistic" if mean_delta > 0 else "TOO OPTIMISTIC"
        warning_severity = (
            "drift detected" if mean_delta > 0
            else "DRIFT (DANGEROUS DIRECTION)"
        )
        logger.warning(
            "[SHADOW CALIBRATION] %s: mean(real - sim) = %+.2fp over last "
            "%d records (band=±%.1fp). Simulator is %s. %s",
            warning_severity,
            mean_delta,
            n_required,
            band,
            direction,
            (
                "Real trades beat sim — consider tightening pessimism."
                if mean_delta > 0
                else "Sim beats real — pessimism is too loose; tighten "
                     "before scaling decisions."
            ),
        )

    # ── Helpers ─────────────────────────────────────────────────────

    def _compute_pessimistic_entry(
        self,
        entry_bar,
        direction: str,
        spread_info,
        pip: float,
    ) -> float:
        """Apply worst-case-fill, spread cross, and entry slippage.

        BUY:  start at high (worst), add spread (we cross to ASK),
              add slippage (pushed further unfavorable).
        SELL: start at low (worst), subtract spread (we cross to BID),
              subtract slippage.
        """
        if self.config.worst_case_fill:
            base = float(entry_bar["high" if direction == "BUY" else "low"])
        else:
            base = float(entry_bar["close"])

        if self.config.include_spread:
            spread_price = spread_info.spread_points * pip
            base = base + spread_price if direction == "BUY" else base - spread_price

        slip_price = spread_info.slippage_points * pip
        return base + slip_price if direction == "BUY" else base - slip_price

    @staticmethod
    def _failed(reason: str) -> SimulatedOutcome:
        """Standard FAILED outcome with the given failure reason."""
        return SimulatedOutcome(
            sim_exit_reason="FAILED",
            sim_failure_reason=reason,
        )
