"""ShadowSimWorker — QThread that drives ShadowSimulator on a 5-min cycle.

Phase D.1 ships only the skeleton (constructor, run() loop, stop(),
get_stats(), signal definitions, and a no-op _run_cycle() stub).
D.2 implements the per-cycle dispatcher (permanent-FAILED fast-path
with batched flush + transient/permanent retry classification).
D.3 wires EXECUTED parity sims to write_calibration.
D.4 integrates into main_window's lifecycle.

Architectural choices locked in the Phase D design proposal:

    Threading model: QThread subclass, mirrors MT5Worker / CsiWorker
        pattern. self._running flag for cooperative shutdown. Top-level
        run() catches Exception so the thread never dies silently.

    Cycle interval: relative (sleep `poll_interval - elapsed` after
        each cycle), not strict-clock-aligned. Avoids thundering-herd
        with MT5Worker's 1-sec cycles and naturally backs off when a
        cycle takes longer than expected.

    Backpressure: max_per_cycle=50 real sims + max_permanent_per_cycle
        =1000 permanent-FAILED fast-path marks per cycle. Bounds the
        cycle's wall time so closeEvent's wait(3000) is honored.

    Shutdown granularity: _running is checked between records inside
        the cycle (D.2 wires this), not just between cycles. Worst-case
        shutdown latency target is ~100ms from stop() to thread exit.

    Failure isolation: per-record try/except (logs WARN, continues),
        per-cycle try/except (logs WARN, sleeps, retries), top-level
        try/except (emits fatal_error signal, exits cleanly).

Phase A's atomic flush primitive composes correctly with this worker:
a crash mid-cycle leaves only persisted records on disk; in-memory
mutations to records currently being processed are lost but those
records remain pending (sim_completed=False) and get retried on
worker restart.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from PyQt6.QtCore import QThread, pyqtSignal

logger = logging.getLogger(__name__)


@dataclass
class RealTradeOutcome:
    """Closed real-trade outcome — input to write_calibration.

    The worker's `real_trade_lookup` callable returns one of these for
    EXECUTED shadow records whose linked real trade is closed, or None
    if the real trade is still open. Decouples ShadowSimWorker from
    PaperTradeRecord's concrete shape — main_window provides a closure
    that resolves shadow_id -> real trade in its own data model.
    """
    pnl_pips: float
    exit_reason: str           # "tp_hit" | "sl_hit" | "signal_exit" | "weekend_close" | ...
    duration_minutes: float


class ShadowSimWorker(QThread):
    """Background worker that drains ShadowLogger.pending_simulation()
    every poll_interval seconds.

    Thread safety:
        * ShadowLogger.pending_simulation() returns a fresh list per call
          (safe to call from this thread while MT5Worker writes via
          log_signal in its own thread).
        * ShadowLogger.write_simulation() / mark_decision() use the
          atomic flush primitive from Phase A — safe from any thread.
        * No shared locks introduced; relies on GIL-protected dict/list
          ops + filesystem-level atomic os.replace.

    Signals (queued to main thread for UI consumption):
        cycle_complete(stats: dict) — emitted at end of each successful
            cycle. Phase E's stats panel subscribes here.
        drift_warning(message: str) — forwarded from
            ShadowSimulator._check_calibration_drift when it fires.
            main_window can surface this via health_alerts.
        fatal_error(message: str) — emitted only if the top-level
            run() loop catches an unrecoverable exception. main_window's
            dead-man's-switch (see closeEvent integration in D.4) escalates.
    """

    cycle_complete = pyqtSignal(dict)
    drift_warning = pyqtSignal(str)
    fatal_error = pyqtSignal(str)

    # Sleep step for interruptible-sleep loop. Smaller = more responsive
    # to stop() but more loop overhead. 0.1s matches MT5Worker's pattern.
    _SLEEP_STEP = 0.1

    def __init__(
        self,
        shadow_logger,                       # ShadowLogger instance
        simulator,                           # ShadowSimulator instance
        poll_interval: float = 300.0,        # 5 min between cycles
        max_per_cycle: int = 50,             # real sims per cycle ceiling
        max_permanent_per_cycle: int = 1000,  # fast-path mark ceiling
        real_trade_lookup: Callable | None = None,  # D.3: shadow_id -> RealTradeOutcome | None
    ) -> None:
        super().__init__()
        if shadow_logger is None:
            raise ValueError("shadow_logger is required")
        if simulator is None:
            raise ValueError("simulator is required")
        if poll_interval <= 0:
            raise ValueError(f"poll_interval must be > 0, got {poll_interval}")
        if max_per_cycle <= 0:
            raise ValueError(f"max_per_cycle must be > 0, got {max_per_cycle}")
        if max_permanent_per_cycle <= 0:
            raise ValueError(
                f"max_permanent_per_cycle must be > 0, got {max_permanent_per_cycle}"
            )

        self._logger = shadow_logger
        self._simulator = simulator
        self.poll_interval = poll_interval
        self.max_per_cycle = max_per_cycle
        self.max_permanent_per_cycle = max_permanent_per_cycle
        self._real_trade_lookup = real_trade_lookup

        # Cooperative-shutdown flag
        self._running = True

        # Cycle stats accumulators (steady-state observables)
        self._cycles_completed: int = 0
        self._total_records_simulated: int = 0
        self._total_calibrations_written: int = 0
        self._total_permanent_failed: int = 0
        self._last_cycle_complete_ts: float = 0.0
        self._current_drift_warning: str | None = None

        # First-run state — log mode flips to STEADY when permanent queue
        # empties. eta calculation uses linear extrapolation from observed
        # records-per-cycle.
        self._first_run_active: bool = True
        self._first_run_start_ts: float = 0.0
        self._first_run_total_permanent_estimated: int = 0
        self._first_run_records_cleared_so_far: int = 0

        # Calibration first-light marker — fired exactly once when the
        # first calibration record gets written (Phase D.3 will trigger).
        self._calibration_first_light_logged: bool = False

    # ── Public API ──────────────────────────────────────────────────

    def stop(self) -> None:
        """Signal the worker to exit at the next interrupt point.

        Cooperative — does NOT forcibly kill the thread. Caller should
        follow with self.wait(timeout) to confirm exit. Worst-case
        latency from stop() to thread exit is ~100ms (one _SLEEP_STEP)
        for sleep interruption + whatever per-record granularity D.2
        wires (target: <100ms per record check)."""
        self._running = False

    def get_stats(self) -> dict[str, Any]:
        """Snapshot of worker state. Used by Phase E's stats panel and
        main_window's dead-man's-switch.

        Pure read — does not mutate any state, safe to call from main
        thread while worker is running. (pending_count is a transient
        snapshot; it can change between calls.)
        """
        try:
            pending_count = len(self._logger.pending_simulation())
        except Exception:
            pending_count = -1  # signal "couldn't read" rather than zero
        return {
            "last_cycle_complete_ts": self._last_cycle_complete_ts,
            "pending_count": pending_count,
            "total_records_simulated": self._total_records_simulated,
            "total_calibrations_written": self._total_calibrations_written,
            "total_permanent_failed": self._total_permanent_failed,
            "current_drift_warning": self._current_drift_warning,
            "cycles_completed": self._cycles_completed,
            "first_run_active": self._first_run_active,
        }

    def is_running(self) -> bool:
        """True until stop() flips the flag. Used by D.2's per-record
        loops to honor stop requests mid-cycle."""
        return self._running

    # ── Thread entry point ──────────────────────────────────────────

    def run(self) -> None:  # noqa: D401
        """Main loop. Runs in the worker thread, NOT the caller thread."""
        logger.info(
            "[SIM-WORKER] starting (poll=%.0fs, max_sim/cycle=%d, max_permanent/cycle=%d)",
            self.poll_interval, self.max_per_cycle, self.max_permanent_per_cycle,
        )
        self._first_run_start_ts = time.time()

        try:
            while self._running:
                cycle_start = time.monotonic()
                stats: dict | None = None
                try:
                    stats = self._run_cycle()
                except Exception as exc:
                    # Per-cycle failure: log + continue. The thread
                    # MUST NOT die from a single bad cycle.
                    logger.warning(
                        "[SIM-WORKER] cycle raised: %s", exc, exc_info=True,
                    )
                    stats = None

                # Emit cycle stats only on successful cycles AND only if
                # we're still running (avoid signaling during shutdown).
                if stats is not None and self._running:
                    self._cycles_completed += 1
                    self._last_cycle_complete_ts = time.time()
                    try:
                        self.cycle_complete.emit(stats)
                    except Exception as exc:
                        # Signal emit failures (e.g., main thread gone)
                        # are non-fatal — keep the worker running.
                        logger.warning(
                            "[SIM-WORKER] cycle_complete emit failed: %s", exc,
                        )

                # Bail out of sleep if stop() arrived during the cycle
                if not self._running:
                    break

                elapsed = time.monotonic() - cycle_start
                sleep_for = max(0.0, self.poll_interval - elapsed)
                self._sleep_interruptible(sleep_for)

        except Exception as exc:
            # Top-level catch: should never reach here unless the
            # exception escaped per-cycle catch (impossible in current
            # code, but defensive). Emit fatal_error for the dead-man's
            # switch and exit cleanly.
            logger.critical(
                "[SIM-WORKER] FATAL: %s", exc, exc_info=True,
            )
            try:
                self.fatal_error.emit(f"ShadowSimWorker fatal: {exc!r}")
            except Exception:
                pass
        finally:
            logger.info(
                "[SIM-WORKER] stopped (cycles=%d, simulated=%d, calibrations=%d, "
                "permanent_failed=%d)",
                self._cycles_completed, self._total_records_simulated,
                self._total_calibrations_written, self._total_permanent_failed,
            )

    # ── Cycle dispatcher (D.2) ──────────────────────────────────────

    def _run_cycle(self) -> dict:
        """Run one cycle of dispatch.

        Two-phase structure:
          Phase 1: permanent-FAILED fast-path. Records that simulate()
                   would reject anyway (strength-rejects with no
                   input_snapshot, malformed records) get marked
                   permanent-FAILED in bulk, bypassing the simulator.
                   Bounded by max_permanent_per_cycle (default 1000).
                   Per-record flushes are throttled by ShadowLogger's
                   30-sec mechanism; force_flush at end of cycle
                   guarantees disk durability.

          Phase 2: real simulation batch. For non-permanent-failure
                   records, call simulator.simulate(). Classify FAILED
                   outcomes as transient (retry next cycle, increment
                   transient_retry_count) or permanent (mark FAILED
                   immediately). Cap transient retries at
                   config.transient_retry_max — escalate to permanent
                   after N to prevent infinite-loop on records whose
                   M1 will never be available. Bounded by max_per_cycle.

        Both phases honor self._running between records — stop()
        arriving mid-cycle exits the inner loops within ~_SLEEP_STEP.

        First-run mode: flips to STEADY on the first cycle that
        attempts a real simulation (i.e., permanent backlog has
        cleared enough room for real work). Logged exactly once.
        """
        pending = self._logger.pending_simulation()

        # Count the permanent backlog at cycle start — the "are we
        # about to clear it" signal that drives the FIRST-RUN exit.
        permanents_at_start = sum(
            1 for r in pending if _is_permanent_failure_record(r)
        )

        permanent_failed_count = self._process_permanent_fastpath(pending)
        sim_results = self._process_real_sims(pending)

        # Phase 3 (D.3): drain the calibration queue. EXECUTED records
        # whose sim is done but real-trade close was still pending in
        # an earlier cycle are picked up here whenever the lookup says
        # the real trade has closed.
        calibrations_written = self._process_calibrations()

        # End-of-cycle force flush — guarantees all in-cycle mutations
        # (permanent-FAILED marks + write_simulations + retry bumps)
        # reach disk before we sleep. Phase A's atomic-write composes
        # correctly: a crash mid-flush leaves a clean rollback state.
        try:
            self._logger.force_flush()
        except Exception as exc:
            logger.warning("[SIM-WORKER] end-of-cycle flush failed: %s", exc)

        # FIRST-RUN -> STEADY mode flip — option 3 (drained-starting-backlog).
        #
        # Trigger: this cycle processed every permanent record that was in
        # the queue at cycle start. Mathematically: permanents_at_start
        # minus permanents-actually-processed (permanent_failed_count) == 0.
        #
        # Why this and not strict post-condition (pending_permanent_count == 0
        # at cycle end): in production, MT5Worker continuously writes new
        # strength-rejects to the journal. Every M5 close (~12/hour, ~1 per
        # 5-min worker cycle) adds ~54 new permanents. Strict post-condition
        # would observe these new arrivals and refuse to flip — meaning
        # FIRST-RUN never ends during active trading hours. That's wrong:
        # FIRST-RUN is about clearing the BACKLOG, not maintaining a perpetual
        # empty queue. The drained-starting-backlog trigger correctly fires
        # when the cycle's starting work is done; new mid-cycle arrivals
        # are next cycle's work, not extending FIRST-RUN.
        #
        # Edge cases handled correctly:
        #   * stop() interrupts Phase 1 mid-loop (only 50 of 100 processed):
        #     starting=100, processed=50, drained=50 != 0 -> no flip.
        #   * Mid-cycle transient-cap escalations create new permanents:
        #     they're not in starting count, get processed next cycle.
        #     FIRST-RUN flips on the cycle that drained the original
        #     backlog; the escalations are noise on top of steady state.
        #   * Steady-state M5 arrivals: not counted in starting backlog;
        #     don't prevent flip. Subsequent cycles handle them normally.
        drained_starting_backlog = (
            permanents_at_start - permanent_failed_count == 0
        )
        will_flip = self._first_run_active and drained_starting_backlog
        if will_flip:
            self._first_run_active = False
            elapsed_min = (time.time() - self._first_run_start_ts) / 60.0
            logger.info(
                "[SIM-WORKER] FIRST-RUN -> STEADY: starting backlog cleared. "
                "Drained %d permanent-FAILEDs from cycle-start queue. "
                "Cumulative across %d cycles (%.1f min): %d permanent-FAILEDs. "
                "Sv2 shadow recompute now in steady state.",
                permanent_failed_count,
                self._cycles_completed + 1,
                elapsed_min,
                self._total_permanent_failed + permanent_failed_count,
            )

        # Update accumulators (cycle_complete signal sees the cumulative
        # totals in get_stats(), but emits only the per-cycle deltas)
        self._total_permanent_failed += permanent_failed_count
        self._total_records_simulated += sim_results["succeeded"]
        self._total_calibrations_written += calibrations_written

        return {
            "permanent_failed_this_cycle": permanent_failed_count,
            "sim_attempted_this_cycle": sim_results["attempted"],
            "sim_succeeded_this_cycle": sim_results["succeeded"],
            "sim_transient_retries_this_cycle": sim_results["transient_retries"],
            "calibrations_written_this_cycle": calibrations_written,
            "first_run_active": self._first_run_active,
        }

    # ── Phase 1: permanent-FAILED fast-path ─────────────────────────

    def _process_permanent_fastpath(self, pending: list) -> int:
        """Mark records that simulate() would FAIL on permanently.

        Identifies via _is_permanent_failure_record (same logic as the
        simulator's defensive guards). Calls mark_permanent_failed
        with force_flush=False — the per-record disk writes are
        throttled by ShadowLogger's 30-sec mechanism, and the cycle's
        end-of-batch force_flush guarantees durability.

        Returns: number of records marked this cycle. Bounded by
        max_permanent_per_cycle.
        """
        permanent_candidates = [
            r for r in pending if _is_permanent_failure_record(r)
        ]
        n_to_process = min(self.max_permanent_per_cycle, len(permanent_candidates))
        processed = 0
        for r in permanent_candidates[:n_to_process]:
            if not self._running:
                break
            try:
                reason = _classify_permanent_reason(r)
                ok = self._logger.mark_permanent_failed(
                    r.shadow_id, reason, force_flush=False,
                )
                if ok:
                    processed += 1
            except Exception as exc:
                # Per-record isolation — one bad record can't kill
                # the cycle. Log, continue.
                logger.warning(
                    "[SIM-WORKER] mark_permanent_failed raised on shadow_id=%s: %s",
                    r.shadow_id, exc,
                )
        return processed

    # ── Phase 2: real simulation batch ──────────────────────────────

    def _process_real_sims(self, pending: list) -> dict:
        """Simulate non-permanent-failure records, classify outcomes.

        Returns dict with attempted / succeeded / transient_retries counts.
        """
        real_candidates = [
            r for r in pending if not _is_permanent_failure_record(r)
        ]
        n_to_process = min(self.max_per_cycle, len(real_candidates))

        attempted = 0
        succeeded = 0
        transient_retries = 0

        for r in real_candidates[:n_to_process]:
            if not self._running:
                break
            try:
                outcome = self._simulator.simulate(r)
                attempted += 1
                self._dispatch_outcome(r, outcome)
                if outcome.sim_exit_reason == "FAILED":
                    if _is_transient_failure_reason(outcome.sim_failure_reason):
                        transient_retries += 1
                else:
                    succeeded += 1
            except Exception as exc:
                logger.warning(
                    "[SIM-WORKER] simulate() raised on shadow_id=%s: %s",
                    r.shadow_id, exc,
                )

        return {
            "attempted": attempted,
            "succeeded": succeeded,
            "transient_retries": transient_retries,
        }

    # ── Phase 3 (D.3): calibration drain ────────────────────────────

    def _process_calibrations(self) -> int:
        """Drain pending_calibration: write deltas for EXECUTED records
        whose real trade is now closed.

        Single-cycle latency by design: an EXECUTED record's sim runs
        first (Phase 2), the record gets sim_completed=True, and the
        record then shows up in pending_calibration starting the
        NEXT cycle. The next cycle's _process_calibrations looks up
        the real trade — if closed, write calibration; if still open,
        the record waits for a future cycle. Worst-case wall-clock
        latency from real-trade-close to calibration-write: one
        poll_interval (5 minutes default).

        Returns: number of calibrations written this cycle.
        """
        if self._real_trade_lookup is None:
            return 0  # no lookup wired (e.g., D.3 unit tests without main_window)

        pending_cal = self._logger.pending_calibration()
        if not pending_cal:
            return 0

        # Late import to avoid touching shadow_simulator at module-load
        # time (keeps import order simple; shadow_simulator imports
        # shadow_logger but not vice versa).
        from takumi_trader.core.shadow_simulator import SimulatedOutcome

        written = 0
        for record in pending_cal:
            if not self._running:
                break
            try:
                real = self._real_trade_lookup(record)
            except Exception as exc:
                logger.warning(
                    "[SIM-WORKER] real_trade_lookup raised shadow_id=%s: %s",
                    record.shadow_id, exc,
                )
                continue
            if real is None:
                # Real trade still open — wait for a future cycle.
                continue

            try:
                # Reconstruct SimulatedOutcome from the record's persisted
                # sim_* fields. write_calibration only needs sim_pnl_pips,
                # sim_exit_reason, sim_duration_minutes, sim_pessimism_applied
                # — those all survive on the record after write_simulation.
                outcome = SimulatedOutcome(
                    sim_exit_time=record.sim_exit_time,
                    sim_exit_price=record.sim_exit_price,
                    sim_exit_reason=record.sim_exit_reason,
                    sim_pnl_pips=record.sim_pnl_pips,
                    sim_mae_pips=record.sim_mae_pips,
                    sim_mfe_pips=record.sim_mfe_pips,
                    sim_duration_minutes=int(record.sim_duration_minutes),
                    sim_pessimism_applied=record.sim_pessimism_applied,
                    sim_failure_reason=record.sim_failure_reason,
                )
                cal_ok = self._simulator.write_calibration(
                    record, outcome,
                    real_pnl_pips=real.pnl_pips,
                    real_exit_reason=real.exit_reason,
                    real_duration_minutes=real.duration_minutes,
                )
                if cal_ok:
                    # Mark this record's calibration done so it stops
                    # appearing in pending_calibration on next cycle.
                    self._logger.mark_calibration_completed(
                        record.shadow_id, force_flush=False,
                    )
                    written += 1
                    if not self._calibration_first_light_logged:
                        self._log_calibration_first_light(record, outcome, real)
                        self._calibration_first_light_logged = True
            except Exception as exc:
                logger.warning(
                    "[SIM-WORKER] write_calibration raised shadow_id=%s: %s",
                    record.shadow_id, exc,
                )
        return written

    def _log_calibration_first_light(self, record, outcome, real) -> None:
        """Emit the CALIBRATION-FIRST-LIGHT log entry.

        Fires exactly once across the worker's lifetime — when the very
        first calibration record gets written. This is the milestone
        moment: shadow logging stops being infrastructure and starts
        being information. Worth marking distinctly in logs so it's
        findable later, and so the operator (Ryosuke) has a concrete
        marker for when the calibration loop went live.
        """
        delta = real.pnl_pips - outcome.sim_pnl_pips
        cap = self._simulator.config.calibration_warn_after_n
        logger.info(
            "[SIM-WORKER CALIBRATION-FIRST-LIGHT] First calibration data point recorded.\n"
            "    shadow_id=%d, pair=%s %s, signal_time=%s\n"
            "    real_pnl=%+.2fp (%s, %.0fmin)\n"
            "    sim_pnl=%+.2fp (%s, %dmin)\n"
            "    delta=%+.2fp (positive = sim was pessimistic; negative = sim too optimistic)\n"
            "    Pessimism stamp: %s\n"
            "    Drift detection requires %d records to activate; warns if mean drifts beyond +/-%.1fp.",
            record.shadow_id, record.pair, record.direction,
            record.signal_time_str or "(unknown)",
            real.pnl_pips, real.exit_reason, real.duration_minutes,
            outcome.sim_pnl_pips, outcome.sim_exit_reason,
            outcome.sim_duration_minutes,
            delta,
            outcome.sim_pessimism_applied,
            cap, self._simulator.config.calibration_warn_band_pips,
        )

    def _dispatch_outcome(self, record, outcome) -> None:
        """Route a SimulatedOutcome to the correct ShadowLogger method.

        Three branches:
            FAILED + transient reason  -> bump_transient_retry; if cap
                                          exceeded, mark_permanent_failed
                                          with transient_giveup marker.
            FAILED + permanent reason  -> mark_permanent_failed with the
                                          simulator's reason verbatim.
            success (TP/SL/TIMEOUT)    -> write_simulation with full
                                          outcome data.
        """
        if outcome.sim_exit_reason == "FAILED":
            if _is_transient_failure_reason(outcome.sim_failure_reason):
                # Transient — bump count; if cap exceeded, escalate
                new_count = self._logger.bump_transient_retry(record.shadow_id)
                cap = self._simulator.config.transient_retry_max
                if new_count >= cap:
                    self._logger.mark_permanent_failed(
                        record.shadow_id,
                        f"transient_giveup_after_{new_count}_retries:"
                        f"{outcome.sim_failure_reason}",
                        force_flush=False,
                    )
            else:
                # Permanent failure surfaced by simulate() — record it
                self._logger.mark_permanent_failed(
                    record.shadow_id,
                    outcome.sim_failure_reason or "unknown_permanent",
                    force_flush=False,
                )
        else:
            # Success — full write_simulation
            self._logger.write_simulation(
                record.shadow_id,
                sim_exit_time=outcome.sim_exit_time,
                sim_exit_price=outcome.sim_exit_price,
                sim_exit_reason=outcome.sim_exit_reason,
                sim_pnl_pips=outcome.sim_pnl_pips,
                sim_mae_pips=outcome.sim_mae_pips,
                sim_mfe_pips=outcome.sim_mfe_pips,
                sim_duration_minutes=outcome.sim_duration_minutes,
                sim_pessimism_applied=outcome.sim_pessimism_applied,
                features=outcome.features,
                sim_failure_reason="",  # not failed
                force_flush=False,
            )

    # ── Helpers ─────────────────────────────────────────────────────

    # ── Helpers ─────────────────────────────────────────────────────

    def _sleep_interruptible(self, seconds: float) -> None:
        """Sleep in _SLEEP_STEP-sized chunks, checking self._running
        each step. Allows stop() to interrupt sleep within ~100ms.
        Same pattern as MT5Worker.run()."""
        if seconds <= 0:
            return
        slept = 0.0
        while slept < seconds and self._running:
            step = min(self._SLEEP_STEP, seconds - slept)
            time.sleep(step)
            slept += step


# ─────────────────────────────────────────────────────────────────────
# Module-level classifiers (pure functions, used by the worker)
# ─────────────────────────────────────────────────────────────────────

# Failure-reason prefixes that the simulator returns for PERMANENT failures.
# Matches the FAILED returns in shadow_simulator.simulate's defensive guards.
# Anything NOT matching one of these prefixes is treated as TRANSIENT (retry-
# eligible) — defensive policy of preferring retry over silent drop on
# unrecognized failure shapes.
_PERMANENT_FAILURE_PREFIXES: tuple[str, ...] = (
    "strength_reject_no_snapshot",
    "invalid_proposal_prices",
    "invalid_direction",
    "insufficient_m1_bars",
)


def _is_transient_failure_reason(failure_reason: str) -> bool:
    """Classify a sim_failure_reason as transient (retry) vs permanent.

    Returns True if the reason should trigger a retry (bump
    transient_retry_count and try again next cycle). Returns False
    if the reason indicates a permanent fault that cannot be fixed
    by waiting (mark sim_completed=True with FAILED).

    Unknown reasons default to transient — when in doubt, retry.
    The transient_retry_max cap prevents infinite loops on records
    whose underlying issue won't resolve.
    """
    if not failure_reason:
        return True  # empty reason = unknown; retry
    return not any(
        failure_reason.startswith(p) for p in _PERMANENT_FAILURE_PREFIXES
    )


def _is_permanent_failure_record(r) -> bool:
    """Records that simulate() will FAIL on permanently — fast-path them.

    Mirrors the simulator's defensive guards in shadow_simulator.simulate's
    Step 1 (input validation). Catching these in the worker avoids
    invoking simulate() entirely — which would just return FAILED with
    one of these reasons and waste CPU.

    Returns True if the record can be marked permanent-FAILED without
    invoking the simulator. False if simulate() should be called.
    """
    # Strength-rejects: lightweight records with no input_snapshot.
    # The simulator's Step 1 returns FAILED("strength_reject_no_snapshot")
    # for these. Fast-path them — ~99% of records on first run.
    if r.block_gate == "strength_engine" and not r.input_snapshot_json:
        return True
    # Invalid proposal prices (defensive — shouldn't happen on real records)
    if r.proposed_entry <= 0 or r.proposed_sl_price <= 0 or r.proposed_tp_price <= 0:
        return True
    # Invalid direction (defensive)
    if r.direction not in ("BUY", "SELL"):
        return True
    return False


def _classify_permanent_reason(r) -> str:
    """Map a record's permanent-failure shape to a stable failure reason.

    Returns a string from the same vocabulary the simulator uses (in
    shadow_simulator._failed) so Edge Miner queries see consistent
    reason strings whether the record was fast-pathed or simulated.

    Order matches _is_permanent_failure_record's check order.
    """
    if r.block_gate == "strength_engine" and not r.input_snapshot_json:
        return "strength_reject_no_snapshot"
    if r.proposed_entry <= 0 or r.proposed_sl_price <= 0 or r.proposed_tp_price <= 0:
        return "invalid_proposal_prices"
    if r.direction not in ("BUY", "SELL"):
        return f"invalid_direction:{r.direction!r}"
    return "unknown_permanent"


# ─────────────────────────────────────────────────────────────────────
# real_trade_lookup factory (D.4)
# ─────────────────────────────────────────────────────────────────────

def make_paper_trade_lookup(journal_path: Path) -> Callable:
    """Build a closure that resolves EXECUTED shadow records to closed
    RealTradeOutcome via paper_trader's on-disk journal.

    Disk-read (NOT a held reference to PaperTrader): the worker runs
    in its own QThread, paper_trader runs on the main thread, and
    save_journal writes the full journal on every close. Reading from
    disk with an mtime-cached refresh is the cleanest thread-safe
    boundary — no shared lock, no main-thread call from worker code.

    Concurrency note: paper_trader.save_journal uses a single
    write_text (no os.replace temp swap). On Windows the file is
    locked during write so a concurrent read either succeeds with the
    pre-write contents (if write hasn't started yet), succeeds with
    the new contents (if write completed), or raises OSError /
    JSONDecodeError mid-write. The factory treats read failure as
    "calibration not yet ready" — returns None and retries next
    cycle. Worst-case extra latency: one poll_interval (5 min).

    Failure modes (each returns None gracefully — worker retries
    automatically next cycle for non-permanent issues):
        * exec_lane != "paper"       — different lane (out of scope)
        * exec_ref_json empty/bad    — record missing/corrupt ref
        * journal_idx not int >= 0   — corrupt ref
        * idx >= len(journal)        — corruption (logged once / id)
        * pair mismatch at idx       — corruption (logged once / id)
        * close_time == 0            — trade still open (wait)
        * journal file missing       — startup race (wait)
        * concurrent write race      — disk OSError (wait)
        * malformed JSON             — concurrent write or corrupt (wait)

    Returns: closure (record) -> RealTradeOutcome | None
    """
    # mtime cache: only re-parse JSON when the file changes.
    # Sentinel mtime=-1.0 forces first-call refresh.
    cache: dict[str, Any] = {"mtime": -1.0, "data": []}

    # One-time corruption warnings per shadow_id — avoid log floods on
    # records that will keep failing every cycle until permanent_failed.
    warned: set[int] = set()

    def _refresh() -> bool:
        """Re-parse journal from disk if mtime advanced. Preserves
        previous cache on read/parse failure so steady-state reads
        succeed even when a write is racing."""
        try:
            mtime = journal_path.stat().st_mtime
        except OSError:
            return False
        if mtime == cache["mtime"] and cache["data"]:
            return True
        try:
            text = journal_path.read_text(encoding="utf-8")
            data = json.loads(text)
        except (OSError, json.JSONDecodeError):
            return False
        if not isinstance(data, list):
            return False
        cache["mtime"] = mtime
        cache["data"] = data
        return True

    def lookup(record) -> RealTradeOutcome | None:
        if record.exec_lane != "paper":
            return None
        if not record.exec_ref_json:
            return None
        try:
            ref = json.loads(record.exec_ref_json)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(ref, dict):
            return None
        idx = ref.get("journal_idx")
        if not isinstance(idx, int) or idx < 0:
            return None

        if not _refresh():
            # Either file is missing or we hit a transient read race;
            # let the worker try again next cycle.
            return None

        data = cache["data"]
        if idx >= len(data):
            if record.shadow_id not in warned:
                logger.warning(
                    "[SIM-WORKER LOOKUP] journal_idx=%d out of bounds "
                    "(journal len=%d) shadow_id=%d pair=%s — possible "
                    "corruption; this record will never calibrate",
                    idx, len(data), record.shadow_id, record.pair,
                )
                warned.add(record.shadow_id)
            return None

        entry = data[idx]
        if not isinstance(entry, dict):
            return None

        entry_pair = entry.get("pair", "")
        if entry_pair != record.pair:
            if record.shadow_id not in warned:
                logger.warning(
                    "[SIM-WORKER LOOKUP] pair mismatch at journal_idx=%d "
                    "shadow_id=%d expects %s, journal has %r — possible "
                    "corruption; this record will never calibrate",
                    idx, record.shadow_id, record.pair, entry_pair,
                )
                warned.add(record.shadow_id)
            return None

        close_time = entry.get("close_time", 0.0)
        close_reason = entry.get("close_reason", "")
        try:
            close_time_f = float(close_time)
        except (TypeError, ValueError):
            return None
        if close_time_f <= 0.0 or not close_reason:
            return None

        try:
            pnl = float(entry.get("pnl_pips", 0.0))
            duration = float(entry.get("duration_minutes", 0.0))
        except (TypeError, ValueError):
            return None

        return RealTradeOutcome(
            pnl_pips=pnl,
            exit_reason=str(close_reason),
            duration_minutes=duration,
        )

    return lookup
