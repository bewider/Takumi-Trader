"""Shadow-trade logging — Phase 1 of Edge Miner foundation.

The real trade journals (`paper_trades_<system>.json`) only contain trades
that passed every filter. That is survivorship bias at the data layer —
it makes any later "are my filters helping or hurting?" analysis
impossible because the filtered population was never recorded.

This module captures **every signal a strategy considers**, regardless
of whether downstream filters allow it through. Three event types per
shadow record:

  1. log_signal()       — emitted by each strategy AT signal generation,
                          BEFORE any filter runs.
  2. mark_decision()    — emitted at every filter / risk gate that
                          rejects the signal.
  3. mark_executed()    — emitted when the signal becomes a real trade,
                          recording which lane (paper / cTrader / mt5)
                          and the lane-specific reference id.

Storage: JSON-per-system (`data/shadow_trades_<strategy_id>.json`),
matching TAKUMI's existing journal pattern. Restart-safe via dataclass
+ generic `setattr` load (same approach as PaperTradeRecord and
TrackedTrade).

Flush policy (Addition 2 from the design review):
    log_signal / mark_decision / mark_executed all flush IMMEDIATELY.
    Shadow capture is the whole point — losing a record between capture
    and flush defeats the build. The 30-second throttle that
    PaperTrader uses for post-close watching is reserved for the
    simulator's bulk update cycle (Phase C), where many records get
    sim_pnl_pips written in one pass.

Pessimism note (carries through to Phase C):
    Real PaperTrader fills are OPTIMISTIC by design (paper_trader.py
    `_check_sl_tp` checks TP first when both SL and TP could fire in
    the same M1 candle). Shadow simulation will be PESSIMISTIC (SL
    first when ambiguous). The intentional asymmetry exposes how much
    of the real journal's edge survives a worst-case fill model — that
    is the very first valuable Edge Miner question.

Fields populated lazily by the simulator (Phase C) all start with
sim_* and are blank/zero on a fresh capture.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import MISSING, asdict, dataclass, field, fields
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ── Decision gates (categorical, used by mark_decision) ──────────────
#
# AUTHORITATIVE GATE VOCABULARY for Edge Miner queries.
#
# Every block site in TAKUMI's signal-flow path tags the rejection with
# exactly one of these constants. Edge Miner uses them to ask categorical
# questions like:
#
#   "Of all blocks attributable to gate X, what fraction would have been
#    profitable if allowed through?"
#   "Which specific gate has the highest false-positive rate?"
#   "Are filters net-positive on EUR pairs but net-negative on JPY pairs?"
#
# Any new gate added in the future MUST:
#   1. Be appended to this module's exports.
#   2. Be added to VALID_GATES (the runtime check).
#   3. Be wired at the corresponding hook point in mt5_worker / main_window.
#   4. Be documented in this docstring with the precise firing condition.
# Silent string mismatches between caller and gate vocabulary would split
# the data permanently — a typo'd gate name produces an orphan category
# that future Edge Miner queries silently exclude.
#
# ── The 10 gates ──
#
# GATE_STRENGTH_ENGINE  — `stoch_engine.check_entry` returns False due to
#                         the M5/M15 strength threshold (base ≥ min_strong
#                         AND quote ≤ min_weak), HTF blocks (H1 soft, H4/D1
#                         hard), or the velocity-AND veto (Apr 30). Fired
#                         from the worker-side capture itself when the
#                         strength gate rejects — this is the highest-
#                         volume gate by far (~99% of Sv2 evaluations).
#
# GATE_DIVERGENCE_SPREAD — main_window pre-conviction filter:
#                         |composite[base] - composite[quote]| < threshold
#                         (default 12.0 pts). Distinct from STRENGTH_ENGINE
#                         because it operates on the all-TF composite, not
#                         the M5/M15 raw scores; rejects a different
#                         population (pairs where M5/M15 disagree but
#                         composite is moderate).
#
# GATE_CONVICTION       — filter_engine.evaluate produces tier ∈ {DIMMED,
#                         SUPPRESSED} on grounds OTHER than structural —
#                         i.e., pure score-based ranking (low HTF trend,
#                         low velocity, low isolation). Distinguished from
#                         GATE_STRUCTURAL by checking
#                         conv.components["structural"].passed first.
#
# GATE_STRUCTURAL       — filter_engine HARD BLOCK on key-level proximity
#                         OR TP clearance failure (forces tier=SUPPRESSED
#                         regardless of score). Categorically different
#                         from CONVICTION because structural rejections
#                         are mechanical safety constraints ("entry would
#                         be unsafe"), not score judgments.
#
# GATE_ADR              — main_window ADR-consumed gate. Pair's session
#                         range is already > _ADR_QUALITY_MAX_PCT (70%) —
#                         "day's already moved enough, low-quality entry."
#                         Currently active for Sv2 + A-tuned only.
#
# GATE_NO_TRADE_WINDOW  — main_window NO_TRADE-window check. JST minute-of-
#                         day outside the trading window (default 07:58–
#                         21:59 JST), OR is_weekend() returns True (Fri
#                         19:00 UTC = Sat 04:00 JST onwards through Sun
#                         22:00 UTC; see session_manager.py for details).
#                         Scoped to pairs in full_candidates (post-
#                         conviction); not applied to upstream rejects.
#
# GATE_NEWS             — news_filter.is_blackout(pair) — currency-aware
#                         RED-news blackout. USDJPY blocks for both USD
#                         and JPY events.
#
# GATE_H1_SWEEP         — H1 liquidity_sweep_pattern detected (added Apr
#                         30): bearish_sweep blocks BUYs, bullish_sweep
#                         blocks SELLs. Falling-knife / pump-and-dump
#                         protection. Fired in mt5_worker before the pair
#                         enters stoch_entry_candidates.
#
# GATE_DUPLICATE        — paper_trader.has_trade(pair) returned True —
#                         already an open Sv2 position for this pair, so
#                         the new signal is dropped to enforce the single-
#                         position-per-pair rule. Distinct categorical
#                         population from GATE_INTERNAL because it
#                         answers a real strategy-design question:
#                         "would the second signal have been profitable?"
#
# GATE_INTERNAL         — Catch-all for defensive / data-quality blocks
#                         that aren't a meaningful Edge Miner category
#                         (bad entry price, NaN data, unreachable
#                         defensive code paths). Reason string carries
#                         the specific case. Kept tight on purpose —
#                         if a population shows up here repeatedly, it
#                         deserves promotion to its own gate.

GATE_STRENGTH_ENGINE = "strength_engine"
GATE_DIVERGENCE_SPREAD = "divergence_spread"
GATE_CONVICTION = "conviction"
GATE_STRUCTURAL = "structural"
GATE_ADR = "adr"
GATE_NO_TRADE_WINDOW = "no_trade_window"
GATE_NEWS = "news"
GATE_H1_SWEEP = "h1_sweep"
GATE_DUPLICATE = "duplicate"
GATE_INTERNAL = "internal"

VALID_GATES = frozenset({
    GATE_STRENGTH_ENGINE, GATE_DIVERGENCE_SPREAD, GATE_CONVICTION,
    GATE_STRUCTURAL, GATE_ADR, GATE_NO_TRADE_WINDOW, GATE_NEWS,
    GATE_H1_SWEEP, GATE_DUPLICATE, GATE_INTERNAL,
})


# ── Outcome statuses ─────────────────────────────────────────────────
STATUS_PENDING = "PENDING"          # captured, no decision yet — orphan if seen on reload
STATUS_EXECUTED = "EXECUTED"        # became a real trade (paper / cTrader / mt5)
STATUS_BLOCKED = "BLOCKED"          # rejected by a gate
STATUS_SKIPPED_NO_DATA = "SKIPPED_NO_DATA"  # M1 history unavailable for sim

VALID_STATUSES = frozenset({
    STATUS_PENDING, STATUS_EXECUTED, STATUS_BLOCKED, STATUS_SKIPPED_NO_DATA,
})


# ── Execution lanes ──────────────────────────────────────────────────
LANE_PAPER = "paper"
LANE_CTRADER = "ctrader"
LANE_MT5 = "mt5"

VALID_LANES = frozenset({LANE_PAPER, LANE_CTRADER, LANE_MT5})


# ─────────────────────────────────────────────────────────────────────
# SCHEMA-EXTENSION PROTOCOL (locked 2026-05-06 from Phase D.1 review)
# ─────────────────────────────────────────────────────────────────────
# When extending ShadowSignalRecord with a new field, follow these
# four steps to preserve backward compatibility with on-disk records:
#
#   1. Add the field with a sensible default (typically 0, "", False,
#      or None). The default MUST be the value that historical records
#      "would have had" if the field had existed all along.
#
#   2. Verify sparse serialization correctly omits the default. After
#      adding the field, write one record with the field at default
#      and one with a non-default value; check the on-disk JSON shows
#      the field key only on the non-default record. _compact_record_dict
#      handles this automatically — but if you change defaults later,
#      verify again.
#
#   3. Test schema evolution explicitly. Load the production journal
#      (or any existing file) and confirm zero load errors, all records
#      reload with the new field at its default. Phase A's setattr-load
#      pattern handles missing fields automatically; this test is the
#      explicit confirmation that it still does.
#
#   4. Document the schema change. Add a comment on the field
#      declaration with the date and rationale, formatted like:
#         field_name: type = default  # Added YYYY-MM-DD (Phase X.Y)
#                                       for <reason>
#
# Reusable across all future schema extensions. The pattern is:
# add → verify omission → test load → document. Don't skip any step.
# ─────────────────────────────────────────────────────────────────────


@dataclass
class ShadowSignalRecord:
    """One captured signal — the unit of the shadow journal.

    Field grouping mirrors PaperTradeRecord's layout for cross-table
    joinability at Edge Miner time. New fields can be appended without
    breaking old saves because load uses generic setattr (any unknown
    field on disk is dropped, any new field on the dataclass gets its
    default value when loading old records).

    Schema-extension protocol: see the SCHEMA-EXTENSION PROTOCOL block
    above this dataclass before adding any new field.

    ── Schema-evolution map (F.12 audit 2026-05-14) ──
    Five field groups across the record's lifecycle. Every field is
    sparse-omitted at default — round-trip is lossless because load
    restores defaults for absent fields.

    1. Identity (set by log_signal at signal time, immutable thereafter)
       shadow_id, strategy_id, signal_time, signal_time_str, pair, direction

    2. Proposed parameters (set by log_signal, immutable)
       proposed_entry, proposed_sl_price, proposed_tp_price,
       proposed_sl_pips, proposed_tp_pips, proposed_lot_size

    3. Decision outcome (set by mark_decision / mark_executed)
       status, block_gate, block_reason, block_metadata_json,
       exec_lane, exec_ref_json
       Lifecycle: PENDING (default) -> BLOCKED OR EXECUTED OR FAILED

    4. Input snapshot for lazy feature recompute (set by log_signal)
       input_snapshot_json
       Phase C ShadowSimulator reads this to recompute the ~143 feat_*
       at sim time without blocking the trading loop.

    5. Simulation + worker state (set by ShadowSimulator + ShadowSimWorker)
       sim_completed                  Phase C   — terminal sim flag
       sim_exit_time/price/reason     Phase C   — exit details
       sim_pnl_pips, sim_mae_pips,    Phase C   — outcome metrics
         sim_mfe_pips, sim_duration_minutes
       sim_pessimism_applied          Phase C   — stamp for the config used
       sim_completed_at               Phase C   — wall-clock of sim run
       sim_failure_reason             Phase D.2 — sparse, only on FAILED
       transient_retry_count          Phase D.1 — sparse, 0 = never retried
       calibration_completed          Phase D.3 — sparse, dual-completion flag

       Why dual completion for EXECUTED records (sim_completed AND
       calibration_completed): the real trade close may lag the
       sim cycle by hours. Without separate flags, EXECUTED records
       would leave pending_simulation after first sim, and calibration
       could never write for trades closing after that point.

    Audit fields: captured_at, last_updated.

    F.12 conclusion (2026-05-14): all three Phase D additions
    (sim_failure_reason, transient_retry_count, calibration_completed)
    are correctly sparse-optional. None promoted to required because
    their meaningful population is conditional (FAILED-only, retry-bumped-
    only, EXECUTED-and-calibrated-only respectively). The sparse-
    serialization protocol keeps the journal compact under all three
    populated patterns.
    """
    # Identity
    shadow_id: int = 0                  # monotonic within this strategy's journal file
    strategy_id: str = ""               # "Sv2", "Sv2-tuned", "AU1", etc.
    signal_time: float = 0.0            # epoch UTC
    signal_time_str: str = ""           # JST display string
    pair: str = ""
    direction: str = ""                 # "BUY" | "SELL"

    # Proposed parameters at signal generation time
    proposed_entry: float = 0.0
    proposed_sl_price: float = 0.0
    proposed_tp_price: float = 0.0
    proposed_sl_pips: float = 0.0
    proposed_tp_pips: float = 0.0
    proposed_lot_size: float = 0.0      # 0.0 if not yet sized

    # Outcome (decision phase, set by mark_decision / mark_executed)
    status: str = STATUS_PENDING
    block_gate: str = ""                # one of VALID_GATES, empty if not BLOCKED
    block_reason: str = ""              # free-form string from the gate
    block_metadata_json: str = ""       # JSON-serialized context dict
    exec_lane: str = ""                 # one of VALID_LANES, empty if not EXECUTED
    exec_ref_json: str = ""             # JSON-serialized lane-specific ref dict

    # Input snapshot for lazy feature recompute (see schema-evolution
    # map in class docstring §4)
    input_snapshot_json: str = ""

    # Simulation outcome — see schema-evolution map §5 for the full
    # set including the Phase D sparse-optional additions.
    sim_completed: bool = False
    sim_exit_time: float = 0.0
    sim_exit_price: float = 0.0
    sim_exit_reason: str = ""           # "TP" | "SL" | "TIMEOUT" | "FAILED"
    sim_pnl_pips: float = 0.0
    sim_mae_pips: float = 0.0
    sim_mfe_pips: float = 0.0
    sim_duration_minutes: float = 0.0
    sim_pessimism_applied: str = ""     # e.g. "worst_case_fill+spread+slip_2pt+sl_first"
    sim_completed_at: float = 0.0
    sim_failure_reason: str = ""        # Phase D.2 sparse-optional; see docstring §5

    # Features (set by ShadowSimulator after lazy recompute; large
    # JSON blob — 143 keys at time of writing; flexible for evolution)
    features_json: str = ""

    # Audit
    captured_at: float = 0.0
    last_updated: float = 0.0

    # Phase D worker state — see schema-evolution map §5
    transient_retry_count: int = 0       # Phase D.1 sparse-optional
    calibration_completed: bool = False  # Phase D.3 sparse-optional


def _compact_record_dict(rec: ShadowSignalRecord) -> dict[str, Any]:
    """Serialize a ShadowSignalRecord as a sparse dict.

    Fields whose value equals the dataclass-declared default are omitted.
    Loading (via setattr in `_load_journal`) restores defaults for any
    field absent from the on-disk dict, so this is round-trip lossless.

    Why: 99% of records are strength-rejects with most proposal / sim /
    exec fields untouched at default. Omitting the noise saves ~50% on
    disk versus a full asdict() dump. Combined with compact JSON
    formatting, on-disk size drops from ~1.1 KB to ~600 B per reject —
    matches the design-review budget exactly.

    ── Format choice rationale (read before "harmonizing" with PaperTrader) ──
    Shadow journals use COMPACT JSON (no indent) AND sparse-dict
    serialization. PaperTrader journals use indented JSON with full
    asdict(). The asymmetry is DELIBERATE and reflects volume regime,
    not stylistic drift:

        PaperTrader:  ~600 records / 6 weeks per system  (~100/week)
                      indented JSON aids manual debugging; size cost trivial.
        ShadowLogger: ~15,000 records / DAY per system   (~150x density)
                      compact + sparse drops on-disk size from ~1.1 KB to
                      ~600 B per reject. At 22 systems post-fan-out, the
                      saving is ~150 MB/day. Manual readability is sacrificed
                      to keep the storage projection sustainable.

    If a human ever needs to read a shadow journal, `python -m json.tool`
    or any IDE pretty-printer reformats them on demand. Loading code
    (Phase A `_load_journal`) is format-agnostic — it accepts compact,
    indented, sparse, or full forms identically, so changing format
    later (e.g., parquet rollover) doesn't require a migration script.
    """
    out: dict[str, Any] = {}
    for f in fields(ShadowSignalRecord):
        val = getattr(rec, f.name)
        if f.default is not MISSING:
            default = f.default
        elif f.default_factory is not MISSING:  # type: ignore[misc]
            default = f.default_factory()  # type: ignore[misc]
        else:
            default = None
        if val != default:
            out[f.name] = val
    return out


@dataclass
class ShadowCalibrationRecord:
    """Sim-vs-real delta for one EXECUTED parity sim (Addition 1).

    Written by Phase C when simulation runs on an EXECUTED record. Used
    to empirically calibrate pessimism: if sim consistently underprints
    real outcomes by, say, -0.5p mean, the slippage model is too harsh.
    Without this we'd be guessing at pessimism levels forever.
    """
    shadow_id: int = 0
    strategy_id: str = ""
    pair: str = ""
    direction: str = ""
    signal_time: float = 0.0
    real_pnl_pips: float = 0.0          # from the linked real trade journal
    sim_pnl_pips: float = 0.0           # from the shadow simulator
    delta_pips: float = 0.0             # real - sim (positive = sim was pessimistic)
    real_exit_reason: str = ""
    sim_exit_reason: str = ""
    real_duration_minutes: float = 0.0
    sim_duration_minutes: float = 0.0
    pessimism_applied: str = ""
    written_at: float = 0.0


# ─────────────────────────────────────────────────────────────────────
# ShadowLogger
# ─────────────────────────────────────────────────────────────────────

class ShadowLogger:
    """Per-strategy shadow journal: capture → decide → execute → simulate.

    One ShadowLogger instance per strategy (e.g., Sv2 has its own).
    The journal file is `data/shadow_trades_<strategy_id>.json`.

    Restart safety: on construction, the journal is loaded into
    memory. Mutations update the in-memory list and then either flush
    immediately (force_flush=True — used by mark_executed for
    calibration-linkage durability) or are throttled (force_flush=
    False — used by the hot-path log_signal / log_strength_reject /
    mark_decision capture path). The throttle window is
    _FLUSH_THROTTLE_SEC; mutations within the window stay in memory
    only until the next throttle interval OR force_flush() is called
    (e.g., on shutdown).

    F.1 (2026-05-14): the throttle was extended to log_signal,
    log_strength_reject, and mark_decision. Previously these flushed
    on every call, which became a main-thread blocker as the journal
    grew (~500ms per 90 MB flush at 104 K records). The hot-path
    log_strength_reject in particular fires ~15-20 K times/day, so
    immediate-flush imposed enormous unnecessary I/O. Throttle window
    of 30s gives at-most-30s recovery loss on crash — observational
    records, not trading-critical state, so acceptable trade-off.
    mark_executed retains force_flush=True default for the rare (low
    frequency) but durability-critical exec_ref linkage.
    """

    # Throttle window for non-force-flush mutations. Worker's
    # cycle-end force_flush + closeEvent's explicit force_flush
    # bound the worst-case durability gap to this many seconds.
    _FLUSH_THROTTLE_SEC = 30.0
    # Backward-compat alias (callers may reference the old name)
    _SIM_UPDATE_FLUSH_THROTTLE_SEC = _FLUSH_THROTTLE_SEC

    def __init__(self, strategy_id: str, journal_path: Path) -> None:
        if not strategy_id:
            raise ValueError("strategy_id required")
        self._strategy_id = strategy_id
        self._journal_path = Path(journal_path)
        self._journal: list[ShadowSignalRecord] = []
        self._next_shadow_id: int = 1
        self._last_flush: float = 0.0
        # Backward-compat alias for callers that read the old name
        self._sim_update_last_flush: float = 0.0
        self._load_journal()

    # ── Load / save ─────────────────────────────────────────────────

    def _load_journal(self) -> None:
        """Restore from disk. Missing file = empty journal (first run)."""
        if not self._journal_path.exists():
            return
        try:
            data = json.loads(self._journal_path.read_text(encoding="utf-8"))
        except Exception as exc:
            # Don't crash on a bad file — log loudly, treat as empty so
            # capture can still proceed. Previous journal is preserved
            # on disk; operator can investigate.
            logger.error(
                "[SHADOW %s] Failed to load %s: %s — starting empty (existing file preserved)",
                self._strategy_id, self._journal_path, exc,
            )
            return
        if not isinstance(data, list):
            logger.error(
                "[SHADOW %s] %s is not a list — starting empty",
                self._strategy_id, self._journal_path,
            )
            return
        max_id = 0
        for d in data:
            r = ShadowSignalRecord()
            for k, v in d.items():
                # Generic setattr load — drops unknown keys, defaults
                # missing fields. Same pattern as PaperTrader.
                if hasattr(r, k):
                    setattr(r, k, v)
            self._journal.append(r)
            if r.shadow_id > max_id:
                max_id = r.shadow_id
        self._next_shadow_id = max_id + 1
        logger.info(
            "[SHADOW %s] Loaded %d records from %s (next id=%d)",
            self._strategy_id, len(self._journal), self._journal_path,
            self._next_shadow_id,
        )

    def _flush_atomic(self) -> None:
        """Write journal to disk via tmp+rename so a crash mid-write
        cannot corrupt the existing file. Existing file is replaced
        atomically by os.replace (POSIX rename semantics on Windows
        as of Python 3.3+).

        Two volume optimizations vs PaperTrader's indent=2 format:
          1. COMPACT JSON (no whitespace) — saves ~50% per record.
          2. SPARSE serialization — fields equal to their dataclass
             default are omitted; dataclass defaults restore them on
             load. Strength-reject records leave most fields at
             defaults (proposal=0, sim_*=0, exec_*="", input_snapshot
             =""), so omitting them saves another ~50%.

        Together: ~600 B/strength-reject on disk vs ~1.1 KB indented
        full-form. At 15K rejects/day for Sv2, the difference is ~7
        MB/day; multiplied across 22 systems post-fan-out, ~150 MB/day.
        Material to the long-term storage projection.

        Loss-less because: dataclass loading already drops unknown
        keys and defaults missing keys (Phase A test [3] confirmed this
        path). A field saved at value v is restored as v; a field
        omitted is restored as the dataclass's default — same outcome.
        """
        try:
            self._journal_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._journal_path.with_suffix(
                self._journal_path.suffix + ".tmp"
            )
            payload = [_compact_record_dict(r) for r in self._journal]
            tmp.write_text(
                json.dumps(payload, separators=(",", ":"), default=str),
                encoding="utf-8",
            )
            import os
            os.replace(tmp, self._journal_path)
            # F.1: stamp the last-flush time inside _flush_atomic so
            # ALL throttle-aware methods share one source of truth.
            # The backward-compat alias keeps Phase D worker code
            # working even though they read the old name.
            _now = time.time()
            self._last_flush = _now
            self._sim_update_last_flush = _now
        except Exception as exc:
            # Flush failure must NEVER take down the trading loop.
            # Log and move on — caller's in-memory record is still
            # alive; next flush attempt will retry.
            logger.error(
                "[SHADOW %s] Flush failed for %s: %s",
                self._strategy_id, self._journal_path, exc,
            )

    # ── Public API: capture ─────────────────────────────────────────

    def log_signal(
        self,
        pair: str,
        direction: str,
        proposed_entry: float,
        proposed_sl_price: float,
        proposed_tp_price: float,
        proposed_sl_pips: float,
        proposed_tp_pips: float,
        signal_time: float | None = None,
        signal_time_str: str = "",
        proposed_lot_size: float = 0.0,
        input_snapshot: dict | None = None,
        force_flush: bool = False,
    ) -> int:
        """Capture a candidate signal, BEFORE any filter runs.

        Returns the shadow_id, which the caller MUST attach to the
        signal object so downstream gates and execution paths can
        reference it via mark_decision / mark_executed.

        input_snapshot is a dict with whatever the lazy feature
        recomputer needs at sim time — typically composite_scores,
        prev_csi, cross_pair_close_prices, M1 history reference.
        Keep small: stored as a JSON blob in the journal record.

        F.1 (2026-05-14): throttled flush by default. The in-memory
        record is appended immediately; disk flush happens on the
        next _FLUSH_THROTTLE_SEC boundary OR on force_flush(). Worker
        cycle-end + closeEvent shutdown bracket the worst-case loss
        window. Pass force_flush=True if you absolutely need immediate
        disk durability (rare — Phase B gate sites don't).
        """
        if direction not in ("BUY", "SELL"):
            raise ValueError(f"direction must be BUY or SELL, got {direction!r}")
        if not pair:
            raise ValueError("pair required")

        now = time.time() if signal_time is None else float(signal_time)
        rec = ShadowSignalRecord(
            shadow_id=self._next_shadow_id,
            strategy_id=self._strategy_id,
            signal_time=now,
            signal_time_str=signal_time_str,
            pair=pair,
            direction=direction,
            proposed_entry=float(proposed_entry),
            proposed_sl_price=float(proposed_sl_price),
            proposed_tp_price=float(proposed_tp_price),
            proposed_sl_pips=float(proposed_sl_pips),
            proposed_tp_pips=float(proposed_tp_pips),
            proposed_lot_size=float(proposed_lot_size),
            status=STATUS_PENDING,
            input_snapshot_json=(
                json.dumps(input_snapshot, default=str)
                if input_snapshot else ""
            ),
            captured_at=time.time(),
            last_updated=time.time(),
        )
        self._journal.append(rec)
        self._next_shadow_id += 1
        now = time.time()
        if force_flush or (now - self._last_flush) >= self._FLUSH_THROTTLE_SEC:
            self._flush_atomic()
        return rec.shadow_id

    # ── Public API: lightweight strength-reject capture ───────────

    def log_strength_reject(
        self,
        pair: str,
        direction: str,
        reason: str,
        m5_base: float, m5_quote: float,
        m15_base: float, m15_quote: float,
        h1_base: float, h1_quote: float,
        h4_base: float, h4_quote: float,
        d1_base: float, d1_quote: float,
        spread_points: float,
        m5_atr_pips: float,
        h1_atr_pips: float,
        usd_score: float,
        ccy_dispersion: float,
        session: str,
        signal_time: float | None = None,
        signal_time_str: str = "",
        force_flush: bool = False,
    ) -> int:
        """One-call lightweight capture for strength-gate rejects.

        Creates a single record with status=BLOCKED and gate=strength_engine
        in the journal, with no input_snapshot (rejects don't get
        simulated, so they don't need lazy feature recompute).

        block_metadata_json carries the 15-float + 1-string panel the
        Edge Miner design review specified — every input that was
        available at M5 close and could affect a "should I trade?"
        decision, even though the strength gate itself didn't use them
        all. Edge Miner needs these to ask:
          "Does Sv2 reject too many M5+M15-weak setups during strong
           H4 trends?" — requires H4 scores in rejected records.
          "Is the strength gate's calibration tighter than necessary
           when CCY dispersion is high?" — requires dispersion.
          "Are rejects happening more during specific sessions?" —
           requires session tag.

        Returns shadow_id of the newly-created record.

        Equivalent to:
            sid = log_signal(... empty proposal ..., input_snapshot=None)
            mark_decision(sid, BLOCKED, GATE_STRENGTH_ENGINE, reason, metadata=...)
        but in one flush, not two — saves disk I/O for the hot path
        (15,000+ of these per day vs ~30 strength-passes).
        """
        now = time.time() if signal_time is None else float(signal_time)
        rec = ShadowSignalRecord(
            shadow_id=self._next_shadow_id,
            strategy_id=self._strategy_id,
            signal_time=now,
            signal_time_str=signal_time_str,
            pair=pair,
            direction=direction,
            # Proposal fields stay at zero — strength-rejects have no entry
            # to simulate. Detected at sim time and skipped.
            status=STATUS_BLOCKED,
            block_gate=GATE_STRENGTH_ENGINE,
            block_reason=reason,
            block_metadata_json=json.dumps({
                "m5_base": round(m5_base, 2), "m5_quote": round(m5_quote, 2),
                "m15_base": round(m15_base, 2), "m15_quote": round(m15_quote, 2),
                "h1_base": round(h1_base, 2), "h1_quote": round(h1_quote, 2),
                "h4_base": round(h4_base, 2), "h4_quote": round(h4_quote, 2),
                "d1_base": round(d1_base, 2), "d1_quote": round(d1_quote, 2),
                "spread_points": round(spread_points, 1),
                "m5_atr_pips": round(m5_atr_pips, 2),
                "h1_atr_pips": round(h1_atr_pips, 2),
                "usd_score": round(usd_score, 2),
                "ccy_dispersion": round(ccy_dispersion, 3),
                "session": session,
            }, default=str),
            captured_at=time.time(),
            last_updated=time.time(),
        )
        self._journal.append(rec)
        self._next_shadow_id += 1
        now = time.time()
        if force_flush or (now - self._last_flush) >= self._FLUSH_THROTTLE_SEC:
            self._flush_atomic()
        return rec.shadow_id

    # ── Public API: mark decision (block / skip) ───────────────────

    def mark_decision(
        self,
        shadow_id: int,
        status: str,
        gate: str = "",
        reason: str = "",
        metadata: dict | None = None,
        force_flush: bool = False,
    ) -> bool:
        """Attach a block / skip outcome to a captured signal.

        For BLOCKED status, gate must be one of VALID_GATES so Edge
        Miner can ask categorical questions. Free-form `reason` carries
        the human-readable detail (e.g., "H4 against: GBP=2.1/JPY=7.8").

        Returns True if the record was found and updated; False if the
        shadow_id is unknown (caller may have already marked the
        record, or the id is wrong — both are non-fatal anomalies).

        Idempotent: a record marked BLOCKED then marked BLOCKED again
        with the same gate is a no-op. Re-marking with a different
        terminal status is logged as a warning but accepted (the most
        recent decision wins; this can happen if a downstream gate
        fires after an upstream gate already rejected — rare but real).
        """
        if status not in VALID_STATUSES:
            raise ValueError(f"status must be in {VALID_STATUSES}, got {status!r}")
        if status == STATUS_BLOCKED and gate not in VALID_GATES:
            raise ValueError(
                f"gate must be in {VALID_GATES} for BLOCKED, got {gate!r}"
            )
        if status == STATUS_EXECUTED:
            raise ValueError(
                "use mark_executed() for EXECUTED status (it carries lane+ref)"
            )

        rec = self._find(shadow_id)
        if rec is None:
            logger.warning(
                "[SHADOW %s] mark_decision: shadow_id=%d not found",
                self._strategy_id, shadow_id,
            )
            return False

        if rec.status not in (STATUS_PENDING, status):
            logger.warning(
                "[SHADOW %s] mark_decision: shadow_id=%d already terminal "
                "(was %s, now %s) — overwriting",
                self._strategy_id, shadow_id, rec.status, status,
            )

        rec.status = status
        rec.block_gate = gate
        rec.block_reason = reason
        rec.block_metadata_json = (
            json.dumps(metadata, default=str) if metadata else ""
        )
        rec.last_updated = time.time()
        now = time.time()
        if force_flush or (now - self._last_flush) >= self._FLUSH_THROTTLE_SEC:
            self._flush_atomic()
        return True

    # ── Public API: mark executed ──────────────────────────────────

    def mark_executed(
        self,
        shadow_id: int,
        lane: str,
        ref: dict,
    ) -> bool:
        """Attach a real-execution reference to a captured signal.

        ref shape per lane (matches the design-review mapping):
          paper:   {"system": "<strategy_id>", "journal_idx": <int>}
          ctrader: {"position_id": <int>, "volume": <float>}
          mt5:     {"ticket": <int>}

        Returns True if updated; False if shadow_id unknown.
        """
        if lane not in VALID_LANES:
            raise ValueError(f"lane must be in {VALID_LANES}, got {lane!r}")
        if not isinstance(ref, dict):
            raise ValueError(f"ref must be dict, got {type(ref).__name__}")

        rec = self._find(shadow_id)
        if rec is None:
            logger.warning(
                "[SHADOW %s] mark_executed: shadow_id=%d not found",
                self._strategy_id, shadow_id,
            )
            return False

        rec.status = STATUS_EXECUTED
        rec.exec_lane = lane
        rec.exec_ref_json = json.dumps(ref, default=str)
        rec.last_updated = time.time()
        self._flush_atomic()
        return True

    # ── Public API: simulator-side updates (Phase C will call) ────

    def write_simulation(
        self,
        shadow_id: int,
        sim_exit_time: float,
        sim_exit_price: float,
        sim_exit_reason: str,
        sim_pnl_pips: float,
        sim_mae_pips: float,
        sim_mfe_pips: float,
        sim_duration_minutes: float,
        sim_pessimism_applied: str,
        features: dict | None = None,
        sim_failure_reason: str = "",     # Phase D.2: persist failure reason for FAILED outcomes
        force_flush: bool = False,
    ) -> bool:
        """Patch in the simulator's outcome on a captured signal.

        Called by ShadowSimWorker on its 5-min cycle. Throttled to
        flush at most every _SIM_UPDATE_FLUSH_THROTTLE_SEC because a
        single sim cycle can update many records at once; force_flush=
        True at the end of the worker's batch makes sure the last
        batch reaches disk.

        sim_failure_reason: Phase D.2 addition. When sim_exit_reason
        == "FAILED", this carries the reason (no_m1_data, etc.) so
        Edge Miner can categorize failures. Empty string for non-FAILED
        outcomes — sparse-omitted at default.
        """
        rec = self._find(shadow_id)
        if rec is None:
            logger.warning(
                "[SHADOW %s] write_simulation: shadow_id=%d not found",
                self._strategy_id, shadow_id,
            )
            return False

        rec.sim_completed = True
        rec.sim_exit_time = float(sim_exit_time)
        rec.sim_exit_price = float(sim_exit_price)
        rec.sim_exit_reason = sim_exit_reason
        rec.sim_pnl_pips = float(sim_pnl_pips)
        rec.sim_mae_pips = float(sim_mae_pips)
        rec.sim_mfe_pips = float(sim_mfe_pips)
        rec.sim_duration_minutes = float(sim_duration_minutes)
        rec.sim_pessimism_applied = sim_pessimism_applied
        rec.sim_completed_at = time.time()
        rec.sim_failure_reason = sim_failure_reason
        if features is not None:
            rec.features_json = json.dumps(features, default=str)
        rec.last_updated = time.time()

        now = time.time()
        if force_flush or (now - self._sim_update_last_flush) >= self._SIM_UPDATE_FLUSH_THROTTLE_SEC:
            self._flush_atomic()
            self._sim_update_last_flush = now
        return True

    # ── Phase D worker helpers ───────────────────────────────────────

    def mark_permanent_failed(
        self,
        shadow_id: int,
        failure_reason: str,
        force_flush: bool = False,
    ) -> bool:
        """Mark a record as permanent-FAILED without simulation data.

        Used by ShadowSimWorker's two paths:
          1. Fast-path for records that simulate() will reject (strength-
             rejects with no input_snapshot, malformed records). Saves
             the cost of invoking simulate() at all.
          2. Transient-retry-cap escalation: when transient_retry_count
             >= config.transient_retry_max, the worker gives up and
             stamps the record with a transient_giveup_after_N failure
             reason.

        Sets sim_completed=True so pending_simulation() no longer
        returns the record. Matches write_simulation's throttle behavior.
        """
        rec = self._find(shadow_id)
        if rec is None:
            logger.warning(
                "[SHADOW %s] mark_permanent_failed: shadow_id=%d not found",
                self._strategy_id, shadow_id,
            )
            return False

        rec.sim_completed = True
        rec.sim_completed_at = time.time()
        rec.sim_exit_reason = "FAILED"
        rec.sim_failure_reason = failure_reason
        rec.last_updated = time.time()

        now = time.time()
        if force_flush or (now - self._sim_update_last_flush) >= self._SIM_UPDATE_FLUSH_THROTTLE_SEC:
            self._flush_atomic()
            self._sim_update_last_flush = now
        return True

    def bump_transient_retry(self, shadow_id: int) -> int:
        """Increment transient_retry_count on a record. Returns the new count.

        Used by ShadowSimWorker when simulate() returns a transient
        FAILED outcome (no_m1_data, data_too_recent, empty_m1). The
        record stays at sim_completed=False (gets retried by next
        cycle's pending_simulation query) until either:
          * Retry succeeds and write_simulation is called (sets sim_completed=True), or
          * transient_retry_count >= config.transient_retry_max, at which point
            the worker calls mark_permanent_failed.

        Returns -1 if shadow_id not found.
        """
        rec = self._find(shadow_id)
        if rec is None:
            logger.warning(
                "[SHADOW %s] bump_transient_retry: shadow_id=%d not found",
                self._strategy_id, shadow_id,
            )
            return -1
        rec.transient_retry_count += 1
        rec.last_updated = time.time()

        now = time.time()
        if (now - self._sim_update_last_flush) >= self._SIM_UPDATE_FLUSH_THROTTLE_SEC:
            self._flush_atomic()
            self._sim_update_last_flush = now
        return rec.transient_retry_count

    def force_flush(self) -> None:
        """Public flush — used at app shutdown and at end of sim batch."""
        self._flush_atomic()
        self._sim_update_last_flush = time.time()

    # ── Read API (for ShadowSimWorker + LiveCandleDialog stats) ────

    def pending_simulation(self) -> list[ShadowSignalRecord]:
        """Records that need the simulator: terminal status, no sim yet."""
        return [
            r for r in self._journal
            if r.status in (STATUS_EXECUTED, STATUS_BLOCKED)
            and not r.sim_completed
        ]

    def pending_calibration(self) -> list[ShadowSignalRecord]:
        """Records awaiting calibration write: EXECUTED + sim done + cal not yet done.

        Phase D.3 introduced `calibration_completed` as a state distinct
        from `sim_completed` because EXECUTED records need both: the
        simulator runs first (write_simulation → sim_completed=True),
        then the worker waits for the linked real trade to close and
        writes the calibration delta (calibration_completed=True).

        Worker uses this query each cycle to drain the calibration
        backlog. Records may sit in this queue for arbitrary time
        (until the linked real trade closes — minutes to hours). On
        worker restart, the queue rebuilds correctly from the journal
        because the flag is persisted.
        """
        return [
            r for r in self._journal
            if r.status == STATUS_EXECUTED
            and r.sim_completed
            and not r.calibration_completed
            # Only successful sims need calibration — FAILED sims have
            # no comparable pnl_pips for the delta. Skip those.
            and r.sim_exit_reason in ("TP", "SL", "TIMEOUT")
        ]

    def mark_calibration_completed(
        self, shadow_id: int, force_flush: bool = False,
    ) -> bool:
        """Set calibration_completed=True on a record after write_calibration succeeds.

        Used by ShadowSimWorker.D.3 after the simulator's write_calibration
        appends a delta to ShadowCalibrationLog. Throttled flush matching
        write_simulation/mark_permanent_failed.
        """
        rec = self._find(shadow_id)
        if rec is None:
            logger.warning(
                "[SHADOW %s] mark_calibration_completed: shadow_id=%d not found",
                self._strategy_id, shadow_id,
            )
            return False
        rec.calibration_completed = True
        rec.last_updated = time.time()

        now = time.time()
        if force_flush or (now - self._sim_update_last_flush) >= self._SIM_UPDATE_FLUSH_THROTTLE_SEC:
            self._flush_atomic()
            self._sim_update_last_flush = now
        return True

    def all_records(self) -> list[ShadowSignalRecord]:
        """Defensive copy of every record (for stats panels, queries)."""
        return list(self._journal)

    def count_today(self, status: str | None = None) -> int:
        """Count records captured today (JST), optionally filtered by status."""
        from datetime import datetime, timezone, timedelta
        jst = timezone(timedelta(hours=9))
        today_ord = datetime.now(jst).toordinal()
        n = 0
        for r in self._journal:
            r_ord = datetime.fromtimestamp(r.signal_time, tz=jst).toordinal()
            if r_ord != today_ord:
                continue
            if status is None or r.status == status:
                n += 1
        return n

    def orphan_count(self) -> int:
        """Records still in PENDING after a restart — indicates a crash
        between log_signal and mark_decision/mark_executed. Not fatal,
        but flagged for the startup self-test."""
        return sum(1 for r in self._journal if r.status == STATUS_PENDING)

    # ── Internal ───────────────────────────────────────────────────

    def _find(self, shadow_id: int) -> ShadowSignalRecord | None:
        # Linear search is fine — journals stay small (~hundreds of
        # records per system per day; pruning is a future concern).
        for r in self._journal:
            if r.shadow_id == shadow_id:
                return r
        return None


# ─────────────────────────────────────────────────────────────────────
# ShadowCalibrationLog (Addition 1)
# ─────────────────────────────────────────────────────────────────────

class ShadowCalibrationLog:
    """Append-only log of (sim outcome - real outcome) for EXECUTED parity sims.

    Path: data/shadow_calibration.json (single file, all systems pooled —
    pessimism calibration is a system-agnostic property of the simulator).

    Format: list[ShadowCalibrationRecord] in JSON. Append-on-write,
    flush immediately. Sized to remain small (~one record per executed
    trade across all systems, e.g., a few thousand entries before
    pruning becomes worth thinking about).

    Phase C ShadowSimulator constructs ShadowCalibrationRecord and
    appends here whenever it finishes a sim on an EXECUTED record AND
    the linked real-trade record is closed (so real_pnl_pips is known).
    """

    def __init__(self, calibration_path: Path) -> None:
        self._path = Path(calibration_path)
        self._records: list[ShadowCalibrationRecord] = []
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.error(
                "[SHADOW-CAL] Failed to load %s: %s — starting empty",
                self._path, exc,
            )
            return
        if not isinstance(data, list):
            logger.error(
                "[SHADOW-CAL] %s is not a list — starting empty", self._path,
            )
            return
        for d in data:
            r = ShadowCalibrationRecord()
            for k, v in d.items():
                if hasattr(r, k):
                    setattr(r, k, v)
            self._records.append(r)
        logger.info(
            "[SHADOW-CAL] Loaded %d calibration records from %s",
            len(self._records), self._path,
        )

    def append(self, rec: ShadowCalibrationRecord) -> None:
        if not rec.written_at:
            rec.written_at = time.time()
        rec.delta_pips = rec.real_pnl_pips - rec.sim_pnl_pips
        self._records.append(rec)
        self._flush_atomic()

    def _flush_atomic(self) -> None:
        # Calibration log is small (one record per executed trade across all
        # systems, ~thousands of records over many months). Indent=2 here is
        # fine — readability over the marginal disk-space saving.
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix(self._path.suffix + ".tmp")
            payload = [asdict(r) for r in self._records]
            tmp.write_text(
                json.dumps(payload, indent=2, default=str),
                encoding="utf-8",
            )
            import os
            os.replace(tmp, self._path)
        except Exception as exc:
            logger.error("[SHADOW-CAL] Flush failed: %s", exc)

    def all_records(self) -> list[ShadowCalibrationRecord]:
        return list(self._records)

    def summary(self) -> dict[str, Any]:
        """Mean / median / std of delta_pips across all calibration
        records. Used by self-test and the LiveCandleDialog stats panel
        to surface "is the simulator well-calibrated?" at a glance.

        Interpretation:
            mean delta ≈ 0    → simulator pessimism is well-calibrated
            mean delta > 0    → simulator is too pessimistic (real beats sim)
            mean delta < 0    → simulator is too optimistic (sim beats real)
        Sample size matters: <50 records is noise.
        """
        if not self._records:
            return {"n": 0, "mean": 0.0, "median": 0.0, "std": 0.0}
        deltas = [r.delta_pips for r in self._records]
        n = len(deltas)
        mean = sum(deltas) / n
        sorted_d = sorted(deltas)
        median = (
            sorted_d[n // 2] if n % 2 == 1
            else (sorted_d[n // 2 - 1] + sorted_d[n // 2]) / 2
        )
        var = sum((d - mean) ** 2 for d in deltas) / n
        std = var ** 0.5
        return {"n": n, "mean": mean, "median": median, "std": std}
