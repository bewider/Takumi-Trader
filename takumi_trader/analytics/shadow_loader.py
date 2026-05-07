"""Shared shadow journal loading + filtering utilities for Tier 1 analytics.

Built 2026-05-07 per the Tier 1 spec. Read-only — never modifies journals,
never touches production code, never connects to MT5.

Schema notes (verified against actual production data 2026-05-07):
* Shadow record status field is `status` (not `outcome_status` as the
  spec drafted). Values: "PENDING" | "BLOCKED" | "EXECUTED" | "FAILED".
* Calibration record uses `pessimism_applied` (not `pessimism_config`
  as the spec drafted).
* Strength-rejects are lightweight (~656B) — many sim_* / proposed_*
  fields are sparse-omitted. Defensive accessors handle missing fields
  via typed defaults so callers don't need to check before access.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# Typed defaults — used by ShadowRecord/CalibrationRecord __getattr__
# so callers can access any documented field without KeyError or None
# guards. Sparse fields default to typed zero values matching the
# dataclass design in shadow_logger.py.
# ─────────────────────────────────────────────────────────────────────

_SHADOW_DEFAULTS: dict[str, Any] = {
    # Identity
    "shadow_id": 0,
    "strategy_id": "",
    "signal_time": 0.0,
    "signal_time_str": "",
    "captured_at": 0.0,
    "last_updated": 0.0,
    "pair": "",
    "direction": "",
    # Decision/outcome — default "PENDING" matches ShadowSignalRecord
    # dataclass default; sparse serialization omits the field when its
    # value equals the default, so missing field == status=="PENDING".
    "status": "PENDING",                # PENDING | BLOCKED | EXECUTED | FAILED
    "block_gate": "",
    "block_reason": "",
    "block_metadata_json": "",
    "exec_lane": "",
    "exec_ref_json": "",
    # Strength-pass payload (sparse — only on records that passed strength)
    "proposed_entry": 0.0,
    "proposed_sl_price": 0.0,
    "proposed_tp_price": 0.0,
    "proposed_sl_pips": 0.0,
    "proposed_tp_pips": 0.0,
    "input_snapshot_json": "",
    "features_json": "",
    # Simulation outcome (set by ShadowSimWorker after sim completes)
    "sim_completed": False,
    "sim_completed_at": 0.0,
    "sim_exit_time": 0.0,
    "sim_exit_price": 0.0,
    "sim_exit_reason": "",
    "sim_failure_reason": "",
    "sim_pnl_pips": 0.0,
    "sim_mae_pips": 0.0,
    "sim_mfe_pips": 0.0,
    "sim_duration_minutes": 0,
    "sim_pessimism_applied": "",
    # Retry/calibration state
    "transient_retry_count": 0,
    "calibration_completed": False,
}

_CALIBRATION_DEFAULTS: dict[str, Any] = {
    "shadow_id": 0,
    "strategy_id": "",
    "pair": "",
    "direction": "",
    "signal_time": 0.0,
    "real_pnl_pips": 0.0,
    "sim_pnl_pips": 0.0,
    "delta_pips": 0.0,
    "real_exit_reason": "",
    "sim_exit_reason": "",
    "real_duration_minutes": 0.0,
    "sim_duration_minutes": 0.0,
    "pessimism_applied": "",
    "written_at": 0.0,
}


# ─────────────────────────────────────────────────────────────────────
# Record wrappers
# ─────────────────────────────────────────────────────────────────────

@dataclass
class ShadowRecord:
    """Lightweight wrapper around a shadow journal record dict.

    Defensive __getattr__ → record.get(key, typed_default). Callers can
    treat any documented field as always-present without KeyError or
    None checks. Use `.raw` to access the underlying dict directly when
    you need to check whether a field was actually populated vs defaulted.
    """
    raw: dict

    def __getattr__(self, name: str):
        if name == "raw":
            raise AttributeError(name)
        if name in _SHADOW_DEFAULTS:
            return self.raw.get(name, _SHADOW_DEFAULTS[name])
        # Unknown attribute — raise so typos surface
        raise AttributeError(
            f"ShadowRecord has no field {name!r} (known: "
            f"{sorted(_SHADOW_DEFAULTS)[:5]}... + {len(_SHADOW_DEFAULTS) - 5} more)"
        )

    @property
    def signal_dt(self) -> datetime:
        return datetime.fromtimestamp(self.signal_time, tz=timezone.utc)

    @property
    def block_metadata(self) -> dict:
        if not self.block_metadata_json:
            return {}
        try:
            return json.loads(self.block_metadata_json)
        except json.JSONDecodeError:
            return {}

    @property
    def exec_ref(self) -> dict:
        if not self.exec_ref_json:
            return {}
        try:
            return json.loads(self.exec_ref_json)
        except json.JSONDecodeError:
            return {}

    @property
    def is_strength_reject(self) -> bool:
        """True if this is a lightweight strength-reject (no input_snapshot)."""
        return self.block_gate == "strength_engine" and not self.input_snapshot_json

    @property
    def is_strength_pass(self) -> bool:
        """True if this is a full strength-pass record."""
        return bool(self.input_snapshot_json)


@dataclass
class CalibrationRecord:
    """Lightweight wrapper around a calibration log record dict."""
    raw: dict

    def __getattr__(self, name: str):
        if name == "raw":
            raise AttributeError(name)
        if name in _CALIBRATION_DEFAULTS:
            return self.raw.get(name, _CALIBRATION_DEFAULTS[name])
        raise AttributeError(f"CalibrationRecord has no field {name!r}")

    @property
    def signal_dt(self) -> datetime:
        return datetime.fromtimestamp(self.signal_time, tz=timezone.utc)


# ─────────────────────────────────────────────────────────────────────
# Loaders
# ─────────────────────────────────────────────────────────────────────

def load_shadow_journal(
    path: Path,
    since: datetime | None = None,
    until: datetime | None = None,
) -> list[ShadowRecord]:
    """Load shadow journal with optional UTC date-range filter.

    Returns list of ShadowRecord wrappers, sorted by signal_time
    ascending. Defensive against malformed records — logs warning and
    skips, never crashes. Returns empty list if path doesn't exist.
    """
    path = Path(path)
    if not path.exists():
        logger.warning("[ANALYTICS] journal not found: %s", path)
        return []

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logger.warning("[ANALYTICS] failed to parse journal %s: %s", path, exc)
        return []
    if not isinstance(raw, list):
        logger.warning("[ANALYTICS] journal root is not a list: %s", path)
        return []

    records = [ShadowRecord(r) for r in raw if isinstance(r, dict)]

    if since is not None:
        since_ts = since.timestamp()
        records = [r for r in records if r.signal_time >= since_ts]
    if until is not None:
        until_ts = until.timestamp()
        records = [r for r in records if r.signal_time < until_ts]

    records.sort(key=lambda r: r.signal_time)
    return records


def load_calibration_log(
    path: Path,
    since: datetime | None = None,
    until: datetime | None = None,
) -> list[CalibrationRecord]:
    """Load calibration log with optional UTC date-range filter."""
    path = Path(path)
    if not path.exists():
        logger.warning("[ANALYTICS] calibration log not found: %s", path)
        return []

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logger.warning("[ANALYTICS] failed to parse calibration log %s: %s", path, exc)
        return []
    if not isinstance(raw, list):
        return []

    records = [CalibrationRecord(r) for r in raw if isinstance(r, dict)]

    if since is not None:
        since_ts = since.timestamp()
        records = [r for r in records if r.signal_time >= since_ts]
    if until is not None:
        until_ts = until.timestamp()
        records = [r for r in records if r.signal_time < until_ts]

    records.sort(key=lambda r: r.signal_time)
    return records


# ─────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────

def parse_date_arg(s: str) -> datetime:
    """Parse YYYY-MM-DD or YYYY-MM-DDTHH:MM[:SS] into UTC datetime."""
    fmts = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(
        f"Cannot parse date {s!r} (try YYYY-MM-DD or YYYY-MM-DDTHH:MM)"
    )


def classify_session(signal_time_utc: float) -> str:
    """Three-bucket session classifier matching BrokerSpreadModel.

    00:00-07:00 UTC -> tokyo
    07:00-12:00 UTC -> normal (London-only)
    12:00-16:00 UTC -> overlap (London + NY)
    16:00-21:00 UTC -> normal (NY-only)
    21:00-24:00 UTC -> tokyo
    """
    hour = datetime.fromtimestamp(signal_time_utc, tz=timezone.utc).hour
    if 12 <= hour < 16:
        return "overlap"
    if 7 <= hour < 12 or 16 <= hour < 21:
        return "normal"
    return "tokyo"


# Pair categorisation for calibration decomposition + filter analysis.
# Mapping reflects how the simulator's pessimism stack interacts with
# typical broker spread + tick patterns per pair group.

_USD_MAJORS = frozenset({
    "EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
})
_JPY_CROSSES = frozenset({
    "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY",
})
_WIDE_CROSSES = frozenset({
    "GBPAUD", "GBPNZD", "GBPCAD", "GBPCHF",
    "EURAUD", "EURNZD", "EURCAD", "EURCHF", "EURGBP",
    "AUDCAD", "AUDCHF", "AUDNZD",
    "NZDCAD", "NZDCHF",
    "CADCHF",
})
_GOLD = frozenset({"XAUUSD"})


def categorize_pair(pair: str) -> str:
    """Return one of: 'USD majors', 'JPY crosses', 'Wide crosses',
    'Gold', 'Other'."""
    if pair in _USD_MAJORS:
        return "USD majors"
    if pair in _JPY_CROSSES:
        return "JPY crosses"
    if pair in _WIDE_CROSSES:
        return "Wide crosses"
    if pair in _GOLD:
        return "Gold"
    return "Other"


# Recognized gate values — kept in sync with shadow_logger.py constants.
# Used by verify_schema_health to detect schema drift.
KNOWN_GATES: frozenset[str] = frozenset({
    "strength_engine", "divergence_spread", "structural", "conviction",
    "no_trade_window", "news", "h1_sweep", "adr", "duplicate", "internal",
})


def format_pip(value: float | None, sign: bool = True) -> str:
    """Pretty-print a pip value with optional leading sign."""
    if value is None:
        return "n/a"
    s = "+" if value >= 0 and sign else ""
    return f"{s}{value:.2f}p"
