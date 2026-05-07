"""Tier 1 Script 4: shadow schema health verification.

Defensive check that journal records are well-formed. Catches schema
drift, data corruption, sparse-field anomalies, and statistical
impossibilities. Read-only, never mutates the journal.

Usage:
    python -m takumi_trader.analytics.verify_schema_health \
        --journal data/shadow_trades_Sv2.json \
        --calibration data/shadow_calibration_Sv2.json \
        --output data/analytics/schema_health_2026-05-12.txt
"""
from __future__ import annotations

import argparse
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from takumi_trader.analytics.shadow_loader import (  # noqa: E402
    KNOWN_GATES, load_calibration_log, load_shadow_journal,
)


_VALID_STATUSES = frozenset({"PENDING", "BLOCKED", "EXECUTED", "FAILED"})
_VALID_DIRECTIONS = frozenset({"BUY", "SELL"})


class HealthReport:
    def __init__(self):
        self.critical: list[str] = []
        self.warning: list[str] = []
        self.info: list[str] = []
        self.passes: list[str] = []
        self.sections: list[list[str]] = []

    def crit(self, msg: str) -> None:
        self.critical.append(msg)

    def warn(self, msg: str) -> None:
        self.warning.append(msg)

    def passing(self, msg: str) -> None:
        self.passes.append(msg)


def _is_finite(x) -> bool:
    if not isinstance(x, (int, float)):
        return False
    return math.isfinite(float(x))


def _check_journal_structure(records, hr: HealthReport) -> list[str]:
    """Section 1: identity fields + valid statuses."""
    n = len(records)
    n_zero_id = sum(1 for r in records if r.shadow_id == 0)
    n_zero_time = sum(1 for r in records if r.signal_time == 0.0)
    bad_status = [r.status for r in records if r.status not in _VALID_STATUSES]
    bad_dir = [r.direction for r in records if r.direction not in _VALID_DIRECTIONS]

    # Duplicate shadow_id
    id_counter = Counter(r.shadow_id for r in records)
    dups = {sid: c for sid, c in id_counter.items() if c > 1 and sid != 0}

    lines = ["──── 1. JOURNAL RECORD STRUCTURE ────"]
    if n_zero_id == 0 and not dups:
        hr.passing("All records have non-zero unique shadow_id")
        lines.append("  ✓ All records have non-zero, unique shadow_id")
    else:
        if n_zero_id:
            hr.crit(f"{n_zero_id} records with shadow_id=0")
            lines.append(f"  ⚠ {n_zero_id} records with shadow_id=0 — invalid")
        if dups:
            hr.crit(f"{len(dups)} duplicate shadow_id values")
            lines.append(f"  ⚠ {len(dups)} duplicate shadow_id values")

    if n_zero_time == 0:
        hr.passing("All records have valid signal_time")
        lines.append("  ✓ All records have valid signal_time")
    else:
        hr.crit(f"{n_zero_time} records with signal_time=0")
        lines.append(f"  ⚠ {n_zero_time} records with signal_time=0")

    if not bad_status:
        hr.passing(f"All status values in {sorted(_VALID_STATUSES)}")
        lines.append(f"  ✓ All status values in {sorted(_VALID_STATUSES)}")
    else:
        unique_bad = set(bad_status)
        hr.crit(f"Unknown status values: {unique_bad}")
        lines.append(f"  ⚠ Unknown status values found: {sorted(unique_bad)}")

    if not bad_dir:
        hr.passing("All direction values in {BUY, SELL}")
        lines.append("  ✓ All direction values in {BUY, SELL}")
    else:
        unique_bad = set(bad_dir)
        hr.warn(f"Unknown direction values: {unique_bad}")
        lines.append(f"  ⚠ Unknown direction values: {sorted(unique_bad)}")

    # Pending count (should be 0 in steady state)
    n_pending = sum(1 for r in records if r.status == "PENDING")
    if n_pending == 0:
        lines.append("  ✓ Zero PENDING records (steady state)")
    else:
        hr.warn(f"{n_pending} PENDING records (expected 0 in steady state)")
        lines.append(
            f"  ⚠ {n_pending} PENDING records (expected 0 in steady state)"
        )
    return lines


def _check_numeric_sanity(records, hr: HealthReport) -> list[str]:
    """Section 2: NaN, Inf, sign violations, range checks."""
    nan_count = 0
    inf_count = 0
    bad_mae = 0
    bad_mfe = 0
    bad_dur = 0
    extreme_pnl = 0
    zero_dur = 0

    for r in records:
        for field in ("sim_pnl_pips", "sim_mae_pips", "sim_mfe_pips",
                      "sim_duration_minutes", "proposed_entry",
                      "proposed_sl_price", "proposed_tp_price"):
            v = getattr(r, field)
            if isinstance(v, float):
                if math.isnan(v):
                    nan_count += 1
                elif math.isinf(v):
                    inf_count += 1
        # MAE / MFE should be >= 0 by construction
        if r.sim_completed and r.sim_mae_pips < 0:
            bad_mae += 1
        if r.sim_completed and r.sim_mfe_pips < 0:
            bad_mfe += 1
        if r.sim_completed and r.sim_duration_minutes < 0:
            bad_dur += 1
        if abs(float(r.sim_pnl_pips)) > 200:
            extreme_pnl += 1
        if (r.sim_completed and r.sim_duration_minutes == 0
                and r.sim_exit_reason in ("TP", "SL", "TIMEOUT")):
            zero_dur += 1

    lines = ["", "──── 2. NUMERIC FIELD SANITY ────"]
    if nan_count == 0:
        lines.append("  ✓ No NaN values in numeric fields")
    else:
        hr.crit(f"{nan_count} NaN values"); lines.append(f"  ⚠ {nan_count} NaN values")
    if inf_count == 0:
        lines.append("  ✓ No Infinity values")
    else:
        hr.crit(f"{inf_count} Inf values"); lines.append(f"  ⚠ {inf_count} Inf values")
    lines.append(
        f"  {'✓' if bad_mae == 0 else '⚠'} MAE >= 0 (violations: {bad_mae})"
    )
    if bad_mae:
        hr.warn(f"{bad_mae} negative MAE values")
    lines.append(
        f"  {'✓' if bad_mfe == 0 else '⚠'} MFE >= 0 (violations: {bad_mfe})"
    )
    if bad_mfe:
        hr.warn(f"{bad_mfe} negative MFE values")
    lines.append(
        f"  {'✓' if bad_dur == 0 else '⚠'} duration >= 0 (violations: {bad_dur})"
    )
    if bad_dur:
        hr.warn(f"{bad_dur} negative durations")
    if extreme_pnl == 0:
        lines.append("  ✓ All sim_pnl within ±200p")
    else:
        hr.warn(f"{extreme_pnl} records with |sim_pnl| > 200p")
        lines.append(
            f"  ⚠ {extreme_pnl} records with |sim_pnl| > 200p (large but plausible)"
        )
    if zero_dur > 0:
        hr.warn(f"{zero_dur} sim_completed records with duration=0")
        lines.append(
            f"  ⚠ {zero_dur} sim_completed (TP/SL/TIMEOUT) records with duration=0"
        )
    return lines


def _check_sparse_distribution(records, hr: HealthReport) -> list[str]:
    """Section 3: sparse field population sanity. Verifies the
    sparse-serialization design is working correctly."""
    n = len(records)
    fields_to_check = [
        ("transient_retry_count", lambda r: r.transient_retry_count != 0),
        ("sim_failure_reason", lambda r: bool(r.sim_failure_reason)),
        ("calibration_completed", lambda r: bool(r.calibration_completed)),
        ("exec_ref_json", lambda r: bool(r.exec_ref_json)),
        ("input_snapshot_json", lambda r: bool(r.input_snapshot_json)),
        ("features_json", lambda r: bool(r.features_json)),
    ]
    lines = [
        "",
        "──── 3. SPARSE FIELD DISTRIBUTION ────",
        "  Field                          Populated   Default   Notes",
        "  " + "─" * 60,
    ]
    notes_map = {
        "transient_retry_count": "(only retry-bumped)",
        "sim_failure_reason": "(only failed sims)",
        "calibration_completed": "(only calibrated)",
        "exec_ref_json": "(only EXECUTED)",
        "input_snapshot_json": "(only strength-passes)",
        "features_json": "(only EXECUTED w/ features)",
    }
    for fname, check in fields_to_check:
        pop = sum(1 for r in records if check(r))
        defaulted = n - pop
        note = notes_map.get(fname, "")
        lines.append(
            f"  {fname:<28s}  {pop:>9,}   {defaulted:>7,}   {note}"
        )
    return lines


def _check_gates(records, hr: HealthReport) -> list[str]:
    """Section 4: gate values are recognized + no schema drift."""
    seen = {r.block_gate for r in records if r.block_gate}
    drift = seen - KNOWN_GATES
    lines = [
        "",
        "──── 4. GATE VALUE INTEGRITY ────",
        f"  Recognized gates ({len(KNOWN_GATES)}): {sorted(KNOWN_GATES)}",
    ]
    if not drift:
        hr.passing("No unrecognized gate values")
        lines.append("  ✓ All block_gate values are recognized (no schema drift)")
    else:
        hr.crit(f"Unrecognized gate values: {sorted(drift)}")
        lines.append(
            f"  ⚠ Unknown gate values (schema drift?): {sorted(drift)}"
        )
    return lines


def _check_calibration(cals, journal_records, hr: HealthReport) -> list[str]:
    """Section 5: calibration log integrity + cross-file consistency."""
    lines = ["", "──── 5. CALIBRATION LOG INTEGRITY ────"]
    if not cals:
        lines.append("  (no calibration entries yet)")
        return lines

    bad_finite = 0
    bad_delta = 0
    bad_id = 0
    cal_ids = set()
    for c in cals:
        if not _is_finite(c.real_pnl_pips) or not _is_finite(c.sim_pnl_pips):
            bad_finite += 1
        # delta_pips should equal real_pnl_pips - sim_pnl_pips
        expected_delta = float(c.real_pnl_pips) - float(c.sim_pnl_pips)
        if abs(float(c.delta_pips) - expected_delta) > 0.01:
            bad_delta += 1
        if c.shadow_id == 0:
            bad_id += 1
        cal_ids.add(c.shadow_id)
    n_cal = len(cals)
    n_dups = n_cal - len(cal_ids)

    if bad_finite == 0:
        lines.append("  ✓ All real_pnl/sim_pnl values are finite")
    else:
        hr.crit(f"{bad_finite} non-finite PnL values")
        lines.append(f"  ⚠ {bad_finite} non-finite PnL values")
    if bad_delta == 0:
        lines.append("  ✓ All delta_pips = real - sim (verified)")
    else:
        hr.crit(f"{bad_delta} delta_pips fields don't match real - sim")
        lines.append(f"  ⚠ {bad_delta} delta_pips inconsistent with real - sim")
    if bad_id == 0 and n_dups == 0:
        lines.append("  ✓ All calibration shadow_ids unique and non-zero")
    else:
        hr.warn(f"{n_dups} duplicate cal shadow_ids; {bad_id} zero ids")

    # Cross-file: every cal shadow_id must exist in journal
    journal_ids = {r.shadow_id for r in journal_records if r.shadow_id != 0}
    missing = cal_ids - journal_ids
    lines.append("")
    lines.append("──── 6. CROSS-FILE CONSISTENCY ────")
    lines.append(f"  Calibrations reference: {len(cal_ids)} unique shadow_ids")
    if not missing:
        lines.append(f"    Of those, in journal:   {len(cal_ids)} / {len(cal_ids)} ✓")
    else:
        hr.crit(f"{len(missing)} cal entries reference missing journal records")
        lines.append(f"    Missing in journal: {len(missing)} ⚠ ({sorted(missing)[:5]}...)")
    # Linked records should have status=EXECUTED + sim_completed
    by_id = {r.shadow_id: r for r in journal_records if r.shadow_id in cal_ids}
    not_exec = sum(1 for r in by_id.values() if r.status != "EXECUTED")
    not_sim = sum(1 for r in by_id.values() if not r.sim_completed)
    lines.append(
        f"  Linked records with status=EXECUTED: {len(by_id) - not_exec} / {len(by_id)} "
        f"{'✓' if not_exec == 0 else '⚠'}"
    )
    if not_exec:
        hr.warn(f"{not_exec} cal-linked records not EXECUTED")
    lines.append(
        f"  Linked records with sim_completed=True: {len(by_id) - not_sim} / {len(by_id)} "
        f"{'✓' if not_sim == 0 else '⚠'}"
    )
    if not_sim:
        hr.warn(f"{not_sim} cal-linked records not sim_completed")
    return lines


def _section_summary(hr: HealthReport) -> list[str]:
    n_crit = len(hr.critical)
    n_warn = len(hr.warning)
    overall = (
        "GOOD (no critical issues)" if n_crit == 0
        else "ISSUES — investigate critical findings"
    )
    lines = [
        "",
        "──── 7. ANOMALY SUMMARY ────",
        f"  CRITICAL: {n_crit}",
        f"  WARNING:  {n_warn}",
        f"  PASSED:   {len(hr.passes)}",
        f"  Overall:  {overall}",
    ]
    if hr.critical:
        lines.append("")
        lines.append("  Critical findings:")
        for msg in hr.critical:
            lines.append(f"    - {msg}")
    if hr.warning:
        lines.append("")
        lines.append("  Warnings:")
        for msg in hr.warning:
            lines.append(f"    - {msg}")
    return lines


def _section_actions(hr: HealthReport) -> list[str]:
    if not hr.critical and not hr.warning:
        return [
            "",
            "──── 8. RECOMMENDED ACTIONS ────",
            "  None — schema health is clean.",
        ]
    actions = []
    for msg in hr.warning + hr.critical:
        if "PENDING" in msg:
            actions.append(
                "Investigate orphan PENDING records (mark_alert_mgr_orphans path)"
            )
        elif "duration=0" in msg:
            actions.append(
                "Inspect zero-duration sims — signal_time == exit_time accidentally?"
            )
        elif "non-finite" in msg or "NaN" in msg or "Inf" in msg:
            actions.append(
                "Investigate numeric corruption — pessimism stack edge cases"
            )
        elif "delta_pips inconsistent" in msg:
            actions.append(
                "Check ShadowSimulator.write_calibration delta computation"
            )
        elif "schema drift" in msg or "Unknown" in msg:
            actions.append(
                "Audit gate constants vs shadow_logger.py — possible new gate added"
            )
    actions = list(dict.fromkeys(actions))  # dedupe preserving order
    lines = [
        "",
        "──── 8. RECOMMENDED ACTIONS ────",
    ]
    if not actions:
        lines.append("  (warnings present but no specific action recipe matched)")
    for i, action in enumerate(actions, 1):
        lines.append(f"  {i}. {action}")
    return lines


def build_report(records, cals, journal_name: str, cal_name: str) -> str:
    bar = "=" * 67
    hr = HealthReport()
    header = [
        bar,
        "  SHADOW SCHEMA HEALTH REPORT",
        f"  Journal:     {journal_name} ({len(records):,} records)",
        f"  Calibration: {cal_name} ({len(cals)} records)",
        f"  Verified:    {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        bar,
    ]
    sections = [
        *_check_journal_structure(records, hr),
        *_check_numeric_sanity(records, hr),
        *_check_sparse_distribution(records, hr),
        *_check_gates(records, hr),
        *_check_calibration(cals, records, hr),
        *_section_summary(hr),
        *_section_actions(hr),
        bar,
    ]
    return "\n".join(header + sections)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--journal", default="data/shadow_trades_Sv2.json")
    p.add_argument("--calibration", default="data/shadow_calibration_Sv2.json")
    p.add_argument("--output", default=None)
    args = p.parse_args(argv)

    journal_path = Path(args.journal)
    cal_path = Path(args.calibration)
    records = load_shadow_journal(journal_path)
    cals = load_calibration_log(cal_path)
    report = build_report(records, cals, journal_path.name, cal_path.name)

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(report + "\n", encoding="utf-8")
        print(f"Report written: {out}")
    else:
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
        print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
