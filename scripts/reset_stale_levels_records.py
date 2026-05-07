"""One-shot reset script for the stale_proposed_levels bug cleanup.

Background: 2026-05-07. ShadowSimulator's pessimistic-entry mechanism
overshot proposed SL/TP levels on 210 records (0.35% of 60K sims),
producing structurally-impossible outcomes — "SL hit" with positive
PnL, "TP hit" with negative PnL. Fix A (commit 747f056) added a
defensive guard so future records hit by the same mechanism return
FAILED("stale_proposed_levels") instead of producing nonsense.

This script cleans up the historical contamination:
  - Resets sim state on the 210 affected journal records so the
    worker re-simulates them. Under Fix A's guard, they will become
    permanent-FAILED with reason "stale_proposed_levels" — correct
    classification rather than phantom-profit poison.
  - Removes the 1 affected calibration entry (EURCHF SELL,
    shadow_id=29686). When that paper trade re-closes (or if Sv2
    hits another EURCHF SELL), a new calibration entry generates
    cleanly under the fixed simulator.

Defensive design:
  1. Backup journal + calibration log to timestamped paths first
  2. Verify backups are loadable (round-trip-parse) before touching
     the originals
  3. Filter precisely: targets ONLY records matching the bug pattern
     (sim_exit_reason in {SL, TP} AND impossible-sign sim_pnl_pips)
  4. Atomic write back via tmp + os.replace
  5. Post-mutation count verification (expected counts must match)
  6. Dry-run mode by default — pass --execute to actually write

Run:
    # Dry-run (shows what would change, doesn't touch files)
    python scripts/reset_stale_levels_records.py

    # Execute (writes backups, then mutates)
    python scripts/reset_stale_levels_records.py --execute
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


# Fields cleared on the 210 reset records. These are the sim_*
# fields ShadowLogger.write_simulation populates plus the
# completion/calibration flags. mark_decision-set fields
# (status, block_gate, etc.) are PRESERVED.
_RESET_FIELDS_TO_DEFAULT: dict[str, object] = {
    "sim_completed": False,
    "sim_exit_time": 0.0,
    "sim_exit_price": 0.0,
    "sim_exit_reason": "",
    "sim_failure_reason": "",
    "sim_pnl_pips": 0.0,
    "sim_mae_pips": 0.0,
    "sim_mfe_pips": 0.0,
    "sim_duration_minutes": 0,
    "sim_pessimism_applied": "",
    "sim_completed_at": 0.0,
    "transient_retry_count": 0,
    "calibration_completed": False,
}


def is_bug_pattern(record: dict) -> bool:
    """Return True if record matches the stale_proposed_levels bug:
    sim_exit_reason='SL' with positive pnl, OR sim_exit_reason='TP'
    with negative pnl. These are structurally impossible outcomes."""
    if not record.get("sim_completed", False):
        return False
    reason = record.get("sim_exit_reason", "")
    try:
        pnl = float(record.get("sim_pnl_pips", 0.0))
    except (TypeError, ValueError):
        return False
    if reason == "SL" and pnl > 0:
        return True
    if reason == "TP" and pnl < 0:
        return True
    return False


def backup_file(path: Path, stamp: str) -> Path:
    """Copy file to a timestamped backup path. Returns backup path.
    Raises if source missing or copy fails."""
    if not path.exists():
        raise FileNotFoundError(f"{path} does not exist")
    backup = path.with_suffix(path.suffix + f".pre_reset_{stamp}.bak")
    shutil.copy2(path, backup)
    return backup


def verify_backup(backup: Path, original: Path) -> None:
    """Round-trip parse to verify backup is valid + same shape as original."""
    backup_data = json.loads(backup.read_text(encoding="utf-8"))
    original_data = json.loads(original.read_text(encoding="utf-8"))
    if not isinstance(backup_data, list) or not isinstance(original_data, list):
        raise ValueError("non-list root in backup or original")
    if len(backup_data) != len(original_data):
        raise ValueError(
            f"length mismatch: backup={len(backup_data)} vs original={len(original_data)}"
        )


def atomic_json_write(path: Path, data: list) -> None:
    """tmp + os.replace atomic write — same primitive as ShadowLogger uses."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def reset_journal(journal_path: Path, dry_run: bool) -> tuple[int, list[int]]:
    """Reset sim_* fields on records matching the bug pattern.

    Returns: (n_reset, sample_shadow_ids_first_5)"""
    data = json.loads(journal_path.read_text(encoding="utf-8"))
    n_reset = 0
    sample_ids: list[int] = []
    for r in data:
        if not isinstance(r, dict):
            continue
        if not is_bug_pattern(r):
            continue
        n_reset += 1
        if len(sample_ids) < 5:
            sample_ids.append(r.get("shadow_id", 0))
        if dry_run:
            continue
        for field, default in _RESET_FIELDS_TO_DEFAULT.items():
            r[field] = default
    if not dry_run:
        atomic_json_write(journal_path, data)
    return n_reset, sample_ids


def reset_calibration(cal_path: Path, dry_run: bool) -> tuple[int, list[int]]:
    """Remove calibration entries linked to affected shadow_ids."""
    if not cal_path.exists():
        return 0, []
    data = json.loads(cal_path.read_text(encoding="utf-8"))
    affected_ids: list[int] = []
    keep: list[dict] = []
    for entry in data:
        if not isinstance(entry, dict):
            keep.append(entry)
            continue
        # Re-derive bug pattern via the sim_pnl/sim_exit_reason fields
        # in the calibration entry itself.
        reason = entry.get("sim_exit_reason", "")
        try:
            pnl = float(entry.get("sim_pnl_pips", 0.0))
        except (TypeError, ValueError):
            keep.append(entry)
            continue
        if (reason == "SL" and pnl > 0) or (reason == "TP" and pnl < 0):
            affected_ids.append(entry.get("shadow_id", 0))
        else:
            keep.append(entry)
    if not dry_run:
        atomic_json_write(cal_path, keep)
    return len(affected_ids), affected_ids


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--journal", default="data/shadow_trades_Sv2.json")
    p.add_argument("--calibration", default="data/shadow_calibration_Sv2.json")
    p.add_argument("--execute", action="store_true",
                   help="Actually mutate files. Without this, dry-run only.")
    args = p.parse_args(argv)

    journal_path = Path(args.journal)
    cal_path = Path(args.calibration)

    print("=" * 67)
    print("  STALE_PROPOSED_LEVELS RESET")
    print(f"  Journal:     {journal_path}")
    print(f"  Calibration: {cal_path}")
    print(f"  Mode:        {'EXECUTE' if args.execute else 'DRY-RUN'}")
    print("=" * 67)

    if not journal_path.exists():
        print(f"  ERROR: journal not found: {journal_path}")
        return 1

    # Step 1: backup (only if executing)
    if args.execute:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        print(f"\n  Step 1: backing up to *.pre_reset_{stamp}.bak ...")
        try:
            j_backup = backup_file(journal_path, stamp)
            print(f"    journal backup ok:     {j_backup.name}")
        except Exception as exc:
            print(f"    BACKUP FAILED: {exc} — aborting")
            return 1
        if cal_path.exists():
            try:
                c_backup = backup_file(cal_path, stamp)
                print(f"    calibration backup ok: {c_backup.name}")
            except Exception as exc:
                print(f"    CAL BACKUP FAILED: {exc} — aborting")
                return 1
        # Verify backups are loadable
        try:
            verify_backup(j_backup, journal_path)
            print(f"    journal backup verified (loadable + length match)")
            if cal_path.exists():
                verify_backup(c_backup, cal_path)
                print(f"    calibration backup verified")
        except Exception as exc:
            print(f"    BACKUP VERIFY FAILED: {exc} — aborting (originals untouched)")
            return 1

    # Step 2: dry-run scan to count and confirm before mutating
    print(f"\n  Step 2: scanning journal for bug-pattern records ...")
    n_journal, j_sample_ids = reset_journal(journal_path, dry_run=True)
    print(f"    {n_journal:,} records match the bug pattern")
    if j_sample_ids:
        print(f"    sample shadow_ids: {j_sample_ids}")

    n_cal, cal_ids = reset_calibration(cal_path, dry_run=True)
    print(f"  Step 3: scanning calibration for bug-pattern entries ...")
    print(f"    {n_cal} calibration entries match the bug pattern")
    if cal_ids:
        print(f"    affected shadow_ids: {cal_ids}")

    if not args.execute:
        print("\n  DRY-RUN complete — no files modified.")
        print("  Pass --execute to backup + reset.")
        return 0

    # Step 3: execute the actual reset
    print(f"\n  Step 4: executing journal reset ...")
    n_reset_actual, _ = reset_journal(journal_path, dry_run=False)
    print(f"    reset {n_reset_actual} journal records")

    print(f"  Step 5: executing calibration reset ...")
    n_cal_removed, _ = reset_calibration(cal_path, dry_run=False)
    print(f"    removed {n_cal_removed} calibration entries")

    # Step 6: post-mutation verification
    print(f"\n  Step 6: post-mutation verification ...")
    n_journal_post, _ = reset_journal(journal_path, dry_run=True)
    if n_journal_post != 0:
        print(f"    ⚠ post-reset journal still has {n_journal_post} bug-pattern records")
        print("       investigation required — original is preserved in the .bak file")
        return 1
    print(f"    ✓ journal: 0 bug-pattern records remain (was {n_reset_actual})")

    n_cal_post, _ = reset_calibration(cal_path, dry_run=True)
    if n_cal_post != 0:
        print(f"    ⚠ post-reset calibration still has {n_cal_post} bug entries")
        return 1
    print(f"    ✓ calibration: 0 bug entries remain (was {n_cal_removed})")

    print("\n" + "=" * 67)
    print(f"  RESET COMPLETE")
    print(f"  Journal records reset:      {n_reset_actual}")
    print(f"  Calibration entries removed: {n_cal_removed}")
    print(f"  Backups: data/*.pre_reset_{stamp}.bak")
    print(f"  Worker will re-simulate the {n_reset_actual} records on next cycles;")
    print(f"  with Fix A's guard active, they should now classify as")
    print(f"  permanent-FAILED('stale_proposed_levels') rather than phantom-profitable.")
    print("=" * 67)
    return 0


if __name__ == "__main__":
    sys.exit(main())
