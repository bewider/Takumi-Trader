"""One-off migration: copy signal data from tracker → journal for OPEN trades.

The previous bug was that `open_paper_trade` wrote the journal record BEFORE
`_stamp_entry_signals` filled the signal fields. So any trades still OPEN in
the journal have zeros for the signal columns, while their corresponding
tracker entries DO have the stamped values (tracker is serialized on every
save, and the stamping does happen on the tracker).

This script walks all 6 trackers, looks up matching OPEN journal records by
(pair, entry_time), and copies the signal fields from tracker → journal.

SAFE to run while TAKUMI is stopped. Run before restarting to backfill the
currently-OPEN trades visible in the UI.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

DATA = Path(r"D:\Trading\TAKUMI Trader\data")

PAIRS = [
    ("tracked_trades.json",          "paper_trades.json"),
    ("tracked_trades_ss.json",       "paper_trades_ss.json"),
    ("tracked_trades_atr.json",      "paper_trades_atr.json"),
    ("tracked_trades_qm4.json",      "paper_trades_qm4.json"),
    ("tracked_trades_a_tuned.json",  "paper_trades_a_tuned.json"),
    ("tracked_trades_b_tuned.json",  "paper_trades_b_tuned.json"),
]

SYNC_FIELDS = (
    "entry_m5_base", "entry_m5_quote",
    "entry_m15_base", "entry_m15_quote",
    "entry_h1_base", "entry_h1_quote",
    "entry_h4_base", "entry_h4_quote",
    "entry_div_spread", "entry_spread_std",
    "entry_h1_atr_pips", "entry_tier", "entry_structural",
    "entry_tick_volume_ratio", "entry_momentum_buildup_sec",
    "entry_dist_day_high_pips", "entry_dist_day_low_pips",
    "entry_dist_week_high_pips", "entry_dist_week_low_pips",
    "entry_dist_month_high_pips", "entry_dist_month_low_pips",
    "entry_cluster_count", "entry_dist_00_pips", "entry_dist_000_pips",
    "entry_session_minutes_in", "entry_day_of_week",
    "entry_prev_trade_result", "entry_concurrent_trades",
    "entry_m1_body_pct", "entry_m1_direction", "entry_atr_ratio",
)


def main() -> int:
    total_matched = 0
    total_fields_updated = 0

    for tracker_name, journal_name in PAIRS:
        tracker_file = DATA / tracker_name
        journal_file = DATA / journal_name

        if not tracker_file.exists() or not journal_file.exists():
            print(f"  [{tracker_name}] skipping — file(s) missing")
            continue

        try:
            with open(tracker_file, "r", encoding="utf-8") as f:
                tracker_data = json.load(f)
        except Exception as exc:
            print(f"  [{tracker_name}] failed to load: {exc}")
            continue

        try:
            with open(journal_file, "r", encoding="utf-8") as f:
                journal_data = json.load(f)
        except Exception as exc:
            print(f"  [{journal_name}] failed to load: {exc}")
            continue

        # Tracker format: plain list of trade dicts OR {"active_trades": [...]}
        if isinstance(tracker_data, list):
            active_trades = tracker_data
        elif isinstance(tracker_data, dict):
            active_trades = tracker_data.get("active_trades", [])
            if not isinstance(active_trades, list):
                active_trades = []
        else:
            active_trades = []

        # Build lookup: (pair, int(entry_time)) -> tracker trade dict
        tracker_by_key = {}
        for t in active_trades:
            pair = t.get("pair", "")
            et = t.get("entry_time", 0)
            if pair and et:
                tracker_by_key[(pair, int(et))] = t

        if not tracker_by_key:
            print(f"  [{tracker_name}] no active trades — nothing to sync")
            continue

        # Journal format: list of records OR {"trades": [...]}
        if isinstance(journal_data, dict):
            records = journal_data.get("trades", [])
        else:
            records = journal_data

        matched_here = 0
        fields_here = 0
        for rec in records:
            # Only OPEN records need syncing
            if rec.get("close_reason"):
                continue
            pair = rec.get("pair", "")
            et = rec.get("entry_time", 0)
            if not (pair and et):
                continue
            trk = tracker_by_key.get((pair, int(et)))
            if trk is None:
                continue

            matched_here += 1
            for field in SYNC_FIELDS:
                trk_val = trk.get(field)
                if trk_val is None:
                    continue
                # Only overwrite if the journal value is still the default zero/empty
                cur = rec.get(field)
                is_default = (
                    cur is None
                    or cur == 0
                    or cur == 0.0
                    or cur == ""
                )
                if is_default and trk_val not in (None, 0, 0.0, ""):
                    rec[field] = trk_val
                    fields_here += 1

        if matched_here > 0:
            # Save journal back in its original format
            if isinstance(journal_data, dict):
                journal_data["trades"] = records
                out = journal_data
            else:
                out = records
            with open(journal_file, "w", encoding="utf-8") as f:
                json.dump(out, f, indent=2, default=str)
            print(
                f"  [{journal_name}] matched {matched_here} OPEN records, "
                f"updated {fields_here} fields"
            )
            total_matched += matched_here
            total_fields_updated += fields_here
        else:
            print(f"  [{journal_name}] no OPEN records matched")

    print()
    print(
        f"Total: {total_matched} OPEN records matched, "
        f"{total_fields_updated} field values filled in"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
