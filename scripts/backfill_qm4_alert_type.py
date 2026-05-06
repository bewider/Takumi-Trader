"""Infer qm4_alert_type for old QM4 trades from captured M15/H1/H4 scores.

QM4 alert types and their thresholds:
  MTF   — M15+H1+H4 each at extreme:    WEAK ≤ 0.3 / STRONG ≥ 9.7
  MTFC  — M15+H1+H4 cumulative sum:     WEAK ≤ 0.9 / STRONG ≥ 29.1
  HTF   — D1+W1+MN each:                (cannot infer — we don't have D1/W1/MN for old trades)
  HTFC  — D1+W1+MN sum                  (same)
  XHTF  — D1+W1+MN tighter              (same)
  CUM   — all 6 TFs sum                 (same)
  PAIR/X — pair-specific signal          (cannot infer from stoch alone)

Strategy: for each untyped record with M15/H1/H4 captured, check if MTF or MTFC
condition is met for the trade direction.

  BUY = base strong + quote weak
  SELL = base weak + quote strong

If MTF (more restrictive) is met, label as MTF; else if MTFC met, label as MTFC.
If neither, label as "MTF?" (likely an HTF/XHTF/CUM/PAIR alert we can't determine).
For records with no signal data at all, leave as empty (truly unrecoverable).

Run while TAKUMI is stopped.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

DATA = Path(r"D:\Trading\TAKUMI Trader\data")
JOURNAL = DATA / "paper_trades_qm4.json"

# QM4 thresholds (from qm4_alerts.py)
MTF_WEAK = 0.3
MTF_STRONG = 9.7
MTFC_WEAK = 0.9
MTFC_STRONG = 29.1


def infer_alert_type(rec: dict) -> str | None:
    """Return inferred type or None if not enough data."""
    direction = rec.get("direction", "")
    if direction not in ("BUY", "SELL"):
        return None

    m15_b = float(rec.get("entry_m15_base", 0) or 0)
    m15_q = float(rec.get("entry_m15_quote", 0) or 0)
    h1_b = float(rec.get("entry_h1_base", 0) or 0)
    h1_q = float(rec.get("entry_h1_quote", 0) or 0)
    h4_b = float(rec.get("entry_h4_base", 0) or 0)
    h4_q = float(rec.get("entry_h4_quote", 0) or 0)

    # No signal data captured at all
    if (m15_b == 0 and m15_q == 0 and h1_b == 0 and h1_q == 0
            and h4_b == 0 and h4_q == 0):
        return None

    if direction == "BUY":
        # Base should be STRONG (≥9.7), quote should be WEAK (≤0.3)
        mtf_strong = (m15_b >= MTF_STRONG and h1_b >= MTF_STRONG and h4_b >= MTF_STRONG)
        mtf_weak = (m15_q <= MTF_WEAK and h1_q <= MTF_WEAK and h4_q <= MTF_WEAK)
        mtfc_strong = (m15_b + h1_b + h4_b) >= MTFC_STRONG
        mtfc_weak = (m15_q + h1_q + h4_q) <= MTFC_WEAK
    else:  # SELL
        mtf_strong = (m15_q >= MTF_STRONG and h1_q >= MTF_STRONG and h4_q >= MTF_STRONG)
        mtf_weak = (m15_b <= MTF_WEAK and h1_b <= MTF_WEAK and h4_b <= MTF_WEAK)
        mtfc_strong = (m15_q + h1_q + h4_q) >= MTFC_STRONG
        mtfc_weak = (m15_b + h1_b + h4_b) <= MTFC_WEAK

    # MTF is more restrictive — check first
    if mtf_strong or mtf_weak:
        return "MTF"
    if mtfc_strong or mtfc_weak:
        return "MTFC"
    # Has data but doesn't match MTF/MTFC — must be HTF/XHTF/CUM/PAIR
    return "?"


def main() -> int:
    if not JOURNAL.exists():
        print(f"ERROR: {JOURNAL} not found")
        return 1

    recs = json.loads(JOURNAL.read_text(encoding="utf-8"))
    if isinstance(recs, dict):
        recs_list = recs.get("trades", [])
    else:
        recs_list = recs

    inferred_mtf = 0
    inferred_mtfc = 0
    inferred_unknown = 0
    no_data = 0
    already_typed = 0

    for r in recs_list:
        if r.get("qm4_alert_type"):
            already_typed += 1
            continue
        t = infer_alert_type(r)
        if t == "MTF":
            r["qm4_alert_type"] = "MTF?"  # mark inferred with ?
            inferred_mtf += 1
        elif t == "MTFC":
            r["qm4_alert_type"] = "MTFC?"
            inferred_mtfc += 1
        elif t == "?":
            r["qm4_alert_type"] = "?"
            inferred_unknown += 1
        else:
            no_data += 1

    # Save
    if isinstance(recs, dict):
        recs["trades"] = recs_list
        out = recs
    else:
        out = recs_list
    JOURNAL.write_text(
        json.dumps(out, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    print(f"Already typed: {already_typed}")
    print(f"Inferred MTF? : {inferred_mtf}")
    print(f"Inferred MTFC?: {inferred_mtfc}")
    print(f"Inferred ?    : {inferred_unknown}  (had data but no MTF/MTFC match — likely HTF/XHTF/CUM/PAIR)")
    print(f"No signal data: {no_data}  (left blank — can't infer without historical reload)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
