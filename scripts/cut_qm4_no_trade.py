"""Remove QM4 journal entries that opened during 05:00–07:57 JST.

These morning NO_TRADE window trades have been losing money. The user
wants them cut from the journal so the performance stats reflect only
trades taken during active hours.

Run while TAKUMI is stopped.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA = Path(r"D:\Trading\TAKUMI Trader\data")
JOURNAL = DATA / "paper_trades_qm4.json"
JST = timezone(timedelta(hours=9))

# Block window
NO_TRADE_START = 5 * 60       # 05:00 JST (300 minutes)
NO_TRADE_END   = 7 * 60 + 57  # 07:57 JST (477 minutes)


def is_no_trade(entry_time: float) -> bool:
    dt = datetime.fromtimestamp(entry_time, tz=JST)
    hm = dt.hour * 60 + dt.minute
    return NO_TRADE_START <= hm <= NO_TRADE_END


def main() -> int:
    if not JOURNAL.exists():
        print(f"ERROR: {JOURNAL} not found")
        return 1

    recs = json.loads(JOURNAL.read_text(encoding="utf-8"))
    if isinstance(recs, dict):
        recs_list = recs.get("trades", [])
    else:
        recs_list = recs

    total = len(recs_list)
    cut = []
    kept = []

    for r in recs_list:
        et = r.get("entry_time", 0)
        if et > 0 and is_no_trade(et):
            cut.append(r)
        else:
            kept.append(r)

    # Stats on what we're cutting
    cut_closed = [r for r in cut if r.get("close_reason")]
    cut_open = [r for r in cut if not r.get("close_reason")]
    cut_pnl = sum(r.get("pnl_pips", 0.0) for r in cut_closed)
    cut_wins = sum(1 for r in cut_closed if r.get("is_win"))
    cut_losses = len(cut_closed) - cut_wins
    cut_wr = cut_wins / len(cut_closed) * 100 if cut_closed else 0

    print(f"QM4 journal: {total} total records")
    print(f"  Cutting {len(cut)} trades in 05:00-07:57 JST window:")
    print(f"    Closed: {len(cut_closed)} (W:{cut_wins} L:{cut_losses} WR:{cut_wr:.1f}%)")
    print(f"    P/L lost by cutting: {cut_pnl:+.1f}p")
    print(f"    OPEN:   {len(cut_open)}")
    print(f"  Keeping {len(kept)} trades")
    print()

    # Show each cut trade
    if cut:
        print("  Cut trades:")
        for r in cut:
            dt = datetime.fromtimestamp(r.get("entry_time", 0), tz=JST)
            t = dt.strftime("%m-%d %H:%M")
            pair = r.get("pair", "?")
            dr = r.get("direction", "?")
            pnl = r.get("pnl_pips", 0.0)
            reason = r.get("close_reason", "OPEN")
            sess = r.get("session", "")
            print(f"    {t}  {pair:7} {dr:4}  {pnl:>+6.1f}p  {reason or 'OPEN':8}  sess={sess}")

    # Save
    if isinstance(recs, dict):
        recs["trades"] = kept
        out = recs
    else:
        out = kept
    JOURNAL.write_text(
        json.dumps(out, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    print(f"\nSaved. {len(kept)} records remain in {JOURNAL.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
