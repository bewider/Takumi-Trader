"""Find pairs that become GBPJPY-level performers once bad hours are removed.

For each pair, across all 5 currency-strength systems combined:
  1. Compute baseline stats (all hours)
  2. Test removing each possible "bad hours" combination
  3. Find the version with highest P/L that still has >= 8 trades

Reports pairs where hour-filtering lifts them into top-tier performance.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA = Path(r"D:\Trading\TAKUMI Trader\data")
JST = timezone(timedelta(hours=9))

SYSTEMS = [
    ("Sv2",     "paper_trades.json"),
    ("SS",      "paper_trades_ss.json"),
    ("ATR",     "paper_trades_atr.json"),
    ("A-tuned", "paper_trades_a_tuned.json"),
    ("B-tuned", "paper_trades_b_tuned.json"),
]

MIN_TRADES = 8
MIN_WR_AFTER = 80.0     # Target: >= 80% WR after filter (GBPJPY is 91%)
MIN_PNL_AFTER = 30.0    # Target: >= +30p after filter


def load_closed_by_pair() -> dict[str, list[dict]]:
    by_pair: dict[str, list[dict]] = defaultdict(list)
    for _, fname in SYSTEMS:
        p = DATA / fname
        if not p.exists():
            continue
        recs = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(recs, dict):
            recs = recs.get("trades", [])
        for r in recs:
            if r.get("close_reason"):
                by_pair[r.get("pair", "?")].append(r)
    return by_pair


def hour_of(r: dict) -> int:
    return datetime.fromtimestamp(r.get("entry_time", 0), tz=JST).hour


def stats(recs: list[dict]) -> tuple[int, int, float, float]:
    n = len(recs)
    if n == 0:
        return 0, 0, 0.0, 0.0
    wins = sum(1 for r in recs if r.get("is_win"))
    pnl = sum(r.get("pnl_pips", 0.0) for r in recs)
    wr = wins / n * 100
    return n, wins, pnl, wr


def find_best_hour_filter(recs: list[dict]) -> tuple[set[int], dict]:
    """Find the set of hours to BLOCK that maximizes P/L subject to >= 8 trades.

    Greedy approach: start with all hours allowed, then remove the hour whose
    removal most improves P/L, until no improvement possible or n drops below 8.
    """
    allowed = {h for h in range(24)}
    best = dict(zip(("n", "wins", "pnl", "wr"), stats([r for r in recs if hour_of(r) in allowed])))

    while True:
        # For each allowed hour, test removing it
        best_candidate = None
        for h in list(allowed):
            trial = allowed - {h}
            sub = [r for r in recs if hour_of(r) in trial]
            if len(sub) < MIN_TRADES:
                continue
            n, w, pnl, wr = stats(sub)
            # Prefer higher P/L; on tie, higher WR
            if (pnl > best["pnl"] + 0.01) or (
                abs(pnl - best["pnl"]) < 0.01 and wr > best["wr"] + 0.5
            ):
                if best_candidate is None or pnl > best_candidate[2]:
                    best_candidate = (h, n, pnl, wr, w)

        if best_candidate is None:
            break
        h_to_remove, n, pnl, wr, w = best_candidate
        allowed.discard(h_to_remove)
        best = {"n": n, "wins": w, "pnl": pnl, "wr": wr}

    blocked = set(range(24)) - allowed
    return blocked, best


def main() -> int:
    by_pair = load_closed_by_pair()
    print(f"Analyzing {len(by_pair)} pairs across 5 systems (Sv2/SS/ATR/A-tuned/B-tuned)\n")

    # Baseline reference: GBPJPY
    gbp = by_pair.get("GBPJPY", [])
    if gbp:
        _n, _w, _pnl, _wr = stats(gbp)
        print(f"REFERENCE — GBPJPY (no filter): n={_n}  WR={_wr:.1f}%  P/L={_pnl:+.1f}p")
        print(f"Target for gems: n>={MIN_TRADES}, WR>={MIN_WR_AFTER:.0f}%, P/L>={MIN_PNL_AFTER:+.0f}p\n")

    results: list[tuple[str, int, float, float, set[int], dict]] = []
    for pair, recs in by_pair.items():
        if len(recs) < MIN_TRADES:
            continue
        n0, w0, pnl0, wr0 = stats(recs)
        blocked, best = find_best_hour_filter(recs)
        results.append((pair, n0, pnl0, wr0, blocked, best))

    # Sort: pairs that reach "gem" status first, by post-filter P/L
    gems = [r for r in results
            if r[5]["wr"] >= MIN_WR_AFTER and r[5]["pnl"] >= MIN_PNL_AFTER]
    gems.sort(key=lambda x: -x[5]["pnl"])

    honorable = [r for r in results
                 if r not in gems
                 and r[5]["wr"] >= 70.0 and r[5]["pnl"] >= 15.0
                 and r[5]["n"] >= MIN_TRADES]
    honorable.sort(key=lambda x: -x[5]["pnl"])

    # Display
    print(f"=== GEMS (n>={MIN_TRADES}, WR>={MIN_WR_AFTER:.0f}%, P/L>={MIN_PNL_AFTER:+.0f}p after hour filter) ===")
    if not gems:
        print("  None found with the strict criteria.\n")
    for pair, n0, pnl0, wr0, blocked, best in gems:
        blocked_str = ", ".join(f"{h:02d}h" for h in sorted(blocked)) or "none"
        print(f"  {pair:<8} ORIG: n={n0:>3} WR={wr0:>5.1f}% P/L={pnl0:>+6.1f}p  "
              f"→ FILTERED: n={best['n']:>3} WR={best['wr']:>5.1f}% P/L={best['pnl']:>+6.1f}p  "
              f"[block: {blocked_str}]")

    print()
    print(f"=== HONORABLE MENTIONS (n>={MIN_TRADES}, WR>=70%, P/L>=+15p) ===")
    for pair, n0, pnl0, wr0, blocked, best in honorable:
        blocked_str = ", ".join(f"{h:02d}h" for h in sorted(blocked)) or "none"
        print(f"  {pair:<8} ORIG: n={n0:>3} WR={wr0:>5.1f}% P/L={pnl0:>+6.1f}p  "
              f"→ FILTERED: n={best['n']:>3} WR={best['wr']:>5.1f}% P/L={best['pnl']:>+6.1f}p  "
              f"[block: {blocked_str}]")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
