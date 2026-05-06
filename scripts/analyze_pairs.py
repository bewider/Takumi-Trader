"""Per-pair performance breakdown for each system (except QM4).

For each system, ranks all pairs by total P/L and highlights:
  - Top performers (most profitable)
  - Bottom performers (most costly)
  - Win-rate outliers
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

DATA = Path(r"D:\Trading\TAKUMI Trader\data")

SYSTEMS = [
    ("Sv2",      "paper_trades.json"),
    ("SS",       "paper_trades_ss.json"),
    ("ATR",      "paper_trades_atr.json"),
    ("A-tuned",  "paper_trades_a_tuned.json"),
    ("B-tuned",  "paper_trades_b_tuned.json"),
]


def load_closed(path: Path) -> list[dict]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = data.get("trades", [])
    return [r for r in data if r.get("close_reason")]


def analyze_system(name: str, path: Path):
    closed = load_closed(path)
    if not closed:
        return

    # Overall stats
    total_n = len(closed)
    total_wins = sum(1 for r in closed if r.get("is_win"))
    total_pnl = sum(r.get("pnl_pips", 0.0) for r in closed)
    total_wr = total_wins / total_n * 100 if total_n else 0

    print(f"\n{'=' * 80}")
    print(f"  {name}  |  {total_n} trades  |  WR {total_wr:.1f}%  |  P/L {total_pnl:+.1f}p  |  avg {total_pnl / total_n:+.2f}p")
    print(f"{'=' * 80}")

    # Group by pair
    pairs: dict[str, list[dict]] = defaultdict(list)
    for r in closed:
        pairs[r.get("pair", "?")].append(r)

    # Compute stats per pair
    rows: list[tuple[str, int, int, float, float, float, int, int]] = []
    for pair_name, recs in pairs.items():
        n = len(recs)
        wins = sum(1 for r in recs if r.get("is_win"))
        losses = n - wins
        pnl = sum(r.get("pnl_pips", 0.0) for r in recs)
        wr = wins / n * 100 if n else 0
        avg = pnl / n if n else 0
        tp = sum(1 for r in recs if r.get("close_reason") == "tp_hit")
        sl = sum(1 for r in recs if r.get("close_reason") == "sl_hit")
        avg_sl = sum(r.get("sl_pips", 0) for r in recs) / n if n else 0
        avg_tp = sum(r.get("tp_pips", 0) for r in recs) / n if n else 0
        rows.append((pair_name, n, wins, losses, wr, pnl, avg, tp, sl, avg_sl, avg_tp))

    # Sort by P/L descending
    rows.sort(key=lambda x: x[5], reverse=True)

    print(f"  {'Pair':<8} {'n':>4} {'W':>3} {'L':>3} {'WR':>6} {'P/L':>9} {'avg':>7} {'TP':>3} {'SL':>3} {'avgSL':>6} {'avgTP':>6}")
    print(f"  {'-' * 72}")

    top_pairs = []
    bottom_pairs = []

    for pair_name, n, wins, losses, wr, pnl, avg, tp, sl, avg_sl, avg_tp in rows:
        marker = ""
        if pnl >= 10 and n >= 5:
            marker = " +"
            top_pairs.append((pair_name, n, wr, pnl, avg))
        elif pnl <= -10 and n >= 5:
            marker = " -"
            bottom_pairs.append((pair_name, n, wr, pnl, avg))
        elif n < 5:
            marker = " ?"
        print(f"  {pair_name:<8} {n:>4} {wins:>3} {losses:>3} {wr:>5.1f}% {pnl:>+8.1f}p {avg:>+6.2f}p {tp:>3} {sl:>3} {avg_sl:>5.1f}p {avg_tp:>5.1f}p{marker}")

    # Summary
    if top_pairs or bottom_pairs:
        print()
        if top_pairs:
            top_pnl = sum(p[3] for p in top_pairs)
            print(f"  TOP pairs ({len(top_pairs)} pairs, {sum(p[1] for p in top_pairs)} trades): {top_pnl:+.1f}p")
            for p, n, wr, pnl, avg in top_pairs:
                print(f"    {p:<8} n={n:>3}  WR={wr:>5.1f}%  P/L={pnl:>+7.1f}p  avg={avg:>+5.2f}p")
        if bottom_pairs:
            bot_pnl = sum(p[3] for p in bottom_pairs)
            print(f"  BOTTOM pairs ({len(bottom_pairs)} pairs, {sum(p[1] for p in bottom_pairs)} trades): {bot_pnl:+.1f}p")
            for p, n, wr, pnl, avg in bottom_pairs:
                print(f"    {p:<8} n={n:>3}  WR={wr:>5.1f}%  P/L={pnl:>+7.1f}p  avg={avg:>+5.2f}p")

        # What-if: keep only top, drop bottom
        kept = [r for r in closed if r.get("pair") not in {p[0] for p in bottom_pairs}]
        kept_n = len(kept)
        kept_pnl = sum(r.get("pnl_pips", 0.0) for r in kept)
        kept_wins = sum(1 for r in kept if r.get("is_win"))
        kept_wr = kept_wins / kept_n * 100 if kept_n else 0
        removed = total_n - kept_n
        print(f"\n  What-if (drop bottom pairs): removed={removed} trades")
        print(f"    {total_n:>4} trades  WR={total_wr:>5.1f}%  P/L={total_pnl:>+8.1f}p  ->  "
              f"{kept_n:>4} trades  WR={kept_wr:>5.1f}%  P/L={kept_pnl:>+8.1f}p  "
              f"(+{kept_pnl - total_pnl:+.1f}p, +{kept_wr - total_wr:+.1f}pp WR)")


def main():
    print("Per-pair performance analysis (closed trades only)")
    for name, fname in SYSTEMS:
        analyze_system(name, DATA / fname)


if __name__ == "__main__":
    main()
