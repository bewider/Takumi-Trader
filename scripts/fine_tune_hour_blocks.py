"""Zoom into 5-minute bins to find precise bad windows per pair.

For each of the 4 target pairs (GBPJPY, AUDUSD, GBPUSD, EURCAD):
  1. Collect all closed trades across 5 systems
  2. Bucket by 5-min window (288 per day)
  3. Run a sliding window (3-bin = 15min) to find persistent loss clusters
  4. Report precise HH:MM–HH:MM windows to block
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA = Path(r"D:\Trading\TAKUMI Trader\data")
JST = timezone(timedelta(hours=9))

SYSTEMS = [
    "paper_trades.json",          # Sv2
    "paper_trades_ss.json",       # SS
    "paper_trades_atr.json",      # ATR
    "paper_trades_a_tuned.json",  # A-tuned
    "paper_trades_b_tuned.json",  # B-tuned
]

TARGET_PAIRS = ["GBPJPY", "AUDUSD", "GBPUSD", "EURCAD"]

BIN_MIN = 5     # minutes per bucket
SCAN_RANGE = 2  # +/- bins merged for sliding window (total = 2*SCAN_RANGE+1 bins)


def load_pair_trades(pair: str) -> list[dict]:
    out = []
    for fname in SYSTEMS:
        p = DATA / fname
        if not p.exists():
            continue
        recs = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(recs, dict):
            recs = recs.get("trades", [])
        for r in recs:
            if r.get("close_reason") and r.get("pair") == pair:
                out.append(r)
    return out


def minute_of_day(r: dict) -> int:
    dt = datetime.fromtimestamp(r.get("entry_time", 0), tz=JST)
    return dt.hour * 60 + dt.minute


def bin_of_trade(r: dict) -> int:
    """Return the 5-min bin index (0..287)."""
    return minute_of_day(r) // BIN_MIN


def fmt_bin(b: int) -> str:
    """Format bin index as HH:MM."""
    total_min = b * BIN_MIN
    return f"{total_min // 60:02d}:{total_min % 60:02d}"


def fmt_window(start_bin: int, end_bin: int) -> str:
    """Format [start_bin, end_bin] inclusive as HH:MM–HH:MM."""
    end_min = (end_bin + 1) * BIN_MIN  # exclusive end for display
    return f"{fmt_bin(start_bin)}–{end_min // 60:02d}:{end_min % 60:02d}"


def analyze_pair(pair: str) -> None:
    trades = load_pair_trades(pair)
    n_total = len(trades)
    if n_total == 0:
        print(f"\n=== {pair} — no trades ===")
        return

    # Bucket by 5-min bin
    bins: dict[int, list[dict]] = defaultdict(list)
    for r in trades:
        bins[bin_of_trade(r)].append(r)

    # Overall stats
    wins = sum(1 for r in trades if r.get("is_win"))
    pnl = sum(r.get("pnl_pips", 0) for r in trades)
    print(f"\n{'═'*80}")
    print(f"  {pair} — {n_total} trades  WR={wins/n_total*100:.1f}%  P/L={pnl:+.1f}p")
    print(f"{'═'*80}")

    # Show all bins that have >= 2 trades, sorted by P/L (worst first)
    bin_stats = []
    for b, recs in bins.items():
        nb = len(recs)
        wb = sum(1 for r in recs if r.get("is_win"))
        pb = sum(r.get("pnl_pips", 0) for r in recs)
        bin_stats.append((b, nb, wb, pb))

    # Sliding window: for each bin, sum stats across +/- SCAN_RANGE neighbors
    # This finds "clusters" of adjacent bad bins
    cluster_stats = []
    for center_b in range(288):
        window = [b for b in range(center_b - SCAN_RANGE, center_b + SCAN_RANGE + 1)
                  if b in bins]
        if not window:
            continue
        recs = [r for b in window for r in bins[b]]
        n = len(recs)
        if n < 3:  # need at least 3 trades in the window
            continue
        w = sum(1 for r in recs if r.get("is_win"))
        p = sum(r.get("pnl_pips", 0) for r in recs)
        if p < 0:
            cluster_stats.append((center_b, n, w, p))

    # Sort by P/L ascending (worst first)
    cluster_stats.sort(key=lambda x: x[3])

    # Deduplicate overlapping clusters — keep only local minima
    # (a cluster is a local minimum if its P/L is worse than adjacent centers)
    seen_bins: set[int] = set()
    bad_clusters = []
    for center_b, n, w, p in cluster_stats:
        window_bins = set(range(center_b - SCAN_RANGE, center_b + SCAN_RANGE + 1))
        if seen_bins & window_bins:
            continue
        seen_bins |= window_bins
        bad_clusters.append((center_b, n, w, p))

    # Show worst clusters
    print(f"\n  Worst 15-minute clusters (sorted by P/L):")
    print(f"  {'Window':<16} {'n':>3} {'WR':>6} {'P/L':>9}")
    print(f"  {'-'*45}")
    for center_b, n, w, p in bad_clusters[:8]:
        if p > -3:
            break
        win_str = fmt_window(center_b - SCAN_RANGE, center_b + SCAN_RANGE)
        wr = w / n * 100
        flag = " !!!" if p < -15 else " !!" if p < -8 else ""
        print(f"  {win_str:<16} {n:>3} {wr:>5.1f}% {p:>+8.1f}p{flag}")

    # Recommendations: merge adjacent bad clusters into blockable ranges
    # Find contiguous "bad" bins (bins where P/L sum in window is strongly negative)
    bad_bins = set()
    for center_b, n, w, p in bad_clusters:
        if p < -5:  # significant badness
            for b in range(center_b - SCAN_RANGE, center_b + SCAN_RANGE + 1):
                if b in bins:
                    bad_bins.add(b)

    # Merge contiguous bad bins
    merged = []
    if bad_bins:
        sorted_bins = sorted(bad_bins)
        cur_start = sorted_bins[0]
        cur_end = sorted_bins[0]
        for b in sorted_bins[1:]:
            if b <= cur_end + 2:  # allow up to 2-bin gap (10 min)
                cur_end = b
            else:
                merged.append((cur_start, cur_end))
                cur_start = b
                cur_end = b
        merged.append((cur_start, cur_end))

    # Compute pnl impact of each merged range
    print(f"\n  RECOMMENDED BLOCK WINDOWS:")
    if not merged:
        print(f"    (no persistent bad clusters — pair is clean)")
        return

    for start_b, end_b in merged:
        recs = [r for b in range(start_b, end_b + 1) for r in bins.get(b, [])]
        n = len(recs)
        w = sum(1 for r in recs if r.get("is_win"))
        p = sum(r.get("pnl_pips", 0) for r in recs)
        wr = w / n * 100 if n else 0
        print(f"    {fmt_window(start_b, end_b):<16}  n={n:>3}  WR={wr:>5.1f}%  P/L={p:>+7.1f}p")


def main() -> int:
    print(f"Fine-tuning bad windows at {BIN_MIN}-minute resolution")
    print(f"Sliding window: +/- {SCAN_RANGE} bins ({(2*SCAN_RANGE+1)*BIN_MIN}-min sweep)")
    for pair in TARGET_PAIRS:
        analyze_pair(pair)

    # Also build the final config proposal
    print(f"\n\n{'═'*80}")
    print(f"  PROPOSED MT5 MIRROR CONFIG (minute-precise blocks)")
    print(f"{'═'*80}")
    print(f'"minute_blacklist_per_pair": {{')
    for pair in TARGET_PAIRS:
        trades = load_pair_trades(pair)
        if not trades:
            continue
        bins: dict[int, list[dict]] = defaultdict(list)
        for r in trades:
            bins[bin_of_trade(r)].append(r)

        cluster_stats = []
        for center_b in range(288):
            window = [b for b in range(center_b - SCAN_RANGE, center_b + SCAN_RANGE + 1)
                      if b in bins]
            if not window:
                continue
            recs = [r for b in window for r in bins[b]]
            if len(recs) < 3:
                continue
            p = sum(r.get("pnl_pips", 0) for r in recs)
            if p < 0:
                cluster_stats.append((center_b, len(recs), p))
        cluster_stats.sort(key=lambda x: x[2])

        seen_bins: set[int] = set()
        bad_bins: set[int] = set()
        for center_b, n, p in cluster_stats:
            window_bins = set(range(center_b - SCAN_RANGE, center_b + SCAN_RANGE + 1))
            if seen_bins & window_bins:
                continue
            seen_bins |= window_bins
            if p < -5:
                for b in range(center_b - SCAN_RANGE, center_b + SCAN_RANGE + 1):
                    if b in bins:
                        bad_bins.add(b)

        if bad_bins:
            sorted_bins = sorted(bad_bins)
            merged = []
            cur_start = sorted_bins[0]
            cur_end = sorted_bins[0]
            for b in sorted_bins[1:]:
                if b <= cur_end + 2:
                    cur_end = b
                else:
                    merged.append((cur_start, cur_end))
                    cur_start = b
                    cur_end = b
            merged.append((cur_start, cur_end))
            windows = [[fmt_bin(s), fmt_bin(e + 1) if (e + 1) * BIN_MIN < 24 * 60 else "24:00"] for s, e in merged]
            ranges_str = ", ".join(f'["{s}", "{e}"]' for s, e in windows)
            print(f'  "{pair}": [{ranges_str}],')
    print(f'}}')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
