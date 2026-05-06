"""Minute-level drill-down around London open transition (15:00-19:00 JST).

Buckets trades into 5-minute windows and shows P/L per window for each system.
Helps identify the exact minutes where losses cluster.

Session boundaries (JST summer):
  14:45-15:25  Frankfurt_open
  15:26-15:44  EU
  15:45-16:35  London_open
  16:36-20:44  London
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
    ("QM4",     "paper_trades_qm4.json"),
    ("A-tuned", "paper_trades_a_tuned.json"),
    ("B-tuned", "paper_trades_b_tuned.json"),
]

# Zoom window: 14:00 to 21:00 JST (covers Frankfurt_open through US_open)
WINDOW_START_MIN = 14 * 60  # 14:00 JST
WINDOW_END_MIN = 21 * 60    # 21:00 JST
BUCKET_MIN = 5              # 5-minute buckets

# Session boundary markers for display
SESSION_BOUNDARIES = [
    (14 * 60 + 45, "Frankfurt_open"),
    (15 * 60 + 26, "EU"),
    (15 * 60 + 45, "London_open"),
    (16 * 60 + 36, "London"),
    (20 * 60 + 45, "US_open"),
]


def load_closed(path: Path) -> list[dict]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = data.get("trades", [])
    return [r for r in data if r.get("close_reason")]


def bucket_minute(r: dict) -> int | None:
    """Return the minute-of-day (JST) rounded down to BUCKET_MIN."""
    try:
        dt = datetime.fromtimestamp(r["entry_time"], tz=JST)
    except Exception:
        return None
    m = dt.hour * 60 + dt.minute
    if not (WINDOW_START_MIN <= m < WINDOW_END_MIN):
        return None
    return (m // BUCKET_MIN) * BUCKET_MIN


def format_bucket(m: int) -> str:
    h, mm = divmod(m, 60)
    end_m = m + BUCKET_MIN
    eh, emm = divmod(end_m, 60)
    return f"{h:02d}:{mm:02d}-{eh:02d}:{emm:02d}"


def session_at_minute(m: int) -> str:
    """Return session name for given minute-of-day."""
    last = "Afternoon"
    for thr, name in SESSION_BOUNDARIES:
        if m < thr:
            return last
        last = name
    return last


def analyze_system(name: str, path: Path):
    closed = load_closed(path)
    if not closed:
        return

    buckets = defaultdict(list)
    for r in closed:
        b = bucket_minute(r)
        if b is not None:
            buckets[b].append(r)

    if not buckets:
        return

    print(f"\n═══ {name} — zoom 14:00–21:00 JST in {BUCKET_MIN}-min buckets ═══")
    print(f"  {'window':<13} {'sess':<15} {'n':>4} {'WR':>6} {'P/L':>9} {'avg':>7} {'bar'}")

    total_pnl = 0.0
    total_n = 0
    worst_windows: list[tuple[int, int, float, float]] = []

    for m in range(WINDOW_START_MIN, WINDOW_END_MIN, BUCKET_MIN):
        recs = buckets.get(m, [])
        n = len(recs)
        if n == 0:
            continue
        wins = sum(1 for r in recs if r.get("is_win"))
        pnl = sum(r.get("pnl_pips", 0.0) for r in recs)
        wr = wins / n * 100
        avg = pnl / n
        sess = session_at_minute(m)

        # Visual bar (one char per 2 pips, green right / red left)
        bar_w = int(abs(pnl) / 2)
        if pnl >= 0:
            bar = " " * 20 + "|" + "█" * min(bar_w, 20)
        else:
            bar = " " * (20 - min(bar_w, 20)) + "█" * min(bar_w, 20) + "|"

        marker = " !" if pnl < -5 else "  "
        print(f"  {format_bucket(m):<13} {sess:<15} {n:>4} {wr:>5.1f}% "
              f"{pnl:>+8.1f}p {avg:>+6.2f}p  {bar}{marker}")

        total_pnl += pnl
        total_n += n
        worst_windows.append((m, n, pnl, wr))

    # Rank worst windows with n>=5
    sig = [w for w in worst_windows if w[1] >= 5]
    sig.sort(key=lambda x: x[2])
    if sig and sig[0][2] < -5:
        print(f"\n  Worst windows (n≥5):")
        for m, n, pnl, wr in sig[:6]:
            if pnl >= -5:
                break
            print(f"    {format_bucket(m)}  {session_at_minute(m):<15}  n={n:>3}  WR={wr:>5.1f}%  P/L={pnl:>+7.1f}p")


def main():
    print(f"Minute-level London zoom ({BUCKET_MIN}-min buckets, 14:00-21:00 JST)")
    print(f"Today: {datetime.now(JST).strftime('%Y-%m-%d %H:%M JST')}")
    for name, fname in SYSTEMS:
        analyze_system(name, DATA / fname)


if __name__ == "__main__":
    main()
