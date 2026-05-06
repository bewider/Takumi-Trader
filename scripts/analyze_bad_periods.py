"""Analyze each system's journal for under-performing time periods.

For each of the 6 systems, bucket closed trades by:
  - Session (Australia, Tokyo_open, Morning, Afternoon, Frankfurt_open, ...)
  - Hour of day (JST)
  - Day of week
  - Session × Day of week

Flag buckets as "bad" if:
  - Sample size >= MIN_TRADES
  - Net P/L is negative OR WR < 45%

Then show:
  - Current performance (all trades)
  - Adjusted performance (after excluding bad buckets)
  - Delta in pips & WR
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA = Path(r"D:\Trading\TAKUMI Trader\data")
JST = timezone(timedelta(hours=9))

SYSTEMS = [
    ("Sv2",      "paper_trades.json"),
    ("SS",       "paper_trades_ss.json"),
    ("ATR",      "paper_trades_atr.json"),
    ("QM4",      "paper_trades_qm4.json"),
    ("A-tuned",  "paper_trades_a_tuned.json"),
    ("B-tuned",  "paper_trades_b_tuned.json"),
]

MIN_TRADES = 8   # require this many trades in a bucket to consider it meaningful
BAD_WR = 45.0    # WR below this is "bad"
DOW_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def load_closed(path: Path) -> list[dict]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = data.get("trades", [])
    return [r for r in data if r.get("close_reason")]


def stats(recs: list[dict]) -> dict:
    n = len(recs)
    if n == 0:
        return {"n": 0, "pnl": 0.0, "wr": 0.0, "wins": 0, "losses": 0, "avg": 0.0}
    wins = sum(1 for r in recs if r.get("is_win"))
    pnl = sum(r.get("pnl_pips", 0.0) for r in recs)
    return {
        "n": n,
        "pnl": round(pnl, 1),
        "wr": round(wins / n * 100, 1),
        "wins": wins,
        "losses": n - wins,
        "avg": round(pnl / n, 2),
    }


def bucket_by(recs: list[dict], keyfn) -> dict:
    groups = defaultdict(list)
    for r in recs:
        try:
            k = keyfn(r)
        except Exception:
            continue
        if k is None:
            continue
        groups[k].append(r)
    return {k: stats(v) for k, v in groups.items()}


def print_bad_buckets(title: str, buckets: dict, label_fmt=lambda k: str(k)):
    bad = [(k, s) for k, s in buckets.items()
           if s["n"] >= MIN_TRADES and (s["pnl"] < 0 or s["wr"] < BAD_WR)]
    if not bad:
        return []
    bad.sort(key=lambda kv: kv[1]["pnl"])  # worst first
    print(f"  {title}:")
    print(f"    {'bucket':<28} {'n':>4} {'WR':>6} {'P/L':>9} {'avg':>7}")
    for k, s in bad:
        marker = " !" if s["pnl"] < 0 else "  "
        print(f"    {label_fmt(k):<28} {s['n']:>4} {s['wr']:>5.1f}% {s['pnl']:>+8.1f}p {s['avg']:>+6.2f}p{marker}")
    return bad


def analyze_system(name: str, path: Path):
    closed = load_closed(path)
    if not closed:
        print(f"\n=== {name}: no data ===")
        return

    base = stats(closed)
    print(f"\n═══ {name} — {base['n']} closed trades ═══")
    print(f"  Current : n={base['n']:>4}  WR={base['wr']:>5.1f}%  P/L={base['pnl']:>+8.1f}p  avg={base['avg']:>+6.2f}p")

    # Bucket definitions
    by_session = bucket_by(closed, lambda r: r.get("session") or "Unknown")
    by_hour = bucket_by(
        closed,
        lambda r: datetime.fromtimestamp(r["entry_time"], tz=JST).hour,
    )
    by_dow = bucket_by(
        closed,
        lambda r: datetime.fromtimestamp(r["entry_time"], tz=JST).weekday(),
    )
    by_sess_dow = bucket_by(
        closed,
        lambda r: (
            r.get("session") or "Unknown",
            DOW_NAMES[datetime.fromtimestamp(r["entry_time"], tz=JST).weekday()],
        ),
    )

    bad_sess = print_bad_buckets("By session", by_session)
    bad_hour = print_bad_buckets(
        "By hour (JST)",
        by_hour,
        label_fmt=lambda h: f"{h:02d}:00-{(h+1)%24:02d}:00",
    )
    bad_dow = print_bad_buckets(
        "By day of week",
        by_dow,
        label_fmt=lambda d: DOW_NAMES[d],
    )
    bad_sess_dow = print_bad_buckets(
        "By session × day",
        by_sess_dow,
        label_fmt=lambda k: f"{k[1]} / {k[0]}",
    )

    # Compute "avoid" impact using each filter independently
    print(f"\n  What-if analysis (excluding bad buckets):")
    for filter_name, bad_keys, keyfn in [
        ("Avoid bad sessions        ", {k for k, _ in bad_sess}, lambda r: r.get("session") or "Unknown"),
        ("Avoid bad hours           ", {k for k, _ in bad_hour}, lambda r: datetime.fromtimestamp(r["entry_time"], tz=JST).hour),
        ("Avoid bad DoW             ", {k for k, _ in bad_dow}, lambda r: datetime.fromtimestamp(r["entry_time"], tz=JST).weekday()),
        ("Avoid bad session×DoW     ", {k for k, _ in bad_sess_dow}, lambda r: (r.get("session") or "Unknown", DOW_NAMES[datetime.fromtimestamp(r["entry_time"], tz=JST).weekday()])),
    ]:
        if not bad_keys:
            continue
        kept = [r for r in closed if keyfn(r) not in bad_keys]
        removed = len(closed) - len(kept)
        adj = stats(kept)
        delta_pnl = adj["pnl"] - base["pnl"]
        delta_wr = adj["wr"] - base["wr"]
        print(f"    {filter_name}  removed={removed:>3}  "
              f"→ n={adj['n']:>4}  WR={adj['wr']:>5.1f}%  P/L={adj['pnl']:>+8.1f}p  "
              f"Δpnl={delta_pnl:>+7.1f}p  ΔWR={delta_wr:>+5.1f}pp")


def main():
    print(f"Bad-period analysis (thresholds: n≥{MIN_TRADES} AND (P/L<0 OR WR<{BAD_WR}%))")
    print(f"Today (JST): {datetime.now(JST).strftime('%Y-%m-%d %H:%M')}")
    for name, fname in SYSTEMS:
        analyze_system(name, DATA / fname)


if __name__ == "__main__":
    main()
