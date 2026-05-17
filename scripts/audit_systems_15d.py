"""15-day cross-system trade audit.

Walks every paper_trades_*.json, checks structural integrity per trade,
computes basic per-system stats, and flags anomalies. Read-only — does
not mutate any journal.

Checks performed per trade:
  1. Identity: entry_price, entry_time, pair, direction populated
  2. SL/TP sign: SL on correct side of entry, TP on correct side
  3. PnL sign vs exit reason:
     * close_reason='tp_hit' should have pnl_pips > 0 (or ~ 0 with tight stops)
     * close_reason='sl_hit' should have pnl_pips < 0 (or ~ 0 with break-even stops)
  4. Duration sanity: 0 < duration_minutes < 24*60*7 (1 week max plausible)
  5. PnL magnitude: |pnl_pips| < 1000 (anything else is outlier)
  6. Direction valid: BUY or SELL

Cross-trade checks:
  7. Duplicates: same (pair, direction, entry_time) within 60 seconds
  8. Currently open trades: count + oldest open age
  9. NO_TRADE window violations (Sv2-family: never entry between
     05:00-07:57 JST; QM4: never between 05:00-07:57 JST)

Run from repo root:
    python scripts/audit_systems_15d.py
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


JST = timezone(timedelta(hours=9))


# Per-system journal mapping
SYSTEMS = {
    "Sv2 (System A)":        "paper_trades.json",
    "Sv2-tuned":             "paper_trades_a_tuned.json",
    "Sv2+SS (System B)":     "paper_trades_ss.json",
    "Sv2+ATR (System C)":    "paper_trades_atr.json",
    "Sv2+SS-tuned":          "paper_trades_b_tuned.json",
    "Sv2-upgraded":          "paper_trades_sv2_upgraded.json",
    "Breakout":              "paper_trades_breakout.json",
    "Squeeze":               "paper_trades_squeeze.json",
    "Squeeze-REV":           "paper_trades_squeeze_rev.json",
    "Divergence":            "paper_trades_divergence.json",
    "DTC-combo":             "paper_trades_dtc_combo.json",
    "QM4":                   "paper_trades_qm4.json",
    "Sv2-live":              "paper_trades_sv2_live.json",
    "Sv2-Tun-live":          "paper_trades_sv2_a_tuned_live.json",
    "Sv2+SS-live":           "paper_trades_sv2_ss_live.json",
    "Sv2+SS-Tun-live":       "paper_trades_sv2_b_tuned_live.json",
    "Sv2+ATR-live":          "paper_trades_sv2_atr_live.json",
    "AU1 London":            "paper_trades_au1_london.json",
    "AU2 NY-ORB":            "paper_trades_au2_ny_orb.json",
    "AU3 Pullback":          "paper_trades_au3_pullback.json",
    "AU4 USD-Div":           "paper_trades_au4_divergence.json",
    "AU5 MeanRev":           "paper_trades_au5_mean_rev.json",
}


def load_trades(path: Path) -> list[dict]:
    if not path.exists() or path.stat().st_size < 10:
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return [t for t in data if isinstance(t, dict)]
    except json.JSONDecodeError:
        return []


def filter_recent(trades: list[dict], days: int) -> list[dict]:
    """Last N days of trades by entry_time."""
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    return [t for t in trades if t.get("entry_time", 0) >= cutoff]


def pip_for(pair: str) -> float:
    return 0.01 if "JPY" in pair else 0.0001


def check_structural(trade: dict) -> list[str]:
    """Per-trade structural integrity checks. Returns list of issue strings."""
    issues = []
    pair = trade.get("pair", "")
    direction = trade.get("direction", "")
    entry = trade.get("entry_price", 0.0)
    sl_price = trade.get("sl_price", 0.0)
    tp_price = trade.get("tp_price", 0.0)
    close_reason = trade.get("close_reason", "")
    pnl = trade.get("pnl_pips", 0.0)
    duration = trade.get("duration_minutes", 0.0)
    is_gold = pair == "XAUUSD"

    # 1. Identity
    if not pair:
        issues.append("missing pair")
    if direction not in ("BUY", "SELL"):
        issues.append(f"invalid direction: {direction!r}")
    if entry <= 0:
        issues.append(f"invalid entry_price: {entry}")

    # 2. SL/TP sign vs direction
    if entry > 0 and sl_price > 0:
        if direction == "BUY" and sl_price >= entry:
            issues.append(f"BUY but SL ({sl_price}) >= entry ({entry})")
        elif direction == "SELL" and sl_price <= entry:
            issues.append(f"SELL but SL ({sl_price}) <= entry ({entry})")
    if entry > 0 and tp_price > 0:
        if direction == "BUY" and tp_price <= entry:
            issues.append(f"BUY but TP ({tp_price}) <= entry ({entry})")
        elif direction == "SELL" and tp_price >= entry:
            issues.append(f"SELL but TP ({tp_price}) >= entry ({entry})")

    # 3. PnL sign vs exit reason (only for closed trades)
    if close_reason == "tp_hit" and pnl < -1.0:
        issues.append(f"tp_hit but pnl negative: {pnl:+.2f}p")
    if close_reason == "sl_hit" and pnl > 1.0:
        issues.append(f"sl_hit but pnl positive: {pnl:+.2f}p")

    # 4. Duration sanity (only closed trades have meaningful duration).
    # Note: duration=0 was common pre-2026-05-17 due to the wrong-side-
    # SL bug in paper_trader.open_paper_trade (trade hit SL at entry+
    # epsilon for ~0p in <1min, rounded to 0). With that fix shipped,
    # remaining duration=0 trades are genuine immediate SL hits.
    if close_reason and duration <= 0:
        issues.append("closed but duration=0")
    if duration > 60 * 24 * 7:
        issues.append(f"duration > 1 week: {duration:.0f}min")

    # 5. PnL magnitude.
    # XAUUSD pip = $0.01 movement; gold's typical daily range of $50-200
    # = 5000-20000 pips. Forex-tuned threshold of 1000 pips would false-
    # positive on every gold trade. Use higher threshold for gold pairs.
    pnl_threshold = 50000 if is_gold else 1000
    if abs(pnl) > pnl_threshold:
        issues.append(f"|pnl| > {pnl_threshold}p: {pnl:+.2f}p")
    import math
    if not math.isfinite(pnl):
        issues.append(f"non-finite pnl: {pnl}")

    return issues


def check_duplicates(trades: list[dict]) -> list[tuple[int, int]]:
    """Find pairs of trades with same (pair, direction, entry_time within 60s).
    Returns list of (idx_a, idx_b) duplicate pairs."""
    dups = []
    by_key: dict[tuple, list[int]] = defaultdict(list)
    for i, t in enumerate(trades):
        key = (t.get("pair", ""), t.get("direction", ""))
        by_key[key].append(i)
    for key, idxs in by_key.items():
        if len(idxs) < 2:
            continue
        # Sort by entry_time, scan adjacent
        idxs_sorted = sorted(idxs, key=lambda i: trades[i].get("entry_time", 0))
        for i in range(len(idxs_sorted) - 1):
            a, b = idxs_sorted[i], idxs_sorted[i + 1]
            ta = trades[a].get("entry_time", 0)
            tb = trades[b].get("entry_time", 0)
            if abs(tb - ta) < 60:
                dups.append((a, b))
    return dups


def check_no_trade_window(trade: dict, system: str) -> str | None:
    """NO_TRADE window: 05:00-07:57 JST. Applies to all main systems.
    Returns the violation string or None."""
    et = trade.get("entry_time", 0)
    if et <= 0:
        return None
    dt_jst = datetime.fromtimestamp(et, tz=JST)
    minute_of_day = dt_jst.hour * 60 + dt_jst.minute
    if 300 <= minute_of_day <= 477:  # 05:00..07:57
        return f"entry at {dt_jst.strftime('%H:%M')} JST in NO_TRADE window (05:00-07:57)"
    return None


def audit_system(system: str, path: Path, days: int) -> dict:
    """Run all checks on a single system's journal."""
    all_trades = load_trades(path)
    recent = filter_recent(all_trades, days)
    n_recent = len(recent)

    # Closed vs open
    closed = [t for t in recent if t.get("close_reason", "")]
    open_trades = [t for t in recent if not t.get("close_reason", "")]
    n_closed = len(closed)
    n_open = len(open_trades)

    # PnL stats over closed
    pnls = [float(t.get("pnl_pips", 0.0)) for t in closed]
    if pnls:
        wins = [p for p in pnls if p > 0]
        win_rate = 100.0 * len(wins) / len(pnls)
        mean_pnl = sum(pnls) / len(pnls)
        total = sum(pnls)
        max_pnl = max(pnls)
        min_pnl = min(pnls)
    else:
        win_rate = 0.0; mean_pnl = 0.0; total = 0.0; max_pnl = 0.0; min_pnl = 0.0

    # Structural issues
    issues_per_trade = []
    for t in recent:
        issues = check_structural(t)
        # NO_TRADE window check
        ntv = check_no_trade_window(t, system)
        if ntv:
            issues.append(ntv)
        if issues:
            issues_per_trade.append((t, issues))

    # Duplicates
    duplicates = check_duplicates(recent)

    # Oldest open trade
    oldest_open_age_h = None
    if open_trades:
        now = datetime.now(timezone.utc).timestamp()
        oldest_open = min(open_trades, key=lambda t: t.get("entry_time", 0))
        oldest_open_age_h = (now - oldest_open.get("entry_time", now)) / 3600

    return {
        "n_recent": n_recent,
        "n_closed": n_closed,
        "n_open": n_open,
        "win_rate": win_rate,
        "mean_pnl": mean_pnl,
        "total_pnl": total,
        "max_pnl": max_pnl,
        "min_pnl": min_pnl,
        "issues_per_trade": issues_per_trade,
        "duplicates": duplicates,
        "oldest_open_age_h": oldest_open_age_h,
        "open_trades_sample": open_trades[:3],
    }


def main() -> int:
    DAYS = 15
    print("=" * 78)
    print(f"  SYSTEM AUDIT — past {DAYS} trading days")
    print(f"  Run at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 78)

    total_issues = 0
    total_dups = 0
    flagged_systems: list[str] = []

    # Per-system stats table
    print()
    print(f"{'System':<20s} {'Recent':>7s} {'Closed':>7s} {'Open':>5s} "
          f"{'WinRt':>6s} {'MeanΔ':>7s} {'Total':>9s} {'Issues':>7s} {'Dups':>5s}")
    print("-" * 86)

    audits = {}
    for system, fname in SYSTEMS.items():
        path = Path("data") / fname
        report = audit_system(system, path, DAYS)
        audits[system] = report

        n_issues = len(report["issues_per_trade"])
        n_dups = len(report["duplicates"])
        total_issues += n_issues
        total_dups += n_dups
        if n_issues or n_dups or (report["n_open"] > 0 and report.get("oldest_open_age_h", 0) and report["oldest_open_age_h"] > 24):
            flagged_systems.append(system)

        print(
            f"{system:<20s} {report['n_recent']:>7d} {report['n_closed']:>7d} {report['n_open']:>5d} "
            f"{report['win_rate']:>5.1f}% {report['mean_pnl']:>+6.2f}p "
            f"{report['total_pnl']:>+8.1f}p {n_issues:>7d} {n_dups:>5d}"
        )

    print()
    print(f"Aggregate: {total_issues} structural issues, {total_dups} duplicate-pair occurrences across {len(SYSTEMS)} systems")

    # Detailed findings for flagged systems
    if flagged_systems:
        print()
        print("=" * 78)
        print("  DETAILED FINDINGS — flagged systems")
        print("=" * 78)
        for system in flagged_systems:
            r = audits[system]
            print(f"\n── {system} ──")
            # Structural issues — sample first 5
            if r["issues_per_trade"]:
                print(f"  Structural issues: {len(r['issues_per_trade'])} trades affected (showing up to 5):")
                for t, issues in r["issues_per_trade"][:5]:
                    et = t.get("entry_time", 0)
                    dt_s = datetime.fromtimestamp(et, tz=timezone.utc).strftime("%m-%d %H:%M UTC") if et else "?"
                    pair = t.get("pair", "?"); direction = t.get("direction", "?")
                    print(f"    {dt_s} {pair:<7} {direction:<4}: {' | '.join(issues[:3])}")
                if len(r["issues_per_trade"]) > 5:
                    print(f"    ... and {len(r['issues_per_trade']) - 5} more")
            # Duplicates
            if r["duplicates"]:
                print(f"  Duplicates (same pair+dir within 60s): {len(r['duplicates'])} pairs")
            # Stale open trades
            if r["oldest_open_age_h"] and r["oldest_open_age_h"] > 24:
                print(f"  Oldest open trade age: {r['oldest_open_age_h']:.1f}h ({r['n_open']} open)")
                for t in r["open_trades_sample"]:
                    et = t.get("entry_time", 0)
                    dt_s = datetime.fromtimestamp(et, tz=timezone.utc).strftime("%m-%d %H:%M UTC") if et else "?"
                    pair = t.get("pair", "?"); direction = t.get("direction", "?")
                    print(f"    open: {dt_s} {pair:<7} {direction:<4}")

    print()
    print("=" * 78)
    print(f"  AUDIT COMPLETE — {len(flagged_systems)} systems flagged for detail review")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
