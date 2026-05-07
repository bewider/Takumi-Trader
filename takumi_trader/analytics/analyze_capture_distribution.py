"""Tier 1 Script 1: shadow capture distribution report.

Read-only analysis of the shadow journal — what's being captured,
how the gate distribution looks, where pair coverage is thin,
which (pair × direction × gate) combinations dominate.

Usage:
    python -m takumi_trader.analytics.analyze_capture_distribution \
        --journal data/shadow_trades_Sv2.json \
        --since 2026-05-05 --until 2026-05-12 \
        --output data/analytics/capture_distribution_2026-05-12.txt

All flags optional; defaults: journal=data/shadow_trades_Sv2.json,
since=24h ago UTC, until=now UTC, output=stdout.
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure imports work whether invoked as `python -m ...` or directly
_REPO = Path(__file__).resolve().parent.parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from takumi_trader.analytics.shadow_loader import (  # noqa: E402
    KNOWN_GATES, ShadowRecord, classify_session, load_shadow_journal, parse_date_arg,
)


def _bar(width: int, max_width: int = 20) -> str:
    """Unicode block bar of given proportional width."""
    full = max(0, min(max_width, width))
    return "█" * full


def _pct(num: int, den: int) -> float:
    return 100.0 * num / den if den else 0.0


def _section_record_breakdown(records: list[ShadowRecord]) -> list[str]:
    total = len(records)
    rejects = [r for r in records if r.is_strength_reject]
    passes = [r for r in records if r.is_strength_pass]
    n_rej, n_pass = len(rejects), len(passes)

    by_status = Counter(r.status for r in passes)
    n_exec = by_status.get("EXECUTED", 0)
    n_blocked = by_status.get("BLOCKED", 0)
    n_pending = by_status.get("PENDING", 0)
    n_failed = by_status.get("FAILED", 0)

    pending_marker = " ⚠" if n_pending > 0 else ""

    lines = [
        "──── 1. RECORD TYPE BREAKDOWN ────",
        f"  Strength-rejects (lightweight): {n_rej:>10,}  ({_pct(n_rej, total):.1f}%)",
        f"  Strength-passes  (full record): {n_pass:>10,}  ({_pct(n_pass, total):.1f}%)",
        "    └─ Status breakdown:",
        f"       EXECUTED (passed all filters): {n_exec:>5,}  ({_pct(n_exec, n_pass):.1f}% of passes)",
        f"       BLOCKED  (filtered downstream): {n_blocked:>5,}  ({_pct(n_blocked, n_pass):.1f}%)",
        f"       PENDING  (orphaned):            {n_pending:>5,}  ({_pct(n_pending, n_pass):.1f}%){pending_marker}",
        f"       FAILED   (sim permanent):       {n_failed:>5,}  ({_pct(n_failed, n_pass):.1f}%)",
    ]
    return lines


def _section_gate_distribution(records: list[ShadowRecord]) -> list[str]:
    blocked = [
        r for r in records
        if r.status == "BLOCKED" and r.block_gate
        and r.block_gate != "strength_engine"
    ]
    counts = Counter(r.block_gate for r in blocked)
    total = sum(counts.values())

    lines = [
        "",
        "──── 2. GATE DISTRIBUTION (BLOCKED downstream) ────",
    ]
    if not counts:
        lines.append("  (no downstream blocks in window)")
        return lines

    max_n = max(counts.values())
    # Render in canonical order — known gates first, then any drift values
    ordered = sorted(counts.items(), key=lambda kv: -kv[1])
    for gate, n in ordered:
        pct = _pct(n, total)
        bar = _bar(int(round(20.0 * n / max_n)))
        marker = "" if gate in KNOWN_GATES else "  ⚠ unknown gate (schema drift?)"
        lines.append(f"  {gate:<22s} {n:>5,}  ({pct:>5.1f}%)  {bar}{marker}")

    lines.append(f"  strength_engine: [N/A — see Section 1, lightweight rejects]")
    return lines


def _section_pair_coverage(records: list[ShadowRecord]) -> list[str]:
    passes = [r for r in records if r.is_strength_pass]
    pairs = sorted({r.pair for r in passes if r.pair})
    if not pairs:
        return [
            "",
            "──── 3. PAIR COVERAGE ────",
            "  (no strength-passes in window)",
        ]

    lines = [
        "",
        "──── 3. PAIR COVERAGE — strength-passes only ────",
        "  Pair      Total   Exec   Filter%   Top reject gate",
        "  " + "─" * 56,
    ]
    rows = []
    for pair in pairs:
        prs = [r for r in passes if r.pair == pair]
        n_total = len(prs)
        n_exec = sum(1 for r in prs if r.status == "EXECUTED")
        n_blk = sum(1 for r in prs if r.status == "BLOCKED")
        filter_pct = _pct(n_blk, n_total)
        gate_counter = Counter(r.block_gate for r in prs if r.status == "BLOCKED")
        top_gate, _ = gate_counter.most_common(1)[0] if gate_counter else ("", 0)
        flag = " ⚠ never executes" if n_total > 0 and n_exec == 0 else ""
        rows.append((n_total, pair, n_exec, filter_pct, top_gate, flag))
    # Sort by total descending so highest-volume pairs lead
    rows.sort(reverse=True)
    for n_total, pair, n_exec, filter_pct, top_gate, flag in rows:
        lines.append(
            f"  {pair:<8s}  {n_total:>5}  {n_exec:>5}  {filter_pct:>6.1f}%   {top_gate}{flag}"
        )

    n_pairs_with_passes = len(pairs)
    n_pairs_no_exec = sum(1 for n_total, _, n_exec, *_ in rows if n_total > 0 and n_exec == 0)
    lines.append(
        f"  Pairs with strength-passes: {n_pairs_with_passes}; "
        f"pairs that never executed: {n_pairs_no_exec}"
    )
    return lines


def _section_top_blocked_combos(records: list[ShadowRecord]) -> list[str]:
    blocked = [
        r for r in records
        if r.status == "BLOCKED" and r.block_gate
        and r.block_gate != "strength_engine"
    ]
    counter = Counter(
        (r.pair, r.direction, r.block_gate) for r in blocked
    )
    if not counter:
        return [
            "",
            "──── 4. TOP BLOCKED (pair x direction x gate) ────",
            "  (no downstream blocks in window)",
        ]

    lines = [
        "",
        "──── 4. TOP 10 MOST-BLOCKED (pair x direction x gate) ────",
        "  Combination                    Count  Most common reason",
        "  " + "─" * 60,
    ]
    for (pair, direction, gate), n in counter.most_common(10):
        # Sample a reason from the most recent record matching this combo
        sample = next(
            (r for r in reversed(blocked)
             if r.pair == pair and r.direction == direction and r.block_gate == gate),
            None,
        )
        reason = sample.block_reason if sample else ""
        # Truncate reason to fit
        reason = reason[:48] + ("..." if len(reason) > 48 else "")
        combo = f"{pair} {direction} {gate}"
        lines.append(f"  {combo:<32s} {n:>4}   {reason}")
    return lines


def _section_session(records: list[ShadowRecord]) -> list[str]:
    passes = [r for r in records if r.is_strength_pass]
    if not passes:
        return [
            "",
            "──── 5. SESSION DISTRIBUTION (strength-passes) ────",
            "  (no strength-passes)",
        ]
    counter = Counter(classify_session(r.signal_time) for r in passes)
    total = sum(counter.values())
    lines = [
        "",
        "──── 5. SESSION DISTRIBUTION (strength-passes) ────",
    ]
    for sess in ("tokyo", "normal", "overlap"):
        n = counter.get(sess, 0)
        lines.append(f"  {sess:<10s} {n:>5}  ({_pct(n, total):.1f}%)")
    return lines


def _section_anomalies(records: list[ShadowRecord]) -> list[str]:
    flags = []
    n_pending = sum(1 for r in records if r.is_strength_pass and r.status == "PENDING")
    if n_pending > 0:
        flags.append(
            f"  ⚠ {n_pending} PENDING strength-pass records "
            "(should be 0 in steady state — check orphan-marker)"
        )
    # Pairs with 100% filter rate
    by_pair = {}
    for r in records:
        if not r.is_strength_pass or not r.pair:
            continue
        by_pair.setdefault(r.pair, [0, 0])
        if r.status == "EXECUTED":
            by_pair[r.pair][0] += 1
        elif r.status == "BLOCKED":
            by_pair[r.pair][1] += 1
    for pair, (n_exec, n_blk) in by_pair.items():
        total = n_exec + n_blk
        if total >= 5 and n_exec == 0:
            flags.append(
                f"  ⚠ {pair}: {n_blk} strength-passes, 0 executions "
                f"(100% filter rate, n>=5)"
            )
    # Schema-drift gate values
    drift_gates = {
        r.block_gate for r in records
        if r.block_gate and r.block_gate not in KNOWN_GATES
    }
    if drift_gates:
        flags.append(
            f"  ⚠ Unknown gate values (schema drift?): {sorted(drift_gates)}"
        )
    # GATE_INTERNAL fires (defensive paths, should be rare)
    n_internal = sum(1 for r in records if r.block_gate == "internal")
    if n_internal > 0:
        flags.append(
            f"  ⚠ {n_internal} GATE_INTERNAL fires (defensive path triggered)"
        )

    lines = [
        "",
        "──── 6. ANOMALIES & FLAGS ────",
    ]
    if not flags:
        lines.append("  ✓ No anomalies detected.")
    else:
        lines.extend(flags)
    return lines


def build_report(
    records: list[ShadowRecord],
    journal_name: str,
    since: datetime,
    until: datetime,
) -> str:
    bar = "=" * 67
    header = [
        bar,
        "  SHADOW CAPTURE DISTRIBUTION REPORT",
        f"  Journal: {journal_name}",
        f"  Window:  {since.strftime('%Y-%m-%d %H:%M UTC')} -> "
        f"{until.strftime('%Y-%m-%d %H:%M UTC')}",
        f"  Total records in window: {len(records):,}",
        bar,
    ]
    sections = [
        *_section_record_breakdown(records),
        *_section_gate_distribution(records),
        *_section_pair_coverage(records),
        *_section_top_blocked_combos(records),
        *_section_session(records),
        *_section_anomalies(records),
        bar,
    ]
    return "\n".join(header + sections)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--journal", default="data/shadow_trades_Sv2.json")
    p.add_argument("--since", default=None,
                   help="UTC start (YYYY-MM-DD or YYYY-MM-DDTHH:MM); default 24h ago")
    p.add_argument("--until", default=None,
                   help="UTC end; default now")
    p.add_argument("--output", default=None,
                   help="Output file path; default stdout")
    args = p.parse_args(argv)

    since = parse_date_arg(args.since) if args.since else (
        datetime.now(timezone.utc) - timedelta(days=1)
    )
    until = parse_date_arg(args.until) if args.until else datetime.now(timezone.utc)

    journal_path = Path(args.journal)
    records = load_shadow_journal(journal_path, since=since, until=until)
    report = build_report(records, journal_path.name, since, until)

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(report + "\n", encoding="utf-8")
        print(f"Report written: {out}")
    else:
        # Use UTF-8 stream for box-drawing chars on Windows consoles
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
        print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
