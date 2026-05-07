"""Tier 1 Script 3: filter value-add counterfactual analysis.

For each downstream filter gate, compute "what would today's expectancy
have been WITHOUT this gate firing." Tells the operator which filters
are net-positive (saving losses) vs net-negative (destroying profit).

Limitations (mandatory caveats embedded in the output):
* Counterfactual relies on simulated PnL — biased by pessimism config.
* Net-effect verdicts are directional, NOT statistically significant.
* If calibration delta shows sim-too-pessimistic, "filter helps"
  verdicts on marginal cases are biased toward "helps" — read
  alongside analyze_calibration_trend output.

Usage:
    python -m takumi_trader.analytics.analyze_filter_value_add \
        --journal data/shadow_trades_Sv2.json \
        --since 2026-05-05 --until 2026-05-12 \
        --output data/analytics/filter_value_add_2026-05-12.txt
"""
from __future__ import annotations

import argparse
import statistics
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from takumi_trader.analytics.shadow_loader import (  # noqa: E402
    KNOWN_GATES, ShadowRecord, format_pip, load_shadow_journal, parse_date_arg,
)


def _section_baseline(records: list[ShadowRecord]) -> tuple[list[str], dict]:
    """Real expectancy from EXECUTED trades whose paper trade closed.

    Reads sim_pnl_pips from EXECUTED records as a proxy when real_pnl
    is not available in the journal. This script does NOT load the
    paper_trades.json; the calibration trend script is the authority
    for real-vs-sim comparisons. Here we only need a baseline for
    "what's the executed shadow expectancy this window."
    """
    executed = [r for r in records if r.status == "EXECUTED" and r.sim_completed]
    if not executed:
        return [
            "──── 1. EXECUTED-TRADE BASELINE (sim PnL on executed trades) ────",
            "  No executed trades in window with completed simulation.",
        ], {"executed_n": 0, "exec_mean": None}
    pnls = [float(r.sim_pnl_pips) for r in executed]
    mean = statistics.mean(pnls)
    total = sum(pnls)
    wins = sum(1 for p in pnls if p > 0)
    win_rate = 100.0 * wins / len(pnls)

    lines = [
        "──── 1. EXECUTED-TRADE BASELINE (sim PnL on executed trades) ────",
        f"  Executed trades:   {len(executed)}",
        f"  Sim expectancy:    {format_pip(mean)} / trade",
        f"  Sim total P&L:     {format_pip(total)}",
        f"  Sim win rate:      {win_rate:.0f}% ({wins}/{len(executed)})",
        "  (Sim PnL — for real comparison, see analyze_calibration_trend output)",
    ]
    return lines, {"executed_n": len(executed), "exec_mean": mean}


def _per_gate_expectancy(records: list[ShadowRecord]) -> dict:
    """For each gate, compute the mean sim_pnl of records that gate
    blocked. Includes only records with sim_completed=True and a
    sim_exit_reason in {TP, SL, TIMEOUT} (FAILED sims excluded)."""
    blocked = [
        r for r in records
        if r.status == "BLOCKED" and r.block_gate
        and r.block_gate != "strength_engine"
        and r.sim_completed
        and r.sim_exit_reason in ("TP", "SL", "TIMEOUT")
    ]
    by_gate: dict[str, list[ShadowRecord]] = {}
    for r in blocked:
        by_gate.setdefault(r.block_gate, []).append(r)
    return by_gate


def _verdict(mean: float) -> tuple[str, str]:
    """Return (verdict_text, color_marker)."""
    if mean < -0.5:
        return ("✓ filter helps (saves losses)", "✓")
    if mean > 0.5:
        return ("✗ filter hurts (destroys profit)", "✗")
    return ("≈ neutral", "≈")


def _section_per_gate(records: list[ShadowRecord]) -> tuple[list[str], list[dict]]:
    by_gate = _per_gate_expectancy(records)

    lines = [
        "",
        "──── 2. GATE-BY-GATE COUNTERFACTUAL ────",
        "  'What if this gate had let everything through?' — uses sim PnL.",
        "",
        "  Gate                  Blocked   Sim Exp     Net Effect",
        "  " + "─" * 60,
    ]
    flagged: list[dict] = []
    # Sort by absolute net-effect magnitude descending for visual scan
    rows = []
    for gate in sorted(by_gate, key=lambda g: -abs(_mean_safe(by_gate[g]))):
        recs = by_gate[gate]
        n = len(recs)
        mean = _mean_safe(recs)
        verdict, _ = _verdict(mean)
        net_pip = mean * n  # if filter was disabled, this is the PnL impact
        rows.append((gate, n, mean, net_pip, verdict))
    # Append zero-fire gates from the known set
    fired_gates = set(by_gate.keys())
    for gate in sorted(KNOWN_GATES - fired_gates - {"strength_engine"}):
        rows.append((gate, 0, None, 0.0, "no fires this window"))
    for gate, n, mean, net_pip, verdict in rows:
        if mean is None:
            lines.append(f"  {gate:<22s} {n:>5}        —            {verdict}")
        else:
            lines.append(
                f"  {gate:<22s} {n:>5}   {format_pip(mean):>8s}   "
                f"{verdict}  (cum {format_pip(net_pip)})"
            )
        if mean is not None and mean > 0.5:
            flagged.append({
                "gate": gate, "n": n, "mean": mean,
                "if_disabled_pnl_delta": net_pip,
            })

    # Net contribution
    total_blocks = sum(n for _, n, mean, *_ in rows if mean is not None)
    total_pnl_saved_or_destroyed = sum(
        -mean * n  # saved if mean negative (avoided losses); destroyed if positive
        for _, n, mean, *_ in rows
        if mean is not None
    )
    if total_blocks > 0:
        per_block = total_pnl_saved_or_destroyed / total_blocks
        verb = "saved" if total_pnl_saved_or_destroyed >= 0 else "destroyed"
        lines.append("  " + "─" * 60)
        lines.append(
            f"  Net filter contribution: {format_pip(total_pnl_saved_or_destroyed)} "
            f"{verb} across {total_blocks} blocks ({format_pip(per_block, sign=True)}/blocked)"
        )
    return lines, flagged


def _mean_safe(records: list[ShadowRecord]) -> float | None:
    if not records:
        return None
    return statistics.mean(float(r.sim_pnl_pips) for r in records)


def _section_flagged(
    records: list[ShadowRecord],
    flagged: list[dict],
) -> list[str]:
    if not flagged:
        return [
            "",
            "──── 3. FLAGGED FILTERS (potentially net-negative) ────",
            "  ✓ No filters appear net-negative in this window.",
        ]

    lines = [
        "",
        "──── 3. FLAGGED FILTERS (potentially net-negative) ────",
    ]
    for f in flagged:
        gate = f["gate"]
        n = f["n"]
        mean = f["mean"]
        delta = f["if_disabled_pnl_delta"]
        lines.append("")
        lines.append(
            f"  ⚠ {gate}: blocked {n} trades, avg sim PnL = {format_pip(mean)}"
        )
        lines.append(
            f"    If gate disabled, total P&L would change by {format_pip(delta)}"
        )
        # Sample 3-5 records for spot-check
        sample_recs = [
            r for r in records
            if r.block_gate == gate and r.sim_completed
            and r.sim_exit_reason in ("TP", "SL", "TIMEOUT")
            and float(r.sim_pnl_pips) > 0
        ][:5]
        if sample_recs:
            lines.append("    Sample of profitable blocks (would have been wins):")
            for r in sample_recs:
                ts = r.signal_dt.strftime("%Y-%m-%d %H:%M UTC")
                lines.append(
                    f"        {r.pair} {r.direction} {ts}  "
                    f"sim {format_pip(float(r.sim_pnl_pips))}"
                )
        lines.append(f"    -> RECOMMENDATION: investigate {gate} specifically")
    return lines


def _section_caveats(window_days: float) -> list[str]:
    return [
        "",
        "──── 4. CONFIDENCE WARNINGS ────",
        "  ⚠ Sim outcomes use pessimistic config — real expectancy may differ.",
        "    Read alongside analyze_calibration_trend output for delta context.",
        f"  ⚠ Window is {window_days:.1f} days — below most statistical thresholds.",
        "  ⚠ Net-effect verdicts are directional, NOT statistically significant.",
        "  ⚠ Per-gate sample sizes vary; small-n verdicts (n<30) are weakest.",
        "  ⚠ If calibration shows sim too-pessimistic (positive delta mean), ",
        "    'filter helps' verdicts on borderline cases are biased toward 'helps'.",
    ]


def build_report(
    records: list[ShadowRecord],
    journal_name: str,
    since: datetime,
    until: datetime,
) -> str:
    bar = "=" * 67
    window_days = (until - since).total_seconds() / 86400.0
    header = [
        bar,
        "  FILTER VALUE-ADD ANALYSIS",
        f"  Journal: {journal_name}",
        f"  Window:  {since.strftime('%Y-%m-%d %H:%M UTC')} -> "
        f"{until.strftime('%Y-%m-%d %H:%M UTC')}  ({window_days:.1f} days)",
        bar,
    ]
    baseline_lines, baseline_stats = _section_baseline(records)
    per_gate_lines, flagged = _section_per_gate(records)
    flagged_lines = _section_flagged(records, flagged)
    caveats = _section_caveats(window_days)
    return "\n".join(
        header + baseline_lines + per_gate_lines + flagged_lines + caveats + [bar]
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--journal", default="data/shadow_trades_Sv2.json")
    p.add_argument("--since", default=None)
    p.add_argument("--until", default=None)
    p.add_argument("--output", default=None)
    args = p.parse_args(argv)

    since = parse_date_arg(args.since) if args.since else (
        datetime.now(timezone.utc) - timedelta(days=7)
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
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
        print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
