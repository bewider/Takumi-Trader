"""Tier 1 Script 2: calibration trend analysis.

Read-only analysis of the calibration log — rolling deltas, decomposition
by exit pattern / duration / pair category, and heuristic tuning guidance.

Tuning guidance is direction-only (per the Phase E classification softening):
the script scores three hypotheses (constant offset / SL-first / slippage)
based on observed pattern characteristics and labels them STRONG / MEDIUM
/ WEAK rather than confidently prescribing a tuning lever.

Usage:
    python -m takumi_trader.analytics.analyze_calibration_trend \
        --calibration data/shadow_calibration_Sv2.json \
        --output data/analytics/calibration_trend_2026-05-12.txt
"""
from __future__ import annotations

import argparse
import statistics
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from takumi_trader.analytics.shadow_loader import (  # noqa: E402
    CalibrationRecord, categorize_pair, format_pip, load_calibration_log, parse_date_arg,
)


def _dedupe_calibrations(cals: list[CalibrationRecord]) -> tuple[list[CalibrationRecord], int]:
    """Defensive read-time dedupe by (shadow_id, signal_time).

    The persistence-timing bug (project_persistence_timing_bug.md) can
    produce duplicate calibration entries when shutdown hits between
    cal_log.append() and the delayed mark_calibration_completed flush.
    Reset-script dedupe (--dedupe-cal) handles this on disk; this
    function adds belt-and-suspenders at read-time so analytics stay
    correct even if the disk cleanup hasn't run yet.

    Keeps the FIRST occurrence by written_at ascending (oldest write).
    Returns (deduped_list, n_duplicates_removed).
    """
    sorted_cals = sorted(cals, key=lambda c: float(c.written_at))
    seen: set[tuple] = set()
    out: list[CalibrationRecord] = []
    n_dup = 0
    for c in sorted_cals:
        key = (int(c.shadow_id), float(c.signal_time))
        if key in seen:
            n_dup += 1
            continue
        seen.add(key)
        out.append(c)
    return out, n_dup


def _short_exit(reason: str) -> str:
    m = {
        "tp_hit": "TP", "sl_hit": "SL",
        "TP": "TP", "SL": "SL", "TIMEOUT": "TO",
        "FAILED": "FX", "signal_exit": "SX", "weekend_close": "WC",
    }
    return m.get(reason, (reason[:4] or "?"))


def _section_rolling_deltas(cals: list[CalibrationRecord]) -> list[str]:
    if not cals:
        return [
            "──── 1. ROLLING DELTAS ────",
            "  (no calibration data yet)",
        ]
    # Sort newest first for "last N" window calculations
    by_recency = sorted(cals, key=lambda r: r.written_at, reverse=True)
    deltas = [float(r.delta_pips) for r in by_recency]
    n = len(deltas)

    def _mean_of(k: int) -> float | None:
        if n < k:
            return None
        return statistics.mean(deltas[:k])

    latest = deltas[0]
    last3 = _mean_of(3)
    last5 = _mean_of(5)
    last10 = _mean_of(10)
    overall = statistics.mean(deltas)
    stdev = statistics.stdev(deltas) if n >= 2 else 0.0
    max_d = max(deltas)
    min_d = min(deltas)
    n_neg = sum(1 for d in deltas if d < 0)

    drift_status = (
        "(insufficient data — need n>=10 to activate drift detector)"
        if last10 is None
        else (
            "WITHIN BAND ✓"
            if abs(last10) <= 1.5
            else (
                "TOO OPTIMISTIC — DANGEROUS ⚠"
                if last10 < -1.5
                else "OVER-PESSIMISTIC — investigate via decomposition below"
            )
        )
    )

    lines = [
        "──── 1. ROLLING DELTAS ────",
        f"  Latest:        {format_pip(latest)}",
        f"  Last 3 mean:   {format_pip(last3)}",
        f"  Last 5 mean:   {format_pip(last5)}",
        f"  Last 10 mean:  {format_pip(last10)}",
        f"  All time:      {format_pip(overall)}  (n={n})",
        f"  Std dev:       ±{stdev:.2f}p",
        f"  Max:           {format_pip(max_d)}",
        f"  Min:           {format_pip(min_d)}",
        f"  Negative:      {n_neg}  "
        f"({'DANGEROUS direction!' if n_neg else 'no optimistic-side entries'})",
        "",
        "  Drift band:    ±1.5p around zero",
        f"  Status:        {drift_status}",
    ]
    return lines


def _section_exit_patterns(cals: list[CalibrationRecord]) -> list[str]:
    if not cals:
        return ["", "──── 2. DELTA BY EXIT-PATTERN ────", "  (no data)"]
    pattern_groups: dict[tuple[str, str], list[float]] = {}
    for r in cals:
        key = (_short_exit(r.real_exit_reason), _short_exit(r.sim_exit_reason))
        pattern_groups.setdefault(key, []).append(float(r.delta_pips))

    lines = [
        "",
        "──── 2. DELTA BY EXIT-PATTERN ────",
        "  real -> sim   Count   Mean Δ     Notes",
        "  " + "─" * 56,
    ]
    notes_map = {
        ("TP", "TP"): "sim correctly predicted win",
        ("TP", "SL"): "⚠ SL-first disambiguation flipped a winner",
        ("SL", "SL"): "sim correctly predicted loss",
        ("SL", "TP"): "⚠ DANGEROUS — sim too optimistic on a real loser",
    }
    total = sum(len(v) for v in pattern_groups.values())
    for (re_, se_), deltas in sorted(
        pattern_groups.items(), key=lambda kv: -len(kv[1])
    ):
        n = len(deltas)
        mean = statistics.mean(deltas)
        note = notes_map.get((re_, se_), "")
        lines.append(
            f"  {re_:<4s} -> {se_:<4s} {n:>5}  {format_pip(mean):>8s}    {note}"
        )

    # Highlight TP→SL dominance (SL-first hypothesis support)
    tp_sl = len(pattern_groups.get(("TP", "SL"), []))
    if total > 0 and tp_sl / total > 0.4:
        lines.append(
            f"  TP->SL dominant ({100*tp_sl/total:.0f}%): SL-first hypothesis SUPPORTED"
        )
    return lines


def _section_duration_buckets(cals: list[CalibrationRecord]) -> list[str]:
    if not cals:
        return ["", "──── 3. DELTA BY DURATION BUCKET ────", "  (no data)"]
    buckets: dict[str, list[float]] = {
        "< 30 min":     [],
        "30-90 min":    [],
        "90-180 min":   [],
        "> 180 min":    [],
    }
    for r in cals:
        d = float(r.real_duration_minutes)
        if d < 30:
            buckets["< 30 min"].append(float(r.delta_pips))
        elif d < 90:
            buckets["30-90 min"].append(float(r.delta_pips))
        elif d < 180:
            buckets["90-180 min"].append(float(r.delta_pips))
        else:
            buckets["> 180 min"].append(float(r.delta_pips))

    lines = [
        "",
        "──── 3. DELTA BY DURATION BUCKET ────",
        "  Duration       Count   Mean Δ",
        "  " + "─" * 36,
    ]
    means = []
    for label, deltas in buckets.items():
        if deltas:
            mean = statistics.mean(deltas)
            means.append(mean)
            lines.append(f"  {label:<13s}  {len(deltas):>5}   {format_pip(mean):>8s}")
        else:
            lines.append(f"  {label:<13s}  {0:>5}   (none)")

    # Are means consistent across buckets (constant offset) or do they
    # diverge (trade-shape dependent)?
    if len(means) >= 2:
        spread = max(means) - min(means)
        if spread < 1.5:
            lines.append("  Spread across buckets is small (<1.5p) — constant-offset behavior")
        else:
            lines.append(
                f"  Spread across buckets is {spread:.1f}p — "
                "trade-shape dependent (likely SL-first lever)"
            )
    return lines


def _section_pair_categories(cals: list[CalibrationRecord]) -> list[str]:
    if not cals:
        return ["", "──── 4. DELTA BY PAIR CATEGORY ────", "  (no data)"]
    cats: dict[str, list[float]] = {}
    for r in cals:
        cat = categorize_pair(r.pair)
        cats.setdefault(cat, []).append(float(r.delta_pips))

    lines = [
        "",
        "──── 4. DELTA BY PAIR CATEGORY ────",
        "  Category         Count   Mean Δ",
        "  " + "─" * 36,
    ]
    means = []
    for cat in ("USD majors", "JPY crosses", "Wide crosses", "Gold", "Other"):
        if cat in cats:
            mean = statistics.mean(cats[cat])
            means.append(mean)
            lines.append(
                f"  {cat:<15s}  {len(cats[cat]):>5}   {format_pip(mean):>8s}"
            )
    if len(means) >= 2:
        spread = max(means) - min(means)
        if spread > 3.0:
            lines.append(
                f"  Pair-category spread is {spread:.1f}p — "
                "category-specific bias (tight-stop pair effect plausible)"
            )
    return lines


def _section_pessimism_configs(cals: list[CalibrationRecord]) -> list[str]:
    counter = Counter(r.pessimism_applied for r in cals)
    if not counter:
        return ["", "──── 5. PESSIMISM CONFIG VARIANTS ────", "  (no data)"]
    lines = [
        "",
        "──── 5. PESSIMISM CONFIG VARIANTS ────",
    ]
    for cfg, n in counter.most_common():
        cfg_short = cfg if len(cfg) < 80 else cfg[:77] + "..."
        lines.append(f"  ({n:>3} records) {cfg_short}")
    if len(counter) > 1:
        lines.append(
            "  ⚠ Multiple pessimism configs in the log — historical re-runs?"
        )
    return lines


def _section_tuning_guidance(cals: list[CalibrationRecord]) -> list[str]:
    """Heuristic, decomposition-based hypothesis scoring.

    NOT a confident lever prescription. Each hypothesis labeled
    STRONG / MEDIUM / WEAK based on observed pattern characteristics."""
    if len(cals) < 3:
        return [
            "",
            "──── 6. TUNING GUIDANCE (decomposition-based) ────",
            f"  (insufficient data: n={len(cals)}, need >=3 for hypothesis scoring)",
        ]

    deltas = [float(r.delta_pips) for r in cals]
    overall_mean = statistics.mean(deltas)
    overall_stdev = statistics.stdev(deltas) if len(deltas) >= 2 else 0.0

    # Hypothesis 1: constant offset (slippage). Evidence: low std
    # relative to mean, similar means across pair categories.
    cv = overall_stdev / abs(overall_mean) if abs(overall_mean) > 0.1 else 999
    h1_strong = cv < 0.5 and abs(overall_mean) > 1.5
    h1_weak = cv > 1.5 or abs(overall_mean) <= 1.5

    # Hypothesis 2: SL-first dominance. Evidence: high TP→SL fraction +
    # short trades show larger delta than long trades.
    tp_sl = sum(
        1 for r in cals
        if _short_exit(r.real_exit_reason) == "TP" and _short_exit(r.sim_exit_reason) == "SL"
    )
    h2_tp_sl_pct = tp_sl / len(cals)
    short_deltas = [float(r.delta_pips) for r in cals if r.real_duration_minutes < 30]
    long_deltas = [float(r.delta_pips) for r in cals if r.real_duration_minutes >= 90]
    short_mean = statistics.mean(short_deltas) if short_deltas else None
    long_mean = statistics.mean(long_deltas) if long_deltas else None
    duration_diverges = (
        short_mean is not None and long_mean is not None
        and (short_mean - long_mean) > 3.0
    )
    h2_strong = h2_tp_sl_pct > 0.5 and duration_diverges
    h2_medium = h2_tp_sl_pct > 0.3 or duration_diverges
    h2_weak = h2_tp_sl_pct < 0.2 and not duration_diverges

    # Hypothesis 3: pair-shape dependent (JPY tight-stop concentration)
    jpy_cals = [r for r in cals if categorize_pair(r.pair) == "JPY crosses"]
    nonjpy_cals = [r for r in cals if categorize_pair(r.pair) != "JPY crosses"]
    jpy_mean = statistics.mean(float(r.delta_pips) for r in jpy_cals) if jpy_cals else None
    nonjpy_mean = statistics.mean(float(r.delta_pips) for r in nonjpy_cals) if nonjpy_cals else None
    h3_strong = (
        jpy_mean is not None and nonjpy_mean is not None
        and (jpy_mean - nonjpy_mean) > 4.0
    )

    def _label(strong: bool, weak: bool, medium: bool = False) -> str:
        if strong:
            return "STRONG"
        if weak:
            return "WEAK"
        if medium:
            return "MEDIUM"
        return "MEDIUM"

    h1_label = _label(h1_strong, h1_weak)
    h2_label = _label(h2_strong, h2_weak, h2_medium)
    h3_label = "STRONG" if h3_strong else "WEAK or insufficient sample"

    lines = [
        "",
        "──── 6. TUNING GUIDANCE (decomposition-based) ────",
        "  Hypothesis scoring (heuristic, NOT confident prescription):",
        "",
        f"  H1: Constant offset (slippage values)              : {h1_label}",
        f"      cv={cv:.2f} — low cv + nonzero mean = constant offset",
        f"  H2: SL-first dominance (architectural lever)       : {h2_label}",
        f"      TP->SL fraction={100*h2_tp_sl_pct:.0f}%  duration-diverges={duration_diverges}",
        f"  H3: Pair-shape dependent (JPY tight-stop effect)   : {h3_label}",
    ]
    if jpy_mean is not None and nonjpy_mean is not None:
        lines.append(
            f"      JPY mean Δ {format_pip(jpy_mean)}  vs non-JPY mean Δ "
            f"{format_pip(nonjpy_mean)}  (diff {format_pip(jpy_mean - nonjpy_mean)})"
        )
    lines.extend([
        "",
        "  Note: lever choice (slippage vs SL-first) requires n>=10 minimum",
        "        plus per-bucket statistical confidence; this section is ",
        "        directional only.",
    ])
    return lines


def _section_milestones(cals: list[CalibrationRecord]) -> list[str]:
    n = len(cals)
    needed_10 = max(0, 10 - n)
    needed_50 = max(0, 50 - n)
    lines = [
        "",
        "──── 7. NEXT MILESTONE ────",
        f"  Current n = {n}",
        f"  At n=10: rolling-10 mean activates drift detector ({needed_10} more needed)",
        f"  At n=50: per-bucket means have statistical resolution ({needed_50} more needed)",
    ]
    return lines


def build_report(cals: list[CalibrationRecord], log_name: str) -> str:
    bar = "=" * 67
    # Defensive read-time dedupe — see _dedupe_calibrations docstring.
    cals, n_dup = _dedupe_calibrations(cals)
    if cals:
        first_dt = cals[0].signal_dt
        last_dt = cals[-1].signal_dt
        window = f"{first_dt.date().isoformat()} -> {last_dt.date().isoformat()}"
    else:
        window = "(no entries)"
    header = [
        bar,
        "  CALIBRATION TREND ANALYSIS",
        f"  Log:           {log_name}",
        f"  Total entries: {len(cals)}",
        f"  Window:        {window}",
    ]
    if n_dup > 0:
        header.append(
            f"  WARNING:       {n_dup} duplicate cal entries deduped at read-time."
        )
        header.append(
            f"                 Run reset_stale_levels_records.py --dedupe-cal to fix on disk."
        )
    header.append(bar)
    sections = [
        *_section_rolling_deltas(cals),
        *_section_exit_patterns(cals),
        *_section_duration_buckets(cals),
        *_section_pair_categories(cals),
        *_section_pessimism_configs(cals),
        *_section_tuning_guidance(cals),
        *_section_milestones(cals),
        bar,
    ]
    return "\n".join(header + sections)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--calibration", default="data/shadow_calibration_Sv2.json")
    p.add_argument("--since", default=None,
                   help="UTC start; default no filter (all calibrations)")
    p.add_argument("--until", default=None)
    p.add_argument("--output", default=None)
    args = p.parse_args(argv)

    since = parse_date_arg(args.since) if args.since else None
    until = parse_date_arg(args.until) if args.until else None

    cal_path = Path(args.calibration)
    cals = load_calibration_log(cal_path, since=since, until=until)
    report = build_report(cals, cal_path.name)

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
