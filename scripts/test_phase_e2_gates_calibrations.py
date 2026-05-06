"""Phase E.2 tests — gate distribution + recent calibrations.

4 tests required by the E.2 review-gate spec:
  1. Gate distribution sorts by count descending; percentages sum ~100%
  2. Strength-engine gate is excluded from gate distribution
     (already covered in Section 1's reject count)
  3. Rolling-10 mean correctness: shows "not yet" at n<10, computes
     correct mean over MOST RECENT 10 entries when n>=10
  4. Drift banner activates at |mean| > 1.5p band, with separate
     pessimistic-vs-dangerous styling

Run from repo root:
    python scripts/test_phase_e2_gates_calibrations.py
"""
from __future__ import annotations

import json
import sys
import tempfile
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

# Console may be cp1252 on Windows; UTF-8-encode stdout so headers
# with box-drawing characters survive the print pipeline.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from PyQt6.QtCore import QObject, pyqtSignal  # noqa: E402
from PyQt6.QtWidgets import QApplication  # noqa: E402

from takumi_trader.ui.shadow_stats_panel import (  # noqa: E402
    ShadowStatsPanel, today_start_utc,
)


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


_APP = QApplication.instance() or QApplication(sys.argv)


class _StubWorker(QObject):
    cycle_complete = pyqtSignal(dict)
    drift_warning = pyqtSignal(str)
    fatal_error = pyqtSignal(str)


def _make_record(*, signal_time, block_gate, status="BLOCKED", pair="EURUSD"):
    return {
        "signal_time": signal_time, "block_gate": block_gate,
        "status": status, "pair": pair, "direction": "BUY",
    }


def _make_calibration(*, written_at, pair="EURUSD", delta=0.0,
                      real_pnl=0.0, sim_pnl=0.0,
                      real_exit="tp_hit", sim_exit="TP",
                      real_min=10.0, sim_min=10):
    return {
        "shadow_id": int(written_at), "strategy_id": "Sv2",
        "pair": pair, "direction": "BUY",
        "signal_time": written_at - 100.0,
        "real_pnl_pips": real_pnl, "sim_pnl_pips": sim_pnl,
        "delta_pips": delta,
        "real_exit_reason": real_exit, "sim_exit_reason": sim_exit,
        "real_duration_minutes": real_min, "sim_duration_minutes": sim_min,
        "pessimism_applied": "wcf+sp",
        "written_at": written_at,
    }


def _write(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")


def _make_panel(td_path: Path, journal_recs, cal_recs):
    journal = td_path / "shadow_trades_Sv2.json"
    cal = td_path / "shadow_calibration_Sv2.json"
    _write(journal, journal_recs)
    _write(cal, cal_recs)
    return ShadowStatsPanel(
        shadow_journal_path=journal, calibration_log_path=cal,
        sim_worker=_StubWorker(), refresh_interval_ms=0,
    )


# ─────────────────────────────────────────────────────────────────────
# Test 1 — gate distribution sorting + percentages
# ─────────────────────────────────────────────────────────────────────

def test_1_gate_distribution_sort_and_pct():
    print("\n[1] gate distribution sorts by count, percentages sum to 100%")
    with tempfile.TemporaryDirectory() as td:
        anchor = today_start_utc()
        recs = []
        # 10 divergence_spread, 4 structural, 2 conviction, 1 h1_sweep
        for i in range(10):
            recs.append(_make_record(
                signal_time=anchor + i, block_gate="divergence_spread",
            ))
        for i in range(4):
            recs.append(_make_record(
                signal_time=anchor + 100 + i, block_gate="structural",
            ))
        for i in range(2):
            recs.append(_make_record(
                signal_time=anchor + 200 + i, block_gate="conviction",
            ))
        recs.append(_make_record(
            signal_time=anchor + 300, block_gate="h1_sweep",
        ))

        panel = _make_panel(Path(td), recs, [])
        body = panel._gate_body.text()

        # Sort order: divergence_spread first (count=10), then structural (4),
        # conviction (2), h1_sweep (1). Verify divergence_spread appears
        # BEFORE structural in the rendered HTML.
        i_div = body.find("divergence_spread")
        i_struct = body.find("structural")
        i_conv = body.find("conviction")
        i_h1 = body.find("h1_sweep")
        if not (0 <= i_div < i_struct < i_conv < i_h1):
            _fail(
                f"sort order wrong: div={i_div} struct={i_struct} "
                f"conv={i_conv} h1={i_h1}"
            )
        _ok("rendered in descending-count order")

        # Each percentage should appear in the body. Total = 17.
        # 10/17=58.8%, 4/17=23.5%, 2/17=11.8%, 1/17=5.9%
        for pct_str in ("58.8", "23.5", "11.8", "5.9"):
            if pct_str not in body:
                _fail(f"expected percentage {pct_str}% missing from body")
        _ok("percentages render correctly (58.8 / 23.5 / 11.8 / 5.9)")


# ─────────────────────────────────────────────────────────────────────
# Test 2 — strength_engine excluded from gate distribution
# ─────────────────────────────────────────────────────────────────────

def test_2_strength_engine_excluded():
    print("\n[2] strength_engine gate is excluded from Section 2")
    with tempfile.TemporaryDirectory() as td:
        anchor = today_start_utc()
        recs = []
        for i in range(50):
            recs.append(_make_record(
                signal_time=anchor + i, block_gate="strength_engine",
            ))
        recs.append(_make_record(
            signal_time=anchor + 100, block_gate="conviction",
        ))

        panel = _make_panel(Path(td), recs, [])
        body = panel._gate_body.text()

        # strength_engine MUST NOT appear in the gate distribution body
        if "strength_engine" in body:
            _fail("strength_engine should be excluded from Section 2")

        # conviction (the only downstream gate) should appear at 100%
        if "conviction" not in body:
            _fail("conviction should appear in Section 2")
        if "100.0%" not in body:
            _fail("expected 100.0% for sole downstream gate")
        _ok("strength_engine excluded; conviction shows 100.0%")


# ─────────────────────────────────────────────────────────────────────
# Test 3 — rolling-10 mean correctness
# ─────────────────────────────────────────────────────────────────────

def test_3_rolling_10_mean():
    print("\n[3] rolling-10 mean: 'not yet' at n<10, correct mean at n>=10")
    with tempfile.TemporaryDirectory() as td:
        # 9 entries → not yet activated
        cals_9 = [
            _make_calibration(written_at=1000.0 + i, delta=1.0)
            for i in range(9)
        ]
        panel = _make_panel(Path(td), [], cals_9)
        body = panel._calibration_body.text()
        if "not yet" not in body.lower():
            _fail(f"n=9 should show 'not yet', got: {body[-200:]!r}")
        if "n=9" not in body:
            _fail(f"n=9 indicator missing: {body[-200:]!r}")
        _ok("n=9 shows 'not yet' message")

        # 12 entries — verify rolling mean = mean of 10 MOST RECENT
        # written_at:        100, 200, ..., 1200 (older to newer)
        # delta:             0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5,
        #                    4.0, 4.5, 5.0, 5.5
        # most-recent-10 (written_at 300..1200): deltas 1.0..5.5 step 0.5
        # mean = (1.0+1.5+2.0+2.5+3.0+3.5+4.0+4.5+5.0+5.5)/10 = 32.5/10 = 3.25
        cals_12 = [
            _make_calibration(written_at=100.0 + 100.0 * i, delta=0.0 + 0.5 * i)
            for i in range(12)
        ]
        # Update the on-disk file so the panel re-reads
        cal_path = Path(td) / "shadow_calibration_Sv2.json"
        time.sleep(1.05)  # ensure mtime advances
        _write(cal_path, cals_12)
        panel.refresh()
        body12 = panel._calibration_body.text()
        if "+3.25p" not in body12:
            _fail(f"rolling-10 mean expected +3.25p in body, got: {body12[-300:]!r}")
        # NOT 'not yet' anymore
        if "not yet" in body12.lower():
            _fail(f"n=12 should not show 'not yet': {body12[-200:]!r}")
        _ok("n=12 rolling-10 mean = +3.25p (most recent 10, oldest 2 dropped)")


# ─────────────────────────────────────────────────────────────────────
# Test 4 — drift banner activates outside ±1.5p band
# ─────────────────────────────────────────────────────────────────────

def test_4_drift_banner_activation():
    print("\n[4] drift banner: pessimistic > +1.5p, dangerous < -1.5p, off in band")
    with tempfile.TemporaryDirectory() as td:
        # Within band (mean ≈ 0.5p) — banner should be reset
        cals_within = [
            _make_calibration(written_at=1000.0 + i, delta=0.5)
            for i in range(10)
        ]
        panel = _make_panel(Path(td), [], cals_within)
        header_within = panel._calibration_header.text()
        if "DRIFT WARNING" in header_within or "Drift band exceeded" in header_within:
            _fail(f"in-band mean should not trigger banner, got: {header_within!r}")
        _ok(f"within band: header='{header_within}'")

        # Pessimistic (mean = +5p > +1.5p band)
        cal_path = Path(td) / "shadow_calibration_Sv2.json"
        time.sleep(1.05)
        cals_pess = [
            _make_calibration(written_at=2000.0 + i, delta=5.0)
            for i in range(10)
        ]
        _write(cal_path, cals_pess)
        panel.refresh()
        header_pess = panel._calibration_header.text()
        if "Drift band exceeded" not in header_pess:
            _fail(f"pessimistic mean should set pessimistic banner: {header_pess!r}")
        _ok(f"pessimistic: header='{header_pess}'")

        # Dangerous (mean = -3p < -1.5p band)
        time.sleep(1.05)
        cals_dang = [
            _make_calibration(written_at=3000.0 + i, delta=-3.0)
            for i in range(10)
        ]
        _write(cal_path, cals_dang)
        panel.refresh()
        header_dang = panel._calibration_header.text()
        if "DRIFT WARNING" not in header_dang:
            _fail(f"dangerous mean should set DRIFT WARNING: {header_dang!r}")
        _ok(f"dangerous: header='{header_dang}'")

        # Body should mention TOO OPTIMISTIC — DANGEROUS
        body_dang = panel._calibration_body.text()
        if "OPTIMISTIC" not in body_dang or "DANGEROUS" not in body_dang:
            _fail(f"dangerous body missing classification: {body_dang[-300:]!r}")
        _ok("dangerous classification in body text")

        # Pessimistic-direction body should NOT commit to a specific
        # tuning lever (slippage vs SL-first) — softened 2026-05-07.
        # The rolling mean alone can't disambiguate; classification
        # must say "investigate" with a decomposition pointer.
        body_pess = panel._calibration_body.text()
        time.sleep(1.05)
        cal_path = Path(td) / "shadow_calibration_Sv2.json"
        _write(cal_path, [
            _make_calibration(written_at=4000.0 + i, delta=5.0)
            for i in range(10)
        ])
        panel.refresh()
        body_pess = panel._calibration_body.text()
        # Must NOT name a specific lever
        for forbidden in ("slippage-tunable", "SL-first dominant"):
            if forbidden in body_pess:
                _fail(
                    f"pessimistic body should not commit to lever {forbidden!r} "
                    f"(softened 2026-05-07): {body_pess[-300:]!r}"
                )
        # Must include "investigate" + decomposition guidance
        if "investigate" not in body_pess or "decompose" not in body_pess:
            _fail(
                f"pessimistic body missing softened guidance: {body_pess[-300:]!r}"
            )
        _ok("softened: no lever-specific guidance, 'investigate' + decomposition pointer present")


# ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 64)
    print("Phase E.2 — Gate distribution + Recent calibrations")
    print("=" * 64)
    test_1_gate_distribution_sort_and_pct()
    test_2_strength_engine_excluded()
    test_3_rolling_10_mean()
    test_4_drift_banner_activation()
    print("\n" + "=" * 64)
    print("ALL E.2 TESTS PASSED")
    print("=" * 64)


if __name__ == "__main__":
    main()
