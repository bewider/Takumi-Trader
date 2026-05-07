"""Phase C.2 fixture tests — pessimistic fill simulation core.

6 fixtures per the C.2 spec:
  1. Clean TP hit (BUY)
  2. Clean SL hit (SELL)
  3. Ambiguous candle (high+low both touch SL and TP) — verifies SL-first rule
  4. Timeout (no SL/TP within max_hold)
  5. FAILED (no M1 data)
  6. Session-spread asymmetry (Tokyo trade pays more friction than NY-overlap)

Plus one bonus dry-run test that takes a real strength-pass record from
yesterday's journal and runs simulate() on it. The output is dumped for
review-gate eyeballing — it confirms the algorithm produces sensible
numbers on real data shapes, not just synthetic fixtures.

All tests use stub MT5 (deterministic) so they don't depend on a live
trading connection.

Run from repo root:
    python scripts/test_phase_c2_simulator.py
"""
from __future__ import annotations

import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from takumi_trader.core.broker_spread_model import BrokerSpreadModel  # noqa: E402
from takumi_trader.core.m1_cache import M1Cache  # noqa: E402
from takumi_trader.core.shadow_logger import (  # noqa: E402
    ShadowSignalRecord,
    GATE_STRENGTH_ENGINE,
)
from takumi_trader.core.shadow_simulator import (  # noqa: E402
    ShadowSimulator, ShadowSimulatorConfig, SimulatedOutcome,
)


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


# ── Synthetic-bar helpers ───────────────────────────────────────────

_M1_DTYPE = np.dtype([
    ("time", "i8"),
    ("open", "f8"),
    ("high", "f8"),
    ("low", "f8"),
    ("close", "f8"),
])


def _make_bars(start_dt: datetime, ohlc_seq: list[tuple]) -> np.ndarray:
    """Construct M1 bars from a list of (open, high, low, close) tuples.

    Each bar is one minute apart starting at start_dt. Time is epoch seconds.
    """
    arr = np.empty(len(ohlc_seq), dtype=_M1_DTYPE)
    base = int(start_dt.timestamp())
    for i, (o, h, l, c) in enumerate(ohlc_seq):
        arr[i] = (base + i * 60, o, h, l, c)
    return arr


class FakeMT5:
    """Stub MT5 that returns pre-built bars from a generator function."""
    TIMEFRAME_M1 = 1
    def __init__(self, generator):
        self._gen = generator
    def copy_rates_range(self, pair, tf, start_dt, end_dt):
        return self._gen(pair, start_dt, end_dt)


def _build_simulator(bars_generator) -> ShadowSimulator:
    """Spin up a sim with stub MT5, real cache, real spread model."""
    td = tempfile.mkdtemp()
    cfg = ShadowSimulatorConfig()
    cache = M1Cache(Path(td), mt5_module=FakeMT5(bars_generator))
    spread_model = BrokerSpreadModel(cfg)
    return ShadowSimulator(
        m1_cache=cache, spread_model=spread_model,
        feature_engine=None, config=cfg,
    )


def _make_record(
    pair: str = "EURUSD",
    direction: str = "BUY",
    signal_time: float | None = None,
    entry: float = 1.0500,
    sl: float = 1.0480,
    tp: float = 1.0540,
) -> ShadowSignalRecord:
    """Build a minimal strength-pass record for sim input."""
    if signal_time is None:
        # Use a past time well outside the recency-guard window
        signal_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp()
    return ShadowSignalRecord(
        shadow_id=1, strategy_id="Sv2",
        signal_time=signal_time,
        pair=pair, direction=direction,
        proposed_entry=entry,
        proposed_sl_price=sl, proposed_tp_price=tp,
        proposed_sl_pips=20.0, proposed_tp_pips=40.0,
        input_snapshot_json='{"composite_scores": {}}',  # nonempty so guard doesn't trip
    )


# ─────────────────────────────────────────────────────────────────────
# Fixture tests
# ─────────────────────────────────────────────────────────────────────

def test_1_clean_tp_hit_buy():
    """BUY hits TP cleanly on bar 3, no SL touched along the way."""
    print("\n[1] Clean TP hit (BUY)")
    signal_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)

    # Build 5 bars: entry candle, two neutral, TP hit on bar 3, two more
    def gen(pair, s, e):
        return _make_bars(signal_time, [
            (1.0498, 1.0502, 1.0497, 1.0500),  # entry candle (idx 0)
            (1.0500, 1.0510, 1.0498, 1.0508),  # bar 1
            (1.0508, 1.0530, 1.0505, 1.0528),  # bar 2
            (1.0528, 1.0541, 1.0525, 1.0540),  # bar 3 — TP hit at 1.0540
            (1.0540, 1.0545, 1.0535, 1.0542),  # bar 4 (shouldn't matter)
        ])

    sim = _build_simulator(gen)
    rec = _make_record(direction="BUY", signal_time=signal_time.timestamp(),
                       entry=1.0500, sl=1.0480, tp=1.0540)
    out = sim.simulate(rec)

    if out.sim_exit_reason != "TP":
        _fail(f"exit_reason: {out.sim_exit_reason}")
    if abs(out.sim_exit_price - 1.0540) > 1e-9:
        _fail(f"exit_price: {out.sim_exit_price}")
    # sim_entry should be > 1.0500 (worst-case high + spread + slippage)
    # bar 0 high = 1.0502, EURUSD overlap spread = 0.05pt = 0.0000050,
    # forex slippage = 0.3pt = 0.0000300. So sim_entry ≈ 1.0502 + 0.0000350 ≈ 1.0502350
    # pnl_pips = (1.0540 - 1.0502350) / 0.0001 ≈ 37.65
    if out.sim_pnl_pips <= 0:
        _fail(f"pnl_pips should be positive (TP hit): {out.sim_pnl_pips}")
    if out.sim_pnl_pips > 40.0:
        _fail(f"pnl_pips exceeded 40 (proposed TP), got {out.sim_pnl_pips} — pessimism not applied")
    if "wcf" not in out.sim_pessimism_applied or "sp" not in out.sim_pessimism_applied:
        _fail(f"pessimism_applied missing wcf+sp: {out.sim_pessimism_applied}")
    _ok(f"TP exit at 1.0540, sim_pnl=+{out.sim_pnl_pips}p (< 40 due to pessimism)")


def test_2_clean_sl_hit_sell():
    """SELL hits SL — price walks up to SL on bar 2."""
    print("\n[2] Clean SL hit (SELL)")
    signal_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)

    def gen(pair, s, e):
        return _make_bars(signal_time, [
            (1.0500, 1.0502, 1.0498, 1.0500),  # entry candle
            (1.0500, 1.0510, 1.0500, 1.0508),  # walking up
            (1.0508, 1.0521, 1.0507, 1.0518),  # bar 2 — high 1.0521 hits SL 1.0520
            (1.0518, 1.0525, 1.0515, 1.0520),  # shouldn't matter
        ])

    sim = _build_simulator(gen)
    # SELL with SL above entry, TP below
    rec = _make_record(direction="SELL", signal_time=signal_time.timestamp(),
                       entry=1.0500, sl=1.0520, tp=1.0460)
    out = sim.simulate(rec)

    if out.sim_exit_reason != "SL":
        _fail(f"exit_reason: {out.sim_exit_reason}")
    # SELL SL exit: sim_sl + sl_slippage. sl_slip_forex = 0.5pt = 0.000050
    # exit_price ≈ 1.0520 + 0.000050 = 1.0520500
    expected_exit = 1.0520 + 0.5 * 0.0001
    if abs(out.sim_exit_price - expected_exit) > 1e-9:
        _fail(f"exit_price: {out.sim_exit_price}, expected ~{expected_exit}")
    if out.sim_pnl_pips >= 0:
        _fail(f"pnl_pips should be negative (SL on SELL): {out.sim_pnl_pips}")
    _ok(f"SL exit at {out.sim_exit_price:.5f} (with +0.5pt slip), pnl={out.sim_pnl_pips}p")


def test_3_ambiguous_candle_sl_first():
    """Single bar's high+low both touch SL and TP — config.ambiguous_candle_assume_sl_first
    determines outcome. Default True -> SL wins.
    """
    print("\n[3] Ambiguous candle — SL fires first (default config)")
    signal_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)

    def gen(pair, s, e):
        return _make_bars(signal_time, [
            (1.0500, 1.0502, 1.0498, 1.0500),  # entry candle (calm)
            (1.0500, 1.0501, 1.0499, 1.0500),  # bar 1 (waiting)
            # Bar 2: explosive bar — high 1.0512 (above TP 1.0510),
            # low 1.0488 (below SL 1.0490). Both fire same minute.
            (1.0500, 1.0512, 1.0488, 1.0501),
            (1.0501, 1.0510, 1.0498, 1.0506),  # shouldn't matter
        ])

    sim = _build_simulator(gen)
    rec = _make_record(direction="BUY", signal_time=signal_time.timestamp(),
                       entry=1.0500, sl=1.0490, tp=1.0510)
    out = sim.simulate(rec)

    if out.sim_exit_reason != "SL":
        _fail(
            f"ambiguous candle did NOT fire SL-first as configured: "
            f"exit_reason={out.sim_exit_reason}"
        )
    # SL exit price for BUY: sim_sl - sl_slip = 1.0490 - 0.5pt
    expected = 1.0490 - 0.5 * 0.0001
    if abs(out.sim_exit_price - expected) > 1e-9:
        _fail(f"exit_price: {out.sim_exit_price}, expected ~{expected}")
    _ok(f"ambiguous bar -> SL exit at {out.sim_exit_price:.5f} (pessimism rule fired)")

    # Bonus: same fixture with config.ambiguous_candle_assume_sl_first = False
    cfg2 = ShadowSimulatorConfig(ambiguous_candle_assume_sl_first=False)
    cache2 = M1Cache(Path(tempfile.mkdtemp()), mt5_module=FakeMT5(gen))
    spread2 = BrokerSpreadModel(cfg2)
    sim2 = ShadowSimulator(m1_cache=cache2, spread_model=spread2,
                           feature_engine=None, config=cfg2)
    out2 = sim2.simulate(rec)
    if out2.sim_exit_reason != "TP":
        _fail(f"with rule disabled, expected TP, got {out2.sim_exit_reason}")
    _ok(f"toggle off -> TP exit at {out2.sim_exit_price:.5f} (control)")


def test_4_timeout():
    """No SL/TP hit within max_hold_minutes — exit at last bar's close."""
    print("\n[4] Timeout (no hit within max_hold)")
    signal_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)

    # Build 240 minutes of bars all within a tight 1-pip range, never hitting wide SL/TP
    def gen(pair, s, e):
        ohlc = [(1.0500, 1.0501, 1.0499, 1.0500) for _ in range(240)]
        return _make_bars(signal_time, ohlc)

    sim = _build_simulator(gen)
    # Wide SL/TP that won't trigger
    rec = _make_record(direction="BUY", signal_time=signal_time.timestamp(),
                       entry=1.0500, sl=1.0400, tp=1.0600)
    out = sim.simulate(rec)

    if out.sim_exit_reason != "TIMEOUT":
        _fail(f"exit_reason: {out.sim_exit_reason}")
    if out.sim_exit_price != 1.0500:
        _fail(f"timeout exit should be last bar close 1.0500, got {out.sim_exit_price}")
    # Duration should be close to max_hold_minutes
    if out.sim_duration_minutes < 200:
        _fail(f"duration too short for timeout: {out.sim_duration_minutes}min")
    _ok(f"TIMEOUT after {out.sim_duration_minutes}min, exit={out.sim_exit_price:.5f}")


def test_5_failed_no_m1_data():
    """fetch_m1 returns None -> FAILED outcome with reason."""
    print("\n[5] FAILED — no M1 data")
    signal_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)

    # Generator returns empty array (MT5 has no bars for this window)
    def gen(pair, s, e):
        return np.empty(0, dtype=_M1_DTYPE)

    sim = _build_simulator(gen)
    rec = _make_record(direction="BUY", signal_time=signal_time.timestamp())
    out = sim.simulate(rec)

    if out.sim_exit_reason != "FAILED":
        _fail(f"expected FAILED, got {out.sim_exit_reason}")
    if "empty" not in out.sim_failure_reason.lower() and \
       "no_m1" not in out.sim_failure_reason.lower():
        _fail(f"failure reason should reference data unavailability: {out.sim_failure_reason}")
    if out.sim_pnl_pips != 0.0:
        _fail(f"FAILED outcome should leave pnl at 0.0, got {out.sim_pnl_pips}")
    _ok(f"FAILED with reason={out.sim_failure_reason}")


def test_6_session_spread_asymmetry():
    """Same signal at Tokyo (03:00 UTC) vs overlap (14:00 UTC).

    Tokyo entry pays a wider spread, so its sim_pnl_pips should be
    measurably worse than the overlap entry by ≈ (tokyo_spread - overlap_spread).
    """
    print("\n[6] Session-spread asymmetry (Tokyo vs Overlap)")
    # EURUSD: overlap spread 0.05pt, tokyo spread 0.30pt -> 0.25pt difference

    overlap_t = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)
    tokyo_t = datetime(2026, 4, 15, 3, 0, 0, tzinfo=timezone.utc)

    # Same OHLC structure for both runs — the only thing that differs is
    # the signal_time, hence session classification.
    def make_gen(start_dt):
        def gen(pair, s, e):
            return _make_bars(start_dt, [
                (1.0498, 1.0502, 1.0497, 1.0500),  # entry candle
                (1.0500, 1.0510, 1.0498, 1.0508),
                (1.0508, 1.0530, 1.0505, 1.0528),
                (1.0528, 1.0541, 1.0525, 1.0540),  # TP hit
            ])
        return gen

    # Overlap sim
    sim_o = _build_simulator(make_gen(overlap_t))
    rec_o = _make_record(direction="BUY", signal_time=overlap_t.timestamp(),
                         entry=1.0500, sl=1.0480, tp=1.0540)
    out_o = sim_o.simulate(rec_o)

    # Tokyo sim
    sim_t = _build_simulator(make_gen(tokyo_t))
    rec_t = _make_record(direction="BUY", signal_time=tokyo_t.timestamp(),
                         entry=1.0500, sl=1.0480, tp=1.0540)
    out_t = sim_t.simulate(rec_t)

    if out_o.sim_exit_reason != "TP" or out_t.sim_exit_reason != "TP":
        _fail(
            f"both should TP — overlap={out_o.sim_exit_reason} tokyo={out_t.sim_exit_reason}"
        )
    # Tokyo should pay more friction -> lower pnl. Difference ≈ 0.25pt.
    delta = out_o.sim_pnl_pips - out_t.sim_pnl_pips
    if delta <= 0:
        _fail(f"Tokyo should be worse than overlap, but tokyo={out_t.sim_pnl_pips} >= overlap={out_o.sim_pnl_pips}")
    if delta < 0.20 or delta > 0.30:
        _fail(
            f"Expected ~0.25pt spread difference, got {delta:.2f}p "
            f"(overlap={out_o.sim_pnl_pips}p, tokyo={out_t.sim_pnl_pips}p)"
        )
    _ok(
        f"overlap pnl={out_o.sim_pnl_pips}p, tokyo pnl={out_t.sim_pnl_pips}p, "
        f"delta={delta:.2f}p (matches spread differential)"
    )


# ─────────────────────────────────────────────────────────────────────
# Fix A (2026-05-07) — stale_proposed_levels guard
# ─────────────────────────────────────────────────────────────────────

def test_7_stale_levels_guard_sell_overshoot():
    """SELL signal: bars[0] price drifts up 10p between signal_time and
    bars[0], so WCF→LOW lands sim_entry ABOVE proposed SL. Guard must
    fire and return FAILED('stale_proposed_levels')."""
    print("\n[7] stale-levels guard fires on SELL with sim_entry > SL_price")
    signal_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)

    # Proposed: sell at 1.0500, SL 1.0510 (10p above), TP 1.0480 (20p below).
    # bars[0] LOW = 1.0512 (already past proposed SL by 2p).
    # WCF for SELL takes LOW=1.0512, then -spread-slip → ~1.05116.
    # sim_entry 1.0512 > SL 1.0510 → guard fires.
    def gen(pair, s, e):
        return _make_bars(signal_time, [
            (1.0515, 1.0518, 1.0512, 1.0516),  # entry candle, all above proposed SL
            (1.0516, 1.0520, 1.0510, 1.0512),
        ])

    sim = _build_simulator(gen)
    rec = _make_record(direction="SELL", signal_time=signal_time.timestamp(),
                       entry=1.0500, sl=1.0510, tp=1.0480)
    out = sim.simulate(rec)

    if out.sim_exit_reason != "FAILED":
        _fail(f"expected FAILED, got {out.sim_exit_reason} pnl={out.sim_pnl_pips}")
    if out.sim_failure_reason != "stale_proposed_levels":
        _fail(f"failure_reason: expected 'stale_proposed_levels', got {out.sim_failure_reason!r}")
    _ok("SELL with sim_entry > SL_price -> FAILED(stale_proposed_levels)")


def test_8_stale_levels_guard_buy_overshoot():
    """Symmetric BUY case: bars[0] LOW puts sim_entry below proposed SL."""
    print("\n[8] stale-levels guard fires on BUY with sim_entry < SL_price")
    signal_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)

    # Proposed: buy at 1.0500, SL 1.0490 (10p below), TP 1.0520 (20p above).
    # bars[0] HIGH = 1.0488 (below proposed SL).
    # WCF for BUY takes HIGH=1.0488, then +spread+slip → ~1.04884.
    # sim_entry 1.04884 < SL 1.0490 → guard fires.
    def gen(pair, s, e):
        return _make_bars(signal_time, [
            (1.0485, 1.0488, 1.0482, 1.0486),
            (1.0486, 1.0490, 1.0480, 1.0488),
        ])

    sim = _build_simulator(gen)
    rec = _make_record(direction="BUY", signal_time=signal_time.timestamp(),
                       entry=1.0500, sl=1.0490, tp=1.0520)
    out = sim.simulate(rec)

    if out.sim_exit_reason != "FAILED":
        _fail(f"expected FAILED, got {out.sim_exit_reason} pnl={out.sim_pnl_pips}")
    if out.sim_failure_reason != "stale_proposed_levels":
        _fail(f"failure_reason: expected 'stale_proposed_levels', got {out.sim_failure_reason!r}")
    _ok("BUY with sim_entry < SL_price -> FAILED(stale_proposed_levels)")


def test_9_stale_levels_guard_tp_overshoot():
    """SELL where bars[0] drifted DOWN below proposed TP: sim_entry below TP, guard fires."""
    print("\n[9] stale-levels guard fires on SELL with sim_entry below TP_price")
    signal_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)

    # Proposed: sell at 1.0500, SL 1.0510, TP 1.0480.
    # bars[0] LOW = 1.0475 (below proposed TP 1.0480).
    # WCF for SELL takes LOW=1.0475, sim_entry < TP → guard fires.
    def gen(pair, s, e):
        return _make_bars(signal_time, [
            (1.0478, 1.0480, 1.0475, 1.0476),
            (1.0476, 1.0480, 1.0470, 1.0473),
        ])

    sim = _build_simulator(gen)
    rec = _make_record(direction="SELL", signal_time=signal_time.timestamp(),
                       entry=1.0500, sl=1.0510, tp=1.0480)
    out = sim.simulate(rec)

    if out.sim_exit_reason != "FAILED":
        _fail(f"expected FAILED, got {out.sim_exit_reason}")
    if out.sim_failure_reason != "stale_proposed_levels":
        _fail(f"failure_reason: {out.sim_failure_reason!r}")
    _ok("SELL with sim_entry < TP_price -> FAILED(stale_proposed_levels)")


def test_10_normal_sell_passes_guard():
    """Regression: a normal SELL where sim_entry stays between SL and TP
    must NOT fire the guard. This catches false-positive guard fires."""
    print("\n[10] normal SELL with sim_entry between SL and TP passes guard")
    signal_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)

    # Proposed: sell at 1.0500, SL 1.0520, TP 1.0460.
    # bars[0] LOW = 1.0498 (within range).
    # sim_entry ~1.04976 → between TP 1.0460 and SL 1.0520. Guard does NOT fire.
    def gen(pair, s, e):
        return _make_bars(signal_time, [
            (1.0500, 1.0502, 1.0498, 1.0500),  # entry candle
            (1.0500, 1.0510, 1.0500, 1.0508),
            (1.0508, 1.0521, 1.0507, 1.0518),  # SL hit on bar 2
        ])

    sim = _build_simulator(gen)
    rec = _make_record(direction="SELL", signal_time=signal_time.timestamp(),
                       entry=1.0500, sl=1.0520, tp=1.0460)
    out = sim.simulate(rec)

    if out.sim_exit_reason == "FAILED" and out.sim_failure_reason == "stale_proposed_levels":
        _fail("guard FALSE-POSITIVE on normal SELL — should NOT fire")
    if out.sim_exit_reason != "SL":
        _fail(f"expected SL exit, got {out.sim_exit_reason}")
    _ok("normal SELL passes guard cleanly, hits SL as expected")


def test_11_normal_buy_passes_guard():
    """Regression: a normal BUY where sim_entry stays between SL and TP."""
    print("\n[11] normal BUY with sim_entry between SL and TP passes guard")
    signal_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)

    def gen(pair, s, e):
        return _make_bars(signal_time, [
            (1.0498, 1.0502, 1.0497, 1.0500),  # entry candle
            (1.0500, 1.0510, 1.0498, 1.0508),
            (1.0508, 1.0530, 1.0505, 1.0528),
            (1.0528, 1.0541, 1.0525, 1.0540),  # TP hit
        ])

    sim = _build_simulator(gen)
    rec = _make_record(direction="BUY", signal_time=signal_time.timestamp(),
                       entry=1.0500, sl=1.0480, tp=1.0540)
    out = sim.simulate(rec)

    if out.sim_exit_reason == "FAILED" and out.sim_failure_reason == "stale_proposed_levels":
        _fail("guard FALSE-POSITIVE on normal BUY — should NOT fire")
    if out.sim_exit_reason != "TP":
        _fail(f"expected TP exit, got {out.sim_exit_reason}")
    _ok("normal BUY passes guard cleanly, hits TP as expected")


def test_12_stale_levels_classified_permanent_in_worker():
    """Worker's _is_transient_failure_reason must classify
    'stale_proposed_levels' as PERMANENT, not transient. Otherwise
    records would be retried 12 times with no possibility of
    success (same failure mode as the empty_m1 bug)."""
    print("\n[12] worker classifies stale_proposed_levels as PERMANENT (no retry)")
    from takumi_trader.core.shadow_sim_worker import _is_transient_failure_reason
    if _is_transient_failure_reason("stale_proposed_levels"):
        _fail("stale_proposed_levels classified as transient — would retry 12 times wastefully")
    _ok("stale_proposed_levels classified as permanent (no retry budget)")


# ── Bonus: real strength-pass dry-run from yesterday's journal ─────

def diagnostic_real_record():
    """Pull a real strength-pass from yesterday's journal and dump simulate() output."""
    print("\n[diagnostic] Dry-run on a real journal record (no live MT5; M1Cache disk-only)")
    journal_path = _REPO / "data" / "shadow_trades_Sv2.json"
    if not journal_path.exists():
        print("  (skipped — journal not present on this machine)")
        return

    import json
    data = json.loads(journal_path.read_text(encoding="utf-8"))
    # Find a strength-pass record (proposed_entry > 0)
    candidates = [
        d for d in data
        if d.get("proposed_entry", 0) > 0
        and d.get("status") == "BLOCKED"
        and d.get("block_gate") in ("divergence_spread", "structural")
    ]
    if not candidates:
        print("  (no eligible strength-pass record found in journal)")
        return

    sample = candidates[len(candidates) // 2]  # pick a middle one
    rec = ShadowSignalRecord()
    for k, v in sample.items():
        if hasattr(rec, k):
            setattr(rec, k, v)

    # Build sim with disk-only cache (no live MT5 — we may or may not have parquet
    # for this signal_time, that's OK — FAILED is acceptable, just want to verify
    # the algorithm runs end-to-end without exception)
    cfg = ShadowSimulatorConfig()
    cache = M1Cache(_REPO / "data" / "m1_cache", mt5_module=None)
    spread_model = BrokerSpreadModel(cfg)
    sim = ShadowSimulator(m1_cache=cache, spread_model=spread_model,
                          feature_engine=None, config=cfg)

    print(f"  Real record: shadow_id={rec.shadow_id} {rec.signal_time_str} {rec.pair} {rec.direction}")
    print(f"               proposed_entry={rec.proposed_entry} sl={rec.proposed_sl_price} tp={rec.proposed_tp_price}")

    out = sim.simulate(rec)
    print(f"  Sim outcome: {out.sim_exit_reason}")
    if out.sim_exit_reason == "FAILED":
        print(f"               failure_reason={out.sim_failure_reason}")
        print(f"               (expected — no M1 cache for past signal_time on dry-run machine)")
    else:
        print(f"               sim_exit_price={out.sim_exit_price:.5f}")
        print(f"               sim_pnl_pips={out.sim_pnl_pips}")
        print(f"               sim_mae_pips={out.sim_mae_pips}")
        print(f"               sim_mfe_pips={out.sim_mfe_pips}")
        print(f"               sim_duration_minutes={out.sim_duration_minutes}")
        print(f"               sim_pessimism_applied={out.sim_pessimism_applied}")


if __name__ == "__main__":
    print("Phase C.2 fixture tests")
    test_1_clean_tp_hit_buy()
    test_2_clean_sl_hit_sell()
    test_3_ambiguous_candle_sl_first()
    test_4_timeout()
    test_5_failed_no_m1_data()
    test_6_session_spread_asymmetry()
    # Fix A (2026-05-07) — stale_proposed_levels guard
    test_7_stale_levels_guard_sell_overshoot()
    test_8_stale_levels_guard_buy_overshoot()
    test_9_stale_levels_guard_tp_overshoot()
    test_10_normal_sell_passes_guard()
    test_11_normal_buy_passes_guard()
    test_12_stale_levels_classified_permanent_in_worker()

    diagnostic_real_record()

    print("\nALL C.2 FIXTURE TESTS PASSED")
