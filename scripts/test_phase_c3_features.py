"""Phase C.3 fixture tests — lazy feature recompute.

Tests:
  1. extract_feat_dict produces 138 feat_* keys with default values from empty input
  2. extract_feat_dict applies cross_pair baskets correctly
  3. _recompute_features returns None when M1Cache has no data
  4. _recompute_features handles missing input_snapshot_json gracefully
  5. simulate() with feature_engine populates outcome.features for successful sims
  6. simulate() with feature_engine LEAVES outcome.features=None on FAILED outcomes
  7. HISTORICAL-VS-CURRENT — same record run with bars from yesterday vs today
     produces different feature values (proves historical bars flow through)

Plus a diagnostic that pulls a real strength-pass record from the journal,
runs simulate() with synthetic-but-historical bars, dumps all 138 feat_*
keys alphabetically with values for architect review.

Run from repo root:
    python scripts/test_phase_c3_features.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from takumi_trader.core.broker_spread_model import BrokerSpreadModel  # noqa: E402
from takumi_trader.core.m1_cache import M1Cache  # noqa: E402
from takumi_trader.core.shadow_logger import ShadowSignalRecord  # noqa: E402
from takumi_trader.core.shadow_simulator import (  # noqa: E402
    ShadowSimulator, ShadowSimulatorConfig,
)
from takumi_trader.features.feature_engine import (  # noqa: E402
    FeatureEngine, extract_feat_dict,
)


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


_M1_DTYPE = np.dtype([
    ("time", "i8"), ("open", "f8"), ("high", "f8"),
    ("low", "f8"), ("close", "f8"),
])


def _make_walk(start_dt: datetime, n: int, base_price: float = 1.10,
               step: float = 0.0001) -> np.ndarray:
    """Construct n M1 bars with a smoothly trending price."""
    arr = np.empty(n, dtype=_M1_DTYPE)
    base = int(start_dt.timestamp())
    for i in range(n):
        o = base_price + step * i
        arr[i] = (base + i * 60, o, o + 0.0002, o - 0.0001, o + 0.0001)
    return arr


class FakeMT5:
    TIMEFRAME_M1 = 1
    def __init__(self, generator):
        self._gen = generator
    def copy_rates_range(self, pair, tf, start_dt, end_dt):
        return self._gen(pair, start_dt, end_dt)


# ─────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────

def test_1_extract_feat_dict_default():
    """Empty input -> 138 feat_* keys with documented defaults."""
    print("\n[1] extract_feat_dict produces 138 keys with defaults")
    out = extract_feat_dict({}, cross_pair_data=None, pair="EURUSD")
    if len(out) != 138:
        _fail(f"expected 138 keys, got {len(out)}")
    non_feat = [k for k in out if not k.startswith("feat_")]
    if non_feat:
        _fail(f"non-feat_* keys: {non_feat}")
    # Spot-check defaults
    if out["feat_h1_hurst"] != 0.5:
        _fail(f"feat_h1_hurst default mismatch: {out['feat_h1_hurst']}")
    if out["feat_csi_strong_count"] != 0:
        _fail(f"feat_csi_strong_count default mismatch")
    if out["feat_h1_regime"] != "":
        _fail(f"feat_h1_regime default should be empty string")
    if out["feat_schema_version"] != 2:
        _fail(f"feat_schema_version: {out['feat_schema_version']}")
    _ok(f"138 feat_* keys with documented defaults")


def test_2_extract_feat_dict_cross_pair_baskets():
    """cross_pair_data passed -> synthetic baskets populated."""
    print("\n[2] extract_feat_dict cross-pair baskets")
    cross = {
        "EURUSD": 1.10, "EURJPY": 165.0, "EURGBP": 0.86,
        "EURAUD": 1.65, "EURCAD": 1.50, "EURCHF": 0.97,
        "USDJPY": 150.0, "GBPJPY": 191.0, "AUDJPY": 100.0,
        "CADJPY": 110.0, "NZDJPY": 92.0, "CHFJPY": 170.0,
        "GBPUSD": 1.27, "GBPAUD": 1.93, "GBPCAD": 1.74,
        "GBPCHF": 1.13, "GBPNZD": 2.07,
        "AUDUSD": 0.66, "AUDCAD": 0.90, "AUDNZD": 1.07,
        "AUDCHF": 0.58,
    }
    out = extract_feat_dict({}, cross_pair_data=cross, pair="EURUSD")
    if "feat_eur_index" not in out:
        _fail("feat_eur_index missing — cross-pair baskets didn't populate")
    if out["feat_eur_index"] == 0.0:
        _fail("feat_eur_index is 0.0 — basket calc didn't fire")
    if "feat_jpy_index" not in out:
        _fail("feat_jpy_index missing")
    if "feat_gbp_index" not in out:
        _fail("feat_gbp_index missing")
    if "feat_aud_index" not in out:
        _fail("feat_aud_index missing")
    if "feat_triangular_arb_pips" not in out:
        _fail("feat_triangular_arb_pips missing")
    _ok(
        f"baskets: eur={out['feat_eur_index']:.3f}, "
        f"jpy={out['feat_jpy_index']:.3f}, gbp={out['feat_gbp_index']:.3f}, "
        f"aud={out['feat_aud_index']:.3f}"
    )


def test_3_recompute_features_no_m1_data():
    """When M1Cache has no data, _recompute_features returns None."""
    print("\n[3] _recompute_features handles M1Cache miss")
    cfg = ShadowSimulatorConfig()
    # Cache with no MT5 -> all fetches return None
    cache = M1Cache(Path(tempfile.mkdtemp()), mt5_module=None)
    spread = BrokerSpreadModel(cfg)
    fe = FeatureEngine()
    sim = ShadowSimulator(
        m1_cache=cache, spread_model=spread,
        feature_engine=fe, config=cfg,
    )

    rec = ShadowSignalRecord(
        shadow_id=1, strategy_id="Sv2", pair="EURUSD", direction="BUY",
        signal_time=datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp(),
        proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
        input_snapshot_json='{"composite_scores": {"USD": 5.0}}',
    )
    result = sim._recompute_features(rec)
    if result is not None:
        _fail(f"expected None for M1Cache miss, got {len(result)} keys")
    _ok("M1Cache miss -> _recompute_features returns None")


def test_4_recompute_features_no_snapshot():
    """Empty input_snapshot_json -> _recompute_features returns None."""
    print("\n[4] _recompute_features handles missing snapshot")
    cfg = ShadowSimulatorConfig()
    cache = M1Cache(Path(tempfile.mkdtemp()), mt5_module=None)
    spread = BrokerSpreadModel(cfg)
    fe = FeatureEngine()
    sim = ShadowSimulator(
        m1_cache=cache, spread_model=spread,
        feature_engine=fe, config=cfg,
    )

    rec = ShadowSignalRecord(
        shadow_id=1, strategy_id="Sv2", pair="EURUSD", direction="BUY",
        signal_time=datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc).timestamp(),
        proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
        input_snapshot_json="",  # missing
    )
    result = sim._recompute_features(rec)
    if result is not None:
        _fail(f"expected None for missing snapshot, got result")
    _ok("empty snapshot -> _recompute_features returns None")


def test_5_simulate_populates_features_on_success():
    """simulate() with feature_engine + good data -> outcome.features is dict."""
    print("\n[5] simulate() populates outcome.features on TP/SL/TIMEOUT")
    signal_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)

    # Generator returns lookback bars (24h before signal_time)
    # AND forward bars (signal_time + max_hold)
    def gen(pair, start_dt, end_dt):
        # Compute total minutes in the window and produce bars covering it
        n_min = int((end_dt - start_dt).total_seconds() // 60)
        if n_min <= 0:
            return np.empty(0, dtype=_M1_DTYPE)
        return _make_walk(start_dt, n_min)

    cfg = ShadowSimulatorConfig()
    cache = M1Cache(Path(tempfile.mkdtemp()), mt5_module=FakeMT5(gen))
    spread = BrokerSpreadModel(cfg)
    fe = FeatureEngine()
    sim = ShadowSimulator(
        m1_cache=cache, spread_model=spread,
        feature_engine=fe, config=cfg,
    )

    snap = {
        "composite_scores": {"USD": 5.0, "EUR": 7.0, "GBP": 4.0, "JPY": 5.0,
                             "CAD": 5.5, "AUD": 4.5, "NZD": 5.0, "CHF": 5.5},
        "cross_pair_close_prices": {"EURUSD": 1.10, "USDJPY": 150.0, "EURJPY": 165.0},
    }
    rec = ShadowSignalRecord(
        shadow_id=1, strategy_id="Sv2", pair="EURUSD", direction="BUY",
        signal_time=signal_time.timestamp(),
        proposed_entry=1.1000, proposed_sl_price=1.0980, proposed_tp_price=1.1100,
        input_snapshot_json=json.dumps(snap),
    )
    out = sim.simulate(rec)

    if out.sim_exit_reason == "FAILED":
        _fail(f"sim FAILED: {out.sim_failure_reason}")
    if out.features is None:
        _fail("outcome.features is None after successful sim")
    if not isinstance(out.features, dict):
        _fail(f"outcome.features is {type(out.features).__name__}, expected dict")
    if len(out.features) < 138:
        _fail(f"feat_* count below 138: {len(out.features)}")
    if not all(k.startswith("feat_") for k in out.features):
        _fail("non-feat_* keys leaked")
    _ok(f"successful sim ({out.sim_exit_reason}) -> {len(out.features)} feat_* keys populated")


def test_6_simulate_skips_features_on_failed():
    """simulate() with FAILED outcome -> outcome.features stays None."""
    print("\n[6] simulate() leaves features=None on FAILED")
    signal_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)

    def gen(pair, start_dt, end_dt):
        return np.empty(0, dtype=_M1_DTYPE)  # always empty -> FAILED

    cfg = ShadowSimulatorConfig()
    cache = M1Cache(Path(tempfile.mkdtemp()), mt5_module=FakeMT5(gen))
    spread = BrokerSpreadModel(cfg)
    fe = FeatureEngine()
    sim = ShadowSimulator(m1_cache=cache, spread_model=spread,
                          feature_engine=fe, config=cfg)

    rec = ShadowSignalRecord(
        shadow_id=1, strategy_id="Sv2", pair="EURUSD", direction="BUY",
        signal_time=signal_time.timestamp(),
        proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
        input_snapshot_json='{"composite_scores": {}}',
    )
    out = sim.simulate(rec)
    if out.sim_exit_reason != "FAILED":
        _fail(f"expected FAILED, got {out.sim_exit_reason}")
    if out.features is not None:
        _fail(f"expected features=None on FAILED, got {type(out.features).__name__}")
    _ok("FAILED outcome -> features stays None (no recompute attempted)")


def test_7_historical_vs_current_bars():
    """The acid test for historical-feature-recompute correctness.

    Build two simulators, both with the SAME signal time, but the underlying
    M1Cache backed by FakeMT5 generators that produce DIFFERENT prices for
    the same time window. Run feature recompute on each. The resulting
    feat_* dicts must DIFFER — proving that the feature recompute is
    consuming the bars we passed in, not silently fetching current state.

    If the dicts were identical, it would mean the historical bars aren't
    flowing through and we'd be back to the compute_entry_features bug
    that motivated Path A.
    """
    print("\n[7] HISTORICAL-VS-CURRENT: bars passed-in are honored, not silently re-fetched")
    signal_time = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)

    # Generator A — falling prices in lookback window
    def gen_falling(pair, start_dt, end_dt):
        n_min = int((end_dt - start_dt).total_seconds() // 60)
        if n_min <= 0:
            return np.empty(0, dtype=_M1_DTYPE)
        # Start at 1.20, fall to ~1.10 over the window
        arr = np.empty(n_min, dtype=_M1_DTYPE)
        base = int(start_dt.timestamp())
        for i in range(n_min):
            o = 1.20 - (0.0001 * i)
            arr[i] = (base + i * 60, o, o + 0.0001, o - 0.0002, o - 0.0001)
        return arr

    # Generator B — rising prices in lookback window
    def gen_rising(pair, start_dt, end_dt):
        n_min = int((end_dt - start_dt).total_seconds() // 60)
        if n_min <= 0:
            return np.empty(0, dtype=_M1_DTYPE)
        # Start at 1.05, rise to ~1.15 over the window
        arr = np.empty(n_min, dtype=_M1_DTYPE)
        base = int(start_dt.timestamp())
        for i in range(n_min):
            o = 1.05 + (0.0001 * i)
            arr[i] = (base + i * 60, o, o + 0.0002, o - 0.0001, o + 0.0001)
        return arr

    snap = {"composite_scores": {"USD": 5.0, "EUR": 7.0},
            "cross_pair_close_prices": {"EURUSD": 1.10}}
    rec = ShadowSignalRecord(
        shadow_id=1, strategy_id="Sv2", pair="EURUSD", direction="BUY",
        signal_time=signal_time.timestamp(),
        proposed_entry=1.10, proposed_sl_price=1.099, proposed_tp_price=1.102,
        input_snapshot_json=json.dumps(snap),
    )

    # Run feature recompute against each generator
    def run_with_gen(gen):
        cfg = ShadowSimulatorConfig()
        cache = M1Cache(Path(tempfile.mkdtemp()), mt5_module=FakeMT5(gen))
        spread = BrokerSpreadModel(cfg)
        fe = FeatureEngine()
        sim = ShadowSimulator(m1_cache=cache, spread_model=spread,
                              feature_engine=fe, config=cfg)
        return sim._recompute_features(rec)

    feat_falling = run_with_gen(gen_falling)
    feat_rising = run_with_gen(gen_rising)

    if feat_falling is None:
        _fail("falling generator produced no features (M1 fetch failed)")
    if feat_rising is None:
        _fail("rising generator produced no features (M1 fetch failed)")

    # Compare: there MUST be price-derived features that differ.
    # Falling-bars lookback should have DIFFERENT h1_adx, h1_lr_slope,
    # m15_realized_var, etc. than rising-bars.
    differing_keys: list[str] = []
    for k in sorted(feat_falling.keys()):
        if isinstance(feat_falling[k], (int, float)) and isinstance(feat_rising[k], (int, float)):
            if abs(feat_falling[k] - feat_rising[k]) > 1e-9:
                differing_keys.append(k)

    if not differing_keys:
        _fail(
            "feat_* dicts are IDENTICAL between falling and rising bars — "
            "this means the historical bars aren't flowing through to "
            "compute_for_entry. Path A is broken."
        )

    # We expect at least 5+ price-derived features to differ. If only 1-2
    # differ, something might be subtly wrong even if not catastrophically.
    if len(differing_keys) < 5:
        _fail(
            f"Only {len(differing_keys)} keys differ between falling/rising "
            f"bars (expected >=5). Suspicious — historical bars may not be "
            f"fully consumed. Differing keys: {differing_keys}"
        )

    _ok(
        f"{len(differing_keys)} feat_* keys differ between falling/rising "
        f"bars -> historical bars ARE flowing through (Path A working). "
        f"Sample: {differing_keys[:3]}"
    )


# ── Diagnostic: real-record alphabetical feature dump ───────────────

def diagnostic_real_record_dump():
    """Pull a real strength-pass record from the journal, run feature recompute
    against synthetic-but-historical bars, and dump all 138 feat_* keys
    alphabetically with values. The architect scans this for:
      * Features that are zero across the board (suggests broken input)
      * Wildly out-of-range values (RSI=500, ATR_pips=0.0001)
      * Names suggesting data we didn't snapshot
    """
    print("\n[diagnostic] Real-record feature dump (138 keys alphabetical)")
    journal_path = _REPO / "data" / "shadow_trades_Sv2.json"
    if not journal_path.exists():
        print("  (skipped — journal not present)")
        return

    data = json.loads(journal_path.read_text(encoding="utf-8"))
    candidates = [
        d for d in data
        if d.get("proposed_entry", 0) > 0
        and d.get("status") == "BLOCKED"
        and d.get("block_gate") in ("divergence_spread", "structural")
        and d.get("input_snapshot_json")
    ]
    if not candidates:
        print("  (no eligible record)")
        return
    sample = candidates[len(candidates) // 2]
    rec = ShadowSignalRecord()
    for k, v in sample.items():
        if hasattr(rec, k):
            setattr(rec, k, v)

    # Use synthetic FakeMT5 so we don't depend on parquet cache being populated
    def gen(pair, start_dt, end_dt):
        n_min = int((end_dt - start_dt).total_seconds() // 60)
        if n_min <= 0:
            return np.empty(0, dtype=_M1_DTYPE)
        return _make_walk(start_dt, n_min, base_price=rec.proposed_entry)

    cfg = ShadowSimulatorConfig()
    cache = M1Cache(Path(tempfile.mkdtemp()), mt5_module=FakeMT5(gen))
    spread = BrokerSpreadModel(cfg)
    fe = FeatureEngine()
    sim = ShadowSimulator(m1_cache=cache, spread_model=spread,
                          feature_engine=fe, config=cfg)

    out = sim.simulate(rec)
    print(f"  Real record: shadow_id={rec.shadow_id} {rec.signal_time_str} "
          f"{rec.pair} {rec.direction}")
    print(f"  Sim outcome: {out.sim_exit_reason} pnl={out.sim_pnl_pips}p")
    if out.features is None:
        print("  features=None — recompute failed; nothing to dump")
        return

    print(f"\n  Feature dump ({len(out.features)} keys, alphabetical):")
    for k in sorted(out.features.keys()):
        v = out.features[k]
        # Format value compactly
        if isinstance(v, float):
            vs = f"{v:.6g}"
        elif isinstance(v, bool):
            vs = str(v)
        else:
            vs = str(v)[:40]
        print(f"    {k:<40} = {vs}")

    # Anomaly scan
    print()
    print(f"  -- Quick anomaly scan --")
    zero_count = sum(
        1 for v in out.features.values()
        if isinstance(v, (int, float)) and v == 0
    )
    print(f"    Numeric features at exactly 0.0: {zero_count} / {len(out.features)}")
    nan_count = sum(
        1 for v in out.features.values()
        if isinstance(v, float) and (v != v)  # NaN
    )
    inf_count = sum(
        1 for v in out.features.values()
        if isinstance(v, float) and v in (float("inf"), float("-inf"))
    )
    print(f"    NaN values: {nan_count}")
    print(f"    Infinite values: {inf_count}")

    if nan_count > 0 or inf_count > 0:
        print(f"    ⚠ NaN/Inf detected — surfaces calibration issues for review")


if __name__ == "__main__":
    print("Phase C.3 fixture tests")
    test_1_extract_feat_dict_default()
    test_2_extract_feat_dict_cross_pair_baskets()
    test_3_recompute_features_no_m1_data()
    test_4_recompute_features_no_snapshot()
    test_5_simulate_populates_features_on_success()
    test_6_simulate_skips_features_on_failed()
    test_7_historical_vs_current_bars()
    diagnostic_real_record_dump()
    print("\nALL C.3 FIXTURE TESTS PASSED")
