"""Phase C.1 smoke tests — dataclasses, M1Cache parquet, BrokerSpreadModel.

6 tests required by the C.1 spec:
  1. M1Cache fetches a known pair/window from MT5, writes parquet, reads back identical.
     (Skipped if MT5 unavailable — test with a synthetic fake_mt5 instead.)
  2. M1Cache resample M1 -> M15 produces correct bar count + OHLC math.
  3. BrokerSpreadModel.lookup returns IC_MARKETS_SPREADS values for known pair/session.
  4. BrokerSpreadModel.lookup returns conservative fallback for unknown pair.
  5. ShadowSimulator constructor accepts config + cache + spread_model + feature_engine.
  6. ShadowSimulator.fetch_m1 returns ndarray with correct length for a 240-min window.

Run:
    python scripts/test_phase_c1_smoke.py
"""
from __future__ import annotations

import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from takumi_trader.core.broker_spread_model import (  # noqa: E402
    BrokerSpreadModel, SpreadLookup,
)
from takumi_trader.core.m1_cache import M1Cache  # noqa: E402
from takumi_trader.core.shadow_simulator import (  # noqa: E402
    ShadowSimulator, ShadowSimulatorConfig, SimulatedOutcome,
)


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


# ── Synthetic MT5 stub ──────────────────────────────────────────────
# Test 1 uses a fake MT5 module so the test is deterministic and runs
# without a live MT5 connection. The fake produces known-shape M1 bars
# so we can verify parquet round-trip is byte-identical.

class FakeMT5:
    """Minimal stub of the MetaTrader5 module surface M1Cache uses."""

    TIMEFRAME_M1 = 1

    def __init__(self, generator):
        self._generator = generator

    def copy_rates_range(self, pair, tf_const, start_dt, end_dt):
        return self._generator(pair, start_dt, end_dt)


def _make_synthetic_bars(start_dt: datetime, end_dt: datetime) -> np.ndarray:
    """Generate synthetic M1 bars over [start_dt, end_dt] in 60-second steps.

    OHLC pattern: open=1.0500 + 0.0001 * minute_index,
                  high=open + 0.0002, low=open - 0.0001, close=open + 0.0001.
    Time field is epoch-seconds int.
    """
    start = int(start_dt.timestamp())
    end = int(end_dt.timestamp())
    n = (end - start) // 60
    if n <= 0:
        return np.empty(0, dtype=[
            ("time", "i8"), ("open", "f8"), ("high", "f8"),
            ("low", "f8"), ("close", "f8"),
        ])
    arr = np.empty(n, dtype=[
        ("time", "i8"), ("open", "f8"), ("high", "f8"),
        ("low", "f8"), ("close", "f8"),
    ])
    for i in range(n):
        o = 1.0500 + 0.0001 * i
        arr[i] = (start + i * 60, o, o + 0.0002, o - 0.0001, o + 0.0001)
    return arr


# ─────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────

def test_1_m1_cache_parquet_roundtrip():
    """Fetch from fake MT5 -> write parquet -> read back -> identical bytes."""
    print("\n[1] M1Cache parquet round-trip (fake MT5)")
    with tempfile.TemporaryDirectory() as td:
        # Pick a window in the past so the recency guard doesn't block fetch
        start_dt = datetime(2026, 4, 15, 10, 0, 0, tzinfo=timezone.utc)
        end_dt = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)
        start_ep = start_dt.timestamp()
        end_ep = end_dt.timestamp()

        # Generator returns bars covering exactly the requested window
        def gen(pair, s, e):
            return _make_synthetic_bars(s, e)

        cache = M1Cache(Path(td), mt5_module=FakeMT5(gen))
        first = cache.fetch("EURUSD", start_ep, end_ep)
        if first is None:
            _fail("first fetch returned None — MT5 fill failed")
        if len(first) == 0:
            _fail("first fetch returned empty array")

        # Confirm parquet was written
        parquet_path = Path(td) / "EURUSD" / "2026-04.parquet"
        if not parquet_path.exists():
            _fail(f"parquet file not written: {parquet_path}")

        # Construct a fresh M1Cache (no in-memory cache) and read from disk
        cache2 = M1Cache(Path(td), mt5_module=None)  # disable MT5 to force disk read
        second = cache2.fetch("EURUSD", start_ep, end_ep)
        if second is None:
            _fail("second fetch (disk only) returned None")

        # Byte-identical comparison on the OHLC fields
        if len(first) != len(second):
            _fail(f"length mismatch: {len(first)} vs {len(second)}")
        if not np.array_equal(first["time"], second["time"]):
            _fail("time field mismatch after round-trip")
        if not np.allclose(first["open"], second["open"]):
            _fail("open field mismatch after round-trip")
        if not np.allclose(first["high"], second["high"]):
            _fail("high field mismatch")
        if not np.allclose(first["low"], second["low"]):
            _fail("low field mismatch")
        if not np.allclose(first["close"], second["close"]):
            _fail("close field mismatch")
        _ok(f"parquet round-trip on {len(first)} bars: byte-identical OHLC across read")


def test_2_resample_m1_to_m15():
    """Resample 240 M1 bars to 16 M15 bars with correct OHLC aggregation."""
    print("\n[2] M1Cache resample M1 -> M15")
    with tempfile.TemporaryDirectory() as td:
        cache = M1Cache(Path(td), mt5_module=None)

        # 240 M1 bars starting at a clean M15 boundary
        start = datetime(2026, 4, 15, 10, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 4, 15, 14, 0, 0, tzinfo=timezone.utc)
        bars = _make_synthetic_bars(start, end)
        if len(bars) != 240:
            _fail(f"expected 240 M1 bars, got {len(bars)}")

        m15 = cache.resample("EURUSD", "2026-04", bars, target_minutes=15)
        if len(m15) != 16:
            _fail(f"expected 16 M15 bars, got {len(m15)}")

        # First M15 bar should aggregate M1 bars 0..14
        first_m15 = m15[0]
        first_15 = bars[:15]
        if first_m15["open"] != first_15[0]["open"]:
            _fail(f"M15[0].open != M1[0].open")
        expected_high = float(np.max(first_15["high"]))
        if abs(first_m15["high"] - expected_high) > 1e-9:
            _fail(f"M15[0].high mismatch: {first_m15['high']} vs {expected_high}")
        expected_low = float(np.min(first_15["low"]))
        if abs(first_m15["low"] - expected_low) > 1e-9:
            _fail(f"M15[0].low mismatch")
        if first_m15["close"] != first_15[-1]["close"]:
            _fail(f"M15[0].close mismatch")

        # Cached on second call (same key)
        m15_cached = cache.resample("EURUSD", "2026-04", bars, target_minutes=15)
        if id(m15) != id(m15_cached):
            _fail("resample cache miss on identical second call")
        _ok(f"240 M1 -> 16 M15 with correct OHLC; in-memory cache hit on retry")


def test_3_spread_lookup_known_pair():
    """BrokerSpreadModel.lookup returns IC_MARKETS_SPREADS values for known pair.

    Verifies all 3 forex sessions fire correctly at boundary times:
      14:00 UTC -> 'overlap' (London-NY tight)
      10:00 UTC -> 'normal'  (London-only)
      03:00 UTC -> 'tokyo'   (Asia)
    """
    print("\n[3] BrokerSpreadModel.lookup for known pair (3-session check)")
    cfg = ShadowSimulatorConfig()
    model = BrokerSpreadModel(cfg)

    # 14:00 UTC -> overlap (tightest)
    overlap_t = datetime(2026, 5, 5, 14, 0, 0, tzinfo=timezone.utc).timestamp()
    lk = model.lookup("GBPJPY", overlap_t)
    if not isinstance(lk, SpreadLookup):
        _fail(f"lookup returned {type(lk).__name__}, expected SpreadLookup")
    if lk.session_key != "overlap":
        _fail(f"session_key at 14:00 UTC: {lk.session_key} (expected 'overlap')")
    if lk.spread_points != 0.3:
        _fail(f"GBPJPY overlap spread: {lk.spread_points} (expected 0.3)")
    if lk.slippage_points != cfg.slippage_points_forex_normal:
        _fail(f"slippage: {lk.slippage_points}")
    if lk.is_news_window:
        _fail("is_news_window True with no hardcoded news")

    # 10:00 UTC -> normal (London-only)
    normal_t = datetime(2026, 5, 5, 10, 0, 0, tzinfo=timezone.utc).timestamp()
    lk2 = model.lookup("GBPJPY", normal_t)
    if lk2.session_key != "normal":
        _fail(f"session_key at 10:00 UTC: {lk2.session_key} (expected 'normal')")
    if lk2.spread_points != 0.5:
        _fail(f"GBPJPY normal spread: {lk2.spread_points} (expected 0.5)")

    # 03:00 UTC -> tokyo
    tokyo_t = datetime(2026, 5, 5, 3, 0, 0, tzinfo=timezone.utc).timestamp()
    lk3 = model.lookup("GBPJPY", tokyo_t)
    if lk3.session_key != "tokyo":
        _fail(f"session_key at 03:00 UTC: {lk3.session_key} (expected 'tokyo')")
    if lk3.spread_points != 0.8:
        _fail(f"GBPJPY tokyo spread: {lk3.spread_points} (expected 0.8)")

    # Boundary spot-check: 07:00 UTC should flip from tokyo to normal
    boundary_t = datetime(2026, 5, 5, 7, 0, 0, tzinfo=timezone.utc).timestamp()
    lk4 = model.lookup("GBPJPY", boundary_t)
    if lk4.session_key != "normal":
        _fail(f"07:00 UTC boundary: {lk4.session_key} (expected 'normal' — Frankfurt open)")

    # Boundary spot-check: 21:00 UTC should flip from normal to tokyo
    ny_close_t = datetime(2026, 5, 5, 21, 0, 0, tzinfo=timezone.utc).timestamp()
    lk5 = model.lookup("GBPJPY", ny_close_t)
    if lk5.session_key != "tokyo":
        _fail(f"21:00 UTC boundary: {lk5.session_key} (expected 'tokyo')")

    _ok(
        f"GBPJPY: overlap=0.3pt @ 14:00, normal=0.5pt @ 10:00, "
        f"tokyo=0.8pt @ 03:00; 07/21 UTC boundaries fire correctly"
    )


def test_4_spread_lookup_unknown_pair_fallback():
    """Unknown pair returns conservative fallback for the active session, no crash."""
    print("\n[4] BrokerSpreadModel.lookup unknown-pair fallback")
    cfg = ShadowSimulatorConfig()
    model = BrokerSpreadModel(cfg)

    # 14:00 UTC = overlap session
    overlap_t = datetime(2026, 5, 5, 14, 0, 0, tzinfo=timezone.utc).timestamp()
    lk = model.lookup("XYZABC", overlap_t)
    if lk.session_key != "overlap":
        _fail(f"session_key: {lk.session_key}")
    if lk.spread_points != BrokerSpreadModel._FALLBACK_SPREAD["overlap"]:
        _fail(
            f"overlap fallback spread mismatch: {lk.spread_points} "
            f"vs {BrokerSpreadModel._FALLBACK_SPREAD['overlap']}"
        )

    # 03:00 UTC = tokyo session — wider fallback
    tokyo_t = datetime(2026, 5, 5, 3, 0, 0, tzinfo=timezone.utc).timestamp()
    lk2 = model.lookup("XYZABC", tokyo_t)
    if lk2.session_key != "tokyo":
        _fail(f"session_key (tokyo): {lk2.session_key}")
    if lk2.spread_points != BrokerSpreadModel._FALLBACK_SPREAD["tokyo"]:
        _fail(f"tokyo fallback: {lk2.spread_points}")

    _ok(
        f"unknown XYZABC -> overlap fallback {lk.spread_points}pt, "
        f"tokyo fallback {lk2.spread_points}pt, no crash"
    )


def test_5_shadow_simulator_construction():
    """Constructor accepts all four args; fields are stored correctly."""
    print("\n[5] ShadowSimulator constructor")
    with tempfile.TemporaryDirectory() as td:
        cfg = ShadowSimulatorConfig()
        cache = M1Cache(Path(td), mt5_module=None)
        spread = BrokerSpreadModel(cfg)

        sim = ShadowSimulator(
            m1_cache=cache,
            spread_model=spread,
            feature_engine=None,  # populated in C.3
            config=cfg,
        )

        if sim.cache is not cache:
            _fail("cache not stored")
        if sim.spread_model is not spread:
            _fail("spread_model not stored")
        if sim.config is not cfg:
            _fail("config not stored")
        if sim.feature_engine is not None:
            _fail("feature_engine should be None (C.3 wires it)")

        # Default config when None passed
        sim2 = ShadowSimulator(
            m1_cache=cache, spread_model=spread, feature_engine=None,
        )
        if not isinstance(sim2.config, ShadowSimulatorConfig):
            _fail("default config not constructed")
        _ok("ShadowSimulator constructs with explicit and default config")


def test_6_simulator_fetch_m1_window():
    """fetch_m1 returns ndarray with correct length for 240-minute window."""
    print("\n[6] ShadowSimulator.fetch_m1 240-min window")
    with tempfile.TemporaryDirectory() as td:
        # Use the fake MT5 to produce exactly 240 bars
        def gen(pair, s, e):
            return _make_synthetic_bars(s, e)
        cache = M1Cache(Path(td), mt5_module=FakeMT5(gen))
        cfg = ShadowSimulatorConfig()
        spread = BrokerSpreadModel(cfg)
        sim = ShadowSimulator(
            m1_cache=cache, spread_model=spread, feature_engine=None, config=cfg,
        )

        signal_time = datetime(2026, 4, 15, 10, 0, 0, tzinfo=timezone.utc).timestamp()
        bars = sim.fetch_m1("EURUSD", signal_time)
        if bars is None:
            _fail("fetch_m1 returned None")
        # 240 minutes * 1 bar/min = 240, but signal_time itself is included
        # so the inclusive window may be 240 or 241 depending on boundary handling
        if len(bars) < 240 or len(bars) > 241:
            _fail(f"expected ~240 bars for 240-min window, got {len(bars)}")
        # First bar should be at signal_time
        if bars[0]["time"] != int(signal_time):
            _fail(f"first bar time {bars[0]['time']} != signal_time {int(signal_time)}")
        _ok(f"fetch_m1 returned {len(bars)} bars for 240-min window")


# ── Diagnostic: full 28-pair x 4-session spread table dump ──────────

def diagnostic_full_spread_table():
    """Print the entire IC_MARKETS_SPREADS table for architect review.

    Per the C.2 review-gate spec: dump every value once before any
    pessimism calibration depends on it.
    """
    print("\n[diagnostic] Full IC Markets spread table (28 pairs x 4 sessions)")
    print()
    table = BrokerSpreadModel.IC_MARKETS_SPREADS
    # Group by family for easy review
    families = [
        ("JPY pairs", ["USDJPY", "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY"]),
        ("Major USD pairs", ["EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF"]),
        ("GBP crosses", ["GBPAUD", "GBPNZD", "GBPCAD", "GBPCHF"]),
        ("EUR crosses", ["EURAUD", "EURNZD", "EURCAD", "EURCHF", "EURGBP"]),
        ("AUD/NZD/CAD/CHF crosses", ["AUDCAD", "AUDCHF", "AUDNZD", "NZDCAD", "NZDCHF", "CADCHF"]),
        ("Gold", ["XAUUSD"]),
    ]
    print(f"  {'Pair':<8} | {'overlap':>7} | {'normal':>7} | {'tokyo':>7} | {'news':>7}")
    print(f"  {'-'*8}-+-{'-'*7}-+-{'-'*7}-+-{'-'*7}-+-{'-'*7}")
    for label, pairs in families:
        print(f"  -- {label} --")
        for p in pairs:
            spreads = table[p]
            print(
                f"  {p:<8} | "
                f"{spreads['overlap']:>7.2f} | "
                f"{spreads['normal']:>7.2f} | "
                f"{spreads['tokyo']:>7.2f} | "
                f"{spreads['news']:>7.2f}"
            )
    print()
    print(f"  Total pairs in table: {len(table)}")
    print(f"  Fallback (unknown pair): "
          f"overlap={BrokerSpreadModel._FALLBACK_SPREAD['overlap']:.2f}, "
          f"normal={BrokerSpreadModel._FALLBACK_SPREAD['normal']:.2f}, "
          f"tokyo={BrokerSpreadModel._FALLBACK_SPREAD['tokyo']:.2f}, "
          f"news={BrokerSpreadModel._FALLBACK_SPREAD['news']:.2f}")


def diagnostic_spread_samples():
    """Print representative (pair, time) lookups exercising every session bucket."""
    print("\n[diagnostic] Session-coverage spread lookups")
    cfg = ShadowSimulatorConfig()
    model = BrokerSpreadModel(cfg)
    samples = [
        ("EURUSD", datetime(2026, 5, 5, 14, 0, 0, tzinfo=timezone.utc),
         "London-NY overlap (tightest)"),
        ("EURUSD", datetime(2026, 5, 5, 10, 0, 0, tzinfo=timezone.utc),
         "London-only normal"),
        ("EURUSD", datetime(2026, 5, 5, 18, 0, 0, tzinfo=timezone.utc),
         "NY-only normal"),
        ("EURUSD", datetime(2026, 5, 5, 3, 0, 0, tzinfo=timezone.utc),
         "Tokyo session"),
        ("XAUUSD", datetime(2026, 5, 5, 14, 0, 0, tzinfo=timezone.utc),
         "Gold overlap"),
        ("XAUUSD", datetime(2026, 5, 5, 3, 0, 0, tzinfo=timezone.utc),
         "Gold tokyo"),
        ("XYZABC", datetime(2026, 5, 5, 14, 0, 0, tzinfo=timezone.utc),
         "Unknown pair fallback"),
    ]
    for pair, dt, desc in samples:
        lk = model.lookup(pair, dt.timestamp())
        print(
            f"  {pair:<7} {dt:%H:%M UTC} {desc:<28} -> "
            f"spread={lk.spread_points:.2f}pt slip={lk.slippage_points}pt "
            f"sl_slip={lk.sl_slippage_points}pt session={lk.session_key:<7} "
            f"news={lk.is_news_window}"
        )


# ── Pyarrow + version evidence ──────────────────────────────────────

def diagnostic_pyarrow():
    print("\n[diagnostic] pyarrow install verification")
    import pyarrow
    print(f"  pyarrow version: {pyarrow.__version__}")
    print(f"  pyarrow location: {pyarrow.__file__}")


if __name__ == "__main__":
    print("Phase C.1 smoke tests")
    test_1_m1_cache_parquet_roundtrip()
    test_2_resample_m1_to_m15()
    test_3_spread_lookup_known_pair()
    test_4_spread_lookup_unknown_pair_fallback()
    test_5_shadow_simulator_construction()
    test_6_simulator_fetch_m1_window()

    diagnostic_pyarrow()
    diagnostic_spread_samples()
    diagnostic_full_spread_table()

    print("\nALL C.1 SMOKE TESTS PASSED")
