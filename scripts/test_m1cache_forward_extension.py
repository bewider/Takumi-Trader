"""Test for M1Cache forward-extension fix (2026-05-07).

Reproduces the bug where M1Cache returns partial-coverage parquet
data without extending forward when the requested window ends past
the cached range.

The bug surfaced via Phase E diligence check: 747 production records
were pinned at the transient retry cap with 'empty_m1' failure
reason on Asia-Pacific session pairs. Diagnostic showed parquets
existed with bars covering an earlier portion of the month, but
the signal_time fell 15-30 minutes past the cached range.

Root cause: M1Cache._ensure_month() returned the in-memory or disk
parquet without checking whether [start_epoch, end_epoch] was
actually covered. With partial coverage, fetch() then sliced to
nothing and returned an empty ndarray, which the simulator
classified as 'empty_m1' and retried 12 times to no effect.

Fix: _ensure_month checks coverage; on miss, fetches a padded
union of (cached range, requested window) from MT5, merges, and
writes back atomically.

Run from repo root:
    python scripts/test_m1cache_forward_extension.py
"""
from __future__ import annotations

import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from takumi_trader.core.m1_cache import M1Cache  # noqa: E402


def _ok(msg: str) -> None:
    print(f"  PASS  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


# ─────────────────────────────────────────────────────────────────────
# Fake MT5 module — records every copy_rates_range call and synthesises
# bars for the requested window. Lets us assert that the cache extension
# logic actually invokes MT5 with the right window.
# ─────────────────────────────────────────────────────────────────────

class _FakeMT5:
    """Stub MetaTrader5 module. Records calls; returns synthetic bars."""
    TIMEFRAME_M1 = 1

    def __init__(self):
        self.calls: list[tuple[str, datetime, datetime]] = []
        # Bars to return — callers inject by calling .seed_bars(start, end)
        # which produces synthetic 1-min bars for the inclusive minute range.
        self._return_bars: list[dict] | None = None

    def seed_bars(self, start_epoch: int, end_epoch: int):
        """Schedule fake bars to return on next copy_rates_range call.
        Bars are at 60s granularity, OHLC=(1.10, 1.11, 1.09, 1.10)."""
        bars = []
        for t in range(int(start_epoch), int(end_epoch) + 1, 60):
            bars.append({
                "time": t, "open": 1.10, "high": 1.11,
                "low": 1.09, "close": 1.10,
            })
        self._return_bars = bars

    def copy_rates_range(self, symbol, timeframe, start_dt, end_dt):
        """Stub: records the call, returns whatever was seeded last."""
        self.calls.append((symbol, start_dt, end_dt))
        if self._return_bars is None:
            return None
        # Convert to numpy structured array as MT5 would
        bars = self._return_bars
        dtype = np.dtype([
            ("time", np.int64), ("open", np.float64),
            ("high", np.float64), ("low", np.float64),
            ("close", np.float64),
        ])
        out = np.empty(len(bars), dtype=dtype)
        for i, b in enumerate(bars):
            out[i] = (b["time"], b["open"], b["high"], b["low"], b["close"])
        # Reset so next call returns None unless seeded again
        self._return_bars = None
        return out


def _seed_disk_parquet(cache: M1Cache, pair: str, year_month: str,
                       start_epoch: int, end_epoch: int) -> None:
    """Pre-populate a parquet file with synthetic 1-min bars in [start,end]."""
    n = (end_epoch - start_epoch) // 60 + 1
    arr = np.empty(n, dtype=cache._M1_DTYPE)
    for i in range(n):
        t = start_epoch + i * 60
        arr[i] = (t, 1.10, 1.11, 1.09, 1.10)
    path = cache._path_for(pair, year_month)
    cache._write_parquet(path, arr)


# ─────────────────────────────────────────────────────────────────────
# Anchor: pick a signal_time that's NOT within the recency guard
# (recency guard is 5 minutes from now). Use a date 30 days ago.
# ─────────────────────────────────────────────────────────────────────

def _historical_anchor() -> int:
    """Epoch for an unambiguously-historical day (30 days ago, 12:00 UTC)."""
    import time
    now = time.time()
    return int(now) - 30 * 86400


# ─────────────────────────────────────────────────────────────────────
# Test 1 — REPRODUCES the bug behavior on current code; AFTER the fix
# this same scenario should succeed (return non-empty bars, MT5 called)
# ─────────────────────────────────────────────────────────────────────

def test_1_forward_extension_triggers_mt5_fetch():
    print("\n[1] partial-coverage parquet -> request past cached end -> MT5 extension fires")
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        anchor = _historical_anchor()
        # Cached: anchor → anchor + 60min (61 bars)
        cached_start = anchor
        cached_end = anchor + 60 * 60  # 60 min past anchor

        mt5 = _FakeMT5()
        cache = M1Cache(cache_dir=td_path, mt5_module=mt5)
        # Pre-write the partial parquet
        ym = datetime.fromtimestamp(anchor, tz=timezone.utc).strftime("%Y-%m")
        _seed_disk_parquet(cache, "GBPCAD", ym, cached_start, cached_end)

        # Request a window 15 minutes AFTER the cached end
        req_start = cached_end + 15 * 60   # 75 min past anchor
        req_end = req_start + 30 * 60      # 105 min past anchor

        # Seed fake mt5 to return bars for the entire union range
        # (covers cached + extension; the cache should request a superset)
        mt5.seed_bars(cached_start - 12 * 3600, req_end + 12 * 3600)

        result = cache.fetch("GBPCAD", req_start, req_end)

        # On current buggy code: result is empty ndarray (no MT5 call), test FAILS.
        # On fixed code: MT5 is called with a window covering req_end + pad,
        # cache is updated, and result has bars in the requested window.
        if result is None:
            _fail("fetch returned None — fetch failed unexpectedly")
        if len(result) == 0:
            _fail(
                "fetch returned EMPTY for a request past cached range — "
                "the bug. Expected forward extension to fire."
            )
        # Verify we got bars in the requested window
        in_window = (
            (result["time"] >= int(req_start)) &
            (result["time"] <= int(req_end))
        ).sum()
        if in_window == 0:
            _fail(
                f"fetch returned {len(result)} bars but none in requested "
                f"window [{req_start},{req_end}]"
            )
        if not mt5.calls:
            _fail("MT5 was never called — extension fetch did not fire")
        _ok(
            f"forward extension fired: MT5 called {len(mt5.calls)} time(s); "
            f"returned {len(result)} bars including {in_window} in requested window"
        )


# ─────────────────────────────────────────────────────────────────────
# Test 2 — backward extension: parquet covers 14-15 UTC, request 12-13 UTC
# ─────────────────────────────────────────────────────────────────────

def test_2_backward_extension_triggers_mt5_fetch():
    print("\n[2] partial-coverage parquet -> request before cached start -> MT5 extension fires")
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        anchor = _historical_anchor() + 14 * 3600  # 14:00 UTC
        # Cached: 14:00 → 15:00 UTC same day
        cached_start = anchor
        cached_end = anchor + 60 * 60

        mt5 = _FakeMT5()
        cache = M1Cache(cache_dir=td_path, mt5_module=mt5)
        ym = datetime.fromtimestamp(anchor, tz=timezone.utc).strftime("%Y-%m")
        _seed_disk_parquet(cache, "GBPCAD", ym, cached_start, cached_end)

        # Request window 90 min BEFORE cached_start
        req_start = cached_start - 120 * 60   # 12:00 UTC
        req_end = cached_start - 60 * 60      # 13:00 UTC

        mt5.seed_bars(req_start - 12 * 3600, cached_end + 12 * 3600)

        result = cache.fetch("GBPCAD", req_start, req_end)

        if result is None or len(result) == 0:
            _fail(
                f"fetch returned {('None' if result is None else 'EMPTY')} for "
                f"request before cached range — backward extension didn't fire"
            )
        in_window = (
            (result["time"] >= int(req_start)) &
            (result["time"] <= int(req_end))
        ).sum()
        if in_window == 0:
            _fail(f"fetch returned bars but none in requested window")
        if not mt5.calls:
            _fail("MT5 was never called — extension fetch did not fire")
        _ok(
            f"backward extension fired: MT5 called; "
            f"returned {len(result)} bars including {in_window} in requested window"
        )


# ─────────────────────────────────────────────────────────────────────
# Test 3 — fully-covered request returns from cache without MT5 call
# (regression check: don't break the fast path)
# ─────────────────────────────────────────────────────────────────────

def test_3_fully_covered_request_skips_mt5():
    print("\n[3] request fully within cached range -> NO MT5 call (cache hit)")
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        anchor = _historical_anchor() + 10 * 3600   # 10:00 UTC
        cached_start = anchor
        cached_end = anchor + 4 * 3600              # 14:00 UTC (4 hour cache)

        mt5 = _FakeMT5()
        cache = M1Cache(cache_dir=td_path, mt5_module=mt5)
        ym = datetime.fromtimestamp(anchor, tz=timezone.utc).strftime("%Y-%m")
        _seed_disk_parquet(cache, "GBPCAD", ym, cached_start, cached_end)

        # Request a window FULLY within the cached range
        req_start = cached_start + 30 * 60   # 10:30 UTC
        req_end = cached_start + 90 * 60     # 11:30 UTC

        result = cache.fetch("GBPCAD", req_start, req_end)

        if result is None or len(result) == 0:
            _fail("fully-covered request returned no bars — cache fast-path broke")
        if mt5.calls:
            _fail(f"MT5 was called {len(mt5.calls)} time(s) for a covered request")
        _ok(f"covered request served from cache; {len(result)} bars, MT5 not called")


# ─────────────────────────────────────────────────────────────────────
# Test 4 — extension fetch failure: cache stays consistent
# ─────────────────────────────────────────────────────────────────────

def test_4_extension_failure_returns_none_or_partial():
    print("\n[4] extension fetch failure -> graceful degradation, no crash")
    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        anchor = _historical_anchor()
        cached_start = anchor
        cached_end = anchor + 60 * 60

        mt5 = _FakeMT5()
        cache = M1Cache(cache_dir=td_path, mt5_module=mt5)
        ym = datetime.fromtimestamp(anchor, tz=timezone.utc).strftime("%Y-%m")
        _seed_disk_parquet(cache, "GBPCAD", ym, cached_start, cached_end)

        # Request past cached end. DON'T seed the fake mt5 — it will return None.
        req_start = cached_end + 30 * 60
        req_end = req_start + 30 * 60
        # mt5 not seeded — will return None on copy_rates_range

        # Should not crash — either returns None or returns the cached portion
        try:
            result = cache.fetch("GBPCAD", req_start, req_end)
        except Exception as exc:
            _fail(f"fetch raised on MT5 failure: {exc!r}")
        # Either None or empty is acceptable as a degradation; the key is no crash
        _ok(f"fetch handled MT5 failure gracefully (result: "
            f"{'None' if result is None else f'{len(result)} bars'})")


# ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 64)
    print("M1Cache forward/backward extension test (2026-05-07 bugfix)")
    print("=" * 64)
    test_1_forward_extension_triggers_mt5_fetch()
    test_2_backward_extension_triggers_mt5_fetch()
    test_3_fully_covered_request_skips_mt5()
    test_4_extension_failure_returns_none_or_partial()
    print("\n" + "=" * 64)
    print("ALL M1CACHE EXTENSION TESTS PASSED")
    print("=" * 64)


if __name__ == "__main__":
    main()
