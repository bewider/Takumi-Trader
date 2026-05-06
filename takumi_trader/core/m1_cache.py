"""M1Cache — parquet-backed M1 OHLC cache with lazy MT5 fill (Phase C.1).

Design rationale:

    * One file per pair per month: data/m1_cache/<pair>/<YYYY-MM>.parquet
      Per-day files would create thousands of small parquet files (slow
      to enumerate, weak compression). Per-quarter files would cause
      partial-month reads to load too much. Per-month is the sweet spot
      for the typical Edge Miner query window (signal_time +/- 4 hours).

    * Schema: time:int64 (epoch seconds), open/high/low/close:float64.
      Numeric only, no metadata bloat. tick_volume / spread / etc. would
      add 30-40% file size for fields neither simulator nor Edge Miner
      will use in Phase 1.

    * Atomic parquet writes via tmp + os.replace. Same atomicity guarantee
      as ShadowLogger's JSON writes — a crash mid-write cannot leave a
      partial / corrupt parquet on disk.

    * Lazy fill: on miss, fetch a 24h-padded window from MT5 (12h before,
      12h after the requested span). Amortizes MT5 round-trip cost across
      future requests in the same window.

    * In-memory cache: per (pair, year_month), parsed DataFrame is kept
      around for the lifetime of the M1Cache instance. ShadowSimWorker's
      5-min batch typically processes records clustered in time, so the
      same monthly file gets hit repeatedly; in-memory caching avoids
      re-parsing the parquet on every fetch.

    * Resampled M1 -> M15/H1/H4 cached in-memory only. Spec decision:
      don't persist resampled bars; they're cheap to recompute (4 numpy
      ops per resample) and the persisted M1 is the source of truth.
      The resample cache lives within a single simulator session;
      Phase D's worker constructs one M1Cache per cycle.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


# Padding around miss-fill MT5 fetches — half the configured value
# applied symmetrically on each side.
_MT5_FETCH_PAD_SECONDS = 12 * 3600  # 12 hours

# Minimum gap between signal_time and "now" for which we expect MT5
# to have written the M1 bar. Fetches within this window may legitimately
# return None ("data too recent"), which the simulator treats as FAILED
# until the next worker cycle retries.
_RECENCY_GUARD_SECONDS = 5 * 60  # 5 minutes


class M1Cache:
    """M1 OHLC cache. Parquet on disk, lazy MT5 fill, in-memory parse cache.

    Construct one instance per ShadowSimWorker cycle. The instance owns
    its in-memory caches; constructing fresh per-cycle keeps memory
    bounded and ensures a long-running worker doesn't accumulate stale
    parsed DataFrames.
    """

    # M1 bar dtype written into parquet. Edge Miner can read this same
    # schema directly via pandas / polars / pyarrow.
    _M1_DTYPE = np.dtype([
        ("time", np.int64),
        ("open", np.float64),
        ("high", np.float64),
        ("low", np.float64),
        ("close", np.float64),
    ])

    def __init__(
        self,
        cache_dir: Path,
        mt5_module=None,
    ) -> None:
        """Construct the cache.

        Args:
            cache_dir: where pair subdirectories live, typically
                `data/m1_cache/`. Created on first write.
            mt5_module: imported MetaTrader5 module. None disables lazy
                MT5 fill (cache becomes read-only — useful for tests).
        """
        self.cache_dir = Path(cache_dir)
        self.mt5 = mt5_module
        # In-memory parse cache: (pair, year_month) -> structured ndarray
        self._parsed: dict[tuple[str, str], np.ndarray] = {}
        # Resampled bars cache: (pair, year_month, target_minutes) -> ndarray
        self._resampled: dict[tuple[str, str, int], np.ndarray] = {}

    # ── Path helpers ────────────────────────────────────────────────

    def _path_for(self, pair: str, year_month: str) -> Path:
        return self.cache_dir / pair / f"{year_month}.parquet"

    @staticmethod
    def _months_covered(start_epoch: float, end_epoch: float) -> list[str]:
        """Yield 'YYYY-MM' strings for every month touching [start, end]."""
        if end_epoch < start_epoch:
            return []
        start_dt = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_epoch, tz=timezone.utc)
        months: list[str] = []
        cur = datetime(start_dt.year, start_dt.month, 1, tzinfo=timezone.utc)
        end_marker = datetime(end_dt.year, end_dt.month, 1, tzinfo=timezone.utc)
        while cur <= end_marker:
            months.append(f"{cur.year:04d}-{cur.month:02d}")
            # Increment month
            if cur.month == 12:
                cur = datetime(cur.year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                cur = datetime(cur.year, cur.month + 1, 1, tzinfo=timezone.utc)
        return months

    # ── Public API ──────────────────────────────────────────────────

    def fetch(
        self,
        pair: str,
        start_epoch: float,
        end_epoch: float,
    ) -> np.ndarray | None:
        """Return M1 bars in [start, end] window as a structured ndarray.

        Returns None if data is unavailable — typically because:
            * signal_time is too recent (within the 5-min recency guard
              and MT5 hasn't yet provided the bar)
            * the requested window pre-dates MT5's available history
            * MT5 is disconnected and the cache miss can't be filled

        On success: ndarray with M1Cache._M1_DTYPE shape, sorted by time.
        Empty windows return a zero-length ndarray, NOT None — None
        means "couldn't determine", empty array means "no bars in range".
        """
        if end_epoch < start_epoch:
            return np.empty(0, dtype=self._M1_DTYPE)

        # Recency guard: signals from the very recent past may legitimately
        # have no M1 bar yet. Caller should retry on the next worker cycle.
        now = datetime.now(timezone.utc).timestamp()
        if start_epoch >= now - _RECENCY_GUARD_SECONDS:
            return None

        months = self._months_covered(start_epoch, end_epoch)
        chunks: list[np.ndarray] = []
        for ym in months:
            chunk = self._ensure_month(pair, ym, start_epoch, end_epoch)
            if chunk is None:
                # MT5 disconnected or fetch failed — propagate None
                return None
            if len(chunk) > 0:
                chunks.append(chunk)

        if not chunks:
            return np.empty(0, dtype=self._M1_DTYPE)

        merged = np.concatenate(chunks)
        # Slice to exact window
        mask = (merged["time"] >= int(start_epoch)) & (
            merged["time"] <= int(end_epoch)
        )
        sliced = merged[mask]
        return sliced

    def resample(
        self,
        pair: str,
        year_month: str,
        m1_bars: np.ndarray,
        target_minutes: int,
    ) -> np.ndarray:
        """Resample M1 bars to a coarser TF (M15, H1, H4 etc.).

        In-memory cached per (pair, year_month, target_minutes) for the
        lifetime of this M1Cache instance. The resampled bars do NOT
        persist to disk — they're cheap to recompute and would
        duplicate the source-of-truth M1 storage.

        OHLC aggregation: open=first.open, high=max(highs), low=min(lows),
        close=last.close. time = first M1 time of the bucket.
        """
        cache_key = (pair, year_month, target_minutes)
        if cache_key in self._resampled:
            return self._resampled[cache_key]

        if len(m1_bars) == 0:
            empty = np.empty(0, dtype=self._M1_DTYPE)
            self._resampled[cache_key] = empty
            return empty

        bucket_seconds = target_minutes * 60
        # Floor each M1 bar's time to its bucket boundary
        buckets = (m1_bars["time"] // bucket_seconds) * bucket_seconds
        unique_buckets, idx_starts = np.unique(buckets, return_index=True)
        idx_starts = np.append(idx_starts, len(m1_bars))

        out = np.empty(len(unique_buckets), dtype=self._M1_DTYPE)
        for i, bucket_start in enumerate(unique_buckets):
            sl = slice(idx_starts[i], idx_starts[i + 1])
            chunk = m1_bars[sl]
            out[i]["time"] = int(bucket_start)
            out[i]["open"] = chunk[0]["open"]
            out[i]["high"] = float(np.max(chunk["high"]))
            out[i]["low"] = float(np.min(chunk["low"]))
            out[i]["close"] = chunk[-1]["close"]

        self._resampled[cache_key] = out
        return out

    # ── Internal: per-month ensure-loaded ───────────────────────────

    def _ensure_month(
        self,
        pair: str,
        year_month: str,
        start_epoch: float,
        end_epoch: float,
    ) -> np.ndarray | None:
        """Return the requested month's M1 data as ndarray, ensuring the
        requested [start_epoch, end_epoch] window is covered.

        Coverage-aware (2026-05-07 bugfix): the prior implementation
        returned in-memory or disk-cached data without checking whether
        the cached range actually covered the request. With partial
        coverage (e.g., parquet has 00:00-14:50 UTC but request is
        15:05 UTC), the caller's slice produced an empty ndarray, the
        simulator classified it as 'empty_m1', and the worker's retry
        loop burned 12 cycles before giving up. Found via Phase E
        diligence check on 2026-05-07; 747 records were pinned at the
        retry cap with this exact failure shape.

        Resolution order:
            1. Load existing data (memory cache → disk parquet)
            2. If no data OR existing data does NOT cover the request,
               fetch a padded UNION of (existing range, requested range)
               from MT5, replace cache + parquet, return.
            3. If MT5 unavailable and existing data partial: return
               what we have (caller may still get useful slices).
            4. If MT5 unavailable and no existing data: return None.

        Empty months (parquet exists, request fully covered, no rows
        in window) still return zero-length array.
        """
        cache_key = (pair, year_month)

        # Step 1: load whatever we have (memory or disk)
        existing: np.ndarray | None = self._parsed.get(cache_key)
        if existing is None:
            path = self._path_for(pair, year_month)
            if path.exists():
                try:
                    existing = self._read_parquet(path)
                    self._parsed[cache_key] = existing
                except Exception as exc:
                    logger.warning(
                        "[M1CACHE] failed to read %s: %s — falling back to MT5",
                        path, exc,
                    )
                    existing = None

        # Step 2: coverage check
        if existing is not None and len(existing) > 0:
            cached_min = int(existing["time"].min())
            cached_max = int(existing["time"].max())
            if int(start_epoch) >= cached_min and int(end_epoch) <= cached_max:
                # Fully covered — fast path
                return existing

        # Need MT5 to fetch (or extend coverage)
        if self.mt5 is None:
            if existing is not None:
                # Partial coverage but no MT5 — return best effort
                return existing
            logger.warning(
                "[M1CACHE] miss for %s/%s and mt5 disabled", pair, year_month,
            )
            return None

        # Compute the fetch window: union of existing range and requested
        # range, padded by 12h each side, clipped to the calendar month.
        # Fetching the union (rather than just the missing portion) keeps
        # the parquet write atomic and avoids merge-dedupe complexity.
        if existing is not None and len(existing) > 0:
            union_start = min(int(existing["time"].min()), int(start_epoch))
            union_end = max(int(existing["time"].max()), int(end_epoch))
        else:
            union_start = int(start_epoch)
            union_end = int(end_epoch)

        fetch_start, fetch_end = self._compute_fetch_window(
            year_month, union_start, union_end,
        )
        df = self._fetch_from_mt5(pair, fetch_start, fetch_end)
        if df is None:
            # MT5 fetch failed. If we had partial cached data, serve it
            # rather than returning None — the caller may still get a
            # useful slice if the request happens to overlap.
            return existing

        # Replace cache atomically
        path = self._path_for(pair, year_month)
        try:
            self._write_parquet(path, df)
        except Exception as exc:
            logger.warning(
                "[M1CACHE] failed to write %s: %s (data still served from memory)",
                path, exc,
            )

        self._parsed[cache_key] = df
        return df

    @staticmethod
    def _compute_fetch_window(
        year_month: str,
        start_epoch: float,
        end_epoch: float,
    ) -> tuple[float, float]:
        """Return (fetch_start, fetch_end) clipped to the calendar month."""
        year, month = map(int, year_month.split("-"))
        month_start = datetime(year, month, 1, tzinfo=timezone.utc).timestamp()
        if month == 12:
            month_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc).timestamp()
        else:
            month_end = datetime(year, month + 1, 1, tzinfo=timezone.utc).timestamp()
        # 12h pad each side, clipped to month boundaries
        fetch_start = max(month_start, start_epoch - _MT5_FETCH_PAD_SECONDS)
        fetch_end = min(month_end, end_epoch + _MT5_FETCH_PAD_SECONDS)
        return fetch_start, fetch_end

    # ── MT5 + parquet I/O ───────────────────────────────────────────

    def _fetch_from_mt5(
        self,
        pair: str,
        start_epoch: float,
        end_epoch: float,
    ) -> np.ndarray | None:
        """Pull M1 bars from MT5 for the given UTC window.

        Returns ndarray with our schema, or None on failure. Retries
        once on transient errors before giving up.
        """
        if self.mt5 is None:
            return None
        for attempt in (1, 2):
            try:
                rates = self.mt5.copy_rates_range(
                    pair,
                    self.mt5.TIMEFRAME_M1,
                    datetime.fromtimestamp(start_epoch, tz=timezone.utc),
                    datetime.fromtimestamp(end_epoch, tz=timezone.utc),
                )
                if rates is None or len(rates) == 0:
                    logger.warning(
                        "[M1CACHE] MT5 returned no bars for %s [%s, %s]",
                        pair, start_epoch, end_epoch,
                    )
                    if attempt == 2:
                        return None
                    continue
                # Translate MT5 named-tuple rate array to our schema
                out = np.empty(len(rates), dtype=self._M1_DTYPE)
                out["time"] = rates["time"]
                out["open"] = rates["open"]
                out["high"] = rates["high"]
                out["low"] = rates["low"]
                out["close"] = rates["close"]
                return out
            except Exception as exc:
                logger.warning(
                    "[M1CACHE] mt5.copy_rates_range attempt %d failed: %s",
                    attempt, exc,
                )
        return None

    @classmethod
    def _read_parquet(cls, path: Path) -> np.ndarray:
        """Read a parquet file and return our structured ndarray.

        Uses pyarrow's native -> numpy path (no pandas detour). pandas is
        NOT a project dependency, and adding it for the sole purpose of
        a parquet -> ndarray conversion would bloat install size by
        ~50 MB. pyarrow's column.to_numpy() does the same job with one
        less dependency.
        """
        import pyarrow.parquet as pq
        table = pq.read_table(path)
        n = table.num_rows
        out = np.empty(n, dtype=cls._M1_DTYPE)
        out["time"] = table.column("time").to_numpy().astype(np.int64)
        out["open"] = table.column("open").to_numpy().astype(np.float64)
        out["high"] = table.column("high").to_numpy().astype(np.float64)
        out["low"] = table.column("low").to_numpy().astype(np.float64)
        out["close"] = table.column("close").to_numpy().astype(np.float64)
        return out

    @classmethod
    def _write_parquet(cls, path: Path, arr: np.ndarray) -> None:
        """Atomic parquet write: tmp file, rename. Same pattern as
        ShadowLogger's atomic JSON writes — crash mid-write cannot
        produce a corrupt or partial parquet."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        table = pa.table({
            "time": pa.array(arr["time"], type=pa.int64()),
            "open": pa.array(arr["open"], type=pa.float64()),
            "high": pa.array(arr["high"], type=pa.float64()),
            "low": pa.array(arr["low"], type=pa.float64()),
            "close": pa.array(arr["close"], type=pa.float64()),
        })
        pq.write_table(table, tmp, compression="snappy")
        os.replace(tmp, path)
