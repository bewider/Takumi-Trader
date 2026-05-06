"""Dukascopy Historical Data Downloader — free M1 candle data.

Downloads M1 OHLCV candle data from Dukascopy's public data feed.
Data is available from 2003 onwards, no account required.

Binary .bi5 format (LZMA compressed):
  Each record = 24 bytes:
    - uint32: time offset (seconds from start of day)
    - uint32: open  (price × multiplier)
    - uint32: high
    - uint32: low
    - uint32: close
    - float32: volume

Multiplier: 100_000 for 5-digit pairs, 1_000 for 3-digit (JPY) pairs.

Usage:
    downloader = DukascopyDownloader(data_dir=Path("data/dukascopy"))
    downloader.download_pair("EURUSD", date(2025, 1, 3), date(2025, 3, 21),
                             progress_callback=my_cb)
"""

from __future__ import annotations

import io
import lzma
import logging
import struct
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable

import numpy as np

logger = logging.getLogger(__name__)

# Dukascopy data feed base URL
_BASE_URL = "https://datafeed.dukascopy.com/datafeed"

# Price multiplier: JPY pairs use 1000, everything else 100000
_JPY_PAIRS = {
    "USDJPY", "EURJPY", "GBPJPY", "AUDJPY", "NZDJPY", "CADJPY", "CHFJPY",
}

# All 28 pairs we trade
ALL_28_PAIRS = [
    "EURUSD", "GBPUSD", "AUDUSD", "NZDUSD",
    "USDCAD", "USDCHF", "USDJPY",
    "EURGBP", "EURAUD", "EURNZD", "EURCAD", "EURCHF", "EURJPY",
    "GBPAUD", "GBPNZD", "GBPCAD", "GBPCHF", "GBPJPY",
    "AUDNZD", "AUDCAD", "AUDCHF", "AUDJPY",
    "NZDCAD", "NZDCHF", "NZDJPY",
    "CADCHF", "CADJPY",
    "CHFJPY",
]

# Struct format for one M1 candle record (24 bytes)
_RECORD_FMT = ">IIIIIf"  # big-endian: 5 uint32 + 1 float32
_RECORD_SIZE = struct.calcsize(_RECORD_FMT)  # 24 bytes


def _price_multiplier(pair: str) -> float:
    """Get price multiplier for decoding Dukascopy integer prices."""
    return 1_000.0 if pair in _JPY_PAIRS else 100_000.0


def _parse_bi5(data: bytes, pair: str, day: date) -> list[tuple]:
    """Parse a decompressed .bi5 binary blob into M1 candle records.

    Returns list of (unix_time, open, high, low, close, volume).
    """
    import calendar
    mult = _price_multiplier(pair)
    # Use UTC midnight to avoid local timezone issues
    day_start_ts = calendar.timegm((day.year, day.month, day.day, 0, 0, 0))

    records = []
    offset = 0
    while offset + _RECORD_SIZE <= len(data):
        time_off, o, h, l, c, vol = struct.unpack_from(_RECORD_FMT, data, offset)
        offset += _RECORD_SIZE

        # Skip empty candles
        if o == 0 and h == 0 and l == 0 and c == 0:
            continue

        unix_ts = day_start_ts + time_off
        records.append((
            unix_ts,
            o / mult,
            h / mult,
            l / mult,
            c / mult,
            vol,
        ))

    return records


class DukascopyDownloader:
    """Downloads and stores Dukascopy M1 candle data."""

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def get_parquet_path(self, pair: str) -> Path:
        """Get the Parquet file path for a pair."""
        return self.data_dir / f"{pair}_M1.parquet"

    def has_data(self, pair: str) -> bool:
        """Check if we have local data for a pair."""
        return self.get_parquet_path(pair).exists()

    def get_date_range(self, pair: str) -> tuple[date, date] | None:
        """Get the date range of stored data for a pair."""
        path = self.get_parquet_path(pair)
        if not path.exists():
            return None
        try:
            import pyarrow.parquet as pq
            table = pq.read_table(path, columns=["time"])
            times = table.column("time").to_pylist()
            if not times:
                return None
            return (
                datetime.utcfromtimestamp(min(times)).date(),
                datetime.utcfromtimestamp(max(times)).date(),
            )
        except Exception:
            return None

    def get_existing_dates(self, pair: str) -> set[date]:
        """Return the set of dates already stored locally for a pair."""
        path = self.get_parquet_path(pair)
        if not path.exists():
            return set()
        try:
            import pyarrow.parquet as pq
            table = pq.read_table(path, columns=["time"])
            times = table.column("time").to_pylist()
            return {datetime.utcfromtimestamp(t).date() for t in times}
        except Exception:
            return set()

    def download_pair(
        self,
        pair: str,
        start: date,
        end: date,
        progress_callback: Callable[[str, int, int], None] | None = None,
        skip_existing: bool = True,
    ) -> int:
        """Download M1 data for a single pair from Dukascopy.

        Args:
            pair: Currency pair (e.g. "EURUSD")
            start: Start date (inclusive)
            end: End date (inclusive)
            progress_callback: Optional callback(pair, current_day, total_days)
            skip_existing: If True, skip dates already present in local data.

        Returns:
            Number of M1 candles downloaded.
        """
        import urllib.request
        import urllib.error

        # Determine which dates we can skip
        existing_dates = self.get_existing_dates(pair) if skip_existing else set()
        if existing_dates:
            logger.info(
                "%s: %d dates already cached, downloading only missing days",
                pair, len(existing_dates),
            )

        all_records: list[tuple] = []
        current = start
        total_days = (end - start).days + 1
        day_count = 0
        skipped = 0
        errors = 0

        while current <= end:
            day_count += 1

            # Skip weekends (Sat=5, Sun=6)
            if current.weekday() >= 5:
                current += timedelta(days=1)
                continue

            # Skip dates already in local data
            if current in existing_dates:
                skipped += 1
                if progress_callback:
                    progress_callback(pair, day_count, total_days)
                current += timedelta(days=1)
                continue

            # Dukascopy URL: month is 0-indexed!
            url = (
                f"{_BASE_URL}/{pair}/"
                f"{current.year}/{current.month - 1:02d}/{current.day:02d}/"
                f"BID_candles_min_1.bi5"
            )

            try:
                req = urllib.request.Request(url)
                req.add_header("User-Agent", "Mozilla/5.0 (TAKUMITrader)")
                with urllib.request.urlopen(req, timeout=30) as resp:
                    compressed = resp.read()

                if len(compressed) > 0:
                    try:
                        raw = lzma.decompress(compressed)
                        records = _parse_bi5(raw, pair, current)
                        all_records.extend(records)
                    except lzma.LZMAError:
                        # Some days may have empty/invalid data
                        logger.debug("LZMA decode error for %s %s", pair, current)
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    # No data for this day (holiday, etc.)
                    pass
                else:
                    errors += 1
                    logger.warning("HTTP %d for %s %s", e.code, pair, current)
            except Exception as e:
                errors += 1
                logger.warning("Error downloading %s %s: %s", pair, current, e)

            if progress_callback:
                progress_callback(pair, day_count, total_days)

            # Rate limiting: be polite to Dukascopy servers
            time.sleep(0.05)

            current += timedelta(days=1)

        if not all_records:
            if skipped > 0:
                logger.info("%s: all dates already cached (%d skipped)", pair, skipped)
            else:
                logger.warning("No data downloaded for %s", pair)
            return 0

        # Convert to numpy structured array and save as Parquet
        n = len(all_records)
        logger.info(
            "Downloaded %d M1 candles for %s (%d skipped, %d errors)",
            n, pair, skipped, errors,
        )

        self._save_parquet(pair, all_records)
        return n

    def _save_parquet(self, pair: str, records: list[tuple]) -> None:
        """Save M1 records to Parquet file, merging with existing data."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create new table
        times, opens, highs, lows, closes, volumes = zip(*records)
        new_table = pa.table({
            "time": pa.array(times, type=pa.int64()),
            "open": pa.array(opens, type=pa.float64()),
            "high": pa.array(highs, type=pa.float64()),
            "low": pa.array(lows, type=pa.float64()),
            "close": pa.array(closes, type=pa.float64()),
            "volume": pa.array(volumes, type=pa.float64()),
        })

        path = self.get_parquet_path(pair)

        # Merge with existing data if present
        if path.exists():
            try:
                existing = pq.read_table(path)
                combined = pa.concat_tables([existing, new_table])
                # Deduplicate by time using pyarrow (memory-efficient, no Python lists)
                time_col = combined.column("time")
                sort_idx = pa.compute.sort_indices(combined, sort_keys=[("time", "ascending")])
                combined = combined.take(sort_idx)
                # Remove consecutive duplicates (sorted, so dups are adjacent)
                sorted_times = combined.column("time")
                if len(sorted_times) > 1:
                    prev = sorted_times.slice(0, len(sorted_times) - 1)
                    curr = sorted_times.slice(1)
                    not_dup = pa.compute.not_equal(prev, curr)
                    # Keep first row always + rows where time differs from previous
                    keep_mask = pa.concat_arrays([
                        pa.array([True]),
                        not_dup,
                    ])
                    combined = combined.filter(keep_mask)
                new_table = combined
            except Exception as e:
                logger.warning("Error merging existing data for %s: %s", pair, e)

        pq.write_table(new_table, path, compression="snappy")
        logger.info("Saved %d candles to %s", len(new_table), path.name)

    def download_all(
        self,
        start: date,
        end: date,
        pairs: list[str] | None = None,
        progress_callback: Callable[[str, int, int, int, int], None] | None = None,
    ) -> dict[str, int]:
        """Download M1 data for all pairs.

        Args:
            start: Start date
            end: End date
            pairs: List of pairs to download (default: ALL_28_PAIRS)
            progress_callback: callback(pair, pair_idx, total_pairs, day, total_days)

        Returns:
            Dict of {pair: candle_count}
        """
        if pairs is None:
            pairs = ALL_28_PAIRS

        results: dict[str, int] = {}
        total_pairs = len(pairs)

        for pair_idx, pair in enumerate(pairs):
            def _pair_progress(p: str, day: int, total_days: int) -> None:
                if progress_callback:
                    progress_callback(p, pair_idx + 1, total_pairs, day, total_days)

            count = self.download_pair(pair, start, end, progress_callback=_pair_progress)
            results[pair] = count

        return results

    def load_pair(self, pair: str) -> np.ndarray | None:
        """Load M1 data for a pair from Parquet as a numpy structured array.

        Returns array compatible with MT5's copy_rates format:
            dtype: time(i8), open(f8), high(f8), low(f8), close(f8),
                   tick_volume(i8), spread(i4), real_volume(i8)
        """
        path = self.get_parquet_path(pair)
        if not path.exists():
            return None

        try:
            import pyarrow.parquet as pq
            table = pq.read_table(path)

            n = len(table)
            if n == 0:
                return None

            # Build structured array matching MT5 format
            dt = np.dtype([
                ("time", "i8"), ("open", "f8"), ("high", "f8"),
                ("low", "f8"), ("close", "f8"), ("tick_volume", "i8"),
                ("spread", "i4"), ("real_volume", "i8"),
            ])
            arr = np.zeros(n, dtype=dt)
            arr["time"] = table.column("time").to_numpy()
            arr["open"] = table.column("open").to_numpy()
            arr["high"] = table.column("high").to_numpy()
            arr["low"] = table.column("low").to_numpy()
            arr["close"] = table.column("close").to_numpy()
            arr["tick_volume"] = table.column("volume").to_numpy().astype("i8")
            arr["spread"] = 0
            arr["real_volume"] = 0

            return arr
        except Exception as e:
            logger.error("Error loading %s: %s", path, e)
            return None

    def load_pair_range(
        self, pair: str, start: date, end: date
    ) -> np.ndarray | None:
        """Load M1 data for a specific date range."""
        arr = self.load_pair(pair)
        if arr is None:
            return None

        start_ts = int(datetime(start.year, start.month, start.day).timestamp())
        end_ts = int(datetime(end.year, end.month, end.day, 23, 59, 59).timestamp())

        mask = (arr["time"] >= start_ts) & (arr["time"] <= end_ts)
        filtered = arr[mask]

        return filtered if len(filtered) > 0 else None
