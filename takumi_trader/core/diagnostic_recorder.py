"""Diagnostic Recorder — per-minute algo state logging.

Every minute, records a CSV row with scores, thresholds, filter results,
and trade status for ALL 27 display pairs. Used for post-analysis to
understand why the algo did or didn't take a trade at any given moment.

Output: DATA/diagnostics/YYYY-MM-DD_algo_state.csv
"""

import csv
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from takumi_trader.core.strength import (
    CalculationResult,
    DISPLAY_PAIRS,
    CURRENCIES,
    TIMEFRAME_LABELS,
)

logger = logging.getLogger(__name__)

# Throttle: one record per 55 seconds (slightly under 1 min to catch each minute)
_MIN_INTERVAL = 55.0


class DiagnosticRecorder:
    """Records per-minute algo state to CSV for post-analysis."""

    def __init__(self) -> None:
        self._last_record_time: float = 0.0
        self._file_date: str = ""
        self._writer: csv.DictWriter | None = None
        self._file = None
        self._header_written = False

    def _ensure_file(self) -> bool:
        """Open or rotate the CSV file for today's date."""
        today = datetime.now().strftime("%Y-%m-%d")
        if today == self._file_date and self._writer is not None:
            return True

        # Close old file
        if self._file:
            try:
                self._file.close()
            except Exception:
                pass

        # Create new file
        try:
            import sys
            if getattr(sys, 'frozen', False):
                base = Path(sys.executable).parent / "data" / "diagnostics"
            else:
                base = Path(__file__).resolve().parent.parent.parent / "data" / "diagnostics"
            base.mkdir(parents=True, exist_ok=True)

            filepath = base / f"{today}_algo_state.csv"
            file_exists = filepath.exists() and filepath.stat().st_size > 0

            self._file = open(filepath, "a", newline="", encoding="utf-8")
            self._file_date = today
            self._header_written = file_exists
            self._writer = None  # Will create with fieldnames on first record
            return True
        except Exception as e:
            logger.warning("Diagnostic file error: %s", e)
            return False

    def record(
        self,
        result: CalculationResult,
        open_pairs: set[str] | None = None,
        session: str = "",
    ) -> None:
        """Record current algo state if enough time has passed."""
        now = time.time()
        if now - self._last_record_time < _MIN_INTERVAL:
            return

        if not self._ensure_file():
            return

        self._last_record_time = now
        now_dt = datetime.now()

        try:
            row: dict[str, Any] = {
                "timestamp": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "session": session or result.session_label,
            }

            # Per-currency scores for each TF
            for ccy in CURRENCIES:
                for tf in TIMEFRAME_LABELS:
                    tr = result.timeframes.get(tf)
                    if tr and ccy in tr.currency_scores:
                        row[f"{ccy}_{tf}"] = round(tr.currency_scores[ccy], 2)
                    else:
                        row[f"{ccy}_{tf}"] = ""

                # Composite scores
                row[f"{ccy}_composite"] = round(result.composite_scores.get(ccy, 0), 2)
                row[f"{ccy}_htf_composite"] = round(result.htf_composite_scores.get(ccy, 0), 2)

                # Momentum
                phase = result.momentum_phases.get(ccy)
                if phase:
                    row[f"{ccy}_velocity"] = round(phase.velocity, 3)
                    row[f"{ccy}_accel_mag"] = phase.accel_magnitude
                else:
                    row[f"{ccy}_velocity"] = ""
                    row[f"{ccy}_accel_mag"] = ""

            # Per-pair data
            for pair in DISPLAY_PAIRS:
                base, quote = pair[:3], pair[3:]

                # Per-TF pass/fail
                for tf in TIMEFRAME_LABELS:
                    tr = result.timeframes.get(tf)
                    if tr and base in tr.currency_scores and quote in tr.currency_scores:
                        b_sc = tr.currency_scores[base]
                        q_sc = tr.currency_scores[quote]
                        row[f"{pair}_{tf}_base"] = round(b_sc, 2)
                        row[f"{pair}_{tf}_quote"] = round(q_sc, 2)
                        row[f"{pair}_{tf}_spread"] = round(b_sc - q_sc, 2)
                    else:
                        row[f"{pair}_{tf}_base"] = ""
                        row[f"{pair}_{tf}_quote"] = ""
                        row[f"{pair}_{tf}_spread"] = ""

                # Composite spread
                b_comp = result.composite_scores.get(base, 0)
                q_comp = result.composite_scores.get(quote, 0)
                row[f"{pair}_composite_spread"] = round(b_comp - q_comp, 2)

                # HTF spread (for exits)
                b_htf = result.htf_composite_scores.get(base, 0)
                q_htf = result.htf_composite_scores.get(quote, 0)
                row[f"{pair}_htf_spread"] = round(b_htf - q_htf, 2)

                # Structural levels
                sl = result.structural_levels.get(pair, {})
                row[f"{pair}_day_high"] = sl.get("prev_day_high", "")
                row[f"{pair}_day_low"] = sl.get("prev_day_low", "")

                # Close price
                row[f"{pair}_close"] = result.close_prices.get(pair, "")

                # ACCEL candidate
                accel = result.accel_candidates.get(pair)
                row[f"{pair}_accel"] = f"{accel[0]}:{accel[1]}" if accel else ""

                # Open trade
                row[f"{pair}_open"] = "YES" if open_pairs and pair in open_pairs else ""

            # Create writer on first record (now we know all fieldnames)
            if self._writer is None:
                self._writer = csv.DictWriter(self._file, fieldnames=list(row.keys()))
                if not self._header_written:
                    self._writer.writeheader()
                    self._header_written = True

            self._writer.writerow(row)
            self._file.flush()

        except Exception as e:
            logger.warning("Diagnostic record error: %s", e)

    def close(self) -> None:
        """Close the CSV file."""
        if self._file:
            try:
                self._file.close()
            except Exception:
                pass
            self._file = None
            self._writer = None
