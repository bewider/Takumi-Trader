"""Persistent JSON log for QM4/CSI alerts.

Appends every fired QM4Alert and QM4PairAlert to a single JSON Lines file
(one JSON object per line) so the Alert Performance dialog can load and
display the full history without loading the entire file into memory.

File: data/csi_alert_log.json  (path supplied by the caller)
Schema per line:
    {
        "kind":       "currency" | "pair",
        "timestamp":  1234567890.12,   # Unix time
        -- for kind == "currency" --
        "currency":   "JPY",
        "direction":  "WEAK",
        "alert_type": "HTF",
        "reason":     "individual",
        "tf_scores":  {"D1": 0.8, "W1": 0.7, "MN": 0.9},
        "cumulative": 2.4,
        "alignment":  4,
        "depth_pct":  20.0,
        "best_pair":  "GBPJPY",
        -- for kind == "pair" --
        "pair":           "GBPJPY",
        "direction":      "BUY",
        "alert_type":     "HTF",
        "trigger_type":   "individual",
        "base_scores":    {"D1": 9.2, "W1": 9.1, "MN": 9.3},
        "quote_scores":   {"D1": 0.8, "W1": 0.7, "MN": 0.9},
        "base_alignment": 5,
        "quote_alignment":4,
        "spread":         8.4,
    }
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from takumi_trader.core.qm4_alerts import QM4Alert, QM4PairAlert

logger = logging.getLogger(__name__)


class QM4AlertLog:
    """Append-only JSON Lines log for QM4/CSI alerts."""

    def __init__(self, log_file: Path) -> None:
        """Initialise the log.

        Args:
            log_file: Path to the .json log file (created on first append).
        """
        self._path = log_file
        self._path.parent.mkdir(parents=True, exist_ok=True)

    # ── Write ──────────────────────────────────────────────────────

    def append(self, alert: QM4Alert | QM4PairAlert) -> None:
        """Serialise *alert* and append it as one JSON line."""
        try:
            record = self._serialise(alert)
            with self._path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception:
            logger.exception("QM4AlertLog: failed to append alert")

    # ── Read ───────────────────────────────────────────────────────

    def load(self) -> list[dict]:
        """Load and return all logged alert records (oldest first).

        Malformed lines are skipped with a warning.
        """
        if not self._path.exists():
            return []
        records: list[dict] = []
        with self._path.open(encoding="utf-8") as fh:
            for lineno, raw in enumerate(fh, 1):
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    records.append(json.loads(raw))
                except json.JSONDecodeError:
                    logger.warning(
                        "QM4AlertLog: skipping malformed line %d in %s",
                        lineno, self._path,
                    )
        return records

    # ── Housekeeping ───────────────────────────────────────────────

    def clear(self) -> None:
        """Delete the log file (history cleared)."""
        try:
            if self._path.exists():
                self._path.unlink()
        except Exception:
            logger.exception("QM4AlertLog: failed to clear log")

    # ── Serialisation ──────────────────────────────────────────────

    @staticmethod
    def _serialise(alert: QM4Alert | QM4PairAlert) -> dict:
        """Convert an alert dataclass to a plain dict for JSON storage."""
        if isinstance(alert, QM4Alert):
            return {
                "kind":       "currency",
                "timestamp":  alert.timestamp,
                "currency":   alert.currency,
                "direction":  alert.direction,
                "alert_type": alert.alert_type,
                "reason":     alert.reason,
                "tf_scores":  alert.tf_scores,
                "cumulative": alert.cumulative,
                "alignment":  alert.alignment,
                "depth_pct":  alert.depth_pct,
                "best_pair":  alert.best_pair,
            }
        else:
            return {
                "kind":            "pair",
                "timestamp":       alert.timestamp,
                "pair":            alert.pair,
                "direction":       alert.direction,
                "alert_type":      alert.alert_type,
                "trigger_type":    alert.trigger_type,
                "base_scores":     alert.base_scores,
                "quote_scores":    alert.quote_scores,
                "base_alignment":  alert.base_alignment,
                "quote_alignment": alert.quote_alignment,
                "spread":          alert.spread,
            }
