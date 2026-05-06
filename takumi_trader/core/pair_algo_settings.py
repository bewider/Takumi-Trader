"""Pair-specific algorithm settings manager with history.

Stores per-pair calculation parameters (EMA period, ROC decay, thresholds, etc.)
that were optimized via backtest. Each pair maintains:
  - Current active settings
  - History of previous settings (pushed when new settings are applied)

Storage: data/pair_algo_settings.json
"""

from __future__ import annotations

import json
import logging
import sys
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _data_file() -> Path:
    """Get the path to the pair algo settings JSON file."""
    if getattr(sys, "frozen", False):
        base = Path(sys.executable).parent / "data"
    else:
        base = Path(__file__).resolve().parent.parent.parent / "data"
    base.mkdir(parents=True, exist_ok=True)
    return base / "pair_algo_settings.json"


def _load_all() -> dict[str, Any]:
    """Load all pair algo settings from disk."""
    path = _data_file()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("Failed to load pair algo settings: %s", e)
        return {}


def _save_all(data: dict[str, Any]) -> None:
    """Save all pair algo settings to disk."""
    path = _data_file()
    try:
        path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    except Exception as e:
        logger.warning("Failed to save pair algo settings: %s", e)


def get_pair_settings(pair: str) -> dict | None:
    """Get the current active settings for a pair.

    Returns None if no settings have been saved for this pair.
    """
    data = _load_all()
    pair_data = data.get(pair)
    if pair_data and "current" in pair_data:
        return pair_data["current"]
    return None


def get_pair_history(pair: str) -> list[dict]:
    """Get the settings history for a pair (newest first).

    Returns empty list if no history exists.
    """
    data = _load_all()
    pair_data = data.get(pair, {})
    return pair_data.get("history", [])


def get_all_pairs_with_settings() -> list[str]:
    """Get list of all pairs that have saved settings."""
    data = _load_all()
    return sorted(p for p in data if data[p].get("current"))


def save_pair_settings(
    pair: str,
    calc_params: dict,
    stats: dict | None = None,
    sltp: dict | None = None,
    source: str = "optimizer",
    backtest_period: str = "",
) -> None:
    """Save new settings for a pair, pushing any existing settings to history.

    Args:
        pair: Currency pair (e.g., "GBPJPY")
        calc_params: Dict of CalcParams fields (ema_period, roc_decay, etc.)
        stats: Optional performance stats (trades, wr, total_r, exp_r, etc.)
        sltp: Optional SL/TP ATR multiplier settings (sl_atr, tp_atr, sl_pips, tp_pips)
        source: Where the settings came from ("optimizer", "manual", "backtest")
        backtest_period: Human-readable backtest period string
    """
    data = _load_all()

    if pair not in data:
        data[pair] = {"current": None, "history": []}

    # Push current settings to history if they exist
    old_current = data[pair].get("current")
    if old_current:
        history = data[pair].get("history", [])
        history.insert(0, deepcopy(old_current))  # newest first
        # Keep last 50 history entries
        data[pair]["history"] = history[:50]

    # Build new current settings
    new_settings = {
        "set_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": source,
        "backtest_period": backtest_period,
        **calc_params,
    }

    if stats:
        new_settings.update({
            "trades": stats.get("trades", 0),
            "wr": stats.get("wr", 0.0),
            "exp_r": stats.get("exp_r", 0.0),
            "avg_final": stats.get("avg_final", 0.0),
            "total_r": stats.get("total_r", 0.0),
            "avg_mfe": stats.get("avg_mfe", 0.0),
            "avg_mae": stats.get("avg_mae", 0.0),
        })

    if sltp:
        new_settings.update({
            "sl_atr": sltp.get("sl_atr", 0),
            "tp_atr": sltp.get("tp_atr", 0),
            "sl_pips": sltp.get("sl_pips", 0),
            "tp_pips": sltp.get("tp_pips", 0),
        })

    data[pair]["current"] = new_settings
    _save_all(data)
    logger.info("Pair algo settings saved for %s: %s", pair, new_settings)


def delete_pair_settings(pair: str) -> None:
    """Delete all settings (current + history) for a pair."""
    data = _load_all()
    if pair in data:
        del data[pair]
        _save_all(data)
        logger.info("Pair algo settings deleted for %s", pair)


def restore_from_history(pair: str, index: int) -> bool:
    """Restore a historical setting as the current setting.

    The current setting is pushed to history, and the selected
    history entry becomes the new current.

    Args:
        pair: Currency pair
        index: Index in history list (0 = most recent)

    Returns:
        True if successful, False if index out of range.
    """
    data = _load_all()
    pair_data = data.get(pair)
    if not pair_data:
        return False

    history = pair_data.get("history", [])
    if index < 0 or index >= len(history):
        return False

    # Push current to history
    old_current = pair_data.get("current")
    if old_current:
        history.insert(0, deepcopy(old_current))

    # Pop the selected entry from history and make it current
    restored = history.pop(index + (1 if old_current else 0))
    restored["set_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    restored["source"] = f"restored (was: {restored.get('source', 'unknown')})"

    pair_data["current"] = restored
    pair_data["history"] = history[:50]

    _save_all(data)
    logger.info("Restored history entry %d for %s", index, pair)
    return True
