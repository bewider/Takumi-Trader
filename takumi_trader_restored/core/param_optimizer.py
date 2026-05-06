"""Parameter optimizer for strategy tuning — joint entry + exit optimization.

Runs grid search over calculation parameters AND SL/TP ATR multipliers
together to find the best combined setup per pair.

Architecture:
  1. Group combos by (ema_period, roc_decay, sensitivity) — these affect raw
     strength computation and require a full backtest each.
  2. Run ONE backtest per group with the loosest thresholds to capture ALL
     possible alerts.
  3. For each group's outcomes, sweep post-hoc (instant):
     a) threshold_m1, min_divergence_spread, require_acceleration
     b) SL/TP ATR multipliers — simulate fixed SL/TP using MFE/MAE data
  4. Rank ALL combinations by Expected R per trade.
"""

from __future__ import annotations

import itertools
import logging
import time
from dataclasses import dataclass, field
from typing import Callable

from takumi_trader.core.alert_performance import AlertOutcome
from takumi_trader.core.backtester import BacktestConfig, BacktestEngine, CalcParams
from takumi_trader.core.strength import DISPLAY_PAIRS

logger = logging.getLogger(__name__)


# ── Result Container ────────────────────────────────────────────────

@dataclass
class OptimizationResult:
    """Result of a single parameter combination backtest."""
    label: str                          # Human-readable description
    calc_params: dict                   # CalcParams as dict
    filter_config: dict                 # Filter settings
    pair: str                           # Pair tested (or "ALL")
    trades: int = 0
    wins: int = 0
    wr: float = 0.0
    avg_mfe: float = 0.0
    avg_mae: float = 0.0
    avg_final: float = 0.0
    total_r: float = 0.0               # Using ATR-based SL
    exp_r: float = 0.0                 # Expected R per trade
    sl_atr: float = 0.0                # SL in ATR multiplier (0 = signal-based exit)
    tp_atr: float = 0.0                # TP in ATR multiplier (0 = signal-based exit)
    sl_pips: float = 0.0               # Avg SL in pips
    tp_pips: float = 0.0               # Avg TP in pips
    # Exit category breakdown — HYBRID mode (SL/TP + signal exit)
    n_tp_hit: int = 0                   # Trades that hit TP
    n_sl_hit: int = 0                   # Trades that hit SL
    n_signal: int = 0                   # Trades that exited on signal (neither hit)
    r_tp_hit: float = 0.0              # Total R from TP hits
    r_sl_hit: float = 0.0              # Total R from SL hits
    r_signal: float = 0.0              # Total R from signal exits
    # STRICT "set & forget" mode (no signal exit, assume SL hit if neither)
    strict_wr: float = 0.0
    strict_total_r: float = 0.0
    strict_exp_r: float = 0.0
    strict_n_tp_hit: int = 0
    strict_n_sl_hit: int = 0
    strict_r_tp_hit: float = 0.0
    strict_r_sl_hit: float = 0.0
    outcomes: list[AlertOutcome] = field(default_factory=list)


# ── Parameter Grid Definitions ──────────────────────────────────────

# Filter combinations (fast, post-hoc)
FILTER_GRID = {
    "filter_htf":  [False, True],
    "filter_vel":  [False, True],
    "filter_isol": [False, True],
    "filter_structural":  [True],  # Always on — Key Level + TP Clearance
    "conviction_threshold": [30, 45, 60, 75],
}

# Calculation parameters
CALC_PARAM_GRID = {
    "ema_period":       [4],              # Locked to live system — EMA4 (A+B+C combo)
    "roc_decay":        [0.2],            # Locked to live system (mt5_worker.py)
    "sensitivity":      [1.0],
    "threshold_m1":     [5.5, 6.0, 6.5, 7.0],
    "min_divergence_spread": [10.0, 12.0, 14.0],
    "require_acceleration":  [False, True],
}

CALC_PARAM_GRID_FAST = {
    "ema_period":       [4],              # Locked to live system — EMA4 (A+B+C combo)
    "roc_decay":        [0.2],            # Locked to live system (mt5_worker.py)
    "sensitivity":      [1.0],
    "threshold_m1":     [5.5, 6.5, 7.0],
    "min_divergence_spread": [10.0, 12.0],
    "require_acceleration":  [False, True],
}

CALC_PARAM_GRID_TURBO = {
    "ema_period":       [4],              # Locked to live system — EMA4 (A+B+C combo)
    "roc_decay":        [0.2],            # Locked to live system (mt5_worker.py)
    "sensitivity":      [1.0],
    "threshold_m1":     [5.5, 6.5],
    "min_divergence_spread": [12.0],
    "require_acceleration":  [False, True],
}

# SL/TP ATR multiplier grid
SLTP_GRID = {
    "sl_atr": [0.3, 0.4, 0.5, 0.6, 0.8, 1.0],
    "tp_atr": [0.5, 0.8, 1.0, 1.2, 1.5, 2.0, 3.0],  # No cap — optimizer picks best WR
}


def _count_combos(grid: dict) -> int:
    total = 1
    for values in grid.values():
        total *= len(values)
    return total


def _grid_iter(grid: dict):
    keys = list(grid.keys())
    for values in itertools.product(*[grid[k] for k in keys]):
        yield dict(zip(keys, values))


# ── SL/TP simulation on MFE/MAE data ──────────────────────────────

def _simulate_sl_tp_for_trade(
    sl_pips: float,
    tp_pips: float,
    running_mfe: list[float],
    running_mae: list[float],
    signal_exit_pnl: float,
) -> tuple[float, str]:
    """Bar-by-bar replay to determine which of SL/TP is hit first.

    Walks through the per-bar running MFE/MAE history and finds the
    first bar where TP or SL would have been triggered.  When both
    trigger on the same bar, TP wins (price must pass through TP to
    reach SL at the same extreme).

    Returns (pnl_pips, exit_type) where exit_type is "tp_hit",
    "sl_hit", or "signal".
    """
    tp_bar = -1
    sl_bar = -1
    for bar_idx in range(len(running_mfe)):
        if tp_bar < 0 and running_mfe[bar_idx] >= tp_pips:
            tp_bar = bar_idx
        if sl_bar < 0 and running_mae[bar_idx] >= sl_pips:
            sl_bar = bar_idx
        if tp_bar >= 0 and sl_bar >= 0:
            break

    if tp_bar >= 0 and sl_bar >= 0:
        if tp_bar <= sl_bar:
            return tp_pips, "tp_hit"
        else:
            return -sl_pips, "sl_hit"
    elif tp_bar >= 0:
        return tp_pips, "tp_hit"
    elif sl_bar >= 0:
        return -sl_pips, "sl_hit"
    else:
        return signal_exit_pnl, "signal"


def _simulate_sltp(
    outcomes: list[AlertOutcome],
    sl_atr: float,
    tp_atr: float,
    strict: bool = False,
) -> dict:
    """Simulate fixed SL/TP on trade outcomes using bar-by-bar replay.

    Uses per-bar running MFE/MAE history recorded during backtesting to
    replay each M1 bar in order, determining whether SL or TP is hit
    first — exactly matching real-time trading behaviour.

    For each trade, one of three exit types:
    - TP HIT: bar-by-bar MFE reaches TP before MAE reaches SL
    - SL HIT: bar-by-bar MAE reaches SL before MFE reaches TP
    - SIGNAL EXIT (hybrid only): Neither SL nor TP hit during trade lifetime

    Args:
        strict: If True, "set & forget" mode — no signal exit. Trades where
                neither SL nor TP hit are assumed to eventually hit SL (worst case).

    Returns dict with stats including per-category breakdowns.
    """
    if not outcomes:
        return {}

    reward_ratio = tp_atr / sl_atr if sl_atr > 0 else 1.0
    wins = 0
    total_r = 0.0
    valid = 0
    sl_pips_sum = 0.0
    tp_pips_sum = 0.0

    # Per-category tracking
    n_tp_hit = 0
    n_sl_hit = 0
    n_signal = 0
    r_tp_hit = 0.0
    r_sl_hit = 0.0
    r_signal = 0.0
    signal_wins = 0

    for o in outcomes:
        atr = o.entry_atr_pips
        if atr <= 0:
            continue

        sl_pips = sl_atr * atr
        tp_pips = tp_atr * atr
        sl_pips_sum += sl_pips
        tp_pips_sum += tp_pips
        valid += 1

        # Use bar-by-bar replay if history is available
        running_mfe = o.bar_running_mfe if hasattr(o, "bar_running_mfe") else []
        running_mae = o.bar_running_mae if hasattr(o, "bar_running_mae") else []

        if running_mfe and running_mae:
            # ── Bar-by-bar replay (accurate, matches real trading) ──
            pnl, exit_type = _simulate_sl_tp_for_trade(
                sl_pips, tp_pips, running_mfe, running_mae, o.final_pnl_pips,
            )
            trade_r = pnl / sl_pips if sl_pips > 0 else 0.0

            if exit_type == "tp_hit":
                total_r += reward_ratio
                r_tp_hit += reward_ratio
                n_tp_hit += 1
                wins += 1
            elif exit_type == "sl_hit":
                total_r -= 1.0
                r_sl_hit -= 1.0
                n_sl_hit += 1
            else:
                # Signal exit — neither SL nor TP hit during trade
                if strict:
                    # Set & forget: assume eventual SL hit (worst case)
                    total_r -= 1.0
                    r_sl_hit -= 1.0
                    n_sl_hit += 1
                else:
                    total_r += trade_r
                    r_signal += trade_r
                    n_signal += 1
                    if trade_r > 0:
                        wins += 1
                        signal_wins += 1
        else:
            # ── Legacy fallback: single MFE/MAE values (old data) ──
            sl_hit = o.mae_pips >= sl_pips
            tp_hit = o.mfe_pips >= tp_pips

            if sl_hit and tp_hit:
                # Both hit — use timing to determine order
                if o.time_to_mae_minutes < o.time_to_mfe_minutes:
                    total_r -= 1.0
                    r_sl_hit -= 1.0
                    n_sl_hit += 1
                else:
                    total_r += reward_ratio
                    r_tp_hit += reward_ratio
                    n_tp_hit += 1
                    wins += 1
            elif sl_hit:
                total_r -= 1.0
                r_sl_hit -= 1.0
                n_sl_hit += 1
            elif tp_hit:
                total_r += reward_ratio
                r_tp_hit += reward_ratio
                n_tp_hit += 1
                wins += 1
            else:
                if strict:
                    total_r -= 1.0
                    r_sl_hit -= 1.0
                    n_sl_hit += 1
                else:
                    trade_r = o.final_pnl_pips / sl_pips if sl_pips > 0 else 0
                    total_r += trade_r
                    r_signal += trade_r
                    n_signal += 1
                    if trade_r > 0:
                        wins += 1
                        signal_wins += 1

    if valid < 3:
        return {}

    wr = wins / valid * 100
    exp_r = total_r / valid

    return {
        "trades": valid,
        "wins": wins,
        "wr": round(wr, 1),
        "total_r": round(total_r, 1),
        "exp_r": round(exp_r, 3),
        "sl_atr": sl_atr,
        "tp_atr": tp_atr,
        "avg_sl_pips": round(sl_pips_sum / valid, 1),
        "avg_tp_pips": round(tp_pips_sum / valid, 1),
        "rr": round(reward_ratio, 1),
        # Per-category breakdown
        "n_tp_hit": n_tp_hit,
        "n_sl_hit": n_sl_hit,
        "n_signal": n_signal,
        "r_tp_hit": round(r_tp_hit, 1),
        "r_sl_hit": round(r_sl_hit, 1),
        "r_signal": round(r_signal, 1),
        "signal_wins": signal_wins,
    }


# ── Scoring helpers ─────────────────────────────────────────────────

def _build_label(combo: dict, sl_atr: float = 0, tp_atr: float = 0) -> str:
    """Build a human-readable label from parameter combo."""
    parts = []
    if combo.get("ema_period", 4) != 4: parts.append(f"EMA{combo['ema_period']}")
    if combo.get("roc_decay", 0.2) != 0.2: parts.append(f"decay={combo['roc_decay']}")
    if combo.get("sensitivity", 1.0) != 1.0: parts.append(f"sens={combo['sensitivity']}")
    if combo.get("threshold_m1", 6.5) != 6.5: parts.append(f"thr={combo['threshold_m1']}")
    if combo.get("min_divergence_spread", 12.0) != 12.0: parts.append(f"div={combo['min_divergence_spread']}")
    if combo.get("require_acceleration", False): parts.append("ACCEL")
    if sl_atr > 0 and tp_atr > 0:
        parts.append(f"SL={sl_atr}×ATR TP={tp_atr}×ATR")
    label = " | ".join(parts) if parts else "DEFAULT"
    return label


def _score_outcomes_with_sltp(
    outcomes: list[AlertOutcome],
    combo: dict,
    pair: str,
    sl_atr: float,
    tp_atr: float,
) -> OptimizationResult | None:
    """Score outcomes with SL/TP simulation applied (both hybrid and strict)."""
    if len(outcomes) < 3:
        return None

    sim = _simulate_sltp(outcomes, sl_atr, tp_atr, strict=False)
    sim_strict = _simulate_sltp(outcomes, sl_atr, tp_atr, strict=True)
    if not sim:
        return None

    n = sim["trades"]
    avg_mfe = sum(o.mfe_pips for o in outcomes) / len(outcomes)
    avg_mae = sum(o.mae_pips for o in outcomes) / len(outcomes)
    avg_final = sum(o.final_pnl_pips for o in outcomes) / len(outcomes)

    return OptimizationResult(
        label=_build_label(combo, sl_atr, tp_atr),
        calc_params=combo,
        filter_config={"filters": "ALL OFF", "conviction": 0},
        pair=pair,
        trades=n,
        wins=sim["wins"],
        wr=sim["wr"],
        avg_mfe=round(avg_mfe, 1),
        avg_mae=round(avg_mae, 1),
        avg_final=round(avg_final, 1),
        total_r=sim["total_r"],
        exp_r=sim["exp_r"],
        sl_atr=sl_atr,
        tp_atr=tp_atr,
        sl_pips=sim["avg_sl_pips"],
        tp_pips=sim["avg_tp_pips"],
        n_tp_hit=sim["n_tp_hit"],
        n_sl_hit=sim["n_sl_hit"],
        n_signal=sim["n_signal"],
        r_tp_hit=sim["r_tp_hit"],
        r_sl_hit=sim["r_sl_hit"],
        r_signal=sim["r_signal"],
        # Strict mode results
        strict_wr=sim_strict.get("wr", 0.0) if sim_strict else 0.0,
        strict_total_r=sim_strict.get("total_r", 0.0) if sim_strict else 0.0,
        strict_exp_r=sim_strict.get("exp_r", 0.0) if sim_strict else 0.0,
        strict_n_tp_hit=sim_strict.get("n_tp_hit", 0) if sim_strict else 0,
        strict_n_sl_hit=sim_strict.get("n_sl_hit", 0) if sim_strict else 0,
        strict_r_tp_hit=sim_strict.get("r_tp_hit", 0.0) if sim_strict else 0.0,
        strict_r_sl_hit=sim_strict.get("r_sl_hit", 0.0) if sim_strict else 0.0,
        outcomes=outcomes,
    )


def _score_outcomes_signal_exit(
    outcomes: list[AlertOutcome],
    combo: dict,
    pair: str,
) -> OptimizationResult | None:
    """Score outcomes using signal-based exit (no fixed SL/TP)."""
    n = len(outcomes)
    if n < 3:
        return None

    wins = sum(1 for o in outcomes if o.final_pnl_pips > 0)
    wr = wins / n * 100
    avg_mfe = sum(o.mfe_pips for o in outcomes) / n
    avg_mae = sum(o.mae_pips for o in outcomes) / n
    avg_final = sum(o.final_pnl_pips for o in outcomes) / n

    # Total R using 0.5 ATR as reference SL
    avg_atr = sum(o.entry_atr_pips for o in outcomes if o.entry_atr_pips > 0)
    atr_count = sum(1 for o in outcomes if o.entry_atr_pips > 0)
    if atr_count > 0:
        avg_atr /= atr_count
        sl_pips = avg_atr * 0.5
        total_r = sum(o.final_pnl_pips / sl_pips for o in outcomes) if sl_pips > 0 else 0
        exp_r = total_r / n
    else:
        total_r = 0
        exp_r = 0

    return OptimizationResult(
        label=_build_label(combo) + " (signal exit)",
        calc_params=combo,
        filter_config={"filters": "ALL OFF", "conviction": 0},
        pair=pair,
        trades=n,
        wins=wins,
        wr=round(wr, 1),
        avg_mfe=round(avg_mfe, 1),
        avg_mae=round(avg_mae, 1),
        avg_final=round(avg_final, 1),
        total_r=round(total_r, 1),
        exp_r=round(exp_r, 3),
        outcomes=outcomes,
    )


# ── Filter Optimization ─────────────────────────────────────────────

def optimize_filters(
    base_outcomes: list[AlertOutcome],
    pair: str | None = None,
) -> list[dict]:
    """Sweep filter combinations on pre-computed outcomes."""
    outcomes = base_outcomes
    if pair:
        outcomes = [o for o in outcomes if o.pair == pair]

    if not outcomes:
        return []

    results = []
    for combo in _grid_iter(FILTER_GRID):
        conv_thr = combo["conviction_threshold"]
        filtered = [o for o in outcomes if o.conviction_score >= conv_thr]

        n = len(filtered)
        if n < 5:
            continue

        wins = sum(1 for o in filtered if o.final_pnl_pips > 0)
        wr = wins / n * 100
        avg_mfe = sum(o.mfe_pips for o in filtered) / n
        avg_mae = sum(o.mae_pips for o in filtered) / n
        avg_final = sum(o.final_pnl_pips for o in filtered) / n

        avg_atr = sum(o.entry_atr_pips for o in filtered if o.entry_atr_pips > 0)
        atr_count = sum(1 for o in filtered if o.entry_atr_pips > 0)
        if atr_count > 0:
            avg_atr /= atr_count
            sl_pips = avg_atr * 0.4
            total_r = sum(o.final_pnl_pips / sl_pips for o in filtered) if sl_pips > 0 else 0
        else:
            total_r = 0

        filter_label = []
        if combo["filter_htf"]: filter_label.append("HTF")
        if combo["filter_vel"]: filter_label.append("VEL")
        if combo["filter_isol"]: filter_label.append("ISOL")
        if combo.get("filter_structural"): filter_label.append("STRUCT")
        label = "+".join(filter_label) if filter_label else "NONE"
        label += f" conv={conv_thr}"

        results.append({
            "label": label,
            "filters": combo,
            "trades": n,
            "wins": wins,
            "wr": round(wr, 1),
            "avg_mfe": round(avg_mfe, 1),
            "avg_mae": round(avg_mae, 1),
            "avg_final": round(avg_final, 1),
            "total_r": round(total_r, 1),
            "exp_r": round(total_r / n, 2) if n > 0 else 0,
        })

    results.sort(key=lambda x: x["exp_r"], reverse=True)
    return results


# ── Joint Entry + Exit Optimization ─────────────────────────────────

def optimize_calc_params(
    base_config: BacktestConfig,
    pair: str,
    fast_mode: bool = True,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> list[OptimizationResult]:
    """Joint optimization of entry params AND exit (SL/TP) together.

    For each strength group:
    1. Run ONE backtest with loosest thresholds
    2. For each threshold combo, filter outcomes post-hoc
    3. For each filtered set, sweep ALL SL/TP multiplier combos
    4. Also evaluate signal-based exit (no fixed SL/TP) for comparison
    5. Rank ALL by Expected R per trade

    This finds the best COMBINED entry + exit strategy.
    """
    if fast_mode:
        grid = CALC_PARAM_GRID_TURBO
    else:
        grid = CALC_PARAM_GRID_FAST

    total_combos = _count_combos(grid)
    n_sltp = _count_combos(SLTP_GRID)

    # Split into strength params (full backtest) and threshold params (post-hoc)
    strength_grid = {
        k: v for k, v in grid.items()
        if k in ("ema_period", "roc_decay", "sensitivity")
    }
    threshold_grid = {
        k: v for k, v in grid.items()
        if k in ("threshold_m1", "min_divergence_spread", "require_acceleration")
    }

    n_strength = _count_combos(strength_grid) if strength_grid else 1
    n_threshold = _count_combos(threshold_grid) if threshold_grid else 1

    logger.info(
        "Joint optimization: %d strength groups × %d threshold × %d SL/TP = %d total combos for %s",
        n_strength, n_threshold, n_sltp + 1,  # +1 for signal exit
        n_strength * n_threshold * (n_sltp + 1), pair,
    )

    # Fetch data ONCE
    if progress_callback:
        progress_callback(0, n_strength, f"Fetching data for {pair}...")

    data_cfg = BacktestConfig(
        days_back=base_config.days_back,
        start_date=base_config.start_date,
        use_dukascopy=base_config.use_dukascopy,
        post_exit_hours=base_config.post_exit_hours,
        exit_spread_threshold=base_config.exit_spread_threshold,
        use_htf_exit=base_config.use_htf_exit,
        filter_news=base_config.filter_news,
        single_pair=pair,
    )
    data_engine = BacktestEngine(data_cfg)
    shared_data = data_engine.fetch_data()
    if not shared_data:
        logger.error("No data fetched for %s", pair)
        return []

    logger.info("Data fetched, running %d backtests + post-hoc SL/TP sweep", n_strength)

    min_threshold_m1 = min(threshold_grid.get("threshold_m1", [6.5]))
    min_divergence = min(threshold_grid.get("min_divergence_spread", [12.0]))

    start_time = time.time()
    results: list[OptimizationResult] = []

    strength_combos = list(_grid_iter(strength_grid)) if strength_grid else [{}]
    threshold_combos = list(_grid_iter(threshold_grid)) if threshold_grid else [{}]
    sltp_combos = list(_grid_iter(SLTP_GRID))

    for grp_idx, strength_combo in enumerate(strength_combos):
        if progress_callback:
            elapsed = time.time() - start_time
            eta = (elapsed / max(grp_idx, 1)) * (n_strength - grp_idx)
            progress_callback(
                grp_idx + 1, n_strength,
                f"Group {grp_idx+1}/{n_strength} | ETA: {eta:.0f}s"
            )

        # Run backtest with LOOSEST thresholds
        cp = CalcParams(
            ema_period=strength_combo.get("ema_period", 8),
            roc_decay=strength_combo.get("roc_decay", 0.3),
            sensitivity=strength_combo.get("sensitivity", 1.0),
            threshold_m1=min_threshold_m1,
            threshold_m5=min_threshold_m1 - 0.5,
            threshold_m15=min_threshold_m1 - 1.0,
            threshold_h1=min_threshold_m1 - 1.5,
            min_divergence_spread=min_divergence,
            require_acceleration=False,
        )

        cfg = BacktestConfig(
            days_back=base_config.days_back,
            start_date=base_config.start_date,
            use_dukascopy=base_config.use_dukascopy,
            post_exit_hours=base_config.post_exit_hours,
            exit_spread_threshold=base_config.exit_spread_threshold,
            use_htf_exit=base_config.use_htf_exit,
            filter_htf=False, filter_vel=False, filter_isol=False, filter_structural=True,
            filter_news=base_config.filter_news,  # hard block — always enforced
            conviction_threshold=0,
            calc_params=cp,
            single_pair=pair,
        )

        try:
            engine = BacktestEngine(cfg, prefetched_data=shared_data)
            base_outcomes = engine.run()
        except Exception as e:
            logger.warning("Group %d failed: %s", grp_idx, e)
            continue

        if len(base_outcomes) < 3:
            logger.info("Group %d: only %d outcomes (need ≥3), skipping", grp_idx, len(base_outcomes))
            continue

        logger.info(
            "Group %d: %d outcomes, ATR values: %s, bar_history lens: %s",
            grp_idx, len(base_outcomes),
            [round(o.entry_atr_pips, 1) for o in base_outcomes[:5]],
            [len(o.bar_running_mfe) if hasattr(o, 'bar_running_mfe') else 0 for o in base_outcomes[:5]],
        )

        # Sweep threshold params post-hoc
        for thr_combo in threshold_combos:
            thr_m1 = thr_combo.get("threshold_m1", 6.5)
            div_spread = thr_combo.get("min_divergence_spread", 12.0)
            req_accel = thr_combo.get("require_acceleration", False)

            # Filter by threshold
            filtered = []
            for o in base_outcomes:
                if o.strength_spread < div_spread:
                    continue
                if req_accel and o.conviction_score < 50:
                    continue
                filtered.append(o)

            if len(filtered) < 3:
                logger.debug(
                    "Group %d thr %s: only %d filtered outcomes (need ≥3)",
                    grp_idx, thr_combo, len(filtered),
                )
                continue

            logger.info(
                "Group %d thr %s: %d filtered outcomes, sweeping %d SL/TP combos",
                grp_idx, thr_combo, len(filtered), len(sltp_combos),
            )
            full_combo = {**strength_combo, **thr_combo}

            # A) Signal-based exit (no fixed SL/TP) — for comparison
            sig_result = _score_outcomes_signal_exit(filtered, full_combo, pair)
            if sig_result is not None:
                results.append(sig_result)

            # B) Sweep ALL SL/TP combos on this filtered set
            for sltp in sltp_combos:
                sl = sltp["sl_atr"]
                tp = sltp["tp_atr"]
                # Skip unreasonable combos (TP must be > SL for positive expectancy)
                if tp < sl * 0.5:
                    continue
                r = _score_outcomes_with_sltp(filtered, full_combo, pair, sl, tp)
                if r is not None and r.exp_r > -0.5:  # Don't keep obviously terrible combos
                    results.append(r)

    # Sort by: (1) highest WR first, then (2) best E[R] as tiebreaker
    # This prioritizes strike rate over raw R:R, producing more realistic TPs
    # that the market can actually reach on most trades.
    results.sort(key=lambda x: (x.wr, x.exp_r), reverse=True)

    elapsed = time.time() - start_time
    logger.info(
        "Joint optimization complete: %d strength groups, %d total results, %.1fs",
        n_strength, len(results), elapsed,
    )

    return results


# ── Combined Full Optimization ───────────────────────────────────────

def full_optimize(
    base_config: BacktestConfig,
    pair: str,
    fast_mode: bool = True,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> dict:
    """Run joint entry+exit optimization and filter optimization.

    Phase 1: Joint entry params + SL/TP sweep
    Phase 2: Filter sweep on the best combo's trades

    Returns dict with all results for display.
    """
    t0 = time.time()

    if progress_callback:
        progress_callback(0, 100, f"Phase 1: Joint entry+exit optimization for {pair}...")

    calc_results = optimize_calc_params(
        base_config, pair, fast_mode=fast_mode,
        progress_callback=progress_callback,
    )

    if not calc_results:
        return {
            "calc_results": [],
            "filter_results": [],
            "best_calc": {},
            "best_filter": {},
            "best_combined_label": "No results",
            "total_time": time.time() - t0,
        }

    # Phase 2: Filter sweep on best combo's outcomes
    best_calc = calc_results[0]
    if progress_callback:
        progress_callback(95, 100, "Phase 2: Filter optimization on best params...")

    filter_results = optimize_filters(best_calc.outcomes, pair=pair)
    best_filter = filter_results[0] if filter_results else {}

    # Combined label
    combined = f"{pair}: {best_calc.label}"
    if best_filter:
        combined += f" + {best_filter['label']}"

    return {
        "calc_results": calc_results[:30],
        "filter_results": filter_results[:20],
        "best_calc": best_calc.calc_params,
        "best_sltp": {"sl_atr": best_calc.sl_atr, "tp_atr": best_calc.tp_atr,
                       "sl_pips": best_calc.sl_pips, "tp_pips": best_calc.tp_pips},
        "best_filter": best_filter.get("filters", {}),
        "best_combined_label": combined,
        "total_time": time.time() - t0,
    }
