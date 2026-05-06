"""Compare all 5 entry method options on AUDJPY."""

import numpy as np
import time
from datetime import datetime, timezone
from takumi_trader.core.backtester import BacktestEngine, BacktestConfig, CalcParams
from takumi_trader.core.strength import ZSCORE_WINDOW
import takumi_trader.core.strength as strength_mod


def run_test(label, config, patch_fn=None, unpatch_fn=None):
    if patch_fn:
        patch_fn()
    t0 = time.time()
    engine = BacktestEngine(config)
    results = engine.run()
    elapsed = time.time() - t0
    if unpatch_fn:
        unpatch_fn()

    wins = sum(1 for r in results if r.final_pnl_pips > 0)
    total = len(results)
    wr = wins / total * 100 if total else 0
    total_pnl = sum(r.final_pnl_pips for r in results)
    avg_pnl = total_pnl / total if total else 0
    avg_mfe = sum(r.mfe_pips for r in results) / total if total else 0
    avg_mae = sum(r.mae_pips for r in results) / total if total else 0

    std_count = sum(1 for r in results if getattr(r, "entry_type", "standard") == "standard")
    accel_count = sum(1 for r in results if getattr(r, "entry_type", "standard") == "acceleration")

    extra = ""
    if accel_count > 0:
        extra = f" | std={std_count} accel={accel_count}"

    print(
        f"  {label:45s} | {total:>5d} | WR {wr:>5.1f}% | "
        f"Avg {avg_pnl:>+6.1f}p | Total {total_pnl:>+8.0f}p | "
        f"MFE {avg_mfe:>5.1f} | MAE {avg_mae:>5.1f} | {elapsed:>5.1f}s{extra}"
    )
    return results


pair = "AUDJPY"
cp = CalcParams()
cp.ema_period = 6
cp.roc_decay = 0.2
cp.threshold_m1 = 5.5
cp.threshold_m5 = 5.0
cp.threshold_m15 = 4.5
cp.threshold_h1 = 4.0

print("=" * 140)
print(f"AUDJPY ENTRY METHOD COMPARISON - Jan 5 to Mar 20, 2026 (Dukascopy)")
print(f"Base: ema=6, roc=0.2, threshold_m1=5.5")
print("=" * 140)

# BASELINE
config_base = BacktestConfig(
    days_back=0, start_date="2026-01-05", use_dukascopy=True,
    calc_params=cp, single_pair=pair, allow_session_reentry=True,
    use_accel_entry=False,
)
run_test("BASELINE: Standard only (all 4 TFs extreme)", config_base)

# OPTION A: Acceleration Detector
config_a = BacktestConfig(
    days_back=0, start_date="2026-01-05", use_dukascopy=True,
    calc_params=cp, single_pair=pair, allow_session_reentry=True,
    use_accel_entry=True, accel_min_velocity=1.5, accel_min_spread=6.0, accel_min_htf_agree=2,
)
run_test("OPTION A: Accel Detector (vel>1.5, 2HTF)", config_a)

config_a2 = BacktestConfig(
    days_back=0, start_date="2026-01-05", use_dukascopy=True,
    calc_params=cp, single_pair=pair, allow_session_reentry=True,
    use_accel_entry=True, accel_min_velocity=2.0, accel_min_spread=8.0, accel_min_htf_agree=2,
)
run_test("OPTION A (tighter): vel>2.0, spread>8, 2HTF", config_a2)

config_a3 = BacktestConfig(
    days_back=0, start_date="2026-01-05", use_dukascopy=True,
    calc_params=cp, single_pair=pair, allow_session_reentry=True,
    use_accel_entry=True, accel_min_velocity=1.5, accel_min_spread=6.0, accel_min_htf_agree=3,
)
run_test("OPTION A (strict): vel>1.5, 3HTF agree", config_a3)

# OPTION B: Shorter Z-Score Window
original_zscore = strength_mod.ZSCORE_WINDOW


def patch_z50():
    strength_mod.ZSCORE_WINDOW = 50


def patch_z30():
    strength_mod.ZSCORE_WINDOW = 30


def unpatch_z():
    strength_mod.ZSCORE_WINDOW = original_zscore


config_b = BacktestConfig(
    days_back=0, start_date="2026-01-05", use_dukascopy=True,
    calc_params=cp, single_pair=pair, allow_session_reentry=True,
    use_accel_entry=False,
)
run_test("OPTION B: Z-Score window = 50 (was 120)", config_b, patch_z50, unpatch_z)
run_test("OPTION B: Z-Score window = 30 (was 120)", config_b, patch_z30, unpatch_z)

# OPTION C: Faster EMA
for ema in [3, 4]:
    cp_c = CalcParams()
    cp_c.ema_period = ema
    cp_c.roc_decay = 0.2
    cp_c.threshold_m1 = 5.5
    cp_c.threshold_m5 = 5.0
    cp_c.threshold_m15 = 4.5
    cp_c.threshold_h1 = 4.0
    config_c = BacktestConfig(
        days_back=0, start_date="2026-01-05", use_dukascopy=True,
        calc_params=cp_c, single_pair=pair, allow_session_reentry=True,
        use_accel_entry=False,
    )
    run_test(f"OPTION C: Fast EMA={ema} (was 6)", config_c)

# OPTION D: Relax TF requirements
cp_d = CalcParams()
cp_d.ema_period = 6
cp_d.roc_decay = 0.2
cp_d.threshold_m1 = 5.5
cp_d.threshold_m5 = 5.0
cp_d.threshold_m15 = 4.5
cp_d.threshold_h1 = 0.5  # Nearly always passes
config_d = BacktestConfig(
    days_back=0, start_date="2026-01-05", use_dukascopy=True,
    calc_params=cp_d, single_pair=pair, allow_session_reentry=True,
    use_accel_entry=False,
)
run_test("OPTION D: 3/4 TFs (H1 threshold=0.5)", config_d)

cp_d2 = CalcParams()
cp_d2.ema_period = 6
cp_d2.roc_decay = 0.2
cp_d2.threshold_m1 = 0.5  # Nearly always passes
cp_d2.threshold_m5 = 5.0
cp_d2.threshold_m15 = 4.5
cp_d2.threshold_h1 = 4.0
config_d2 = BacktestConfig(
    days_back=0, start_date="2026-01-05", use_dukascopy=True,
    calc_params=cp_d2, single_pair=pair, allow_session_reentry=True,
    use_accel_entry=False,
)
run_test("OPTION D: 3/4 TFs (M1 threshold=0.5)", config_d2)

# OPTION E: ROC-Heavy
for roc in [0.1, 0.05]:
    cp_e = CalcParams()
    cp_e.ema_period = 6
    cp_e.roc_decay = roc
    cp_e.threshold_m1 = 5.5
    cp_e.threshold_m5 = 5.0
    cp_e.threshold_m15 = 4.5
    cp_e.threshold_h1 = 4.0
    config_e = BacktestConfig(
        days_back=0, start_date="2026-01-05", use_dukascopy=True,
        calc_params=cp_e, single_pair=pair, allow_session_reentry=True,
        use_accel_entry=False,
    )
    run_test(f"OPTION E: ROC decay={roc} (was 0.2)", config_e)

# COMBOS
config_ab = BacktestConfig(
    days_back=0, start_date="2026-01-05", use_dukascopy=True,
    calc_params=cp, single_pair=pair, allow_session_reentry=True,
    use_accel_entry=True, accel_min_velocity=1.5, accel_min_spread=6.0, accel_min_htf_agree=2,
)
run_test("COMBO A+B: Accel + Z-Score=50", config_ab, patch_z50, unpatch_z)

cp_ad = CalcParams()
cp_ad.ema_period = 6
cp_ad.roc_decay = 0.2
cp_ad.threshold_m1 = 5.5
cp_ad.threshold_m5 = 5.0
cp_ad.threshold_m15 = 4.5
cp_ad.threshold_h1 = 0.5
config_ad = BacktestConfig(
    days_back=0, start_date="2026-01-05", use_dukascopy=True,
    calc_params=cp_ad, single_pair=pair, allow_session_reentry=True,
    use_accel_entry=True, accel_min_velocity=1.5, accel_min_spread=6.0, accel_min_htf_agree=2,
)
run_test("COMBO A+D: Accel + 3/4 TFs (no H1)", config_ad)

print()
print("=" * 140)
print("DONE! Compare trades, WR, and total profit to find the best approach.")
