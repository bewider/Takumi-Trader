"""Compare different meter calculation methods against QM4 reference values."""
import MetaTrader5 as mt5
import numpy as np

mt5.initialize()

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "NZD", "CHF"]
ALL_28 = [
    "EURUSD","GBPUSD","AUDUSD","NZDUSD","USDCAD","USDCHF","USDJPY",
    "EURGBP","EURAUD","EURNZD","EURCAD","EURCHF","EURJPY",
    "GBPAUD","GBPNZD","GBPCAD","GBPCHF","GBPJPY",
    "AUDNZD","AUDCAD","AUDCHF","AUDJPY",
    "NZDCAD","NZDCHF","NZDJPY","CADCHF","CADJPY","CHFJPY",
]
CP = {c: [] for c in CURRENCIES}
for p in ALL_28:
    CP[p[:3]].append((p, True))
    CP[p[3:]].append((p, False))

# QM4 reference values from user's screenshot
QM4 = {
    "M15": {"USD":4.5,"EUR":4.1,"GBP":2.7,"JPY":8.0,"CAD":6.0,"AUD":5.3,"NZD":4.5,"CHF":4.0},
    "H1":  {"USD":5.4,"EUR":3.4,"GBP":2.1,"JPY":5.0,"CAD":8.9,"AUD":7.3,"NZD":6.2,"CHF":4.3},
    "H4":  {"USD":5.7,"EUR":3.3,"GBP":2.7,"JPY":4.8,"CAD":9.0,"AUD":4.3,"NZD":5.8,"CHF":6.5},
    "D1":  {"USD":5.7,"EUR":5.0,"GBP":5.3,"JPY":5.0,"CAD":4.3,"AUD":3.3,"NZD":5.2,"CHF":5.2},
}

def rsi_simple(closes, period):
    if len(closes) < period + 1:
        return 50.0
    d = np.diff(closes[-(period+1):])
    g = np.mean(np.where(d > 0, d, 0.0))
    lo = np.mean(np.where(d < 0, -d, 0.0))
    return 100.0 - 100.0 / (1.0 + g / lo) if lo > 0 else 100.0

def rsi_wilder(closes, period):
    if len(closes) < period + 2:
        return 50.0
    d = np.diff(closes)
    g = np.where(d > 0, d, 0.0)
    lo = np.where(d < 0, -d, 0.0)
    ag = np.mean(g[:period])
    al = np.mean(lo[:period])
    for i in range(period, len(g)):
        ag = (ag * (period - 1) + g[i]) / period
        al = (al * (period - 1) + lo[i]) / period
    return 100.0 - 100.0 / (1.0 + ag / al) if al > 0 else 100.0

def calc_rsi(tf_label, tf_const, rsi_fn, period, fetch_bars=None):
    if fetch_bars is None:
        fetch_bars = period + 20
    pair_rsi = {}
    for p in ALL_28:
        r = mt5.copy_rates_from_pos(p, tf_const, 0, fetch_bars)
        if r is None or len(r) < period + 1:
            continue
        pair_rsi[p] = rsi_fn(r["close"].astype(np.float64), period)
    scores = {}
    for c in CURRENCIES:
        vals = [pair_rsi[p] if ib else 100.0 - pair_rsi[p]
                for p, ib in CP[c] if p in pair_rsi]
        scores[c] = round(np.mean(vals) / 10.0, 1) if vals else 5.0
    return scores

def calc_roc_minmax(tf_label, tf_const, period):
    pair_roc = {}
    for p in ALL_28:
        r = mt5.copy_rates_from_pos(p, tf_const, 0, period + 20)
        if r is None or len(r) < period + 1:
            continue
        cl = r["close"].astype(np.float64)
        cur, past = cl[-1], cl[-(period + 1)]
        if past != 0:
            pair_roc[p] = ((cur - past) / past) * 100.0
    raw = {}
    for c in CURRENCIES:
        vals = [pair_roc[p] if ib else -pair_roc[p]
                for p, ib in CP[c] if p in pair_roc]
        raw[c] = np.mean(vals) if vals else 0.0
    vl = list(raw.values())
    mn, mx = min(vl), max(vl)
    rng = mx - mn
    return {c: round(((raw[c] - mn) / rng) * 10.0, 1) if rng > 0 else 5.0
            for c in CURRENCIES}

def calc_roc_rsi_hybrid(tf_label, tf_const, period):
    """ROC normalized to 0-100 using sigmoid, then averaged like RSI."""
    pair_roc = {}
    for p in ALL_28:
        r = mt5.copy_rates_from_pos(p, tf_const, 0, period + 20)
        if r is None or len(r) < period + 1:
            continue
        cl = r["close"].astype(np.float64)
        cur, past = cl[-1], cl[-(period + 1)]
        if past != 0:
            roc_pct = ((cur - past) / past) * 100.0
            # Normalize ROC to 0-100 using sigmoid
            # Scale factor depends on typical ROC range
            normalized = 100.0 / (1.0 + np.exp(-roc_pct * 5.0))
            pair_roc[p] = normalized
    scores = {}
    for c in CURRENCIES:
        vals = [pair_roc[p] if ib else 100.0 - pair_roc[p]
                for p, ib in CP[c] if p in pair_roc]
        scores[c] = round(np.mean(vals) / 10.0, 1) if vals else 5.0
    return scores

# Define all methods to test
TFS = [
    ("M15", mt5.TIMEFRAME_M15),
    ("H1", mt5.TIMEFRAME_H1),
    ("H4", mt5.TIMEFRAME_H4),
    ("D1", mt5.TIMEFRAME_D1),
]

methods = [
    ("RSI-Simple(14)",    lambda t, tc: calc_rsi(t, tc, rsi_simple, 14)),
    ("RSI-Simple(7)",     lambda t, tc: calc_rsi(t, tc, rsi_simple, 7)),
    ("RSI-Simple(10)",    lambda t, tc: calc_rsi(t, tc, rsi_simple, 10)),
    ("RSI-Wilder(14)",    lambda t, tc: calc_rsi(t, tc, rsi_wilder, 14, 200)),
    ("RSI-Wilder(7)",     lambda t, tc: calc_rsi(t, tc, rsi_wilder, 7, 100)),
    ("RSI-Wilder(10)",    lambda t, tc: calc_rsi(t, tc, rsi_wilder, 10, 150)),
    ("RSI-Wilder(21)",    lambda t, tc: calc_rsi(t, tc, rsi_wilder, 21, 250)),
    ("ROC(14) minmax",    lambda t, tc: calc_roc_minmax(t, tc, 14)),
    ("ROC(7) minmax",     lambda t, tc: calc_roc_minmax(t, tc, 7)),
    ("ROC-Sigmoid(14)",   lambda t, tc: calc_roc_rsi_hybrid(t, tc, 14)),
    ("ROC-Sigmoid(7)",    lambda t, tc: calc_roc_rsi_hybrid(t, tc, 7)),
]

print("=" * 120)
print("METHOD COMPARISON vs QM4")
print("=" * 120)

overall_errors = {n: 0.0 for n, _ in methods}
overall_count = {n: 0 for n, _ in methods}

for tfl, tfc in TFS:
    qr = QM4.get(tfl, {})
    print(f"\n  {tfl}:")
    hdr = f"  {'Method':<20s}"
    for c in CURRENCIES:
        hdr += f" {c:>5s}"
    hdr += f"  {'Error':>5s}"
    print(hdr)

    print(f"  {'QM4':<20s}", end="")
    for c in CURRENCIES:
        print(f" {qr.get(c, 0):>5.1f}", end="")
    print()

    for name, fn in methods:
        sc = fn(tfl, tfc)
        total_err = 0
        cnt = 0
        print(f"  {name:<20s}", end="")
        for c in CURRENCIES:
            v = sc.get(c, 5.0)
            ref = qr.get(c, 5.0)
            err = abs(v - ref)
            total_err += err
            cnt += 1
            marker = " " if err < 1.0 else "*" if err < 2.0 else "!"
            print(f" {v:>4.1f}{marker}", end="")
        avg_err = total_err / cnt if cnt else 0
        print(f"  {avg_err:>5.2f}")
        overall_errors[name] += total_err
        overall_count[name] += cnt

print(f"\n{'=' * 120}")
print("OVERALL RANKING (lowest avg error = best match to QM4):")
print("-" * 50)
ranked = sorted(overall_errors.items(), key=lambda x: x[1])
for i, (name, total_err) in enumerate(ranked):
    cnt = overall_count[name]
    avg = total_err / cnt if cnt > 0 else 999
    marker = " <== WINNER" if i == 0 else ""
    print(f"  {i+1}. {name:<20s}  avg error: {avg:.2f}{marker}")

mt5.shutdown()
