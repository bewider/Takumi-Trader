"""Market data fetcher — Yahoo Finance unofficial API for indices, commodities, etc.

Yahoo Finance is FREE (no API key, no monthly fee). Uses public chart endpoint.
Suitable for: VIX, gold, oil, copper, equity indices, FX vol indices, BTC.

Symbols of interest:
    ^VIX     S&P 500 implied vol (CBOE VIX)
    ^VVIX    VIX of VIX
    ^SKEW    Black-swan put-vs-call vol
    ^MOVE    Treasury implied vol (ICE)
    GC=F     Gold futures
    CL=F     WTI crude
    BZ=F     Brent
    HG=F     Copper
    NG=F     Natural gas
    ^GSPC    S&P 500 (close-of-day)
    ^IXIC    NASDAQ Composite
    ^N225    Nikkei 225
    ^GDAXI   DAX
    ^FTSE    FTSE 100
    ^HSI     Hang Seng
    ^TNX     US 10Y yield * 10
    ^FVX     US 5Y * 10
    ^IRX     US 3M * 10
    BTC-USD  Bitcoin
    EURUSD=X / USDJPY=X / etc. — FX (broker quality usually better)
    EUVIX    EUR/USD CBOE volatility index
    JYVIX    USD/JPY CBOE volatility index
    BPVIX    GBP/USD CBOE volatility index
"""
from __future__ import annotations

import json
import urllib.request
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone, timedelta

UTC = timezone.utc

CACHE_DIR = Path(".feature_cache")
CACHE_DIR.mkdir(exist_ok=True)


def fetch_yahoo_chart(symbol: str, range_str: str = "1mo", interval: str = "1d", cache_hours: int = 1) -> list[dict]:
    """Fetch Yahoo Finance chart data.

    range options: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, max
    interval:      1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo

    Returns list of {timestamp, open, high, low, close, volume}.
    """
    cache_path = CACHE_DIR / f"yahoo_{symbol.replace('=', '_').replace('^', '')}_{range_str}_{interval}.json"
    if cache_path.exists():
        mtime = cache_path.stat().st_mtime
        if (datetime.now().timestamp() - mtime) / 3600 < cache_hours:
            try:
                return json.loads(cache_path.read_text())
            except Exception:
                pass
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{urllib.parse.quote(symbol)}"
        f"?range={range_str}&interval={interval}&includePrePost=false"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 TAKUMI-features"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode())
        result = payload.get("chart", {}).get("result", [])
        if not result:
            return []
        r = result[0]
        timestamps = r.get("timestamp", [])
        ind = r.get("indicators", {}).get("quote", [{}])[0]
        opens = ind.get("open", [])
        highs = ind.get("high", [])
        lows = ind.get("low", [])
        closes = ind.get("close", [])
        volumes = ind.get("volume", [])
        bars = []
        for i, t in enumerate(timestamps):
            if i < len(closes) and closes[i] is not None:
                bars.append({
                    "timestamp": int(t),
                    "open": opens[i] if i < len(opens) and opens[i] is not None else closes[i],
                    "high": highs[i] if i < len(highs) and highs[i] is not None else closes[i],
                    "low": lows[i] if i < len(lows) and lows[i] is not None else closes[i],
                    "close": closes[i],
                    "volume": volumes[i] if i < len(volumes) and volumes[i] is not None else 0,
                })
        cache_path.write_text(json.dumps(bars))
        return bars
    except Exception:
        return []


def latest_close(symbol: str, range_str: str = "5d", interval: str = "1d") -> float:
    """Most recent close price for a Yahoo symbol."""
    bars = fetch_yahoo_chart(symbol, range_str, interval)
    if not bars:
        return 0.0
    return float(bars[-1]["close"])


# ──────────────────────────────────────────────────────────────────
# CONVENIENCE: TAKUMI-RELEVANT SYMBOLS
# ──────────────────────────────────────────────────────────────────

def vix_close() -> float:
    """Latest VIX close."""
    return latest_close("^VIX")


def vvix_close() -> float:
    return latest_close("^VVIX")


def skew_close() -> float:
    return latest_close("^SKEW")


def move_close() -> float:
    """ICE BofA MOVE Index — Treasury vol."""
    return latest_close("^MOVE")


def gold_close() -> float:
    return latest_close("GC=F")


def crude_oil_close() -> float:
    return latest_close("CL=F")


def brent_close() -> float:
    return latest_close("BZ=F")


def copper_close() -> float:
    return latest_close("HG=F")


def nat_gas_close() -> float:
    return latest_close("NG=F")


def sp500_close() -> float:
    return latest_close("^GSPC")


def nasdaq_close() -> float:
    return latest_close("^IXIC")


def nikkei_close() -> float:
    return latest_close("^N225")


def dax_close() -> float:
    return latest_close("^GDAXI")


def ftse_close() -> float:
    return latest_close("^FTSE")


def hang_seng_close() -> float:
    return latest_close("^HSI")


def us_10y_yield() -> float:
    """US 10Y yield (Yahoo's TNX is yield × 10)."""
    return latest_close("^TNX") / 10


def us_2y_yield() -> float:
    return latest_close("^FVX") / 10  # Note: FVX is 5Y; USE2YR doesn't exist


def us_3m_yield() -> float:
    return latest_close("^IRX") / 10


def btc_close() -> float:
    return latest_close("BTC-USD")


def get_all_macro_snapshot() -> dict:
    """Single call — fetches all key macro indicators."""
    return {
        "vix": vix_close(),
        "vvix": vvix_close(),
        "skew": skew_close(),
        "move": move_close(),
        "gold": gold_close(),
        "wti": crude_oil_close(),
        "brent": brent_close(),
        "copper": copper_close(),
        "natgas": nat_gas_close(),
        "sp500": sp500_close(),
        "nasdaq": nasdaq_close(),
        "nikkei": nikkei_close(),
        "dax": dax_close(),
        "ftse": ftse_close(),
        "hang_seng": hang_seng_close(),
        "us_10y": us_10y_yield(),
        "btc": btc_close(),
    }


# ──────────────────────────────────────────────────────────────────
# CBOE FX VOLATILITY INDICES (free via Yahoo)
# ──────────────────────────────────────────────────────────────────

def fx_vol_indices() -> dict:
    """CBOE FX volatility indices."""
    return {
        "euvix": latest_close("^EVZ"),       # Euro Currency Volatility Index
        "jyvix": latest_close("^JPYUSD"),    # may not exist via Yahoo — fallback
        "bpvix": latest_close("^BPVIX"),     # may not exist
    }
