"""Treasury yields & rate-related features.

Fetches from FRED API (free with registration: https://fred.stlouisfed.org).
Falls back to cached values if API unavailable.

To enable: set FRED_API_KEY environment variable, or pass api_key directly.
Free tier: 100 req/sec, no monthly fee.
"""
from __future__ import annotations

import json
import os
import urllib.request
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone, timedelta

UTC = timezone.utc

# FRED series IDs for TAKUMI-relevant rates
FRED_SERIES = {
    "us_10y": "DGS10",
    "us_2y": "DGS2",
    "us_30y": "DGS30",
    "us_3m": "DGS3MO",
    "fed_funds_upper": "DFEDTARU",
    "fed_funds_lower": "DFEDTARL",
    "sofr": "SOFR",
    "us_real_10y_tips": "DFII10",
    "hy_oas": "BAMLH0A0HYM2",  # ICE BofA HY corporate index
    "ig_oas": "BAMLC0A0CM",    # ICE BofA IG corporate index
    "vix_close": "VIXCLS",
    "ted_spread": "TEDRATE",
    # ECB equivalents are limited in FRED — supplement from ECB SDW separately
    "euro_10y_proxy": "IRLTLT01EZM156N",  # EU long-term rate
    "uk_10y_proxy": "IRLTLT01GBM156N",
    "japan_10y_proxy": "IRLTLT01JPM156N",
    "canada_10y_proxy": "IRLTLT01CAM156N",
    # Currency-related
    "eu_short_rate": "IR3TIB01EZM156N",
}


CACHE_DIR = Path(".feature_cache")
CACHE_DIR.mkdir(exist_ok=True)


def fetch_fred_series(series_id: str, api_key: str = None, cache_hours: int = 24) -> list[tuple[str, float]]:
    """Fetch a FRED series. Returns list of (date_str, value) tuples.

    Caches result to disk for `cache_hours` hours to avoid hammering API.
    """
    api_key = api_key or os.environ.get("FRED_API_KEY", "")
    cache_path = CACHE_DIR / f"fred_{series_id}.json"
    # Check cache freshness
    if cache_path.exists():
        try:
            mtime = cache_path.stat().st_mtime
            if (datetime.now().timestamp() - mtime) / 3600 < cache_hours:
                data = json.loads(cache_path.read_text())
                return [(d["date"], d["value"]) for d in data]
        except Exception:
            pass
    if not api_key:
        return []  # No API key — silent fail
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={urllib.parse.quote(series_id)}"
        f"&api_key={api_key}&file_type=json&sort_order=desc&limit=500"
    )
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            payload = json.loads(resp.read().decode())
        observations = []
        for obs in payload.get("observations", []):
            try:
                v = float(obs["value"])
                observations.append({"date": obs["date"], "value": v})
            except (ValueError, KeyError):
                continue
        cache_path.write_text(json.dumps(observations))
        return [(d["date"], d["value"]) for d in observations]
    except Exception:
        return []


def latest_yield(series_id: str, api_key: str = None) -> float:
    """Get the most recent value for a FRED series."""
    data = fetch_fred_series(series_id, api_key)
    if not data:
        return 0.0
    return float(data[0][1])


def yield_curve_slope(api_key: str = None) -> dict:
    """Compute key yield curve slopes."""
    us_10y = latest_yield("DGS10", api_key)
    us_2y = latest_yield("DGS2", api_key)
    us_3m = latest_yield("DGS3MO", api_key)
    us_30y = latest_yield("DGS30", api_key)
    return {
        "us_10y": us_10y,
        "us_2y": us_2y,
        "us_3m": us_3m,
        "us_30y": us_30y,
        "spread_10y_2y": us_10y - us_2y,
        "spread_10y_3m": us_10y - us_3m,
        "spread_30y_10y": us_30y - us_10y,
        "curve_inverted": (us_10y - us_2y) < 0,
    }


def real_yields(api_key: str = None) -> dict:
    """Real (TIPS) yields — inflation-adjusted."""
    return {
        "us_real_10y": latest_yield("DFII10", api_key),
    }


def rate_differential(base: str, quote: str, api_key: str = None) -> float:
    """Yield differential for a currency pair (10Y proxy)."""
    series_map = {
        "USD": "DGS10", "EUR": "IRLTLT01EZM156N", "GBP": "IRLTLT01GBM156N",
        "JPY": "IRLTLT01JPM156N", "CAD": "IRLTLT01CAM156N",
    }
    b = latest_yield(series_map.get(base, ""), api_key)
    q = latest_yield(series_map.get(quote, ""), api_key)
    return b - q


def credit_spreads(api_key: str = None) -> dict:
    """High-yield + investment-grade credit spreads (option-adjusted)."""
    return {
        "hy_oas_pct": latest_yield("BAMLH0A0HYM2", api_key),
        "ig_oas_pct": latest_yield("BAMLC0A0CM", api_key),
    }


def vix_latest(api_key: str = None) -> float:
    """Latest VIX close."""
    return latest_yield("VIXCLS", api_key)


def ted_spread(api_key: str = None) -> float:
    """TED Spread — measures USD funding stress (LIBOR vs Treasury)."""
    return latest_yield("TEDRATE", api_key)
