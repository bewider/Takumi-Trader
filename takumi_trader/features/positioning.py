"""CFTC Commitments of Traders (COT) data.

Free CSV from CFTC.gov, weekly Friday update reflecting Tuesday's positioning.
URL pattern: https://www.cftc.gov/dea/futures/deacmesf.htm

For each currency we track speculator (Non-Commercial) and commercial net
positioning, plus a Z-score vs trailing 52 weeks.
"""
from __future__ import annotations

import json
import urllib.request
from pathlib import Path
from datetime import datetime, timezone, timedelta
import csv
import io

UTC = timezone.utc

CACHE_DIR = Path(".feature_cache")
CACHE_DIR.mkdir(exist_ok=True)


# CFTC market codes for FX (CME futures)
COT_MARKETS = {
    "EUR": "099741",
    "GBP": "096742",
    "JPY": "097741",
    "CHF": "092741",
    "CAD": "090741",
    "AUD": "232741",
    "NZD": "112741",
    "MXN": "095741",  # included for completeness
}


def fetch_cot_legacy_zip(year: int) -> bytes:
    """Fetch annual COT legacy combined report ZIP."""
    cache_path = CACHE_DIR / f"cot_legacy_{year}.zip"
    if cache_path.exists():
        return cache_path.read_bytes()
    url = f"https://www.cftc.gov/files/dea/history/dea_fut_xls_{year}.zip"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read()
        cache_path.write_bytes(data)
        return data
    except Exception:
        return b""


def fetch_cot_weekly_txt(cache_hours: int = 48) -> str:
    """Fetch latest weekly COT text report (alternative to Excel/ZIP)."""
    cache_path = CACHE_DIR / "cot_weekly.txt"
    if cache_path.exists():
        mtime = cache_path.stat().st_mtime
        if (datetime.now().timestamp() - mtime) / 3600 < cache_hours:
            try:
                return cache_path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                pass
    # CFTC weekly text URL (legacy report)
    url = "https://www.cftc.gov/dea/newcot/deacot.txt"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            text = resp.read().decode("utf-8", errors="ignore")
        cache_path.write_text(text, encoding="utf-8")
        return text
    except Exception:
        return ""


def parse_cot_currency_positioning(text: str, currency: str) -> dict:
    """Extract net positioning for a single currency from the legacy text report.

    Returns:
        {
            'date': str,
            'noncomm_long': int,    # speculator long
            'noncomm_short': int,
            'noncomm_net': int,
            'comm_long': int,       # commercial/hedger
            'comm_short': int,
            'comm_net': int,
            'open_interest': int,
        }
    """
    # Map currency to search string
    search_terms = {
        "EUR": "EURO FX",
        "GBP": "BRITISH POUND",
        "JPY": "JAPANESE YEN",
        "CHF": "SWISS FRANC",
        "CAD": "CANADIAN DOLLAR",
        "AUD": "AUSTRALIAN DOLLAR",
        "NZD": "NZ DOLLAR",
    }
    term = search_terms.get(currency)
    if not term:
        return {}
    # Find the section
    upper = text.upper()
    idx = upper.find(term)
    if idx < 0:
        return {}
    # Take the next ~30 lines from that point
    section = text[idx: idx + 5000]
    # Extract Non-Commercial and Commercial rows. Format is fixed-width columns.
    lines = section.split("\n")
    out = {"currency": currency}
    for i, line in enumerate(lines):
        if "Non-Commercial" in line and i + 1 < len(lines):
            try:
                # Next data line has positioning numbers
                # Column structure varies; here's a rough heuristic
                data_line = lines[i + 1]
                parts = [p.replace(",", "") for p in data_line.split() if p.replace(",", "").isdigit()]
                if len(parts) >= 2:
                    out["noncomm_long"] = int(parts[0])
                    out["noncomm_short"] = int(parts[1])
                    out["noncomm_net"] = out["noncomm_long"] - out["noncomm_short"]
            except Exception:
                pass
        if "Commercial" in line and "Non-" not in line and i + 1 < len(lines):
            try:
                data_line = lines[i + 1]
                parts = [p.replace(",", "") for p in data_line.split() if p.replace(",", "").isdigit()]
                if len(parts) >= 2:
                    out["comm_long"] = int(parts[0])
                    out["comm_short"] = int(parts[1])
                    out["comm_net"] = out["comm_long"] - out["comm_short"]
            except Exception:
                pass
    return out


def get_latest_cot_snapshot() -> dict:
    """Get latest COT snapshot for all major currencies.

    Returns dict keyed by currency: positioning info.
    """
    text = fetch_cot_weekly_txt()
    if not text:
        return {}
    out = {}
    for ccy in ("EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD"):
        info = parse_cot_currency_positioning(text, ccy)
        if info:
            out[ccy] = info
    return out


def cot_zscore_history_path(currency: str) -> Path:
    """Path to historical CSV cache for a currency's net positioning."""
    return CACHE_DIR / f"cot_history_{currency}.csv"


def append_cot_history(currency: str, date_str: str, net_position: int) -> None:
    """Append today's reading to the historical CSV (idempotent on date)."""
    p = cot_zscore_history_path(currency)
    # Read existing
    existing = []
    if p.exists():
        with p.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing = list(reader)
    # Check if already have this date
    if any(row["date"] == date_str for row in existing):
        return
    existing.append({"date": date_str, "net": str(net_position)})
    with p.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "net"])
        writer.writeheader()
        writer.writerows(existing)


def cot_zscore(currency: str, current_net: int, lookback_weeks: int = 52) -> float:
    """Z-score of current net position vs trailing 52 weeks.

    |z| > 2 = extreme positioning (contrarian signal).
    """
    p = cot_zscore_history_path(currency)
    if not p.exists():
        return 0.0
    nets = []
    with p.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                nets.append(int(row["net"]))
            except Exception:
                continue
    if len(nets) < 5:
        return 0.0
    recent = nets[-lookback_weeks:]
    import numpy as np
    arr = np.array(recent, dtype=np.float64)
    if arr.std() < 1e-12:
        return 0.0
    return float((current_net - arr.mean()) / arr.std(ddof=1))
