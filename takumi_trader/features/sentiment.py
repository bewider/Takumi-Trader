"""News & sentiment scrapers — all FREE sources.

Includes:
    ForexLive RSS scraping
    FXStreet RSS
    Reddit r/forex sentiment (free Reddit API)
    Wikipedia page views (free Wikimedia API)
    Google Trends (pytrends, free)
    Simple keyword-based sentiment classifier (no ML model needed)

NOTE: Twitter/X API is no longer free. Excluded.
NOTE: For higher-quality NLP, add OpenAI API integration ($-per-call).
      Pure keyword-based sentiment included here as zero-cost fallback.
"""
from __future__ import annotations

import json
import urllib.request
import urllib.parse
import re
from pathlib import Path
from datetime import datetime, timezone, timedelta

UTC = timezone.utc

CACHE_DIR = Path(".feature_cache")
CACHE_DIR.mkdir(exist_ok=True)


# ──────────────────────────────────────────────────────────────────
# 1. RSS news feeds
# ──────────────────────────────────────────────────────────────────

def fetch_rss(url: str, cache_minutes: int = 15) -> list[dict]:
    """Generic RSS fetcher. Returns list of {title, pub_date, link, summary}."""
    cache_path = CACHE_DIR / f"rss_{abs(hash(url))}.json"
    if cache_path.exists():
        mtime = cache_path.stat().st_mtime
        if (datetime.now().timestamp() - mtime) / 60 < cache_minutes:
            try:
                return json.loads(cache_path.read_text())
            except Exception:
                pass
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            xml = resp.read().decode("utf-8", errors="ignore")
    except Exception:
        return []
    # Lightweight RSS parser via regex (avoid feedparser dep)
    items = re.findall(r"<item>(.*?)</item>", xml, re.DOTALL)
    out = []
    for item in items:
        title_m = re.search(r"<title><!\[CDATA\[(.*?)\]\]></title>", item) or re.search(r"<title>(.*?)</title>", item)
        date_m = re.search(r"<pubDate>(.*?)</pubDate>", item)
        link_m = re.search(r"<link>(.*?)</link>", item)
        desc_m = re.search(r"<description><!\[CDATA\[(.*?)\]\]></description>", item) or re.search(r"<description>(.*?)</description>", item)
        out.append({
            "title": title_m.group(1).strip() if title_m else "",
            "pub_date": date_m.group(1).strip() if date_m else "",
            "link": link_m.group(1).strip() if link_m else "",
            "summary": (desc_m.group(1).strip() if desc_m else "")[:500],
        })
    cache_path.write_text(json.dumps(out))
    return out


def forexlive_news() -> list[dict]:
    """ForexLive central bank news RSS."""
    return fetch_rss("https://www.forexlive.com/feed")


def fxstreet_news() -> list[dict]:
    """FXStreet major news RSS."""
    return fetch_rss("https://www.fxstreet.com/rss/news")


def investing_news() -> list[dict]:
    """Investing.com forex news."""
    return fetch_rss("https://www.investing.com/rss/news_25.rss")


# ──────────────────────────────────────────────────────────────────
# 2. SIMPLE KEYWORD-BASED SENTIMENT
# ──────────────────────────────────────────────────────────────────

POSITIVE_TERMS = {
    "rally", "surge", "soar", "jump", "rise", "climb", "gain", "boost",
    "strengthen", "outperform", "beat", "exceed", "record high", "bull",
    "hawkish", "improve", "expand", "robust", "strong", "upbeat", "optimistic",
    "growth", "recovery", "upgrade",
}

NEGATIVE_TERMS = {
    "plunge", "tumble", "slump", "drop", "fall", "decline", "weaken",
    "underperform", "miss", "record low", "bear", "dovish", "deteriorate",
    "contract", "weak", "downbeat", "pessimistic", "recession", "downgrade",
    "crash", "sell-off", "rout",
}


def keyword_sentiment(text: str) -> dict:
    """Return (sentiment_score in [-1, +1], positive_count, negative_count).

    Pure keyword-based — fast and zero-dependency. Lower quality than NLP
    but adequate for headline-level signals.
    """
    if not text:
        return {"sentiment": 0.0, "pos_count": 0, "neg_count": 0}
    text_lower = text.lower()
    pos = sum(1 for term in POSITIVE_TERMS if term in text_lower)
    neg = sum(1 for term in NEGATIVE_TERMS if term in text_lower)
    total = pos + neg
    if total == 0:
        return {"sentiment": 0.0, "pos_count": 0, "neg_count": 0}
    return {"sentiment": (pos - neg) / total, "pos_count": pos, "neg_count": neg}


def aggregate_news_sentiment(currency: str = None) -> dict:
    """Aggregate sentiment across all news feeds, optionally filtered by currency."""
    all_news = forexlive_news() + fxstreet_news() + investing_news()
    if currency:
        # Filter by currency mention in title or summary
        ccy_terms = {currency.upper()}
        if currency == "USD":
            ccy_terms.update({"DOLLAR", "GREENBACK", "FED"})
        if currency == "EUR":
            ccy_terms.update({"EURO", "ECB"})
        if currency == "GBP":
            ccy_terms.update({"POUND", "STERLING", "BOE"})
        if currency == "JPY":
            ccy_terms.update({"YEN", "BOJ"})
        all_news = [
            n for n in all_news
            if any(term in (n["title"] + n["summary"]).upper() for term in ccy_terms)
        ]
    if not all_news:
        return {"avg_sentiment": 0.0, "n_articles": 0}
    sentiments = []
    for n in all_news[:50]:  # cap
        s = keyword_sentiment(n["title"] + " " + n["summary"])
        sentiments.append(s["sentiment"])
    return {
        "avg_sentiment": float(sum(sentiments) / len(sentiments)),
        "n_articles": len(sentiments),
        "headlines": [n["title"] for n in all_news[:5]],
    }


def news_flow_rate(window_minutes: int = 60) -> float:
    """Headlines per minute across all feeds in the last `window_minutes`."""
    all_news = forexlive_news() + fxstreet_news() + investing_news()
    if not all_news:
        return 0.0
    now = datetime.now(UTC)
    cutoff = now - timedelta(minutes=window_minutes)
    count = 0
    for n in all_news:
        try:
            # Parse RSS date format
            for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S GMT"):
                try:
                    dt = datetime.strptime(n["pub_date"], fmt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=UTC)
                    if dt >= cutoff:
                        count += 1
                    break
                except ValueError:
                    continue
        except Exception:
            continue
    return count / window_minutes


# ──────────────────────────────────────────────────────────────────
# 3. REDDIT — r/forex sentiment (free API, no auth needed for reads)
# ──────────────────────────────────────────────────────────────────

def fetch_subreddit_top(subreddit: str = "forex", time_filter: str = "day", limit: int = 25) -> list[dict]:
    """Fetch top posts from a subreddit (no auth required for read)."""
    cache_path = CACHE_DIR / f"reddit_{subreddit}_{time_filter}.json"
    if cache_path.exists():
        mtime = cache_path.stat().st_mtime
        if (datetime.now().timestamp() - mtime) / 60 < 15:
            try:
                return json.loads(cache_path.read_text())
            except Exception:
                pass
    url = f"https://www.reddit.com/r/{subreddit}/top.json?t={time_filter}&limit={limit}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "TAKUMI-features/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode())
    except Exception:
        return []
    posts = []
    for child in payload.get("data", {}).get("children", []):
        d = child.get("data", {})
        posts.append({
            "title": d.get("title", ""),
            "score": d.get("score", 0),
            "comments": d.get("num_comments", 0),
            "created_utc": int(d.get("created_utc", 0)),
        })
    cache_path.write_text(json.dumps(posts))
    return posts


def reddit_forex_sentiment() -> dict:
    """Aggregate sentiment across top r/forex posts in last 24h."""
    posts = fetch_subreddit_top("forex", "day", 25)
    if not posts:
        return {"avg_sentiment": 0.0, "n_posts": 0, "total_engagement": 0}
    sentiments = [keyword_sentiment(p["title"])["sentiment"] for p in posts]
    return {
        "avg_sentiment": float(sum(sentiments) / len(sentiments)),
        "n_posts": len(sentiments),
        "total_engagement": int(sum(p["score"] + p["comments"] for p in posts)),
    }


# ──────────────────────────────────────────────────────────────────
# 4. Wikipedia page views (free Wikimedia API)
# ──────────────────────────────────────────────────────────────────

def wiki_page_views(article: str = "Federal_Reserve_System", days: int = 7) -> list[int]:
    """Get daily Wikipedia page views for an article. Free API, no key."""
    cache_path = CACHE_DIR / f"wiki_{article}.json"
    if cache_path.exists():
        mtime = cache_path.stat().st_mtime
        if (datetime.now().timestamp() - mtime) / 3600 < 24:
            try:
                return json.loads(cache_path.read_text())
            except Exception:
                pass
    end_date = datetime.now(UTC).strftime("%Y%m%d")
    start_date = (datetime.now(UTC) - timedelta(days=days)).strftime("%Y%m%d")
    url = (
        f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
        f"/en.wikipedia/all-access/all-agents/"
        f"{urllib.parse.quote(article)}/daily/{start_date}/{end_date}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "TAKUMI-features/1.0 trader@example.com"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode())
    except Exception:
        return []
    views = [int(item["views"]) for item in payload.get("items", [])]
    cache_path.write_text(json.dumps(views))
    return views


def wiki_attention_change(article: str, days: int = 7) -> float:
    """Recent vs prior-period view ratio. >1.5 = attention spike."""
    views = wiki_page_views(article, days * 2)
    if len(views) < 6:
        return 1.0
    recent = views[-days:]
    prior = views[-days * 2: -days]
    if not prior:
        return 1.0
    avg_recent = sum(recent) / len(recent)
    avg_prior = sum(prior) / len(prior) if prior else 1
    return float(avg_recent / avg_prior) if avg_prior > 0 else 1.0


# ──────────────────────────────────────────────────────────────────
# 5. Google Trends (free via pytrends — install pytrends if available)
# ──────────────────────────────────────────────────────────────────

def google_trends(keyword: str, days: int = 7) -> dict:
    """Google Trends search interest (0-100). Requires pytrends if installed.

    Returns dict with current value and change.
    """
    try:
        from pytrends.request import TrendReq
    except ImportError:
        return {"available": False, "reason": "pytrends not installed (pip install pytrends)"}
    try:
        pytrends = TrendReq(hl="en-US", tz=0)
        timeframe = f"now {days}-d"
        pytrends.build_payload([keyword], timeframe=timeframe, geo="")
        df = pytrends.interest_over_time()
        if df.empty:
            return {"available": True, "current": 0, "avg_period": 0}
        cur = int(df[keyword].iloc[-1])
        avg = float(df[keyword].mean())
        return {"available": True, "current": cur, "avg_period": avg}
    except Exception:
        return {"available": False, "reason": "pytrends fetch failed"}
