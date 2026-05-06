"""Session-aware trading session detection.

Uses the same JST-based session boundaries as the backtester to ensure
live trading and backtest results are directly comparable.

Session schedule (all times JST):
  07:00-08:44  Australia
  08:45-09:35  Tokyo_open
  09:36-12:08  Morning
  12:09-15:44  Afternoon
  15:45-16:25  Frankfurt_open
  16:26-16:44  EU
  16:45-17:35  London_open
  17:36-20:44  London
  20:45-21:35  US_open
  21:36-05:00  US
  05:01-06:59  NO_TRADE
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

_JST = timezone(timedelta(hours=9))

# Same session table as backtester.py — (start_h, start_m), (end_h, end_m), label
# All times are JST
#
# ┌─────────────────────────────────────────────────────────────────┐
# │ DST SWITCH GUIDE (Europe changes, Japan doesn't)               │
# │                                                                 │
# │ WINTER (last Sun Oct → last Sun Mar):                           │
# │   12:09-15:44  Afternoon                                        │
# │   15:45-16:25  Frankfurt_open                                   │
# │   16:26-16:44  EU                                               │
# │   16:45-17:35  London_open                                      │
# │   17:36-20:44  London                                           │
# │                                                                 │
# │ SUMMER (last Sun Mar → last Sun Oct):  ← CURRENTLY ACTIVE      │
# │   12:09-14:44  Afternoon                                        │
# │   14:45-15:25  Frankfurt_open                                   │
# │   15:26-15:44  EU                                               │
# │   15:45-16:35  London_open                                      │
# │   16:36-20:44  London                                           │
# │                                                                 │
# │ To switch: just tell Claude "switch to summer/winter times"     │
# └─────────────────────────────────────────────────────────────────┘

_SESSIONS: list[tuple[tuple[int, int], tuple[int, int], str]] = [
    ((8, 0), (8, 44), "Australia"),
    ((8, 45), (9, 35), "Tokyo_open"),
    ((9, 36), (12, 8), "Morning"),
    # ── DST-affected sessions (currently SUMMER) ──
    ((12, 9), (14, 44), "Afternoon"),
    ((14, 45), (15, 25), "Frankfurt_open"),
    ((15, 26), (15, 44), "EU"),
    ((15, 45), (16, 35), "London_open"),
    ((16, 36), (20, 44), "London"),
    # ── End DST-affected ──
    ((20, 45), (21, 35), "US_open"),
    ((21, 36), (23, 59), "US"),
    ((0, 0), (5, 0), "US"),
    ((5, 1), (7, 59), "NO_TRADE"),
]


def _jst_hm(dt_utc: datetime) -> int:
    """Convert a UTC datetime to JST hour*60+minute."""
    jst_dt = dt_utc.astimezone(_JST)
    return jst_dt.hour * 60 + jst_dt.minute


def _match_session(hm: int) -> str:
    """Match a JST hour*60+minute value to a session label."""
    for (sh, sm), (eh, em), label in _SESSIONS:
        start = sh * 60 + sm
        end = eh * 60 + em
        if start <= hm <= end:
            return label
    return "off_hours"


def get_current_session() -> str:
    """Determine which trading session is currently active.

    Returns the session label matching the backtester's session table.
    All boundaries are in JST (Asia/Tokyo, UTC+9, no DST).
    """
    now_utc = datetime.now(timezone.utc)
    return _match_session(_jst_hm(now_utc))


def minutes_since_session_start(ts: float) -> int:
    """Compute minutes elapsed since the current session began (JST)."""
    dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
    jst_dt = dt_utc.astimezone(_JST)
    hm_now = jst_dt.hour * 60 + jst_dt.minute
    for (sh, sm), (eh, em), _label in _SESSIONS:
        start = sh * 60 + sm
        end = eh * 60 + em
        if start <= hm_now <= end:
            return max(0, hm_now - start)
    return 0


def get_session_for_timestamp(ts: float) -> str:
    """Get the session for an arbitrary unix timestamp.

    Used for backfilling historical trades with session info.
    """
    dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
    return _match_session(_jst_hm(dt_utc))


def get_session_label() -> str:
    """Return the current session name, or WEEKEND if market is closed."""
    if is_weekend():
        return "WEEKEND"
    return get_current_session()


def is_no_trade_session() -> bool:
    """Check if we're in the NO_TRADE window (05:01-08:00 JST) or weekend."""
    return get_current_session() in ("NO_TRADE", "WEEKEND")


def is_weekend() -> bool:
    """Check if the forex market is closed (weekend) OR within the
    weekend pre-close buffer.

    Real forex hours: market closes Friday ~22:00 UTC (Sat 07:00 JST)
    and reopens Sunday ~22:00 UTC (Mon 07:00 JST).

    TAKUMI policy (2026-05-02): treat Sat 04:00 JST onwards as "weekend"
    so the per-cycle weekend close-all routine fires 3 hours before the
    real broker close, and no new trades open into the gap. Sat 04:00 JST
    is Fri 19:00 UTC, hence the `hour >= 19` boundary on Friday below.
    """
    now_utc = datetime.now(timezone.utc)
    weekday = now_utc.weekday()  # 0=Mon, 5=Sat, 6=Sun
    hour = now_utc.hour

    if weekday == 5:  # Saturday — always closed
        return True
    if weekday == 6:  # Sunday — closed until ~22:00 UTC
        return hour < 22
    if weekday == 4 and hour >= 19:  # Friday from 19:00 UTC = Sat 04:00 JST
        return True
    return False


# ── Legacy compatibility ──────────────────────────────────────────

# Keep these for any code that imports them, but they're no longer used
# for session detection (we use the JST table above instead).
SESSION_DEFINITIONS: dict = {}
_SESSION_PRIORITY: list = []


def get_session_transition_blend(
    transition_minutes: float = 15.0,
) -> tuple[str, str | None, float]:
    """Check if we're near a session boundary.

    Returns (current_session, None, 0.0) — transition blending
    is not used with the JST table approach.
    """
    return get_current_session(), None, 0.0


def is_within_active_window(windows: list[dict] | None = None) -> bool:
    """Check if current time is within any active trading window.

    With the JST session table, this always returns True unless
    we're in NO_TRADE.
    """
    if windows is None:
        return get_current_session() != "NO_TRADE"
    return True


# ─────────────────────────────────────────────────────────────────────
# NY session helpers for AU Gold suite (2026-04-24)
# ─────────────────────────────────────────────────────────────────────
# JST never changes clocks. NY observes DST (EST/EDT). For the AU2 NY
# Open Range Breakout strategy we need to know "is it 09:30 NY local right
# now?" — which in JST is 23:30 (winter, EST) or 22:30 (summer, EDT).
# These helpers encapsulate the conversion so the rest of the codebase
# stays pure-JST.

def _try_zoneinfo_ny():
    """Return ZoneInfo('America/New_York') if stdlib supports it.
    Returns None on Python < 3.9 or missing tzdata."""
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("America/New_York")
    except Exception:
        return None


_NY_TZ = _try_zoneinfo_ny()


def ny_local_hm_to_jst(ny_hour: int, ny_minute: int, when: datetime | None = None) -> tuple[int, int]:
    """Convert a NY-local wall clock time (e.g. 9:30 NY = market open) into
    the equivalent JST wall-clock (hour, minute), DST-aware.

    Example:
        09:30 NY EDT (summer) -> 22:30 JST
        09:30 NY EST (winter) -> 23:30 JST

    Args:
        ny_hour, ny_minute: NY-local hour/minute
        when: datetime to anchor the DST lookup (defaults to "now"). Use
              this when computing bounds for a specific trading day.

    Falls back to a fixed NY-EST (GMT-5) assumption if zoneinfo/tzdata
    is unavailable — this is slightly wrong 8 months of the year but
    keeps the app functional on minimal installs.
    """
    if when is None:
        when = datetime.now(timezone.utc)
    if _NY_TZ is not None:
        # Build a NY-local datetime for the same calendar day as `when`
        ny_when = when.astimezone(_NY_TZ).replace(
            hour=ny_hour, minute=ny_minute, second=0, microsecond=0,
        )
        jst_when = ny_when.astimezone(_JST)
        return jst_when.hour, jst_when.minute
    # Fallback (no tzdata): assume EST year-round (off by 1h in summer)
    # UTC = NY + 5, JST = UTC + 9, so JST = NY + 14
    total = ny_hour * 60 + ny_minute + 14 * 60
    total %= 24 * 60
    return total // 60, total % 60


def ny_session_to_jst_minutes(ny_start_hm: tuple[int, int],
                               ny_end_hm: tuple[int, int],
                               when: datetime | None = None) -> tuple[int, int]:
    """Convert an NY-local time window (e.g. 9:30-11:00 NY) to JST
    minute-of-day pairs. Returns (start_min, end_min), each 0-1439.

    The caller uses these to compare against `jst_now.hour*60+jst_now.minute`.
    """
    sh, sm = ny_local_hm_to_jst(*ny_start_hm, when=when)
    eh, em = ny_local_hm_to_jst(*ny_end_hm, when=when)
    return sh * 60 + sm, eh * 60 + em
