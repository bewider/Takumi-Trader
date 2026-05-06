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
# │ WINTER (last Sun Oct → last Sun Mar):  ← CURRENTLY ACTIVE      │
# │   12:09-15:44  Afternoon                                        │
# │   15:45-16:25  Frankfurt_open                                   │
# │   16:26-16:44  EU                                               │
# │   16:45-17:35  London_open                                      │
# │   17:36-20:44  London                                           │
# │                                                                 │
# │ SUMMER (last Sun Mar → last Sun Oct):                           │
# │   12:09-14:44  Afternoon                                        │
# │   14:45-15:25  Frankfurt_open                                   │
# │   15:26-15:44  EU                                               │
# │   15:45-16:35  London_open                                      │
# │   16:36-20:44  London                                           │
# │                                                                 │
# │ To switch: just tell Claude "switch to summer/winter times"     │
# └─────────────────────────────────────────────────────────────────┘

_SESSIONS: list[tuple[tuple[int, int], tuple[int, int], str]] = [
    ((7, 0), (8, 44), "Australia"),
    ((8, 45), (9, 35), "Tokyo_open"),
    ((9, 36), (12, 8), "Morning"),
    # ── DST-affected sessions (currently WINTER) ──
    ((12, 9), (15, 44), "Afternoon"),
    ((15, 45), (16, 25), "Frankfurt_open"),
    ((16, 26), (16, 44), "EU"),
    ((16, 45), (17, 35), "London_open"),
    ((17, 36), (20, 44), "London"),
    # ── End DST-affected ──
    ((20, 45), (21, 35), "US_open"),
    ((21, 36), (23, 59), "US"),
    ((0, 0), (5, 0), "US"),
    ((5, 1), (6, 59), "NO_TRADE"),
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


def get_session_for_timestamp(ts: float) -> str:
    """Get the session for an arbitrary unix timestamp.

    Used for backfilling historical trades with session info.
    """
    dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
    return _match_session(_jst_hm(dt_utc))


def get_session_label() -> str:
    """Return the current session name (same as get_current_session)."""
    return get_current_session()


def is_no_trade_session() -> bool:
    """Check if we're in the NO_TRADE window (05:01-06:59 JST)."""
    return get_current_session() == "NO_TRADE"


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
