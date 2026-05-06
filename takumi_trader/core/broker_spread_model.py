"""IC Markets ECN raw account spread + slippage model.

Used by ShadowSimulator (Phase C) to compute pessimistic-but-realistic
entry/exit fills. The point of this model is to bias simulated outcomes
slightly worse than real broker execution, so an edge surviving the
simulation has high confidence of surviving live trading.

Spreads vary by:
    * pair          — major USD pairs ~0.1pt, GBP/NZD crosses up to 1.5+pt
    * session       — Tokyo wider, London/NY-overlap tightest, off-hours wide
    * news proximity — NFP/FOMC/CPI windows spike spreads 5-10x

Initial implementation uses static IC Markets typicals from the design-
review table. Phase F (post-fan-out) replaces the static table with a
dynamic model populated from observed MT5 spreads — but until then,
static defaults reflecting actual IC Markets ECN raw conditions are the
right calibration target.

CRITICAL CALIBRATION NOTE (do not edit without empirical evidence):
The IC Markets pessimism configuration was set by Ryosuke based on his
broker's actual ECN raw spreads (0.0-0.5 typical) and very low slippage.
Numbers tuned for OANDA / Pepperstone / generic forex brokers DO NOT
APPLY here — over-pessimism is just as harmful as under-pessimism in
shadow simulation, because it kills profitable rules during validation
that would actually have survived in live execution. ShadowCalibrationLog
will empirically tighten these from observed (real_pnl - sim_pnl) deltas.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import ClassVar, TYPE_CHECKING

if TYPE_CHECKING:
    from takumi_trader.core.shadow_simulator import ShadowSimulatorConfig


@dataclass
class SpreadLookup:
    """Result of a (pair, time) spread query. All values in price points."""
    spread_points: float          # bid-ask spread (cost paid on entry)
    slippage_points: float        # entry slippage applied past the fill price
    sl_slippage_points: float     # stop-out slippage applied past the SL price
    is_news_window: bool          # whether news pricing is active
    session_key: str              # "tokyo" | "normal" — for diagnostics


class BrokerSpreadModel:
    """IC Markets ECN raw account spread model.

    Lookup: (pair, signal_time_utc) -> SpreadLookup.

    Session classification (UTC hour bucket — three-bucket model per
    architect refinement 2026-05-05):
        00:00-07:00  -> "tokyo"           (Tokyo session — widest spreads)
        07:00-12:00  -> "normal"          (Frankfurt/London — early London entrants
                                            already active; spreads tightening but
                                            not yet at overlap depths)
        12:00-16:00  -> "overlap"         (London + NY simultaneously — TIGHTEST
                                            spreads of the day)
        16:00-21:00  -> "normal"          (NY-only after London close — tighter
                                            than Tokyo, looser than overlap)
        21:00-24:00  -> "tokyo"           (Sydney/early Asia — wider)

    Why three buckets matter: the Sv2 trading window is 07:58-21:59 JST =
    22:58-12:59 UTC, which spans all three regimes. A two-bucket model
    would either over-penalize the 07:00-12:00 London-only hours or
    under-penalize the 22:58-07:00 Tokyo hours. Three buckets calibrate
    correctly across Sv2's actual trading day.

    News windows: empty by default; Phase F integrates with news_filter
    to dynamically classify "near a RED event" cycles. Until then,
    `is_news_window` is always False — see HARDCODED_NEWS_WINDOWS docstring.
    """

    # ── IC Markets typical spreads in points ─────────────────────────
    # Format: pair -> {session_key: spread_points}
    # session_key in {"overlap", "normal", "tokyo", "news"}
    # "overlap" = London-NY overlap (12:00-16:00 UTC) — tightest
    # "normal"  = London-only (07-12 UTC) OR NY-only (16-21 UTC) — moderate
    # "tokyo"   = Tokyo + Sydney (21-07 UTC) — widest
    # "news"    = NFP/FOMC/CPI blackout-window spike pricing (Phase F integration)
    #
    # ── CALIBRATION METHODOLOGY (anchored 2026-05-05) ──
    # Architect-confirmed anchor values: EURUSD, GBPJPY, GBPNZD, XAUUSD.
    # Other pairs interpolate by liquidity tier:
    #
    #   Majors (EURUSD, USDJPY):    overlap ≈ 0.5  × old_normal,  normal = old_normal
    #   Semi-majors:                 overlap ≈ 0.6  × old_normal,  normal ≈ 1.25 × old
    #   JPY crosses:                 overlap ≈ 0.6  × old_normal,  normal ≈ 1.25 × old
    #   Wide crosses:                overlap ≈ 0.67 × old_normal,  normal ≈ 1.2  × old
    #   Asia-Pacific-only (AUDNZD):  near-flat overlap-vs-normal (minimal London benefit)
    #
    # When updating: if you adjust one ratio, audit the peer pairs to maintain
    # internal consistency. Anchored pairs (EURUSD/GBPJPY/GBPNZD/XAUUSD) take
    # precedence over interpolation if they conflict. Per-pair human review
    # surfaced calibration outliers in the C.1 review gate (AUDJPY peer-match
    # to CADJPY, AUDNZD near-flat overlap, GBPNZD/GBPCAD normal not exceeding
    # original single-bucket value); future updates should walk through the
    # full table again the same way.
    IC_MARKETS_SPREADS: ClassVar[dict[str, dict[str, float]]] = {
        # ── JPY pairs ─────────────────────────────────────────────
        "USDJPY": {"overlap": 0.05, "normal": 0.1,  "tokyo": 0.2, "news": 1.5},
        "EURJPY": {"overlap": 0.15, "normal": 0.25, "tokyo": 0.4, "news": 1.8},
        "GBPJPY": {"overlap": 0.3,  "normal": 0.5,  "tokyo": 0.8, "news": 3.0},
        # AUDJPY overlap=0.30 to peer-match CADJPY (post-C.1 review):
        # original 0.20 over-discounted London participation in AUD vs CAD.
        "AUDJPY": {"overlap": 0.3,  "normal": 0.4,  "tokyo": 0.5, "news": 2.0},
        "NZDJPY": {"overlap": 0.35, "normal": 0.6,  "tokyo": 0.8, "news": 2.5},
        "CADJPY": {"overlap": 0.3,  "normal": 0.5,  "tokyo": 0.6, "news": 2.2},
        "CHFJPY": {"overlap": 0.35, "normal": 0.6,  "tokyo": 0.8, "news": 2.5},
        # ── Major USD pairs ───────────────────────────────────────
        "EURUSD": {"overlap": 0.05, "normal": 0.1,  "tokyo": 0.3, "news": 1.5},
        "GBPUSD": {"overlap": 0.15, "normal": 0.25, "tokyo": 0.5, "news": 2.0},
        "AUDUSD": {"overlap": 0.15, "normal": 0.25, "tokyo": 0.4, "news": 1.5},
        "NZDUSD": {"overlap": 0.2,  "normal": 0.35, "tokyo": 0.5, "news": 1.8},
        "USDCAD": {"overlap": 0.15, "normal": 0.25, "tokyo": 0.4, "news": 1.8},
        "USDCHF": {"overlap": 0.15, "normal": 0.25, "tokyo": 0.4, "news": 1.5},
        # ── GBP crosses ───────────────────────────────────────────
        "GBPAUD": {"overlap": 0.6,  "normal": 1.0,  "tokyo": 1.5, "news": 4.0},
        # GBPNZD normal=1.5 anchored to old single-bucket value (post-C.1
        # review): the overlap/normal split must center around the old
        # value, not push normal wider than it. overlap stays at 1.0.
        "GBPNZD": {"overlap": 1.0,  "normal": 1.5,  "tokyo": 2.5, "news": 5.0},
        # GBPCAD normal=1.10 — small tightening toward the original 1.0
        # for the same reason as GBPNZD (post-C.1 review).
        "GBPCAD": {"overlap": 0.7,  "normal": 1.1,  "tokyo": 1.8, "news": 4.5},
        "GBPCHF": {"overlap": 0.6,  "normal": 1.0,  "tokyo": 1.5, "news": 4.0},
        # ── EUR crosses ───────────────────────────────────────────
        "EURAUD": {"overlap": 0.35, "normal": 0.6,  "tokyo": 1.0, "news": 3.0},
        "EURNZD": {"overlap": 0.7,  "normal": 1.2,  "tokyo": 1.8, "news": 4.0},
        "EURCAD": {"overlap": 0.35, "normal": 0.6,  "tokyo": 1.0, "news": 3.0},
        "EURCHF": {"overlap": 0.3,  "normal": 0.5,  "tokyo": 0.8, "news": 2.5},
        "EURGBP": {"overlap": 0.15, "normal": 0.25, "tokyo": 0.4, "news": 1.8},
        # ── AUD/NZD/CAD/CHF crosses ───────────────────────────────
        "AUDCAD": {"overlap": 0.35, "normal": 0.6,  "tokyo": 1.0, "news": 3.0},
        "AUDCHF": {"overlap": 0.35, "normal": 0.6,  "tokyo": 1.0, "news": 3.0},
        # AUDNZD overlap=0.55 — near-flat with normal (post-C.1 review).
        # Asia-Pacific-only liquidity profile: minimal London benefit, so
        # overlap doesn't tighten as much as for crosses with USD/EUR/GBP
        # London participation. 0.05 differential preserves a tiny benefit.
        "AUDNZD": {"overlap": 0.55, "normal": 0.6,  "tokyo": 1.0, "news": 3.0},
        "NZDCAD": {"overlap": 0.4,  "normal": 0.75, "tokyo": 1.2, "news": 3.5},
        "NZDCHF": {"overlap": 0.4,  "normal": 0.75, "tokyo": 1.2, "news": 3.5},
        "CADCHF": {"overlap": 0.35, "normal": 0.6,  "tokyo": 1.0, "news": 3.0},
        # ── Gold ──────────────────────────────────────────────────
        "XAUUSD": {"overlap": 1.0,  "normal": 1.8,  "tokyo": 2.5, "news": 8.0},
    }

    # Conservative fallback for any pair not in the table above.
    # Logged at WARNING the first time it's used so we know to add the pair.
    _FALLBACK_SPREAD: ClassVar[dict[str, float]] = {
        "overlap": 1.5, "normal": 2.0, "tokyo": 3.0, "news": 6.0,
    }

    # Hardcoded news windows for Phase C.1 — list of (event, start_utc, end_utc) tuples.
    #
    # TODO(Phase-F): integrate news_filter.is_blackout(pair, time) here.
    # Until then, is_news_window always returns False — the simulator
    # uses normal/overlap/tokyo slippage even during NFP/FOMC/CPI windows.
    # This UNDERESTIMATES real friction on news days.
    #
    # Predicted calibration-log signature when this gap matters: on days
    # with high-impact USD events, mean(real_pnl - sim_pnl) will become
    # NEGATIVE (sim outprints real because sim isn't paying news-spread
    # cost). Drift detection in C.4 will surface this; it's an EXPECTED
    # gap until Phase F closes it, not a simulator bug.
    HARDCODED_NEWS_WINDOWS: ClassVar[list[tuple[str, datetime, datetime]]] = [
        # Empty until Phase F news_filter integration
    ]

    def __init__(self, config: "ShadowSimulatorConfig") -> None:
        """Construct with a ShadowSimulatorConfig so slippage values come
        from the canonical config rather than being duplicated here."""
        self.config = config
        # Track unknown pairs so we only WARN once per pair, not per call
        self._unknown_pair_logged: set[str] = set()

    # ── Session classification ──────────────────────────────────────

    @staticmethod
    def classify_session(signal_time_utc: float) -> str:
        """Map epoch UTC seconds to session bucket.

        Three buckets (lower-bound inclusive, upper-bound exclusive):
            12:00-16:00 UTC -> 'overlap'   (London + NY simultaneously)
            07:00-12:00 UTC -> 'normal'    (Frankfurt/London-only)
            16:00-21:00 UTC -> 'normal'    (NY-only after London close)
            00:00-07:00 UTC -> 'tokyo'     (Tokyo session — widest)
            21:00-24:00 UTC -> 'tokyo'     (Sydney + early Asia)

        The 07:00 boundary is deliberate: Frankfurt is open from 06:00 UTC
        and London participants are entering by 07:00. Spreads at 07:00
        are noticeably tighter than 03:00, so lumping 07:00-08:00 with
        Tokyo (as the previous two-bucket model did) over-penalized that
        hour. The 12:00-16:00 'overlap' is the genuinely tightest window
        when both London and NY are simultaneously active; carving it
        out from 'normal' lets pessimism calibration distinguish the
        two regimes Sv2 actually trades across.
        """
        dt = datetime.fromtimestamp(signal_time_utc, tz=timezone.utc)
        h = dt.hour
        if 12 <= h < 16:
            return "overlap"
        if 7 <= h < 12 or 16 <= h < 21:
            return "normal"
        return "tokyo"

    def _is_news_active(self, signal_time_utc: float) -> bool:
        """True if the timestamp falls inside any hardcoded news window."""
        if not self.HARDCODED_NEWS_WINDOWS:
            return False
        dt = datetime.fromtimestamp(signal_time_utc, tz=timezone.utc)
        for _name, start, end in self.HARDCODED_NEWS_WINDOWS:
            if start <= dt <= end:
                return True
        return False

    # ── Public lookup ───────────────────────────────────────────────

    def lookup(self, pair: str, signal_time_utc: float) -> SpreadLookup:
        """Return SpreadLookup for the given (pair, time)."""
        is_news = self._is_news_active(signal_time_utc)
        session_key = "news" if is_news else self.classify_session(signal_time_utc)

        # Resolve spread from the per-pair table or fall back
        spreads = self.IC_MARKETS_SPREADS.get(pair)
        if spreads is None:
            if pair not in self._unknown_pair_logged:
                import logging
                logging.getLogger(__name__).warning(
                    "[SPREAD] unknown pair %r — using conservative fallback "
                    "(spread=%.1fpt). Add to IC_MARKETS_SPREADS table.",
                    pair, self._FALLBACK_SPREAD[session_key],
                )
                self._unknown_pair_logged.add(pair)
            spread = self._FALLBACK_SPREAD[session_key]
        else:
            spread = spreads.get(session_key, spreads["normal"])

        # Slippage from config — gold vs forex, normal vs news
        is_gold = self.is_gold(pair)
        if is_gold:
            slip = (
                self.config.slippage_points_gold_news if is_news
                else self.config.slippage_points_gold_normal
            )
            sl_slip = self.config.sl_slippage_points_gold
        else:
            slip = (
                self.config.slippage_points_forex_news if is_news
                else self.config.slippage_points_forex_normal
            )
            sl_slip = self.config.sl_slippage_points_forex

        return SpreadLookup(
            spread_points=spread,
            slippage_points=slip,
            sl_slippage_points=sl_slip,
            is_news_window=is_news,
            session_key=session_key,
        )

    @staticmethod
    def is_gold(pair: str) -> bool:
        """XAUUSD detection. AU Gold strategies use the same symbol via
        broker-specific resolver (XAUUSD.raw, XAUUSDm, etc.) but the
        unprefixed canonical name is what reaches this layer."""
        return pair == "XAUUSD"
