"""QM4 Standalone Alert Engine — extreme currency strength detection.

Fires alerts when any single currency shows extreme weakness or strength
across three independent alert types, plus a 4th PAIR type that flags
high-conviction trade opportunities when strong and weak currencies align.

── Alert Type 1: MTF (Medium Timeframe) ────────────────────────────────
  Watches M15 + H1 + H4. Two trigger modes (OR logic):
  1. Individual: ALL three TFs at extreme (≤0.3 weak / ≥9.7 strong)
  2. Cumulative: SUM of three TFs at extreme (≤0.9 weak / ≥29.1 strong)

── Alert Type 2: HTF (High Timeframe) ──────────────────────────────────
  Watches D1 + W1 + MN. Two trigger modes (OR logic):
  1. Individual: ALL three TFs at extreme (≤1.0 weak / ≥9.0 strong)
  2. Cumulative: SUM of three TFs at extreme (≤4.0 weak / ≥26.0 strong)

── Alert Type 3: XHTF (Extreme High Timeframe) ─────────────────────────
  Watches D1 + W1 + MN. Individual only — tighter thresholds:
  ALL three TFs at extreme (≤0.4 weak / ≥9.6 strong)

── Alert Type 4: PAIR (Trade Suggestion) ───────────────────────────────
  Fires when one currency is STRONG and another is WEAK under the same
  alert type (MTF / HTF / XHTF). The result is a directional trade idea:
  BUY  = strong base + weak quote  (e.g. GBP STRONG + JPY WEAK → BUY GBPJPY)
  SELL = weak base  + strong quote (e.g. AUD WEAK + USD STRONG → SELL AUDUSD)

Each alert type has its own cooldown key so all four can fire
independently for the same currency / pair.

Note: HTF and XHTF watch the same TFs (D1/W1/MN) but XHTF requires
tighter extremes. XHTF firing implies HTF would also have fired, but
they carry separate notifications and cooldowns.

Alignment score: count of 6 TFs (M15/H1/H4/D1/W1/MN) where the score
is in the extreme zone (≤2.0 weak / ≥8.0 strong).

Depth %: how far past the threshold the current reading is, expressed
as a percentage of the threshold range.

Separate from the main TAKUMI alert pipeline. Uses its own sound file
and cooldown tracking so alerts are instantly distinguishable by ear.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "NZD", "CHF"]

# MTF group: medium timeframes (fast momentum signal)
MTF_TFS = ["M15", "H1", "H4"]

# HTF / XHTF group: high timeframes (macro bias)
HTF_TFS = ["D1", "W1", "MN"]

# All 6 TFs tracked — used for alignment scoring
ALL_TFS = ["M15", "H1", "H4", "D1", "W1", "MN"]

# Alignment thresholds — TF is "extreme" if score crosses these
ALIGN_WEAK   = 2.0   # score ≤ this counts as weak-aligned
ALIGN_STRONG = 8.0   # score ≥ this counts as strong-aligned


def _tfs_for_alert(alert_type: str) -> tuple[str, ...]:
    """Map an alert type to the TFs used for counter-currency selection.

    This ensures pair selection is scored on the SAME timeframes that
    triggered the alert — avoiding cases where HTF scores dilute a
    short-term momentum thesis (or vice versa). Example: an MTF alert
    fires because M15+H1+H4 are all extreme; we should rank counter
    currencies by those same 3 TFs, not by a 6-TF average that could
    be biased by D1/W1/MN.
    """
    if alert_type in ("MTF", "MTFC"):
        return ("M15", "H1", "H4")
    if alert_type in ("HTF", "HTFC", "XHTF"):
        return ("D1", "W1", "MN")
    if alert_type == "CUM":
        return ("M15", "H1", "H4", "D1", "W1", "MN")
    return ("M15", "H1", "H4", "D1", "W1", "MN")  # safe fallback

# All 28 standard FX pairs (used for pair alerts)
ALL_28_PAIRS = [
    "EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
    "EURGBP", "EURAUD", "EURNZD", "EURCAD", "EURCHF", "EURJPY",
    "GBPAUD", "GBPNZD", "GBPCAD", "GBPCHF", "GBPJPY",
    "AUDNZD", "AUDCAD", "AUDCHF", "AUDJPY",
    "NZDCAD", "NZDCHF", "NZDJPY",
    "CADCHF", "CADJPY",
    "CHFJPY",
]

_ALL_28_SET: frozenset[str] = frozenset(ALL_28_PAIRS)


def _canonical_pair(a: str, b: str) -> str:
    """Return the canonical market pair name for currencies a and b."""
    if (a + b) in _ALL_28_SET:
        return a + b
    return b + a


# ── Dataclasses ──────────────────────────────────────────────────────────


@dataclass
class QM4Alert:
    """A single QM4 extreme-strength alert for a currency."""

    currency: str                  # e.g. "JPY"
    direction: str                 # "WEAK" or "STRONG"
    alert_type: str                # "MTF", "HTF", or "XHTF"
    tf_scores: dict[str, float]    # e.g. {"M15": 0.2, "H1": 0.1, "H4": 0.3}
    reason: str                    # "individual" or "cumulative"
    cumulative: float              # sum of the 3 TF scores
    alignment: int = 0             # count of 6 TFs in extreme zone (0-6)
    depth_pct: float = 0.0         # how far past threshold (%)
    best_pair: str = ""            # suggested trade pair, e.g. "GBPJPY"
    # All candidate pairs sorted by counter-strength (best first).
    # Used as fallback when the best pair is blocked by filters.
    candidate_pairs: list[str] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)

    @property
    def label(self) -> str:
        """Human-readable alert label."""
        parts = " | ".join(f"{tf}={v:.1f}" for tf, v in self.tf_scores.items())
        pair_hint = f" → {self.best_pair}" if self.best_pair else ""
        return (
            f"[{self.alert_type}] {self.currency} {self.direction}"
            f" ({parts}) sum={self.cumulative:.1f}"
            f" align={self.alignment}/6 depth={self.depth_pct:.0f}%"
            f" [{self.reason}]{pair_hint}"
        )


@dataclass
class QM4PairAlert:
    """A QM4 trade-opportunity alert: strong currency + weak currency = pair."""

    pair: str                      # e.g. "GBPJPY"
    direction: str                 # "BUY" or "SELL"
    alert_type: str                # "MTF", "HTF", or "XHTF"
    trigger_type: str              # "individual" or "cumulative"
    base_scores: dict[str, float]  # TF scores for base currency
    quote_scores: dict[str, float] # TF scores for quote currency
    base_alignment: int            # alignment count for base currency (0-6)
    quote_alignment: int           # alignment count for quote currency (0-6)
    spread: float                  # avg(strong_6tf) - avg(weak_6tf) — conviction
    timestamp: float = field(default_factory=time.time)

    @property
    def label(self) -> str:
        """Human-readable pair alert label."""
        base = self.pair[:3]
        quote = self.pair[3:]
        base_parts = " ".join(f"{tf}={v:.1f}" for tf, v in self.base_scores.items())
        quote_parts = " ".join(f"{tf}={v:.1f}" for tf, v in self.quote_scores.items())
        return (
            f"[{self.alert_type} PAIR] {self.pair} {self.direction}"
            f"  {base}:[{base_parts}] align={self.base_alignment}/6"
            f"  {quote}:[{quote_parts}] align={self.quote_alignment}/6"
            f"  spread={self.spread:.1f} [{self.trigger_type}]"
        )


# ── Engine ────────────────────────────────────────────────────────────────


class QM4AlertEngine:
    """Watches currency scores for extreme conditions across four alert types.

    MTF  (M15/H1/H4)  — individual only: all 3 TFs at extreme
    HTF  (D1/W1/MN)   — individual only: all 3 TFs at extreme
    XHTF (D1/W1/MN)   — individual only: tighter thresholds
    CUM  (all 6 TFs)  — cumulative: sum of M15+H1+H4+D1+W1+MN
    PAIR              — strong + weak confluence → trade suggestion

    Accepts scores in the same format as compute_scores():
        {(currency, tf_label): float}  e.g. {("JPY", "M15"): 0.2, ...}

    Returns a list of QM4Alert / QM4PairAlert for any that just triggered.
    Per-currency, per-type, per-direction cooldown prevents alert spam.
    """

    # ── MTF Thresholds (M15 / H1 / H4) — individual only ─────────
    MTF_WEAK_INDIVIDUAL   = 0.3    # Each TF must be ≤ this
    MTF_STRONG_INDIVIDUAL = 9.7    # Each TF must be ≥ this

    # ── HTF Thresholds (D1 / W1 / MN) — individual only ──────────
    HTF_WEAK_INDIVIDUAL   = 1.0    # Each TF must be ≤ this
    HTF_STRONG_INDIVIDUAL = 9.0    # Each TF must be ≥ this

    # ── XHTF Thresholds (D1 / W1 / MN — tighter) — individual only
    XHTF_WEAK_INDIVIDUAL   = 0.4   # Each TF must be ≤ this
    XHTF_STRONG_INDIVIDUAL = 9.6   # Each TF must be ≥ this

    # ── MTFC Thresholds (M15 + H1 + H4 summed) ──────────────────
    MTFC_WEAK_CUMULATIVE   = 0.9   # Sum of 3 MTF TFs must be ≤ this
    MTFC_STRONG_CUMULATIVE = 29.1  # Sum of 3 MTF TFs must be ≥ this

    # ── HTFC Thresholds (D1 + W1 + MN summed) ────────────────────
    HTFC_WEAK_CUMULATIVE   = 4.0   # Sum of 3 HTF TFs must be ≤ this
    HTFC_STRONG_CUMULATIVE = 26.0  # Sum of 3 HTF TFs must be ≥ this

    # ── CUM Thresholds (all 6 TFs summed) ─────────────────────────
    CUM_WEAK_CUMULATIVE   = 8.0    # Sum of 6 TFs must be ≤ this
    CUM_STRONG_CUMULATIVE = 52.0   # Sum of 6 TFs must be ≥ this

    def __init__(self, cooldown_seconds: int = 300) -> None:
        """Initialize the QM4 alert engine.

        Args:
            cooldown_seconds: Minimum seconds between alerts for the same
                              currency + type + direction combination.
                              Default 5 minutes.
        """
        self.cooldown_seconds = cooldown_seconds
        # key: "{currency}_{alert_type}_{direction}"  e.g. "JPY_MTF_WEAK"
        # or:  "{pair}_{alert_type}_{direction}"       e.g. "GBPJPY_HTF_BUY"
        self._last_alert_time: dict[str, float] = {}
        # Confirmation counter: requires 2 consecutive reads at extreme
        # before firing. Filters out single-cycle OCR misreads.
        self._confirm_count: dict[str, int] = {}
        self._CONFIRM_REQUIRED = 2

    # ── Public API ────────────────────────────────────────────────

    def check(
        self, scores: dict[tuple[str, str], float]
    ) -> list[QM4Alert | QM4PairAlert]:
        """Check all currencies and pairs for extreme conditions.

        Args:
            scores: {(currency, tf_label): score_0_to_10}
                    Same format returned by takumi_meter.compute_scores().

        Returns:
            List of QM4Alert / QM4PairAlert for newly triggered items.
        """
        # Track which keys are attempted this cycle; reset others at end
        self._fired_keys_this_cycle: set[str] = set()

        # ── 1. Gather all-TF scores per currency (alignment + spread) ──
        ccy_all: dict[str, dict[str, float]] = {}
        for ccy in CURRENCIES:
            tf_map: dict[str, float] = {}
            for tf in ALL_TFS:
                v = scores.get((ccy, tf))
                if v is not None:
                    tf_map[tf] = v
            ccy_all[ccy] = tf_map

        # ── 2. Currency extreme status per alert type (reused for pairs) ─
        # {ccy: {"MTF": ..., "HTF": ..., "XHTF": ..., "HTFC": ..., "CUM": ...}}
        ccy_status: dict[str, dict[str, str | None]] = {
            ccy: {"MTF": None, "MTFC": None, "HTF": None, "XHTF": None, "HTFC": None, "CUM": None}
            for ccy in CURRENCIES
        }
        for ccy in CURRENCIES:
            mtf_s = self._gather(scores, ccy, MTF_TFS)
            if mtf_s is not None:
                ccy_status[ccy]["MTF"]  = self._extreme_direction_mtf(mtf_s)
                ccy_status[ccy]["MTFC"] = self._extreme_direction_mtfc(mtf_s)
            htf_s = self._gather(scores, ccy, HTF_TFS)
            if htf_s is not None:
                ccy_status[ccy]["HTF"]  = self._extreme_direction_htf(htf_s)
                ccy_status[ccy]["XHTF"] = self._extreme_direction_xhtf(htf_s)
                ccy_status[ccy]["HTFC"] = self._extreme_direction_htfc(htf_s)
            all_s = self._gather(scores, ccy, ALL_TFS)
            if all_s is not None:
                ccy_status[ccy]["CUM"] = self._extreme_direction_cum(all_s)

        # ── 3. Fire currency alerts ───────────────────────────────────
        currency_alerts: list[QM4Alert] = []
        for ccy in CURRENCIES:
            mtf_s = self._gather(scores, ccy, MTF_TFS)
            if mtf_s is not None:
                alert = self._check_mtf(ccy, mtf_s)
                if alert is not None:
                    currency_alerts.append(alert)

                alert = self._check_mtfc(ccy, mtf_s)
                if alert is not None:
                    currency_alerts.append(alert)

            htf_s = self._gather(scores, ccy, HTF_TFS)
            if htf_s is not None:
                alert = self._check_htf(ccy, htf_s)
                if alert is not None:
                    currency_alerts.append(alert)

                alert = self._check_xhtf(ccy, htf_s)
                if alert is not None:
                    currency_alerts.append(alert)

                alert = self._check_htfc(ccy, htf_s)
                if alert is not None:
                    currency_alerts.append(alert)

            all_s = self._gather(scores, ccy, ALL_TFS)
            if all_s is not None:
                alert = self._check_cum(ccy, all_s)
                if alert is not None:
                    currency_alerts.append(alert)

        # ── 4. Enrich currency alerts with alignment / depth / pair ──
        for alert in currency_alerts:
            all_s = ccy_all.get(alert.currency, {})
            alert.alignment = self._compute_alignment(all_s, alert.direction)
            alert.depth_pct = self._compute_depth(alert)
            alert.best_pair = self._find_best_pair(
                alert.currency, alert.direction, ccy_all, alert.alert_type
            )
            alert.candidate_pairs = self._find_candidate_pairs(
                alert.currency, alert.direction, ccy_all, alert.alert_type
            )

        # ── 5. Pair alerts ────────────────────────────────────────────
        pair_alerts = self._check_pairs(scores, ccy_status, ccy_all)

        # Reset confirmation counters for conditions that were NOT met this cycle.
        # If a condition disappears (OCR misread last cycle), its counter resets.
        stale = [k for k in self._confirm_count if k not in self._fired_keys_this_cycle]
        for k in stale:
            del self._confirm_count[k]

        return currency_alerts + pair_alerts

    # ── Currency direction detectors (pure, no side-effects) ──────

    def _extreme_direction_mtf(self, tf_s: dict[str, float]) -> str | None:
        m15, h1, h4 = tf_s["M15"], tf_s["H1"], tf_s["H4"]
        if (m15 <= self.MTF_WEAK_INDIVIDUAL
                and h1 <= self.MTF_WEAK_INDIVIDUAL
                and h4 <= self.MTF_WEAK_INDIVIDUAL):
            return "WEAK"
        if (m15 >= self.MTF_STRONG_INDIVIDUAL
                and h1 >= self.MTF_STRONG_INDIVIDUAL
                and h4 >= self.MTF_STRONG_INDIVIDUAL):
            return "STRONG"
        return None

    def _extreme_direction_htf(self, tf_s: dict[str, float]) -> str | None:
        d1, w1, mn = tf_s["D1"], tf_s["W1"], tf_s["MN"]
        if (d1 <= self.HTF_WEAK_INDIVIDUAL
                and w1 <= self.HTF_WEAK_INDIVIDUAL
                and mn <= self.HTF_WEAK_INDIVIDUAL):
            return "WEAK"
        if (d1 >= self.HTF_STRONG_INDIVIDUAL
                and w1 >= self.HTF_STRONG_INDIVIDUAL
                and mn >= self.HTF_STRONG_INDIVIDUAL):
            return "STRONG"
        return None

    def _extreme_direction_xhtf(self, tf_s: dict[str, float]) -> str | None:
        d1, w1, mn = tf_s["D1"], tf_s["W1"], tf_s["MN"]
        if (
            d1 <= self.XHTF_WEAK_INDIVIDUAL
            and w1 <= self.XHTF_WEAK_INDIVIDUAL
            and mn <= self.XHTF_WEAK_INDIVIDUAL
        ):
            return "WEAK"
        if (
            d1 >= self.XHTF_STRONG_INDIVIDUAL
            and w1 >= self.XHTF_STRONG_INDIVIDUAL
            and mn >= self.XHTF_STRONG_INDIVIDUAL
        ):
            return "STRONG"
        return None

    def _extreme_direction_mtfc(self, tf_s: dict[str, float]) -> str | None:
        total = tf_s["M15"] + tf_s["H1"] + tf_s["H4"]
        if total <= self.MTFC_WEAK_CUMULATIVE:
            return "WEAK"
        if total >= self.MTFC_STRONG_CUMULATIVE:
            return "STRONG"
        return None

    def _extreme_direction_htfc(self, tf_s: dict[str, float]) -> str | None:
        total = tf_s["D1"] + tf_s["W1"] + tf_s["MN"]
        if total <= self.HTFC_WEAK_CUMULATIVE:
            return "WEAK"
        if total >= self.HTFC_STRONG_CUMULATIVE:
            return "STRONG"
        return None

    def _extreme_direction_cum(self, tf_s: dict[str, float]) -> str | None:
        total = sum(tf_s.values())
        if total <= self.CUM_WEAK_CUMULATIVE:
            return "WEAK"
        if total >= self.CUM_STRONG_CUMULATIVE:
            return "STRONG"
        return None

    # ── Internal alert checkers ───────────────────────────────────

    def _check_cum(
        self, ccy: str, tf_scores: dict[str, float]
    ) -> QM4Alert | None:
        """Check CUM (all 6 TFs summed) for extreme conditions."""
        total = sum(tf_scores.values())

        if total <= self.CUM_WEAK_CUMULATIVE:
            return self._try_fire(
                ccy, "WEAK", "CUM", tf_scores, "cumulative", total
            )
        if total >= self.CUM_STRONG_CUMULATIVE:
            return self._try_fire(
                ccy, "STRONG", "CUM", tf_scores, "cumulative", total
            )
        return None

    def _check_mtfc(
        self, ccy: str, tf_scores: dict[str, float]
    ) -> QM4Alert | None:
        """Check MTFC (M15+H1+H4 summed) for extreme conditions."""
        total = tf_scores["M15"] + tf_scores["H1"] + tf_scores["H4"]

        if total <= self.MTFC_WEAK_CUMULATIVE:
            return self._try_fire(
                ccy, "WEAK", "MTFC", tf_scores, "cumulative", total
            )
        if total >= self.MTFC_STRONG_CUMULATIVE:
            return self._try_fire(
                ccy, "STRONG", "MTFC", tf_scores, "cumulative", total
            )
        return None

    def _check_htfc(
        self, ccy: str, tf_scores: dict[str, float]
    ) -> QM4Alert | None:
        """Check HTFC (D1+W1+MN summed) for extreme conditions."""
        d1 = tf_scores["D1"]
        w1 = tf_scores["W1"]
        mn = tf_scores["MN"]
        total = d1 + w1 + mn

        if total <= self.HTFC_WEAK_CUMULATIVE:
            return self._try_fire(
                ccy, "WEAK", "HTFC", tf_scores, "cumulative", total
            )
        if total >= self.HTFC_STRONG_CUMULATIVE:
            return self._try_fire(
                ccy, "STRONG", "HTFC", tf_scores, "cumulative", total
            )
        return None

    def _check_mtf(
        self, ccy: str, tf_scores: dict[str, float]
    ) -> QM4Alert | None:
        """Check MTF (M15/H1/H4) — individual only."""
        m15   = tf_scores["M15"]
        h1    = tf_scores["H1"]
        h4    = tf_scores["H4"]
        total = m15 + h1 + h4

        if (m15 <= self.MTF_WEAK_INDIVIDUAL
                and h1 <= self.MTF_WEAK_INDIVIDUAL
                and h4 <= self.MTF_WEAK_INDIVIDUAL):
            return self._try_fire(ccy, "WEAK", "MTF", tf_scores, "individual", total)

        if (m15 >= self.MTF_STRONG_INDIVIDUAL
                and h1 >= self.MTF_STRONG_INDIVIDUAL
                and h4 >= self.MTF_STRONG_INDIVIDUAL):
            return self._try_fire(ccy, "STRONG", "MTF", tf_scores, "individual", total)

        return None

    def _check_htf(
        self, ccy: str, tf_scores: dict[str, float]
    ) -> QM4Alert | None:
        """Check HTF (D1/W1/MN) — individual only."""
        d1    = tf_scores["D1"]
        w1    = tf_scores["W1"]
        mn    = tf_scores["MN"]
        total = d1 + w1 + mn

        if (d1 <= self.HTF_WEAK_INDIVIDUAL
                and w1 <= self.HTF_WEAK_INDIVIDUAL
                and mn <= self.HTF_WEAK_INDIVIDUAL):
            return self._try_fire(ccy, "WEAK", "HTF", tf_scores, "individual", total)

        if (d1 >= self.HTF_STRONG_INDIVIDUAL
                and w1 >= self.HTF_STRONG_INDIVIDUAL
                and mn >= self.HTF_STRONG_INDIVIDUAL):
            return self._try_fire(ccy, "STRONG", "HTF", tf_scores, "individual", total)

        return None

    def _check_xhtf(
        self, ccy: str, tf_scores: dict[str, float]
    ) -> QM4Alert | None:
        """Check XHTF (D1/W1/MN) — tighter thresholds, individual only."""
        d1    = tf_scores["D1"]
        w1    = tf_scores["W1"]
        mn    = tf_scores["MN"]
        total = d1 + w1 + mn

        if (
            d1 <= self.XHTF_WEAK_INDIVIDUAL
            and w1 <= self.XHTF_WEAK_INDIVIDUAL
            and mn <= self.XHTF_WEAK_INDIVIDUAL
        ):
            return self._try_fire(
                ccy, "WEAK", "XHTF", tf_scores, "individual", total
            )

        if (
            d1 >= self.XHTF_STRONG_INDIVIDUAL
            and w1 >= self.XHTF_STRONG_INDIVIDUAL
            and mn >= self.XHTF_STRONG_INDIVIDUAL
        ):
            return self._try_fire(
                ccy, "STRONG", "XHTF", tf_scores, "individual", total
            )

        return None

    # ── Pair alert checker ────────────────────────────────────────

    def _check_pairs(
        self,
        scores: dict[tuple[str, str], float],
        ccy_status: dict[str, dict[str, str | None]],
        ccy_all: dict[str, dict[str, float]],
    ) -> list[QM4PairAlert]:
        """Scan all 28 pairs for strong+weak confluence and fire pair alerts."""
        alerts: list[QM4PairAlert] = []

        for pair in ALL_28_PAIRS:
            base  = pair[:3]
            quote = pair[3:]

            if base not in ccy_status or quote not in ccy_status:
                continue

            for atype in ("MTF", "MTFC", "HTF", "XHTF", "HTFC", "CUM"):
                if atype in ("MTF", "MTFC"):
                    tfs = MTF_TFS
                elif atype == "CUM":
                    tfs = ALL_TFS
                else:  # HTF, XHTF, HTFC
                    tfs = HTF_TFS
                base_dir  = ccy_status[base].get(atype)
                quote_dir = ccy_status[quote].get(atype)

                base_s  = self._gather(scores, base,  tfs) or {}
                quote_s = self._gather(scores, quote, tfs) or {}

                # BUY: base STRONG + quote WEAK
                if base_dir == "STRONG" and quote_dir == "WEAK":
                    trigger = self._pair_trigger(base_s, quote_s, atype, "BUY")
                    spread  = self._compute_spread(ccy_all, base, quote)
                    b_align = self._compute_alignment(
                        ccy_all.get(base, {}), "STRONG"
                    )
                    q_align = self._compute_alignment(
                        ccy_all.get(quote, {}), "WEAK"
                    )
                    alert = self._try_fire_pair(
                        pair, "BUY", atype, trigger,
                        base_s, quote_s, b_align, q_align, spread,
                    )
                    if alert:
                        alerts.append(alert)

                # SELL: base WEAK + quote STRONG
                elif base_dir == "WEAK" and quote_dir == "STRONG":
                    trigger = self._pair_trigger(base_s, quote_s, atype, "SELL")
                    spread  = self._compute_spread(ccy_all, quote, base)
                    b_align = self._compute_alignment(
                        ccy_all.get(base, {}), "WEAK"
                    )
                    q_align = self._compute_alignment(
                        ccy_all.get(quote, {}), "STRONG"
                    )
                    alert = self._try_fire_pair(
                        pair, "SELL", atype, trigger,
                        base_s, quote_s, b_align, q_align, spread,
                    )
                    if alert:
                        alerts.append(alert)

        return alerts

    # ── Enrichment helpers ────────────────────────────────────────

    def _compute_alignment(
        self, all_scores: dict[str, float], direction: str
    ) -> int:
        """Count of 6 TFs in the extreme zone for the given direction."""
        count = 0
        for tf in ALL_TFS:
            v = all_scores.get(tf)
            if v is None:
                continue
            if direction == "WEAK" and v <= ALIGN_WEAK:
                count += 1
            elif direction == "STRONG" and v >= ALIGN_STRONG:
                count += 1
        return count

    def _compute_depth(self, alert: QM4Alert) -> float:
        """Depth past threshold as a percentage.

        WEAK:   (threshold_sum - actual_sum) / threshold_sum * 100
        STRONG: (actual_sum - threshold_sum) / (max - threshold_sum) * 100
        """
        if alert.alert_type == "CUM":
            MAX_SUM = 60.0  # 6 TFs × 10.0
            th_weak   = self.CUM_WEAK_CUMULATIVE     # 8.0
            th_strong = self.CUM_STRONG_CUMULATIVE   # 52.0
        else:
            MAX_SUM = 30.0  # 3 TFs × 10.0
            if alert.alert_type == "MTFC":
                th_weak   = self.MTFC_WEAK_CUMULATIVE   # 0.9
                th_strong = self.MTFC_STRONG_CUMULATIVE # 29.1
            elif alert.alert_type == "MTF":
                th_weak   = self.MTF_WEAK_INDIVIDUAL   * 3  # 0.9
                th_strong = self.MTF_STRONG_INDIVIDUAL * 3  # 29.1
            elif alert.alert_type == "HTFC":
                th_weak   = self.HTFC_WEAK_CUMULATIVE   # 4.0
                th_strong = self.HTFC_STRONG_CUMULATIVE # 26.0
            elif alert.alert_type == "HTF":
                th_weak   = self.HTF_WEAK_INDIVIDUAL   * 3  # 3.0
                th_strong = self.HTF_STRONG_INDIVIDUAL * 3  # 27.0
            else:  # XHTF
                th_weak   = self.XHTF_WEAK_INDIVIDUAL   * 3  # 1.2
                th_strong = self.XHTF_STRONG_INDIVIDUAL * 3  # 28.8

        if alert.direction == "WEAK":
            if th_weak <= 0:
                return 0.0
            return round(
                (th_weak - alert.cumulative) / th_weak * 100, 1
            )
        else:
            denom = MAX_SUM - th_strong
            if denom <= 0:
                return 0.0
            return round(
                (alert.cumulative - th_strong) / denom * 100, 1
            )

    def _find_best_pair(
        self,
        ccy: str,
        direction: str,
        ccy_all: dict[str, dict[str, float]],
        alert_type: str = "",
    ) -> str:
        """Find the most extreme counter-currency and return the canonical pair.

        For a WEAK currency → find the STRONGEST counter-currency.
        For a STRONG currency → find the WEAKEST counter-currency.
        Returns empty string if no suitable counter is found.

        Counter selection is scored on the same TFs that triggered the alert
        (via `_tfs_for_alert`). This prevents an HTF-biased average from
        picking a counter that is actively moving the same way on the short
        TFs — the known CHFJPY-at-17:09 whipsaw case.
        """
        tfs = _tfs_for_alert(alert_type)
        own_vals = [ccy_all.get(ccy, {}).get(tf, 5.0) for tf in tfs]
        own_avg  = sum(own_vals) / len(own_vals) if own_vals else 5.0

        best_ccy  = ""
        best_diff = -1.0

        for c in CURRENCIES:
            if c == ccy:
                continue
            c_tfs = ccy_all.get(c, {})
            if not c_tfs:
                continue
            vals = [c_tfs.get(tf, 5.0) for tf in tfs]
            avg = sum(vals) / len(vals)

            if direction == "WEAK":
                diff = avg - own_avg          # we want the STRONGEST counter
            else:
                diff = own_avg - avg          # we want the WEAKEST counter

            if diff > best_diff:
                best_diff = diff
                best_ccy  = c

        if not best_ccy:
            return ""

        # Build canonical pair: STRONG currency = base
        if direction == "WEAK":
            return _canonical_pair(best_ccy, ccy)   # best_ccy STRONG / ccy WEAK
        else:
            return _canonical_pair(ccy, best_ccy)   # ccy STRONG / best_ccy WEAK

    def _find_candidate_pairs(
        self,
        ccy: str,
        direction: str,
        ccy_all: dict[str, dict[str, float]],
        alert_type: str = "",
    ) -> list[str]:
        """Return ALL candidate pairs sorted by alert-matched-TF spread.

        Counters are ranked using the SAME timeframes that triggered the
        alert (via `_tfs_for_alert`): MTF/MTFC → M15+H1+H4, HTF/HTFC/XHTF →
        D1+W1+MN, CUM → all 6. The pair with the widest divergence on
        those TFs comes first.

        Used as fallback list when the best pair is blocked by filters.
        """
        tfs = _tfs_for_alert(alert_type)
        own_tfs = ccy_all.get(ccy, {})
        own_sum = sum(own_tfs.get(tf, 5.0) for tf in tfs)

        scored: list[tuple[float, str]] = []
        for c in CURRENCIES:
            if c == ccy:
                continue
            c_tfs = ccy_all.get(c, {})
            if not c_tfs:
                continue
            c_sum = sum(c_tfs.get(tf, 5.0) for tf in tfs)
            if direction == "WEAK":
                # Weak ccy — we want the counter with highest sum on these TFs
                spread = c_sum - own_sum
            else:
                # Strong ccy — we want the counter with lowest sum on these TFs
                spread = own_sum - c_sum
            if spread > 0:  # only pairs with positive differential
                scored.append((spread, c))

        # Sort by spread descending (widest divergence first)
        scored.sort(key=lambda x: -x[0])

        pairs: list[str] = []
        for _, c in scored:
            if direction == "WEAK":
                pairs.append(_canonical_pair(c, ccy))
            else:
                pairs.append(_canonical_pair(ccy, c))
        return pairs

    def _compute_spread(
        self,
        ccy_all: dict[str, dict[str, float]],
        strong_ccy: str,
        weak_ccy: str,
    ) -> float:
        """Strength differential (strong avg - weak avg) across all 6 TFs."""
        sv = list(ccy_all.get(strong_ccy, {}).values())
        wv = list(ccy_all.get(weak_ccy, {}).values())
        s_avg = sum(sv) / len(sv) if sv else 5.0
        w_avg = sum(wv) / len(wv) if wv else 5.0
        return round(s_avg - w_avg, 1)

    def _pair_trigger(
        self,
        base_s: dict[str, float],
        quote_s: dict[str, float],
        atype: str,
        direction: str,
    ) -> str:
        """Determine whether the pair trigger is 'individual' or 'cumulative'."""
        if atype == "XHTF":
            return "individual"

        # For MTF/HTF: 'individual' if both sides trigger via individual TFs
        if atype == "MTF":
            if direction == "BUY":
                # base strong individual + quote weak individual
                b_ind = all(
                    base_s.get(tf, 5.0) >= self.MTF_STRONG_INDIVIDUAL
                    for tf in MTF_TFS
                )
                q_ind = all(
                    quote_s.get(tf, 5.0) <= self.MTF_WEAK_INDIVIDUAL
                    for tf in MTF_TFS
                )
            else:
                b_ind = all(
                    base_s.get(tf, 5.0) <= self.MTF_WEAK_INDIVIDUAL
                    for tf in MTF_TFS
                )
                q_ind = all(
                    quote_s.get(tf, 5.0) >= self.MTF_STRONG_INDIVIDUAL
                    for tf in MTF_TFS
                )
            return "individual" if (b_ind and q_ind) else "cumulative"
        else:  # HTF
            if direction == "BUY":
                b_ind = all(
                    base_s.get(tf, 5.0) >= self.HTF_STRONG_INDIVIDUAL
                    for tf in HTF_TFS
                )
                q_ind = all(
                    quote_s.get(tf, 5.0) <= self.HTF_WEAK_INDIVIDUAL
                    for tf in HTF_TFS
                )
            else:
                b_ind = all(
                    base_s.get(tf, 5.0) <= self.HTF_WEAK_INDIVIDUAL
                    for tf in HTF_TFS
                )
                q_ind = all(
                    quote_s.get(tf, 5.0) >= self.HTF_STRONG_INDIVIDUAL
                    for tf in HTF_TFS
                )
            return "individual" if (b_ind and q_ind) else "cumulative"

    # ── Fire helpers ──────────────────────────────────────────────

    def _try_fire(
        self,
        ccy: str,
        direction: str,
        alert_type: str,
        tf_scores: dict[str, float],
        reason: str,
        cumulative: float,
    ) -> QM4Alert | None:
        """Fire a currency alert if confirmed and cooldown has elapsed.

        Requires the extreme condition to be present for 2 consecutive
        reads before firing, to filter out single-cycle OCR misreads.
        """
        now = time.time()
        key = f"{ccy}_{alert_type}_{direction}"

        # Confirmation: must see this condition 2 consecutive times
        self._confirm_count[key] = self._confirm_count.get(key, 0) + 1
        self._fired_keys_this_cycle.add(key)
        if self._confirm_count[key] < self._CONFIRM_REQUIRED:
            return None

        # HTF-related types get 15-minute cooldown, others use default
        cooldown = 900 if alert_type in ("HTF", "HTFC", "XHTF") else self.cooldown_seconds
        if now - self._last_alert_time.get(key, 0.0) < cooldown:
            return None

        self._last_alert_time[key] = now
        alert = QM4Alert(
            currency=ccy,
            direction=direction,
            alert_type=alert_type,
            tf_scores=dict(tf_scores),
            reason=reason,
            cumulative=round(cumulative, 1),
        )
        logger.info("QM4 alert fired: %s", alert.label)
        return alert

    def _try_fire_pair(
        self,
        pair: str,
        direction: str,
        alert_type: str,
        trigger_type: str,
        base_scores: dict[str, float],
        quote_scores: dict[str, float],
        base_alignment: int,
        quote_alignment: int,
        spread: float,
    ) -> QM4PairAlert | None:
        """Fire a pair alert if the cooldown has elapsed."""
        now = time.time()
        key = f"{pair}_{alert_type}_{direction}"
        if now - self._last_alert_time.get(key, 0.0) < self.cooldown_seconds:
            return None

        self._last_alert_time[key] = now
        alert = QM4PairAlert(
            pair=pair,
            direction=direction,
            alert_type=alert_type,
            trigger_type=trigger_type,
            base_scores=dict(base_scores),
            quote_scores=dict(quote_scores),
            base_alignment=base_alignment,
            quote_alignment=quote_alignment,
            spread=spread,
        )
        logger.info("QM4 pair alert fired: %s", alert.label)
        return alert

    # ── Low-level helpers ─────────────────────────────────────────

    def _gather(
        self,
        scores: dict[tuple[str, str], float],
        ccy: str,
        tfs: list[str],
    ) -> dict[str, float] | None:
        """Collect scores for a currency across the given TF list.

        Returns None if any TF is missing (data not yet available).
        """
        result: dict[str, float] = {}
        for tf in tfs:
            val = scores.get((ccy, tf))
            if val is None:
                return None
            result[tf] = val
        return result
