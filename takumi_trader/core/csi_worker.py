"""CSI Worker — background QThread that computes currency strength scores.

Runs its own MT5 connection (same pattern as takumi_meter.py) so it is
completely non-invasive to the main TAKUMI trading pipeline.

Emits `scores_ready` every 15 seconds with a dict keyed by
    (currency, tf_label) -> float (0.0 – 10.0)

covering all six timeframes: M15, H1, H4, D1, W1, MN.
"""

from __future__ import annotations

import logging
import time

import numpy as np
import MetaTrader5 as mt5

from PyQt6.QtCore import QThread, pyqtSignal

logger = logging.getLogger(__name__)

# ── Constants (mirrored from takumi_meter.py) ─────────────────────────────

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "NZD", "CHF"]

ALL_28_PAIRS = [
    "EURUSD", "GBPUSD", "AUDUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY",
    "EURGBP", "EURAUD", "EURNZD", "EURCAD", "EURCHF", "EURJPY",
    "GBPAUD", "GBPNZD", "GBPCAD", "GBPCHF", "GBPJPY",
    "AUDNZD", "AUDCAD", "AUDCHF", "AUDJPY",
    "NZDCAD", "NZDCHF", "NZDJPY",
    "CADCHF", "CADJPY",
    "CHFJPY",
]

_CCY_PAIRS: dict[str, list[tuple[str, bool]]] = {ccy: [] for ccy in CURRENCIES}
for _p in ALL_28_PAIRS:
    _CCY_PAIRS[_p[:3]].append((_p, True))
    _CCY_PAIRS[_p[3:]].append((_p, False))

# Per-TF config: (label, mt5_const, method, period, power, fetch_bars)
# Identical to takumi_meter.py — proven best match to QM4 FSM
TIMEFRAMES = [
    ("M15", mt5.TIMEFRAME_M15, "stoch", 2, 0.7, 30),
    ("H1",  mt5.TIMEFRAME_H1,  "stoch", 5, 0.5, 40),
    ("H4",  mt5.TIMEFRAME_H4,  "stoch", 7, 0.5, 50),
    ("D1",  mt5.TIMEFRAME_D1,  "stoch", 5, 0.5, 30),
    ("W1",  mt5.TIMEFRAME_W1,  "stoch", 5, 0.5, 30),
    ("MN",  mt5.TIMEFRAME_MN1, "stoch", 5, 0.5, 30),
]

# Interval between score computations
_INTERVAL_SECONDS = 15


# ── Indicator helpers ─────────────────────────────────────────────────────


def _stochastic(
    closes: np.ndarray, highs: np.ndarray, lows: np.ndarray, period: int
) -> float:
    """Compute Stochastic %K (0-100)."""
    if len(closes) < period:
        return 50.0
    h = np.max(highs[-period:])
    lo = np.min(lows[-period:])
    if h == lo:
        return 50.0
    return float(((closes[-1] - lo) / (h - lo)) * 100.0)


def _rsi_wilder(closes: np.ndarray, period: int) -> float:
    """Compute Wilder's smoothed RSI (0-100)."""
    if len(closes) < period + 2:
        return 50.0
    deltas = np.diff(closes)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    if len(gains) < period:
        return 50.0
    avg_gain = float(np.mean(gains[:period]))
    avg_loss = float(np.mean(losses[:period]))
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return float(100.0 - 100.0 / (1.0 + avg_gain / avg_loss))


# ── Score computation (ported from takumi_meter.compute_scores) ───────────


def compute_scores() -> dict[tuple[str, str], float]:
    """Compute currency strength scores for all 6 TFs and 8 currencies.

    Returns:
        {(currency, tf_label): score_0_to_10}
        e.g. {("JPY", "M15"): 0.2, ("USD", "D1"): 8.7, ...}
    """
    scores: dict[tuple[str, str], float] = {}

    for tf_label, tf_const, method, period, power, fetch_bars in TIMEFRAMES:
        pair_raw: dict[str, float] = {}

        for pair in ALL_28_PAIRS:
            try:
                rates = mt5.copy_rates_from_pos(pair, tf_const, 0, fetch_bars)
                if rates is None or len(rates) < period + 2:
                    continue
                closes = rates["close"].astype(np.float64)
                highs  = rates["high"].astype(np.float64)
                lows   = rates["low"].astype(np.float64)

                if method == "stoch":
                    pair_raw[pair] = _stochastic(closes, highs, lows, period)
                elif method == "rsi":
                    pair_raw[pair] = _rsi_wilder(closes, period)
                elif method == "blend":
                    pair_raw[pair] = (
                        _rsi_wilder(closes, period)
                        + _stochastic(closes, highs, lows, period)
                    ) * 0.5
            except Exception:
                continue

        for ccy in CURRENCIES:
            vals: list[float] = []
            for pair, is_base in _CCY_PAIRS[ccy]:
                if pair in pair_raw:
                    v = pair_raw[pair]
                    vals.append(v if is_base else 100.0 - v)

            if vals:
                avg = float(np.mean(vals))
                if power != 1.0:
                    centered = (avg - 50.0) / 50.0
                    scaled   = float(np.sign(centered) * abs(centered) ** power)
                    raw      = (scaled + 1.0) * 5.0
                else:
                    raw = avg / 10.0
                scores[(ccy, tf_label)] = round(max(0.0, min(10.0, raw)), 1)
            else:
                scores[(ccy, tf_label)] = 5.0

    return scores


# ── Worker thread ─────────────────────────────────────────────────────────


class CsiWorker(QThread):
    """Background worker that emits CSI scores on a timer.

    Two modes:
    - **Computed** (default): connects to MT5 and computes scores every 15 s.
    - **OCR**: reads the QM4 screen via Tesseract every 1 s.

    If OCR fails repeatedly (5 consecutive failures), the worker falls
    back to computed mode automatically and retries OCR every 30 s.

    Signals:
        scores_ready(dict): {(currency, tf_label): float} for all 6 TFs.
        status_changed(str): Human-readable status line.
    """

    scores_ready:   pyqtSignal = pyqtSignal(dict)
    status_changed: pyqtSignal = pyqtSignal(str)

    _MAX_OCR_FAILURES = 5
    _OCR_INTERVAL = 1.5       # seconds between OCR reads
    _OCR_RETRY_INTERVAL = 30  # seconds between OCR retries after fallback

    def __init__(self, parent=None, ocr_mode: bool = False) -> None:
        super().__init__(parent)
        self._stop_flag = False
        self._ocr_mode = ocr_mode
        self._mt5_initialized = False
        # Pre-read OCR settings on the main thread (QSettings is safer here)
        self._ocr_reader_instance = None
        if ocr_mode:
            self._ocr_reader_instance = self._create_ocr_reader()

    # ── Public control ────────────────────────────────────────────

    def set_ocr_mode(self, enabled: bool) -> None:
        """Toggle OCR mode at runtime (takes effect on next cycle)."""
        self._ocr_mode = enabled

    def stop(self) -> None:
        """Request the worker to stop and wait for it to finish."""
        self._stop_flag = True
        self.wait()

    # ── QThread entry point ───────────────────────────────────────

    def run(self) -> None:
        """Main loop: OCR or computed scores on a timer."""
        ocr_reader = self._ocr_reader_instance
        ocr_fail_count = 0
        using_fallback = False
        last_ocr_retry = 0.0

        if self._ocr_mode and ocr_reader is None:
            logger.warning("CsiWorker: OCR reader not available, using computed")
            self._ocr_mode = False

        # If not OCR mode (or OCR setup failed), init MT5 immediately
        if not self._ocr_mode:
            self._init_mt5()

        while not self._stop_flag:
            t0 = time.time()

            # ── Determine which mode to use this cycle ────────
            use_ocr_this_cycle = (
                self._ocr_mode
                and ocr_reader is not None
                and not using_fallback
            )

            # If in fallback, periodically retry OCR
            if using_fallback and self._ocr_mode and ocr_reader is not None:
                if time.time() - last_ocr_retry > self._OCR_RETRY_INTERVAL:
                    use_ocr_this_cycle = True
                    last_ocr_retry = time.time()

            # ── OCR path ──────────────────────────────────────
            if use_ocr_this_cycle:
                try:
                    scores = ocr_reader.read_scores()
                    logger.info("CsiWorker OCR: got %d scores", len(scores))
                    elapsed_ms = int((time.time() - t0) * 1000)
                    self.scores_ready.emit(scores)
                    self.status_changed.emit(
                        f"CSI/OCR: read ({elapsed_ms} ms)"
                    )
                    ocr_fail_count = 0
                    if using_fallback:
                        using_fallback = False
                        self.status_changed.emit("CSI/OCR: recovered from fallback")
                        logger.info("CsiWorker: OCR recovered, switching back")
                except Exception as e:
                    ocr_fail_count += 1
                    # Only log the first few failures, then go quiet
                    if ocr_fail_count <= self._MAX_OCR_FAILURES:
                        logger.warning(
                            "CsiWorker: OCR error (%d/%d): %s",
                            ocr_fail_count, self._MAX_OCR_FAILURES, e,
                        )
                    self.status_changed.emit(
                        f"CSI/OCR: error ({ocr_fail_count}/{self._MAX_OCR_FAILURES})"
                    )
                    if ocr_fail_count == self._MAX_OCR_FAILURES:
                        using_fallback = True
                        last_ocr_retry = time.time()
                        self.status_changed.emit(
                            "CSI/OCR: QM4 not visible, using computed fallback"
                        )
                        logger.warning(
                            "CsiWorker: OCR failed %d times, falling back to computed "
                            "(will retry every 30s silently)",
                            ocr_fail_count,
                        )
                        self._init_mt5()

            # ── Computed path (default or fallback) ───────────
            elif self._mt5_initialized:
                try:
                    scores = compute_scores()
                    elapsed_ms = int((time.time() - t0) * 1000)
                    self.scores_ready.emit(scores)
                    mode = "CSI/fallback" if using_fallback else "CSI"
                    self.status_changed.emit(
                        f"{mode}: updated ({elapsed_ms} ms)"
                    )
                except Exception:
                    logger.exception("CsiWorker: compute error")
                    self.status_changed.emit("CSI: compute error")

            # ── Sleep ─────────────────────────────────────────
            if self._ocr_mode and not using_fallback:
                interval = self._OCR_INTERVAL
            else:
                interval = _INTERVAL_SECONDS
            deadline = time.time() + interval
            while not self._stop_flag and time.time() < deadline:
                time.sleep(0.1)

            # ── Check if mode was toggled at runtime ──────────
            if self._ocr_mode and ocr_reader is None:
                ocr_reader = self._create_ocr_reader()
                ocr_fail_count = 0
                using_fallback = False
            elif not self._ocr_mode:
                if not self._mt5_initialized:
                    self._init_mt5()

        if self._mt5_initialized:
            mt5.shutdown()
            logger.info("CsiWorker: MT5 shut down")

    # ── Helpers ───────────────────────────────────────────────────

    def _init_mt5(self) -> None:
        """Lazy-initialise MT5 connection (only when needed)."""
        if self._mt5_initialized:
            return
        self.status_changed.emit("CSI: connecting to MT5…")
        if mt5.initialize():
            info = mt5.account_info()
            broker = info.server if info else "unknown"
            self.status_changed.emit(f"CSI: connected ({broker})")
            logger.info("CsiWorker: connected to %s", broker)
            self._mt5_initialized = True
        else:
            self.status_changed.emit("CSI: MT5 connection failed")
            logger.error("CsiWorker: mt5.initialize() failed")

    @staticmethod
    def _create_ocr_reader():
        """Create a QM4OcrReader from QSettings calibration data."""
        from PyQt6.QtCore import QSettings
        s = QSettings("TAKUMITrader", "TAKUMITrader")
        left   = s.value("ocr/region_left",   0, type=int)
        top    = s.value("ocr/region_top",     0, type=int)
        width  = s.value("ocr/region_width",   0, type=int)
        height = s.value("ocr/region_height",  0, type=int)
        header = s.value("ocr/header_height", 25, type=int)
        tess   = s.value("ocr/tesseract_path", "", type=str) or None

        if width < 50 or height < 50:
            logger.warning("CsiWorker: OCR region not calibrated (w=%d, h=%d)", width, height)
            return None

        try:
            from takumi_trader.core.qm4_ocr import QM4OcrReader
            return QM4OcrReader(
                region=(left, top, width, height),
                header_height=header,
                tesseract_cmd=tess,
            )
        except Exception as e:
            logger.error("CsiWorker: failed to create OCR reader: %s", e)
            return None
