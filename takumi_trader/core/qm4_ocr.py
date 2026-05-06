"""QM4 OCR Reader — captures the QM4 Currency Strength Meter screen and
reads the 48 numeric values (8 currencies × 6 timeframes) using RapidOCR.

RapidOCR (deep-learning based) runs on the full captured image in a single
pass, detecting all numbers with their bounding boxes.  Results are mapped
to the grid using detected header positions and y-coordinate bands.

Falls back to per-cell Tesseract if RapidOCR misses individual cells.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
from PIL import Image

logger = logging.getLogger(__name__)

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "NZD", "CHF"]

_QM4_ROW_TFS = ["M15", "H1", "H4", "D1", "W1", "M1"]
_TF_LABELS = frozenset(("M15", "H1", "H4", "D1", "W1", "M1"))
_SKIP_LABELS = frozenset({"TIME", "FRAME", "DATA", "TIME FRAME"})

_TF_MAP: dict[str, str] = {
    "M15": "M15", "H1": "H1", "H4": "H4",
    "D1": "D1", "W1": "W1", "M1": "MN",
}

_NUM_ROWS = 6
_NUM_COLS = 8


class QM4OcrError(Exception):
    """Raised when the OCR pipeline fails critically."""


def _parse_value(text: str) -> float | None:
    """Parse a detected text string to a 0.0-10.0 float."""
    text = text.strip().replace(" ", "")
    if not text:
        return None
    try:
        v = float(text)
        return round(v, 1) if 0.0 <= v <= 10.0 else None
    except ValueError:
        # Try removing stray characters
        digits = "".join(c for c in text if c.isdigit() or c == ".")
        if digits:
            try:
                v = float(digits)
                return round(v, 1) if 0.0 <= v <= 10.0 else None
            except ValueError:
                pass
    return None


class QM4OcrReader:
    """Captures and OCR-reads the QM4 currency strength grid.

    Uses RapidOCR (deep-learning) for the primary read, which detects
    all numbers in a single pass with their positions.  Much more
    accurate than per-cell Tesseract for small text.

    Parameters
    ----------
    region : tuple[int, int, int, int]
        (left, top, width, height) of the capture region in screen pixels.
    header_height : int
        Pixel height of the header row (used for row boundary calculation).
    tesseract_cmd : str | None
        Path to tesseract.exe for fallback per-cell OCR.
    """

    def __init__(
        self,
        region: tuple[int, int, int, int],
        header_height: int = 25,
        tesseract_cmd: str | None = None,
    ) -> None:
        self._left, self._top, self._width, self._height = region
        self._header_h = header_height
        self._tesseract_cmd = tesseract_cmd

        self._sct = None
        self._rapid_ocr = None
        self._pytesseract = None
        self._col_lefts: list[int] | None = None  # cached for Tesseract mode

    def read_scores(self) -> dict[tuple[str, str], float]:
        """Full pipeline: capture → OCR → map to grid.

        Uses RapidOCR if available, otherwise falls back to Tesseract.
        Returns ``{("USD", "M15"): 5.7, ...}`` for all 48 cells.
        """
        self._ensure_deps()

        # Subprocess mode does its own capture — skip local capture
        if self._rapid_ocr == "subprocess":
            return self._read_subprocess()

        img = self._capture()

        if self._rapid_ocr != "tesseract_fallback":
            return self._read_rapid(img)
        else:
            return self._read_tesseract(img)

    def _read_rapid(self, img: Image.Image) -> dict[tuple[str, str], float]:
        """OCR using RapidOCR — single-pass full-image detection."""
        arr = np.array(img)
        result, _elapse = self._rapid_ocr(arr)

        if not result:
            raise QM4OcrError("RapidOCR returned no results")

        # Separate header labels, TF labels, and numeric values
        header_positions: dict[str, float] = {}  # currency → x center
        numeric_detections: list[tuple[float, float, float]] = []  # (x_center, y_center, value)

        for box, text, _conf in result:
            text = text.strip()
            # box is [[x0,y0],[x1,y1],[x2,y2],[x3,y3]] (4 corners)
            x_center = (box[0][0] + box[2][0]) / 2
            y_center = (box[0][1] + box[2][1]) / 2

            text_upper = text.upper()
            if text_upper in CURRENCIES:
                header_positions[text_upper] = x_center
                continue
            if text_upper in {"TIME", "FRAME", "DATA", "TIME FRAME"}:
                continue
            if text_upper in _TF_LABELS:
                continue

            val = _parse_value(text)
            if val is not None:
                numeric_detections.append((x_center, y_center, val))

        # Build column mapping from header positions
        if len(header_positions) < 6:
            raise QM4OcrError(
                f"Header detection failed: found {len(header_positions)} currencies"
            )

        # Sort currencies by x position to get column order
        sorted_ccys = sorted(header_positions.items(), key=lambda kv: kv[1])
        col_centers = {ccy: x for ccy, x in sorted_ccys}

        # Determine row boundaries from image dimensions
        data_h = img.height - self._header_h
        row_h = data_h / _NUM_ROWS
        row_bands: list[tuple[float, float, str]] = []
        for r, tf_qm4 in enumerate(_QM4_ROW_TFS):
            y_lo = self._header_h + r * row_h
            y_hi = self._header_h + (r + 1) * row_h
            row_bands.append((y_lo, y_hi, _TF_MAP[tf_qm4]))

        # Map each detection to the nearest (currency, timeframe)
        scores: dict[tuple[str, str], float] = {}

        for x, y, val in numeric_detections:
            # Find closest column
            best_ccy = min(col_centers, key=lambda c: abs(col_centers[c] - x))
            # Find row
            best_tf = None
            for y_lo, y_hi, tf_code in row_bands:
                if y_lo <= y <= y_hi:
                    best_tf = tf_code
                    break
            if best_tf is None:
                continue

            key = (best_ccy, best_tf)
            # If multiple detections map to the same cell, keep the one
            # closer to the column center
            if key not in scores or abs(col_centers[best_ccy] - x) < 20:
                scores[key] = val

        # Fill missing cells with 5.0 neutral
        failed = 0
        for tf_qm4 in _QM4_ROW_TFS:
            tf_code = _TF_MAP[tf_qm4]
            for ccy in CURRENCIES:
                if (ccy, tf_code) not in scores:
                    scores[(ccy, tf_code)] = 5.0
                    failed += 1

        if failed > 12:
            raise QM4OcrError(f"OCR unreliable: {failed}/48 cells not detected")
        if failed > 0:
            logger.warning("QM4 OCR: %d/48 cells fell back to neutral", failed)

        return scores

    # ── Subprocess mode (RapidOCR in clean process) ─────────────────

    _ocr_server_proc = None  # persistent subprocess

    def _ensure_server(self) -> None:
        """Start the persistent OCR server if not running."""
        import subprocess, sys, json

        if self._ocr_server_proc is not None and self._ocr_server_proc.poll() is None:
            return  # already running

        cmd = [
            sys.executable, "-m", "takumi_trader.core.qm4_ocr_server",
        ]
        self.__class__._ocr_server_proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(Path(__file__).resolve().parent.parent.parent),
        )
        # Wait for "ready" signal
        ready_line = self._ocr_server_proc.stdout.readline().strip()
        try:
            ready = json.loads(ready_line)
            if ready.get("status") == "ready":
                logger.info("QM4 OCR server started (persistent)")
            else:
                raise QM4OcrError(f"OCR server startup failed: {ready_line}")
        except Exception as e:
            raise QM4OcrError(f"OCR server startup failed: {e}")

    def _read_subprocess(self) -> dict[tuple[str, str], float]:
        """Send request to persistent OCR server (model stays loaded in memory)."""
        import json

        self._ensure_server()

        request = json.dumps({
            "left": self._left, "top": self._top,
            "width": self._width, "height": self._height,
            "header_h": self._header_h,
        })

        try:
            self._ocr_server_proc.stdin.write(request + "\n")
            self._ocr_server_proc.stdin.flush()
            response_line = self._ocr_server_proc.stdout.readline().strip()
        except (BrokenPipeError, OSError):
            # Server died — restart on next call
            self.__class__._ocr_server_proc = None
            raise QM4OcrError("OCR server crashed, will restart")

        if not response_line:
            self.__class__._ocr_server_proc = None
            raise QM4OcrError("OCR server returned empty response")

        try:
            data = json.loads(response_line)
        except json.JSONDecodeError:
            raise QM4OcrError(f"OCR server invalid output: {response_line[:200]}")

        if "error" in data:
            raise QM4OcrError(f"OCR: {data['error']}")

        raw_scores = data.get("scores", {})
        scores: dict[tuple[str, str], float] = {}
        failed = 0
        for key, val in raw_scores.items():
            ccy, tf = key.split(",")
            scores[(ccy, tf)] = float(val)
            if float(val) == 5.0:
                failed += 1

        if failed > 12:
            raise QM4OcrError(f"OCR unreliable: {failed}/48 cells not detected")
        if failed > 0:
            logger.warning("QM4 OCR (server): %d/48 cells fell back to neutral", failed)

        return scores

    # ── Tesseract fallback ─────────────────────────────────────────

    def _init_tesseract(self) -> None:
        """Initialize pytesseract with auto-detected path."""
        try:
            import pytesseract
            cmd = self._tesseract_cmd
            if not cmd:
                import shutil
                cmd = shutil.which("tesseract")
                if not cmd:
                    for p in [
                        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
                        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
                    ]:
                        if Path(p).exists():
                            cmd = p
                            break
            if cmd:
                pytesseract.pytesseract.tesseract_cmd = cmd
            self._pytesseract = pytesseract
        except ImportError:
            raise QM4OcrError("Neither RapidOCR nor Tesseract available")

    def _read_tesseract(self, img: Image.Image) -> dict[tuple[str, str], float]:
        """OCR using per-cell Tesseract with majority voting."""
        from collections import Counter

        # Auto-detect columns from header on first call
        if self._col_lefts is None:
            header = img.crop((0, 0, img.width, self._header_h))
            data = self._pytesseract.image_to_data(
                header, output_type=self._pytesseract.Output.DICT,
                config="--psm 6",
            )
            self._col_lefts = [
                data["left"][i] for i, w in enumerate(data["text"])
                if w.strip() and w.strip().upper() not in
                {"TIME", "FRAME", "DATA", "TIME FRAME"}
            ]
            if len(self._col_lefts) != _NUM_COLS:
                # Fallback: equal spacing
                lw = img.width // 5
                dw = (img.width - lw) / _NUM_COLS
                self._col_lefts = [int(lw + c * dw) for c in range(_NUM_COLS)]

        cl = self._col_lefts
        row_h = (img.height - self._header_h) / _NUM_ROWS

        # Cell bounds as midpoints between headers
        cell_bounds: list[tuple[int, int]] = []
        for c in range(_NUM_COLS):
            left = max(0, cl[0] - (cl[1] - cl[0]) // 2) if c == 0 else (cl[c-1] + cl[c]) // 2
            right = img.width if c == _NUM_COLS - 1 else (cl[c] + cl[c+1]) // 2
            cell_bounds.append((left, right))

        scores: dict[tuple[str, str], float] = {}
        failed = 0

        for r in range(_NUM_ROWS):
            tf_code = _TF_MAP[_QM4_ROW_TFS[r]]
            y0 = int(self._header_h + r * row_h) + 2
            y1 = int(self._header_h + (r + 1) * row_h) - 2

            for c in range(_NUM_COLS):
                x0, x1 = cell_bounds[c]
                cell = img.crop((x0, y0, x1, y1))
                val = self._ocr_cell_tesseract(cell)
                ccy = CURRENCIES[c]
                if val is not None:
                    scores[(ccy, tf_code)] = val
                else:
                    scores[(ccy, tf_code)] = 5.0
                    failed += 1

        if failed > 12:
            raise QM4OcrError(f"OCR unreliable: {failed}/48 cells failed")
        if failed > 0:
            logger.warning("QM4 OCR (Tesseract): %d/48 cells fell back to neutral", failed)
        return scores

    def _ocr_cell_tesseract(self, cell: Image.Image) -> float | None:
        """OCR a single cell with Tesseract using majority voting."""
        from collections import Counter

        arr = np.array(cell)
        gray = np.array(cell.convert("L"))
        avg_r = float(np.mean(arr[:, :, 0]))
        avg_g = float(np.mean(arr[:, :, 1]))
        is_red = avg_r > 160 and avg_g < 110
        is_colored = is_red or (avg_g > 120 and avg_r < 170)

        def _otsu(g: np.ndarray) -> int:
            h = np.bincount(g.ravel(), minlength=256).astype(float)
            tot = g.size
            s = float(np.sum(np.arange(256) * h))
            wb = sb = 0.0
            bt = 0
            bv = 0.0
            for t in range(256):
                wb += h[t]
                if wb == 0:
                    continue
                wf = tot - wb
                if wf == 0:
                    break
                sb += t * h[t]
                v = wb * wf * ((sb / wb) - ((s - sb) / wf)) ** 2
                if v > bv:
                    bv = v
                    bt = t
            return bt

        strategies: list[tuple[np.ndarray, int]] = [(gray, _otsu(gray))]
        if is_colored:
            strategies.append((arr[:, :, 1], _otsu(arr[:, :, 1])))
            strategies.append((arr[:, :, 0], _otsu(arr[:, :, 0])))
            strategies.append((arr[:, :, 2], _otsu(arr[:, :, 2])))
            strategies.append((gray, 60))
            strategies.append((gray, 80))
            strategies.append((gray, 110))
            strategies.append((gray, 140))
        else:
            strategies.append((gray, 120))
            strategies.append((gray, 160))

        votes: Counter[float] = Counter()
        for src, thresh in strategies:
            src_pil = Image.fromarray(src, "L")
            big = src_pil.resize(
                (src_pil.width * 4, src_pil.height * 4),
                Image.Resampling.LANCZOS,
            )
            big_arr = np.array(big)
            binary = np.where(big_arr < thresh, 0, 255).astype(np.uint8)
            proc = Image.fromarray(binary, "L")
            padded = Image.new("L", (proc.width + 40, proc.height + 40), 255)
            padded.paste(proc, (20, 20))
            for psm in ("8", "7"):
                text = self._pytesseract.image_to_string(
                    padded,
                    config=f"--psm {psm} -c tessedit_char_whitelist=0123456789.",
                ).strip()
                val = _parse_value(text)
                if val is not None:
                    votes[val] += 1

        if not votes:
            return None
        return votes.most_common(1)[0][0]

    # ── Internals ─────────────────────────────────────────────────

    def _ensure_deps(self) -> None:
        if self._rapid_ocr is None:
            try:
                from rapidocr_onnxruntime import RapidOCR
                self._rapid_ocr = RapidOCR()
                logger.info("QM4 OCR: using RapidOCR engine (direct)")
            except Exception as e:
                logger.warning(
                    "QM4 OCR: RapidOCR can't load in-process (%s), "
                    "using subprocess mode", e,
                )
                self._rapid_ocr = "subprocess"

    def _capture(self) -> Image.Image:
        if self._sct is None:
            try:
                import mss
                self._sct = mss.mss()
            except ImportError:
                raise QM4OcrError("mss not installed (pip install mss)")
        monitor = {
            "left": self._left, "top": self._top,
            "width": self._width, "height": self._height,
        }
        try:
            shot = self._sct.grab(monitor)
        except Exception as e:
            raise QM4OcrError(f"Screen capture failed: {e}") from e
        return Image.frombytes("RGB", shot.size, shot.rgb)
