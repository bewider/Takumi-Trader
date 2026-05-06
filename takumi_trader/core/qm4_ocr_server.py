"""Persistent OCR server — stays running and processes requests via stdin/stdout.

The RapidOCR model is loaded ONCE at startup, then each request just captures
the screen and runs inference. This avoids the ~1s startup overhead per call.

Protocol: read one line from stdin (JSON args), write one line to stdout (JSON result).
"""

import json
import sys


def main() -> None:
    import mss
    import numpy as np
    from PIL import Image, ImageEnhance
    from rapidocr_onnxruntime import RapidOCR

    CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "NZD", "CHF"]
    TF_LABELS = {"M15", "H1", "H4", "D1", "W1", "M1"}
    TF_MAP = {"M15": "M15", "H1": "H1", "H4": "H4",
              "D1": "D1", "W1": "W1", "M1": "MN"}
    QM4_ROW_TFS = ["M15", "H1", "H4", "D1", "W1", "M1"]

    # Load model ONCE
    ocr = RapidOCR()
    sct = mss.mss()

    # Signal ready
    print(json.dumps({"status": "ready"}), flush=True)

    # Process requests in a loop
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            args = json.loads(line)
            left = args["left"]
            top = args["top"]
            width = args["width"]
            height = args["height"]
            header_h = args["header_h"]

            # Capture
            shot = sct.grab({"left": left, "top": top, "width": width, "height": height})
            img = Image.frombytes("RGB", shot.size, shot.rgb)

            # Dual pass: original + gamma
            def run_ocr_pass(ocr_engine, img_arr, header_pos, num_dets, seen):
                result, _ = ocr_engine(img_arr)
                if not result:
                    return
                for box, text, _conf in result:
                    text_clean = text.strip()
                    text_upper = text_clean.upper()
                    x_center = (box[0][0] + box[2][0]) / 2
                    y_center = (box[0][1] + box[2][1]) / 2
                    if text_upper in CURRENCIES:
                        if text_upper not in header_pos:
                            header_pos[text_upper] = x_center
                        continue
                    if text_upper in {"TIME", "FRAME", "DATA", "TIME FRAME"} or text_upper in TF_LABELS:
                        continue
                    cleaned = "".join(c for c in text_clean if c.isdigit() or c == ".")
                    if cleaned:
                        try:
                            v = float(cleaned)
                            if 0.0 <= v <= 10.0:
                                pos_key = (round(x_center / 20), round(y_center / 20))
                                if pos_key not in seen:
                                    seen.add(pos_key)
                                    num_dets.append((x_center, y_center, round(v, 1)))
                        except ValueError:
                            pass

            header_positions = {}
            numeric_detections = []
            seen_positions = set()

            # Pass 1: original
            run_ocr_pass(ocr, np.array(img), header_positions, numeric_detections, seen_positions)

            # Pass 2 (if needed): gamma
            if len(numeric_detections) < 48:
                gray = np.array(img.convert("L"))
                gamma_lut = np.array(
                    [min(255, int((i / 255.0) ** 0.3 * 255)) for i in range(256)],
                    dtype=np.uint8,
                )
                gamma_rgb = np.stack([gamma_lut[gray]] * 3, axis=2)
                run_ocr_pass(ocr, gamma_rgb, header_positions, numeric_detections, seen_positions)

            if not numeric_detections:
                print(json.dumps({"error": "No OCR results"}), flush=True)
                continue

            # Fill missing header positions
            detected = sorted(header_positions.items(), key=lambda kv: kv[1])
            if len(detected) >= 2:
                spacings = [detected[i+1][1] - detected[i][1] for i in range(len(detected)-1)]
                avg_spacing = sum(spacings) / len(spacings)
                for ccy in CURRENCIES:
                    if ccy not in header_positions:
                        idx = CURRENCIES.index(ccy)
                        if idx > 0 and CURRENCIES[idx-1] in header_positions:
                            header_positions[ccy] = header_positions[CURRENCIES[idx-1]] + avg_spacing
                        elif idx < 7 and CURRENCIES[idx+1] in header_positions:
                            header_positions[ccy] = header_positions[CURRENCIES[idx+1]] - avg_spacing

            if len(header_positions) < 6:
                print(json.dumps({"error": f"Header detection failed: {len(header_positions)} currencies"}), flush=True)
                continue

            # Map to grid
            col_centers = {ccy: x for ccy, x in header_positions.items()}
            data_h = height - header_h
            row_h = data_h / 6

            scores = {}
            for x, y, val in numeric_detections:
                best_ccy = min(col_centers, key=lambda c: abs(col_centers[c] - x))
                best_tf = None
                for r, tf_qm4 in enumerate(QM4_ROW_TFS):
                    y_lo = header_h + r * row_h
                    y_hi = header_h + (r + 1) * row_h
                    if y_lo <= y <= y_hi:
                        best_tf = TF_MAP[tf_qm4]
                        break
                if best_tf:
                    key = f"{best_ccy},{best_tf}"
                    scores[key] = val

            # Fill missing
            for tf_qm4 in QM4_ROW_TFS:
                tf_code = TF_MAP[tf_qm4]
                for ccy in CURRENCIES:
                    key = f"{ccy},{tf_code}"
                    if key not in scores:
                        scores[key] = 5.0

            print(json.dumps({"scores": scores}), flush=True)

        except Exception as e:
            print(json.dumps({"error": str(e)}), flush=True)


if __name__ == "__main__":
    main()
