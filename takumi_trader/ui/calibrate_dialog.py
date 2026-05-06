"""Calibration dialog for QM4 OCR screen reading.

Lets the user:
1. Draw a rectangle on-screen to define the QM4 data grid region
2. Fine-tune coordinates with spinboxes
3. Set the header row height
4. Set the Tesseract path
5. Test OCR to verify the values are read correctly
"""

from __future__ import annotations

import os

from PyQt6.QtCore import QSettings, Qt, pyqtSignal, QRect
from PyQt6.QtGui import QColor, QFont, QGuiApplication
from PyQt6.QtWidgets import (
    QDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QFileDialog,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

SETTINGS_ORG = "TAKUMITrader"
SETTINGS_APP = "TAKUMITrader"


class _CursorTracker(QDialog):
    """Small floating dialog that shows live cursor position.

    The user positions their mouse over the QM4 grid corner and presses
    Enter or clicks 'Capture' to record the coordinates.  Two clicks
    (top-left, then bottom-right) define the region.
    """

    coordinate_captured = pyqtSignal(int, int)  # screen x, y

    def __init__(self, label: str, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle(f"Pick: {label}")
        self.setWindowFlags(
            Qt.WindowType.Window
            | Qt.WindowType.WindowStaysOnTopHint
        )
        self.setFixedSize(340, 100)

        layout = QVBoxLayout(self)
        self._info = QLabel(
            f"Move your mouse to the <b>{label}</b> of the QM4 data grid,\n"
            "then click <b>Capture</b> (or press Enter)."
        )
        self._info.setWordWrap(True)
        layout.addWidget(self._info)

        self._pos_label = QLabel("Cursor: (—, —)")
        self._pos_label.setFont(QFont("Consolas", 12, QFont.Weight.Bold))
        self._pos_label.setStyleSheet("color: #e07020;")
        layout.addWidget(self._pos_label)

        btn_row = QHBoxLayout()
        btn_capture = QPushButton("Capture")
        btn_capture.setStyleSheet(
            "background: #1b8a2a; color: white; font-weight: bold; padding: 6px 20px;"
        )
        btn_capture.clicked.connect(self._do_capture)
        btn_row.addWidget(btn_capture)
        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(self.reject)
        btn_row.addWidget(btn_cancel)
        layout.addLayout(btn_row)

        # Timer to poll cursor position
        from PyQt6.QtCore import QTimer, QCursor
        self._cursor_cls = QCursor
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._update_pos)
        self._timer.start(50)

    def _update_pos(self) -> None:
        pos = self._cursor_cls.pos()
        self._pos_label.setText(f"Cursor: ({pos.x()}, {pos.y()})")

    def _do_capture(self) -> None:
        pos = self._cursor_cls.pos()
        self.coordinate_captured.emit(pos.x(), pos.y())
        self.accept()

    def keyPressEvent(self, event) -> None:
        if event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
            self._do_capture()
        elif event.key() == Qt.Key.Key_Escape:
            self.reject()
        else:
            super().keyPressEvent(event)


class CalibrateDialog(QDialog):
    """Dialog for configuring the QM4 OCR screen region."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("QM4 OCR Calibration")
        self.setMinimumWidth(500)
        self._setup_ui()
        self._load_settings()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setSpacing(10)

        # Instructions
        instr = QLabel(
            "Define the screen region of the QM4 data grid.\n"
            "The region must include the header row (Time Frame | USD | EUR…)\n"
            "and all 6 data rows (M15 through M1). Exclude the title bar."
        )
        instr.setWordWrap(True)
        instr.setStyleSheet("color: #444; font-size: 10pt;")
        layout.addWidget(instr)

        # Select Region button
        btn_select = QPushButton("Select Region on Screen…")
        btn_select.setStyleSheet(
            "QPushButton { background: #e07020; color: white; padding: 8px 16px;"
            " border-radius: 4px; font-weight: bold; font-size: 12px; }"
        )
        btn_select.clicked.connect(self._start_selection)
        layout.addWidget(btn_select)

        # Coordinate spinboxes
        coord_group = QGroupBox("Region Coordinates (screen pixels)")
        coord_layout = QHBoxLayout(coord_group)
        self._spin_left = self._make_spin("Left:", -8000, 8000, coord_layout)
        self._spin_top = self._make_spin("Top:", -4000, 4000, coord_layout)
        self._spin_width = self._make_spin("Width:", 50, 4000, coord_layout)
        self._spin_height = self._make_spin("Height:", 50, 2000, coord_layout)
        layout.addWidget(coord_group)

        # Header height
        header_row = QHBoxLayout()
        header_row.addWidget(QLabel("Header row height:"))
        self._spin_header = QSpinBox()
        self._spin_header.setRange(10, 80)
        self._spin_header.setValue(25)
        self._spin_header.setSuffix(" px")
        header_row.addWidget(self._spin_header)
        header_row.addStretch()
        layout.addLayout(header_row)

        # Tesseract path
        tess_row = QHBoxLayout()
        tess_row.addWidget(QLabel("Tesseract path:"))
        self._txt_tess = QLineEdit()
        self._txt_tess.setPlaceholderText("Leave empty to use system PATH")
        tess_row.addWidget(self._txt_tess, stretch=1)
        btn_tess_browse = QPushButton("Browse…")
        btn_tess_browse.clicked.connect(self._browse_tesseract)
        tess_row.addWidget(btn_tess_browse)
        layout.addLayout(tess_row)

        # Test buttons
        test_row = QHBoxLayout()
        btn_preview = QPushButton("Capture Preview")
        btn_preview.clicked.connect(self._capture_preview)
        test_row.addWidget(btn_preview)
        btn_test = QPushButton("Test OCR")
        btn_test.setStyleSheet(
            "QPushButton { background: #1b8a2a; color: white; }"
        )
        btn_test.clicked.connect(self._test_ocr)
        test_row.addWidget(btn_test)
        test_row.addStretch()
        layout.addLayout(test_row)

        # Preview / results area
        self._preview_label = QLabel("No preview yet. Click 'Capture Preview' or 'Test OCR'.")
        self._preview_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._preview_label.setMinimumHeight(80)
        self._preview_label.setStyleSheet(
            "background: #f0f0f0; border: 1px solid #ccc; padding: 8px;"
        )
        layout.addWidget(self._preview_label)

        # Results table (shown after Test OCR)
        self._result_table = QTableWidget()
        self._result_table.setVisible(False)
        self._result_table.setMaximumHeight(200)
        layout.addWidget(self._result_table)

        # OK / Cancel
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_ok = QPushButton("OK")
        btn_ok.clicked.connect(self._accept)
        btn_row.addWidget(btn_ok)
        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(self.reject)
        btn_row.addWidget(btn_cancel)
        layout.addLayout(btn_row)

        self.setStyleSheet("""
            QDialog { background: #f5f5f5; }
            QLabel { color: #333; }
            QGroupBox { color: #333; font-weight: bold; }
            QPushButton { background: #4a6fa5; color: white; border: none;
                          padding: 6px 14px; border-radius: 3px; }
            QPushButton:hover { background: #5a83bf; }
            QSpinBox, QLineEdit { background: white; color: #333;
                                   border: 1px solid #ccc; padding: 4px;
                                   border-radius: 3px; }
        """)

    def _make_spin(
        self, label: str, lo: int, hi: int, layout: QHBoxLayout
    ) -> QSpinBox:
        layout.addWidget(QLabel(label))
        spin = QSpinBox()
        spin.setRange(lo, hi)
        spin.setValue(0)
        layout.addWidget(spin)
        return spin

    def _load_settings(self) -> None:
        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        self._spin_left.setValue(s.value("ocr/region_left", 0, type=int))
        self._spin_top.setValue(s.value("ocr/region_top", 0, type=int))
        self._spin_width.setValue(s.value("ocr/region_width", 550, type=int))
        self._spin_height.setValue(s.value("ocr/region_height", 280, type=int))
        self._spin_header.setValue(s.value("ocr/header_height", 25, type=int))
        self._txt_tess.setText(s.value("ocr/tesseract_path", "", type=str))

    def _save_settings(self) -> None:
        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        s.setValue("ocr/region_left", self._spin_left.value())
        s.setValue("ocr/region_top", self._spin_top.value())
        s.setValue("ocr/region_width", self._spin_width.value())
        s.setValue("ocr/region_height", self._spin_height.value())
        s.setValue("ocr/header_height", self._spin_header.value())
        s.setValue("ocr/tesseract_path", self._txt_tess.text())

    def _start_selection(self) -> None:
        """Two-click region selection using simple message boxes."""
        try:
            from PyQt6.QtGui import QCursor

            # Step 1: top-left
            reply = QMessageBox.information(
                self, "Step 1 of 2",
                "Position your mouse cursor on the TOP-LEFT corner\n"
                "of the QM4 data grid (the 'Time Frame' header cell),\n"
                "then click OK.\n\n"
                "Tip: move this dialog out of the way first.",
                QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel,
            )
            if reply != QMessageBox.StandardButton.Ok:
                return

            pos1 = QCursor.pos()
            tl_x, tl_y = pos1.x(), pos1.y()

            # Step 2: bottom-right
            reply = QMessageBox.information(
                self, "Step 2 of 2",
                f"Top-left captured: ({tl_x}, {tl_y})\n\n"
                "Now position your mouse cursor on the BOTTOM-RIGHT corner\n"
                "of the QM4 data grid (after the last M1/CHF cell),\n"
                "then click OK.",
                QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel,
            )
            if reply != QMessageBox.StandardButton.Ok:
                return

            pos2 = QCursor.pos()
            br_x, br_y = pos2.x(), pos2.y()

            # Compute region
            left = min(tl_x, br_x)
            top = min(tl_y, br_y)
            width = abs(br_x - tl_x)
            height = abs(br_y - tl_y)

            if width < 20 or height < 20:
                self._preview_label.setText("Region too small — try again.")
                return

            self._spin_left.setValue(left)
            self._spin_top.setValue(top)
            self._spin_width.setValue(width)
            self._spin_height.setValue(height)
            self._preview_label.setText(
                f"Region set: {width} x {height} at ({left}, {top})"
            )
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Selection failed: {e}")

    def _browse_tesseract(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self, "Select tesseract.exe", "",
            "Executable (*.exe);;All Files (*)",
        )
        if path:
            self._txt_tess.setText(path)

    def _get_region(self) -> tuple[int, int, int, int]:
        return (
            self._spin_left.value(),
            self._spin_top.value(),
            self._spin_width.value(),
            self._spin_height.value(),
        )

    def _capture_preview(self) -> None:
        """Capture the current region and show it as a thumbnail in the dialog."""
        try:
            import mss
            from PyQt6.QtGui import QImage, QPixmap
            sct = mss.mss()
            l, t, w, h = self._get_region()
            shot = sct.grab({"left": l, "top": t, "width": w, "height": h})
            # Convert to QPixmap and display inline
            qimg = QImage(
                shot.rgb, shot.width, shot.height,
                shot.width * 3,
                QImage.Format.Format_RGB888,
            )
            pixmap = QPixmap.fromImage(qimg)
            # Scale to fit the label (max 600px wide)
            if pixmap.width() > 600:
                pixmap = pixmap.scaledToWidth(600, Qt.TransformationMode.SmoothTransformation)
            self._preview_label.setPixmap(pixmap)
            self._preview_label.setMinimumHeight(pixmap.height() + 10)
            self._preview_label.setText("")  # clear text, show image
        except Exception as e:
            self._preview_label.setText(f"Capture failed: {e}")

    def _test_ocr(self) -> None:
        """Run one OCR cycle and show results in a fullscreen popup."""
        from takumi_trader.core.qm4_ocr import QM4OcrReader, QM4OcrError, CURRENCIES
        from PyQt6.QtGui import QImage, QPixmap
        from PyQt6.QtWidgets import QHeaderView

        region = self._get_region()
        tess = self._txt_tess.text() or None
        header_h = self._spin_header.value()

        reader = QM4OcrReader(
            region=region,
            header_height=header_h,
            tesseract_cmd=tess,
        )

        # Capture the screenshot for display
        try:
            import mss
            with mss.mss() as sct:
                l, t, w, h = region
                shot = sct.grab({"left": l, "top": t, "width": w, "height": h})
                qimg = QImage(shot.rgb, shot.width, shot.height,
                              shot.width * 3, QImage.Format.Format_RGB888)
                capture_pixmap = QPixmap.fromImage(qimg)
        except Exception:
            capture_pixmap = None

        try:
            scores = reader.read_scores()
        except QM4OcrError as e:
            self._preview_label.setText(f"OCR Error: {e}")
            return
        except Exception as e:
            self._preview_label.setText(f"Unexpected error: {e}")
            return

        # Build fullscreen results dialog
        result_dlg = QDialog(self)
        result_dlg.setWindowTitle("QM4 OCR Test Results")
        result_dlg.setWindowFlags(
            Qt.WindowType.Window | Qt.WindowType.WindowMaximizeButtonHint
                | Qt.WindowType.WindowCloseButtonHint
        )
        result_dlg.showMaximized()

        dlg_layout = QVBoxLayout(result_dlg)

        # Show captured screenshot at top
        if capture_pixmap:
            img_label = QLabel()
            scaled = capture_pixmap.scaledToHeight(
                min(300, capture_pixmap.height() * 2),
                Qt.TransformationMode.SmoothTransformation,
            )
            img_label.setPixmap(scaled)
            img_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            img_label.setStyleSheet("background: #222; padding: 8px;")
            dlg_layout.addWidget(img_label)

        # Status label
        tfs = ["M15", "H1", "H4", "D1", "W1", "MN"]
        failed_count = sum(
            1 for tf in tfs for ccy in CURRENCIES
            if scores.get((ccy, tf)) == 5.0
        )
        status = QLabel(
            f"OCR complete — 48 cells read, ~{failed_count} neutral fallbacks. "
            "Compare the table below with the screenshot above."
        )
        status.setFont(QFont("Segoe UI", 11))
        status.setStyleSheet("padding: 6px; color: #333;")
        status.setAlignment(Qt.AlignmentFlag.AlignCenter)
        dlg_layout.addWidget(status)

        # Results table
        tbl = QTableWidget(len(tfs), len(CURRENCIES))
        tbl.setHorizontalHeaderLabels(CURRENCIES)
        tbl.setVerticalHeaderLabels(tfs)
        tbl.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        tbl.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        tbl.setStyleSheet("""
            QTableWidget { font-size: 18px; font-weight: bold;
                           gridline-color: #ccc; border: 1px solid #aaa; }
            QHeaderView::section { font-size: 14px; font-weight: bold;
                                    background: #e0e4e8; padding: 6px; }
        """)

        for r, tf in enumerate(tfs):
            for c, ccy in enumerate(CURRENCIES):
                val = scores.get((ccy, tf))
                if val is not None:
                    item = QTableWidgetItem(f"{val:.1f}")
                    if val >= 8.5:
                        item.setBackground(QColor("#a5d6a7"))
                        item.setForeground(QColor("#1b5e20"))
                    elif val <= 1.5:
                        item.setBackground(QColor("#ef9a9a"))
                        item.setForeground(QColor("#b71c1c"))
                else:
                    item = QTableWidgetItem("?")
                    item.setBackground(QColor("#ffcccc"))
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                tbl.setItem(r, c, item)

        dlg_layout.addWidget(tbl, stretch=1)

        # Close button
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(result_dlg.close)
        dlg_layout.addWidget(btn_close, alignment=Qt.AlignmentFlag.AlignRight)

        self._preview_label.setText("OCR test complete — see results window.")
        result_dlg.exec()

    def _accept(self) -> None:
        self._save_settings()
        self.accept()
