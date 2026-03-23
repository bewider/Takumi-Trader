"""Settings dialog for sound file, cooldown, and alert preferences."""

from __future__ import annotations

import os

from PyQt6.QtCore import QSettings
from PyQt6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDoubleSpinBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
)

from takumi_trader.core.alerts import play_sound
from takumi_trader.ui.explanation_dialog import ExplanationDialog

SETTINGS_ORG = "TAKUMITrader"
SETTINGS_APP = "TAKUMITrader"


def load_settings() -> dict:
    """Load persisted settings from QSettings.

    Returns:
        Dict with keys: sound_file, sound_enabled, cooldown_seconds, + ctrader_* keys.
    """
    s = QSettings(SETTINGS_ORG, SETTINGS_APP)
    return {
        "sound_file": s.value("alerts/sound_file", "", type=str),
        "sound_enabled": s.value("alerts/sound_enabled", True, type=bool),
        "cooldown_seconds": s.value("alerts/cooldown_seconds", 60, type=int),
        "font_size": s.value("ui/font_size", 10, type=int),
        "compact_mode": s.value("ui/compact_mode", False, type=bool),
        # cTrader auto-trading
        "ctrader_enabled": s.value("ctrader/enabled", False, type=bool),
        "ctrader_client_id": s.value("ctrader/client_id", "", type=str),
        "ctrader_client_secret": s.value("ctrader/client_secret", "", type=str),
        "ctrader_access_token": s.value("ctrader/access_token", "", type=str),
        "ctrader_account_id": s.value("ctrader/account_id", "", type=str),
        "ctrader_lot_size": s.value("ctrader/lot_size", 0.01, type=float),
        "ctrader_auto_open": s.value("ctrader/auto_open", True, type=bool),
        "ctrader_auto_close": s.value("ctrader/auto_close", True, type=bool),
        "ctrader_max_positions": s.value("ctrader/max_positions", 3, type=int),
    }


def save_settings(settings: dict) -> None:
    """Persist settings to QSettings."""
    s = QSettings(SETTINGS_ORG, SETTINGS_APP)
    s.setValue("alerts/sound_file", settings["sound_file"])
    s.setValue("alerts/sound_enabled", settings["sound_enabled"])
    s.setValue("alerts/cooldown_seconds", settings["cooldown_seconds"])
    s.setValue("ui/font_size", settings["font_size"])
    s.setValue("ui/compact_mode", settings.get("compact_mode", False))
    # cTrader auto-trading
    s.setValue("ctrader/enabled", settings.get("ctrader_enabled", False))
    s.setValue("ctrader/client_id", settings.get("ctrader_client_id", ""))
    s.setValue("ctrader/client_secret", settings.get("ctrader_client_secret", ""))
    s.setValue("ctrader/access_token", settings.get("ctrader_access_token", ""))
    s.setValue("ctrader/account_id", settings.get("ctrader_account_id", ""))
    s.setValue("ctrader/lot_size", settings.get("ctrader_lot_size", 0.01))
    s.setValue("ctrader/auto_open", settings.get("ctrader_auto_open", True))
    s.setValue("ctrader/auto_close", settings.get("ctrader_auto_close", True))
    s.setValue("ctrader/max_positions", settings.get("ctrader_max_positions", 3))


class SettingsDialog(QDialog):
    """Dialog for configuring alert sound and cooldown settings."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self.setMinimumWidth(450)
        self._setup_ui()
        self._load_current()

    def _setup_ui(self) -> None:
        """Build the dialog layout."""
        layout = QVBoxLayout(self)
        layout.setSpacing(12)

        # Sound enabled
        self.chk_sound = QCheckBox("Enable sound alerts")
        layout.addWidget(self.chk_sound)

        # Sound file picker
        file_row = QHBoxLayout()
        file_row.addWidget(QLabel("Sound file:"))
        self.txt_file = QLineEdit()
        self.txt_file.setReadOnly(True)
        self.txt_file.setPlaceholderText("Select a .wav or .mp3 file…")
        file_row.addWidget(self.txt_file, stretch=1)

        self.btn_browse = QPushButton("Browse…")
        self.btn_browse.clicked.connect(self._browse_file)
        file_row.addWidget(self.btn_browse)

        self.btn_test = QPushButton("\u25b6 Test")
        self.btn_test.clicked.connect(self._test_sound)
        file_row.addWidget(self.btn_test)

        layout.addLayout(file_row)

        # Font size
        font_row = QHBoxLayout()
        font_row.addWidget(QLabel("Table font size:"))
        self.spin_font_size = QSpinBox()
        self.spin_font_size.setRange(7, 18)
        self.spin_font_size.setValue(10)
        self.spin_font_size.setSuffix(" pt")
        font_row.addWidget(self.spin_font_size)
        font_row.addStretch()
        layout.addLayout(font_row)

        # Cooldown (displayed in minutes, stored as seconds internally)
        cd_row = QHBoxLayout()
        cd_row.addWidget(QLabel("Alert cooldown:"))
        self.spin_cooldown = QSpinBox()
        self.spin_cooldown.setRange(1, 30)
        self.spin_cooldown.setValue(1)
        self.spin_cooldown.setSuffix(" min")
        cd_row.addWidget(self.spin_cooldown)
        cd_row.addStretch()
        layout.addLayout(cd_row)

        # Compact mode
        self.chk_compact = QCheckBox("Compact table mode")
        layout.addWidget(self.chk_compact)

        # ── cTrader Auto-Trading ──────────────────────────────────
        ct_group = QGroupBox("cTrader Auto-Trading (Demo)")
        ct_layout = QVBoxLayout(ct_group)
        ct_layout.setSpacing(6)

        self.chk_ctrader_enabled = QCheckBox("Enable cTrader auto-trading")
        ct_layout.addWidget(self.chk_ctrader_enabled)

        # Credentials
        cred_pairs = [
            ("Client ID:", "ctrader_client_id", False),
            ("Client Secret:", "ctrader_client_secret", True),
            ("Access Token:", "ctrader_access_token", True),
            ("Account ID:", "ctrader_account_id", False),
        ]
        self._ct_inputs: dict[str, QLineEdit] = {}
        for label_text, key, secret in cred_pairs:
            row = QHBoxLayout()
            lbl = QLabel(label_text)
            lbl.setFixedWidth(100)
            row.addWidget(lbl)
            inp = QLineEdit()
            if secret:
                inp.setEchoMode(QLineEdit.EchoMode.Password)
            inp.setPlaceholderText(f"Enter {label_text.replace(':', '').lower()}")
            row.addWidget(inp, stretch=1)
            self._ct_inputs[key] = inp
            ct_layout.addLayout(row)

        # Lot size + max positions
        param_row = QHBoxLayout()
        param_row.addWidget(QLabel("Lot size:"))
        self.spin_ct_lot = QDoubleSpinBox()
        self.spin_ct_lot.setRange(0.01, 1.0)
        self.spin_ct_lot.setSingleStep(0.01)
        self.spin_ct_lot.setDecimals(2)
        self.spin_ct_lot.setValue(0.01)
        param_row.addWidget(self.spin_ct_lot)
        param_row.addSpacing(16)
        param_row.addWidget(QLabel("Max positions:"))
        self.spin_ct_max_pos = QSpinBox()
        self.spin_ct_max_pos.setRange(1, 10)
        self.spin_ct_max_pos.setValue(3)
        param_row.addWidget(self.spin_ct_max_pos)
        param_row.addStretch()
        ct_layout.addLayout(param_row)

        # Auto-open / auto-close
        self.chk_ct_auto_open = QCheckBox("Auto-open orders on FULL conviction alerts")
        self.chk_ct_auto_close = QCheckBox("Auto-close on URGENT exit signal")
        ct_layout.addWidget(self.chk_ct_auto_open)
        ct_layout.addWidget(self.chk_ct_auto_close)

        layout.addWidget(ct_group)

        # Explanation button
        layout.addSpacing(8)
        btn_explanation = QPushButton("\u2753 Explanation — User Manual")
        btn_explanation.clicked.connect(self._open_explanation)
        layout.addWidget(btn_explanation)

        # Pair Algo Settings button
        btn_pair_algo = QPushButton("\U0001f9ec Pair Algorithm Settings")
        btn_pair_algo.setToolTip("View/manage per-pair optimized calculation parameters")
        btn_pair_algo.clicked.connect(self._open_pair_algo)
        layout.addWidget(btn_pair_algo)

        # Buttons
        layout.addSpacing(4)
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_ok = QPushButton("OK")
        btn_ok.clicked.connect(self._accept)
        btn_row.addWidget(btn_ok)
        btn_cancel = QPushButton("Cancel")
        btn_cancel.clicked.connect(self.reject)
        btn_row.addWidget(btn_cancel)
        layout.addLayout(btn_row)

        # Style
        self.setStyleSheet(
            """
            QDialog { background: #f5f5f5; color: #222222; }
            QLabel { color: #222222; }
            QLineEdit { background: #ffffff; color: #222222; border: 1px solid #cccccc;
                        padding: 4px; border-radius: 3px; }
            QCheckBox { color: #222222; }
            QPushButton { background: #4a6fa5; color: #ffffff; border: none;
                          padding: 6px 16px; border-radius: 3px; }
            QPushButton:hover { background: #5a83bf; }
            QSpinBox { background: #ffffff; color: #222222; border: 1px solid #cccccc;
                       padding: 4px; border-radius: 3px; }
            """
        )

    def _load_current(self) -> None:
        """Load current settings into widgets."""
        settings = load_settings()
        self.chk_sound.setChecked(settings["sound_enabled"])
        self.txt_file.setText(settings["sound_file"])
        self.spin_cooldown.setValue(max(1, settings["cooldown_seconds"] // 60))
        self.spin_font_size.setValue(settings["font_size"])
        self.chk_compact.setChecked(settings.get("compact_mode", False))
        # cTrader
        self.chk_ctrader_enabled.setChecked(settings.get("ctrader_enabled", False))
        for key, inp in self._ct_inputs.items():
            inp.setText(str(settings.get(key, "")))
        self.spin_ct_lot.setValue(settings.get("ctrader_lot_size", 0.01))
        self.spin_ct_max_pos.setValue(settings.get("ctrader_max_positions", 3))
        self.chk_ct_auto_open.setChecked(settings.get("ctrader_auto_open", True))
        self.chk_ct_auto_close.setChecked(settings.get("ctrader_auto_close", True))

    def _browse_file(self) -> None:
        """Open file dialog to select a sound file."""
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Alert Sound",
            "",
            "Sound Files (*.wav *.mp3);;All Files (*)",
        )
        if path:
            self.txt_file.setText(path)

    def _test_sound(self) -> None:
        """Play the currently selected sound file."""
        path = self.txt_file.text()
        if path and os.path.isfile(path):
            play_sound(path)

    def _open_pair_algo(self) -> None:
        """Open the Pair Algorithm Settings as a modal dialog."""
        from takumi_trader.ui.pair_algo_dialog import PairAlgoDialog
        dlg = PairAlgoDialog(parent=self)
        dlg.exec()

    def _open_explanation(self) -> None:
        """Open the explanation / user manual dialog."""
        dialog = ExplanationDialog(self)
        dialog.exec()

    def _accept(self) -> None:
        """Save settings and close."""
        save_settings(self.get_settings())
        self.accept()

    def get_settings(self) -> dict:
        """Return the current widget values as a dict."""
        return {
            "sound_file": self.txt_file.text(),
            "sound_enabled": self.chk_sound.isChecked(),
            "cooldown_seconds": self.spin_cooldown.value() * 60,
            "font_size": self.spin_font_size.value(),
            "compact_mode": self.chk_compact.isChecked(),
            # cTrader
            "ctrader_enabled": self.chk_ctrader_enabled.isChecked(),
            "ctrader_client_id": self._ct_inputs["ctrader_client_id"].text(),
            "ctrader_client_secret": self._ct_inputs["ctrader_client_secret"].text(),
            "ctrader_access_token": self._ct_inputs["ctrader_access_token"].text(),
            "ctrader_account_id": self._ct_inputs["ctrader_account_id"].text(),
            "ctrader_lot_size": self.spin_ct_lot.value(),
            "ctrader_auto_open": self.chk_ct_auto_open.isChecked(),
            "ctrader_auto_close": self.chk_ct_auto_close.isChecked(),
            "ctrader_max_positions": self.spin_ct_max_pos.value(),
        }
