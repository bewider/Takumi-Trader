"""Quick-access filter toggle toolbar (Phase 8.7).

Horizontal strip with toggle buttons for each alert quality filter.
Provides instant toggle without opening settings dialog.
"""

from __future__ import annotations

from PyQt6.QtCore import QSettings, pyqtSignal
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import QHBoxLayout, QPushButton, QWidget

from takumi_trader.core.filter_engine import FilterSettings

SETTINGS_ORG = "TAKUMITrader"
SETTINGS_APP = "TAKUMITrader"

# Filter definitions: (settings_key, display_name, FilterSettings attr)
_FILTERS = [
    ("filter_trend_regime_enabled", "HTF", "trend_regime_enabled"),
    ("filter_strength_velocity_enabled", "VEL", "strength_velocity_enabled"),
    ("filter_isolation_enabled", "ISOL", "isolation_enabled"),
    ("filter_structural_enabled", "STRUCT", "structural_enabled"),
]

# Conviction threshold presets (cycle through on click)
_CONV_PRESETS = [50, 60, 70, 80]


class FilterToolbar(QWidget):
    """Horizontal toolbar with toggle buttons for each filter."""

    filters_changed = pyqtSignal()

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._filter_settings = FilterSettings()
        self._load_from_qsettings()

        layout = QHBoxLayout(self)
        layout.setContentsMargins(4, 1, 4, 1)
        layout.setSpacing(3)

        self._buttons: dict[str, QPushButton] = {}

        for settings_key, display_name, attr_name in _FILTERS:
            is_on = getattr(self._filter_settings, attr_name)
            btn = QPushButton(self._btn_text(display_name, is_on))
            btn.setCheckable(True)
            btn.setChecked(is_on)
            btn.setFont(QFont("Segoe UI", 9))
            btn.toggled.connect(
                lambda checked, key=settings_key, attr=attr_name, name=display_name: self._on_toggle(
                    key, attr, name, checked
                )
            )
            self._apply_button_style(btn)
            layout.addWidget(btn)
            self._buttons[settings_key] = btn

        layout.addStretch()

        # Conviction threshold button
        self._conv_idx = _CONV_PRESETS.index(self._filter_settings.conviction_full_threshold) \
            if self._filter_settings.conviction_full_threshold in _CONV_PRESETS else 2
        self._conv_btn = QPushButton(
            f"Conv \u2265 {self._filter_settings.conviction_full_threshold}"
        )
        self._conv_btn.setFont(QFont("Segoe UI", 9))
        self._conv_btn.setToolTip("Click to cycle conviction threshold")
        self._conv_btn.clicked.connect(self._cycle_threshold)
        self._conv_btn.setStyleSheet(
            "QPushButton { background: #e0e0e0; color: #333; border: 1px solid #bbb;"
            " padding: 2px 5px; border-radius: 3px; font-size: 11px; }"
            " QPushButton:hover { background: #d0d0d0; }"
        )
        layout.addWidget(self._conv_btn)

        self.setStyleSheet("background: #f0f0f0; border-bottom: 1px solid #d0d0d0;")
        self.setFixedHeight(32)

    @property
    def filter_settings(self) -> FilterSettings:
        return self._filter_settings

    @staticmethod
    def _btn_text(name: str, on: bool) -> str:
        return f"\u2705 {name}" if on else f"\u274c {name}"

    def _on_toggle(self, settings_key: str, attr_name: str, display_name: str, checked: bool) -> None:
        setattr(self._filter_settings, attr_name, checked)
        self._save_to_qsettings()
        btn = self._buttons.get(settings_key)
        if btn:
            btn.setText(self._btn_text(display_name, checked))
            self._apply_button_style(btn)
        self.filters_changed.emit()

    def _cycle_threshold(self) -> None:
        self._conv_idx = (self._conv_idx + 1) % len(_CONV_PRESETS)
        new_val = _CONV_PRESETS[self._conv_idx]
        self._filter_settings.conviction_full_threshold = new_val
        self._filter_settings.conviction_dimmed_threshold = max(20, new_val - 25)
        self._conv_btn.setText(f"Conv \u2265 {new_val}")
        self._save_to_qsettings()
        self.filters_changed.emit()

    def _apply_button_style(self, btn: QPushButton) -> None:
        btn.setStyleSheet(
            """
            QPushButton {
                padding: 2px 5px; border-radius: 3px;
                border: 1px solid #cc9999; background: #f5e0e0;
                color: #993333; font-size: 11px;
            }
            QPushButton:checked {
                background: #d4edda; color: #155724;
                border: 1px solid #88bb99;
            }
            QPushButton:hover { border: 1px solid #888; }
            """
        )

    def _load_from_qsettings(self) -> None:
        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        fs = self._filter_settings
        fs.trend_regime_enabled = s.value("filters/trend_regime", True, type=bool)
        fs.strength_velocity_enabled = s.value("filters/velocity", True, type=bool)
        fs.isolation_enabled = s.value("filters/isolation", True, type=bool)
        fs.structural_enabled = s.value("filters/structural", True, type=bool)
        fs.conviction_full_threshold = s.value("filters/conv_full", 70, type=int)
        fs.conviction_dimmed_threshold = s.value("filters/conv_dimmed", 45, type=int)

    def _save_to_qsettings(self) -> None:
        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        fs = self._filter_settings
        s.setValue("filters/trend_regime", fs.trend_regime_enabled)
        s.setValue("filters/velocity", fs.strength_velocity_enabled)
        s.setValue("filters/isolation", fs.isolation_enabled)
        s.setValue("filters/structural", fs.structural_enabled)
        s.setValue("filters/conv_full", fs.conviction_full_threshold)
        s.setValue("filters/conv_dimmed", fs.conviction_dimmed_threshold)

    def sync_from_settings(self, fs: FilterSettings) -> None:
        """Sync toolbar state from external FilterSettings (e.g. settings dialog)."""
        self._filter_settings = fs
        for settings_key, display_name, attr_name in _FILTERS:
            btn = self._buttons.get(settings_key)
            if btn:
                is_on = getattr(fs, attr_name)
                btn.blockSignals(True)
                btn.setChecked(is_on)
                btn.setText(self._btn_text(display_name, is_on))
                self._apply_button_style(btn)
                btn.blockSignals(False)
        self._conv_btn.setText(f"Conv \u2265 {fs.conviction_full_threshold}")
        self._save_to_qsettings()
