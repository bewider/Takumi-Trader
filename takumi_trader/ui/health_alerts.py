"""Centralized popup-alert manager for TAKUMI runtime failures.

Goal: surface ANY broken connection, rejected order, failed save, or other
silent failure as a visible popup so the operator can react. Designed to
be non-blocking, deduplicated, and rate-limited so it doesn't spam during
flapping.

Severity levels:
    "warning"  → yellow icon, auto-cooldown 5 min
    "error"    → red icon, auto-cooldown 5 min, taskbar flash
    "critical" → red icon, auto-cooldown 2 min, taskbar flash, sound

Public API:
    health = HealthAlerts(parent_window)
    health.notify(severity, source, message, dedup_key=None, cooldown=None)
    health.notify_recovery(source, message, dedup_key=None)
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Optional

from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtWidgets import QApplication, QMessageBox, QWidget

logger = logging.getLogger(__name__)

_DEFAULT_COOLDOWNS = {
    "warning": 300,
    "error": 300,
    "critical": 120,
}

_ICONS = {
    "warning": QMessageBox.Icon.Warning,
    "error": QMessageBox.Icon.Critical,
    "critical": QMessageBox.Icon.Critical,
}

_LOG = {
    "warning": logger.warning,
    "error": logger.error,
    "critical": logger.critical,
}


class HealthAlerts:
    """Modeless popup-alert manager with per-key cooldown.

    The same `dedup_key` only triggers a NEW popup once per `cooldown_sec`.
    If a previous popup with the same key is still on-screen, its message
    is updated in-place and the dialog is brought to front instead of
    spawning a new one.
    """

    def __init__(self, parent: QWidget):
        self._parent = parent
        # key → epoch timestamp of last alert
        self._last_alert: dict[str, float] = {}
        # key → currently-displayed dialog (so we update in place)
        self._open_dialogs: dict[str, QMessageBox] = {}

    # ──────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────

    def notify(
        self,
        severity: str,
        source: str,
        message: str,
        dedup_key: Optional[str] = None,
        cooldown: Optional[float] = None,
    ) -> None:
        """Show a popup alert.

        Args:
            severity: "warning" | "error" | "critical"
            source: short subsystem name (e.g. "MT5", "cTrader", "DTC", "Journal")
            message: full description shown in the popup body
            dedup_key: identical keys within cooldown produce no new popup
                       (default = source + first 40 chars of message)
            cooldown: seconds before this key may pop again (default by severity)
        """
        if severity not in _ICONS:
            severity = "warning"
        key = dedup_key or f"{source}:{message[:40]}"
        cd = cooldown if cooldown is not None else _DEFAULT_COOLDOWNS[severity]
        now = time.time()
        last = self._last_alert.get(key, 0.0)
        if last > 0 and now - last < cd:
            # Within cooldown — log but suppress popup
            logger.info("[HEALTH] %s/%s suppressed (cooldown %.0fs remaining)",
                        source, key, cd - (now - last))
            return
        self._last_alert[key] = now

        # Always log loudly
        _LOG.get(severity, logger.warning)("[HEALTH-ALERT] %s | %s | %s",
                                            severity.upper(), source, message)

        # Append to in-app alert panel if available
        self._append_to_alert_panel(severity, source, message)

        # If a dialog with this key is already open, just refresh it
        existing = self._open_dialogs.get(key)
        if existing is not None:
            try:
                existing.setText(self._format_text(severity, source, message))
                existing.activateWindow()
                existing.raise_()
                return
            except Exception:
                # Dialog may have been destroyed mid-update; fall through
                self._open_dialogs.pop(key, None)

        # Build a fresh modeless popup
        dlg = QMessageBox(self._parent)
        dlg.setIcon(_ICONS[severity])
        dlg.setWindowTitle(self._format_title(severity, source))
        dlg.setText(self._format_text(severity, source, message))
        dlg.setStandardButtons(QMessageBox.StandardButton.Ok)
        # Stay on top so it's not buried under TAKUMI's main window
        dlg.setWindowFlags(
            dlg.windowFlags()
            | Qt.WindowType.WindowStaysOnTopHint
        )
        dlg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
        # Track + cleanup on close
        dlg.finished.connect(lambda _r, k=key: self._open_dialogs.pop(k, None))
        # Modeless show — does NOT block trading
        dlg.show()
        self._open_dialogs[key] = dlg

        # Flash the OS taskbar so the user notices even if minimized
        try:
            QApplication.alert(self._parent, 0)  # 0 = until window focused
        except Exception:
            pass

        # Critical alerts also play the existing alert sound, if available
        if severity == "critical":
            try:
                from takumi_trader.core.alerts import play_sound
                play_sound("")  # uses configured default
            except Exception:
                pass

    def notify_recovery(
        self,
        source: str,
        message: str,
        dedup_key: Optional[str] = None,
    ) -> None:
        """Quietly mark a previous failure as resolved.

        - Closes any open dialog with the matching dedup_key
        - Resets the cooldown so a new failure on the same key alerts immediately
        - Logs an [HEALTH-RECOVERY] line
        - Appends a green note to the in-app alert panel
        """
        key = dedup_key or f"{source}:recovery"
        logger.info("[HEALTH-RECOVERY] %s | %s", source, message)
        # Close any open popup for this key
        existing = self._open_dialogs.pop(key, None)
        if existing is not None:
            try:
                existing.close()
            except Exception:
                pass
        # Reset cooldown so future failure on same key alerts at once
        self._last_alert.pop(key, None)
        # Note in alert panel
        self._append_to_alert_panel("recovery", source, message)

    # ──────────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _format_title(severity: str, source: str) -> str:
        sev_label = {"warning": "Warning", "error": "Error",
                     "critical": "CRITICAL"}.get(severity, severity.upper())
        return f"TAKUMI {sev_label} — {source}"

    @staticmethod
    def _format_text(severity: str, source: str, message: str) -> str:
        ts = datetime.now().strftime("%H:%M:%S")
        return f"[{ts}] {source}\n\n{message}"

    def _append_to_alert_panel(self, severity: str, source: str, message: str) -> None:
        """Add a coloured one-liner to the parent's alert history panel."""
        if not (self._parent and hasattr(self._parent, "_alert_history")):
            return
        emoji = {
            "warning": "\u26a0\ufe0f",       # ⚠️
            "error": "\u274c",                # ❌
            "critical": "\U0001f6a8",         # 🚨
            "recovery": "\u2705",             # ✅
        }.get(severity, "\u26a0\ufe0f")
        color = {
            "warning": "#f57c00",
            "error": "#c62828",
            "critical": "#b71c1c",
            "recovery": "#2e7d32",
        }.get(severity, "#f57c00")
        ts = datetime.now().strftime("%H:%M:%S")
        # Trim long messages
        msg_short = message.replace("\n", " ")[:140]
        html = (
            f'<span style="font-size:9pt; color:#666;">[{ts}]</span> '
            f'<span style="font-size:10pt; color:{color}; font-weight:bold;">'
            f'{emoji} {severity.upper()}: {source}</span> '
            f'<span style="font-size:10pt;">{msg_short}</span>'
        )
        try:
            self._parent._alert_history.appendleft(html)
            if hasattr(self._parent, "_refresh_alert_panel"):
                self._parent._refresh_alert_panel()
        except Exception:
            pass


class StalenessWatchdog:
    """Watchdog that fires an alert if no data tick is observed for a while.

    The MT5 worker emits `data_ready` once per cycle (~1 s). If the worker
    deadlocks, the MT5 terminal freezes, or the data feed dies, no signal
    arrives. This watchdog polls a `last_data_ts` attribute on the parent
    and pops a critical alert when the gap exceeds `threshold_sec`.

    Recovery: when fresh data arrives, the parent should call
    `notify_recovery` itself; this class only triggers the alarm.
    """

    def __init__(
        self,
        parent: QWidget,
        health: HealthAlerts,
        threshold_sec: float = 90.0,
        check_interval_ms: int = 15000,
    ):
        self._parent = parent
        self._health = health
        self._threshold = threshold_sec
        self._fired = False
        self._timer = QTimer(parent)
        self._timer.timeout.connect(self._tick)
        self._timer.start(check_interval_ms)

    def _tick(self) -> None:
        last_ts = getattr(self._parent, "_last_data_ts", 0.0) or 0.0
        if last_ts <= 0:
            # No data yet — startup phase, don't alert
            return
        gap = time.time() - last_ts
        if gap > self._threshold:
            if not self._fired:
                self._health.notify(
                    "critical", "Data Feed",
                    f"No MT5 ticks received for {gap:.0f} seconds.\n\n"
                    "TAKUMI is BLIND — paper trades and live cTrader orders "
                    "are paused. Check the MT5 terminal:\n"
                    "  1. Is MT5 still running and logged in?\n"
                    "  2. Is the data subscription active?\n"
                    "  3. Restart MT5 if needed.",
                    dedup_key="data_feed_stale",
                    cooldown=180,
                )
                self._fired = True
        else:
            if self._fired:
                self._health.notify_recovery(
                    "Data Feed",
                    f"Tick stream resumed (gap was {gap:.0f}s).",
                    dedup_key="data_feed_stale",
                )
                self._fired = False
