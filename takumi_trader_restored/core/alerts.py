"""Windows toast notifications and sound playback for trend alerts."""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path

import pygame

logger = logging.getLogger(__name__)

# Initialize pygame mixer once
_mixer_initialized = False


def _ensure_mixer() -> bool:
    """Initialize pygame mixer if not already done."""
    global _mixer_initialized
    if not _mixer_initialized:
        try:
            pygame.mixer.init()
            _mixer_initialized = True
        except Exception:
            logger.exception("Failed to initialize pygame mixer")
            return False
    return True


def play_sound(file_path: str) -> None:
    """Play a WAV or MP3 sound file in a background thread.

    Args:
        file_path: Path to a .wav or .mp3 file.
    """
    if not file_path or not os.path.isfile(file_path):
        logger.warning("Sound file not found: %s", file_path)
        return

    def _play() -> None:
        try:
            if not _ensure_mixer():
                return
            pygame.mixer.music.load(file_path)
            pygame.mixer.music.play()
            while pygame.mixer.music.get_busy():
                time.sleep(0.1)
        except Exception:
            logger.exception("Error playing sound: %s", file_path)

    thread = threading.Thread(target=_play, daemon=True)
    thread.start()


def send_toast_notification(title: str, body: str) -> None:
    """Send a Windows 11 toast notification.

    Args:
        title: Notification title.
        body: Notification body text.
    """
    try:
        from windows_toasts import Toast, WindowsToaster

        toaster = WindowsToaster("TAKUMI Trader")
        toast = Toast()
        toast.text_fields = [title, body]
        toaster.show_toast(toast)
    except ImportError:
        logger.warning("windows-toasts not installed; skipping toast notification")
    except Exception:
        logger.exception("Failed to send toast notification")


class AlertManager:
    """Manages alert cooldowns and triggers for trend alerts."""

    def __init__(self, cooldown_seconds: int = 60) -> None:
        """Initialize the alert manager.

        Args:
            cooldown_seconds: Minimum seconds between alerts for the same pair.
        """
        self.cooldown_seconds = cooldown_seconds
        self._last_alert_time: dict[str, float] = {}
        self._previous_alert_pairs: set[str] = set()
        self.sound_enabled: bool = True
        self.sound_file: str = ""

    def update_cooldown(self, seconds: int) -> None:
        """Update the cooldown period."""
        self.cooldown_seconds = seconds

    def check_and_fire(
        self, current_alert_pairs: dict[str, tuple[str, dict[str, float]]]
    ) -> list[str]:
        """Check for new alerts and fire notifications.

        Args:
            current_alert_pairs: Dict of pair -> (direction_label, {tf: score}).
                e.g. {"GBPJPY": ("STRONG BUY", {"M1": 9.3, "M5": 9.1, "M15": 9.5})}

        Returns:
            List of pair symbols that newly triggered alerts.
        """
        current_set = set(current_alert_pairs.keys())
        newly_entered = current_set - self._previous_alert_pairs
        self._previous_alert_pairs = current_set

        now = time.time()
        fired: list[str] = []

        for pair in newly_entered:
            last_time = self._last_alert_time.get(pair, 0.0)
            if now - last_time < self.cooldown_seconds:
                continue

            direction, scores = current_alert_pairs[pair]
            self._last_alert_time[pair] = now
            fired.append(pair)

            # Build notification text
            score_parts = " | ".join(
                f"{tf}: {scores[tf]:+.1f}" for tf in ["M1", "M5", "M15"]
            )
            body = f"{pair} — {direction} ({score_parts})"

            # Toast
            send_toast_notification("TAKUMI Trader", body)

            # Sound
            if self.sound_enabled and self.sound_file:
                play_sound(self.sound_file)

            logger.info("Alert fired: %s", body)

        return fired
