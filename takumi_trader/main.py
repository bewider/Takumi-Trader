"""Entry point for TAKUMI Trader application."""

from __future__ import annotations

import logging
import sys
import traceback

_CRASH_LOG = "D:\\Trading\\crash_log.txt"


def main() -> None:
    """Launch the TAKUMI Trader application."""
    try:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )

        from PyQt6.QtWidgets import QApplication
        from takumi_trader.ui.main_window import MainWindow

        app = QApplication(sys.argv)
        app.setApplicationName("TAKUMI Trader")
        app.setOrganizationName("TAKUMITrader")

        # NOTE: Twisted reactor is started lazily by CTraderBridge.start()
        # only when cTrader auto-trading is actually enabled.

        window = MainWindow()
        window.show()

        sys.exit(app.exec())

    except Exception:
        tb = traceback.format_exc()
        try:
            with open(_CRASH_LOG, "w", encoding="utf-8") as f:
                f.write(tb)
        except Exception:
            pass
        raise


if __name__ == "__main__":
    # Required for multiprocessing on Windows (spawn) and PyInstaller bundles
    from multiprocessing import freeze_support
    freeze_support()
    main()
