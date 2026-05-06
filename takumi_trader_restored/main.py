"""Entry point for TAKUMI Trader application."""

from __future__ import annotations

import logging
import sys
import threading
import traceback
from datetime import datetime

_CRASH_LOG = "D:\\Trading\\crash_log.txt"


def _write_crash(context: str, exc_text: str) -> None:
    """Append crash info to the crash log file."""
    try:
        with open(_CRASH_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"{datetime.now().isoformat()} — {context}\n")
            f.write(exc_text)
            f.write("\n")
    except Exception:
        pass


def _sys_excepthook(exc_type, exc_value, exc_tb):
    """Global uncaught exception handler."""
    tb = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    logging.getLogger(__name__).critical("Uncaught exception:\n%s", tb)
    _write_crash("sys.excepthook", tb)


def _thread_excepthook(args):
    """Uncaught exception in a thread."""
    tb = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
    logging.getLogger(__name__).critical("Thread %s crash:\n%s", args.thread, tb)
    _write_crash(f"threading.excepthook ({args.thread})", tb)


def main() -> None:
    """Launch the TAKUMI Trader application."""
    # Install global exception hooks
    sys.excepthook = _sys_excepthook
    threading.excepthook = _thread_excepthook

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
        _write_crash("main() exception", tb)
        raise


if __name__ == "__main__":
    # Required for multiprocessing on Windows (spawn) and PyInstaller bundles
    from multiprocessing import freeze_support
    freeze_support()
    main()
