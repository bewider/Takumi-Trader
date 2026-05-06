"""Minimal test to debug frozen exe crash."""
import sys
import traceback

try:
    with open("test_crash_log.txt", "w") as f:
        f.write("Step 1: Python started\n")

        from datetime import datetime, timezone, timedelta
        f.write("Step 2: datetime imported\n")

        try:
            from zoneinfo import ZoneInfo
            jst = ZoneInfo("Asia/Tokyo")
            f.write(f"Step 3: ZoneInfo OK: {jst}\n")
        except Exception as e:
            jst = timezone(timedelta(hours=9))
            f.write(f"Step 3: ZoneInfo failed ({e}), using fallback\n")

        f.write(f"Step 4: JST time = {datetime.now(jst)}\n")

        import pygame
        f.write(f"Step 5: pygame OK: {pygame.ver}\n")

        from PyQt6.QtWidgets import QApplication
        f.write("Step 6: PyQt6 OK\n")

        from takumi_trader.ui.main_window import MainWindow
        f.write("Step 7: MainWindow imported OK\n")

        app = QApplication(sys.argv)
        f.write("Step 8: QApplication created\n")

        window = MainWindow()
        f.write("Step 9: MainWindow created\n")

        window.show()
        f.write("Step 10: Window shown - ALL OK\n")

        sys.exit(app.exec())

except Exception:
    tb = traceback.format_exc()
    with open("test_crash_log.txt", "a") as f:
        f.write(f"\nCRASH:\n{tb}\n")
    print(tb)
    input("Press Enter...")
