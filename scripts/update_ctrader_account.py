"""One-off: switch cTrader account to 9984764 and set risk to 2%.

Reads/writes the same QSettings store that takumi_trader.ui.settings_dialog
uses, so changes take effect on next TAKUMI restart.
"""
from __future__ import annotations

import sys
from PyQt6.QtCore import QSettings, QCoreApplication

SETTINGS_ORG = "TAKUMITrader"
SETTINGS_APP = "TAKUMITrader"

NEW_ACCOUNT_ID = "9984764"
NEW_RISK_PCT = 2.0  # percent (stored as 2.0, runtime divides by 100)


def main() -> int:
    # QCoreApplication is needed for QSettings on some platforms
    app = QCoreApplication.instance() or QCoreApplication(sys.argv)

    s = QSettings(SETTINGS_ORG, SETTINGS_APP)

    # Show current state
    print("BEFORE:")
    print(f"  ctrader_account_id = {s.value('ctrader/account_id', '(unset)', type=str)!r}")
    print(f"  ctrader_risk_pct   = {s.value('ctrader/risk_pct', None)!r}")
    print(f"  ctrader_enabled    = {s.value('ctrader/enabled', None)!r}")
    print(f"  ctrader_auto_open  = {s.value('ctrader/auto_open', None)!r}")
    print(f"  ctrader_max_pos    = {s.value('ctrader/max_positions', None)!r}")

    # Update
    s.setValue("ctrader/account_id", NEW_ACCOUNT_ID)
    s.setValue("ctrader/risk_pct", NEW_RISK_PCT)
    s.sync()

    # Verify
    print("\nAFTER:")
    print(f"  ctrader_account_id = {s.value('ctrader/account_id', '(unset)', type=str)!r}")
    print(f"  ctrader_risk_pct   = {s.value('ctrader/risk_pct', None)!r}")
    print(f"  ctrader_enabled    = {s.value('ctrader/enabled', None)!r}")
    print(f"  ctrader_auto_open  = {s.value('ctrader/auto_open', None)!r}")
    print(f"  ctrader_max_pos    = {s.value('ctrader/max_positions', None)!r}")

    print("\n✓ QSettings updated. Restart TAKUMI for changes to take effect.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
