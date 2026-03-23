# Takumi Trader: Execution Engine (匠トレーダー)
*A private, AI-assisted trade execution bridge for Windows 11.*

TAKUMI (匠 — master craftsman) is a proprietary algorithmic execution engine that bridges local analysis logic with a high-performance execution layer via the **cTrader Open API**.

## Key Features
* **Automated Order Execution:** Direct-to-market order placement via cTrader Open API based on proprietary signal triggers.
* **Dynamic Pair Management:** Real-time monitoring and active management of multiple currency pairs simultaneously.
* **Risk Governance:** Automated enforcement of Stop-Loss (SL) and Take-Profit (TP) parameters to ensure capital protection.
* **AI-Integrated Development:** Built and maintained using a Claude Code workflow for high-standard code architecture and version stability.

## System Architecture
* **Framework:** PyQt6 native Windows 11 desktop application.
* **Connectivity:** cTrader Open API v2 via Protobuf/TCP (Twisted reactor).
* **Data Handling:** Optimized for low-latency data processing and local storage.

```
takumi_trader/
├── main.py                          # Application entry point
├── core/
│   ├── ctrader_worker.py            # cTrader Open API bridge (Twisted + Qt)
│   ├── ctrader_position_manager.py  # Position tracking & duplicate prevention
│   └── ...                          # Proprietary analysis modules (local only)
└── ui/
    ├── settings_dialog.py           # Settings & cTrader configuration
    └── ...                          # Additional dialogs (local only)
```

## cTrader Open API Integration

The cTrader integration uses the [cTrader Open API](https://help.ctrader.com/open-api/) via Protobuf over TCP:

- **Authentication flow**: Application auth → Account auth → Symbol list → Ready
- **Order execution**: Market orders with configurable lot sizes
- **Position lifecycle**: Open → Track → Close (manual or signal-based)
- **Reconnection**: Automatic reconnect with exponential backoff
- **Thread safety**: Twisted reactor in daemon thread, Qt signals via `QMetaObject.invokeMethod`

### Key files:
- [`takumi_trader/core/ctrader_worker.py`](takumi_trader/core/ctrader_worker.py) — Full cTrader Open API client
- [`takumi_trader/core/ctrader_position_manager.py`](takumi_trader/core/ctrader_position_manager.py) — Position state management
- [`takumi_trader/ui/settings_dialog.py`](takumi_trader/ui/settings_dialog.py) — cTrader configuration UI

## Dependencies

```
PyQt6
ctrader_open_api
service_identity
```

## Setup

```bash
pip install -r requirements.txt
python -m takumi_trader.main
```

## Security & Privacy
This repository contains the **cTrader integration layer** and application skeleton.
* **Proprietary Logic:** All analysis and strategy modules are stored locally and are not part of this public repository.
* **Credentials:** All API keys (`Client ID`, `Secret`) and tokens are managed via local settings and are strictly excluded from version control.

---
*Note: This application is intended for private, personal use and is not for commercial distribution.*
