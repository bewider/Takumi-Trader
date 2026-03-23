# TAKUMI Trader (匠トレーダー)

A real-time **forex currency strength scanner** and **automated trading application** built with Python and PyQt6.

TAKUMI (匠 — master craftsman) analyzes the relative strength of 8 major currencies across 28 forex pairs in real time, generating high-conviction trade signals with automated execution via the **cTrader Open API**.

## Features

- **Real-time currency strength analysis** — Monitors 28 forex pairs with 1-minute resolution
- **Multi-timeframe regime detection** — H4/D1 trend alignment for higher conviction signals
- **Conviction-based filtering** — 4 independent quality filters score each signal (0–100)
- **Session-aware trading** — Adapts to Tokyo, London, Frankfurt, NY session characteristics
- **Automated trade execution** — Opens and closes positions via cTrader Open API (Protobuf/TCP)
- **Position management** — Tracks open positions, prevents duplicates, auto-reconciles with broker
- **Backtesting engine** — Full historical backtesting with Dukascopy M1 data
- **Parameter optimization** — Joint entry + exit optimization with SL/TP grid search
- **Desktop notifications** — Toast alerts and sound notifications for trade signals

## Architecture

```
takumi_trader/
├── main.py                          # Application entry point
├── core/
│   ├── ctrader_worker.py            # cTrader Open API bridge (Twisted + Qt)
│   ├── ctrader_position_manager.py  # Position tracking & duplicate prevention
│   ├── strength.py                  # Currency strength calculations (Numba JIT)
│   ├── backtester.py                # Backtesting engine
│   ├── param_optimizer.py           # Parameter optimization
│   └── ...                          # Signal filters, exit engine, session logic
└── ui/
    ├── main_window.py               # Main scanner UI
    ├── settings_dialog.py           # Settings & cTrader configuration
    ├── backtest_dialog.py           # Backtest & optimizer UI
    └── ...                          # Additional dialogs
```

## cTrader Integration

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

## Requirements

- Python 3.11+
- MetaTrader 5 (live data source)
- cTrader demo or live account (for automated trading)

## Dependencies

```
PyQt6
numpy
numba
ctrader_open_api
service_identity
tzdata
MetaTrader5
windows-toasts
pygame
```

## Setup

```bash
pip install -r requirements.txt
python -m takumi_trader.main
```

## Note

This repository contains the **cTrader integration layer** and application skeleton. The proprietary trading algorithms (signal generation, exit logic, optimization engine) are not included in this public repository.

## License

All rights reserved. This code is shared for cTrader Open API application review purposes.
