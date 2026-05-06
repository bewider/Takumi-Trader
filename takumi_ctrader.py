"""TAKUMI cTrader Executor — Mirrors paper trades to cTrader.

Runs as a separate process alongside the main TAKUMI Trader app.
Watches for trade signals and executes them on cTrader via Open API.

Usage:  python takumi_ctrader.py

Architecture:
  Main App writes signals → data/ctrader_signals.json
  This executor reads signals → executes on cTrader
  Results written → data/ctrader_executions.json
"""

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from threading import Thread

from twisted.internet import reactor

from ctrader_open_api import Client, Protobuf, TcpProtocol, Auth, EndPoints
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import *
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ctrader")

# Paths
BASE_DIR = Path(__file__).parent
CONFIG_FILE = BASE_DIR / "data" / "ctrader_config.json"
SIGNALS_FILE = BASE_DIR / "data" / "ctrader_signals.json"
EXECUTIONS_FILE = BASE_DIR / "data" / "ctrader_executions.json"

# cTrader symbol mapping (pair name → cTrader symbol ID)
# Will be populated dynamically after connection
_SYMBOL_MAP: dict[str, int] = {}
_SYMBOL_DETAILS: dict[int, dict] = {}

# Track processed signals to avoid duplicates
_PROCESSED_SIGNALS: set[str] = set()

# Global client reference
_CLIENT: Client | None = None
_ACCOUNT_ID: int = 0
_ACCESS_TOKEN: str = ""
_CONNECTED: bool = False
_AUTHENTICATED: bool = False


def _load_config() -> dict:
    """Load cTrader configuration."""
    if not CONFIG_FILE.exists():
        logger.error("Config file not found: %s", CONFIG_FILE)
        sys.exit(1)
    return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))


def _load_executions() -> list[dict]:
    """Load execution history."""
    if EXECUTIONS_FILE.exists():
        try:
            return json.loads(EXECUTIONS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


def _save_execution(execution: dict) -> None:
    """Append an execution record."""
    history = _load_executions()
    history.append(execution)
    # Keep last 500
    if len(history) > 500:
        history = history[-500:]
    EXECUTIONS_FILE.write_text(
        json.dumps(history, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def _pip_value(pair: str) -> float:
    """Get pip value for a pair."""
    return 0.01 if "JPY" in pair else 0.0001


def _calculate_volume(pair: str, sl_pips: float, risk_pct: float, balance: float) -> int:
    """Calculate position volume in units (lots * 100000).

    Risk-based sizing: risk_amount = balance * risk_pct / 100
    volume = risk_amount / (sl_pips * pip_value)
    """
    if sl_pips <= 0:
        return 100000  # Default 1 lot

    risk_amount = balance * risk_pct / 100.0
    pip = _pip_value(pair)

    # Volume in units (1 lot = 100000 units)
    volume = risk_amount / (sl_pips * pip)

    # Round to nearest 1000 units (0.01 lot)
    volume = max(1000, round(volume / 1000) * 1000)

    return int(volume)


def _on_connected(client: Client):
    """Called when TCP connection is established."""
    global _CONNECTED
    _CONNECTED = True
    logger.info("TCP connected to cTrader")

    # Authenticate application
    config = _load_config()
    request = ProtoOAApplicationAuthReq()
    request.clientId = config["client_id"]
    request.clientSecret = config["client_secret"]
    deferred = client.send(request)
    deferred.addCallbacks(_on_app_auth, _on_error)


def _on_app_auth(result):
    """Called after application authentication."""
    logger.info("Application authenticated")

    # Authenticate account
    global _ACCOUNT_ID, _ACCESS_TOKEN
    config = _load_config()
    _ACCOUNT_ID = int(config["account_id"])
    _ACCESS_TOKEN = config["access_token"]

    request = ProtoOAAccountAuthReq()
    request.ctidTraderAccountId = _ACCOUNT_ID
    request.accessToken = _ACCESS_TOKEN
    deferred = _CLIENT.send(request)
    deferred.addCallbacks(_on_account_auth, _on_error)


def _on_account_auth(result):
    """Called after account authentication."""
    global _AUTHENTICATED
    _AUTHENTICATED = True
    logger.info("Account %d authenticated — ready to trade!", _ACCOUNT_ID)

    # Load symbol list
    request = ProtoOASymbolsListReq()
    request.ctidTraderAccountId = _ACCOUNT_ID
    deferred = _CLIENT.send(request)
    deferred.addCallbacks(_on_symbols_loaded, _on_error)


def _on_symbols_loaded(result):
    """Called when symbol list is received."""
    if hasattr(result, 'symbol'):
        for sym in result.symbol:
            name = sym.symbolName if hasattr(sym, 'symbolName') else ""
            # Map common forex pair names
            clean = name.replace("/", "").replace("_", "").upper()
            if len(clean) == 6:
                _SYMBOL_MAP[clean] = sym.symbolId
                _SYMBOL_DETAILS[sym.symbolId] = {
                    "name": name,
                    "id": sym.symbolId,
                    "digits": getattr(sym, 'digits', 5),
                }

    logger.info("Loaded %d symbols (forex: %d)", len(_SYMBOL_DETAILS), len(_SYMBOL_MAP))

    # Log some known pairs
    for pair in ["EURUSD", "GBPJPY", "AUDUSD"]:
        if pair in _SYMBOL_MAP:
            logger.info("  %s → ID %d", pair, _SYMBOL_MAP[pair])

    # Start signal polling
    _start_signal_polling()


def _on_error(error):
    """Handle errors."""
    logger.error("cTrader error: %s", error)


def _on_execution_report(result):
    """Called when an order execution is reported."""
    logger.info("Execution report received")


def _place_order(pair: str, direction: str, volume: int, sl_price: float, tp_price: float, label: str = "TAKUMI"):
    """Place a market order on cTrader."""
    if not _AUTHENTICATED or _CLIENT is None:
        logger.warning("Not authenticated, cannot place order")
        return

    symbol_id = _SYMBOL_MAP.get(pair)
    if symbol_id is None:
        logger.warning("Symbol not found for %s", pair)
        return

    request = ProtoOANewOrderReq()
    request.ctidTraderAccountId = _ACCOUNT_ID
    request.symbolId = symbol_id
    request.orderType = ProtoOAOrderType.MARKET
    request.tradeSide = ProtoOATradeSide.BUY if direction == "BUY" else ProtoOATradeSide.SELL
    request.volume = volume
    request.label = label

    # Set SL/TP as absolute prices
    if sl_price > 0:
        request.stopLoss = sl_price
    if tp_price > 0:
        request.takeProfit = tp_price

    logger.info(
        "PLACING ORDER: %s %s vol=%d SL=%.5f TP=%.5f",
        direction, pair, volume, sl_price, tp_price,
    )

    deferred = _CLIENT.send(request)
    deferred.addCallbacks(
        lambda r: _on_order_placed(r, pair, direction, volume, sl_price, tp_price),
        _on_error,
    )


def _on_order_placed(result, pair, direction, volume, sl_price, tp_price):
    """Called after order is placed."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("ORDER PLACED: %s %s vol=%d", direction, pair, volume)

    _save_execution({
        "time": now,
        "pair": pair,
        "direction": direction,
        "volume": volume,
        "sl_price": sl_price,
        "tp_price": tp_price,
        "status": "filled",
        "type": "open",
    })


def _close_position(pair: str):
    """Close all positions for a pair."""
    if not _AUTHENTICATED or _CLIENT is None:
        return

    # Get open positions
    request = ProtoOAReconcileReq()
    request.ctidTraderAccountId = _ACCOUNT_ID
    deferred = _CLIENT.send(request)
    deferred.addCallbacks(
        lambda r: _on_positions_for_close(r, pair),
        _on_error,
    )


def _on_positions_for_close(result, target_pair: str):
    """Find and close positions for a specific pair."""
    if not hasattr(result, 'position'):
        return

    for pos in result.position:
        symbol_id = pos.tradeData.symbolId
        # Find pair name from symbol ID
        pair_name = None
        for name, sid in _SYMBOL_MAP.items():
            if sid == symbol_id:
                pair_name = name
                break

        if pair_name == target_pair:
            # Close this position
            request = ProtoOAClosePositionReq()
            request.ctidTraderAccountId = _ACCOUNT_ID
            request.positionId = pos.positionId
            request.volume = pos.tradeData.volume

            logger.info("CLOSING: %s position %d vol=%d", target_pair, pos.positionId, pos.tradeData.volume)
            deferred = _CLIENT.send(request)
            deferred.addCallbacks(
                lambda r: _on_position_closed(r, target_pair),
                _on_error,
            )


def _on_position_closed(result, pair):
    """Called after position is closed."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("POSITION CLOSED: %s", pair)
    _save_execution({
        "time": now,
        "pair": pair,
        "status": "closed",
        "type": "close",
    })


def _process_signals():
    """Check for new trade signals from the main app."""
    if not _AUTHENTICATED:
        return

    if not SIGNALS_FILE.exists():
        return

    try:
        signals = json.loads(SIGNALS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return

    config = _load_config()
    risk_pct = config.get("risk_percent", 2.0)

    for signal in signals:
        signal_id = signal.get("id", "")
        if signal_id in _PROCESSED_SIGNALS:
            continue

        action = signal.get("action", "")
        pair = signal.get("pair", "")

        if action == "open":
            direction = signal.get("direction", "")
            sl_price = signal.get("sl_price", 0.0)
            tp_price = signal.get("tp_price", 0.0)
            sl_pips = signal.get("sl_pips", 10.0)

            # Calculate volume based on risk
            # Use a default balance for demo — will be replaced with actual
            balance = 10000.0  # Demo balance
            volume = _calculate_volume(pair, sl_pips, risk_pct, balance)

            _place_order(pair, direction, volume, sl_price, tp_price, f"TAKUMI_{signal_id[:8]}")
            _PROCESSED_SIGNALS.add(signal_id)

        elif action == "close":
            _close_position(pair)
            _PROCESSED_SIGNALS.add(signal_id)

    # Clear processed signals from file
    remaining = [s for s in signals if s.get("id", "") not in _PROCESSED_SIGNALS]
    if len(remaining) != len(signals):
        SIGNALS_FILE.write_text(
            json.dumps(remaining, indent=2), encoding="utf-8"
        )


def _start_signal_polling():
    """Start polling for trade signals every 2 seconds."""
    def poll():
        try:
            _process_signals()
        except Exception as e:
            logger.warning("Signal processing error: %s", e)
        # Schedule next poll
        reactor.callLater(2.0, poll)

    reactor.callLater(1.0, poll)
    logger.info("Signal polling started (every 2s)")


def _on_message(client, message):
    """Handle incoming messages from cTrader."""
    if message.payloadType in [ProtoOAExecutionEvent().payloadType]:
        logger.info("Execution event received")
    elif message.payloadType in [ProtoHeartbeatEvent().payloadType]:
        pass  # Heartbeat, ignore


def main():
    global _CLIENT

    config = _load_config()

    if not config.get("access_token"):
        logger.error("No access token! Run ctrader_auth.py first.")
        sys.exit(1)

    if not config.get("account_id"):
        logger.error("No account ID! Run ctrader_auth.py first.")
        sys.exit(1)

    if not config.get("enabled", False):
        logger.warning("cTrader trading is DISABLED in config.")
        logger.warning("Set 'enabled': true in data/ctrader_config.json to start trading.")
        logger.info("Starting in MONITOR mode (will connect but not trade)...")

    # Create empty signals file if it doesn't exist
    if not SIGNALS_FILE.exists():
        SIGNALS_FILE.write_text("[]", encoding="utf-8")

    # Connect to cTrader
    host = EndPoints.PROTOBUF_LIVE_HOST if config.get("environment") == "live" else EndPoints.PROTOBUF_DEMO_HOST
    port = EndPoints.PROTOBUF_PORT

    logger.info("Connecting to cTrader %s (%s:%s)...", config.get("environment", "demo"), host, port)

    _CLIENT = Client(host, port, TcpProtocol)

    # Set up callbacks
    _CLIENT.setConnectedCallback(_on_connected)
    _CLIENT.setDisconnectedCallback(lambda client, reason: logger.warning("Disconnected: %s", reason))
    _CLIENT.setMessageReceivedCallback(_on_message)

    # Start connection
    _CLIENT.startService()

    print()
    print("=" * 50)
    print("  TAKUMI cTrader Executor")
    print(f"  Account: {config.get('account_id')}")
    print(f"  Mode: {'LIVE' if config.get('enabled') else 'MONITOR (disabled)'}")
    print(f"  Risk: {config.get('risk_percent', 2.0)}%")
    print("=" * 50)
    print()
    print("Watching for trade signals from TAKUMI Trader...")
    print("Press Ctrl+C to stop")
    print()

    reactor.run()


if __name__ == "__main__":
    main()
