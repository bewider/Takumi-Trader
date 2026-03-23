"""cTrader Open API connection bridge — Twisted in thread + Qt signals (Stage 2).

Runs the ctrader_open_api Client inside a Twisted reactor thread.
Communicates back to the PyQt6 main thread via QMetaObject.invokeMethod.

ALL ctrader_open_api / twisted imports are LAZY to avoid crashing the app
if the packages are broken or unavailable in the frozen exe.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from PyQt6.QtCore import QMetaObject, QObject, Qt, Q_ARG, pyqtSignal

logger = logging.getLogger(__name__)

# Payload type IDs for message routing (constants — no imports needed)
_PT_APP_AUTH_RES = 2101
_PT_ACCOUNT_AUTH_RES = 2103
_PT_EXECUTION_EVENT = 2126
_PT_SYMBOLS_LIST_RES = 2115
_PT_ERROR_RES = 2142
_PT_RECONCILE_RES = 2125

# Reactor state
_reactor_started = False
_reactor_lock = threading.Lock()


def _ensure_reactor() -> bool:
    """Start the Twisted reactor in a daemon thread (once). Returns True on success."""
    global _reactor_started
    with _reactor_lock:
        if _reactor_started:
            return True
        try:
            from twisted.internet import reactor

            t = threading.Thread(
                target=reactor.run,
                kwargs={"installSignalHandlers": False},
                daemon=True,
            )
            t.start()
            _reactor_started = True
            logger.info("Twisted reactor started in daemon thread")
            return True
        except Exception as exc:
            logger.error("Failed to start Twisted reactor: %s", exc)
            return False


def _load_ctrader_sdk() -> bool:
    """Try to import ctrader_open_api and populate protobuf registry. Returns True on success."""
    try:
        from ctrader_open_api import Protobuf

        Protobuf.populate()
        return True
    except Exception as exc:
        logger.error("Failed to load cTrader SDK: %s", exc)
        return False


class CTraderBridge(QObject):
    """Qt-thread bridge for cTrader Open API.

    All public methods are safe to call from the Qt main thread.
    Signals are emitted on the Qt main thread.
    If ctrader_open_api is unavailable, all methods are safe no-ops.
    """

    # Signals
    connected = pyqtSignal(bool, str)  # (is_connected, message)
    order_opened = pyqtSignal(str, int, str)  # (pair, position_id, direction)
    order_closed = pyqtSignal(str, int)  # (pair, position_id)
    order_error = pyqtSignal(str, str)  # (pair, error_message)
    positions_synced = pyqtSignal(list)  # list of position dicts

    def __init__(self, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._client: Any = None
        self._config: dict[str, Any] = {}
        self._account_id: int = 0
        self._symbol_map: dict[str, int] = {}  # "EURUSD" -> symbol_id
        self._position_symbols: dict[int, str] = {}  # position_id -> pair
        self._is_connected = False
        self._reconnect_delay = 5.0
        self._max_reconnect_delay = 60.0
        self._stopping = False
        self._sdk_loaded = False

    # ── Public API (called from Qt thread) ────────────────────────

    def start(self, config: dict[str, Any]) -> None:
        """Connect to cTrader with the given config.

        Starts the Twisted reactor on first call (lazy).
        """
        self._config = config
        self._account_id = int(config.get("ctrader_account_id", 0))
        self._stopping = False
        self._reconnect_delay = 5.0

        # Lazy init: start reactor + load SDK only when actually needed
        if not self._sdk_loaded:
            self._sdk_loaded = _load_ctrader_sdk()
            if not self._sdk_loaded:
                logger.error("cTrader SDK unavailable — cannot start")
                return

        if not _ensure_reactor():
            logger.error("Twisted reactor unavailable — cannot start")
            return

        try:
            from twisted.internet import reactor

            reactor.callFromThread(self._connect)
        except Exception as exc:
            logger.error("Failed to schedule cTrader connect: %s", exc)

    def stop(self) -> None:
        """Disconnect and stop reconnection."""
        self._stopping = True
        if self._client:
            try:
                from twisted.internet import reactor

                reactor.callFromThread(self._disconnect)
            except Exception:
                pass

    def open_order(self, pair: str, direction: str, volume_lots: float) -> None:
        """Send market order. volume_lots is in standard lots (e.g. 0.01)."""
        if not self._is_connected:
            return

        symbol_id = self._symbol_map.get(pair.upper())
        if symbol_id is None:
            self._emit_error(pair, f"Symbol {pair} not found in cTrader")
            return

        try:
            from twisted.internet import reactor
            from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
                ProtoOATradeSide,
            )

            # cTrader volume is in cents: 1 lot = 100_000 units = 10_000_000 cents
            volume_cents = int(round(volume_lots * 100_000 * 100))
            trade_side = (
                ProtoOATradeSide.Value("BUY")
                if direction == "BUY"
                else ProtoOATradeSide.Value("SELL")
            )
            reactor.callFromThread(
                self._send_market_order, pair, symbol_id, trade_side, volume_cents
            )
        except Exception as exc:
            logger.error("Failed to send order: %s", exc)

    def close_position(self, position_id: int, volume_lots: float) -> None:
        """Close (or partially close) a position."""
        if not self._is_connected:
            return
        try:
            from twisted.internet import reactor

            volume_cents = int(round(volume_lots * 100_000 * 100))
            reactor.callFromThread(self._send_close_position, position_id, volume_cents)
        except Exception as exc:
            logger.error("Failed to close position: %s", exc)

    def reconcile(self) -> None:
        """Request position reconciliation from broker."""
        if not self._is_connected:
            return
        try:
            from twisted.internet import reactor

            reactor.callFromThread(self._send_reconcile)
        except Exception as exc:
            logger.error("Failed to reconcile: %s", exc)

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    # ── Twisted-thread methods ────────────────────────────────────

    def _connect(self) -> None:
        """Create Client and connect (runs in Twisted thread)."""
        try:
            from ctrader_open_api import Client, EndPoints, TcpProtocol

            host = self._config.get("ctrader_host", EndPoints.PROTOBUF_DEMO_HOST)
            port = int(self._config.get("ctrader_port", EndPoints.PROTOBUF_PORT))

            self._client = Client(host, port, TcpProtocol)
            self._client.setConnectedCallback(self._on_connected)
            self._client.setDisconnectedCallback(self._on_disconnected)
            self._client.setMessageReceivedCallback(self._on_message)
            self._client.startService()
            logger.info("cTrader client connecting to %s:%d", host, port)
        except Exception as exc:
            logger.error("cTrader connect failed: %s", exc)
            self._emit_error("", f"Connect failed: {exc}")

    def _disconnect(self) -> None:
        """Stop client (runs in Twisted thread)."""
        try:
            if self._client:
                self._client.stopService()
                self._client = None
        except Exception:
            pass
        self._is_connected = False

    def _on_connected(self, client: Any) -> None:
        """Connected callback — start auth flow."""
        logger.info("cTrader TCP connected, sending app auth")
        client_id = self._config.get("ctrader_client_id", "")
        client_secret = self._config.get("ctrader_client_secret", "")
        d = client.send(
            "ProtoOAApplicationAuthReq",
            clientId=client_id,
            clientSecret=client_secret,
        )
        d.addErrback(self._on_error, "App auth failed")

    def _on_disconnected(self, client: Any, reason: Any) -> None:
        """Disconnected callback — attempt reconnect."""
        self._is_connected = False
        msg = f"Disconnected: {reason}"
        logger.warning("cTrader %s", msg)
        self._emit_connected(False, msg)

        if not self._stopping:
            try:
                from twisted.internet import reactor

                logger.info(
                    "cTrader reconnecting in %.0fs", self._reconnect_delay
                )
                reactor.callLater(self._reconnect_delay, self._reconnect)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2, self._max_reconnect_delay
                )
            except Exception:
                pass

    def _reconnect(self) -> None:
        """Reconnect after delay (runs in Twisted thread)."""
        if self._stopping:
            return
        self._connect()

    def _on_message(self, client: Any, message: Any) -> None:
        """Handle incoming Protobuf messages (runs in Twisted thread)."""
        try:
            from ctrader_open_api import Protobuf

            payload_type = message.payloadType

            if payload_type == _PT_APP_AUTH_RES:
                logger.info("cTrader app auth OK, sending account auth")
                access_token = self._config.get("ctrader_access_token", "")
                d = client.send(
                    "ProtoOAAccountAuthReq",
                    ctidTraderAccountId=self._account_id,
                    accessToken=access_token,
                )
                d.addErrback(self._on_error, "Account auth failed")

            elif payload_type == _PT_ACCOUNT_AUTH_RES:
                logger.info("cTrader account auth OK, fetching symbols")
                self._reconnect_delay = 5.0  # reset backoff
                d = client.send(
                    "ProtoOASymbolsListReq",
                    ctidTraderAccountId=self._account_id,
                )
                d.addErrback(self._on_error, "Symbol list failed")

            elif payload_type == _PT_SYMBOLS_LIST_RES:
                extracted = Protobuf.extract(message)
                self._build_symbol_map(extracted)
                self._is_connected = True
                self._emit_connected(True, "Connected & authenticated")
                logger.info(
                    "cTrader ready — %d symbols mapped", len(self._symbol_map)
                )
                # Reconcile positions on connect
                self._send_reconcile()

            elif payload_type == _PT_EXECUTION_EVENT:
                extracted = Protobuf.extract(message)
                self._handle_execution_event(extracted)

            elif payload_type == _PT_ERROR_RES:
                extracted = Protobuf.extract(message)
                error_code = getattr(extracted, "errorCode", "?")
                description = getattr(extracted, "description", "Unknown error")
                logger.error("cTrader error: %s — %s", error_code, description)
                self._emit_error("", f"{error_code}: {description}")

            elif payload_type == _PT_RECONCILE_RES:
                extracted = Protobuf.extract(message)
                self._handle_reconcile(extracted)

        except Exception as exc:
            logger.error("cTrader message handling error: %s", exc)

    def _on_error(self, failure: Any, context: str = "") -> None:
        """Deferred errback handler."""
        logger.error("cTrader %s: %s", context, failure)
        self._emit_error("", f"{context}: {failure}")

    # ── Symbol mapping ────────────────────────────────────────────

    def _build_symbol_map(self, symbols_res: Any) -> None:
        """Build symbol name → ID mapping from ProtoOASymbolsListRes."""
        self._symbol_map.clear()
        for symbol in symbols_res.symbol:
            # symbolName may be like "EUR/USD" or "EURUSD"
            name = getattr(symbol, "symbolName", "")
            normalized = name.replace("/", "").replace(" ", "").upper()
            symbol_id = symbol.symbolId
            if normalized:
                self._symbol_map[normalized] = symbol_id

    # ── Order sending ─────────────────────────────────────────────

    def _send_market_order(
        self, pair: str, symbol_id: int, trade_side: int, volume: int
    ) -> None:
        """Send market order (runs in Twisted thread)."""
        if not self._client or not self._is_connected:
            self._emit_error(pair, "Not connected")
            return
        try:
            from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
                ProtoOAOrderType,
            )

            logger.info(
                "cTrader sending MARKET %s %s vol=%d",
                "BUY" if trade_side == 1 else "SELL",
                pair,
                volume,
            )
            d = self._client.send(
                "ProtoOANewOrderReq",
                ctidTraderAccountId=self._account_id,
                symbolId=symbol_id,
                orderType=ProtoOAOrderType.Value("MARKET"),
                tradeSide=trade_side,
                volume=volume,
            )
            d.addErrback(self._on_error, f"Order {pair} failed")
        except Exception as exc:
            logger.error("Failed to send market order: %s", exc)

    def _send_close_position(self, position_id: int, volume: int) -> None:
        """Send close position request (runs in Twisted thread)."""
        if not self._client or not self._is_connected:
            return
        logger.info("cTrader closing position %d vol=%d", position_id, volume)
        d = self._client.send(
            "ProtoOAClosePositionReq",
            ctidTraderAccountId=self._account_id,
            positionId=position_id,
            volume=volume,
        )
        d.addErrback(self._on_error, f"Close position {position_id} failed")

    def _send_reconcile(self) -> None:
        """Send reconcile request (runs in Twisted thread)."""
        if not self._client or not self._is_connected:
            return
        d = self._client.send(
            "ProtoOAReconcileReq",
            ctidTraderAccountId=self._account_id,
        )
        d.addErrback(self._on_error, "Reconcile failed")

    # ── Execution event handling ──────────────────────────────────

    def _handle_execution_event(self, event: Any) -> None:
        """Process execution events (fills, closures)."""
        try:
            from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
                ProtoOAExecutionType,
            )

            exec_type = event.executionType
            position = getattr(event, "position", None)
            order = getattr(event, "order", None)

            if exec_type == ProtoOAExecutionType.Value("ORDER_FILLED"):
                if position:
                    pos_id = position.positionId
                    symbol_id = position.tradeData.symbolId
                    trade_side = position.tradeData.tradeSide
                    price = position.price / 100_000.0 if position.price else 0.0

                    pair = self._symbol_id_to_name(symbol_id)
                    direction = "BUY" if trade_side == 1 else "SELL"
                    self._position_symbols[pos_id] = pair

                    logger.info(
                        "cTrader ORDER_FILLED: %s %s pos=%d @ %.5f",
                        direction, pair, pos_id, price,
                    )
                    self._emit_order_opened(pair, pos_id, direction)

            elif exec_type == ProtoOAExecutionType.Value("ORDER_CANCELLED"):
                if order and position:
                    pos_id = position.positionId
                    pair = self._position_symbols.pop(pos_id, "UNKNOWN")
                    logger.info("cTrader position closed: %s pos=%d", pair, pos_id)
                    self._emit_order_closed(pair, pos_id)

            elif exec_type == ProtoOAExecutionType.Value("ORDER_REJECTED"):
                error_msg = getattr(event, "errorCode", "Rejected")
                pair = ""
                if order:
                    td = getattr(order, "tradeData", None)
                    if td:
                        pair = self._symbol_id_to_name(getattr(td, "symbolId", 0))
                logger.warning("cTrader ORDER_REJECTED: %s %s", pair, error_msg)
                self._emit_error(pair, f"Order rejected: {error_msg}")

        except Exception as exc:
            logger.error("Execution event handling error: %s", exc)

    def _handle_reconcile(self, reconcile_res: Any) -> None:
        """Process reconcile response — sync open positions."""
        positions = []
        for pos in reconcile_res.position:
            symbol_id = pos.tradeData.symbolId
            pair = self._symbol_id_to_name(symbol_id)
            trade_side = pos.tradeData.tradeSide
            direction = "BUY" if trade_side == 1 else "SELL"
            volume_cents = pos.tradeData.volume
            volume_lots = volume_cents / (100_000 * 100)
            price = pos.price / 100_000.0 if pos.price else 0.0
            pos_id = pos.positionId

            self._position_symbols[pos_id] = pair
            positions.append({
                "pair": pair, "direction": direction,
                "position_id": pos_id, "volume": volume_lots, "price": price,
            })

        logger.info("cTrader reconcile: %d open positions", len(positions))
        QMetaObject.invokeMethod(
            self, "_emit_positions_synced_slot",
            Qt.ConnectionType.QueuedConnection, Q_ARG(list, positions),
        )

    def _symbol_id_to_name(self, symbol_id: int) -> str:
        """Reverse lookup: symbol_id → pair name."""
        for name, sid in self._symbol_map.items():
            if sid == symbol_id:
                return name
        return f"ID:{symbol_id}"

    # ── Thread-safe signal emission ───────────────────────────────

    def _emit_connected(self, is_connected: bool, msg: str) -> None:
        QMetaObject.invokeMethod(
            self, "_emit_connected_slot",
            Qt.ConnectionType.QueuedConnection,
            Q_ARG(bool, is_connected), Q_ARG(str, msg),
        )

    def _emit_order_opened(self, pair: str, pos_id: int, direction: str) -> None:
        QMetaObject.invokeMethod(
            self, "_emit_order_opened_slot",
            Qt.ConnectionType.QueuedConnection,
            Q_ARG(str, pair), Q_ARG(int, pos_id), Q_ARG(str, direction),
        )

    def _emit_order_closed(self, pair: str, pos_id: int) -> None:
        QMetaObject.invokeMethod(
            self, "_emit_order_closed_slot",
            Qt.ConnectionType.QueuedConnection,
            Q_ARG(str, pair), Q_ARG(int, pos_id),
        )

    def _emit_error(self, pair: str, msg: str) -> None:
        QMetaObject.invokeMethod(
            self, "_emit_error_slot",
            Qt.ConnectionType.QueuedConnection,
            Q_ARG(str, pair), Q_ARG(str, msg),
        )

    # ── Slots for QMetaObject.invokeMethod ────────────────────────

    def _emit_connected_slot(self, is_connected: bool, msg: str) -> None:
        self.connected.emit(is_connected, msg)

    def _emit_order_opened_slot(self, pair: str, pos_id: int, direction: str) -> None:
        self.order_opened.emit(pair, pos_id, direction)

    def _emit_order_closed_slot(self, pair: str, pos_id: int) -> None:
        self.order_closed.emit(pair, pos_id)

    def _emit_error_slot(self, pair: str, msg: str) -> None:
        self.order_error.emit(pair, msg)

    def _emit_positions_synced_slot(self, positions: list) -> None:
        self.positions_synced.emit(positions)
