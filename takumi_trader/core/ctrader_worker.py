"""cTrader Open API connection bridge — Twisted in thread + Qt signals.

Uses the CANONICAL Spotware pattern from OpenApiPy/samples/ConsoleSample:
- Single Client instance for the entire session (never destroyed)
- ClientService (Twisted) handles TCP reconnection automatically
- Default retryPolicy (Twisted exponential backoff, infinite retries)
- SDK's TcpProtocol sends heartbeats automatically every 20s
- _on_connected fires on initial connect AND every auto-reconnect → re-auth
- _on_disconnected just logs — NO manual reconnection code
- ProtoOAAccountAuthReq is sent automatically after receiving
  ProtoOAApplicationAuthRes if currentAccountId is set (survives reconnects)

Do NOT add:
- Custom retry policies
- Manual heartbeat loops
- Watchdog force-reconnects
- Destroy/recreate Client cycles
These all fight against ClientService's built-in reconnection.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from PyQt6.QtCore import QObject, pyqtSignal

logger = logging.getLogger(__name__)

# Payload type IDs for message routing (constants — no imports needed)
_PT_APP_AUTH_RES = 2101
_PT_ACCOUNT_AUTH_RES = 2103
_PT_EXECUTION_EVENT = 2126
_PT_SYMBOLS_LIST_RES = 2115
_PT_ERROR_RES = 2142
_PT_RECONCILE_RES = 2125
_PT_HEARTBEAT = 51

# Reactor state
_reactor_started = False
_reactor_lock = threading.Lock()


def _ensure_reactor() -> bool:
    """Start the Twisted reactor in a daemon thread (once)."""
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
    """Try to import ctrader_open_api and populate protobuf registry."""
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
    Signals are emitted on the Qt main thread (Qt signals are thread-safe).
    """

    # Signals
    connected = pyqtSignal(bool, str)
    order_opened = pyqtSignal(str, int, str)
    order_closed = pyqtSignal(str, int)
    order_error = pyqtSignal(str, str)
    positions_synced = pyqtSignal(list)
    balance_updated = pyqtSignal(float)

    def __init__(self, parent: QObject | None = None) -> None:
        super().__init__(parent)
        self._client: Any = None
        self._config: dict[str, Any] = {}
        self._account_id: int = 0
        self._symbol_map: dict[str, int] = {}
        self._position_symbols: dict[int, str] = {}
        self._is_connected = False
        self._is_authenticated = False  # account-level auth
        self._stopping = False
        self._sdk_loaded = False
        self._error_count = 0
        self._backoff_call: Any = None  # pending callLater handle
        # Rate limit escalation: count consecutive BLOCKED_PAYLOAD_TYPE hits
        # to back off longer each time (5min → 15min → 30min → 60min → give up)
        self._rate_limit_hits = 0
        self._last_rate_limit_ts = 0.0

    # ── Public API (called from Qt thread) ────────────────────────

    def start(self, config: dict[str, Any]) -> None:
        """Connect to cTrader with the given config."""
        self._config = config
        try:
            self._account_id = int(str(config.get("ctrader_account_id", 0) or 0))
        except (ValueError, TypeError):
            logger.error("cTrader invalid account_id: %r", config.get("ctrader_account_id"))
            return
        if self._account_id == 0:
            logger.error("cTrader account_id is 0 — not starting")
            return
        self._stopping = False

        if not self._sdk_loaded:
            self._sdk_loaded = _load_ctrader_sdk()
            if not self._sdk_loaded:
                logger.error("cTrader SDK unavailable — cannot start")
                return

        if not _ensure_reactor():
            return

        try:
            from twisted.internet import reactor
            reactor.callFromThread(self._connect)
        except Exception as exc:
            logger.error("Failed to schedule cTrader connect: %s", exc)

    def stop(self) -> None:
        """Disconnect and stop reconnection permanently (app shutdown)."""
        self._stopping = True
        try:
            from twisted.internet import reactor
            reactor.callFromThread(self._shutdown)
        except Exception:
            pass

    def open_order(
        self, pair: str, direction: str, volume_lots: float,
        sl_price: float = 0.0, tp_price: float = 0.0,
        sl_pips: float = 0.0, tp_pips: float = 0.0,
    ) -> None:
        """Send market order with optional SL/TP."""
        if not self._is_connected:
            self._emit_error(pair, "Not connected")
            return
        symbol_id = self._symbol_map.get(pair.upper())
        if symbol_id is None:
            self._emit_error(pair, f"Symbol {pair} not found")
            return

        # Single-retry on the specific Python 3.13+/protobuf transient
        # init error (2026-05-07 fix). If the first attempt raises with
        # the matching pattern, wait 100ms and try once more. If the
        # retry also fails (or it's a different error), emit to the
        # operator with full traceback.
        last_exc: Exception | None = None
        for attempt in (1, 2):
            try:
                from twisted.internet import reactor
                from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOATradeSide

                volume_cents = int(round(volume_lots * 100_000 * 100))
                trade_side = ProtoOATradeSide.Value("BUY" if direction == "BUY" else "SELL")

                # relativeStopLoss units: 1/100,000 of price
                # Non-JPY: pips × 10 (pip=0.0001)
                # JPY:     pips × 1000 (pip=0.01)
                if "JPY" in pair:
                    sl_pipettes = int(round(sl_pips * 1000)) if sl_pips > 0 else 0
                    tp_pipettes = int(round(tp_pips * 1000)) if tp_pips > 0 else 0
                else:
                    sl_pipettes = int(round(sl_pips * 10)) if sl_pips > 0 else 0
                    tp_pipettes = int(round(tp_pips * 10)) if tp_pips > 0 else 0

                reactor.callFromThread(
                    self._send_market_order, pair, symbol_id, trade_side, volume_cents,
                    sl_pipettes, tp_pipettes,
                )
                if attempt == 2:
                    # Retry recovered — log as warning so the operator
                    # sees the transient happened, but no popup alert.
                    logger.warning(
                        "[cTrader] Order send for %s recovered on retry "
                        "after transient init error: %s",
                        pair, last_exc,
                    )
                return  # success
            except Exception as exc:
                last_exc = exc
                if attempt == 1 and self._is_transient_init_error(exc):
                    logger.info(
                        "[cTrader] Order send for %s hit transient init "
                        "error; retrying after 100ms",
                        pair,
                    )
                    time.sleep(0.1)
                    continue
                # Non-transient error OR retry already exhausted —
                # emit to operator with full traceback for diagnosis.
                logger.error(
                    "Failed to send order (attempt %d/2): %s",
                    attempt, exc, exc_info=True,
                )
                # Emit error so main_window clears _ct_open_positions and
                # doesn't leave the pair silently blocked.
                self._emit_error(pair, f"Order send failed: {exc}")
                return

    def close_position(self, position_id: int, volume_lots: float) -> None:
        if not self._is_connected:
            return
        try:
            from twisted.internet import reactor
            volume_cents = int(round(volume_lots * 100_000 * 100))
            reactor.callFromThread(self._send_close_position, position_id, volume_cents)
        except Exception as exc:
            logger.error("Failed to close position: %s", exc)

    def reconcile(self) -> None:
        if not self._is_connected:
            return
        try:
            from twisted.internet import reactor
            reactor.callFromThread(self._send_reconcile)
        except Exception as exc:
            logger.error("Failed to reconcile: %s", exc)

    def query_balance(self) -> None:
        if not self._is_connected:
            return
        try:
            from twisted.internet import reactor
            reactor.callFromThread(self._query_balance)
        except Exception:
            pass

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    # ── Twisted-thread methods ────────────────────────────────────

    def _connect(self) -> None:
        """Create Client and start service (runs in Twisted thread).

        Canonical Spotware pattern: create Client once, set callbacks,
        startService(). ClientService handles reconnection with the default
        retry policy (Twisted exponential backoff, infinite retries).
        """
        if self._stopping:
            return  # don't reconnect after stop() was called
        self._backoff_call = None  # clear any pending backoff reference
        if self._client:
            # Already have a client — ensure it's running
            if not self._client.running:
                self._client.startService()
            return
        try:
            from ctrader_open_api import Client, EndPoints, TcpProtocol

            # Host selection: ctrader_live config flag chooses between demo/live.
            # Explicit override via ctrader_host still wins if provided.
            is_live = bool(self._config.get("ctrader_live", False))
            default_host = (
                EndPoints.PROTOBUF_LIVE_HOST if is_live
                else EndPoints.PROTOBUF_DEMO_HOST
            )
            host = self._config.get("ctrader_host") or default_host
            port = int(self._config.get("ctrader_port") or EndPoints.PROTOBUF_PORT)

            # Use default retryPolicy (Twisted's built-in exponential backoff)
            self._client = Client(host, port, TcpProtocol)
            self._client.setConnectedCallback(self._on_connected)
            self._client.setDisconnectedCallback(self._on_disconnected)
            self._client.setMessageReceivedCallback(self._on_message)
            self._client.startService()
            logger.info(
                "cTrader connecting to %s:%d (%s)",
                host, port, "LIVE" if is_live else "DEMO",
            )
        except Exception as exc:
            logger.error("cTrader connect failed: %s", exc)
            self._emit_error("", f"Connect failed: {exc}")

    def _shutdown(self) -> None:
        """Full shutdown: stop client (runs in Twisted thread)."""
        # Cancel any pending backoff reconnect
        if self._backoff_call is not None:
            try:
                if self._backoff_call.active():
                    self._backoff_call.cancel()
            except Exception:
                pass
            self._backoff_call = None
        if self._client:
            try:
                self._client.stopService()
            except Exception:
                pass
            self._client = None
        self._is_connected = False
        self._is_authenticated = False
        # Clear stale symbol data
        self._symbol_map.clear()
        self._position_symbols.clear()

    def _backoff_reconnect(self, delay_seconds: int = 300) -> None:
        """Stop the client service, wait, then restart fresh.

        Used when the server rate-limits us — ClientService would just
        reconnect immediately otherwise, triggering the rate limit again.
        Runs in Twisted thread.
        """
        if self._stopping:
            return
        logger.warning("cTrader backoff reconnect scheduled in %ds", delay_seconds)
        self._is_connected = False
        self._is_authenticated = False
        # Clear stale state from the dead connection
        self._symbol_map.clear()
        self._position_symbols.clear()
        # Stop the current service (cancels ClientService's reconnection)
        if self._client:
            try:
                self._client.stopService()
            except Exception:
                pass
            self._client = None
        # Cancel any existing backoff call (avoid duplicates)
        if self._backoff_call is not None:
            try:
                if self._backoff_call.active():
                    self._backoff_call.cancel()
            except Exception:
                pass
        # Schedule a clean restart after the delay
        try:
            from twisted.internet import reactor
            self._backoff_call = reactor.callLater(delay_seconds, self._connect)
        except Exception as exc:
            logger.error("Failed to schedule backoff reconnect: %s", exc)

    def _on_connected(self, client: Any) -> None:
        """Connected callback — send app auth.

        Called by ClientService on initial connect AND every auto-reconnect.
        We always re-send ProtoOAApplicationAuthReq.
        """
        logger.info("cTrader TCP connected, sending app auth")
        self._is_authenticated = False
        self._error_count = 0

        try:
            import datetime as _dt
            with open("D:/Trading/TAKUMI Trader/data/ctrader_connect.log", "a") as f:
                f.write(f"{_dt.datetime.now():%Y-%m-%d %H:%M:%S} CONNECTED\n")
        except Exception:
            pass

        client_id = self._config.get("ctrader_client_id", "")
        client_secret = self._config.get("ctrader_client_secret", "")
        if not client_id or not client_secret:
            logger.error("cTrader missing client_id/client_secret")
            return

        try:
            d = client.send(
                "ProtoOAApplicationAuthReq",
                clientId=client_id,
                clientSecret=client_secret,
            )
            d.addErrback(self._on_error, "App auth")
        except Exception as exc:
            logger.error("cTrader app auth send failed: %s", exc)

    def _on_disconnected(self, client: Any, reason: Any) -> None:
        """Disconnected callback — just log. ClientService auto-reconnects."""
        self._is_connected = False
        self._is_authenticated = False
        logger.warning("cTrader disconnected: %s", reason)
        self.connected.emit(False, f"Disconnected: {reason}")

        try:
            import datetime as _dt
            with open("D:/Trading/TAKUMI Trader/data/ctrader_connect.log", "a") as f:
                f.write(f"{_dt.datetime.now():%Y-%m-%d %H:%M:%S} DISCONNECTED: {reason}\n")
        except Exception:
            pass

    # ── Message handling ─────────────────────────────────────────

    def _on_message(self, client: Any, message: Any) -> None:
        """Handle incoming Protobuf messages (runs in Twisted thread)."""
        try:
            from ctrader_open_api import Protobuf

            pt = message.payloadType

            # Skip heartbeat noise
            if pt == _PT_HEARTBEAT:
                return

            if pt == _PT_APP_AUTH_RES:
                logger.info("cTrader app auth OK")
                # Automatically re-send account auth (canonical pattern)
                if self._account_id:
                    self._send_account_auth(client)

            elif pt == _PT_ACCOUNT_AUTH_RES:
                logger.info("cTrader account auth OK, fetching symbols")
                self._is_authenticated = True
                self._error_count = 0
                d = client.send(
                    "ProtoOASymbolsListReq",
                    ctidTraderAccountId=self._account_id,
                )
                d.addErrback(self._on_error, "Symbol list")

            elif pt == _PT_SYMBOLS_LIST_RES:
                extracted = Protobuf.extract(message)
                self._build_symbol_map(extracted)
                self._is_connected = True
                self._rate_limit_hits = 0  # reset escalation counter on success
                self.connected.emit(True, "Connected & authenticated")
                logger.info("cTrader ready — %d symbols mapped", len(self._symbol_map))
                self._send_reconcile()
                self._query_balance()

            elif pt == 2148:  # ProtoOATraderRes
                extracted = Protobuf.extract(message)
                balance = getattr(extracted, "balance", 0) / 100.0
                if balance > 0:
                    logger.info("cTrader balance: %.2f", balance)
                    self.balance_updated.emit(balance)

            elif pt == _PT_EXECUTION_EVENT:
                extracted = Protobuf.extract(message)
                self._handle_execution_event(extracted)

            elif pt == _PT_ERROR_RES:
                extracted = Protobuf.extract(message)
                code = getattr(extracted, "errorCode", "?")
                desc = getattr(extracted, "description", "Unknown")
                code_s = str(code)
                desc_s = str(desc)
                # Log all error responses to connect log for debugging
                try:
                    import datetime as _dt
                    with open("D:/Trading/TAKUMI Trader/data/ctrader_connect.log", "a") as f:
                        f.write(f"{_dt.datetime.now():%Y-%m-%d %H:%M:%S} ERROR_RES: {code_s} — {desc_s}\n")
                except Exception:
                    pass

                if "ALREADY_LOGGED_IN" in code_s:
                    logger.info("cTrader already logged in, sending account auth")
                    self._send_account_auth(client)
                elif "BLOCKED_PAYLOAD_TYPE" in code_s or "rate limit" in desc_s.lower():
                    # Server is rate-limiting us. Escalate backoff on consecutive
                    # hits since the server's block window is longer than 5 min.
                    now = time.monotonic()
                    if now - self._last_rate_limit_ts < 3600:
                        # Within an hour of the last hit — consecutive
                        self._rate_limit_hits += 1
                    else:
                        self._rate_limit_hits = 1
                    self._last_rate_limit_ts = now

                    # Escalating delay: 15min, 30min, 60min, 120min, then stop
                    delays = [900, 1800, 3600, 7200]
                    if self._rate_limit_hits > len(delays):
                        logger.error(
                            "cTrader RATE LIMITED %d times — stopping reconnection. "
                            "Restart TAKUMI manually after waiting 2+ hours.",
                            self._rate_limit_hits,
                        )
                        self._emit_error(
                            "",
                            f"Rate limit persistent ({self._rate_limit_hits} hits) — "
                            "stopped auto-reconnect. Restart app after 2+ hours.",
                        )
                        # Permanent stop until manual restart
                        self._stopping = True
                        self._shutdown()
                    else:
                        delay = delays[self._rate_limit_hits - 1]
                        logger.error(
                            "cTrader RATE LIMITED (hit #%d): %s. Backing off %d min.",
                            self._rate_limit_hits, desc_s, delay // 60,
                        )
                        self._emit_error(
                            "",
                            f"Rate limited (hit #{self._rate_limit_hits}) — "
                            f"waiting {delay // 60} min",
                        )
                        self._backoff_reconnect(delay_seconds=delay)
                elif "NOT_AUTHORIZED" in code_s or "CH_ACCESS_TOKEN" in desc_s:
                    logger.error("cTrader ACCESS TOKEN EXPIRED: %s — %s", code_s, desc_s)
                    self._emit_error("", f"Access token expired: {desc_s}")
                else:
                    logger.error("cTrader error: %s — %s", code_s, desc_s)
                    self._emit_error("", f"{code_s}: {desc_s}")

            elif pt == _PT_RECONCILE_RES:
                extracted = Protobuf.extract(message)
                self._handle_reconcile(extracted)

        except Exception as exc:
            logger.error("cTrader message handling error: %s", exc)

    def _send_account_auth(self, client: Any) -> None:
        """Send account auth (runs in Twisted thread)."""
        access_token = self._config.get("ctrader_access_token", "")
        try:
            d = client.send(
                "ProtoOAAccountAuthReq",
                ctidTraderAccountId=self._account_id,
                accessToken=access_token,
            )
            d.addErrback(self._on_error, "Account auth")
        except Exception as exc:
            logger.error("cTrader account auth send failed: %s", exc)

    def _on_error(self, failure: Any, context: str = "") -> None:
        """Deferred errback — log with throttling for connection noise."""
        # Filter out "cancelled" errors caused by disconnects (expected)
        err_str = str(failure)
        if "Deferred" in err_str and "Cancelled" in err_str:
            return
        # Order errors are critical — always log (not throttled)
        if context.startswith("Order "):
            logger.warning("cTrader %s: %s", context, err_str[:200])
            return
        # Connection-level errors get throttled to prevent log spam
        if not hasattr(self, "_error_count"):
            self._error_count = 0
        self._error_count += 1
        if self._error_count <= 3:
            logger.warning("cTrader %s: %s", context, err_str[:200])

    # ── Symbol mapping ────────────────────────────────────────────

    def _build_symbol_map(self, symbols_res: Any) -> None:
        # Build new dict locally then assign atomically — prevents the Qt
        # thread from observing a half-rebuilt map during concurrent order sends
        new_map: dict[str, int] = {}
        for symbol in symbols_res.symbol:
            name = getattr(symbol, "symbolName", "")
            normalized = name.replace("/", "").replace(" ", "").upper()
            if normalized:
                new_map[normalized] = symbol.symbolId
        self._symbol_map = new_map

    # ── Order sending ─────────────────────────────────────────────

    @staticmethod
    def _is_transient_init_error(exc: BaseException) -> bool:
        """Match the specific Python 3.13+/protobuf intermittent error.

        Wording: '__init__() should return None, not 'NoneType''.
        Surfaced 2026-05-07 — sometimes fires on cTrader order send
        during the synchronous setup region (protobuf enum lookup, etc.).
        Intermittent rather than systematic, so a single retry after a
        brief delay is appropriate. If retry also fails, we emit the
        original error to the operator.

        Pattern-matches by substring rather than exact-equal so minor
        version differences in Python's error wording don't break the
        match.
        """
        msg = str(exc)
        return (
            "__init__() should return None" in msg
            and "NoneType" in msg
        )

    def _send_market_order(
        self, pair: str, symbol_id: int, trade_side: int, volume: int,
        sl_pipettes: int = 0, tp_pipettes: int = 0,
    ) -> None:
        if not self._client or not self._is_connected:
            self._emit_error(pair, "Not connected")
            return
        try:
            from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOAOrderType

            params = dict(
                ctidTraderAccountId=self._account_id,
                symbolId=symbol_id,
                orderType=ProtoOAOrderType.Value("MARKET"),
                tradeSide=trade_side,
                volume=volume,
            )
            if sl_pipettes > 0:
                params["relativeStopLoss"] = sl_pipettes
            if tp_pipettes > 0:
                params["relativeTakeProfit"] = tp_pipettes

            logger.info(
                "cTrader MARKET %s %s vol=%d SL=%d TP=%d",
                "BUY" if trade_side == 1 else "SELL",
                pair, volume, sl_pipettes, tp_pipettes,
            )

            try:
                with open("D:/Trading/TAKUMI Trader/data/ctrader_orders.log", "a") as _f:
                    _f.write(f"SENDING: {pair} side={trade_side} vol={volume} "
                             f"sl_pip={sl_pipettes} tp_pip={tp_pipettes}\n")
            except Exception:
                pass

            d = self._client.send("ProtoOANewOrderReq", **params)

            def _order_cb(result, p=pair):
                try:
                    from ctrader_open_api import Protobuf
                    with open("D:/Trading/TAKUMI Trader/data/ctrader_orders.log", "a") as _f:
                        if result.payloadType == 2132:
                            ex = Protobuf.extract(result)
                            code = getattr(ex, "errorCode", "?")
                            desc = getattr(ex, "description", "?")
                            _f.write(f"REJECTED: {p} {code}: {desc}\n")
                            self._emit_error(p, f"Order rejected: {code}: {desc}")
                        else:
                            _f.write(f"FILLED: {p} type={result.payloadType}\n")
                except Exception:
                    pass

            d.addCallback(_order_cb)
            d.addErrback(self._on_error, f"Order {pair}")
        except Exception as exc:
            logger.error("Failed to send market order: %s", exc)

    def _send_close_position(self, position_id: int, volume: int) -> None:
        if not self._client or not self._is_connected:
            return
        logger.info("cTrader closing pos=%d vol=%d", position_id, volume)
        d = self._client.send(
            "ProtoOAClosePositionReq",
            ctidTraderAccountId=self._account_id,
            positionId=position_id,
            volume=volume,
        )
        d.addErrback(self._on_error, f"Close pos={position_id}")

    def _query_balance(self) -> None:
        if not self._client or not self._is_connected:
            return
        try:
            d = self._client.send(
                "ProtoOATraderReq",
                ctidTraderAccountId=self._account_id,
            )
            d.addErrback(self._on_error, "Balance query")
        except Exception as exc:
            logger.warning("Balance query error: %s", exc)

    def _send_reconcile(self) -> None:
        if not self._client or not self._is_connected:
            return
        d = self._client.send(
            "ProtoOAReconcileReq",
            ctidTraderAccountId=self._account_id,
        )
        d.addErrback(self._on_error, "Reconcile")

    # ── Execution event handling ──────────────────────────────────

    def _handle_execution_event(self, event: Any) -> None:
        try:
            from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
                ProtoOAExecutionType,
                ProtoOAPositionStatus,
            )

            exec_type = event.executionType
            position = getattr(event, "position", None)
            order = getattr(event, "order", None)

            if exec_type == ProtoOAExecutionType.Value("ORDER_FILLED"):
                if not position:
                    return
                pos_id = position.positionId
                symbol_id = position.tradeData.symbolId
                trade_side = position.tradeData.tradeSide
                price = position.price / 100_000.0 if position.price else 0.0
                pair = self._symbol_id_to_name(symbol_id)
                direction = "BUY" if trade_side == 1 else "SELL"

                # Distinguish OPEN fill from CLOSE fill (SL/TP hit, manual close)
                pos_status = getattr(position, "positionStatus", 0)
                closed_status = ProtoOAPositionStatus.Value("POSITION_STATUS_CLOSED")
                closing_order = bool(getattr(order, "closingOrder", False)) if order else False

                if pos_status == closed_status or closing_order:
                    # Position closed (SL/TP hit, manual close, etc.)
                    self._position_symbols.pop(pos_id, None)
                    logger.info(
                        "cTrader POSITION CLOSED: %s pos=%d @ %.5f",
                        pair, pos_id, price,
                    )
                    self.order_closed.emit(pair, pos_id)
                else:
                    # New position opened
                    self._position_symbols[pos_id] = pair
                    logger.info(
                        "cTrader POSITION OPENED: %s %s pos=%d @ %.5f",
                        direction, pair, pos_id, price,
                    )
                    self.order_opened.emit(pair, pos_id, direction)

            elif exec_type == ProtoOAExecutionType.Value("ORDER_CANCELLED"):
                # Cancelled order (not a position close)
                if order:
                    logger.info("cTrader ORDER_CANCELLED: order=%s", getattr(order, "orderId", "?"))

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
        positions = []
        # Rebuild position_symbols from scratch to prune stale entries
        new_pos_symbols: dict[int, str] = {}
        for pos in reconcile_res.position:
            symbol_id = pos.tradeData.symbolId
            pair = self._symbol_id_to_name(symbol_id)
            direction = "BUY" if pos.tradeData.tradeSide == 1 else "SELL"
            volume_lots = pos.tradeData.volume / (100_000 * 100)
            price = pos.price / 100_000.0 if pos.price else 0.0
            pos_id = pos.positionId
            new_pos_symbols[pos_id] = pair
            positions.append({
                "pair": pair, "direction": direction,
                "position_id": pos_id, "volume": volume_lots, "price": price,
            })
        self._position_symbols = new_pos_symbols
        logger.info("cTrader reconcile: %d open positions", len(positions))
        self.positions_synced.emit(positions)

    def _symbol_id_to_name(self, symbol_id: int) -> str:
        for name, sid in self._symbol_map.items():
            if sid == symbol_id:
                return name
        return f"ID:{symbol_id}"

    def _emit_error(self, pair: str, msg: str) -> None:
        self.order_error.emit(pair, msg)
