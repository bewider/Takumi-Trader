"""MT5 Auto-Trader — order execution with risk-based position sizing.

Sends market orders to MT5 with broker-enforced SL/TP levels.
Position size is auto-calculated so each trade risks exactly X% of
account balance based on the SL distance.

Requires: MT5 terminal with "Allow algorithmic trading" enabled
(Tools → Options → Expert Advisors → ✓ Allow algorithmic trading).
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from PyQt6.QtCore import QObject, pyqtSignal

logger = logging.getLogger(__name__)

# Magic number to identify our orders (filter from manual trades)
DEFAULT_MAGIC = 202603

# Max slippage in points (not pips — 1 pip = 10 points for 5-digit pairs)
DEFAULT_DEVIATION = 30


@dataclass
class MT5Position:
    """Tracked MT5 position."""

    pair: str = ""
    direction: str = ""  # "BUY" or "SELL"
    ticket: int = 0
    volume: float = 0.0  # lots
    open_price: float = 0.0
    sl_price: float = 0.0
    tp_price: float = 0.0
    sl_pips: float = 0.0
    tp_pips: float = 0.0
    open_time: float = 0.0


class MT5PositionManager:
    """Tracks open MT5 positions with disk persistence."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._positions: dict[str, MT5Position] = {}  # pair -> position
        self.load()

    def has_position(self, pair: str) -> bool:
        return pair in self._positions

    def register_open(self, pos: MT5Position) -> None:
        self._positions[pos.pair] = pos
        self.save()
        logger.info(
            "[MT5] Position registered: %s %s ticket=%d vol=%.2f SL=%.5f TP=%.5f",
            pos.direction, pos.pair, pos.ticket, pos.volume, pos.sl_price, pos.tp_price,
        )

    def register_close(self, pair: str) -> MT5Position | None:
        pos = self._positions.pop(pair, None)
        if pos:
            self.save()
            logger.info("[MT5] Position closed: %s ticket=%d", pair, pos.ticket)
        return pos

    def register_close_by_ticket(self, ticket: int) -> MT5Position | None:
        for pair, pos in list(self._positions.items()):
            if pos.ticket == ticket:
                return self.register_close(pair)
        return None

    def get_position(self, pair: str) -> MT5Position | None:
        return self._positions.get(pair)

    @property
    def open_count(self) -> int:
        return len(self._positions)

    @property
    def all_positions(self) -> dict[str, MT5Position]:
        return dict(self._positions)

    def save(self) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            data = [asdict(p) for p in self._positions.values()]
            self._path.write_text(
                json.dumps(data, indent=2), encoding="utf-8"
            )
        except OSError as e:
            logger.warning("Failed to save MT5 positions: %s", e)

    def load(self) -> None:
        if not self._path.exists():
            return
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            for d in data:
                pos = MT5Position(**d)
                self._positions[pos.pair] = pos
            if self._positions:
                logger.info("Loaded %d MT5 positions from disk", len(self._positions))
        except (json.JSONDecodeError, OSError, TypeError) as e:
            logger.warning("Failed to load MT5 positions: %s", e)

    def sync_with_broker(self, magic: int = DEFAULT_MAGIC) -> None:
        """Reconcile with actual MT5 positions.

        Removes positions we track that no longer exist on the broker,
        adds any broker positions with our magic number that we don't track.
        """
        try:
            import MetaTrader5 as mt5

            broker_positions = mt5.positions_get()
            if broker_positions is None:
                return

            # Build set of broker tickets (our magic only)
            broker_tickets: dict[int, object] = {}
            for bp in broker_positions:
                if bp.magic == magic:
                    broker_tickets[bp.ticket] = bp

            # Remove positions we track that are gone from broker
            for pair in list(self._positions.keys()):
                pos = self._positions[pair]
                if pos.ticket not in broker_tickets:
                    logger.info(
                        "[MT5] Position %s ticket=%d no longer on broker — removing",
                        pair, pos.ticket,
                    )
                    del self._positions[pair]

            # Add broker positions we don't track
            for ticket, bp in broker_tickets.items():
                pair = bp.symbol
                if pair not in self._positions:
                    direction = "BUY" if bp.type == 0 else "SELL"
                    pos = MT5Position(
                        pair=pair,
                        direction=direction,
                        ticket=ticket,
                        volume=bp.volume,
                        open_price=bp.price_open,
                        sl_price=bp.sl,
                        tp_price=bp.tp,
                        open_time=bp.time,
                    )
                    self._positions[pair] = pos
                    logger.info(
                        "[MT5] Discovered broker position: %s %s ticket=%d",
                        direction, pair, ticket,
                    )

            self.save()
        except Exception as e:
            logger.warning("[MT5] Sync with broker failed: %s", e)


class MT5Trader(QObject):
    """MT5 order execution with risk-based position sizing.

    Emits signals on order events for UI integration.
    """

    order_opened = pyqtSignal(str, int, str, float)  # pair, ticket, direction, lots
    order_closed = pyqtSignal(str, int, float)        # pair, ticket, pnl
    order_error = pyqtSignal(str, str)                 # pair, error_msg

    def __init__(
        self,
        position_manager: MT5PositionManager,
        magic: int = DEFAULT_MAGIC,
        deviation: int = DEFAULT_DEVIATION,
    ) -> None:
        super().__init__()
        self._pos_mgr = position_manager
        self._magic = magic
        self._deviation = deviation

    # ── Position Sizing ──────────────────────────────────────────

    def calculate_lot_size(
        self,
        pair: str,
        sl_pips: float,
        risk_pct: float,
    ) -> float:
        """Calculate lot size so the trade risks exactly risk_pct of account balance.

        Uses mt5.order_calc_profit() for accurate pip value in account currency.
        Falls back to manual calculation if that fails.

        Returns lot size rounded to broker's lot step (usually 0.01).
        """
        import MetaTrader5 as mt5

        account = mt5.account_info()
        if account is None:
            logger.error("[MT5] Cannot get account info for lot sizing")
            return 0.0

        balance = account.balance
        risk_amount = balance * (risk_pct / 100.0)

        sym = mt5.symbol_info(pair)
        if sym is None:
            logger.error("[MT5] Symbol %s not found", pair)
            return 0.0

        # Calculate pip value for 1 lot using symbol info
        # pip = 10 * point for 5/3-digit pairs, 1 * point for 4/2-digit
        if sym.digits in (3, 5):
            pip_in_price = sym.point * 10
        else:
            pip_in_price = sym.point

        # Use order_calc_profit to get pip value in account currency
        # Simulate a 1-pip move on 1 lot
        tick = mt5.symbol_info_tick(pair)
        if tick is None:
            logger.error("[MT5] Cannot get tick for %s", pair)
            return 0.0

        test_price = tick.ask
        test_price_plus_pip = test_price + pip_in_price

        pip_value_1lot = mt5.order_calc_profit(
            mt5.ORDER_TYPE_BUY, pair, 1.0, test_price, test_price_plus_pip
        )

        if pip_value_1lot is None or pip_value_1lot <= 0:
            # Fallback: manual calculation
            # For JPY account with JPY pair: pip_value ≈ contract_size * pip_in_price
            pip_value_1lot = sym.trade_contract_size * pip_in_price
            if sym.currency_profit != account.currency:
                # Cross-currency conversion needed — use tick value as approximation
                pip_value_1lot = sym.trade_tick_value * (pip_in_price / sym.trade_tick_size)
            logger.warning(
                "[MT5] order_calc_profit failed for %s, using fallback pip_value=%.2f",
                pair, pip_value_1lot,
            )

        if pip_value_1lot <= 0 or sl_pips <= 0:
            logger.error(
                "[MT5] Invalid pip value (%.2f) or SL (%.1f) for %s",
                pip_value_1lot, sl_pips, pair,
            )
            return 0.0

        # lot_size = risk_amount / (sl_pips * pip_value_per_lot)
        raw_lots = risk_amount / (sl_pips * pip_value_1lot)

        # Round to broker's lot step
        lot_step = sym.volume_step
        lot_min = sym.volume_min
        lot_max = sym.volume_max

        lots = max(lot_min, round(raw_lots / lot_step) * lot_step)
        lots = min(lots, lot_max)
        lots = round(lots, 2)

        logger.info(
            "[MT5] Lot sizing %s: balance=%.0f risk=%.1f%% (%.0f %s) "
            "SL=%.1fp pip_val=%.2f → %.2f lots",
            pair, balance, risk_pct, risk_amount, account.currency,
            sl_pips, pip_value_1lot, lots,
        )
        return lots

    # ── Order Execution ──────────────────────────────────────────

    def open_order(
        self,
        pair: str,
        direction: str,
        sl_price: float,
        tp_price: float,
        sl_pips: float = 0.0,
        tp_pips: float = 0.0,
        risk_pct: float = 1.0,
    ) -> int | None:
        """Send a market order with broker-enforced SL/TP.

        Position size is auto-calculated from risk_pct and SL distance.
        Returns the order ticket on success, None on failure.
        """
        import MetaTrader5 as mt5

        # Check algo trading is enabled
        ti = mt5.terminal_info()
        if ti and not ti.trade_allowed:
            msg = "Algo trading disabled in MT5. Enable in Tools → Options → Expert Advisors"
            logger.error("[MT5] %s", msg)
            self.order_error.emit(pair, msg)
            return None

        # Calculate position size
        lots = self.calculate_lot_size(pair, sl_pips, risk_pct)
        if lots <= 0:
            self.order_error.emit(pair, f"Lot size calculation failed (SL={sl_pips:.1f}p)")
            return None

        # Get current price
        tick = mt5.symbol_info_tick(pair)
        if tick is None:
            self.order_error.emit(pair, "No tick data available")
            return None

        sym = mt5.symbol_info(pair)
        if sym is None:
            self.order_error.emit(pair, f"Symbol {pair} not found")
            return None

        # Determine order type and entry price
        if direction == "BUY":
            order_type = mt5.ORDER_TYPE_BUY
            price = tick.ask
        else:
            order_type = mt5.ORDER_TYPE_SELL
            price = tick.bid

        # Determine filling mode from symbol
        filling = mt5.ORDER_FILLING_IOC
        if sym.filling_mode & 1:  # FOK supported
            filling = mt5.ORDER_FILLING_FOK
        elif sym.filling_mode & 2:  # IOC supported
            filling = mt5.ORDER_FILLING_IOC

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": pair,
            "volume": lots,
            "type": order_type,
            "price": price,
            "sl": round(sl_price, sym.digits),
            "tp": round(tp_price, sym.digits),
            "deviation": self._deviation,
            "magic": self._magic,
            "comment": "TAKUMI",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling,
        }

        # Pre-check
        check = mt5.order_check(request)
        if check is None or check.retcode != 0:
            retcode = check.retcode if check else "None"
            comment = check.comment if check else "order_check returned None"
            msg = f"Order check failed: {retcode} — {comment}"
            logger.error("[MT5] %s %s %s: %s", direction, pair, lots, msg)
            self.order_error.emit(pair, msg)
            return None

        # Send order
        result = mt5.order_send(request)
        if result is None:
            msg = f"order_send returned None (error: {mt5.last_error()})"
            logger.error("[MT5] %s", msg)
            self.order_error.emit(pair, msg)
            return None

        if result.retcode != mt5.TRADE_RETCODE_DONE:
            msg = f"Order rejected: {result.retcode} — {result.comment}"
            logger.error("[MT5] %s %s: %s", direction, pair, msg)
            self.order_error.emit(pair, msg)
            return None

        # Success — register position
        ticket = result.order
        fill_price = result.price
        pos = MT5Position(
            pair=pair,
            direction=direction,
            ticket=ticket,
            volume=lots,
            open_price=fill_price,
            sl_price=sl_price,
            tp_price=tp_price,
            sl_pips=sl_pips,
            tp_pips=tp_pips,
            open_time=time.time(),
        )
        self._pos_mgr.register_open(pos)

        logger.info(
            "[MT5] ORDER FILLED: %s %s %.2f lots @ %.5f  SL=%.5f TP=%.5f  ticket=%d",
            direction, pair, lots, fill_price, sl_price, tp_price, ticket,
        )
        self.order_opened.emit(pair, ticket, direction, lots)
        return ticket

    def close_position(self, pair: str) -> bool:
        """Close an open position by pair name.

        Returns True if the close order was sent successfully.
        """
        import MetaTrader5 as mt5

        pos = self._pos_mgr.get_position(pair)
        if pos is None:
            logger.warning("[MT5] No position found for %s", pair)
            return False

        # Opposite order type to close
        if pos.direction == "BUY":
            order_type = mt5.ORDER_TYPE_SELL
            tick = mt5.symbol_info_tick(pair)
            price = tick.bid if tick else 0
        else:
            order_type = mt5.ORDER_TYPE_BUY
            tick = mt5.symbol_info_tick(pair)
            price = tick.ask if tick else 0

        sym = mt5.symbol_info(pair)
        filling = mt5.ORDER_FILLING_IOC
        if sym and sym.filling_mode & 1:
            filling = mt5.ORDER_FILLING_FOK
        elif sym and sym.filling_mode & 2:
            filling = mt5.ORDER_FILLING_IOC

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": pair,
            "volume": pos.volume,
            "type": order_type,
            "position": pos.ticket,
            "price": price,
            "deviation": self._deviation,
            "magic": self._magic,
            "comment": "TAKUMI_CLOSE",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling,
        }

        result = mt5.order_send(request)
        if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
            retcode = result.retcode if result else "None"
            comment = result.comment if result else str(mt5.last_error())
            msg = f"Close failed: {retcode} — {comment}"
            logger.error("[MT5] Close %s: %s", pair, msg)
            self.order_error.emit(pair, msg)
            return False

        # Calculate approximate P/L
        close_price = result.price
        from takumi_trader.core.trade_tracker import pip_value
        pip = pip_value(pair)
        if pos.direction == "BUY":
            pnl_pips = (close_price - pos.open_price) / pip
        else:
            pnl_pips = (pos.open_price - close_price) / pip

        self._pos_mgr.register_close(pair)

        logger.info(
            "[MT5] POSITION CLOSED: %s %s @ %.5f  P/L=%.1f pips  ticket=%d",
            pos.direction, pair, close_price, pnl_pips, pos.ticket,
        )
        self.order_closed.emit(pair, pos.ticket, pnl_pips)
        return True
