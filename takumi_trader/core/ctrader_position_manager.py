"""cTrader position tracking and duplicate prevention (Stage 2)."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class OpenPosition:
    pair: str
    direction: str  # "BUY" or "SELL"
    position_id: int
    volume: float  # in lots
    open_price: float
    open_time: float = field(default_factory=time.time)


class CTraderPositionManager:
    """Track open cTrader positions and prevent duplicates."""

    def __init__(self) -> None:
        self._positions: dict[str, OpenPosition] = {}  # keyed by pair

    @property
    def open_count(self) -> int:
        return len(self._positions)

    def has_position(self, pair: str, direction: str) -> bool:
        """Check if a position exists for this pair in this direction."""
        pos = self._positions.get(pair)
        return pos is not None and pos.direction == direction

    def get_position(self, pair: str) -> OpenPosition | None:
        return self._positions.get(pair)

    def register_open(
        self,
        pair: str,
        direction: str,
        position_id: int,
        volume: float,
        price: float,
    ) -> None:
        self._positions[pair] = OpenPosition(
            pair=pair,
            direction=direction,
            position_id=position_id,
            volume=volume,
            open_price=price,
        )
        logger.info(
            "Position registered: %s %s pos=%d vol=%.2f @ %.5f",
            direction, pair, position_id, volume, price,
        )

    def register_close(self, position_id: int) -> str | None:
        """Remove position by ID. Returns the pair name or None."""
        for pair, pos in list(self._positions.items()):
            if pos.position_id == position_id:
                del self._positions[pair]
                logger.info("Position closed: %s pos=%d", pair, position_id)
                return pair
        return None

    def reconcile(self, positions: list[dict]) -> None:
        """Sync with broker state. positions is a list of dicts with
        keys: pair, direction, position_id, volume, price.
        """
        self._positions.clear()
        for p in positions:
            self._positions[p["pair"]] = OpenPosition(
                pair=p["pair"],
                direction=p["direction"],
                position_id=p["position_id"],
                volume=p["volume"],
                open_price=p.get("price", 0.0),
            )
        logger.info("Positions reconciled: %d open", len(self._positions))

    def all_positions(self) -> dict[str, OpenPosition]:
        return dict(self._positions)

    # ── Persistence ───────────────────────────────────────────────

    def save(self, path: Path) -> None:
        try:
            data = [asdict(p) for p in self._positions.values()]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception as e:
            logger.error("Failed to save cTrader positions: %s", e)

    def load(self, path: Path) -> None:
        if not path.exists():
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            self._positions.clear()
            for item in data:
                pos = OpenPosition(**item)
                self._positions[pos.pair] = pos
            logger.info("Loaded %d cTrader positions from disk", len(self._positions))
        except Exception as e:
            logger.error("Failed to load cTrader positions: %s", e)
