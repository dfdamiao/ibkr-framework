"""
Position management — sync and streaming position data.

Reference: the IBKR TWS API documentation Section 8
"""

import logging
from dataclasses import dataclass

from ibkr.core.connection import ConnectionManager

logger = logging.getLogger(__name__)


@dataclass
class PositionData:
    """Snapshot of a single position."""

    account: str
    symbol: str
    sec_type: str
    exchange: str
    currency: str
    con_id: int
    quantity: float
    avg_cost: float

    @property
    def market_value_estimate(self) -> float:
        """Estimated market value (qty x avgCost). Approximate only."""
        return abs(self.quantity) * self.avg_cost

    @property
    def is_long(self) -> bool:
        return self.quantity > 0

    @property
    def is_short(self) -> bool:
        return self.quantity < 0


def get_positions(
    conn: ConnectionManager, timeout: float = 10.0
) -> dict[int, PositionData]:
    """Get all positions as PositionData objects keyed by conId.

    Args:
        conn: Active ConnectionManager
        timeout: Seconds to wait for position data

    Returns:
        dict mapping conId -> PositionData
    """
    raw = conn.get_positions_sync(timeout=timeout)
    positions = {}
    for con_id, data in raw.items():
        positions[con_id] = PositionData(
            account=data.get("account", ""),
            symbol=data.get("symbol", ""),
            sec_type=data.get("secType", "STK"),
            exchange=data.get("exchange", ""),
            currency=data.get("currency", "USD"),
            con_id=con_id,
            quantity=float(data.get("position", 0)),
            avg_cost=float(data.get("avgCost", 0)),
        )
    logger.info(f"Retrieved {len(positions)} positions")
    return positions


def get_active_positions(
    conn: ConnectionManager, timeout: float = 10.0
) -> dict[int, PositionData]:
    """Get only positions with non-zero quantity."""
    all_pos = get_positions(conn, timeout)
    return {
        cid: p for cid, p in all_pos.items() if p.quantity != 0
    }


def get_position_by_symbol(
    conn: ConnectionManager,
    symbol: str,
    timeout: float = 10.0,
) -> PositionData | None:
    """Find a position by symbol (first match)."""
    all_pos = get_positions(conn, timeout)
    for p in all_pos.values():
        if p.symbol == symbol:
            return p
    return None
