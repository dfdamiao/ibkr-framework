"""
Order tracking — open orders, completed orders, execution history.

Reference: the IBKR TWS API documentation Sections 4, 9
"""

import logging
from dataclasses import dataclass

from ibkr.core.connection import ConnectionManager

logger = logging.getLogger(__name__)


@dataclass
class OpenOrder:
    """Snapshot of an open order."""

    order_id: int
    perm_id: int
    symbol: str
    action: str  # BUY or SELL
    quantity: float
    order_type: str  # MKT, LMT, STP, etc.
    status: str  # Submitted, PreSubmitted, Filled, Cancelled
    filled: float = 0.0
    remaining: float = 0.0
    avg_fill_price: float = 0.0
    parent_id: int = 0
    client_id: int = 0


@dataclass
class ExecutionRecord:
    """Snapshot of a single execution (fill)."""

    exec_id: str
    symbol: str
    side: str  # BOT or SLD
    shares: float
    price: float
    order_id: int
    perm_id: int
    time: str
    exchange: str
    commission: float = 0.0
    order_ref: str = ""


def get_open_orders(
    conn: ConnectionManager, timeout: float = 10.0
) -> list[OpenOrder]:
    """Get open orders for THIS client ID only.

    Args:
        conn: Active ConnectionManager
        timeout: Seconds to wait

    Returns:
        List of OpenOrder objects
    """
    raw = conn.get_open_orders_sync(timeout=timeout)
    return _parse_orders(raw)


def get_all_open_orders(
    conn: ConnectionManager, timeout: float = 10.0
) -> list[OpenOrder]:
    """Get ALL open orders across ALL client IDs.

    Critical for STP order audit — shows orders from all executors.

    Args:
        conn: Active ConnectionManager
        timeout: Seconds to wait

    Returns:
        List of OpenOrder objects
    """
    raw = conn.get_all_open_orders_sync(timeout=timeout)
    return _parse_orders(raw)


def get_stp_orders(
    conn: ConnectionManager, timeout: float = 10.0
) -> list[OpenOrder]:
    """Get only STP (stop-loss) orders across all clients."""
    all_orders = get_all_open_orders(conn, timeout)
    return [o for o in all_orders if o.order_type == "STP"]


def get_executions(conn: ConnectionManager) -> list[ExecutionRecord]:
    """Get today's executions from the connection's cached data.

    Note: execDetails callbacks fire automatically on order fills.
    This reads from the already-populated conn.executions dict.

    Returns:
        List of ExecutionRecord objects
    """
    records = []
    for exec_id, data in conn.executions.items():
        records.append(ExecutionRecord(
            exec_id=exec_id,
            symbol=data.get("symbol", ""),
            side=data.get("side", ""),
            shares=float(data.get("shares", 0)),
            price=float(data.get("price", 0)),
            order_id=int(data.get("orderId", 0)),
            perm_id=int(data.get("permId", 0)),
            time=data.get("time", ""),
            exchange=data.get("exchange", ""),
            commission=conn.commissions.get(exec_id, 0.0),
            order_ref=data.get("orderRef", ""),
        ))
    return records


def _parse_orders(raw: dict[int, dict]) -> list[OpenOrder]:
    """Convert raw order dict to list of OpenOrder objects."""
    orders = []
    for oid, data in raw.items():
        order_obj = data.get("order")
        contract = data.get("contract")
        orders.append(OpenOrder(
            order_id=oid,
            perm_id=data.get("permId", 0),
            symbol=contract.symbol if contract else data.get("symbol", "?"),
            action=order_obj.action if order_obj else "",
            quantity=float(order_obj.totalQuantity) if order_obj else 0.0,
            order_type=order_obj.orderType if order_obj else "",
            status=data.get("status", ""),
            filled=float(data.get("filled", 0)),
            remaining=float(data.get("remaining", 0)),
            avg_fill_price=float(data.get("avgFillPrice", 0)),
            parent_id=data.get("parentId", 0),
            client_id=data.get("clientId", 0),
        ))
    return orders
