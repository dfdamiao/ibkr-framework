"""
Order factory for IBKR TWS API.

Reference: the IBKR TWS API documentation Sections 4-5, Appendix B

All orders set: eTradeOnly=False, firmQuoteOnly=False, outsideRth=True
Never uses MIDPRICE (Error 321 outside RTH).
"""

import logging

from ibapi.order import Order
from ibapi.tag_value import TagValue

from ibkr.config.order_types import (
    BUY, SELL, MKT, STP, REL,
    PATIENT,
    GTC,
)

logger = logging.getLogger(__name__)


def _base_order(action: str, quantity: int) -> Order:
    """Create a base order with common settings."""
    order = Order()
    order.action = action
    order.totalQuantity = quantity
    order.eTradeOnly = False
    order.firmQuoteOnly = False
    order.outsideRth = True
    order.usePriceMgmtAlgo = True
    return order


def adaptive_market(
    action: str,
    quantity: int,
    priority: str = PATIENT,
) -> Order:
    """Create an ADAPTIVE MKT order (empirically best for fills).

    Priority levels:
        Patient  — scans prices slowly, best execution quality
        Normal   — balanced
        Urgent   — immediate execution, worst price
    """
    order = _base_order(action, quantity)
    order.orderType = MKT
    order.algoStrategy = "Adaptive"
    order.algoParams = [TagValue("adaptivePriority", priority)]
    return order


def relative(action: str, quantity: int) -> Order:
    """Create a REL order (pegged to NBBO).

    REL (Relative/Pegged-to-Primary) adjusts the price to track the
    best bid/ask. Good fallback when ADAPTIVE is rejected.
    """
    order = _base_order(action, quantity)
    order.orderType = REL
    order.auxPrice = 0.0  # Offset from NBBO (0 = at NBBO)
    return order


def market(action: str, quantity: int) -> Order:
    """Create a plain MKT order (no algo, fastest execution)."""
    order = _base_order(action, quantity)
    order.orderType = MKT
    return order


def stop_market(
    action: str,
    quantity: int,
    stop_price: float,
) -> Order:
    """Create a STP (Stop Market) order.

    Triggers a market order when price hits stop_price.
    Used for stop-loss orders in bracket configurations.

    Args:
        action: BUY or SELL
        quantity: Number of shares
        stop_price: Trigger price (must be tick-aligned!)
    """
    order = _base_order(action, quantity)
    order.orderType = STP
    order.auxPrice = stop_price
    order.tif = GTC  # Stop-loss should persist
    return order


def bracket_entry(
    action: str,
    quantity: int,
    stop_price: float,
    parent_order_id: int,
    priority: str = PATIENT,
) -> tuple[Order, Order]:
    """Create a bracket order pair: parent MKT + child STP.

    Bracket atomicity:
      - Parent: transmit=False (holds until child attached)
      - Child: parentId=parent, transmit=True (submits entire bracket)

    Args:
        action: BUY or SELL for the entry
        quantity: Number of shares
        stop_price: STP trigger price (must be tick-aligned!)
        parent_order_id: Order ID for the parent
        priority: Adaptive priority for the parent

    Returns:
        (parent_order, stp_child_order)
    """
    # Parent: ADAPTIVE MKT entry
    parent = adaptive_market(action, quantity, priority)
    parent.orderId = parent_order_id
    parent.transmit = False  # Wait for child

    # Child: STP exit (opposite action)
    stp_action = SELL if action == BUY else BUY
    child = stop_market(stp_action, quantity, stop_price)
    child.parentId = parent_order_id
    child.transmit = True  # Submits entire bracket

    return parent, child


def set_order_ref(order: Order, order_ref: str) -> None:
    """Set the orderRef tag on an order.

    Lesson learned: orderRef was passed as parameter but never set
    on the order object — fixed in ibkr.
    """
    order.orderRef = order_ref
