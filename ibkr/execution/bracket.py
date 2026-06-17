"""
Bracket order builder for stop-loss strategies.

Used by: alternative_with_sl (client 10), ma200 (client 15)

Reference: the IBKR TWS API documentation Section 5

Design principles:
  - Bracket atomicity: parent transmit=False, last child transmit=True
  - Dynamic tick size via reqMarketRule (NOT hardcoded 0.05)
  - permId resolution for cross-session STP cancellation
"""

import logging
import time

from ibapi.contract import Contract
from ibapi.order_cancel import OrderCancel

from ibkr.core.connection import ConnectionManager
from ibkr.core.contracts import get_tick_size, round_to_tick
from ibkr.core.orders import (
    adaptive_market,
    stop_market,
    set_order_ref,
)
from ibkr.config.order_types import BUY, SELL, GTC

logger = logging.getLogger(__name__)


class BracketBuilder:
    """Build and manage bracket orders (parent entry + STP child)."""

    def __init__(self, conn: ConnectionManager):
        self.conn = conn
        # Track failed STP cancels for end-of-run warning
        self.failed_stp_cancels: list[dict] = []

    def calculate_stop_price(
        self,
        fill_price: float,
        stop_loss_pct: float,
        contract: Contract,
        action: str = BUY,
    ) -> float:
        """Calculate tick-aligned stop price from fill price.

        Uses reqMarketRule for dynamic tick size resolution.
        This is the CRITICAL fix — the old system hardcoded 0.05 for
        all EU exchanges, which is wrong for e.g. STK.PA (needs 0.02).

        Args:
            fill_price: Actual fill price from IBKR
            stop_loss_pct: Stop loss as fraction (e.g. 0.05 for 5%)
            contract: Contract for tick size lookup
            action: Original entry action (BUY = long STP below,
                SELL = short STP above)

        Returns:
            Tick-aligned stop price
        """
        # Normalize stop_loss_pct to fractional if given as percentage
        sl = stop_loss_pct if stop_loss_pct < 1 else stop_loss_pct / 100

        if action == BUY:
            raw_stop = fill_price * (1 - sl)
        else:
            raw_stop = fill_price * (1 + sl)

        # Get dynamic tick size for this contract at this price level
        tick = get_tick_size(self.conn, contract, raw_stop)
        aligned_stop = round_to_tick(raw_stop, tick)

        logger.info(
            f"  STP price: fill={fill_price:.4f} "
            f"sl={sl:.2%} raw={raw_stop:.4f} "
            f"tick={tick} aligned={aligned_stop:.4f}"
        )
        return aligned_stop

    def submit_bracket_entry(
        self,
        contract: Contract,
        action: str,
        quantity: int,
        stop_loss_pct: float,
        estimated_price: float,
        order_ref: str = "",
        stop_price: float | None = None,
    ) -> tuple[int, int]:
        """Submit a bracket order: parent ADAPTIVE MKT + child STP.

        Args:
            contract: Contract to trade
            action: "BUY" or "SELL" for the entry
            quantity: Number of shares
            stop_loss_pct: Stop loss as fraction (e.g. 0.05). Ignored
                if stop_price is provided.
            estimated_price: Estimated price for initial STP calculation
                (STP is recalculated on actual fill). Ignored if
                stop_price is provided.
            order_ref: OrderRef tag
            stop_price: Optional absolute stop price (e.g. ATR-derived
                stop level). When provided, takes precedence over
                stop_loss_pct + estimated_price — needed for strategies
                like TSMOM whose signal_generator pre-computes the stop.

        Returns:
            (parent_order_id, child_order_id)
        """
        parent_id = self.conn.next_order_id()
        child_id = self.conn.next_order_id()

        if stop_price is None:
            # Calculate initial stop price from estimate
            stop_price = self.calculate_stop_price(
                estimated_price, stop_loss_pct, contract, action
            )
        else:
            # External stop (e.g. an ATR-trailing stop managed by your strategy)
            # still needs tick-snapping. Without this, IBKR rejects the child
            # STP with Error 110 "minimum price variation" — discovered
            # 2026-05-05 BOTZ smoke (stop=37.0853 vs $0.01 USD tick).
            tick = get_tick_size(self.conn, contract, stop_price)
            aligned = round_to_tick(stop_price, tick)
            if aligned != stop_price:
                logger.info(
                    f"  STP price: external={stop_price:.4f} "
                    f"tick={tick} aligned={aligned:.4f}"
                )
            stop_price = aligned

        # Parent: ADAPTIVE MKT entry, transmit=False
        parent = adaptive_market(action, quantity)
        parent.orderId = parent_id
        parent.transmit = False
        if order_ref:
            set_order_ref(parent, order_ref)

        # Child: STP exit (opposite action), transmit=True
        stp_action = SELL if action == BUY else BUY
        child = stop_market(stp_action, quantity, stop_price)
        child.orderId = child_id
        child.parentId = parent_id
        child.tif = GTC
        child.transmit = True  # Submits entire bracket atomically
        # Stamp the child with the SAME strategy orderRef as the parent.
        # Without this the STP child carries an empty orderRef, so
        # stp_audit.scope_stps_to_strategy drops it as 'foreign' and the
        # audit reads every bracket position as 'missing STP'
        # (the '3 missing / 0 orphans' symptom, 2026-06-03). Applies to
        # every bracket strategy.
        if order_ref:
            set_order_ref(child, order_ref)

        logger.info(
            f"  Bracket: parent={parent_id} ({action} {quantity}) "
            f"child={child_id} (STP @ {stop_price:.4f})"
        )

        self.conn.placeOrder(parent_id, contract, parent)
        self.conn.placeOrder(child_id, contract, child)

        return parent_id, child_id

    def update_stp_after_fill(
        self,
        child_order_id: int,
        contract: Contract,
        actual_fill_price: float,
        stop_loss_pct: float,
        quantity: int,
        action: str = BUY,
    ) -> float | None:
        """Recalculate and update STP price after actual fill.

        The initial bracket uses an estimated price. Once the parent
        fills, recalculate the STP with the actual avgFillPrice.

        Returns:
            New stop price, or None if update failed
        """
        new_stop = self.calculate_stop_price(
            actual_fill_price, stop_loss_pct, contract, action
        )

        # Cancel old STP and resubmit with corrected price
        try:
            self.conn.cancelOrder(child_order_id, OrderCancel())
            time.sleep(1.0)
        except Exception as e:
            logger.warning(f"  STP cancel for update failed: {e}")

        # Resubmit with new price
        stp_action = SELL if action == BUY else BUY
        new_child = stop_market(stp_action, quantity, new_stop)
        new_child_id = self.conn.next_order_id()
        new_child.tif = GTC
        self.conn.placeOrder(new_child_id, contract, new_child)

        logger.info(
            f"  STP updated: old_id={child_order_id} "
            f"new_id={new_child_id} price={new_stop:.4f}"
        )
        return new_stop

    def cancel_stop_order(
        self,
        perm_id: int,
        timeout: float = 15.0,
    ) -> bool:
        """Cancel a STP order by permId (cross-session resolution).

        Lesson learned:
          - cancelOrder() only works from SAME client ID
          - Must resolve permId -> session orderId first
          - Pre-populate perm_id_map via reqAllOpenOrders()

        Args:
            perm_id: Permanent order ID of the STP to cancel
            timeout: Seconds to wait for cancel confirmation

        Returns:
            True if cancel confirmed, False if unconfirmed
        """
        # Resolve permId to session orderId
        session_oid = self.conn.perm_id_map.get(perm_id)

        if session_oid is None:
            # Retry: refresh open orders and try again
            logger.info(
                f"  permId {perm_id} not in map, "
                f"refreshing open orders..."
            )
            self.conn.get_all_open_orders_sync(timeout=5.0)
            session_oid = self.conn.perm_id_map.get(perm_id)

        if session_oid is None:
            logger.warning(
                f"  Cannot resolve permId {perm_id} to session orderId"
            )
            self.failed_stp_cancels.append({
                "permId": perm_id,
                "reason": "unresolvable",
            })
            return False

        # Cancel the order
        logger.info(
            f"  Cancelling STP: permId={perm_id} "
            f"-> orderId={session_oid}"
        )
        try:
            self.conn.cancelOrder(session_oid, OrderCancel())
        except Exception as e:
            logger.warning(f"  cancelOrder failed: {e}")
            self.failed_stp_cancels.append({
                "permId": perm_id,
                "orderId": session_oid,
                "reason": str(e),
            })
            return False

        # Wait for cancel confirmation
        start = time.time()
        while (time.time() - start) < timeout:
            status = self.conn.get_order_status(session_oid)
            if status in ("Cancelled", "ApiCancelled"):
                logger.info(f"  STP cancel confirmed: orderId={session_oid}")
                return True
            time.sleep(0.5)

        logger.warning(
            f"  STP cancel NOT confirmed after {timeout}s: "
            f"orderId={session_oid} permId={perm_id}"
        )
        self.failed_stp_cancels.append({
            "permId": perm_id,
            "orderId": session_oid,
            "reason": "timeout",
        })
        return False

    def report_failed_cancels(self) -> None:
        """Log any failed STP cancels at end of run."""
        if not self.failed_stp_cancels:
            return
        logger.warning(
            f"\n{'='*60}\n"
            f"⚠️  {len(self.failed_stp_cancels)} STP CANCEL(S) "
            f"NOT CONFIRMED\n"
            f"{'='*60}"
        )
        for fc in self.failed_stp_cancels:
            logger.warning(f"  {fc}")
        logger.warning(
            "Audit these orphaned STP orders manually "
            "before the next run.\n"
            ""
        )
