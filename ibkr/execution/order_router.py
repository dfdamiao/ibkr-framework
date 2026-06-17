"""
Smart order router with 4-tier fallback chain.

Reference: the IBKR TWS API documentation Section 17.4

Fallback chain:
    1. ADAPTIVE MKT + Patient on SMART
    2. REL on SMART (pegged to NBBO)
    3. ADAPTIVE MKT + Patient on direct exchange
    4. REL on direct exchange
    → FAIL: log and skip

1.5s rejection check between attempts.
"""

import logging
import time

from ibapi.contract import Contract

from ibkr.core.connection import ConnectionManager
from ibkr.core.orders import adaptive_market, relative, set_order_ref

logger = logging.getLogger(__name__)

# Time to wait for rejection callback before assuming order is working
REJECTION_CHECK_DELAY = 1.5


class SmartRouter:
    """Route orders through the 4-tier fallback chain."""

    def __init__(self, conn: ConnectionManager):
        self.conn = conn

    def submit_with_fallback(
        self,
        contract: Contract,
        action: str,
        quantity: int,
        csv_exchange: str = "",
        order_ref: str = "",
    ) -> int | None:
        """Submit an order with automatic fallback on rejection.

        Args:
            contract: IBKR contract to trade
            action: "BUY" or "SELL"
            quantity: Number of shares
            csv_exchange: Direct exchange from contract_mapping.csv
                (used in tiers 3-4)
            order_ref: OrderRef tag for the order

        Returns:
            orderId if submitted (may still be pending fill), None if
            all tiers rejected.
        """
        attempts = self._build_attempts(contract, csv_exchange)

        for tier, (exchange, order_factory_fn, label) in enumerate(
            attempts, start=1
        ):
            order_id = self.conn.next_order_id()
            order = order_factory_fn(action, quantity)
            if order_ref:
                set_order_ref(order, order_ref)

            # Set exchange on contract for this attempt
            submit_contract = Contract()
            submit_contract.conId = contract.conId
            submit_contract.symbol = contract.symbol
            submit_contract.secType = contract.secType
            submit_contract.currency = contract.currency
            submit_contract.exchange = exchange

            logger.info(
                f"  Tier {tier}: {label} on {exchange} "
                f"(orderId={order_id})"
            )

            self.conn.placeOrder(order_id, submit_contract, order)
            time.sleep(REJECTION_CHECK_DELAY)

            # Check for rejection
            if self.conn.has_order_errors(order_id):
                errors = self.conn.order_errors[order_id]
                logger.warning(
                    f"  ⚠️ Tier {tier} rejected: errors={errors}"
                )
                continue

            # Check if order status looks ok (not error/cancelled)
            status = self.conn.get_order_status(order_id)
            if status in ("Cancelled", "Inactive"):
                logger.warning(
                    f"  ⚠️ Tier {tier} status={status}, trying next"
                )
                continue

            logger.info(
                f"  ✅ Tier {tier} accepted (orderId={order_id}, "
                f"status={status or 'Submitted'})"
            )
            return order_id

        # All tiers exhausted
        logger.error(
            f"❌ ALL 4 TIERS REJECTED for {contract.symbol} "
            f"{action} {quantity} — order skipped"
        )
        return None

    def _build_attempts(
        self, contract: Contract, csv_exchange: str
    ) -> list[tuple[str, callable, str]]:
        """Build the ordered list of (exchange, order_factory, label)."""
        attempts = [
            ("SMART", adaptive_market, "ADAPTIVE MKT Patient"),
            ("SMART", relative, "REL pegged-to-NBBO"),
        ]

        # Tiers 3-4: direct exchange (if different from SMART)
        direct = csv_exchange or contract.exchange or ""
        if direct and direct != "SMART":
            attempts.extend([
                (direct, adaptive_market, "ADAPTIVE MKT Patient"),
                (direct, relative, "REL pegged-to-NBBO"),
            ])
        else:
            # If no direct exchange, retry SMART with plain MKT
            from ibkr.core.orders import market
            attempts.extend([
                ("SMART", market, "Plain MKT"),
                ("SMART", relative, "REL retry"),
            ])

        return attempts
