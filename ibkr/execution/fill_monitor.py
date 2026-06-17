"""
Fill monitor — polls order status until filled, timed out, or rejected.

Phase 3 of the non-blocking execution pattern:
  Phase 1: Submit all EXIT orders (5s each)
  Phase 2: Submit all ENTRY orders (5s each)
  Phase 3: Monitor all orders in parallel (2s polling, 10min timeout)
"""

import logging
import time
from dataclasses import dataclass

from ibkr.core.connection import ConnectionManager

logger = logging.getLogger(__name__)

# Terminal order statuses — stop monitoring when reached
TERMINAL_STATUSES = frozenset({
    "Filled",
    "Cancelled",
    "Inactive",
    "ApiCancelled",
})

# Partial fill — still monitoring but record partial data
PARTIAL_STATUSES = frozenset({
    "PreSubmitted",  # EU overnight orders
    "Submitted",
})


@dataclass
class FillResult:
    """Result of monitoring an order to completion."""

    order_id: int
    status: str  # Filled, Cancelled, Timeout, Rejected, PartialFill
    avg_price: float = 0.0
    filled_qty: float = 0.0
    remaining_qty: float = 0.0
    commission: float = 0.0
    perm_id: int = 0

    @property
    def is_filled(self) -> bool:
        return self.status == "Filled"

    @property
    def is_partial(self) -> bool:
        return self.status == "PartialFill"


class FillMonitor:
    """Monitor multiple orders until all are terminal or timed out."""

    def __init__(
        self,
        conn: ConnectionManager,
        timeout: float = 600.0,
        poll_interval: float = 2.0,
    ):
        self.conn = conn
        self.timeout = timeout
        self.poll_interval = poll_interval

    def monitor_all(
        self, order_ids: list[int]
    ) -> dict[int, FillResult]:
        """Monitor all order IDs until filled, cancelled, or timed out.

        Args:
            order_ids: List of order IDs to monitor.

        Returns:
            dict mapping orderId -> FillResult
        """
        if not order_ids:
            return {}

        results: dict[int, FillResult] = {}
        pending = set(order_ids)
        start_time = time.time()

        logger.info(
            f"Monitoring {len(pending)} orders "
            f"(timeout={self.timeout}s, poll={self.poll_interval}s)"
        )

        while pending and (time.time() - start_time) < self.timeout:
            for oid in list(pending):
                order_data = self.conn.orders.get(oid, {})
                status = order_data.get("status", "")

                if status == "Filled":
                    # commissionReport is a separate async callback that
                    # often lands AFTER orderStatus=Filled. If commission
                    # reads 0 right now, wait briefly for the callback.
                    commission = self.conn.get_commission_for_order(oid)
                    if commission == 0.0:
                        for _ in range(20):  # up to 2s @ 0.1s steps
                            time.sleep(0.1)
                            commission = self.conn.get_commission_for_order(oid)
                            if commission > 0:
                                break
                    result = FillResult(
                        order_id=oid,
                        status="Filled",
                        avg_price=order_data.get("avgFillPrice", 0.0),
                        filled_qty=order_data.get("filled", 0.0),
                        remaining_qty=0.0,
                        commission=commission,
                        perm_id=order_data.get("permId", 0),
                    )
                    results[oid] = result
                    pending.discard(oid)
                    symbol = order_data.get("symbol", "?")
                    logger.info(
                        f"  FILLED: orderId={oid} {symbol} "
                        f"@ {result.avg_price:.4f} "
                        f"qty={result.filled_qty} "
                        f"commission={result.commission:.4f}"
                    )

                elif status in ("Cancelled", "Inactive", "ApiCancelled"):
                    results[oid] = FillResult(
                        order_id=oid,
                        status=status,
                        perm_id=order_data.get("permId", 0),
                    )
                    pending.discard(oid)
                    logger.warning(
                        f"  {status}: orderId={oid}"
                    )

                # Check for rejection errors
                elif self.conn.has_order_errors(oid):
                    results[oid] = FillResult(
                        order_id=oid,
                        status="Rejected",
                        perm_id=order_data.get("permId", 0),
                    )
                    pending.discard(oid)
                    errors = self.conn.order_errors[oid]
                    logger.warning(
                        f"  REJECTED: orderId={oid} errors={errors}"
                    )

            if pending:
                time.sleep(self.poll_interval)

        # Handle timeouts — mark remaining as PartialFill or Timeout
        for oid in pending:
            order_data = self.conn.orders.get(oid, {})
            raw_filled = order_data.get("filled", 0.0)
            # Defensive: if connection layer ever leaks a string/Decimal/None
            # through, coerce here so the `> 0` comparison can't crash mid-loop.
            try:
                filled = float(raw_filled) if raw_filled not in ("", None) else 0.0
            except (TypeError, ValueError):
                filled = 0.0
            status = order_data.get("status", "Unknown")

            if filled > 0:
                # EU overnight fills: mark as PartialFill for recovery.
                # Same async-commission workaround as the Filled branch.
                commission = self.conn.get_commission_for_order(oid)
                if commission == 0.0:
                    for _ in range(20):
                        time.sleep(0.1)
                        commission = self.conn.get_commission_for_order(oid)
                        if commission > 0:
                            break
                results[oid] = FillResult(
                    order_id=oid,
                    status="PartialFill",
                    avg_price=order_data.get("avgFillPrice", 0.0),
                    filled_qty=filled,
                    remaining_qty=order_data.get("remaining", 0.0),
                    commission=commission,
                    perm_id=order_data.get("permId", 0),
                )
                logger.warning(
                    f"  PARTIAL: orderId={oid} filled={filled} "
                    f"status={status} — will recover via "
                    f"resolve_pending_fills.py"
                )
            else:
                results[oid] = FillResult(
                    order_id=oid,
                    status="Timeout",
                    perm_id=order_data.get("permId", 0),
                )
                logger.warning(
                    f"  TIMEOUT: orderId={oid} status={status} "
                    f"after {self.timeout}s"
                )

        filled_count = sum(1 for r in results.values() if r.is_filled)
        logger.info(
            f"Monitor complete: {filled_count}/{len(order_ids)} filled"
        )
        return results
