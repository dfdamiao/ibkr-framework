"""Cleanup — cancel any leftover F orders and flatten any F position.

Run after a level7 / level9 test that crashed mid-run. Idempotent: safe
to re-run even when there's nothing to clean up.

Run:
    python ibkr/tests/integration/cleanup.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from ibapi.order_cancel import OrderCancel

from ibkr.account.order_tracker import get_all_open_orders
from ibkr.account.positions import get_active_positions
from ibkr.core.connection import ConnectionManager
from ibkr.core.contracts import build_stock, resolve_conid
from ibkr.execution.order_router import SmartRouter
from ibkr.tests.integration._common import (
    PAPER_PORT,
    TEST_CLIENT_ID,
    TEST_CURRENCY,
    TEST_EXCHANGE,
    TEST_SYMBOL,
    banner,
    setup_logger,
)

logger = setup_logger("cleanup")


def run() -> int:
    print(banner(f"CLEANUP — cancel orders + flatten {TEST_SYMBOL} position"))

    conn = ConnectionManager(port=PAPER_PORT)
    if not conn.connect_with_retry(client_id=TEST_CLIENT_ID):
        logger.error("TWS not reachable on 7497")
        return 1

    cancelled = 0
    flattened = 0

    try:
        # --- Cancel any open F orders ---------------------------------
        orders = get_all_open_orders(conn, timeout=10.0)
        for o in orders:
            if o.symbol == TEST_SYMBOL:
                logger.info(
                    f"Cancelling orderId={o.order_id} permId={o.perm_id} "
                    f"type={o.order_type} qty={o.quantity}"
                )
                conn.cancelOrder(o.order_id, OrderCancel())
                cancelled += 1
        if cancelled:
            time.sleep(2)

        # --- Flatten any F position ------------------------------------
        positions = get_active_positions(conn, timeout=10.0)
        for pos in positions.values():
            if pos.symbol == TEST_SYMBOL and pos.quantity != 0:
                contract = build_stock(
                    TEST_SYMBOL, exchange=TEST_EXCHANGE, currency=TEST_CURRENCY
                )
                contract.conId = pos.con_id or resolve_conid(conn, contract)
                side = "SELL" if pos.quantity > 0 else "BUY"
                qty = int(abs(pos.quantity))
                logger.info(f"Flattening {qty} share {TEST_SYMBOL} via {side}")
                router = SmartRouter(conn)
                oid = router.submit_with_fallback(
                    contract, side, qty, TEST_EXCHANGE,
                    f"CLEANUP-{TEST_SYMBOL}",
                )
                if oid:
                    flattened += 1
                    time.sleep(3)

        logger.info(
            f"Cleanup complete: {cancelled} orders cancelled, "
            f"{flattened} positions flattened"
        )

    finally:
        conn.disconnect_gracefully()

    return 0


if __name__ == "__main__":
    sys.exit(run())
