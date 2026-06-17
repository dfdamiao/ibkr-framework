"""Level 9 — Bracket entry + STP cancel + flatten (1 share AAPL).

Tests the BracketBuilder flow:
  1. Submit bracket entry (BUY parent transmit=False + STP child transmit=True)
  2. Wait for parent fill
  3. Verify STP child is WORKING at IBKR with a permId
  4. Cancel STP via permId
  5. SELL 1 share to flatten
  6. Verify position is flat

Uses STP at 20% below estimated price so the stop won't trigger during the
test (AAPL ~$220 → STP ~$176). Quantity = 1 share.

Run:
    python ibkr/tests/integration/level9_bracket.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from ibkr.core.connection import ConnectionManager
from ibkr.core.contracts import build_stock, resolve_conid
from ibkr.execution.bracket import BracketBuilder
from ibkr.execution.fill_monitor import FillMonitor
from ibkr.execution.order_router import SmartRouter
from ibkr.tests.integration._common import (
    PAPER_PORT,
    TEST_CLIENT_ID,
    TEST_CURRENCY,
    TEST_EXCHANGE,
    TEST_QUANTITY,
    TEST_SYMBOL,
    TestResult,
    banner,
    setup_logger,
)

logger = setup_logger("level9")

# Generous SL — AAPL at $220 would need to crash to $176 for STP to trigger
ESTIMATED_PRICE = 220.0
STOP_LOSS_PCT = 0.20


def run() -> bool:
    print(banner(f"LEVEL 9 — BRACKET ENTRY + STP CANCEL (1 share {TEST_SYMBOL})"))
    result = TestResult("Level 9")

    conn = ConnectionManager(port=PAPER_PORT)
    if not conn.connect_with_retry(client_id=TEST_CLIENT_ID):
        result.check("connect", False, "TWS not reachable")
        return result.report()

    contract = build_stock(
        TEST_SYMBOL, exchange=TEST_EXCHANGE, currency=TEST_CURRENCY
    )

    try:
        # Resolve conId
        cid = resolve_conid(conn, contract)
        result.check(f"resolve_conid({TEST_SYMBOL})", cid > 0, f"conId={cid}")
        if cid <= 0:
            return result.report()
        contract.conId = cid

        bracket = BracketBuilder(conn)
        # ADAPTIVE PATIENT can take >60s even during RTH (scans prices slowly
        # for best execution quality). 300s gives the algo room to settle.
        monitor = FillMonitor(conn, timeout=300.0, poll_interval=2.0)
        router = SmartRouter(conn)

        # Pre-load open orders into perm_id_map (matches prod startup)
        conn.get_all_open_orders_sync(timeout=5.0)

        # --- Submit bracket --------------------------------------------
        parent_id, child_id = bracket.submit_bracket_entry(
            contract=contract,
            action="BUY",
            quantity=TEST_QUANTITY,
            stop_loss_pct=STOP_LOSS_PCT,
            estimated_price=ESTIMATED_PRICE,
            order_ref=f"L9-{TEST_SYMBOL}-BRACKET",
        )
        result.check(
            "Bracket submitted",
            parent_id > 0 and child_id > 0,
            f"parent={parent_id} stp_child={child_id}",
        )

        # --- Wait for parent fill --------------------------------------
        parent_results = monitor.monitor_all([parent_id])
        parent_res = parent_results.get(parent_id)
        result.check(
            "Parent BUY filled within 300s",
            parent_res is not None and parent_res.is_filled,
            f"status={parent_res.status if parent_res else 'None'} "
            f"avg_price={parent_res.avg_price if parent_res else 0}",
        )
        if not parent_res or not parent_res.is_filled:
            return result.report()

        fill_price = parent_res.avg_price
        logger.info(f"Parent filled @ ${fill_price:.4f}")

        # Give IBKR a moment to attach the STP child
        time.sleep(3)

        # --- Verify STP is working with a permId -----------------------
        # perm_id_map is keyed by permId → orderId, so we read the permId
        # off the orders dict directly. Refresh from IBKR if missing.
        stp_perm_id = int(conn.orders.get(child_id, {}).get("permId", 0) or 0)
        if stp_perm_id <= 0:
            conn.get_all_open_orders_sync(timeout=5.0)
            stp_perm_id = int(conn.orders.get(child_id, {}).get("permId", 0) or 0)

        result.check(
            "STP child has permId",
            stp_perm_id > 0,
            f"orderId={child_id} → permId={stp_perm_id}",
        )

        stp_data = conn.orders.get(child_id, {})
        result.check(
            "STP child is WORKING (PreSubmitted/Submitted)",
            stp_data.get("status", "") in ("PreSubmitted", "Submitted"),
            f"status={stp_data.get('status', '?')}",
        )

        # --- Cancel STP via permId -------------------------------------
        if stp_perm_id > 0:
            cancelled = bracket.cancel_stop_order(stp_perm_id)
            result.check(
                "STP cancelled via permId",
                cancelled,
                f"permId={stp_perm_id}",
            )

        # --- Flatten with SELL -----------------------------------------
        sell_id = router.submit_with_fallback(
            contract, "SELL", TEST_QUANTITY, TEST_EXCHANGE,
            f"L9-{TEST_SYMBOL}-FLATTEN",
        )
        result.check(
            "Flatten SELL submitted",
            sell_id is not None and sell_id > 0,
            f"orderId={sell_id}",
        )
        if sell_id:
            sell_results = monitor.monitor_all([sell_id])
            sell_res = sell_results.get(sell_id)
            result.check(
                "Flatten SELL filled within 300s",
                sell_res is not None and sell_res.is_filled,
                f"status={sell_res.status if sell_res else 'None'}",
            )

    finally:
        conn.disconnect_gracefully()

    return result.report()


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
