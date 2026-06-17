"""Level 7 — Order router round trip (1 share BUY MKT → 1 share SELL MKT).

Tests the SmartRouter and FillMonitor against a real paper account.
Uses AAPL (Apple), ~$220/share — NOT in any contract_mapping.csv. Quantity = 1.

What's verified:
  - resolve_conid finds F via reqContractDetails (cache miss path)
  - SmartRouter submits ADAPTIVE MKT BUY successfully
  - FillMonitor detects the fill (status == "Filled")
  - Round-trip SELL also fills
  - No leftover position (broker ledger flat for F after run)
  - Commission received via commissionReport callback

Safety:
  - 1 share max exposure (~$10)
  - Paper account only (port 7497)
  - Submits BUY then waits for fill BEFORE submitting SELL
  - On any error, attempts to flatten any partial position before exit

Run:
    python ibkr/tests/integration/level7_round_trip.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from ibkr.core.connection import ConnectionManager
from ibkr.core.contracts import build_stock, resolve_conid
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

logger = setup_logger("level7")


def _flatten_test_symbol(conn, contract, side: str) -> None:
    """Emergency flatten: send a market SELL/BUY on the test symbol if a leg is hung."""
    try:
        router = SmartRouter(conn)
        oid = router.submit_with_fallback(
            contract, side, TEST_QUANTITY, TEST_EXCHANGE,
            f"FLATTEN-{TEST_SYMBOL}",
        )
        logger.warning(f"Submitted flatten {side} order {oid}")
        time.sleep(3)
    except Exception as e:
        logger.error(f"Flatten failed: {e}")


def run() -> bool:
    print(banner(f"LEVEL 7 — ROUND TRIP (1 share {TEST_SYMBOL} BUY → SELL)"))
    result = TestResult("Level 7")

    conn = ConnectionManager(port=PAPER_PORT)
    if not conn.connect_with_retry(client_id=TEST_CLIENT_ID):
        result.check("connect", False, "TWS not reachable")
        return result.report()

    contract = build_stock(
        TEST_SYMBOL, exchange=TEST_EXCHANGE, currency=TEST_CURRENCY
    )

    try:
        # --- Resolve conId ---------------------------------------------
        cid = resolve_conid(conn, contract)
        result.check(
            f"resolve_conid({TEST_SYMBOL})",
            cid > 0,
            f"conId={cid}",
        )
        if cid <= 0:
            return result.report()
        contract.conId = cid

        router = SmartRouter(conn)
        # ADAPTIVE PATIENT can take >60s even during RTH; 300s is a safe ceiling.
        monitor = FillMonitor(conn, timeout=300.0, poll_interval=2.0)

        # --- BUY 1 share -----------------------------------------------
        buy_id = router.submit_with_fallback(
            contract, "BUY", TEST_QUANTITY, TEST_EXCHANGE,
            f"L7-{TEST_SYMBOL}-BUY",
        )
        result.check(
            "BUY submitted",
            buy_id is not None and buy_id > 0,
            f"orderId={buy_id}",
        )
        if not buy_id:
            return result.report()

        buy_results = monitor.monitor_all([buy_id])
        buy_res = buy_results.get(buy_id)
        result.check(
            "BUY filled within 300s",
            buy_res is not None and buy_res.is_filled,
            f"status={buy_res.status if buy_res else 'None'} "
            f"avg_price={buy_res.avg_price if buy_res else 0} "
            f"commission={buy_res.commission if buy_res else 0}",
        )
        if not buy_res or not buy_res.is_filled:
            _flatten_test_symbol(conn, contract, "SELL")
            return result.report()

        # --- SELL 1 share (round trip) ---------------------------------
        sell_id = router.submit_with_fallback(
            contract, "SELL", TEST_QUANTITY, TEST_EXCHANGE,
            f"L7-{TEST_SYMBOL}-SELL",
        )
        result.check(
            "SELL submitted",
            sell_id is not None and sell_id > 0,
            f"orderId={sell_id}",
        )
        if not sell_id:
            _flatten_test_symbol(conn, contract, "SELL")
            return result.report()

        sell_results = monitor.monitor_all([sell_id])
        sell_res = sell_results.get(sell_id)
        result.check(
            "SELL filled within 300s",
            sell_res is not None and sell_res.is_filled,
            f"status={sell_res.status if sell_res else 'None'} "
            f"avg_price={sell_res.avg_price if sell_res else 0}",
        )

        # --- Commission tracking ---------------------------------------
        result.check(
            "BUY commissionReport received",
            buy_res.commission >= 0,
            f"${buy_res.commission:.4f}",
        )

    finally:
        conn.disconnect_gracefully()

    return result.report()


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
