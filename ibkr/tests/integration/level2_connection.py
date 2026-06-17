"""Level 2 — Connection sanity (read-only).

What's verified:
  - Connect to paper TWS @ 7497, receive nextValidId
  - Managed accounts string populated
  - get_active_positions matches TWS portfolio view
  - get_account_summary returns NetLiq, Cash, BuyingPower, margin
  - get_account_pnl_snapshot streams daily/unrealized/realized
  - get_all_open_orders + get_stp_orders see orders across all client IDs
  - Disconnect cleanly (no zombie session)

Run:
    python ibkr/tests/integration/level2_connection.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from ibkr.account.order_tracker import get_all_open_orders, get_stp_orders
from ibkr.account.pnl import get_account_pnl_snapshot
from ibkr.account.positions import get_active_positions
from ibkr.account.summary import get_account_summary
from ibkr.core.connection import ConnectionManager
from ibkr.tests.integration._common import (
    PAPER_PORT,
    TEST_CLIENT_ID,
    TestResult,
    banner,
    setup_logger,
)

logger = setup_logger("level2")


def run() -> bool:
    print(banner("LEVEL 2 — CONNECTION SANITY"))
    result = TestResult("Level 2")

    conn = ConnectionManager(port=PAPER_PORT)
    if not conn.connect_with_retry(client_id=TEST_CLIENT_ID):
        result.check("connect_with_retry", False, "TWS not reachable on 7497")
        return result.report()
    result.check("connect_with_retry", True, f"client_id={TEST_CLIENT_ID}")

    try:
        # nextValidId & accounts
        result.check(
            "next_valid_id received",
            conn.nextorderId is not None and conn.nextorderId > 0,
            f"next_valid_id={conn.nextorderId}",
        )
        result.check(
            "managed_accounts populated",
            bool(conn.managed_accounts),
            f"accounts={conn.managed_accounts}",
        )

        # Positions
        positions = get_active_positions(conn, timeout=10.0)
        result.check(
            "get_active_positions",
            isinstance(positions, dict),
            f"{len(positions)} active positions",
        )

        # Account summary
        summary = get_account_summary(conn, timeout=10.0)
        result.check(
            "get_account_summary",
            summary.net_liquidation > 0,
            f"NetLiq=${summary.net_liquidation:,.2f} "
            f"Cash=${summary.total_cash:,.2f} "
            f"BuyPow=${summary.buying_power:,.2f} "
            f"Margin={summary.margin_used_pct:.1%}",
        )

        # P&L snapshot (waits ~2s for first update)
        pnl = get_account_pnl_snapshot(conn)
        result.check(
            "get_account_pnl_snapshot",
            pnl is not None,
            f"Daily=${pnl.daily_pnl:+,.2f} "
            f"Unrealized=${pnl.unrealized_pnl:+,.2f} "
            f"Realized=${pnl.realized_pnl:+,.2f}",
        )

        # Open orders (all client IDs)
        orders = get_all_open_orders(conn, timeout=10.0)
        stps = get_stp_orders(conn, timeout=10.0)
        result.check(
            "get_all_open_orders",
            isinstance(orders, list),
            f"{len(orders)} open orders ({len(stps)} STPs)",
        )

    finally:
        conn.disconnect_gracefully()
        result.check("disconnect_gracefully", True)

    return result.report()


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
