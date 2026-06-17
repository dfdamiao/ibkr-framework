"""
P&L tracking — account-wide and per-position.

Reference: the IBKR TWS API documentation Section 8.3

P&L updates stream at ~1/sec after subscription.
"""

import logging
import time
from dataclasses import dataclass

from ibkr.core.connection import ConnectionManager

logger = logging.getLogger(__name__)


@dataclass
class AccountPnL:
    """Account-wide P&L snapshot."""

    daily_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0


@dataclass
class PositionPnL:
    """Per-position P&L snapshot."""

    con_id: int = 0
    position: float = 0.0
    daily_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    market_value: float = 0.0


def subscribe_account_pnl(
    conn: ConnectionManager,
    account: str | None = None,
) -> int:
    """Subscribe to account-wide P&L updates.

    Updates arrive in conn.account_pnl at ~1/sec.

    Args:
        conn: Active ConnectionManager
        account: Account code (default: first managed account)

    Returns:
        reqId for cancellation
    """
    acct = account or (conn.managed_accounts or "").split(",")[0]
    req_id = conn.next_order_id()
    conn.reqPnL(req_id, acct, "")
    logger.debug(f"Subscribed to account P&L (reqId={req_id})")
    return req_id


def cancel_account_pnl(conn: ConnectionManager, req_id: int) -> None:
    """Cancel account P&L subscription."""
    conn.cancelPnL(req_id)


def get_account_pnl_snapshot(
    conn: ConnectionManager,
    account: str | None = None,
    wait: float = 2.0,
) -> AccountPnL:
    """Subscribe, wait for data, return snapshot, cancel.

    Args:
        conn: Active ConnectionManager
        account: Account code
        wait: Seconds to wait for first update

    Returns:
        AccountPnL snapshot
    """
    req_id = subscribe_account_pnl(conn, account)
    time.sleep(wait)  # Wait for at least one update
    cancel_account_pnl(conn, req_id)

    data = conn.account_pnl
    return AccountPnL(
        daily_pnl=data.get("dailyPnL", 0.0),
        unrealized_pnl=data.get("unrealizedPnL", 0.0),
        realized_pnl=data.get("realizedPnL", 0.0),
    )


def subscribe_position_pnl(
    conn: ConnectionManager,
    account: str | None = None,
    con_id: int = 0,
) -> int:
    """Subscribe to per-position P&L updates.

    Updates arrive in conn.position_pnl[reqId] at ~1/sec.

    Args:
        conn: Active ConnectionManager
        account: Account code
        con_id: Contract ID to track

    Returns:
        reqId for cancellation
    """
    acct = account or (conn.managed_accounts or "").split(",")[0]
    req_id = conn.next_order_id()
    conn.reqPnLSingle(req_id, acct, "", con_id)
    logger.debug(
        f"Subscribed to position P&L conId={con_id} (reqId={req_id})"
    )
    return req_id


def cancel_position_pnl(conn: ConnectionManager, req_id: int) -> None:
    """Cancel per-position P&L subscription."""
    conn.cancelPnLSingle(req_id)
