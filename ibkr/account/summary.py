"""
Account summary — NetLiquidation, Cash, BuyingPower, etc.

Reference: the IBKR TWS API documentation Section 8.1-8.2
"""

import logging
from dataclasses import dataclass

from ibkr.core.connection import ConnectionManager

logger = logging.getLogger(__name__)

# Common account summary tags
SUMMARY_TAGS = (
    "NetLiquidation",
    "TotalCashValue",
    "BuyingPower",
    "GrossPositionValue",
    "MaintMarginReq",
    "AvailableFunds",
    "ExcessLiquidity",
    "Cushion",
    "FullMaintMarginReq",
    "FullInitMarginReq",
)


@dataclass
class AccountSummary:
    """Snapshot of key account metrics."""

    net_liquidation: float = 0.0
    total_cash: float = 0.0
    buying_power: float = 0.0
    gross_position_value: float = 0.0
    maint_margin_req: float = 0.0
    available_funds: float = 0.0
    excess_liquidity: float = 0.0
    cushion: float = 0.0  # As percentage (e.g. 0.55 = 55%)
    account: str = ""

    @property
    def margin_used_pct(self) -> float:
        """Margin utilization as percentage."""
        if self.net_liquidation > 0:
            return self.maint_margin_req / self.net_liquidation
        return 0.0


def get_account_summary(
    conn: ConnectionManager,
    tags: tuple[str, ...] = SUMMARY_TAGS,
    timeout: float = 10.0,
) -> AccountSummary:
    """Request account summary and return as AccountSummary.

    Args:
        conn: Active ConnectionManager
        tags: Comma-separated tag names to request
        timeout: Seconds to wait for response

    Returns:
        AccountSummary with populated fields
    """
    conn._account_summary_event.clear()
    conn.account_summary.clear()

    req_id = conn.next_order_id()
    tag_str = ",".join(tags)
    conn.reqAccountSummary(req_id, "All", tag_str)

    if not conn._account_summary_event.wait(timeout=timeout):
        logger.warning(f"Account summary timed out after {timeout}s")

    # Cancel the subscription (it's streaming otherwise)
    conn.cancelAccountSummary(req_id)

    data = conn.account_summary
    summary = AccountSummary(
        net_liquidation=_parse_float(data.get("NetLiquidation")),
        total_cash=_parse_float(data.get("TotalCashValue")),
        buying_power=_parse_float(data.get("BuyingPower")),
        gross_position_value=_parse_float(data.get("GrossPositionValue")),
        maint_margin_req=_parse_float(data.get("MaintMarginReq")),
        available_funds=_parse_float(data.get("AvailableFunds")),
        excess_liquidity=_parse_float(data.get("ExcessLiquidity")),
        cushion=_parse_float(data.get("Cushion")),
        account=conn.managed_accounts or "",
    )

    logger.info(
        f"Account summary: NetLiq=${summary.net_liquidation:,.2f} "
        f"Cash=${summary.total_cash:,.2f} "
        f"BuyPow=${summary.buying_power:,.2f}"
    )
    return summary


def _parse_float(value: str | None) -> float:
    """Parse a string value to float, returning 0.0 on failure."""
    if value is None:
        return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0
