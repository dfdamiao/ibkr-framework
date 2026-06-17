"""
Contract factory and tick size resolution.

Reference: the IBKR TWS API documentation Section 3

CRITICAL: Uses reqMarketRule for dynamic tick sizes per contract.
The old system hardcoded 0.05 for all EU exchanges — wrong.
Different exchanges/price ranges use different tick steps (0.01, 0.02, 0.05).
"""

import logging
from decimal import Decimal, ROUND_HALF_UP
from ibapi.contract import Contract

from ibkr.config.exchanges import (
    EU_EXCHANGES,
    DEFAULT_TICK_SIZE_US,
    DEFAULT_TICK_SIZE_EU,
)

logger = logging.getLogger(__name__)


# --- Manual conId fallback for symbols that fail reqContractDetails ---
MANUAL_CONIDS: dict[str, int] = {
    "CPER": 97462781,
    "DBC": 319355208,
    "GLD": 11801,
    "SPY": 756733,
    "EWZ": 41439381,
    "XLE": 265598,
    "IWM": 9579970,
    "QQQ": 320227571,
    "TLT": 15547841,
    "EFA": 14094,
    "VWO": 27684070,
    "HYG": 43645865,
    "LQD": 5765379,
    "EMB": 97907162,
}


def build_stock(
    symbol: str,
    exchange: str = "SMART",
    currency: str = "USD",
    con_id: int | None = None,
) -> Contract:
    """Build a stock contract.

    Args:
        symbol: Ticker symbol (e.g. "SPY", "CMOD.L")
        exchange: Exchange (default SMART for best routing)
        currency: Trading currency
        con_id: Optional explicit contract ID
    """
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.exchange = exchange
    contract.currency = currency
    if con_id is not None:
        contract.conId = con_id
    return contract


def _csv_str(value, default: str = "") -> str:
    """Coerce a pandas-parsed CSV cell to a clean str.

    Empty CSV cells read back as float NaN, which is *truthy* — handing that to
    the ibapi proto setter (`contractProto.symbol = ...`) raises "bad argument
    type for built-in operation". Returns ``default`` for None / NaN / blank.
    """
    if value is None:
        return default
    s = str(value).strip()
    return default if s == "" or s.lower() == "nan" else s


def from_csv_row(row: dict) -> Contract:
    """Build a contract from a contract_mapping.csv row.

    Expected columns: ibkr_symbol, conId, exchange, currency, sec_type.
    conId-only rows (blank ibkr_symbol) are valid — IBKR places by conId and the
    proto layer skips an empty symbol; a NaN symbol, however, would crash it.
    """
    contract = Contract()
    contract.symbol = _csv_str(row.get("ibkr_symbol")) or _csv_str(row.get("symbol"))
    _cid = _csv_str(row.get("conId"))
    contract.conId = int(float(_cid)) if _cid else 0
    contract.exchange = _csv_str(row.get("exchange"), "SMART")
    contract.currency = _csv_str(row.get("currency"), "USD")
    contract.secType = _csv_str(row.get("sec_type"), "STK")
    return contract


def from_conid(con_id: int, exchange: str = "SMART") -> Contract:
    """Build a minimal contract from conId only (for order placement)."""
    contract = Contract()
    contract.conId = con_id
    contract.exchange = exchange
    return contract


# Cache: (symbol, exchange, currency) -> conId (avoids repeated reqContractDetails
# round-trips when the same symbol is resolved by multiple callers in one session)
_resolved_conid_cache: dict[tuple[str, str, str], int] = {}


def resolve_conid(conn, contract: Contract, timeout: float = 10.0) -> int:
    """Resolve a contract's conId via reqContractDetails.

    Falls back to MANUAL_CONIDS if API lookup fails.
    Returns the conId, or 0 if unresolvable.
    """
    if contract.conId > 0:
        return contract.conId

    # conId is 0 — try manual fallback (faster, no API call)
    if contract.symbol in MANUAL_CONIDS:
        con_id = MANUAL_CONIDS[contract.symbol]
        logger.debug(f"Using manual conId for {contract.symbol}: {con_id}")
        return con_id

    # Session cache: skip the wire if we already resolved this symbol
    cache_key = (
        contract.symbol,
        contract.exchange or "SMART",
        contract.currency or "USD",
    )
    if cache_key in _resolved_conid_cache:
        return _resolved_conid_cache[cache_key]

    # API lookup
    details = conn.get_contract_details_sync(contract, timeout=timeout)
    if details:
        resolved_id = details[0].contract.conId
        _resolved_conid_cache[cache_key] = resolved_id
        logger.debug(f"Resolved {contract.symbol} -> conId {resolved_id}")
        return resolved_id

    logger.warning(f"Could not resolve conId for {contract.symbol}")
    return 0


# ------------------------------------------------------------------
# Tick size resolution via reqMarketRule
# ------------------------------------------------------------------

# Cache: conId -> tick size table [(price_threshold, tick_increment), ...]
_tick_size_cache: dict[int, list[tuple[float, float]]] = {}


def get_tick_table(
    conn, contract: Contract, timeout: float = 10.0
) -> list[tuple[float, float]]:
    """Get the tick size table for a contract via reqMarketRule.

    Returns a list of (price_threshold, tick_increment) tuples, sorted
    by price threshold ascending.

    The tick size for a given price is the increment where the price
    is >= the threshold. Walk the list from the end to find the match.

    Caches per conId (tick tables don't change intra-day).
    """
    if contract.conId in _tick_size_cache:
        return _tick_size_cache[contract.conId]

    # Need contract details to get marketRuleIds
    details = conn.get_contract_details_sync(contract, timeout=timeout)
    if not details:
        logger.warning(
            f"No contract details for {contract.symbol} "
            f"(conId={contract.conId}) — using default tick"
        )
        return []

    cd = details[0]
    market_rule_ids = cd.marketRuleIds
    if not market_rule_ids:
        logger.warning(
            f"No marketRuleIds for {contract.symbol} — using default tick"
        )
        return []

    # marketRuleIds can be two formats:
    #   Format A: "SMART:239,SBF:55,IBIS:1" (exchange:ruleId pairs)
    #   Format B: "1908,1908,98,3048,98" (just rule IDs, no exchange)
    # Format B is common — rule IDs correspond to validExchanges order.
    logger.debug(f"marketRuleIds for {contract.symbol}: {market_rule_ids}")

    # Parse unique rule IDs
    unique_rule_ids = []
    seen = set()
    for entry in market_rule_ids.split(","):
        entry = entry.strip()
        # Handle both "exchange:ruleId" and plain "ruleId" formats
        rid_str = entry.split(":")[-1] if ":" in entry else entry
        try:
            rid = int(rid_str)
            if rid not in seen:
                unique_rule_ids.append(rid)
                seen.add(rid)
        except ValueError:
            pass

    if not unique_rule_ids:
        logger.warning(f"No parseable market rule IDs for {contract.symbol}")
        return []

    # Try each unique rule ID — pick the one with the COARSEST tick
    # at our price level (SMART rule has finest ticks, exchange rules
    # have the actual required minimum)
    best_table = []
    best_rule = None
    for rid in unique_rule_ids:
        table = conn.get_market_rule_sync(rid, timeout=timeout)
        if not table:
            continue
        # Check tick at a reference price (e.g. 50.0)
        ref_tick = table[0][1]
        for threshold, increment in table:
            if 50.0 >= threshold:
                ref_tick = increment
        # Pick the coarsest table (highest tick at reference price)
        # This is the exchange-specific requirement, not SMART's fine ticks
        if not best_table:
            best_table = table
            best_rule = rid
        else:
            best_ref = best_table[0][1]
            for threshold, increment in best_table:
                if 50.0 >= threshold:
                    best_ref = increment
            if ref_tick > best_ref:
                best_table = table
                best_rule = rid

    if best_table:
        _tick_size_cache[contract.conId] = best_table
        logger.debug(
            f"Tick table for {contract.symbol} (rule {best_rule}, "
            f"{len(unique_rule_ids)} unique rules tried): {best_table}"
        )
    else:
        logger.warning(f"All market rules returned empty for {contract.symbol}")

    return best_table


def get_tick_size(
    conn,
    contract: Contract,
    price: float,
    timeout: float = 10.0,
) -> float:
    """Get the correct tick size for a contract at a given price level.

    Uses reqMarketRule for dynamic resolution. Falls back to defaults.

    This is the CRITICAL fix: the old system hardcoded 0.05 for ALL EU
    exchanges. STK.PA needs 0.02 at certain price levels, not 0.05.
    """
    tick_table = get_tick_table(conn, contract, timeout=timeout)

    if tick_table:
        # Walk the table: find the last entry where price >= threshold
        tick = tick_table[0][1]  # Default to first increment
        for threshold, increment in tick_table:
            if price >= threshold:
                tick = increment
            else:
                break
        return tick

    # Fallback when reqMarketRule unavailable
    exchange = contract.exchange or ""
    if exchange in EU_EXCHANGES:
        return DEFAULT_TICK_SIZE_EU
    return DEFAULT_TICK_SIZE_US


def round_to_tick(price: float, tick_size: float) -> float:
    """Round a price to the nearest valid tick increment.

    Uses Decimal arithmetic for precision (avoids floating point errors).

    Example:
        round_to_tick(88.03, 0.05) -> 88.05
        round_to_tick(88.03, 0.02) -> 88.04
        round_to_tick(88.03, 0.01) -> 88.03
    """
    if tick_size <= 0:
        return round(price, 2)

    d_price = Decimal(str(price))
    d_tick = Decimal(str(tick_size))
    rounded = (d_price / d_tick).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP
    ) * d_tick
    return float(rounded)


def clear_tick_cache() -> None:
    """Clear the tick size cache (for testing or session reset)."""
    _tick_size_cache.clear()


def clear_conid_cache() -> None:
    """Clear the resolved conId cache (for testing or session reset)."""
    _resolved_conid_cache.clear()
