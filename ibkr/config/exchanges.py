"""
Exchange routing rules and tick size configuration.

Reference: the IBKR TWS API documentation Section 3, CLAUDE.md quirks
"""

# --- Exchanges that REQUIRE SMART routing ---
# Direct routing fails for these symbols; ADAPTIVE algo needs SMART
SMART_ROUTING_REQUIRED: frozenset[str] = frozenset({
    "AQWA", "ELD", "KRBN", "CMOD.L",
})

# --- European exchanges known to have non-0.01 tick sizes ---
# NOTE: ibkr uses reqMarketRule for DYNAMIC tick sizes per contract.
# This set is kept as a reference/fallback only.
EU_EXCHANGES: frozenset[str] = frozenset({
    "SBF",        # Euronext Paris
    "AEB",        # Euronext Amsterdam
    "IBIS",       # Deutsche Börse (Frankfurt)
    "IBIS2",      # Deutsche Börse secondary
    "BVME",       # Borsa Italiana
    "BVME.ETF",   # Borsa Italiana ETF segment
    "VSE",        # Vienna Stock Exchange
    "SWB",        # Stuttgart
    "FWB",        # Frankfurt
    "LSEETF",     # London Stock Exchange ETF
    "LSE",        # London Stock Exchange
})

# --- Currency by exchange ---
EXCHANGE_CURRENCY: dict[str, str] = {
    "SBF": "EUR",
    "AEB": "EUR",
    "IBIS": "EUR",
    "IBIS2": "EUR",
    "BVME": "EUR",
    "BVME.ETF": "EUR",
    "VSE": "EUR",
    "SWB": "EUR",
    "FWB": "EUR",
    "LSEETF": "GBP",
    "LSE": "GBP",
    "SMART": "USD",  # Default for US
}

# --- Default tick size when reqMarketRule unavailable ---
# US equities: 0.01, EU equities: varies (use reqMarketRule!)
DEFAULT_TICK_SIZE_US = 0.01
DEFAULT_TICK_SIZE_EU = 0.05  # Fallback only — reqMarketRule is preferred

# --- Ports ---
PAPER_PORT = 7497
LIVE_PORT = 7496
GATEWAY_PAPER_PORT = 4002
GATEWAY_LIVE_PORT = 4001

DEFAULT_HOST = "127.0.0.1"
