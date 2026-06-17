"""
Error classification for IBKR TWS API error codes.

Reference: the IBKR TWS API documentation Section 15
"""

from enum import Enum


class ErrorLevel(Enum):
    """Classification of IBKR error severity."""

    INFO = "info"  # Farm connection OK, status messages
    WARNING = "warning"  # Farm inactive, data stale
    REJECT = "reject"  # Order rejected (may retry with fallback)
    CRITICAL = "critical"  # Connection lost, duplicate ID
    CANCEL = "cancel"  # Order cancelled (normal response)
    UNKNOWN = "unknown"


# --- Info codes: suppress or log at DEBUG ---
INFO_CODES: frozenset[int] = frozenset({
    2104,  # Market data farm connection is OK
    2106,  # HMDS data farm connection is OK
    2158,  # Sec-def data farm connection is OK
    2119,  # Market data farm is connecting
})

# --- Warning codes: log at WARNING ---
WARNING_CODES: frozenset[int] = frozenset({
    2107,  # HMDS data farm connection is inactive
    2108,  # Market data farm connection is inactive
    399,   # Order message warning
    10167,  # Displayed price not updated (stale data)
})

# --- Order rejection codes: trigger fallback to next order type ---
ORDER_REJECTION_CODES: frozenset[int] = frozenset({
    110,   # Price does not conform to minimum tick
    201,   # Order rejected (KID document, compliance, etc.)
    321,   # MIDPRICE not supported outside RTH
    354,   # No contract matching request / not subscribed
    387,   # Unsupported execution mode
    442,   # Algo not allowed on this exchange
    4110,  # PRIIPs KID not available
})

# --- Connection/critical codes: may need reconnect or abort ---
CRITICAL_CODES: frozenset[int] = frozenset({
    103,   # Duplicate order ID
    326,   # Client ID already in use
    327,   # Negative client ID
    502,   # Can't connect to TWS
    504,   # Not connected
    509,   # Exception reading socket
    1100,  # Connectivity broken
    10147,  # OrderId already in use
    10148,  # OrderId not current
})

# --- Normal cancel response ---
CANCEL_CODES: frozenset[int] = frozenset({
    202,   # Order cancelled — API client request (normal)
})

# --- Codes that are safe to retry with a different order type ---
RETRYABLE_CODES: frozenset[int] = frozenset({
    110,   # Wrong tick size → round price differently
    321,   # MIDPRICE outside RTH → use ADAPTIVE MKT
    442,   # Algo not allowed → switch to REL or direct routing
})

# --- Connectivity restored codes ---
RESTORED_CODES: frozenset[int] = frozenset({
    1101,  # Connectivity restored, data lost (re-request)
    1102,  # Connectivity restored, data maintained (resume)
})


def classify(error_code: int) -> ErrorLevel:
    """Classify an IBKR error code into a severity level."""
    if error_code in INFO_CODES:
        return ErrorLevel.INFO
    if error_code in WARNING_CODES:
        return ErrorLevel.WARNING
    if error_code in ORDER_REJECTION_CODES:
        return ErrorLevel.REJECT
    if error_code in CRITICAL_CODES:
        return ErrorLevel.CRITICAL
    if error_code in CANCEL_CODES:
        return ErrorLevel.CANCEL
    if error_code in RESTORED_CODES:
        return ErrorLevel.INFO
    return ErrorLevel.UNKNOWN


def is_rejection(error_code: int) -> bool:
    """Check if error code indicates an order rejection (should fallback)."""
    return error_code in ORDER_REJECTION_CODES


def is_info(error_code: int) -> bool:
    """Check if error code is purely informational (suppress or debug)."""
    return error_code in INFO_CODES or error_code in RESTORED_CODES


def is_critical(error_code: int) -> bool:
    """Check if error code requires reconnection or abort."""
    return error_code in CRITICAL_CODES


def should_retry(error_code: int) -> bool:
    """Check if error code is retryable with a different order type."""
    return error_code in RETRYABLE_CODES


def is_duplicate_order_id(error_code: int) -> bool:
    """Check if error means we need a fresh nextValidId."""
    return error_code in (103, 10147, 10148)
