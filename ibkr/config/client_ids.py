"""Client IDs for IBKR API connections.

Every concurrent connection to TWS / IB Gateway needs its own unique integer
client ID (0-99 is a safe range). Assign one per process or strategy so their
orders and callbacks do not collide. The map below is only an example: replace
it with your own names.
"""

# name -> client_id. Pick any unique integers in 0..99.
CLIENT_IDS: dict[str, int] = {
    "default": 0,
    "data": 1,
    "execution": 2,
    "reporting": 3,
}


def get_client_id(name: str) -> int:
    """Return the client ID for a named connection (KeyError if unknown)."""
    return CLIENT_IDS[name]


def validate_client_id(client_id: int) -> bool:
    """True if client_id is within the safe 0-99 range."""
    return 0 <= client_id <= 99
