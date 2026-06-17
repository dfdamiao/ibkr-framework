"""Contract resolution cache tests.

`resolve_conid` looks up a contract's conId via `reqContractDetails` and now
caches by (symbol, exchange, currency) within a session so repeated callers
(executor + bracket builder + tick lookup) don't pay the round-trip twice.

Cache rules:
  - conId already set on the contract → return it (cache untouched, no API call)
  - conId == 0 + symbol in MANUAL_CONIDS → return manual mapping (no cache, no API)
  - conId == 0 + cache hit → return cached value (no API call)
  - conId == 0 + cache miss → call API, populate cache on success
  - failed lookup → return 0, do NOT poison the cache

No TWS connection — uses MagicMock for ConnectionManager.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def _clear_cache():
    """Each test starts with an empty cache."""
    from ibkr.core.contracts import clear_conid_cache
    clear_conid_cache()
    yield
    clear_conid_cache()


def _details(conid: int):
    """Build a fake ContractDetails-like object with .contract.conId."""
    inner = MagicMock()
    inner.contract.conId = conid
    return inner


def test_explicit_conid_skips_api_and_cache():
    """If contract.conId > 0, no API call and no cache write."""
    from ibkr.core.contracts import (
        _resolved_conid_cache,
        build_stock,
        resolve_conid,
    )

    conn = MagicMock()
    contract = build_stock("ACME", exchange="SMART", currency="USD", con_id=12345)

    result = resolve_conid(conn, contract)

    assert result == 12345
    conn.get_contract_details_sync.assert_not_called()
    assert _resolved_conid_cache == {}


def test_manual_fallback_skips_api_and_cache():
    """Symbols in MANUAL_CONIDS short-circuit before the cache + API."""
    from ibkr.core.contracts import (
        _resolved_conid_cache,
        build_stock,
        resolve_conid,
    )

    conn = MagicMock()
    spy = build_stock("SPY", exchange="SMART", currency="USD")

    result = resolve_conid(conn, spy)

    assert result == 756733
    conn.get_contract_details_sync.assert_not_called()
    assert _resolved_conid_cache == {}


def test_api_lookup_populates_cache_and_avoids_repeat_call():
    """First miss hits the wire, second call hits the cache."""
    from ibkr.core.contracts import (
        _resolved_conid_cache,
        build_stock,
        resolve_conid,
    )

    conn = MagicMock()
    conn.get_contract_details_sync.return_value = [_details(98765)]
    contract = build_stock("ZZZZ", exchange="ARCA", currency="USD")

    first = resolve_conid(conn, contract)
    second = resolve_conid(conn, contract)

    assert first == 98765
    assert second == 98765
    # Only ONE API call across both invocations
    assert conn.get_contract_details_sync.call_count == 1
    assert _resolved_conid_cache[("ZZZZ", "ARCA", "USD")] == 98765


def test_cache_keyed_by_symbol_exchange_currency():
    """Same symbol on different exchanges must be cached independently."""
    from ibkr.core.contracts import build_stock, resolve_conid

    conn = MagicMock()
    # First exchange
    conn.get_contract_details_sync.return_value = [_details(111)]
    a = resolve_conid(conn, build_stock("ZZZZ", exchange="ARCA", currency="USD"))

    # Same symbol, different exchange — cache miss → new API call
    conn.get_contract_details_sync.return_value = [_details(222)]
    b = resolve_conid(conn, build_stock("ZZZZ", exchange="IBIS", currency="EUR"))

    assert a == 111
    assert b == 222
    assert conn.get_contract_details_sync.call_count == 2


def test_failed_lookup_does_not_poison_cache():
    """When the API returns nothing (returns 0), a retry must hit the wire again."""
    from ibkr.core.contracts import (
        _resolved_conid_cache,
        build_stock,
        resolve_conid,
    )

    conn = MagicMock()
    conn.get_contract_details_sync.return_value = []  # no details
    contract = build_stock("ZZZZ", exchange="ARCA", currency="USD")

    first = resolve_conid(conn, contract)
    assert first == 0
    assert _resolved_conid_cache == {}

    # Retry: should call the API again, not return cached 0
    conn.get_contract_details_sync.return_value = [_details(333)]
    second = resolve_conid(conn, contract)
    assert second == 333
    assert conn.get_contract_details_sync.call_count == 2


def test_clear_conid_cache_empties_state():
    """clear_conid_cache() resets the module-level dict for test isolation."""
    from ibkr.core.contracts import (
        _resolved_conid_cache,
        build_stock,
        clear_conid_cache,
        resolve_conid,
    )

    conn = MagicMock()
    conn.get_contract_details_sync.return_value = [_details(555)]
    resolve_conid(conn, build_stock("ZZZZ", exchange="ARCA", currency="USD"))
    assert _resolved_conid_cache  # populated

    clear_conid_cache()
    assert _resolved_conid_cache == {}
