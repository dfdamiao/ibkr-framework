"""Position filter + symbol lookup tests for ibkr.account.positions.

The functional API wraps `ConnectionManager.get_positions_sync()` and
transforms the raw dict into typed PositionData objects. These tests verify:

  - PositionData properties (long/short detection, market value)
  - get_positions transforms the raw dict
  - get_active_positions filters out zero-quantity entries
  - get_position_by_symbol matches the IBKR `symbol` field

No TWS connection — Mock ConnectionManager.get_positions_sync.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


def test_position_data_long_short_classification():
    """is_long / is_short / market_value_estimate behave per sign of quantity."""
    from ibkr.account.positions import PositionData

    long_pos = PositionData(
        account="DU123", symbol="SPY", sec_type="STK", exchange="SMART",
        currency="USD", con_id=756733, quantity=10.0, avg_cost=400.00,
    )
    short_pos = PositionData(
        account="DU123", symbol="QQQ", sec_type="STK", exchange="SMART",
        currency="USD", con_id=320227571, quantity=-5.0, avg_cost=380.00,
    )

    assert long_pos.is_long is True
    assert long_pos.is_short is False
    assert long_pos.market_value_estimate == pytest.approx(4000.00)

    assert short_pos.is_long is False
    assert short_pos.is_short is True
    assert short_pos.market_value_estimate == pytest.approx(1900.00)  # |-5| × 380


def test_get_positions_transforms_raw_dict():
    """get_positions reads the raw dict from get_positions_sync and emits typed objects."""
    from ibkr.account.positions import PositionData, get_positions

    conn = MagicMock()
    conn.get_positions_sync.return_value = {
        756733: {
            "account": "DU123", "symbol": "SPY", "secType": "STK",
            "exchange": "SMART", "currency": "USD",
            "position": 10.0, "avgCost": 400.00,
        },
        320227571: {
            "account": "DU123", "symbol": "QQQ", "secType": "STK",
            "exchange": "SMART", "currency": "USD",
            "position": -5.0, "avgCost": 380.00,
        },
    }

    positions = get_positions(conn, timeout=1.0)

    assert len(positions) == 2
    assert isinstance(positions[756733], PositionData)
    assert positions[756733].symbol == "SPY"
    assert positions[756733].quantity == 10.0
    assert positions[320227571].symbol == "QQQ"
    assert positions[320227571].quantity == -5.0


def test_get_active_positions_filters_zero_qty():
    """get_active_positions drops rows where quantity == 0 (closed but still cached)."""
    from ibkr.account.positions import get_active_positions

    conn = MagicMock()
    conn.get_positions_sync.return_value = {
        756733: {"account": "DU123", "symbol": "SPY", "secType": "STK",
                 "exchange": "SMART", "currency": "USD",
                 "position": 10.0, "avgCost": 400.00},
        320227571: {"account": "DU123", "symbol": "QQQ", "secType": "STK",
                    "exchange": "SMART", "currency": "USD",
                    "position": 0.0, "avgCost": 380.00},   # closed but cached
        4391: {"account": "DU123", "symbol": "RY", "secType": "STK",
               "exchange": "TSE", "currency": "CAD",
               "position": 50.0, "avgCost": 165.00},
    }

    active = get_active_positions(conn, timeout=1.0)

    assert len(active) == 2
    assert 756733 in active
    assert 4391 in active
    assert 320227571 not in active  # filtered out


def test_get_position_by_symbol_matches_ibkr_symbol():
    """get_position_by_symbol matches the IBKR `symbol` field, NOT conId.

    Critical for symbol mapping: a CSV ticker like "RY.TO" does NOT match
    the IBKR symbol "RY". This test pins the matcher to the IBKR symbol so
    upstream code knows it must do its own ticker → IBKR-symbol mapping
    via contract_mapping.csv.
    """
    from ibkr.account.positions import get_position_by_symbol

    conn = MagicMock()
    conn.get_positions_sync.return_value = {
        4391: {"account": "DU123", "symbol": "RY", "secType": "STK",
               "exchange": "TSE", "currency": "CAD",
               "position": 50.0, "avgCost": 165.00},
        756733: {"account": "DU123", "symbol": "SPY", "secType": "STK",
                 "exchange": "SMART", "currency": "USD",
                 "position": 10.0, "avgCost": 400.00},
    }

    # Match: bare IBKR symbol
    found = get_position_by_symbol(conn, symbol="RY", timeout=1.0)
    assert found is not None
    assert found.con_id == 4391
    assert found.symbol == "RY"

    # No match: CSV-style ticker with exchange suffix
    not_found = get_position_by_symbol(conn, symbol="RY.TO", timeout=1.0)
    assert not_found is None

    # No match: nonexistent symbol
    none = get_position_by_symbol(conn, symbol="XYZ", timeout=1.0)
    assert none is None
