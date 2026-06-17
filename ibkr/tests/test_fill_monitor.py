"""FillMonitor state-machine tests.

Phase 3 of the executor pipeline polls `conn.orders[oid]['status']` until
each order reaches a terminal state. This pins down the four terminal
transitions:

  - "Filled"       → FillResult(status="Filled", avg_price, filled_qty, commission)
  - "Cancelled"    → FillResult(status="Cancelled")  (also Inactive, ApiCancelled)
  - error attached → FillResult(status="Rejected")
  - timeout reached:
        filled > 0  → FillResult(status="PartialFill")  (EU overnight pattern)
        filled == 0 → FillResult(status="Timeout")

No TWS connection — uses MagicMock for ConnectionManager. Tests use a 0.05s
poll interval and short timeouts so the suite stays fast.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest


def _make_conn(orders: dict[int, dict], errors: dict[int, list] | None = None):
    """Build a MagicMock that behaves like ConnectionManager for the monitor."""
    conn = MagicMock()
    conn.orders = orders
    conn.order_errors = errors or {}
    conn.has_order_errors.side_effect = lambda oid: oid in (errors or {})
    conn.get_commission_for_order.return_value = 1.23
    return conn


def test_filled_order_returns_fillresult_with_avg_price_and_qty():
    from ibkr.execution.fill_monitor import FillMonitor

    orders = {
        1001: {
            "status": "Filled",
            "avgFillPrice": 405.50,
            "filled": 10.0,
            "permId": 999888,
            "symbol": "SPY",
        },
    }
    conn = _make_conn(orders)
    monitor = FillMonitor(conn, timeout=1.0, poll_interval=0.05)

    results = monitor.monitor_all([1001])

    assert 1001 in results
    r = results[1001]
    assert r.status == "Filled"
    assert r.is_filled is True
    assert r.avg_price == pytest.approx(405.50)
    assert r.filled_qty == pytest.approx(10.0)
    assert r.commission == pytest.approx(1.23)
    assert r.perm_id == 999888


def test_cancelled_order_returns_cancelled_result():
    """STP cancel during exit → status=Cancelled is terminal."""
    from ibkr.execution.fill_monitor import FillMonitor

    orders = {1002: {"status": "Cancelled", "permId": 777}}
    conn = _make_conn(orders)
    monitor = FillMonitor(conn, timeout=1.0, poll_interval=0.05)

    results = monitor.monitor_all([1002])

    assert results[1002].status == "Cancelled"
    assert results[1002].perm_id == 777
    assert results[1002].is_filled is False


@pytest.mark.parametrize("status", ["Cancelled", "Inactive", "ApiCancelled"])
def test_all_cancel_variants_are_terminal(status):
    from ibkr.execution.fill_monitor import FillMonitor

    orders = {1003: {"status": status, "permId": 1}}
    conn = _make_conn(orders)
    monitor = FillMonitor(conn, timeout=1.0, poll_interval=0.05)

    results = monitor.monitor_all([1003])

    assert results[1003].status == status


def test_order_with_errors_marked_rejected():
    """has_order_errors(oid) → Rejected, regardless of status field."""
    from ibkr.execution.fill_monitor import FillMonitor

    orders = {1004: {"status": "Submitted", "permId": 42}}
    errors = {1004: [{"code": 201, "msg": "Order rejected - reason: margin"}]}
    conn = _make_conn(orders, errors)
    monitor = FillMonitor(conn, timeout=1.0, poll_interval=0.05)

    results = monitor.monitor_all([1004])

    assert results[1004].status == "Rejected"
    assert results[1004].perm_id == 42


def test_partial_fill_on_timeout_when_filled_qty_positive():
    """EU overnight pattern: order stays PreSubmitted past timeout but already
    filled some quantity → PartialFill (recoverable via resolve_pending_fills)."""
    from ibkr.execution.fill_monitor import FillMonitor

    orders = {
        1005: {
            "status": "PreSubmitted",
            "filled": 7.0,         # partial
            "remaining": 3.0,
            "avgFillPrice": 99.50,
            "permId": 555,
        },
    }
    conn = _make_conn(orders)
    # Short timeout so the test doesn't actually wait 600s
    monitor = FillMonitor(conn, timeout=0.2, poll_interval=0.05)

    results = monitor.monitor_all([1005])

    assert results[1005].status == "PartialFill"
    assert results[1005].is_partial is True
    assert results[1005].filled_qty == pytest.approx(7.0)
    assert results[1005].avg_price == pytest.approx(99.50)


def test_timeout_when_no_fill():
    """Order stuck Submitted/PreSubmitted with zero fill at timeout → Timeout."""
    from ibkr.execution.fill_monitor import FillMonitor

    orders = {1006: {"status": "PreSubmitted", "filled": 0.0, "permId": 7}}
    conn = _make_conn(orders)
    monitor = FillMonitor(conn, timeout=0.2, poll_interval=0.05)

    results = monitor.monitor_all([1006])

    assert results[1006].status == "Timeout"
    assert results[1006].perm_id == 7


def test_empty_order_list_returns_empty_dict():
    """monitor_all([]) must short-circuit — never poll, never sleep."""
    from ibkr.execution.fill_monitor import FillMonitor

    conn = _make_conn({})
    monitor = FillMonitor(conn, timeout=1.0, poll_interval=0.05)

    assert monitor.monitor_all([]) == {}


def test_filled_waits_briefly_for_async_commission():
    """REGRESSION: orderStatus=Filled fires BEFORE commissionReport.

    Reading commission immediately on Filled returned 0 even though IBKR
    actually charged $1 (live AAPL test 2026-04-29). FillMonitor must
    poll briefly for the async commissionReport before snapshotting.
    """
    from ibkr.execution.fill_monitor import FillMonitor

    orders = {
        1001: {"status": "Filled", "avgFillPrice": 269.67, "filled": 1.0,
               "permId": 7},
    }
    conn = MagicMock()
    conn.orders = orders
    conn.order_errors = {}
    conn.has_order_errors.return_value = False

    # Simulate async commissionReport arrival: first 2 reads return 0.0,
    # third returns the real commission ($1.00).
    call_count = {"n": 0}

    def deferred_commission(_oid):
        call_count["n"] += 1
        return 1.00 if call_count["n"] >= 3 else 0.0

    conn.get_commission_for_order.side_effect = deferred_commission

    monitor = FillMonitor(conn, timeout=5.0, poll_interval=0.05)
    results = monitor.monitor_all([1001])

    assert results[1001].status == "Filled"
    assert results[1001].commission == pytest.approx(1.00)


def test_mixed_terminal_states_in_one_pass():
    """Three orders in different terminal states → all resolved in one batch."""
    from ibkr.execution.fill_monitor import FillMonitor

    orders = {
        2001: {
            "status": "Filled", "avgFillPrice": 100.0, "filled": 5.0,
            "permId": 1,
        },
        2002: {"status": "Cancelled", "permId": 2},
        2003: {"status": "Submitted", "permId": 3},  # has error → rejected
    }
    errors = {2003: [{"code": 202, "msg": "rejected"}]}
    conn = _make_conn(orders, errors)
    monitor = FillMonitor(conn, timeout=1.0, poll_interval=0.05)

    results = monitor.monitor_all([2001, 2002, 2003])

    assert results[2001].status == "Filled"
    assert results[2002].status == "Cancelled"
    assert results[2003].status == "Rejected"
