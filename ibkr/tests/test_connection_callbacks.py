"""Pure-callback tests for ConnectionManager.

No TWS connection — exercises the EWrapper callback methods directly to
verify state transitions for nextValidId, order ID assignment, openOrder
permId mapping, the orderStatus-before-openOrder race, error routing,
and the modern (protobuf v2) commission/execution callbacks.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest


@pytest.fixture
def conn():
    """Construct a ConnectionManager without ever calling connect()."""
    from ibkr.core.connection import ConnectionManager

    return ConnectionManager(host="127.0.0.1", port=7497)


def test_run_reader_swallows_benign_teardown_when_disconnecting(conn) -> None:
    """The reader thread must not surface ibapi's teardown race (serverVersion()
    becomes None after disconnect → `None >= int` TypeError) once we've begun
    disconnecting. This is the 'Exception in thread ibkr-reader' the health
    check printed on every run."""
    conn._disconnecting = True
    with patch.object(conn, "run",
                      side_effect=TypeError("'>=' not supported … NoneType")):
        conn._run_reader()  # must NOT raise


def test_run_reader_reraises_real_fault_mid_session(conn) -> None:
    """A reader fault while still connected (not disconnecting) propagates —
    we only suppress the intentional-teardown race, never live failures."""
    conn._disconnecting = False
    with patch.object(conn, "run", side_effect=TypeError("genuine decode bug")):
        with pytest.raises(TypeError):
            conn._run_reader()


def test_disconnect_sets_disconnecting_flag(conn) -> None:
    """disconnect() arms the teardown flag so the reader can classify the race."""
    conn._disconnecting = False
    with patch("ibapi.client.EClient.disconnect"):
        conn.disconnect()
    assert conn._disconnecting is True


def test_next_valid_id_sets_state_and_event(conn) -> None:
    """nextValidId callback stores the id and signals the connected event."""
    assert conn.nextorderId is None
    assert not conn._connected_event.is_set()

    conn.nextValidId(100)

    assert conn.nextorderId == 100
    assert conn._connected_event.is_set()


def test_next_order_id_increments(conn) -> None:
    """next_order_id() returns current value then increments — prevents Error 103."""
    conn.nextorderId = 50
    assert conn.next_order_id() == 50
    assert conn.next_order_id() == 51
    assert conn.next_order_id() == 52
    assert conn.nextorderId == 53


def test_next_order_id_raises_when_not_connected(conn) -> None:
    """next_order_id() before nextValidId() must raise — guards against silent 0/None."""
    with pytest.raises(RuntimeError, match="Not connected"):
        conn.next_order_id()


def test_open_order_populates_perm_id_map(conn) -> None:
    """openOrder callback writes permId → orderId mapping for cross-session STP cancel."""
    contract = SimpleNamespace(symbol="SPY", conId=756733)
    order = SimpleNamespace(permId=998877, orderId=42, action="BUY")
    state = SimpleNamespace(status="Submitted")

    conn.openOrder(orderId=42, contract=contract, order=order, orderState=state)

    assert conn.perm_id_map[998877] == 42
    assert conn.orders[42]["permId"] == 998877
    assert conn.orders[42]["symbol"] == "SPY"


def test_open_order_skips_zero_perm_id(conn) -> None:
    """permId=0 (not yet assigned by IBKR) should not enter the map."""
    contract = SimpleNamespace(symbol="QQQ", conId=320227571)
    order = SimpleNamespace(permId=0, orderId=10, action="BUY")
    state = SimpleNamespace(status="PendingSubmit")

    conn.openOrder(orderId=10, contract=contract, order=order, orderState=state)

    assert 0 not in conn.perm_id_map
    assert conn.orders[10]["permId"] == 0


def test_order_status_before_open_order_race(conn) -> None:
    """orderStatus can fire BEFORE openOrder. setdefault must keep both writes intact.

    Lesson learned (per connection.py L457): the race-prevention pattern uses
    `setdefault(orderId, {})` in both callbacks so neither clobbers the other.
    """
    # orderStatus arrives first
    conn.orderStatus(
        orderId=99,
        status="Filled",
        filled=10.0,
        remaining=0.0,
        avgFillPrice=400.50,
        permId=555111,
        parentId=0,
        lastFillPrice=400.50,
        clientId=5,
        whyHeld="",
        mktCapPrice=0.0,
    )
    assert conn.orders[99]["status"] == "Filled"
    assert conn.orders[99]["filled"] == 10.0

    # openOrder arrives second — must NOT erase the status fields
    contract = SimpleNamespace(symbol="SPY", conId=756733)
    order = SimpleNamespace(permId=555111, orderId=99, action="BUY")
    state = SimpleNamespace(status="Filled")
    conn.openOrder(orderId=99, contract=contract, order=order, orderState=state)

    assert conn.orders[99]["status"] == "Filled"
    assert conn.orders[99]["filled"] == 10.0
    assert conn.orders[99]["permId"] == 555111
    assert conn.perm_id_map[555111] == 99


def test_error_callback_marks_cancelled(conn) -> None:
    """error(reqId, ..., 202, ...) flips the order's status to Cancelled.

    Code 202 is the normal API cancel response.
    """
    conn.orders[77] = {"status": "Submitted", "permId": 1}

    conn.error(reqId=77, errorTime=0, errorCode=202, errorString="Order Canceled")

    assert conn.orders[77]["status"] == "Cancelled"


def test_error_callback_records_rejection(conn) -> None:
    """error() with a rejection code (e.g. 110 tick size) appends to order_errors."""
    conn.error(reqId=42, errorTime=0, errorCode=110,
               errorString="The price does not conform to the minimum tick size")

    assert 110 in conn.order_errors[42]


def test_classify_unknown_does_not_raise(conn) -> None:
    """Unmapped error codes flow through error() without exception."""
    # Just a smoke check — should not raise
    with patch.object(conn, "orders", {}):
        conn.error(reqId=-1, errorTime=0, errorCode=99999,
                   errorString="Synthetic unknown")


def test_order_status_coerces_string_filled_to_float(conn) -> None:
    """REGRESSION: ibapi protobuf v2 sends `filled` as decimal.Decimal, and
    some code paths emit empty string for not-yet-filled orders. Either
    type would crash downstream comparisons (filled > 0). Live L9 test on
    2026-04-29 hit `TypeError: '>' not supported between str and int`.
    The orderStatus handler must coerce to float at source.
    """
    conn.orderStatus(
        orderId=1247, status="PreSubmitted",
        filled="",          # empty string — was the actual crash trigger
        remaining="1",      # string from protobuf path
        avgFillPrice=0.0,
        permId=12345, parentId=0, lastFillPrice=0.0, clientId=98,
        whyHeld="", mktCapPrice=0.0,
    )

    o = conn.orders[1247]
    assert isinstance(o["filled"], float)
    assert o["filled"] == 0.0
    assert isinstance(o["remaining"], float)
    assert o["remaining"] == 1.0
    # Comparison must work — this is what crashed in production
    assert (o["filled"] > 0) is False  # i.e. 0.0 > 0 == False


def test_order_status_coerces_decimal_filled_to_float(conn) -> None:
    """orderStatus handler must accept decimal.Decimal (the modern
    EWrapper type for `filled` and `remaining`) and store as float."""
    from decimal import Decimal

    conn.orderStatus(
        orderId=1248, status="Filled",
        filled=Decimal("1"),
        remaining=Decimal("0"),
        avgFillPrice=269.41,
        permId=999, parentId=0, lastFillPrice=269.41, clientId=98,
        whyHeld="", mktCapPrice=0.0,
    )

    o = conn.orders[1248]
    assert o["filled"] == 1.0
    assert isinstance(o["filled"], float)


def test_open_order_proto_buf_populates_perm_id_map(conn) -> None:
    """REGRESSION: ibapi protobuf v2 dispatches openOrderProtoBuf for bracket
    child orders submitted in the same session — the legacy openOrder is
    NOT called for them. Without this handler, perm_id_map never gets
    the STP child's permId, and BracketBuilder.cancel_stop_order(0)
    silently no-ops, leaving an orphan STP at IBKR.

    Live L9 test on 2026-04-29 caught this — orphan STP left behind even
    though the script reported PASS on most checks.
    """
    contract = SimpleNamespace(symbol="AAPL")
    order = SimpleNamespace(permId=555111, orderId=1252, action="SELL")
    state = SimpleNamespace(status="PreSubmitted")
    proto = SimpleNamespace(
        orderId=1252, contract=contract, order=order, orderState=state
    )

    conn.openOrderProtoBuf(proto)

    assert conn.orders[1252]["permId"] == 555111
    assert conn.orders[1252]["symbol"] == "AAPL"
    assert conn.perm_id_map[555111] == 1252


def test_order_status_proto_buf_coerces_and_maps(conn) -> None:
    """orderStatusProtoBuf must coerce decimal/string fields to float and
    populate perm_id_map (race-condition fix from legacy orderStatus
    must apply on the protobuf path too).
    """
    proto = SimpleNamespace(
        orderId=1252, status="PreSubmitted",
        filled="",                   # empty string from protobuf
        remaining="1",               # string
        avgFillPrice=0.0,
        permId=555111,
        parentId=1251,
        lastFillPrice=0.0,
        clientId=98,
    )

    conn.orderStatusProtoBuf(proto)

    o = conn.orders[1252]
    assert isinstance(o["filled"], float)
    assert o["filled"] == 0.0
    assert isinstance(o["remaining"], float)
    assert o["remaining"] == 1.0
    assert conn.perm_id_map[555111] == 1252


# --- Modern (protobuf v2) execution + commission callbacks -------------------

def test_commission_and_fees_report_populates_commissions(conn) -> None:
    """commissionAndFeesReport (modern callback name) writes to self.commissions
    using the renamed `commissionAndFees` field.

    REGRESSION: ibapi >= protobuf v2 dispatches this callback INSTEAD of the
    legacy `commissionReport`. Live AAPL test on 2026-04-29 showed our old
    handler never fired, so executor logged $0.00 commissions even though
    IBKR charged $1. Pinning the new handler here.
    """
    report = SimpleNamespace(
        execId="exec-abc-1",
        commissionAndFees=1.25,
        currency="USD",
    )

    conn.commissionAndFeesReport(report)

    assert conn.commissions["exec-abc-1"] == pytest.approx(1.25)


def test_commission_and_fees_report_filters_unset_max_double(conn) -> None:
    """IBKR sends sys.float_info.max (≈1.79e308) as the 'unset' sentinel.
    Must be ignored — otherwise commission would corrupt downstream P&L.
    """
    report = SimpleNamespace(
        execId="exec-unset",
        commissionAndFees=1e309,  # > 1e300 sentinel threshold
        currency="USD",
    )

    conn.commissionAndFeesReport(report)

    assert "exec-unset" not in conn.commissions


def test_execution_details_protobuf_populates_executions(conn) -> None:
    """executionDetailsProtoBuf (modern callback) writes execution data to
    self.executions, keyed by execId. get_commission_for_order(orderId) walks
    self.executions to find matching execIds → looks up commission. If this
    callback never populates the dict, commissions are unreachable.
    """
    contract = SimpleNamespace(symbol="AAPL")
    execution = SimpleNamespace(
        execId="exec-aapl-1",
        side="BOT",
        shares=1.0,
        price=269.41,
        orderId=1244,
        permId=999111,
        time="20260429-10:42:00",
        exchange="NASDAQ",
        cumQty=1.0,
        avgPrice=269.41,
    )
    proto = SimpleNamespace(contract=contract, execution=execution)

    conn.executionDetailsProtoBuf(proto)

    assert "exec-aapl-1" in conn.executions
    rec = conn.executions["exec-aapl-1"]
    assert rec["symbol"] == "AAPL"
    assert rec["orderId"] == 1244
    assert rec["price"] == pytest.approx(269.41)
    assert rec["shares"] == pytest.approx(1.0)


def test_get_commission_for_order_sums_protobuf_path(conn) -> None:
    """End-to-end: protobuf execution + AndFees callbacks land in conn,
    get_commission_for_order(orderId) returns the matching sum.
    """
    # First execution: BUY 1 share, commission $1.00
    proto = SimpleNamespace(
        contract=SimpleNamespace(symbol="AAPL"),
        execution=SimpleNamespace(
            execId="e1", side="BOT", shares=1.0, price=270.0,
            orderId=1244, permId=1, time="t", exchange="NASDAQ",
            cumQty=1.0, avgPrice=270.0,
        ),
    )
    conn.executionDetailsProtoBuf(proto)
    conn.commissionAndFeesReport(
        SimpleNamespace(execId="e1", commissionAndFees=1.00, currency="USD")
    )

    assert conn.get_commission_for_order(1244) == pytest.approx(1.00)
    # Other order — should sum to 0
    assert conn.get_commission_for_order(9999) == 0.0


# --- reqExecutions sync pull + conId capture (verify_and_resolve enabler) ----
def test_exec_details_stores_conid(conn) -> None:
    """Legacy execDetails must store conId so verify_and_resolve can match
    fills by conId (immune to the symbol collision that caused the MTE bug)."""
    contract = SimpleNamespace(symbol="MTE1", conId=297475414)
    execution = SimpleNamespace(
        execId="x1", side="BOT", shares=31.0, price=202.4, orderId=7,
        permId=42, time="t", exchange="SBF", cumQty=31.0, avgPrice=202.4,
    )
    conn.execDetails(1, contract, execution)
    assert conn.executions["x1"]["conId"] == 297475414


def test_exec_details_protobuf_stores_conid(conn) -> None:
    proto = SimpleNamespace(
        contract=SimpleNamespace(symbol="MTE1", conId=297475414),
        execution=SimpleNamespace(
            execId="x2", side="BOT", shares=31.0, price=202.4, orderId=7,
            permId=42, time="t", exchange="SBF", cumQty=31.0, avgPrice=202.4,
        ),
    )
    conn.executionDetailsProtoBuf(proto)
    assert conn.executions["x2"]["conId"] == 297475414


def test_exec_details_tolerates_missing_conid(conn) -> None:
    """A fake/partial contract without conId must not crash the callback."""
    contract = SimpleNamespace(symbol="AAPL")  # no conId
    execution = SimpleNamespace(
        execId="x3", side="BOT", shares=1.0, price=1.0, orderId=1,
        permId=1, time="t", exchange="X", cumQty=1.0, avgPrice=1.0,
    )
    conn.execDetails(1, contract, execution)
    assert conn.executions["x3"]["conId"] == 0


def test_exec_details_end_sets_event(conn) -> None:
    conn._exec_event.clear()
    conn.execDetailsEnd(1)
    assert conn._exec_event.is_set()


def test_get_executions_sync_returns_collected(conn, monkeypatch) -> None:
    """get_executions_sync clears, requests, waits for end, returns the dict."""
    def fake_req(req_id, _filter):
        conn.executionDetailsProtoBuf(SimpleNamespace(
            contract=SimpleNamespace(symbol="SPY", conId=756733),
            execution=SimpleNamespace(
                execId="s1", side="BOT", shares=1.0, price=400.0, orderId=5,
                permId=9, time="t", exchange="SMART", cumQty=1.0, avgPrice=400.0),
        ))
        conn.execDetailsEnd(req_id)

    monkeypatch.setattr(conn, "next_order_id", lambda: 1)
    monkeypatch.setattr(conn, "reqExecutions", fake_req)
    out = conn.get_executions_sync(timeout=2.0)
    assert "s1" in out and out["s1"]["conId"] == 756733
