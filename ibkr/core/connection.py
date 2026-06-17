"""
IBKR TWS API Connection Manager.

Built from scratch using ibapi directly.
Reference: the IBKR TWS API documentation Sections 1-2, Appendix E

Design principles:
  1. EWrapper/EClient in ONE class
  2. threading.Event per request with timeout
  3. Client ID isolation
  4. permId for cross-session order resolution
  5. Threaded reader — EClient.run() in daemon thread
  6. Error classification via core.errors
"""

import logging
import socket
import threading
import time
from collections import defaultdict

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order_state import OrderState

from ibkr.core.errors import (
    classify,
    ErrorLevel,
    is_duplicate_order_id,
    ORDER_REJECTION_CODES,
)
from ibkr.config.exchanges import DEFAULT_HOST, PAPER_PORT

logger = logging.getLogger(__name__)

# Suppress verbose ibapi wire-protocol logging (SENDING/REQUEST/ANSWER dumps and
# the "...farm connection is OK" 2104/2106/2158 ANSWER blobs). Executors already
# quiet this via utils.logging_setup, but the orchestrator scripts use a bare
# logging.basicConfig(level=INFO) and don't — so set it here at the connection
# chokepoint, which every TWS-touching entry point imports.
logging.getLogger("ibapi").setLevel(logging.WARNING)
logging.getLogger("ibapi.client").setLevel(logging.WARNING)
logging.getLogger("ibapi.wrapper").setLevel(logging.WARNING)


class ConnectionManager(EWrapper, EClient):
    """
    Full-featured IBKR connection with order/position tracking.

    Usage:
        conn = ConnectionManager()
        if conn.connect_with_retry(client_id=5):
            # Place orders, request positions, etc.
            conn.disconnect_gracefully()

    Context manager:
        with ConnectionManager() as conn:
            conn.connect_with_retry(client_id=5)
            # Use connection
    """

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = PAPER_PORT,
    ):
        EClient.__init__(self, self)
        self.host = host
        self.port = port

        # Connection state
        self.connected = False
        self.nextorderId: int | None = None
        self.managed_accounts: str | None = None
        self.client_id: int | None = None

        # Order tracking
        # orderId -> {status, filled, remaining, avgFillPrice, permId, ...}
        self.orders: dict[int, dict] = {}
        # permId -> session orderId (for cross-session STP cancel resolution)
        self.perm_id_map: dict[int, int] = {}
        # orderId -> list of error codes (for rejection tracking)
        self.order_errors: dict[int, list[int]] = defaultdict(list)

        # Position tracking: conId -> position data dict
        self.positions: dict[int, dict] = {}

        # Account data
        self.account_summary: dict[str, str] = {}
        self.account_pnl: dict[str, float] = {}
        self.position_pnl: dict[int, dict] = {}

        # Portfolio updates (from reqAccountUpdates)
        self.portfolio: dict[int, dict] = {}

        # Execution tracking
        self.executions: dict[str, dict] = {}  # execId -> execution data
        self.commissions: dict[str, float] = {}  # execId -> commission

        # Completed orders (filled/cancelled from previous sessions)
        self.completed_orders: list[dict] = []

        # Synchronization events
        self._connected_event = threading.Event()
        self._positions_event = threading.Event()
        self._open_orders_event = threading.Event()
        self._account_summary_event = threading.Event()
        self._contract_details_event = threading.Event()
        self._market_rule_event = threading.Event()
        self._completed_orders_event = threading.Event()
        self._exec_event = threading.Event()

        # Contract details callback storage
        self._contract_details: list = []
        # Market rule callback storage
        self._market_rules: dict[int, list] = {}

        # Threading
        self._api_thread: threading.Thread | None = None
        # Serializes disconnect() so the daemon reader thread and the main
        # thread can't race ibapi's lock-less check-then-act on self.conn.
        self._disconnect_lock = threading.Lock()
        # True once we've intentionally begun tearing the connection down, so
        # the reader thread can tell a benign teardown race from a real fault.
        self._disconnecting = False

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect_with_retry(
        self,
        client_id: int,
        max_attempts: int = 3,
        timeout: float = 15.0,
    ) -> bool:
        """Connect to TWS with socket pre-check and retry.

        Args:
            client_id: Unique client ID for this connection.
            max_attempts: Number of connection attempts.
            timeout: Seconds to wait for nextValidId after connect.

        Returns:
            True if connected and ready (nextValidId received).
        """
        self.client_id = client_id

        # Socket pre-check — avoid hanging on connect()
        if not self._check_port():
            logger.error(
                f"TWS not listening on {self.host}:{self.port}. "
                "Is Trader Workstation running?"
            )
            return False

        for attempt in range(1, max_attempts + 1):
            try:
                self._connected_event.clear()
                self.nextorderId = None
                self._disconnecting = False

                logger.info(
                    f"Connecting to TWS (attempt {attempt}/{max_attempts}) "
                    f"host={self.host} port={self.port} clientId={client_id}"
                )

                self.connect(self.host, self.port, client_id)

                # Start reader thread (daemon so it dies with main thread)
                self._api_thread = threading.Thread(
                    target=self._run_reader, daemon=True, name="ibkr-reader"
                )
                self._api_thread.start()

                # Wait for nextValidId callback — signals "ready to trade"
                if self._connected_event.wait(timeout=timeout):
                    self.connected = True
                    logger.info(
                        f"Connected. nextValidId={self.nextorderId} "
                        f"account={self.managed_accounts}"
                    )
                    return True

                logger.warning(f"Attempt {attempt}: no nextValidId within {timeout}s")
                self._safe_disconnect()

            except Exception as e:
                logger.warning(f"Attempt {attempt} failed: {e}")
                self._safe_disconnect()

            if attempt < max_attempts:
                wait = 2 * attempt
                logger.info(f"Retrying in {wait}s...")
                time.sleep(wait)

        logger.error(f"Failed to connect after {max_attempts} attempts")
        return False

    def _run_reader(self) -> None:
        """Reader-thread target: run the EClient message loop, swallowing the
        benign teardown race where ibapi's run() touches serverVersion() / conn
        after disconnect() has reset them to None (raising TypeError on
        `None >= MIN_SERVER_VER_PROTOBUF`, or AttributeError on conn). The race
        is only suppressed once we've intentionally begun disconnecting
        (_disconnecting); a fault mid-session still propagates to the daemon
        thread's excepthook so real reader failures stay visible."""
        try:
            self.run()
        except (TypeError, AttributeError) as e:
            if not self._disconnecting:
                raise
            logger.debug(f"Reader thread teardown (benign): {e}")

    def disconnect_gracefully(self) -> None:
        """Clean disconnection with state cleanup."""
        if self.isConnected():
            logger.info("Disconnecting from TWS...")
            try:
                self.disconnect()
            except Exception as e:
                logger.warning(f"Disconnect error (non-fatal): {e}")
        self.connected = False
        self.client_id = None

    def _check_port(self) -> bool:
        """Check if TWS is listening on the configured port."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((self.host, self.port))
            sock.close()
            return result == 0
        except Exception:
            return False

    def _safe_disconnect(self) -> None:
        """Disconnect without raising exceptions."""
        self._disconnecting = True
        try:
            if self.isConnected():
                self.disconnect()
        except Exception:
            pass
        self.connected = False

    def disconnect(self) -> None:
        """Thread-safe override of EClient.disconnect().

        ibapi's disconnect() is a lock-less check-then-act on self.conn:
        `if self.conn is not None: ... self.conn.disconnect() ... self.reset()`
        (reset nulls self.conn). At teardown both the daemon reader thread
        (EClient.run()'s `finally: self.disconnect()`) and the main thread call
        it; the reader can clear the None-check a hair before the main thread's
        reset() nulls self.conn, then crash on `self.conn.disconnect()` with
        AttributeError. Serializing the two callers makes the second observe
        self.conn is None and skip cleanly.
        """
        self._disconnecting = True
        with self._disconnect_lock:
            super().disconnect()

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect_gracefully()
        return False

    # ------------------------------------------------------------------
    # Order ID management
    # ------------------------------------------------------------------

    def next_order_id(self) -> int:
        """Get and auto-increment the next valid order ID.

        Lesson learned: always increment after use to prevent Error 103
        (duplicate order ID) on executor restart.
        """
        if self.nextorderId is None:
            raise RuntimeError("Not connected — nextorderId is None")
        oid: int = self.nextorderId
        self.nextorderId = oid + 1
        return oid

    def refresh_order_id(self) -> bool:
        """Force-refresh nextValidId from TWS.

        Call after reqAllOpenOrders() to avoid Error 103.
        """
        self._connected_event.clear()
        self.reqIds(1)
        return self._connected_event.wait(timeout=5.0)

    # ------------------------------------------------------------------
    # Position requests
    # ------------------------------------------------------------------

    def get_positions_sync(self, timeout: float = 10.0) -> dict[int, dict]:
        """Request all positions and wait for response.

        Returns:
            dict mapping conId -> position data
        """
        self._positions_event.clear()
        self.positions.clear()
        self.reqPositions()
        if not self._positions_event.wait(timeout=timeout):
            logger.warning(f"Position request timed out after {timeout}s")
        return dict(self.positions)

    def get_executions_sync(
        self, timeout: float = 10.0, last_n_days: int = 0
    ) -> dict[str, dict]:
        """Request recent executions and wait.

        reqExecutions fires execDetails / executionDetailsProtoBuf for each fill
        (now carrying conId + avgPrice + permId) then execDetailsEnd. Returns
        {execId: execution data} with the fill prices reqCompletedOrders lacks.

        An empty ExecutionFilter returns only the CURRENT trading day's fills,
        so an entry that filled overnight (after its executor had already
        exited) is invisible to the resolver the next morning. last_n_days > 0
        sets ExecutionFilter.lastNDays so TWS replays that many days of fills
        and the stranded fill becomes resolvable. Needs a TWS build new enough
        for parametrized days-of-executions (older builds error UPDATE_TWS).
        """
        from ibapi.execution import ExecutionFilter

        self._exec_event.clear()
        self.executions.clear()
        exec_filter = ExecutionFilter()
        if last_n_days > 0:
            exec_filter.lastNDays = last_n_days
        req_id = self.next_order_id()
        self.reqExecutions(req_id, exec_filter)
        if not self._exec_event.wait(timeout=timeout):
            logger.warning(f"Executions request timed out after {timeout}s")
        return dict(self.executions)

    # ------------------------------------------------------------------
    # Open orders requests
    # ------------------------------------------------------------------

    def get_open_orders_sync(self, timeout: float = 10.0) -> dict[int, dict]:
        """Request open orders for THIS client and wait.

        Returns:
            dict mapping orderId -> order data
        """
        self._open_orders_event.clear()
        self.reqOpenOrders()
        if not self._open_orders_event.wait(timeout=timeout):
            logger.warning(f"Open orders request timed out after {timeout}s")
        return dict(self.orders)

    def get_all_open_orders_sync(self, timeout: float = 10.0) -> dict[int, dict]:
        """Request ALL open orders across ALL clients and wait.

        Critical for STP cancel resolution — populates perm_id_map.

        Implementation note: openOrderEnd fires after the last openOrder
        callback in theory, but in practice the dispatcher can deliver the
        End event before processing the final openOrder events on busy
        accounts. We add 2s of post-event slack and also bind via
        reqOpenOrders() so the calling client's own orders surface
        reliably (necessary when the same client placed the STP in a
        previous session — TWS sometimes withholds those from the
        anonymous reqAllOpenOrders pool until the client rebinds).
        """
        import time

        # reqOpenOrders() binds the calling client to its own open orders.
        # reqAllOpenOrders() is the anonymous snapshot of all clients' orders.
        # Note: reqAutoOpenOrders() only works for Client 0 (Error 321 otherwise),
        # so it's not used here.
        # Caveat: orders placed by this clientId in a previous disconnected
        # session don't always rebind on reconnect — TWS may withhold them
        # from both reqOpenOrders and reqAllOpenOrders. Callers must tolerate
        # missing orders gracefully (do not blindly cancel orders we can't see).
        try:
            self.reqOpenOrders()
            time.sleep(0.5)
        except Exception as e:
            logger.debug(f"reqOpenOrders failed (non-fatal): {e}")
        self._open_orders_event.clear()
        self.reqAllOpenOrders()
        if not self._open_orders_event.wait(timeout=timeout):
            logger.warning(f"All open orders request timed out after {timeout}s")
        # Drain trailing openOrder callbacks delivered after openOrderEnd.
        time.sleep(2.0)
        logger.info(
            f"get_all_open_orders_sync: {len(self.orders)} orders, "
            f"{len(self.perm_id_map)} permIds mapped"
        )
        return dict(self.orders)

    # ------------------------------------------------------------------
    # Completed orders (filled/cancelled from previous sessions)
    # ------------------------------------------------------------------

    def get_completed_orders_sync(
        self, api_only: bool = False, timeout: float = 10.0
    ) -> list[dict]:
        """Request completed orders (filled/cancelled) and wait.

        This is the API-based way to detect fills that happened when
        the executor was NOT running (STP triggers, manual TWS closes,
        margin liquidations, overnight EU fills).

        Args:
            api_only: If True, only return API-placed orders.
                If False, include orders placed manually in TWS.
            timeout: Seconds to wait

        Returns:
            List of completed order dicts
        """
        self._completed_orders_event.clear()
        self.completed_orders.clear()
        self.reqCompletedOrders(api_only)
        if not self._completed_orders_event.wait(timeout=timeout):
            logger.warning(f"Completed orders request timed out after {timeout}s")
        return list(self.completed_orders)

    # ------------------------------------------------------------------
    # Contract details requests
    # ------------------------------------------------------------------

    def get_contract_details_sync(
        self, contract: Contract, timeout: float = 10.0
    ) -> list:
        """Request contract details and wait for response."""
        self._contract_details_event.clear()
        self._contract_details = []
        req_id = self.next_order_id()
        self.reqContractDetails(req_id, contract)
        if not self._contract_details_event.wait(timeout=timeout):
            logger.warning(f"Contract details request timed out after {timeout}s")
        return list(self._contract_details)

    # ------------------------------------------------------------------
    # Market rule requests (for dynamic tick sizes)
    # ------------------------------------------------------------------

    def get_market_rule_sync(self, market_rule_id: int, timeout: float = 5.0) -> list:
        """Request market rule (tick size table) and wait.

        Returns list of (price_threshold, tick_increment) tuples.
        """
        self._market_rule_event.clear()
        self._market_rules[market_rule_id] = []
        self.reqMarketRule(market_rule_id)
        if not self._market_rule_event.wait(timeout=timeout):
            logger.warning(f"Market rule {market_rule_id} request timed out")
        return self._market_rules.get(market_rule_id, [])

    # ------------------------------------------------------------------
    # EWrapper Callbacks — Connection
    # ------------------------------------------------------------------

    def nextValidId(self, orderId: int) -> None:
        """Called on connection + after reqIds(). Signals ready to trade."""
        self.nextorderId = orderId
        logger.debug(f"nextValidId: {orderId}")
        self._connected_event.set()

    def managedAccounts(self, accountsList: str) -> None:
        """Called on connection with comma-separated account list."""
        self.managed_accounts = accountsList
        logger.debug(f"Managed accounts: {accountsList}")

    def connectionClosed(self) -> None:
        """Called when connection is lost."""
        self.connected = False
        logger.warning("Connection to TWS closed")

    # ------------------------------------------------------------------
    # EWrapper Callbacks — Error handling
    # ------------------------------------------------------------------

    def error(
        self,
        reqId: int,
        errorTime: int,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson: str = "",
    ) -> None:
        """Central error handler with classification."""
        level = classify(errorCode)

        if level == ErrorLevel.INFO:
            logger.debug(f"[INFO {errorCode}] {errorString}")
            return

        if level == ErrorLevel.WARNING:
            logger.warning(f"[WARN {errorCode}] {errorString}")
            return

        if level == ErrorLevel.CANCEL:
            logger.info(f"[CANCEL {errorCode}] reqId={reqId} {errorString}")
            if reqId >= 0 and reqId in self.orders:
                self.orders[reqId]["status"] = "Cancelled"
            return

        if level == ErrorLevel.REJECT:
            logger.warning(f"[REJECT {errorCode}] reqId={reqId} {errorString}")
            if advancedOrderRejectJson:
                logger.warning(f"  Reject detail: {advancedOrderRejectJson}")
            if reqId >= 0:
                self.order_errors[reqId].append(errorCode)
            return

        if level == ErrorLevel.CRITICAL:
            logger.error(f"[CRITICAL {errorCode}] {errorString}")
            if is_duplicate_order_id(errorCode):
                logger.error("  → Need fresh nextValidId (reqIds)")
            return

        # Unknown
        if reqId >= 0:
            logger.warning(f"[{errorCode}] reqId={reqId} {errorString}")
        else:
            logger.info(f"[{errorCode}] {errorString}")

    # ------------------------------------------------------------------
    # EWrapper Callbacks — Orders
    # ------------------------------------------------------------------

    def openOrder(
        self,
        orderId: int,
        contract: Contract,
        order,
        orderState: OrderState,
    ) -> None:
        """Called for each open order. Populates permId mapping.

        Lesson learned: orderStatus can fire BEFORE openOrder.
        Both callbacks must write permId to prevent race condition.
        """
        self.orders.setdefault(orderId, {})
        self.orders[orderId].update(
            {
                "contract": contract,
                "order": order,
                "orderState": orderState,
                "permId": order.permId,
                "symbol": contract.symbol,
            }
        )
        # Cross-session mapping: permId -> session orderId
        if order.permId > 0:
            self.perm_id_map[order.permId] = orderId

    def openOrderEnd(self) -> None:
        """Called after all open orders have been delivered."""
        self._open_orders_event.set()

    def openOrderProtoBuf(self, openOrderProto) -> None:
        """Modern (protobuf v2) replacement for openOrder.

        REGRESSION: TWS dispatches this INSTEAD of legacy openOrder for
        bracket child orders submitted in the same session. Without this
        handler, perm_id_map never gets the child STP's permId — so
        BracketBuilder.cancel_stop_order(perm_id=0) silently no-ops and
        leaves an orphan STP. Live L9 test on 2026-04-29 caught this.
        """
        order_id = openOrderProto.orderId
        contract = openOrderProto.contract
        order = openOrderProto.order
        order_state = openOrderProto.orderState

        self.orders.setdefault(order_id, {})
        self.orders[order_id].update(
            {
                "contract": contract,
                "order": order,
                "orderState": order_state,
                "permId": order.permId,
                "symbol": contract.symbol,
            }
        )
        if order.permId > 0:
            self.perm_id_map[order.permId] = order_id

    def openOrdersEndProtoBuf(self, openOrdersEndProto) -> None:  # noqa: ARG002
        """Modern protobuf openOrderEnd."""
        self._open_orders_event.set()

    def orderStatusProtoBuf(self, orderStatusProto) -> None:
        """Modern (protobuf v2) replacement for orderStatus.

        Same coercion as the legacy handler — protobuf decimal/string
        fields normalized to float at source.
        """

        def _f(v):
            if v in ("", None):
                return 0.0
            try:
                return float(v)
            except (TypeError, ValueError):
                return 0.0

        order_id = orderStatusProto.orderId
        perm_id = orderStatusProto.permId

        self.orders.setdefault(order_id, {})
        self.orders[order_id].update(
            {
                "status": orderStatusProto.status,
                "filled": _f(orderStatusProto.filled),
                "remaining": _f(orderStatusProto.remaining),
                "avgFillPrice": _f(orderStatusProto.avgFillPrice),
                "permId": perm_id,
                "parentId": orderStatusProto.parentId,
                "lastFillPrice": _f(orderStatusProto.lastFillPrice),
                "clientId": orderStatusProto.clientId,
            }
        )
        if perm_id > 0:
            self.perm_id_map[perm_id] = order_id

    def completedOrder(self, contract: Contract, order, orderState: OrderState) -> None:
        """Called for each completed (filled/cancelled) order.

        This fires from reqCompletedOrders — shows fills that happened
        when executor was NOT running (STP triggers, manual closes,
        margin liquidations, overnight EU fills).
        """
        self.completed_orders.append(
            {
                "symbol": contract.symbol,
                "conId": contract.conId,
                "action": order.action,
                "quantity": float(order.totalQuantity),
                "orderType": order.orderType,
                "status": orderState.status,
                "permId": order.permId,
                "parentId": order.parentId,
                "orderRef": order.orderRef,
                "completedTime": orderState.completedTime,
                "contract": contract,
                "order": order,
            }
        )

    def completedOrdersEnd(self) -> None:
        """Called after all completed orders delivered."""
        logger.debug(f"completedOrdersEnd: {len(self.completed_orders)} orders")
        self._completed_orders_event.set()

    def orderStatus(
        self,
        orderId: int,
        status: str,
        filled: float,
        remaining: float,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float = 0.0,
    ) -> None:
        """Called for order status updates (fills, cancels, etc.).

        Lesson learned: this can fire before openOrder.
        Use setdefault to ensure orderId exists in dict.
        """

        # ibapi (protobuf v2) sends `filled` and `remaining` as decimal.Decimal,
        # and some paths emit empty string for not-yet-filled orders. Coerce to
        # float at source so downstream comparisons (filled > 0) stay safe.
        def _f(v):
            if v in ("", None):
                return 0.0
            try:
                return float(v)
            except (TypeError, ValueError):
                return 0.0

        self.orders.setdefault(orderId, {})
        self.orders[orderId].update(
            {
                "status": status,
                "filled": _f(filled),
                "remaining": _f(remaining),
                "avgFillPrice": _f(avgFillPrice),
                "permId": permId,
                "parentId": parentId,
                "lastFillPrice": _f(lastFillPrice),
                "clientId": clientId,
            }
        )
        # Also update perm_id_map from orderStatus (race condition fix)
        if permId > 0:
            self.perm_id_map[permId] = orderId

        logger.debug(
            f"orderStatus: id={orderId} status={status} "
            f"filled={filled} remaining={remaining} "
            f"avgPrice={avgFillPrice} permId={permId}"
        )

    # ------------------------------------------------------------------
    # EWrapper Callbacks — Executions & Commissions
    # ------------------------------------------------------------------

    def execDetails(self, reqId: int, contract: Contract, execution) -> None:
        """Called for each execution (fill) detail."""
        exec_id = execution.execId
        self.executions[exec_id] = {
            "symbol": contract.symbol,
            "conId": getattr(contract, "conId", 0),
            "side": execution.side,
            "shares": execution.shares,
            "price": execution.price,
            "orderId": execution.orderId,
            "permId": execution.permId,
            "orderRef": getattr(execution, "orderRef", ""),
            "time": execution.time,
            "exchange": execution.exchange,
            "cumQty": execution.cumQty,
            "avgPrice": execution.avgPrice,
        }
        logger.info(
            f"execDetails: {contract.symbol} {execution.side} "
            f"{execution.shares}@{execution.price} "
            f"orderId={execution.orderId}"
        )

    def commissionReport(self, commissionReport) -> None:
        """Legacy commissionReport — kept for old TWS versions.

        Modern ibapi (post-protobuf v2) no longer dispatches this callback;
        commissionAndFeesReport below is what fires today.
        """
        exec_id = commissionReport.execId
        commission = commissionReport.commission
        if commission < 1e300:
            self.commissions[exec_id] = commission
            logger.debug(
                f"commission(legacy): execId={exec_id} "
                f"commission={commission:.4f} "
                f"currency={commissionReport.currency}"
            )

    def commissionAndFeesReport(self, commissionAndFeesReport) -> None:
        """Modern replacement for commissionReport (ibapi >= protobuf v2).

        Field renamed: commission → commissionAndFees. We persist into the
        same self.commissions dict so get_commission_for_order works
        regardless of which callback path TWS uses.
        """
        exec_id = commissionAndFeesReport.execId
        commission = commissionAndFeesReport.commissionAndFees
        if commission < 1e300:
            self.commissions[exec_id] = commission
            logger.debug(
                f"commission: execId={exec_id} "
                f"commission={commission:.4f} "
                f"currency={commissionAndFeesReport.currency}"
            )

    def executionDetailsProtoBuf(self, executionDetailsProto) -> None:
        """Protobuf path for execDetails — fires on modern TWS instead of
        the legacy execDetails callback. Populates self.executions same as
        the legacy handler so get_commission_for_order() can match by orderId.
        """
        contract = executionDetailsProto.contract
        execution = executionDetailsProto.execution
        exec_id = execution.execId
        self.executions[exec_id] = {
            "symbol": contract.symbol,
            "conId": getattr(contract, "conId", 0),
            "side": execution.side,
            "shares": execution.shares,
            "price": execution.price,
            "orderId": execution.orderId,
            "permId": execution.permId,
            "orderRef": getattr(execution, "orderRef", ""),
            "time": execution.time,
            "exchange": execution.exchange,
            "cumQty": execution.cumQty,
            "avgPrice": execution.avgPrice,
        }
        logger.info(
            f"executionDetailsProto: {contract.symbol} {execution.side} "
            f"{execution.shares}@{execution.price} "
            f"orderId={execution.orderId}"
        )

    def execDetailsEnd(self, reqId: int) -> None:
        """End of a reqExecutions batch — unblocks get_executions_sync.

        Modern TWS may dispatch executions via executionDetailsProtoBuf but
        still signals completion through this end callback, so setting the
        event here covers both the legacy and protobuf execution paths.
        """
        self._exec_event.set()

    # ------------------------------------------------------------------
    # EWrapper Callbacks — Positions
    # ------------------------------------------------------------------

    def position(
        self,
        account: str,
        contract: Contract,
        pos: float,
        avgCost: float,
    ) -> None:
        """Called for each position held."""
        self.positions[contract.conId] = {
            "account": account,
            "symbol": contract.symbol,
            "secType": contract.secType,
            "exchange": contract.exchange,
            "currency": contract.currency,
            "conId": contract.conId,
            "position": pos,
            "avgCost": avgCost,
            "contract": contract,
        }

    def positionEnd(self) -> None:
        """Called after all positions have been delivered."""
        logger.debug(f"positionEnd: {len(self.positions)} positions")
        self._positions_event.set()

    # ------------------------------------------------------------------
    # EWrapper Callbacks — Account
    # ------------------------------------------------------------------

    def accountSummary(
        self,
        reqId: int,
        account: str,
        tag: str,
        value: str,
        currency: str,
    ) -> None:
        """Called for each account summary tag."""
        self.account_summary[tag] = value

    def accountSummaryEnd(self, reqId: int) -> None:
        """Called after all account summary tags delivered."""
        self._account_summary_event.set()

    def updatePortfolio(
        self,
        contract: Contract,
        position: float,
        marketPrice: float,
        marketValue: float,
        averageCost: float,
        unrealizedPNL: float,
        realizedPNL: float,
        accountName: str,
    ) -> None:
        """Called for portfolio updates (reqAccountUpdates)."""
        self.portfolio[contract.conId] = {
            "symbol": contract.symbol,
            "position": position,
            "marketPrice": marketPrice,
            "marketValue": marketValue,
            "averageCost": averageCost,
            "unrealizedPNL": unrealizedPNL,
            "realizedPNL": realizedPNL,
            "contract": contract,
        }

    # ------------------------------------------------------------------
    # EWrapper Callbacks — P&L
    # ------------------------------------------------------------------

    def pnl(
        self,
        reqId: int,
        dailyPnL: float,
        unrealizedPnL: float,
        realizedPnL: float,
    ) -> None:
        """Account-wide P&L update."""
        self.account_pnl = {
            "dailyPnL": dailyPnL,
            "unrealizedPnL": unrealizedPnL,
            "realizedPnL": realizedPnL,
        }

    def pnlSingle(
        self,
        reqId: int,
        pos: float,
        dailyPnL: float,
        unrealizedPnL: float,
        realizedPnL: float,
        value: float,
    ) -> None:
        """Per-position P&L update."""
        self.position_pnl[reqId] = {
            "position": pos,
            "dailyPnL": dailyPnL,
            "unrealizedPnL": unrealizedPnL,
            "realizedPnL": realizedPnL,
            "value": value,
        }

    # ------------------------------------------------------------------
    # EWrapper Callbacks — Contract details
    # ------------------------------------------------------------------

    def contractDetails(self, reqId: int, contractDetails) -> None:
        """Called for each matching contract."""
        self._contract_details.append(contractDetails)

    def contractDetailsEnd(self, reqId: int) -> None:
        """Called after all contract details delivered."""
        self._contract_details_event.set()

    # ------------------------------------------------------------------
    # EWrapper Callbacks — Market rules (tick size tables)
    # ------------------------------------------------------------------

    def marketRule(self, marketRuleId: int, priceIncrements: list) -> None:
        """Called with tick size table for a market rule ID.

        priceIncrements is a list of PriceIncrement objects with:
          - lowEdge: price threshold
          - increment: tick size at this price level
        """
        self._market_rules[marketRuleId] = [
            (pi.lowEdge, pi.increment) for pi in priceIncrements
        ]
        logger.debug(
            f"marketRule {marketRuleId}: " f"{len(priceIncrements)} price increments"
        )
        self._market_rule_event.set()

    # ------------------------------------------------------------------
    # EWrapper Callbacks — Market data (basic)
    # ------------------------------------------------------------------

    def tickPrice(
        self,
        reqId: int,
        tickType: int,
        price: float,
        attrib,
    ) -> None:
        """Called for market data price ticks."""
        # Store in a generic dict for market data requests
        if not hasattr(self, "_tick_data"):
            self._tick_data = {}
        self._tick_data.setdefault(reqId, {})
        self._tick_data[reqId][tickType] = price

    def tickSnapshotEnd(self, reqId: int) -> None:
        """Called when snapshot market data is complete."""
        pass

    # ------------------------------------------------------------------
    # Utility: check for order errors
    # ------------------------------------------------------------------

    def has_order_errors(
        self,
        order_id: int,
        error_codes: frozenset[int] | None = None,
    ) -> bool:
        """Check if an order has received rejection errors.

        Args:
            order_id: The order ID to check.
            error_codes: Specific codes to check for.
                Defaults to ORDER_REJECTION_CODES.
        """
        codes = error_codes or ORDER_REJECTION_CODES
        return any(c in codes for c in self.order_errors.get(order_id, []))

    def get_order_status(self, order_id: int) -> str | None:
        """Get current status of an order."""
        return self.orders.get(order_id, {}).get("status")

    def get_fill_price(self, order_id: int) -> float | None:
        """Get average fill price for an order."""
        return self.orders.get(order_id, {}).get("avgFillPrice")

    def get_commission_for_order(self, order_id: int) -> float:
        """Sum all commissions for fills belonging to an order."""
        total = 0.0
        for exec_id, exec_data in self.executions.items():
            if exec_data.get("orderId") == order_id:
                total += self.commissions.get(exec_id, 0.0)
        return total
