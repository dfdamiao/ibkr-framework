"""Regression test: ConnectionManager.disconnect() must be thread-safe.

ibapi's EClient.disconnect() is a check-then-act on self.conn with no lock:

    def disconnect(self):
        self.setConnState(EClient.DISCONNECTED)
        if self.conn is not None:        # <-- reader passes this check...
            logger.info("disconnecting")
            self.conn.disconnect()       # <-- ...then derefs None here
            self.wrapper.connectionClosed()
            self.reset()                 # sets self.conn = None

At teardown BOTH the daemon reader thread (EClient.run()'s
`finally: self.disconnect()`) and the main thread (disconnect_gracefully) call
disconnect(). When the reader passes the None-check a hair before the main
thread's reset() nulls self.conn, the reader dereferences None:

    AttributeError: 'NoneType' object has no attribute 'disconnect'

(observed live in the v2 orchestrator health-check teardown, 2026-06-03).

This test forces that exact interleaving and asserts disconnect() is atomic.
"""
from __future__ import annotations

import threading
from types import SimpleNamespace
from unittest.mock import patch

import ibapi.client
import pytest


@pytest.fixture
def conn():
    """Construct a ConnectionManager without ever calling connect()."""
    from ibkr.core.connection import ConnectionManager

    return ConnectionManager(host="127.0.0.1", port=7497)


def test_concurrent_disconnect_is_atomic(conn) -> None:
    """Two threads calling disconnect() at teardown must not raise.

    The second thread is held just past its `self.conn is not None` check
    until the first thread's reset() nulls self.conn, then released so its
    `self.conn.disconnect()` would dereference None on lock-less code. An
    atomic disconnect() serializes the two, so the second sees conn is None
    and skips cleanly.
    """
    # Pretend we hold a live socket; this no-op stands in for the real teardown.
    conn.conn = SimpleNamespace(disconnect=lambda: None)

    b_in_window = threading.Event()   # 2nd thread is past its None-check
    a_reset_done = threading.Event()  # 1st thread has nulled self.conn
    count = [0]
    lk = threading.Lock()

    real_reset = conn.reset

    def signalling_reset() -> None:
        real_reset()                  # ibapi reset() sets self.conn = None
        a_reset_done.set()

    conn.reset = signalling_reset

    def fake_info(msg, *args, **kwargs):
        # Coordinate only on the disconnect log line; everything else passes.
        if msg == "disconnecting":
            with lk:
                count[0] += 1
                n = count[0]
            if n == 1:
                # First thread waits for the second to clear its None-check.
                # Times out harmlessly when disconnect() is atomic (the second
                # thread is serialized behind the lock and never gets here).
                b_in_window.wait(0.5)
            else:
                # Second thread is now past its None-check. Hold it until the
                # first thread nulls self.conn, then let it deref.
                b_in_window.set()
                a_reset_done.wait(1.0)
        return None

    errors: dict[str, BaseException | None] = {}

    def run(label: str) -> None:
        try:
            conn.disconnect()
            errors[label] = None
        except BaseException as exc:  # noqa: BLE001 - capture for assertion
            errors[label] = exc

    with patch.object(ibapi.client.logger, "info", side_effect=fake_info):
        t1 = threading.Thread(target=run, args=("t1",), name="t1")
        t2 = threading.Thread(target=run, args=("t2",), name="t2")
        t1.start()
        t2.start()
        t1.join(3.0)
        t2.join(3.0)

    assert not t1.is_alive() and not t2.is_alive(), "disconnect() deadlocked"
    assert errors.get("t1") is None, f"t1 raised: {errors.get('t1')!r}"
    assert errors.get("t2") is None, f"t2 raised: {errors.get('t2')!r}"
    assert conn.conn is None  # fully torn down
