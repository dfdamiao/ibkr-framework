"""Retry-loop regression: host/port must survive ibapi's disconnect() nulling.

ibapi's EClient.disconnect()/reset() sets self.host = self.port = None. The
connect retry loop must reconnect with the ORIGINAL host/port, not None
(regression: attempts 2/3 logged host=None port=None and failed with
"str, bytes or bytearray expected, not NoneType", turning a transient first
attempt into a hard failure).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest


@pytest.fixture
def conn():
    from ibkr.core.connection import ConnectionManager

    return ConnectionManager(host="127.0.0.1", port=7497)


def test_retry_reconnects_with_original_host_port_after_disconnect_nulls_them(conn):
    calls: list[tuple] = []

    def fake_connect(host, port, client_id):
        calls.append((host, port))

    def fake_disconnect():
        # reproduce ibapi: disconnect() resets host/port to None
        conn.host = None
        conn.port = None

    with (
        patch("ibapi.client.EClient.connect", side_effect=fake_connect),
        patch.object(conn, "_check_port", return_value=True),
        patch.object(conn, "_safe_disconnect", side_effect=fake_disconnect),
        patch.object(conn, "_run_reader"),
        patch("time.sleep"),
    ):
        ok = conn.connect_with_retry(client_id=30, max_attempts=3, timeout=0.01)

    assert ok is False  # never received nextValidId -> all attempts "fail"
    assert calls == [("127.0.0.1", 7497)] * 3  # never None on retry
