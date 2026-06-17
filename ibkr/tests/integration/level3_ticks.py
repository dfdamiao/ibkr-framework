"""Level 3 — Dynamic tick sizes via reqMarketRule (read-only).

The headline v2 improvement vs prod: prod hardcoded 0.05 for every EU
exchange, which is wrong. Different price levels require different
increments (0.01, 0.02, 0.05). This test verifies reqMarketRule returns
the correct table.

What's verified:
  - SPY (US, ARCA) — tick at $500 should be 0.01
  - CSH.PA (Euronext Paris, SBF) — tick at €50 should NOT be 0.05 for all
    levels (typically 0.01 below ~50, 0.005 below 10, 0.05 above some level)
  - get_tick_table caches per conId (second call doesn't re-fetch)
  - round_to_tick aligns prices to the resolved tick

Run:
    python ibkr/tests/integration/level3_ticks.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from ibkr.core.connection import ConnectionManager
from ibkr.core.contracts import (
    build_stock,
    get_tick_size,
    get_tick_table,
    round_to_tick,
)
from ibkr.tests.integration._common import (
    PAPER_PORT,
    TEST_CLIENT_ID,
    TestResult,
    banner,
    setup_logger,
)

logger = setup_logger("level3")


def run() -> bool:
    print(banner("LEVEL 3 — DYNAMIC TICK SIZES"))
    result = TestResult("Level 3")

    conn = ConnectionManager(port=PAPER_PORT)
    if not conn.connect_with_retry(client_id=TEST_CLIENT_ID):
        result.check("connect", False, "TWS not reachable")
        return result.report()

    try:
        # --- US: SPY ----------------------------------------------------
        spy = build_stock("SPY", exchange="SMART", currency="USD")
        spy.conId = 756733
        spy_table = get_tick_table(conn, spy)
        spy_tick = get_tick_size(conn, spy, price=500.0)

        result.check(
            "SPY tick table populated",
            len(spy_table) > 0,
            f"{len(spy_table)} levels",
        )
        result.check(
            "SPY tick at $500 is 0.01",
            abs(spy_tick - 0.01) < 1e-9,
            f"got {spy_tick}",
        )
        result.check(
            "round_to_tick(500.03, 0.01) = 500.03",
            round_to_tick(500.03, 0.01) == 500.03,
        )

        # --- EU: CSH.PA (Euronext Paris) -------------------------------
        csh = build_stock("CSH", exchange="SBF", currency="EUR")
        csh.conId = 46501995
        csh_table = get_tick_table(conn, csh)
        csh_tick_50 = get_tick_size(conn, csh, price=50.0)

        result.check(
            "CSH.PA tick table populated",
            len(csh_table) > 0,
            f"{len(csh_table)} levels",
        )
        # The bug we're fixing: prod assumed 0.05 for ALL EU prices.
        # CSH.PA at €50 typically requires 0.01, NOT 0.05.
        result.check(
            "CSH.PA tick at €50 is finer than hardcoded 0.05",
            csh_tick_50 < 0.05,
            f"got {csh_tick_50} (prod would have used 0.05)",
        )
        # Compare round_to_tick old vs new
        old_rounded = round_to_tick(50.03, 0.05)
        new_rounded = round_to_tick(50.03, csh_tick_50)
        result.check(
            "round_to_tick(50.03, dynamic) preserves precision",
            new_rounded != old_rounded or csh_tick_50 == 0.05,
            f"old={old_rounded} new={new_rounded} tick={csh_tick_50}",
        )

        # --- Cache: second call must not re-fetch -----------------------
        from ibkr.core.contracts import _tick_size_cache
        cached_before = len(_tick_size_cache)
        _ = get_tick_table(conn, csh)
        cached_after = len(_tick_size_cache)
        result.check(
            "Tick cache stable on repeated lookup",
            cached_after == cached_before,
            f"size {cached_before} → {cached_after}",
        )

    finally:
        conn.disconnect_gracefully()

    return result.report()


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
