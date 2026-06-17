"""FillResult → pending_signals.csv status mapping tests.

The Phase 3 monitor returns one FillResult per orderId; BaseExecutor's
_process_results then writes the CSV status. This pins down the mapping:

  EXIT signals:
    Filled       → EXECUTED  (+ closed_trade row, position removed)
    PartialFill  → PARTIAL_FILL  (resolve_fills picks it up)
    Timeout      → PARTIAL_FILL  (regression: was FAILED, but stuck-PreSubmitted
                                  EU exits with filled=0 must stay reconcilable)
    Rejected     → FAILED

  ENTRY signals:
    Filled       → EXECUTED  (+ position row added)
    PartialFill  → PARTIAL_FILL
    Timeout      → stays PENDING + entry_perm_id persisted (idempotent re-run)
    Rejected     → FAILED

No TWS connection — uses tmp_path-backed StrategyConfig + FillResult dataclass.
"""
from __future__ import annotations

import pandas as pd
import pytest

from ibkr.execution.fill_monitor import FillResult


@pytest.fixture
def tmp_strategy(tmp_path):
    from ibkr.execution.strategy_config import StrategyConfig

    root = tmp_path / "fake"
    (root / "deployment" / "signals").mkdir(parents=True)
    (root / "deployment" / "positions").mkdir(parents=True)

    cfg = StrategyConfig(
        name="fake",
        client_id=99,
        strategy_code="FK",
        strategy_root=root,
        signal_column="ticker",
    )

    pd.DataFrame([
        {"ticker": "SPY", "action": "SELL", "status": "PENDING"},
        {"ticker": "QQQ", "action": "BUY", "status": "PENDING"},
    ]).to_csv(cfg.signals_file, index=False)

    return cfg


def _read_status(cfg, ticker: str) -> str:
    df = pd.read_csv(cfg.signals_file)
    return df[df["ticker"] == ticker].iloc[0]["status"]


# --- EXIT signal mapping -----------------------------------------------------

def test_exit_timeout_marked_partial_fill_for_resolve_pickup(tmp_strategy):
    """REGRESSION: stuck-PreSubmitted EXIT with filled=0 → PARTIAL_FILL.

    Previously v2 mapped Timeout exits to FAILED, which resolve_fills.py
    skips (it only scans PENDING / PARTIAL_FILL). Tonight's IGAA.L/BWX
    case (EU overnight exit, stuck PreSubmitted, no fill) demonstrated
    why this is wrong — the order is still WORKING at IBKR and must be
    reconcilable next morning.
    """
    from ibkr.execution.base_executor import BaseExecutor

    ex = BaseExecutor(tmp_strategy)
    signal = pd.Series({"ticker": "SPY", "action": "SELL", "status": "PENDING"})
    ex.exit_order_ids.append((1001, signal))

    results = {1001: FillResult(order_id=1001, status="Timeout", perm_id=42)}
    ex._process_results(results)

    assert _read_status(tmp_strategy, "SPY") == "PARTIAL_FILL"


def test_exit_partial_fill_marked_partial_fill(tmp_strategy):
    """Confirmed PartialFill on exit → PARTIAL_FILL (existing behavior preserved)."""
    from ibkr.execution.base_executor import BaseExecutor

    ex = BaseExecutor(tmp_strategy)
    signal = pd.Series({"ticker": "SPY", "action": "SELL", "status": "PENDING"})
    ex.exit_order_ids.append((1002, signal))

    results = {
        1002: FillResult(
            order_id=1002,
            status="PartialFill",
            avg_price=405.0,
            filled_qty=5.0,
            remaining_qty=5.0,
        ),
    }
    ex._process_results(results)

    assert _read_status(tmp_strategy, "SPY") == "PARTIAL_FILL"


def test_exit_rejected_marked_failed(tmp_strategy):
    """Rejected exits go to FAILED — not recoverable, intentional drop."""
    from ibkr.execution.base_executor import BaseExecutor

    ex = BaseExecutor(tmp_strategy)
    signal = pd.Series({"ticker": "SPY", "action": "SELL", "status": "PENDING"})
    ex.exit_order_ids.append((1003, signal))

    results = {1003: FillResult(order_id=1003, status="Rejected", perm_id=99)}
    ex._process_results(results)

    assert _read_status(tmp_strategy, "SPY") == "FAILED"


# --- ENTRY signal mapping ----------------------------------------------------

def test_entry_timeout_stays_pending_and_persists_perm_id(tmp_strategy):
    """ENTRY Timeout must NOT mark FAILED — order is still working at IBKR.
    Status stays PENDING and entry_perm_id is stamped for idempotent re-run."""
    from ibkr.execution.base_executor import BaseExecutor

    ex = BaseExecutor(tmp_strategy)
    signal = pd.Series({"ticker": "QQQ", "action": "BUY", "status": "PENDING"})
    ex.entry_order_ids.append((2001, signal))

    results = {2001: FillResult(order_id=2001, status="Timeout", perm_id=998877)}
    ex._process_results(results)

    df = pd.read_csv(tmp_strategy.signals_file)
    qqq = df[df["ticker"] == "QQQ"].iloc[0]
    assert qqq["status"] == "PENDING"
    assert int(qqq["entry_perm_id"]) == 998877


def test_entry_partial_fill_marked_partial_fill(tmp_strategy):
    from ibkr.execution.base_executor import BaseExecutor

    ex = BaseExecutor(tmp_strategy)
    signal = pd.Series({"ticker": "QQQ", "action": "BUY", "status": "PENDING"})
    ex.entry_order_ids.append((2002, signal))

    results = {
        2002: FillResult(
            order_id=2002,
            status="PartialFill",
            avg_price=380.0,
            filled_qty=3.0,
            remaining_qty=2.0,
        ),
    }
    ex._process_results(results)

    assert _read_status(tmp_strategy, "QQQ") == "PARTIAL_FILL"
