"""Phase-orchestration tests for BaseExecutor.run().

Replicates the 3-phase execution pipeline:

  Phase 1: PHASE 1: EXIT ORDERS  → _execute_exits(exits)
  Phase 2: PHASE 2: ENTRY ORDERS → _execute_entries(entries)
  Phase 3: PHASE 3: MONITORING   → monitor.monitor_all(all_order_ids)

This test pins the *order* and *conditional skipping* of each phase. The
guarantees we lock in:

  - exits run before entries before monitoring (sequential, not interleaved)
  - empty exits → Phase 1 skipped (no _execute_exits call)
  - empty entries → Phase 2 skipped
  - no orders queued at all → Phase 3 skipped (don't call monitor_all([]))
  - both exits and entries empty → run() exits cleanly without crashing

No TWS connection — uses MagicMock to swap out the connection/router/monitor.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest


@pytest.fixture
def tmp_strategy(tmp_path):
    from ibkr.execution.strategy_config import StrategyConfig

    root = tmp_path / "fake"
    (root / "deployment" / "signals").mkdir(parents=True)
    (root / "deployment" / "positions").mkdir(parents=True)
    return StrategyConfig(
        name="fake",
        client_id=99,
        strategy_code="FK",
        strategy_root=root,
        signal_column="ticker",
    )


def _make_executor(tmp_strategy, signals: pd.DataFrame, monkeypatch):
    """Build a BaseExecutor with `connect()` stubbed so no TWS is needed."""
    from ibkr.execution.base_executor import BaseExecutor

    ex = BaseExecutor(tmp_strategy)

    # Inline-stub connect: skip real TWS, populate router/monitor with mocks
    def fake_connect(self):
        self.conn = MagicMock()
        self.router = MagicMock()
        self.monitor = MagicMock()
        self.monitor.monitor_all.return_value = []
        self.bracket = None
        return True

    monkeypatch.setattr(BaseExecutor, "connect", fake_connect)

    # Pre-set load methods so we can inject canned data
    monkeypatch.setattr(BaseExecutor, "load_signals", lambda self: signals)
    monkeypatch.setattr(BaseExecutor, "load_contract_mapping", lambda self: {})
    monkeypatch.setattr(BaseExecutor, "load_positions", lambda self: pd.DataFrame())

    return ex


def test_phases_run_in_order_exits_then_entries_then_monitor(
    tmp_strategy, monkeypatch
):
    """Both exits AND entries → all 3 phases fire in the documented order."""
    from ibkr.execution.base_executor import BaseExecutor

    signals = pd.DataFrame([
        {"ticker": "SPY", "action": "SELL", "status": "PENDING"},
        {"ticker": "QQQ", "action": "BUY", "status": "PENDING"},
    ])
    ex = _make_executor(tmp_strategy, signals, monkeypatch)

    # Spy on each phase
    call_log: list[str] = []

    def fake_exits(self, df):
        call_log.append(f"exits:{len(df)}")
        # Push at least one orderId so Phase 3 has something to monitor
        self.exit_order_ids.append((1001, df.iloc[0]))

    def fake_entries(self, df):
        call_log.append(f"entries:{len(df)}")
        self.entry_order_ids.append((1002, df.iloc[0]))

    monkeypatch.setattr(BaseExecutor, "_execute_exits", fake_exits)
    monkeypatch.setattr(BaseExecutor, "_execute_entries", fake_entries)
    monkeypatch.setattr(
        BaseExecutor, "_process_results", lambda self, results: call_log.append("process")
    )

    ex.run()

    # Sequence: exits → entries → monitor → process
    assert call_log == ["exits:1", "entries:1", "process"]
    ex.monitor.monitor_all.assert_called_once  # type: ignore[union-attr]_with  # type: ignore[union-attr]([1001, 1002])


def test_no_exits_skips_phase_one(tmp_strategy, monkeypatch):
    """exits.empty → _execute_exits NOT called."""
    from ibkr.execution.base_executor import BaseExecutor

    signals = pd.DataFrame([
        {"ticker": "QQQ", "action": "BUY", "status": "PENDING"},
    ])
    ex = _make_executor(tmp_strategy, signals, monkeypatch)

    exits_called = MagicMock()
    entries_called = MagicMock()

    def fake_entries(self, df):
        entries_called(len(df))
        self.entry_order_ids.append((1002, df.iloc[0]))

    monkeypatch.setattr(BaseExecutor, "_execute_exits", lambda self, df: exits_called(len(df)))
    monkeypatch.setattr(BaseExecutor, "_execute_entries", fake_entries)
    monkeypatch.setattr(BaseExecutor, "_process_results", lambda self, r: None)

    ex.run()

    exits_called.assert_not_called()
    entries_called.assert_called_once_with(1)
    ex.monitor.monitor_all.assert_called_once  # type: ignore[union-attr]()


def test_no_entries_skips_phase_two(tmp_strategy, monkeypatch):
    """entries.empty → _execute_entries NOT called."""
    from ibkr.execution.base_executor import BaseExecutor

    signals = pd.DataFrame([
        {"ticker": "SPY", "action": "SELL", "status": "PENDING"},
    ])
    ex = _make_executor(tmp_strategy, signals, monkeypatch)

    exits_called = MagicMock()
    entries_called = MagicMock()

    def fake_exits(self, df):
        exits_called(len(df))
        self.exit_order_ids.append((1001, df.iloc[0]))

    monkeypatch.setattr(BaseExecutor, "_execute_exits", fake_exits)
    monkeypatch.setattr(BaseExecutor, "_execute_entries", lambda self, df: entries_called(len(df)))
    monkeypatch.setattr(BaseExecutor, "_process_results", lambda self, r: None)

    ex.run()

    exits_called.assert_called_once_with(1)
    entries_called.assert_not_called()
    ex.monitor.monitor_all.assert_called_once  # type: ignore[union-attr]_with  # type: ignore[union-attr]([1001])


def test_no_signals_skips_phase_three_monitor(tmp_strategy, monkeypatch):
    """No orders queued → monitor_all NOT called (don't poll an empty list)."""
    from ibkr.execution.base_executor import BaseExecutor

    signals = pd.DataFrame([
        {"ticker": "SPY", "action": "BUY", "status": "PENDING"},
    ])
    ex = _make_executor(tmp_strategy, signals, monkeypatch)

    # Both phases run but neither appends to *_order_ids → monitor must skip
    monkeypatch.setattr(BaseExecutor, "_execute_exits", lambda self, df: None)
    monkeypatch.setattr(BaseExecutor, "_execute_entries", lambda self, df: None)
    monkeypatch.setattr(BaseExecutor, "_process_results", lambda self, r: None)

    ex.run()

    ex.monitor.monitor_all.assert_not_called  # type: ignore[union-attr]()


def test_empty_signals_csv_returns_early(tmp_strategy, monkeypatch):
    """Empty signal load → run() returns before any phase, no exception."""
    from ibkr.execution.base_executor import BaseExecutor

    ex = _make_executor(tmp_strategy, pd.DataFrame(), monkeypatch)

    # Phases must not be called at all
    monkeypatch.setattr(
        BaseExecutor,
        "_execute_exits",
        lambda self, df: pytest.fail("Phase 1 should not run on empty signals"),
    )
    monkeypatch.setattr(
        BaseExecutor,
        "_execute_entries",
        lambda self, df: pytest.fail("Phase 2 should not run on empty signals"),
    )

    ex.run()  # must complete cleanly
    ex.monitor.monitor_all.assert_not_called  # type: ignore[union-attr]()
