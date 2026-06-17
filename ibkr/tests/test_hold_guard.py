"""HOLD-guard tests for BaseExecutor.run().

`portfolio_state.json::hold.active = true` MUST short-circuit run() before
any IBKR connect, signal load, or phase execution. Bypass = edit JSON.

Pins three guarantees:
  - hold.active = true → connect() / load_signals() / phases never called
  - hold.active = false (or hold key missing) → run() proceeds normally
  - portfolio_state.json missing → run() proceeds normally (not held)
  - malformed JSON → treat as held (fail closed)
"""
from __future__ import annotations

import json
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


def _stub_phases(monkeypatch):
    """Stub connect + loaders + phases so a non-held run() would succeed."""
    from ibkr.execution.base_executor import BaseExecutor

    def fake_connect(self):
        self.conn = MagicMock()
        self.router = MagicMock()
        self.monitor = MagicMock()
        self.monitor.monitor_all.return_value = []
        self.bracket = None
        return True

    monkeypatch.setattr(BaseExecutor, "connect", fake_connect)
    monkeypatch.setattr(BaseExecutor, "load_contract_mapping", lambda self: {})
    monkeypatch.setattr(BaseExecutor, "load_positions", lambda self: pd.DataFrame())
    monkeypatch.setattr(BaseExecutor, "load_signals", lambda self: pd.DataFrame())


def _write_state(cfg, **kwargs):
    cfg.portfolio_state_file.write_text(
        json.dumps(kwargs, indent=2) + "\n"
    )


def test_hold_active_blocks_run(tmp_strategy, monkeypatch):
    from ibkr.execution.base_executor import BaseExecutor

    _stub_phases(monkeypatch)
    _write_state(
        tmp_strategy,
        strategy="fake",
        hold={"active": True, "reason": "test hold", "set_at": "2026-05-17"},
    )

    connect_spy = MagicMock(return_value=True)
    load_spy = MagicMock(return_value=pd.DataFrame())
    monkeypatch.setattr(BaseExecutor, "connect", connect_spy)
    monkeypatch.setattr(BaseExecutor, "load_signals", load_spy)

    BaseExecutor(tmp_strategy).run()

    connect_spy.assert_not_called()
    load_spy.assert_not_called()


def test_hold_inactive_proceeds(tmp_strategy, monkeypatch):
    from ibkr.execution.base_executor import BaseExecutor

    _stub_phases(monkeypatch)
    _write_state(
        tmp_strategy,
        strategy="fake",
        hold={"active": False, "reason": "test", "set_at": "2026-05-17"},
    )

    connect_spy = MagicMock(return_value=False)  # abort after connect attempt
    monkeypatch.setattr(BaseExecutor, "connect", connect_spy)

    BaseExecutor(tmp_strategy).run()

    connect_spy.assert_called_once()


def test_no_hold_key_proceeds(tmp_strategy, monkeypatch):
    from ibkr.execution.base_executor import BaseExecutor

    _stub_phases(monkeypatch)
    _write_state(tmp_strategy, strategy="fake", initial_nav=100_000)

    connect_spy = MagicMock(return_value=False)
    monkeypatch.setattr(BaseExecutor, "connect", connect_spy)

    BaseExecutor(tmp_strategy).run()

    connect_spy.assert_called_once()


def test_missing_portfolio_state_proceeds(tmp_strategy, monkeypatch):
    from ibkr.execution.base_executor import BaseExecutor

    _stub_phases(monkeypatch)
    # No portfolio_state.json file written.
    assert not tmp_strategy.portfolio_state_file.exists()

    connect_spy = MagicMock(return_value=False)
    monkeypatch.setattr(BaseExecutor, "connect", connect_spy)

    BaseExecutor(tmp_strategy).run()

    connect_spy.assert_called_once()


def test_malformed_json_fails_closed(tmp_strategy, monkeypatch):
    """Unreadable portfolio_state.json blocks execution (fail closed)."""
    from ibkr.execution.base_executor import BaseExecutor

    _stub_phases(monkeypatch)
    tmp_strategy.portfolio_state_file.write_text("{not valid json")

    connect_spy = MagicMock(return_value=True)
    monkeypatch.setattr(BaseExecutor, "connect", connect_spy)

    BaseExecutor(tmp_strategy).run()

    connect_spy.assert_not_called()
