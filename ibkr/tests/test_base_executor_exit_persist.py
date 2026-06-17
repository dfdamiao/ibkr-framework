"""Pin BaseExecutor's exit-ID persistence to closed_trades.csv."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ibkr.execution.base_executor import BaseExecutor
from ibkr.execution.fill_monitor import FillResult
from ibkr.execution.strategy_config import StrategyConfig


@pytest.fixture
def tmp_executor(tmp_path: Path) -> BaseExecutor:
    """BaseExecutor with tmp strategy root, ConnectionManager patched."""
    root = tmp_path / "fake_strategy"
    (root / "deployment" / "signals").mkdir(parents=True)
    (root / "deployment" / "positions").mkdir(parents=True)

    cfg = StrategyConfig(
        name="fake",
        client_id=99,
        strategy_code="FK",
        strategy_root=root,
        signal_column="ticker",
    )

    with patch("ibkr.execution.base_executor.ConnectionManager", MagicMock()):
        ex = BaseExecutor(cfg)
    return ex


def test_close_position_writes_exit_ids(tmp_executor: BaseExecutor) -> None:
    """_write_closed_trade must persist exit_order_id + exit_perm_id +
    exit_commission to the closed_trades row."""
    tmp_executor._last_removed_position = {
        "entry_price": 180.05,
        "shares": 100.0,
        "entry_signal_price": 180.0,
        "entry_date": "2026-05-20T09:30:00",
        "entry_order_id": 111,
        "entry_perm_id": 222,
        "entry_commission": 1.0,
        "currency": "USD",
    }

    fill = FillResult(
        order_id=333,
        status="Filled",
        avg_price=195.0,
        filled_qty=100.0,
        remaining_qty=0.0,
        commission=1.0,
        perm_id=444,
    )
    sig = pd.Series({"ticker": "AAPL", "execution_price": 194.5})

    tmp_executor._write_closed_trade(sig, fill)

    df = pd.read_csv(tmp_executor.config.closed_trades_file)
    assert len(df) == 1
    row = df.iloc[0]
    assert int(row["exit_order_id"]) == 333
    assert int(row["exit_perm_id"]) == 444
    assert row["exit_commission"] == pytest.approx(1.0)
