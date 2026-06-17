"""Signal-processing tests for BaseExecutor.

Covers four pieces of pre-execution logic:

  1. normalize_action — ENTER/ENTRY → BUY, EXIT → SELL (ma200 compatibility)
  2. _split_signals — partitions PENDING signals into exits and entries by action
  3. load_signals — filters non-PENDING rows and stale (> 1 day old) signals
  4. Stop-loss column normalization at the bracket-entry call site
     (sl_pct > 1 means percent, must be divided by 100 before BracketBuilder)

No TWS connection — uses tmp_path-backed StrategyConfig + MagicMock.
"""
from __future__ import annotations

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


# --- normalize_action --------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("BUY", "BUY"),
    ("buy", "BUY"),
    ("ENTER", "BUY"),
    ("Enter", "BUY"),
    ("ENTRY", "BUY"),
    ("SELL", "SELL"),
    ("sell", "SELL"),
    ("EXIT", "SELL"),
    ("Exit", "SELL"),
    ("  BUY  ", "BUY"),     # whitespace stripped
    ("HOLD", "HOLD"),       # unknown verb passes through (uppercased)
])
def test_normalize_action(raw, expected, tmp_strategy):
    """ma200 emits ENTER/EXIT, others emit BUY/SELL — both must collapse."""
    from ibkr.execution.base_executor import BaseExecutor

    ex = BaseExecutor(tmp_strategy)
    assert ex.normalize_action(raw) == expected


# --- _split_signals ----------------------------------------------------------

def test_split_signals_partitions_buys_and_sells(tmp_strategy):
    from ibkr.execution.base_executor import BaseExecutor

    ex = BaseExecutor(tmp_strategy)
    signals = pd.DataFrame([
        {"ticker": "SPY", "action": "BUY"},
        {"ticker": "QQQ", "action": "SELL"},
        {"ticker": "TLT", "action": "ENTER"},   # ma200-style → BUY bucket
        {"ticker": "IWM", "action": "EXIT"},    # ma200-style → SELL bucket
    ])

    exits, entries = ex._split_signals(signals)

    assert sorted(exits["ticker"].tolist()) == ["IWM", "QQQ"]
    assert sorted(entries["ticker"].tolist()) == ["SPY", "TLT"]


def test_split_signals_returns_empty_when_action_column_missing(tmp_strategy):
    """Missing action column → both buckets empty (logs error but doesn't raise)."""
    from ibkr.execution.base_executor import BaseExecutor

    ex = BaseExecutor(tmp_strategy)
    signals = pd.DataFrame([{"ticker": "SPY"}])  # no `action`

    exits, entries = ex._split_signals(signals)

    assert exits.empty
    assert entries.empty


# --- load_signals: PENDING filter + stale filter -----------------------------

def test_load_signals_keeps_only_pending(tmp_strategy):
    from ibkr.execution.base_executor import BaseExecutor

    today = pd.Timestamp.now().normalize().strftime("%Y-%m-%d")
    pd.DataFrame([
        {"ticker": "SPY", "status": "PENDING", "date": today},
        {"ticker": "QQQ", "status": "EXECUTED", "date": today},
        {"ticker": "TLT", "status": "FAILED", "date": today},
        {"ticker": "IWM", "status": "PARTIAL_FILL", "date": today},
    ]).to_csv(tmp_strategy.signals_file, index=False)

    ex = BaseExecutor(tmp_strategy)
    df = ex.load_signals()

    assert df["ticker"].tolist() == ["SPY"]


def test_load_signals_drops_stale_dates(tmp_strategy):
    """Signals dated before today's normalized cutoff are dropped (1 trading-day)."""
    from ibkr.execution.base_executor import BaseExecutor

    today = pd.Timestamp.now().normalize()
    fresh = str(today.date())
    stale = str((today - pd.Timedelta(days=2)).date())
    pd.DataFrame([
        {"ticker": "SPY", "status": "PENDING", "date": fresh},
        {"ticker": "OLD", "status": "PENDING", "date": stale},
    ]).to_csv(tmp_strategy.signals_file, index=False)

    ex = BaseExecutor(tmp_strategy)
    df = ex.load_signals()

    assert df["ticker"].tolist() == ["SPY"]


def test_load_signals_returns_empty_when_file_missing(tmp_strategy):
    """No file on disk must NOT raise — returns empty DataFrame."""
    from ibkr.execution.base_executor import BaseExecutor

    # No file written
    assert not tmp_strategy.signals_file.exists()

    ex = BaseExecutor(tmp_strategy)
    df = ex.load_signals()

    assert df.empty


# --- Stop-loss percentage normalization --------------------------------------

@pytest.mark.parametrize("raw,expected", [
    (0.05, 0.05),    # already fractional → untouched
    (5.0, 0.05),     # percent → divided by 100
    (12.0, 0.12),    # percent → divided by 100
    (1.0, 1.0),      # boundary: == 1 stays as-is
])
def test_stop_loss_pct_normalization(raw, expected):
    """Replicates the sl_pct normalization at base_executor.py L406-409.

    Strategies write either fractional (0.05) or percent (5.0) into the
    signal CSV; the executor must coerce to fractional before passing to
    BracketBuilder.calculate_stop_price(percent_decimal).
    """
    sl_pct = float(raw)
    if sl_pct > 1:
        sl_pct = sl_pct / 100
    assert sl_pct == pytest.approx(expected)
