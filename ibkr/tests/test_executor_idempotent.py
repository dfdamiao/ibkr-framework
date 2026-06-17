"""Idempotent re-run tests for BaseExecutor.

The 3-phase pipeline persists `entry_perm_id` to the PENDING signal row on
Phase 3 timeout so a subsequent run can skip the row instead of double-submitting.
This tests:

  1. _persist_pending_perm() writes the perm_id to the right row
  2. _persist_pending_perm() is a no-op for invalid perm_ids (0, negative, missing file)
  3. The skip predicate from _execute_entries (L364-365) evaluates correctly:
        skip iff pd.notna(prior_perm) AND str(prior_perm).strip() not in ("", "0")

No TWS connection — uses tmp_path-backed StrategyConfig.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture
def tmp_strategy(tmp_path):
    """Build a StrategyConfig + minimal CSV layout under tmp_path."""
    from ibkr.execution.strategy_config import StrategyConfig

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

    # Seed signals CSV: 2 PENDING rows, 1 already-EXECUTED row
    pd.DataFrame(
        [
            {"ticker": "SPY", "action": "BUY", "status": "PENDING"},
            {"ticker": "QQQ", "action": "BUY", "status": "PENDING"},
            {"ticker": "TLT", "action": "BUY", "status": "EXECUTED"},
        ]
    ).to_csv(cfg.signals_file, index=False)

    return cfg


def test_persist_pending_perm_writes_to_pending_row(tmp_strategy):
    """Calling _persist_pending_perm with a valid perm_id stamps the matching PENDING row."""
    from ibkr.execution.base_executor import BaseExecutor

    ex = BaseExecutor(tmp_strategy)
    signal = pd.Series({"ticker": "SPY", "status": "PENDING"})

    ex._persist_pending_perm(signal, perm_id=998877)

    df = pd.read_csv(tmp_strategy.signals_file)
    spy_row = df[df["ticker"] == "SPY"].iloc[0]
    assert int(spy_row["entry_perm_id"]) == 998877

    # Other rows untouched (no perm_id leak to QQQ or TLT)
    qqq_row = df[df["ticker"] == "QQQ"].iloc[0]
    tlt_row = df[df["ticker"] == "TLT"].iloc[0]
    assert pd.isna(qqq_row["entry_perm_id"])
    assert pd.isna(tlt_row["entry_perm_id"])


def test_persist_pending_perm_noop_for_invalid_perm(tmp_strategy):
    """perm_id <= 0 must NOT write — guards against IBKR-not-yet-assigned (0) values."""
    from ibkr.execution.base_executor import BaseExecutor

    ex = BaseExecutor(tmp_strategy)
    signal = pd.Series({"ticker": "SPY", "status": "PENDING"})

    ex._persist_pending_perm(signal, perm_id=0)
    ex._persist_pending_perm(signal, perm_id=-1)

    df = pd.read_csv(tmp_strategy.signals_file)
    # entry_perm_id column should not even exist (or be all NA)
    if "entry_perm_id" in df.columns:
        assert all(pd.isna(v) for v in df["entry_perm_id"].tolist())


def test_persist_pending_perm_only_targets_pending_status(tmp_strategy):
    """An EXECUTED row matching the asset must NOT be re-stamped — only PENDING."""
    from ibkr.execution.base_executor import BaseExecutor

    ex = BaseExecutor(tmp_strategy)
    signal = pd.Series({"ticker": "TLT", "status": "EXECUTED"})

    ex._persist_pending_perm(signal, perm_id=12345)

    df = pd.read_csv(tmp_strategy.signals_file)
    tlt_row = df[df["ticker"] == "TLT"].iloc[0]
    # mask in _persist_pending_perm requires status == PENDING; TLT is EXECUTED
    if "entry_perm_id" in df.columns:
        assert bool(pd.isna(tlt_row["entry_perm_id"]))


@pytest.mark.parametrize(
    "prior_perm,should_skip",
    [
        (998877, True),  # real perm_id → skip (idempotent)
        (1, True),  # any positive int → skip
        ("", False),  # blank string → submit
        ("0", False),  # literal zero string → submit
        (0, False),  # numeric zero → submit (status pre-IBKR)
        (None, False),  # missing → submit
        (float("nan"), False),  # NaN → submit
    ],
)
def test_skip_predicate(prior_perm, should_skip):
    """Replicates the predicate at base_executor.py L364-365.

    The actual function is wrapped inside _execute_entries; we test the
    predicate as a stand-alone assertion to keep the contract pinned.
    """
    # Build a 1-row DataFrame and read the scalar back the same way
    # _execute_entries does (signal.get("entry_perm_id")).
    row = pd.DataFrame([{"entry_perm_id": prior_perm}]).iloc[0]
    raw = row.get("entry_perm_id")
    notna: bool = bool(pd.notna(raw)) if not isinstance(raw, pd.Series) else False
    is_truthy_string = notna and str(raw).strip() not in ("", "0")
    assert is_truthy_string is should_skip


class _FakeConn:
    """Minimal stand-in: only the .orders map match_fill-time persistence reads."""

    def __init__(self, orders):
        self.orders = orders


def test_persist_entry_perm_ids_stamps_before_monitor(tmp_strategy):
    """Submission-time persistence: each submitted entry's permId is written to
    its PENDING row BEFORE the monitor, so a kill mid-monitor can't leave a bare
    row a re-run would re-submit. permId resolved from conn.orders[orderId]."""
    from ibkr.execution.base_executor import BaseExecutor

    ex = BaseExecutor(tmp_strategy)
    ex.conn = _FakeConn({100: {"permId": 555}, 200: {"permId": 666}})
    ex.entry_order_ids = [
        (100, pd.Series({"ticker": "SPY", "status": "PENDING"})),
        (200, pd.Series({"ticker": "QQQ", "status": "PENDING"})),
    ]

    ex._persist_entry_perm_ids(settle=0.0)

    df = pd.read_csv(tmp_strategy.signals_file)
    assert int(df[df["ticker"] == "SPY"].iloc[0]["entry_perm_id"]) == 555
    assert int(df[df["ticker"] == "QQQ"].iloc[0]["entry_perm_id"]) == 666
    # the EXECUTED TLT row stays untouched
    assert pd.isna(df[df["ticker"] == "TLT"].iloc[0]["entry_perm_id"])


def test_persist_entry_perm_ids_skips_unresolved(tmp_strategy):
    """If permId hasn't landed yet (not in conn.orders), leave the row bare and
    don't raise — _process_results persists it later on terminal status."""
    from ibkr.execution.base_executor import BaseExecutor

    ex = BaseExecutor(tmp_strategy)
    ex.conn = _FakeConn({})  # permId not yet arrived
    ex.entry_order_ids = [(100, pd.Series({"ticker": "SPY", "status": "PENDING"}))]

    ex._persist_entry_perm_ids(settle=0.0)  # settle=0 → no blocking sleep

    df = pd.read_csv(tmp_strategy.signals_file)
    if "entry_perm_id" in df.columns:
        assert pd.isna(df[df["ticker"] == "SPY"].iloc[0]["entry_perm_id"])


def test_persist_pending_perm_handles_missing_file(tmp_path):
    """If signals_file doesn't exist, _persist_pending_perm must not raise."""
    from ibkr.execution.base_executor import BaseExecutor
    from ibkr.execution.strategy_config import StrategyConfig

    root = tmp_path / "missing"
    cfg = StrategyConfig(
        name="missing",
        client_id=99,
        strategy_code="MS",
        strategy_root=root,
        signal_column="ticker",
    )
    ex = BaseExecutor(cfg)
    signal = pd.Series({"ticker": "SPY"})

    # Should silently no-op, not raise
    ex._persist_pending_perm(signal, perm_id=12345)
    assert not Path(cfg.signals_file).exists()
