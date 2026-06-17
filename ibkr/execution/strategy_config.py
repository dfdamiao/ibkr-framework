"""
Strategy configuration dataclass.

Each strategy executor creates one StrategyConfig with its specific
parameters. All file paths are derived from strategy_root.
"""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StrategyConfig:
    """Configuration for a strategy executor.

    Args:
        name: Strategy identifier (e.g. "mean_reversion")
        client_id: IBKR client ID for this strategy
        strategy_code: Short code for orderRef (e.g. "MR")
        strategy_root: Root path of the strategy deployment folder
        signal_column: Column name in pending_signals.csv that
            identifies the asset (e.g. "pair", "asset", "ticker")
        action_column: Column for signal action. Defaults to "action".
            Values: "BUY"/"SELL" or "ENTER"/"EXIT" (normalized at load).
        has_bracket_stp: Whether this strategy uses bracket STP orders
        stop_loss_column: Column name for stop-loss percentage in
            pending_signals.csv (None if no STP, or if using absolute
            stop_price_column instead)
        stop_price_column: Column name for ABSOLUTE stop price in
            pending_signals.csv. When set, takes precedence over
            stop_loss_column — used by strategies (e.g. TSMOM) whose
            signal_generator pre-computes the stop level.
        position_dollars_column: Column for position sizing dollars
            (None if sizing is done differently)
    """

    name: str
    client_id: int
    strategy_code: str
    strategy_root: Path
    signal_column: str
    action_column: str = "action"
    has_bracket_stp: bool = False
    stop_loss_column: str | None = None
    stop_price_column: str | None = None
    position_dollars_column: str | None = None
    flat_layout: bool = False  # True = files at root, no deployment/ subfolder

    # Derived paths (computed, not stored)
    @property
    def _base(self) -> Path:
        if self.flat_layout:
            return self.strategy_root
        return self.strategy_root / "deployment"

    @property
    def signals_file(self) -> Path:
        return self._base / "signals" / "pending_signals.csv"

    @property
    def positions_file(self) -> Path:
        return self._base / "positions" / "active_positions.csv"

    @property
    def contract_mapping_file(self) -> Path:
        return self._base / "config" / "contract_mapping.csv"

    @property
    def backup_dir(self) -> Path:
        return self._base / "backups"

    @property
    def log_dir(self) -> Path:
        return self._base / "logs"

    @property
    def closed_trades_file(self) -> Path:
        if self.flat_layout:
            return self.strategy_root / "closed_trades.csv"
        return self.strategy_root / "deployment" / "closed_trades.csv"

    @property
    def portfolio_state_file(self) -> Path:
        return self._base / "portfolio_state.json"
