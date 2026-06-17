"""ibkr - a lightweight Interactive Brokers trading framework.

Built directly on the official ``ibapi`` package. Connect to TWS / IB Gateway,
read account state, and place and manage orders.

Layers:
    core/       Connection, contracts, orders, error classification
    account/    Positions, account summary, PnL, order tracking
    execution/  BaseExecutor, SmartRouter, BracketBuilder, FillMonitor
    config/     Client IDs, order types, exchange rules
    utils/      OrderRef, logging, price patch
"""
