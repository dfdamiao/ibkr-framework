"""Build a bracket order (adaptive-market entry + protective stop) and,
with --place, transmit it on the paper account.

A bracket is submitted atomically: the parent entry is held (transmit=False)
until the child stop is attached, then the child submits the whole bracket.

SAFETY: dry-run by default; --place transmits (paper port unless --live).

    # dry run:
    python -m examples.attach_stop --symbol SPY --action BUY --qty 1 --stop 500

    # transmit on the paper account:
    python -m examples.attach_stop --symbol SPY --action BUY --qty 1 --stop 500 --place
"""
from __future__ import annotations

import argparse
import time

from ibkr.config.exchanges import LIVE_PORT, PAPER_PORT
from ibkr.core.connection import ConnectionManager
from ibkr.core.contracts import build_stock
from ibkr.core.orders import bracket_entry, set_order_ref
from ibkr.utils.order_ref import build_order_ref


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--action", choices=["BUY", "SELL"], required=True)
    ap.add_argument("--qty", type=int, required=True)
    ap.add_argument("--stop", type=float, required=True,
                    help="stop-loss trigger price (tick-align it yourself)")
    ap.add_argument("--place", action="store_true",
                    help="actually transmit the bracket (default: dry run)")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--client-id", type=int, default=2)
    args = ap.parse_args()

    contract = build_stock(args.symbol)

    if not args.place:
        print("DRY RUN (no order placed). Would transmit a bracket:")
        print(f"  parent: {args.action} {args.qty} {args.symbol} (adaptive MKT)")
        exit_side = "SELL" if args.action == "BUY" else "BUY"
        print(f"  child : {exit_side} {args.qty} {args.symbol} STP @ {args.stop}")
        print("  Re-run with --place to transmit on the paper account.")
        return

    port = LIVE_PORT if args.live else PAPER_PORT
    with ConnectionManager(port=port) as conn:
        if not conn.connect_with_retry(client_id=args.client_id):
            raise SystemExit("Could not connect to TWS / IB Gateway.")
        parent_id = conn.next_order_id()
        parent, child = bracket_entry(
            args.action, args.qty, args.stop, parent_id,
        )
        set_order_ref(parent, build_order_ref("example", args.symbol))
        child.orderId = conn.next_order_id()

        conn.placeOrder(parent_id, contract, parent)
        conn.placeOrder(child.orderId, contract, child)
        print(f"Placed bracket: parent {parent_id} + stop {child.orderId} "
              f"@ {args.stop}")
        time.sleep(2)


if __name__ == "__main__":
    main()
