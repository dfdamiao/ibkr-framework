"""Place a single stock order through TWS / IB Gateway.

SAFETY:
  - Dry-run by default: it builds and prints the order, and places nothing.
  - Pass --place to actually transmit; it uses the PAPER port unless --live.
  - Never run this unattended.

    # dry run (prints the order, no connection):
    python -m examples.place_order --symbol SPY --action BUY --qty 1

    # transmit on the paper account:
    python -m examples.place_order --symbol SPY --action BUY --qty 1 --place
"""
from __future__ import annotations

import argparse
import time

from ibkr.config.exchanges import LIVE_PORT, PAPER_PORT
from ibkr.core.connection import ConnectionManager
from ibkr.core.contracts import build_stock
from ibkr.core.orders import market, set_order_ref
from ibkr.utils.order_ref import build_order_ref


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--action", choices=["BUY", "SELL"], required=True)
    ap.add_argument("--qty", type=int, required=True)
    ap.add_argument("--place", action="store_true",
                    help="actually transmit the order (default: dry run)")
    ap.add_argument("--live", action="store_true",
                    help="use the live port (default: paper)")
    ap.add_argument("--client-id", type=int, default=2)
    args = ap.parse_args()

    contract = build_stock(args.symbol)
    order = market(args.action, args.qty)
    set_order_ref(order, build_order_ref("example", args.symbol))

    if not args.place:
        print("DRY RUN (no order placed). Would transmit:")
        print(f"  {args.action} {args.qty} {args.symbol}  MKT  "
              f"orderRef={order.orderRef}")
        print("  Re-run with --place to transmit on the paper account.")
        return

    port = LIVE_PORT if args.live else PAPER_PORT
    with ConnectionManager(port=port) as conn:
        if not conn.connect_with_retry(client_id=args.client_id):
            raise SystemExit("Could not connect to TWS / IB Gateway.")
        oid = conn.next_order_id()
        conn.placeOrder(oid, contract, order)
        print(f"Placed order {oid}: {args.action} {args.qty} {args.symbol} (MKT)")
        time.sleep(2)  # let status callbacks arrive before disconnect


if __name__ == "__main__":
    main()
