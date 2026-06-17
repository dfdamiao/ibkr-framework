"""Connect to TWS / IB Gateway and print the account summary + open positions.

Read-only: this places no orders. Start TWS or IB Gateway with the API enabled,
then run against a PAPER account first.

    python -m examples.account_overview              # paper port (7497)
    python -m examples.account_overview --live       # live port (7496)
"""
from __future__ import annotations

import argparse

from ibkr.account.positions import get_active_positions
from ibkr.account.summary import get_account_summary
from ibkr.config.exchanges import LIVE_PORT, PAPER_PORT
from ibkr.core.connection import ConnectionManager


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true",
                    help="use the live port (default: paper)")
    ap.add_argument("--client-id", type=int, default=1)
    args = ap.parse_args()
    port = LIVE_PORT if args.live else PAPER_PORT

    with ConnectionManager(port=port) as conn:
        if not conn.connect_with_retry(client_id=args.client_id):
            raise SystemExit(
                "Could not connect. Is TWS / IB Gateway running with the API "
                "enabled and the port matching --live/paper?"
            )

        s = get_account_summary(conn)
        print(f"Account         : {s.account}")
        print(f"Net liquidation : {s.net_liquidation:,.0f}")
        print(f"Total cash      : {s.total_cash:,.0f}")
        print(f"Buying power    : {s.buying_power:,.0f}")
        print(f"Margin used     : {s.margin_used_pct:.1%}")

        positions = get_active_positions(conn)
        print(f"\nOpen positions  : {len(positions)}")
        for p in positions.values():
            side = "LONG " if p.is_long else "SHORT"
            print(f"  {side} {p.quantity:>8.0f}  {p.symbol:<8} @ {p.avg_cost:.2f}")


if __name__ == "__main__":
    main()
