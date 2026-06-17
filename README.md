# ibkr-framework

A lightweight **Interactive Brokers (TWS API)** trading framework, built
directly on the official [`ibapi`](https://interactivebrokers.github.io/) client.
It gives you a clean, synchronous-feeling layer over the raw asynchronous API:
connect, read account state and positions, build and place orders, attach
protective stops, and track fills.

> ⚠️ **This connects to a live brokerage API and can place real orders.**
> Trading carries substantial risk of loss. Use entirely at your own risk and
> **always test against a PAPER account first.** Not affiliated with Interactive
> Brokers. See [NOTICE](NOTICE).

## Why

The raw TWS API is callback-driven and stateful: every request is asynchronous,
order IDs must be tracked, and connection lifecycle is fiddly. This framework
wraps that into small, testable pieces with synchronous helpers
(`get_positions_sync`, `get_account_summary`, ...) so you can focus on logic.

## Layers

```
ibkr/
  core/        ConnectionManager (lifecycle, order-id tracking, sync request
               helpers), contract builders, order builders, error classification
  account/     account summary, positions, PnL, order tracking
  execution/   BaseExecutor, order router, bracket builder, fill monitor
  config/      exchanges/ports, order types, client-id helpers
  utils/       orderRef tagging, logging setup, historical price patch
```

## Install

```bash
git clone https://github.com/dfdamiao/ibkr-framework.git
cd ibkr-framework
pip install -e .
```

Python >= 3.11. You also need:
- The **TWS API client** (`ibapi`), installed from the
  [IBKR TWS API package](https://interactivebrokers.github.io/) or `pip install ibapi`
  (tested with 10.37.2).
- **TWS** or **IB Gateway** running, with the API enabled
  (Configure → API → Settings → "Enable ActiveX and Socket Clients"). Default
  ports: 7497 paper, 7496 live.

## Quick start

**Account overview** (read-only, places nothing):

```bash
python -m examples.account_overview          # paper port 7497
```

```
Account         : DU1234567
Net liquidation : 100,000
Total cash      : 100,000
Buying power    : 400,000
Margin used     : 0.0%

Open positions  : 2
  LONG       100  SPY      @ 498.21
  SHORT       50  QQQ      @ 431.10
```

**Place an order** (dry-run by default; `--place` transmits on paper):

```bash
python -m examples.place_order --symbol SPY --action BUY --qty 1            # dry run
python -m examples.place_order --symbol SPY --action BUY --qty 1 --place    # paper
```

**Entry + protective stop** as an atomic bracket:

```bash
python -m examples.attach_stop --symbol SPY --action BUY --qty 1 --stop 490 --place
```

## Using the framework directly

```python
from ibkr.core.connection import ConnectionManager
from ibkr.core.contracts import build_stock
from ibkr.core.orders import market, set_order_ref
from ibkr.account.summary import get_account_summary
from ibkr.config.exchanges import PAPER_PORT

with ConnectionManager(port=PAPER_PORT) as conn:
    conn.connect_with_retry(client_id=1)

    summary = get_account_summary(conn)
    print(summary.net_liquidation)

    order = market("BUY", 1)
    set_order_ref(order, "demo|SPY")
    conn.placeOrder(conn.next_order_id(), build_stock("SPY"), order)
```

Order builders in `ibkr.core.orders`: `market`, `adaptive_market`, `relative`,
`stop_market`, `bracket_entry`. Contract builders in `ibkr.core.contracts`:
`build_stock`, `from_conid`, `resolve_conid`, plus tick-size helpers
(`get_tick_size`, `round_to_tick`).

## Configuration

- **Ports / exchanges**: `ibkr/config/exchanges.py` (`PAPER_PORT`, `LIVE_PORT`,
  `GATEWAY_LIVE_PORT`).
- **Client IDs**: every concurrent connection needs a unique integer (0-99).
  `ibkr/config/client_ids.py` has a `validate_client_id` helper and an example map.
- **Order types**: `ibkr/config/order_types.py`.

## Testing

```bash
pip install -e ".[dev]"
pytest -q          # 96 framework unit tests, no live connection required
```

The unit tests mock the IB API, so they run without TWS/Gateway. The scripts in
`ibkr/tests/integration/` are manual, connection-level checks you run yourself
against a paper account (they are not part of the pytest run).

## License

MIT (see [LICENSE](LICENSE)). Disclaimer and third-party attribution for `ibapi`
are in [NOTICE](NOTICE).
