# polyflux

[![PyPI](https://img.shields.io/pypi/v/polyflux-client.svg)](https://pypi.org/project/polyflux-client/)
[![Python](https://img.shields.io/pypi/pyversions/polyflux-client.svg)](https://pypi.org/project/polyflux-client/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**Official Python client for [Polyflux](https://polyflux.io) — stream real-time Polymarket trades from the mempool over WebSocket.**

See every Polymarket trade the millisecond it happens — ~3 seconds before it confirms on-chain. Build trading bots, whale alerts, and real-time signals in a few lines of Python.

> You need a Polyflux API key to stream. **[Get one at polyflux.io →](https://polyflux.io/auth)** (free trial).

## Install

```bash
pip install polyflux-client
```
Then `import polyflux` in your code.

## Quickstart

```python
import asyncio
from polyflux import Client

async def main():
    client = Client("YOUR_API_KEY")          # get one at https://polyflux.io
    async for trade in client.trades():
        print(trade.side, trade.size, trade.price, trade.wallet_address)

asyncio.run(main())
```

Each `Trade` gives you `asset_id`, `wallet_address`, `size`, `price`, `side`
(`buy`/`sell`), `timestamp`, plus helpers like `.notional` (USDC value) and
`.time` (UTC datetime). The full raw message is always on `.raw`.

The client handles connection, parsing, and **automatic reconnection** — just
`async for` over `.trades()` and let it run.

## Resolve markets

The feed identifies markets by `asset_id` (a clob token id). Use `MarketCatalog`
to turn that into a human-readable market:

```python
from polyflux import Client, MarketCatalog

catalog = MarketCatalog(market_fields=["question"])
await catalog.start()   # caches the active-market set under ./data

async for trade in client.trades():
    record = catalog.get(trade.asset_id)
    if record:
        print(record["market"]["question"], trade.size, trade.price)
```

`MarketCatalog` keeps an O(1) `asset_id → market` map with a configurable memory
footprint, refreshes in the background, and can fetch unknown ids on demand. See
[`examples/whale_alert.py`](examples/whale_alert.py) for a full bot.

## Beyond trades: flows & resolutions

The feed carries more than trades. Each event has an `event_type`; typed
iterators give you just the ones you want:

```python
# Stablecoin flows — registry-classified, so a "confirmed" flow is a real
# Polymarket movement, not a guess. Deposits are money entering PM wallets;
# p2p transfers are money moving between them.
async for t in client.transfers():          # deposits + p2p together
    print(t.event_type, t.amount, t.token, t.from_address, "->", t.to_address)
# or client.deposits() / client.p2p_transfers() for one kind

# UMA oracle resolutions — how every market ultimately settles.
async for r in client.resolutions():        # propose / dispute / settle
    if r.is_dispute:                         # rare: a proposed answer was challenged
        print("DISPUTED:", r.title, "by", r.disputer)
```

- **`Transfer`** — `event_type` (`deposit`/`p2p_transfer`), `from_address`,
  `to_address`, `amount` (USD), `token` (`USDC`/`USDC.e`/`USDT`/`pUSD`), `tier`,
  `pm_link_reason`, plus `.is_deposit` / `.is_p2p` / `.is_confirmed`.
- **`Resolution`** — `event_type` (`propose`/`dispute`/`settle`), `proposer`,
  `disputer`, `outcome` (`YES`/`NO`/`50-50`), `market_id`, `ancillary_hash`,
  `proposed_price`, `resolved_price`, plus `.title`, `.market_key`, and
  `.is_propose` / `.is_dispute` / `.is_settle`. Group by **`.market_key`** to
  follow a market across oracle resets — it's stable where `market_id` isn't.

To watch several event types over a **single** connection, loop `client.events()`
and build the model per `event_type` (see the example below). The dedicated
iterators each open their own connection, so use one per process.

## Examples

- [`examples/quickstart.py`](examples/quickstart.py) — stream trades
- [`examples/whale_alert.py`](examples/whale_alert.py) — alert on large trades, named by market
- [`examples/catalog_lookup.py`](examples/catalog_lookup.py) — market-catalog projections
- [`examples/resolutions_and_flows.py`](examples/resolutions_and_flows.py) — deposits, p2p transfers & UMA resolutions

```bash
POLYFLUX_API_KEY=your_key python examples/quickstart.py
```

## Links

- **Get an API key:** https://polyflux.io/auth
- **Guides:** https://polyflux.io/blog
- **Live feed / product:** https://polyflux.io

## License

MIT
