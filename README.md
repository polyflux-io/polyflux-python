# polyflux

**Official Python client for [Polyflux](https://polyflux.io) — stream real-time Polymarket trades from the mempool over WebSocket.**

See every Polymarket trade the millisecond it happens — ~3 seconds before it confirms on-chain. Build trading bots, whale alerts, and real-time signals in a few lines of Python.

> You need a Polyflux API key to stream. **[Get one at polyflux.io →](https://polyflux.io/auth)** (free trial).

## Install

```bash
pip install polyflux
```

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

## Examples

- [`examples/quickstart.py`](examples/quickstart.py) — stream trades
- [`examples/whale_alert.py`](examples/whale_alert.py) — alert on large trades, named by market
- [`examples/catalog_lookup.py`](examples/catalog_lookup.py) — market-catalog projections

```bash
POLYFLUX_API_KEY=your_key python examples/quickstart.py
```

## Links

- **Get an API key:** https://polyflux.io/auth
- **Guides:** https://polyflux.io/blog
- **Live feed / product:** https://polyflux.io

## License

MIT
