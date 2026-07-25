"""Whale alert: print big trades and the market they're on.

Combines the live stream (Client) with MarketCatalog to turn a trade's
asset_id into a human-readable market question.

    pip install polyflux-client
    POLYFLUX_API_KEY=your_key python examples/whale_alert.py

The first run downloads the active-market catalog (~a few minutes) and caches
it under ./data; later runs start instantly.
"""

import asyncio
import os

from polyflux import Client, MarketCatalog, extract_tag_slugs

WHALE_USD = 1000.0   # alert threshold, in USDC notional


async def main():
    # resolve asset_id -> market question + tags; keep unknown ids fillable
    catalog = MarketCatalog(
        market_fields=["question"],
        fields={"tags": extract_tag_slugs},
        on_miss="background",
    )
    await catalog.start()

    client = Client(os.environ["POLYFLUX_API_KEY"])
    print(f"watching for trades over ${WHALE_USD:,.0f} — Ctrl+C to stop\n")

    async for trade in client.trades():
        if (trade.notional or 0) < WHALE_USD:
            continue
        record = catalog.get(trade.asset_id) or {}
        market = (record.get("market") or {}).get("question", "unknown market")
        tags = ", ".join(record.get("tags", []))
        print(
            f"🐋 ${trade.notional:,.0f}  {trade.side.upper():4}  "
            f"{market}  [{tags}]  by {trade.wallet_address}"
        )


if __name__ == "__main__":
    asyncio.run(main())
