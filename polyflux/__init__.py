"""Polyflux — the official Python client for streaming real-time Polymarket trades.

    import asyncio
    from polyflux import Client

    async def main():
        client = Client("YOUR_API_KEY")   # get one at https://polyflux.io
        async for trade in client.trades():
            print(trade.side, trade.size, trade.price, trade.wallet_address)

    asyncio.run(main())

Pair the stream with `MarketCatalog` to resolve each trade's asset_id to the
market it's on.
"""

from .client import Client, DEFAULT_URL, PolyfluxError, AuthError
from .models import Trade, Transfer, Resolution
from .catalog import (
    MarketCatalog,
    extract_tag_labels,
    extract_tag_slugs,
)

__version__ = "0.2.0"

__all__ = [
    "Client",
    "Trade",
    "Transfer",
    "Resolution",
    "MarketCatalog",
    "extract_tag_labels",
    "extract_tag_slugs",
    "PolyfluxError",
    "AuthError",
    "DEFAULT_URL",
    "__version__",
]
