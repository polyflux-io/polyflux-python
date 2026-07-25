"""Minimal example: stream live Polymarket trades.

    pip install polyflux-client
    POLYFLUX_API_KEY=your_key python examples/quickstart.py
"""

import asyncio
import os

from polyflux import Client


async def main():
    client = Client(os.environ["POLYFLUX_API_KEY"])  # get a key at https://polyflux.io
    print("streaming live Polymarket trades — Ctrl+C to stop\n")
    async for trade in client.trades():
        print(f"{trade.side:>4}  {trade.size:>10}  @ {trade.price:<5}  {trade.wallet_address}")


if __name__ == "__main__":
    asyncio.run(main())
