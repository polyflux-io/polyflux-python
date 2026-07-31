"""Stream the non-trade event types: money flows and market resolutions.

    pip install polyflux-client
    POLYFLUX_API_KEY=your_key python examples/resolutions_and_flows.py

The feed carries more than trades. To watch several event types at once, run a
single `.events()` loop (one connection) and build the right typed model per
event_type. For a bot that only cares about one stream, the dedicated iterators
`.deposits()`, `.p2p_transfers()`, `.resolutions()` are simpler — each opens its
own connection, so don't run several of them concurrently on the same key.
"""

import asyncio
import os

from polyflux import Client, Transfer, Resolution


async def main():
    client = Client(os.environ["POLYFLUX_API_KEY"])  # get a key at https://polyflux.io
    print("streaming deposits, p2p transfers, and resolutions — Ctrl+C to stop\n")

    async for e in client.events():                 # one connection, all event types
        et = e.get("event_type")

        if et == "deposit":
            d = Transfer.from_dict(e)
            print(f"[deposit]  ${d.amount:>12,.2f} {d.token:<6} -> {d.to_address}  "
                  f"({d.pm_link_reason})")

        elif et == "p2p_transfer":
            t = Transfer.from_dict(e)
            print(f"[p2p]      ${t.amount:>12,.2f} {t.token:<6} "
                  f"{t.from_address} -> {t.to_address}")

        elif et in ("propose", "dispute", "settle"):
            r = Resolution.from_dict(e)
            if r.is_dispute:                          # rare — a proposed answer got challenged
                print(f"[DISPUTE]  {r.title!r}  disputer={r.disputer}  key={r.market_key}")
            elif r.is_propose:
                print(f"[propose]  {r.title!r}  outcome={r.outcome}  proposer={r.proposer}")
            else:                                     # settle
                print(f"[settle]   {r.title!r}  final={r.outcome}")


if __name__ == "__main__":
    asyncio.run(main())
