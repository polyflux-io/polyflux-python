"""Examples of using MarketCatalog: clob_token_id -> event/market data lookups.

Run directly:  python3 market_catalog_example.py

MarketCatalog keeps a raw dump of all active Gamma events on disk
(data/all_events.json) and loads a configurable projection of it into
memory for O(1) lookups by clob token id.

refresh_interval controls freshness:
  - None  -> analysis mode: use the dump as-is, never touch the network
  - 3600  -> re-download the full set when the dump is older than an hour
             and hourly in the background afterwards (default)
"""

import asyncio
import json

from polyflux import MarketCatalog, extract_tag_slugs

# a token id exactly as it arrives with a trade from the chain
SAMPLE_TOKEN = "98022490269692409998126496127597032490334070080325855126491859374983463996227"


async def example_default():
    """Smallest footprint: tag labels only (~67 MB for all active events)."""
    catalog = MarketCatalog(refresh_interval=None)
    await catalog.start()

    print(f"[default] {len(catalog):,} assets in memory")
    print("[default] tags:", catalog.get_tags(SAMPLE_TOKEN))


async def example_trading_projection():
    """The projection agreed for trading use (~160 MB for all active events).

    event_fields / market_fields copy raw API fields ("*" wildcards allowed,
    missing fields omitted); `fields` adds computed values — here tags
    flattened to slugs.
    """
    catalog = MarketCatalog(
        event_fields=["id", "slug", "startDate", "endDate", "liquidity", "volume"],
        market_fields=["id", "question", "conditionId", "startDate", "endDate",
                       "volume*", "gameStartTime"],
        fields={"tags": extract_tag_slugs},
        refresh_interval=None,
    )
    await catalog.start()

    record = catalog.get(SAMPLE_TOKEN)
    print(f"\n[trading] {len(catalog):,} assets in memory")
    print("[trading] full record for one token:")
    print(json.dumps(record, indent=2)[:600], "...")

    # unknown ids (resolved markets, brand-new markets) return None
    print("[trading] unknown token ->", catalog.get("12345"))


async def example_miss_handling():
    """What happens when a looked-up token is not in the db.

    on_miss="ignore" (default): get() just returns None.
    on_miss="background":       get() returns None immediately, but queues a
        batched API fetch (1s window, misses are grouped) that fills the db —
        the next lookup for that token, or its sibling outcome token, hits.
    await resolve(token):       blocking variant — fetches on the spot and
        returns the record; works in both modes.

    Unknown ids the API can't answer are negative-cached for `miss_ttl`
    seconds (default 300) so repeated trades on them don't hammer the API.
    """
    catalog = MarketCatalog(
        fields={"tags": extract_tag_slugs},
        market_fields=["question"],
        refresh_interval=None,
        on_miss="background",
        miss_ttl=300,
    )
    await catalog.start()

    # in-db token: returned instantly, no network involved
    record = await catalog.resolve(SAMPLE_TOKEN)
    print(f"\n[miss] resolve of known token (no API call): {record['market']['question']}")

    # a token from a market created after the dump would go to the API here:
    #   catalog.get(fresh_token)      -> None now, filled ~1s later
    #   await catalog.resolve(fresh_token) -> record right away


async def example_filtered():
    """`keep` decides which markets enter memory at all — here crypto only."""
    catalog = MarketCatalog(
        fields={"tags": extract_tag_slugs},
        market_fields=["question"],
        keep=lambda ev, mk: any(
            t.get("slug") == "crypto" for t in ev.get("tags") or []
        ),
        refresh_interval=None,
    )
    await catalog.start()
    print(f"\n[filtered] crypto-only: {len(catalog):,} assets in memory")


async def main():
    await example_default()
    await example_trading_projection()
    await example_miss_handling()
    await example_filtered()


if __name__ == "__main__":
    asyncio.run(main())
