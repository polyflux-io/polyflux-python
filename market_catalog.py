"""
MarketCatalog — asset_id (clob token id) -> projected event/market data.

Works in three stages:
  1. Warm-up: load the raw events dump from disk if fresh enough, otherwise
     download the full active-event set from the Gamma keyset API and save it.
     The dump keeps *everything* the API returns (disk is cheap).
  2. Projection: build the in-memory mapping from the raw dump according to
     a `fields` spec, so each deployment only pays RAM for the data it will
     actually look up.
  3. Lookup: get(asset_id) / get_tags(asset_id) are O(1) dict reads.

Downloads are streamed: each API page is projected into the new mapping and
appended to the dump file as it arrives, so the full event list is never held
in memory. Lookups keep serving the previous mapping during a refresh; the
swap to the new mapping is a single atomic reference assignment, and a failed
download leaves both the old mapping and the old dump untouched.

The legacy /events offset pagination is deprecated (offsets > 2000 return
HTTP 422), so the download uses /events/keyset with after_cursor. Note the
page size is capped at 100 server-side and a full crawl takes ~5 minutes.

Usage:
    cache = MarketCatalog()             # default projection: tags only
    await cache.start()
    tags = cache.get_tags(asset_id)

    # declarative projection — pick raw fields from each level
    cache = MarketCatalog(
        event_fields=["slug", "title", "tags", "negRisk"],
        market_fields=["question", "conditionId", "outcomes"],
    )
    await cache.start()
    cache.get(asset_id)
    # {"event": {"slug": ..., "title": ..., "tags": [...], "negRisk": ...},
    #  "market": {"question": ..., "conditionId": ..., "outcomes": [...]}}

    # computed projection — each extractor is called as fn(event, market)
    cache = MarketCatalog(fields={
        "tags":     lambda ev, mk: [t["label"] for t in ev.get("tags") or []],
        "question": lambda ev, mk: mk.get("question"),
    })
    await cache.start()
    cache.get(asset_id)  # {"tags": [...], "question": "..."}

Both styles compose: `fields` entries are merged into the same record next to
the "event"/"market" sub-records.
"""

import asyncio
import fnmatch
import json
import logging
import os
import time
from typing import Any, Callable

import requests

logger = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"

_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_RAW_FILE = os.path.join(_DIR, "data", "all_events.json")
_DEFAULT_REFRESH_INTERVAL = 3600  # full re-download cadence; 0/None disables

Extractor = Callable[[dict, dict], Any]


def extract_tag_labels(event: dict, market: dict) -> list[str]:
    """Default extractor: flat list of tag labels from the event."""
    result = []
    for item in event.get("tags") or []:
        if isinstance(item, dict):
            label = item.get("label") or item.get("slug")
            if label:
                result.append(str(label))
        elif isinstance(item, str) and item:
            result.append(item)
    return result


def extract_tag_slugs(event: dict, market: dict) -> list[str]:
    """Flat list of tag slugs from the event."""
    result = []
    for item in event.get("tags") or []:
        if isinstance(item, dict):
            slug = item.get("slug")
            if slug:
                result.append(str(slug))
        elif isinstance(item, str) and item:
            result.append(item)
    return result


DEFAULT_FIELDS: dict[str, Extractor] = {"tags": extract_tag_labels}

# Market fields the API returns as JSON-encoded strings; decoded on selection.
_JSON_STRING_FIELDS = {"clobTokenIds", "outcomes", "outcomePrices"}


def _select(obj: dict, names: list[str]) -> dict[str, Any]:
    """Project *names* out of *obj*.

    Names may contain ``*`` wildcards (fnmatch style, e.g. ``volume*``).
    Missing fields are omitted from the result rather than stored as None.
    """
    keys: list[str] = []
    for name in names:
        if "*" in name or "?" in name:
            keys.extend(k for k in obj if fnmatch.fnmatchcase(k, name))
        elif name in obj:
            keys.append(name)

    out = {}
    for key in keys:
        value = obj[key]
        if value is None:
            continue
        if key in _JSON_STRING_FIELDS and isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                pass
        out[key] = value
    return out


class MarketCatalog:
    """Asset-id lookup over Gamma events with a configurable memory footprint.

    Parameters
    ----------
    fields:
        Mapping of record key -> extractor(event, market) for computed values.
    event_fields / market_fields:
        Field names copied verbatim from the event / market object into the
        record's "event" / "market" sub-dicts. Names may use ``*`` wildcards
        (e.g. "volume*" selects every volume field); missing/null fields are
        omitted from the record. Market fields the API encodes as JSON
        strings (clobTokenIds, outcomes, outcomePrices) are decoded.
        The event sub-dict is built once per event and shared by all its
        markets, so wide event selections stay cheap in memory.
        If neither `fields` nor `event_fields`/`market_fields` are given, the
        default projection is {"tags": <flat list of tag labels>}.
    keep:
        Optional predicate(event, market) -> bool. Markets it rejects are not
        loaded into memory at all (e.g. keep only events tagged "Crypto").
    raw_file:
        Path of the raw events dump (full API payload, one JSON array).
    refresh_interval:
        Seconds between background full re-downloads. 0 or None means warm up
        once and never touch the network again (analysis mode) as long as the
        dump exists.
    on_miss:
        What get()/get_tags() do for an unknown asset id.
        "ignore" (default): just return None.
        "background": return None immediately, then fetch the market from the
        API in a batched background task and fill the mapping, so subsequent
        lookups hit. For a blocking fetch instead, call `await resolve(id)` —
        that works in either mode.
        Failed/unknown ids are negative-cached for `miss_ttl` seconds so a
        stream of trades on an unindexed market doesn't hammer the API.
    """

    def __init__(
        self,
        fields: dict[str, Extractor] | None = None,
        event_fields: list[str] | None = None,
        market_fields: list[str] | None = None,
        keep: Callable[[dict, dict], bool] | None = None,
        raw_file: str = _DEFAULT_RAW_FILE,
        refresh_interval: int | None = _DEFAULT_REFRESH_INTERVAL,
        on_miss: str = "ignore",
        miss_ttl: int = 300,
    ):
        if on_miss not in ("ignore", "background"):
            raise ValueError(f"on_miss must be 'ignore' or 'background', got {on_miss!r}")
        if fields is None and not event_fields and not market_fields:
            fields = DEFAULT_FIELDS
        self._fields = dict(fields) if fields else {}
        self._event_fields = list(event_fields) if event_fields else []
        self._market_fields = list(market_fields) if market_fields else []
        self._keep = keep
        self._raw_file = raw_file
        self._refresh_interval = refresh_interval or 0
        self._on_miss = on_miss
        self._miss_ttl = miss_ttl
        self._mapping: dict[str, dict[str, Any]] = {}
        self._miss_at: dict[str, float] = {}   # asset_id -> last failed fetch ts
        self._pending: set[str] = set()        # queued for background resolution
        self._flush_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self):
        """Warm up (disk or API) and start the refresh loop if enabled."""
        loop = asyncio.get_event_loop()
        self._mapping = await loop.run_in_executor(None, self._warm_up)
        logger.info("market_catalog: %d asset mappings in memory", len(self._mapping))
        if self._refresh_interval:
            asyncio.create_task(self._refresh_loop())

    def get(self, asset_id: str) -> dict[str, Any] | None:
        """Projected record for *asset_id*, or None if unknown. O(1).

        With on_miss="background", an unknown id additionally queues a
        batched API fetch so later lookups for it (and its sibling outcome
        token) succeed.
        """
        record = self._mapping.get(asset_id)
        if record is None and self._on_miss == "background":
            self._schedule_resolve(asset_id)
        return record

    async def resolve(self, asset_id: str) -> dict[str, Any] | None:
        """Like get(), but on a miss fetches the market from the API right
        away, fills the mapping, and returns the record (None if the API
        doesn't know the id either, negative-cached for miss_ttl)."""
        record = self._mapping.get(asset_id)
        if record is not None:
            return record
        if not self._miss_expired(asset_id):
            return None
        await self._resolve_ids([asset_id])
        return self._mapping.get(asset_id)

    def get_tags(self, asset_id: str) -> list[str] | None:
        """Tag labels for *asset_id*. O(1).

        Works with the default/computed "tags" field, or falls back to raw
        event tags when the projection used event_fields=["tags", ...].
        """
        record = self.get(asset_id)
        if record is None:
            return None
        if "tags" in record:
            return record["tags"]
        event_proj = record.get("event")
        if event_proj and "tags" in event_proj:
            return extract_tag_labels({"tags": event_proj["tags"]}, {})
        return None

    def __len__(self) -> int:
        return len(self._mapping)

    # ------------------------------------------------------------------
    # Warm-up: raw dump on disk, refreshed from the keyset API when stale
    # ------------------------------------------------------------------

    def _warm_up(self) -> dict[str, dict[str, Any]]:
        age = self._raw_age()
        fresh = age is not None and (
            not self._refresh_interval or age < self._refresh_interval
        )
        if fresh:
            mapping = self._load_raw(age)
            if mapping is not None:
                return mapping
        return self._download_and_build()

    def _raw_age(self) -> float | None:
        try:
            return time.time() - os.path.getmtime(self._raw_file)
        except OSError:
            return None

    def _load_raw(self, age: float) -> dict[str, dict[str, Any]] | None:
        """Build the mapping from the on-disk dump, streaming when possible.

        Dumps written by this module hold one event per line, so they are
        projected one event at a time — memory never exceeds the mapping
        plus a single event. Dumps in any other layout (e.g. pretty-printed
        by hand) fall back to a whole-file parse.
        """
        try:
            mapping: dict[str, dict[str, Any]] = {}
            n = 0
            for event in self._iter_raw_events():
                self._project_event(mapping, event)
                n += 1
            logger.info(
                "market_catalog: streamed %d events from %s (age %.0fs)",
                n, self._raw_file, age,
            )
            return mapping
        except json.JSONDecodeError:
            pass  # not one-event-per-line; parse the whole file below
        except OSError as exc:
            logger.warning("market_catalog: failed to read raw dump: %s", exc)
            return None

        try:
            with open(self._raw_file) as f:
                events = json.load(f)
            mapping = {}
            for event in events:
                self._project_event(mapping, event)
            logger.info(
                "market_catalog: loaded %d events from %s (age %.0fs, whole-file parse)",
                len(events), self._raw_file, age,
            )
            return mapping
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("market_catalog: failed to load raw dump: %s", exc)
            return None

    def _iter_raw_events(self):
        """Yield events from a dump written by _download_and_build.

        That writer emits ``[`` + one JSON event per line (comma-separated)
        + ``]``, which is both a valid JSON array for external tools and
        line-parseable here without holding the array in memory.
        """
        with open(self._raw_file) as f:
            for line in f:
                line = line.strip()
                if line.startswith("["):
                    line = line[1:]
                if line.endswith("]"):
                    line = line[:-1]
                if line.endswith(","):
                    line = line[:-1]
                if line:
                    yield json.loads(line)

    # ------------------------------------------------------------------
    # Download (Gamma keyset API) — streamed page by page
    # ------------------------------------------------------------------

    @staticmethod
    def _fetch_page(cursor: str | None) -> dict:
        url = f"{GAMMA_API}/events/keyset?active=true&closed=false&limit=100"
        if cursor:
            url += f"&after_cursor={cursor}"

        for attempt in range(3):
            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code == 200:
                    return resp.json()
                logger.warning("market_catalog: HTTP %d", resp.status_code)
            except requests.RequestException as exc:
                logger.warning("market_catalog: fetch error: %s", exc)
            time.sleep(2 * (attempt + 1))
        raise RuntimeError("market_catalog: page fetch failed after 3 attempts")

    def _download_and_build(self) -> dict[str, dict[str, Any]]:
        """Download all active events, projecting and persisting page by page.

        Each page is projected straight into the new mapping and appended to
        the dump file, so memory holds one page of raw events at a time plus
        the mapping being built. The dump is written to a temp file and only
        replaces the previous one after the download completes, so a failure
        keeps the old dump (and raises, keeping the old mapping).
        """
        mapping: dict[str, dict[str, Any]] = {}
        seen: set[str] = set()
        cursor = None
        n_events = 0

        os.makedirs(os.path.dirname(self._raw_file), exist_ok=True)
        tmp = self._raw_file + ".tmp"
        try:
            with open(tmp, "w") as f:
                f.write("[")
                while True:
                    data = self._fetch_page(cursor)
                    batch = data.get("events") or []
                    for event in batch:
                        if event["id"] in seen:
                            continue
                        seen.add(event["id"])
                        if n_events:
                            f.write(",\n")
                        f.write(json.dumps(event))
                        n_events += 1
                        self._project_event(mapping, event)

                    cursor = data.get("next_cursor")
                    if not batch or not cursor:
                        break
                    time.sleep(0.2)
                f.write("]")
            os.replace(tmp, self._raw_file)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

        logger.info(
            "market_catalog: downloaded %d events, %d assets mapped",
            n_events, len(mapping),
        )
        return mapping

    # ------------------------------------------------------------------
    # Projection
    # ------------------------------------------------------------------

    def _project_event(self, mapping: dict[str, dict[str, Any]], event: dict):
        """Project one event's markets into *mapping*."""
        # built once per event, shared by every market/token of the event
        event_proj = (
            _select(event, self._event_fields) if self._event_fields else None
        )

        for market in event.get("markets", []):
            if self._keep and not self._keep(event, market):
                continue

            raw = market.get("clobTokenIds", "[]")
            if isinstance(raw, str):
                try:
                    token_ids = json.loads(raw)
                except json.JSONDecodeError:
                    continue
            else:
                token_ids = raw
            if not token_ids:
                continue

            try:
                record: dict[str, Any] = {}
                if event_proj is not None:
                    record["event"] = event_proj
                if self._market_fields:
                    record["market"] = _select(market, self._market_fields)
                for name, fn in self._fields.items():
                    record[name] = fn(event, market)
            except Exception as exc:
                logger.warning(
                    "market_catalog: extractor failed on market %s: %s",
                    market.get("id"), exc,
                )
                continue

            # one shared record per market — both outcome tokens point at it
            for asset_id in token_ids:
                if asset_id:
                    mapping[str(asset_id)] = record

    # ------------------------------------------------------------------
    # Miss resolution (single-market lookups by clob token id)
    # ------------------------------------------------------------------

    def _miss_expired(self, asset_id: str) -> bool:
        ts = self._miss_at.get(asset_id)
        return ts is None or (time.time() - ts) >= self._miss_ttl

    def _schedule_resolve(self, asset_id: str):
        if asset_id in self._pending or not self._miss_expired(asset_id):
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return  # no event loop (sync/analysis context) — stay passive
        self._pending.add(asset_id)
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = loop.create_task(self._flush_pending())

    async def _flush_pending(self):
        await asyncio.sleep(1.0)  # let a burst of misses accumulate
        while self._pending:
            batch = [self._pending.pop() for _ in range(min(20, len(self._pending)))]
            try:
                await self._resolve_ids(batch)
            except Exception as exc:
                logger.warning("market_catalog: miss fetch failed: %s", exc)
                now = time.time()
                for asset_id in batch:
                    self._miss_at[asset_id] = now

    async def _resolve_ids(self, asset_ids: list[str]):
        loop = asyncio.get_event_loop()
        markets = await loop.run_in_executor(
            None, self._fetch_markets_by_tokens, asset_ids,
        )
        for market in markets:
            # Rebuild an event object the projection understands: the events
            # embedded in /markets responses carry no tags, but with
            # include_tag=true the market itself does.
            embedded = (market.get("events") or [{}])[0]
            event = dict(embedded)
            event["tags"] = market.get("tags") or []
            event["markets"] = [market]
            self._project_event(self._mapping, event)

        now = time.time()
        for asset_id in asset_ids:
            if asset_id in self._mapping:
                self._miss_at.pop(asset_id, None)
            else:
                self._miss_at[asset_id] = now
        if len(self._miss_at) > 10_000:  # prune expired negative entries
            self._miss_at = {
                a: ts for a, ts in self._miss_at.items()
                if (now - ts) < self._miss_ttl
            }

    @staticmethod
    def _fetch_markets_by_tokens(asset_ids: list[str]) -> list[dict]:
        query = "&".join(f"clob_token_ids={a}" for a in asset_ids)
        url = f"{GAMMA_API}/markets?include_tag=true&{query}"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []

    # ------------------------------------------------------------------
    # Background refresh
    # ------------------------------------------------------------------

    async def _refresh_loop(self):
        loop = asyncio.get_event_loop()
        while True:
            await asyncio.sleep(self._refresh_interval)
            try:
                mapping = await loop.run_in_executor(
                    None, self._download_and_build,
                )
                if not mapping:
                    continue
                self._mapping = mapping
                logger.info(
                    "market_catalog: refreshed, %d asset mappings", len(mapping),
                )
            except Exception as exc:
                logger.warning(
                    "market_catalog: refresh failed, keeping previous data: %s", exc,
                )
