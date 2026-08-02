"""Polyflux streaming client — connect to the Polymarket mempool feed.

    import asyncio
    from polyflux import Client

    async def main():
        client = Client("YOUR_API_KEY")
        async for trade in client.trades():
            print(trade.side, trade.size, trade.price, trade.wallet_address)

    asyncio.run(main())

The client handles connection, JSON parsing, filtering to trade events, and
automatic reconnection with exponential backoff, so a bot can `async for` over
`.trades()` indefinitely without worrying about drops.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import AsyncIterator

import websockets

from .models import Trade, Transfer, Resolution, MoneyFlow

logger = logging.getLogger(__name__)

# event_type groupings used by the typed iterators
_TRANSFER_TYPES = frozenset({"deposit", "p2p_transfer"})
_RESOLUTION_TYPES = frozenset({"propose", "dispute", "settle"})

DEFAULT_URL = "wss://stream.polyflux.io/polymarket"

# WebSocket close codes the server uses to reject a connection permanently:
#   4000 = invalid path (malformed key)   4001 = invalid key (unknown/expired)
#   4003 = forbidden
# Reconnecting on any of these is pointless — fail fast with AuthError.
_AUTH_CLOSE_CODES = {4000, 4001, 4003}
_AUTH_TEXT = ("invalid key", "invalid path", "forbidden", "unauthorized")

_STOP = object()  # sentinel: the reader has ended and no more events will come


class PolyfluxError(Exception):
    """Base class for polyflux client errors."""


class AuthError(PolyfluxError):
    """The API key was rejected (invalid or expired). Get one at https://polyflux.io."""


class Client:
    """A connection to the Polyflux Polymarket feed.

    Parameters
    ----------
    api_key:
        Your Polyflux API key (get one at https://polyflux.io).
    url:
        Base WebSocket URL. The api_key is appended as a path segment.
    reconnect:
        Reconnect automatically on drops (default True). When False, the
        iterators stop if the connection closes.
    max_backoff:
        Cap for the exponential reconnect backoff, in seconds.
    """

    def __init__(
        self,
        api_key: str,
        *,
        url: str = DEFAULT_URL,
        reconnect: bool = True,
        max_backoff: float = 30.0,
        open_timeout: float = 15.0,
        ping_interval: float = 20.0,
    ):
        if not api_key or not isinstance(api_key, str):
            raise ValueError("api_key is required — get one at https://polyflux.io")
        self._api_key = api_key
        self._endpoint = f"{url.rstrip('/')}/{api_key}"
        self._reconnect = reconnect
        self._max_backoff = max_backoff
        self._open_timeout = open_timeout
        self._ping_interval = ping_interval
        self._closed = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def events(self) -> AsyncIterator[dict]:
        """Yield every raw event dict from the feed (trades, redeems, splits…).

        Reconnects automatically unless the client was created with
        reconnect=False. Use `.trades()` if you only want trades.

        The connection runs in a background task, so breaking out of the loop
        (or `close()`) tears it down cleanly — no dangling tasks.
        """
        queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        reader = asyncio.create_task(self._reader(queue))
        try:
            while True:
                item = await queue.get()
                if item is _STOP:
                    break
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            self._closed = True
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reader

    async def _reader(self, queue: asyncio.Queue) -> None:
        """Own the WebSocket lifecycle; push parsed events onto *queue*.

        Runs as a task so cancellation (on consumer break / close) cleanly
        exits the `async with websockets.connect(...)` and closes the socket.
        """
        backoff = 1.0
        try:
            while not self._closed:
                try:
                    async with websockets.connect(
                        self._endpoint,
                        open_timeout=self._open_timeout,
                        ping_interval=self._ping_interval,
                    ) as ws:
                        logger.info("polyflux: connected to %s…", self._endpoint[:40])
                        backoff = 1.0  # reset after a successful connect
                        async for raw in ws:
                            for event in self._parse(raw):
                                await queue.put(event)
                except asyncio.CancelledError:
                    raise
                except websockets.exceptions.ConnectionClosed as exc:
                    msg = str(exc).lower()
                    if exc.code in _AUTH_CLOSE_CODES or any(t in msg for t in _AUTH_TEXT):
                        await queue.put(AuthError(
                            "Polyflux rejected the API key (invalid or expired). "
                            "Get one at https://polyflux.io."
                        ))
                        return
                    logger.warning("polyflux: connection closed: %s", exc)
                except Exception as exc:  # noqa: BLE001 — log + reconnect
                    logger.warning("polyflux: connection error: %s", exc)

                if not self._reconnect or self._closed:
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._max_backoff)
        finally:
            with contextlib.suppress(Exception):
                queue.put_nowait(_STOP)

    async def trades(self) -> AsyncIterator[Trade]:
        """Yield only trade events, as typed `Trade` objects."""
        async for event in self.events():
            if event.get("event_type") == "trade":
                yield Trade.from_dict(event)

    async def transfers(self) -> AsyncIterator[Transfer]:
        """Yield stablecoin flows (deposits **and** p2p transfers) as typed
        `Transfer` objects. Use `.is_deposit` / `.is_p2p` to distinguish, or the
        `.deposits()` / `.p2p_transfers()` iterators for one kind only."""
        async for event in self.events():
            if event.get("event_type") in _TRANSFER_TYPES:
                yield Transfer.from_dict(event)

    async def deposits(self) -> AsyncIterator[Transfer]:
        """Yield only external deposits into Polymarket wallets."""
        async for event in self.events():
            if event.get("event_type") == "deposit":
                yield Transfer.from_dict(event)

    async def money_flows(self) -> AsyncIterator[MoneyFlow]:
        """Yield corrected money-flow events (deposits AND withdrawals) as typed
        `MoneyFlow`. This is the accurate, tx-level feed that supersedes
        `transfers()` / `deposits()` / `p2p_transfers()` (which stream the legacy
        `deposit`/`p2p_transfer` events and are deprecated). Filter with
        `.is_deposit` / `.is_withdrawal` and `.is_external` / `.is_p2p` /
        `.is_unidentified`."""
        async for event in self.events():
            if event.get("event_type") == "money_flow":
                yield MoneyFlow.from_dict(event)

    async def deposits_v2(self) -> AsyncIterator[MoneyFlow]:
        """Corrected deposits only (money entering a PM wallet), as `MoneyFlow`."""
        async for mf in self.money_flows():
            if mf.is_deposit:
                yield mf

    async def withdrawals(self) -> AsyncIterator[MoneyFlow]:
        """Withdrawals only (money leaving a PM wallet), as `MoneyFlow`."""
        async for mf in self.money_flows():
            if mf.is_withdrawal:
                yield mf

    async def p2p_transfers(self) -> AsyncIterator[Transfer]:
        """Yield only wallet-to-wallet transfers between Polymarket wallets."""
        async for event in self.events():
            if event.get("event_type") == "p2p_transfer":
                yield Transfer.from_dict(event)

    async def resolutions(self) -> AsyncIterator[Resolution]:
        """Yield UMA oracle resolution events (propose / dispute / settle) as
        typed `Resolution` objects. Disputes are rare — most events are
        proposals and settlements. Group by `.market_key` to track a market
        across resets."""
        async for event in self.events():
            if event.get("event_type") in _RESOLUTION_TYPES:
                yield Resolution.from_dict(event)

    def close(self) -> None:
        """Signal the iterators to stop after the current connection ends."""
        self._closed = True

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _parse(raw: str | bytes) -> list[dict]:
        """Turn a raw WS message into a list of event dicts.

        The feed sends {"type": "events", "events": [ {...}, ... ]}. We tolerate
        a bare event or a bare list too, so the client survives minor format
        changes without dropping the connection.
        """
        try:
            msg = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return []
        if isinstance(msg, dict):
            if isinstance(msg.get("events"), list):
                return [e for e in msg["events"] if isinstance(e, dict)]
            if msg.get("type") == "events":
                return []
            return [msg]  # a bare event dict
        if isinstance(msg, list):
            return [e for e in msg if isinstance(e, dict)]
        return []
