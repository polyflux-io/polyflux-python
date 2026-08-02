"""Typed representations of the events Polyflux streams.

The feed emits several event types, each with a ``event_type`` field:

* ``trade``                      — a matched Polymarket trade  -> :class:`Trade`
* ``money_flow``                 — corrected deposit/withdrawal x type
                                   -> :class:`MoneyFlow`  (supersedes Transfer)
* ``deposit`` / ``p2p_transfer`` — legacy stablecoin flows (deprecated)
                                   -> :class:`Transfer`
* ``propose`` / ``dispute`` / ``settle`` — UMA oracle resolution lifecycle
                                   -> :class:`Resolution`

Every model keeps the untouched wire message on ``.raw`` so newly-added fields
are always reachable even before this library types them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_time(value: Any) -> datetime | None:
    """Parse a wire timestamp into an aware UTC datetime.

    The feed sends ISO-8601 strings (e.g. ``2026-07-31T15:31:57.9+00:00``);
    epoch seconds/milliseconds are also tolerated for forward-compatibility.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = value / 1000.0 if value > 1e12 else value
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            f = _to_float(value)
            return _to_time(f) if f is not None else None
    return None


@dataclass(slots=True)
class Trade:
    """A single Polymarket trade, as surfaced from the mempool.

    The raw message is always kept on `.raw`; the typed attributes are the
    common fields. Unknown/extra fields stay accessible via `.raw`.
    """

    asset_id: str | None
    wallet_address: str | None
    size: float | None
    price: float | None
    operation_type: str | None   # "buy" | "sell"
    timestamp: float | None      # epoch seconds (may include ms precision)
    raw: dict = field(default_factory=dict, repr=False)

    # --- convenience accessors -------------------------------------------
    @property
    def side(self) -> str | None:
        """Alias for operation_type ("buy" / "sell")."""
        return self.operation_type

    @property
    def is_buy(self) -> bool:
        return (self.operation_type or "").lower() == "buy"

    @property
    def notional(self) -> float | None:
        """size * price — the USDC value of the trade, when both are known."""
        if self.size is None or self.price is None:
            return None
        return self.size * self.price

    @property
    def time(self) -> datetime | None:
        """UTC datetime of the trade, if a timestamp is present."""
        if self.timestamp is not None:
            ts = self.timestamp
            if ts > 1e12:  # milliseconds
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        # feed may send an ISO string rather than an epoch — fall back to raw
        return _to_time(self.raw.get("timestamp"))

    @classmethod
    def from_dict(cls, d: dict) -> "Trade":
        return cls(
            asset_id=d.get("asset_id") or d.get("assetId") or d.get("token_id"),
            wallet_address=d.get("wallet_address") or d.get("walletAddress"),
            size=_to_float(d.get("size")),
            price=_to_float(d.get("price")),
            operation_type=d.get("operation_type") or d.get("operationType") or d.get("side"),
            timestamp=_to_float(d.get("timestamp")),
            raw=d,
        )

    def __repr__(self) -> str:  # concise, readable
        w = (self.wallet_address or "?")[:10]
        return (f"Trade({self.operation_type} size={self.size} "
                f"price={self.price} wallet={w}… asset={str(self.asset_id)[:8]}…)")


@dataclass(slots=True)
class Transfer:
    """A stablecoin movement classified against the Polymarket wallet registry.

    Two flavours, distinguished by :attr:`event_type`:

    * ``deposit``      — external stablecoin arriving into a known PM wallet
                         (a *funder* wallet, not an owner EOA).
    * ``p2p_transfer`` — a stablecoin transfer between two known PM wallets.

    Every classification is registry-backed (exact set membership, no
    heuristics), so a `confirmed` transfer is a true PM flow, not a guess.
    """

    event_type: str | None       # "deposit" | "p2p_transfer"
    from_address: str | None     # sender
    to_address: str | None       # recipient
    amount: float | None         # USD value moved
    token: str | None            # "USDC" | "USDC.e" | "USDT" | "pUSD"
    tier: str | None             # "confirmed" | "unconfirmed"
    pm_link_reason: str | None   # why this counts as a Polymarket flow
    block_number: int | None
    tx_hash: str | None
    timestamp: Any = None        # ISO-8601 string on the wire
    raw: dict = field(default_factory=dict, repr=False)

    @property
    def is_deposit(self) -> bool:
        return self.event_type == "deposit"

    @property
    def is_p2p(self) -> bool:
        return self.event_type == "p2p_transfer"

    @property
    def is_confirmed(self) -> bool:
        """True for the confirmed tier (we never stream unconfirmed today,
        but the field is here so downstream code can stay tier-aware)."""
        return (self.tier or "") == "confirmed"

    @property
    def time(self) -> datetime | None:
        return _to_time(self.timestamp)

    @classmethod
    def from_dict(cls, d: dict) -> "Transfer":
        return cls(
            event_type=d.get("event_type"),
            from_address=d.get("from_address") or d.get("fromAddress"),
            to_address=d.get("to_address") or d.get("toAddress"),
            amount=_to_float(d.get("amount")),
            token=d.get("token"),
            tier=d.get("tier"),
            pm_link_reason=d.get("pm_link_reason"),
            block_number=_to_int(d.get("block_number")),
            tx_hash=d.get("tx_hash"),
            timestamp=d.get("timestamp"),
            raw=d,
        )

    def __repr__(self) -> str:
        f = (self.from_address or "?")[:10]
        t = (self.to_address or "?")[:10]
        kind = "deposit" if self.is_deposit else "p2p"
        return (f"Transfer({kind} {self.amount} {self.token} "
                f"{f}… -> {t}… [{self.tier}])")


@dataclass(slots=True)
class MoneyFlow:
    """A corrected money-flow event: value entering (``deposit``) or leaving
    (``withdrawal``) a Polymarket wallet, classified by where it went.

    Supersedes :class:`Transfer` (``deposit``/``p2p_transfer``). It is tx-level, so
    trade settlements are no longer mislabelled as p2p, and it adds withdrawals.
    Each event is ``op`` x :attr:`type`:

    * ``op``   — ``deposit`` (money in) / ``withdrawal`` (money out), from the
                 perspective of :attr:`wallet_address`.
    * ``type`` — ``external`` (crossed the Polymarket boundary — real money in/out
                 that changes platform balances), ``p2p`` (to/from another PM wallet
                 — internal redistribution, nets to zero platform-wide), or
                 ``unidentified`` (counterparty we could not positively classify —
                 may be an external transfer or an undetected peer).

    A p2p transfer surfaces as two events: the sender's ``withdrawal``/``p2p`` and
    the recipient's ``deposit``/``p2p``.
    """

    op: str | None            # "deposit" | "withdrawal"
    type: str | None          # "external" | "p2p" | "unidentified"
    wallet_address: str | None
    counterparty: str | None
    amount: float | None      # USD value moved
    token: str | None         # "USDC" | "USDC.e" | "USDT" | "pUSD"
    direction: str | None     # "in" | "out"
    block_number: int | None
    tx_hash: str | None
    timestamp: Any = None     # ISO-8601 string on the wire
    raw: dict = field(default_factory=dict, repr=False)

    @property
    def is_deposit(self) -> bool:
        return self.op == "deposit"

    @property
    def is_withdrawal(self) -> bool:
        return self.op == "withdrawal"

    @property
    def is_external(self) -> bool:
        """Real money crossing the Polymarket boundary (changes platform balances)."""
        return self.type == "external"

    @property
    def is_p2p(self) -> bool:
        """Transfer to/from another PM wallet — nets to zero platform-wide."""
        return self.type == "p2p"

    @property
    def is_unidentified(self) -> bool:
        """Counterparty could not be classified (external, or an undetected peer)."""
        return self.type == "unidentified"

    @property
    def time(self) -> datetime | None:
        return _to_time(self.timestamp)

    @classmethod
    def from_dict(cls, d: dict) -> "MoneyFlow":
        return cls(
            op=d.get("op"),
            type=d.get("type") or d.get("flow_type"),
            wallet_address=d.get("wallet_address") or d.get("walletAddress"),
            counterparty=d.get("counterparty"),
            amount=_to_float(d.get("amount")),
            token=d.get("token"),
            direction=d.get("direction"),
            block_number=_to_int(d.get("block_number")),
            tx_hash=d.get("tx_hash"),
            timestamp=d.get("timestamp"),
            raw=d,
        )

    def __repr__(self) -> str:
        w = (self.wallet_address or "?")[:10]
        cp = (self.counterparty or "-")[:10]
        return (f"MoneyFlow({self.op}/{self.type} {self.amount} {self.token} "
                f"wallet={w}… cp={cp}…)")


@dataclass(slots=True)
class Resolution:
    """A UMA Optimistic-Oracle resolution event for a Polymarket market.

    The market's answer moves through a lifecycle, one event per stage:

    * ``propose`` — someone proposes the outcome (a bonded price)
    * ``dispute`` — someone challenges it (forces a reset or, on the 2nd
                    dispute, escalation to the UMA DVM token-holder vote)
    * ``settle``  — the answer is finalized and the bond paid out

    Group related events with :attr:`market_key` (the ancillary hash), which is
    stable across every re-request/reset of the same question — unlike
    :attr:`market_id`, which older market formats don't carry.
    """

    event_type: str | None       # "propose" | "dispute" | "settle"
    oracle_contract: str | None
    requester: str | None        # the adapter that asked the oracle
    proposer: str | None
    disputer: str | None
    identifier: str | None       # UMA price identifier (e.g. YES_OR_NO_QUERY)
    oracle_timestamp: int | None # identifies the request/round
    market_id: str | None        # Polymarket market id (absent in older formats)
    ancillary_hash: str | None   # sha256 of ancillary bytes — stable market key
    proposed_price: float | None
    resolved_price: float | None
    expiration_time: int | None
    payout: str | None
    currency: str | None
    outcome: str | None          # "YES" | "NO" | "50-50" | raw scalar
    ancillary_text: str | None   # question text + resolution rules (truncated)
    block_number: int | None
    tx_hash: str | None
    timestamp: Any = None
    raw: dict = field(default_factory=dict, repr=False)

    @property
    def is_propose(self) -> bool:
        return self.event_type == "propose"

    @property
    def is_dispute(self) -> bool:
        return self.event_type == "dispute"

    @property
    def is_settle(self) -> bool:
        return self.event_type == "settle"

    @property
    def market_key(self) -> str | None:
        """Stable grouping key for a market across resets: the ancillary hash,
        falling back to market_id when a hash isn't present."""
        return self.ancillary_hash or self.market_id

    @property
    def title(self) -> str | None:
        """The market question title, parsed out of the ancillary text."""
        text = self.ancillary_text or ""
        i = text.find("title:")
        if i == -1:
            return None
        rest = text[i + len("title:"):]
        j = rest.find(", description")
        return (rest[:j] if j != -1 else rest).strip() or None

    @property
    def time(self) -> datetime | None:
        return _to_time(self.timestamp)

    @classmethod
    def from_dict(cls, d: dict) -> "Resolution":
        return cls(
            event_type=d.get("event_type"),
            oracle_contract=d.get("oracle_contract"),
            requester=d.get("requester"),
            proposer=d.get("proposer"),
            disputer=d.get("disputer"),
            identifier=d.get("identifier"),
            oracle_timestamp=_to_int(d.get("oracle_timestamp")),
            market_id=d.get("market_id"),
            ancillary_hash=d.get("ancillary_hash"),
            proposed_price=_to_float(d.get("proposed_price")),
            resolved_price=_to_float(d.get("resolved_price")),
            expiration_time=_to_int(d.get("expiration_time")),
            payout=d.get("payout"),
            currency=d.get("currency"),
            outcome=d.get("outcome"),
            ancillary_text=d.get("ancillary_text"),
            block_number=_to_int(d.get("block_number")),
            tx_hash=d.get("tx_hash"),
            timestamp=d.get("timestamp"),
            raw=d,
        )

    def __repr__(self) -> str:
        who = self.disputer if self.is_dispute else self.proposer
        who = (who or "?")[:10]
        return (f"Resolution({self.event_type} outcome={self.outcome} "
                f"market={self.market_id or (self.ancillary_hash or '')[:10]} "
                f"by={who}…)")
