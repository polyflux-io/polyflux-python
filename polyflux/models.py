"""Typed representations of the events Polyflux streams."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
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
        if self.timestamp is None:
            return None
        # tolerate ms or s epochs
        ts = self.timestamp
        if ts > 1e12:  # milliseconds
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)

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
