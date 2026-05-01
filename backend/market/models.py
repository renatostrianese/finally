# backend/market/models.py

from dataclasses import dataclass, field
from typing import Optional
import time


@dataclass
class PricePoint:
    """A single price observation for a ticker.

    Produced by both providers and stored in PriceCache.
    Consumed by SSE handlers, portfolio valuation, and AI tools.
    """
    ticker: str
    price: float         # Current price (latest trade or close)
    prev_price: float    # Price on the previous observation (for direction arrow)
    open: float          # Today's session open price
    high: float          # Today's intraday high
    low: float           # Today's intraday low
    prev_close: float    # Previous trading day's closing price
    volume: Optional[int] = None           # Today's accumulated volume (None if unknown)
    timestamp: float = field(default_factory=time.time)  # Unix seconds (UTC)

    # ------------------------------------------------------------------
    # Derived properties — computed on the fly; not stored
    # ------------------------------------------------------------------

    @property
    def change(self) -> float:
        """Absolute price change from previous close."""
        return round(self.price - self.prev_close, 2)

    @property
    def change_pct(self) -> float:
        """Percentage price change from previous close."""
        if self.prev_close == 0:
            return 0.0
        return round((self.price - self.prev_close) / self.prev_close * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat' relative to the immediately preceding observation."""
        if self.price > self.prev_price:
            return "up"
        elif self.price < self.prev_price:
            return "down"
        return "flat"

    def to_sse_dict(self) -> dict:
        """Serialise to the shape pushed over SSE to the frontend."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "prev_price": self.prev_price,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "prev_close": self.prev_close,
            "change": self.change,
            "change_pct": self.change_pct,
            "direction": self.direction,
            "volume": self.volume,
            "timestamp": self.timestamp,
        }
