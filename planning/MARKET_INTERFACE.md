# Market Interface — Unified Python API for Stock Prices

This document defines the shared market data abstraction layer used throughout the FinAlly backend. All consumer code (SSE streaming, portfolio valuation, AI tools) interacts exclusively with this interface — never with the Massive client or simulator directly.

---

## 1. Design Principles

- **Single source of truth**: one interface, two implementations
- **Environment-driven selection**: `MASSIVE_API_KEY` env var controls which backend is active
- **Zero consumer changes**: swapping backends requires no changes to calling code
- **Async-first**: all I/O is `async`; the background polling task runs in FastAPI's event loop
- **Fail-safe**: if the Massive API is unavailable, the price cache retains its last known values

---

## 2. Data Model

```python
# backend/market/models.py

from dataclasses import dataclass, field
from typing import Optional
import time


@dataclass
class PricePoint:
    """A single price observation for a ticker."""
    ticker: str
    price: float                   # Current price
    prev_price: float              # Previous observation's price (for change direction)
    open: float                    # Today's open
    high: float                    # Today's high
    low: float                     # Today's low
    prev_close: float              # Previous trading day's close
    volume: Optional[int] = None   # Today's volume (None if unavailable)
    timestamp: float = field(default_factory=time.time)  # Unix seconds (UTC)

    @property
    def change(self) -> float:
        """Absolute change from previous close."""
        return self.price - self.prev_close

    @property
    def change_pct(self) -> float:
        """Percentage change from previous close."""
        if self.prev_close == 0:
            return 0.0
        return (self.price - self.prev_close) / self.prev_close * 100

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat' relative to the previous observation."""
        if self.price > self.prev_price:
            return "up"
        elif self.price < self.prev_price:
            return "down"
        return "flat"
```

---

## 3. Abstract Interface

```python
# backend/market/base.py

from abc import ABC, abstractmethod
from typing import Dict, List
from .models import PricePoint


class MarketDataProvider(ABC):
    """
    Abstract base class for all market data providers.
    
    Implementations must be safe to call concurrently — the price cache
    background task calls update_prices() in a tight loop while SSE
    handlers call get_prices() on every connected client's tick.
    """

    @abstractmethod
    async def start(self) -> None:
        """
        Perform any initialisation required before the provider can serve data.
        Called once during FastAPI startup (lifespan).
        """
        ...

    @abstractmethod
    async def stop(self) -> None:
        """
        Gracefully shut down the provider.
        Called once during FastAPI shutdown (lifespan).
        """
        ...

    @abstractmethod
    async def get_prices(self, tickers: List[str]) -> Dict[str, PricePoint]:
        """
        Return the latest known price for each requested ticker.
        
        Returns whatever is in the in-memory cache; never makes a blocking
        network call on the hot path. Returns an empty dict entry for any
        ticker not yet seen.
        
        Args:
            tickers: List of uppercase ticker symbols, e.g. ["AAPL", "MSFT"]
        
        Returns:
            Dict mapping ticker → PricePoint for each ticker found in cache.
            Tickers not yet available are omitted from the result.
        """
        ...

    @abstractmethod
    async def update_tickers(self, tickers: List[str]) -> None:
        """
        Notify the provider that the watchlist has changed.
        
        The provider should begin tracking any new tickers on the next poll
        cycle. Tickers removed from the watchlist may be dropped from the
        cache at the provider's discretion.
        
        Args:
            tickers: Complete current watchlist (not a delta).
        """
        ...
```

---

## 4. Price Cache

The price cache is the shared in-memory store written by the background task and read by SSE handlers. It is intentionally decoupled from the provider implementations.

```python
# backend/market/cache.py

import asyncio
from typing import Dict, List, Optional
from .models import PricePoint


class PriceCache:
    """
    Thread-safe (asyncio-safe) in-memory store for latest price observations.
    
    The background polling task writes here; SSE stream handlers read here.
    Uses asyncio.Event to notify waiting readers when new data arrives.
    """

    def __init__(self) -> None:
        self._data: Dict[str, PricePoint] = {}
        self._lock = asyncio.Lock()
        self._updated = asyncio.Event()

    async def set(self, ticker: str, point: PricePoint) -> None:
        async with self._lock:
            self._data[ticker] = point
        self._updated.set()
        self._updated.clear()

    async def set_many(self, points: Dict[str, PricePoint]) -> None:
        async with self._lock:
            self._data.update(points)
        self._updated.set()
        self._updated.clear()

    async def get(self, ticker: str) -> Optional[PricePoint]:
        async with self._lock:
            return self._data.get(ticker)

    async def get_all(self, tickers: Optional[List[str]] = None) -> Dict[str, PricePoint]:
        async with self._lock:
            if tickers is None:
                return dict(self._data)
            return {t: self._data[t] for t in tickers if t in self._data}

    async def wait_for_update(self, timeout: float = 1.0) -> bool:
        """Block until new data is written or timeout expires. Returns True if data arrived."""
        try:
            await asyncio.wait_for(self._updated.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False
```

---

## 5. Provider Factory

```python
# backend/market/factory.py

import os
import logging
from .base import MarketDataProvider
from .massive_provider import MassiveProvider
from .simulator_provider import SimulatorProvider

logger = logging.getLogger(__name__)


def create_provider(initial_tickers: list[str]) -> MarketDataProvider:
    """
    Instantiate the appropriate market data provider based on environment.
    
    Selection logic:
      - MASSIVE_API_KEY is set and non-empty  →  MassiveProvider
      - Otherwise                             →  SimulatorProvider
    
    Args:
        initial_tickers: The default watchlist to track from startup.
    
    Returns:
        A configured but not-yet-started MarketDataProvider.
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("MASSIVE_API_KEY detected — using Massive REST API for market data")
        return MassiveProvider(api_key=api_key, initial_tickers=initial_tickers)
    else:
        logger.info("No MASSIVE_API_KEY — using built-in market simulator")
        return SimulatorProvider(initial_tickers=initial_tickers)
```

---

## 6. Massive Provider Implementation

```python
# backend/market/massive_provider.py

import asyncio
import logging
import time
from typing import Dict, List

from massive import RESTClient

from .base import MarketDataProvider
from .cache import PriceCache
from .models import PricePoint

logger = logging.getLogger(__name__)

# Conservative polling interval for free-tier compatibility (5 req/min limit)
_DEFAULT_POLL_INTERVAL = 15.0  # seconds


class MassiveProvider(MarketDataProvider):
    """
    Market data provider backed by the Massive (formerly Polygon.io) REST API.
    
    Uses the snapshot endpoint to fetch all watched tickers in a single API
    call per poll cycle. This is rate-efficient: 1 call per interval regardless
    of watchlist size.
    
    Rate limits:
        Free tier  : 5 req/min  → use poll_interval=15s (safe) or 12s (minimum)
        Paid tiers : higher limits → poll_interval can be reduced to 2–5s
    """

    def __init__(
        self,
        api_key: str,
        initial_tickers: List[str],
        poll_interval: float = _DEFAULT_POLL_INTERVAL,
    ) -> None:
        self._client = RESTClient(api_key=api_key)
        self._tickers: List[str] = list(initial_tickers)
        self._poll_interval = poll_interval
        self._cache = PriceCache()
        self._task: asyncio.Task | None = None
        # Track previous prices for direction calculation
        self._prev_prices: Dict[str, float] = {}

    async def start(self) -> None:
        # Do an immediate fetch so the cache is populated before the first SSE request
        await self._fetch_and_cache()
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "MassiveProvider started (poll_interval=%.1fs, tickers=%s)",
            self._poll_interval,
            self._tickers,
        )

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("MassiveProvider stopped")

    async def get_prices(self, tickers: List[str]) -> Dict[str, PricePoint]:
        return await self._cache.get_all(tickers)

    async def update_tickers(self, tickers: List[str]) -> None:
        self._tickers = list(tickers)
        # Eagerly fetch new tickers so they appear in cache immediately
        await self._fetch_and_cache()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._poll_interval)
            await self._fetch_and_cache()

    async def _fetch_and_cache(self) -> None:
        if not self._tickers:
            return
        try:
            points = await asyncio.get_event_loop().run_in_executor(
                None, self._fetch_sync
            )
            await self._cache.set_many(points)
        except Exception as e:
            logger.warning("Massive API fetch failed: %s", e)

    def _fetch_sync(self) -> Dict[str, PricePoint]:
        """Synchronous fetch — run in executor to avoid blocking the event loop."""
        now = time.time()
        points: Dict[str, PricePoint] = {}

        snapshots = self._client.get_snapshot_all_tickers(
            "stocks", tickers=self._tickers
        )

        for snap in snapshots:
            ticker = snap.ticker

            # Best available current price
            if snap.last_trade and snap.last_trade.price:
                price = float(snap.last_trade.price)
            elif snap.day and snap.day.close:
                price = float(snap.day.close)
            else:
                continue  # No price data available for this ticker

            prev_close = float(snap.prev_day.close) if snap.prev_day and snap.prev_day.close else price
            open_price = float(snap.day.open) if snap.day and snap.day.open else price
            high_price = float(snap.day.high) if snap.day and snap.day.high else price
            low_price  = float(snap.day.low)  if snap.day and snap.day.low  else price
            volume     = int(snap.day.volume) if snap.day and snap.day.volume else None

            prev_price = self._prev_prices.get(ticker, price)
            self._prev_prices[ticker] = price

            points[ticker] = PricePoint(
                ticker=ticker,
                price=price,
                prev_price=prev_price,
                open=open_price,
                high=high_price,
                low=low_price,
                prev_close=prev_close,
                volume=volume,
                timestamp=now,
            )

        return points
```

---

## 7. Integration with FastAPI

```python
# backend/main.py (relevant excerpt)

from contextlib import asynccontextmanager
from fastapi import FastAPI
from market.factory import create_provider
from market.cache import PriceCache

DEFAULT_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "BRK.B", "JPM", "V"]

provider = None  # Set during startup

@asynccontextmanager
async def lifespan(app: FastAPI):
    global provider
    provider = create_provider(initial_tickers=DEFAULT_TICKERS)
    await provider.start()
    yield
    await provider.stop()

app = FastAPI(lifespan=lifespan)


# SSE streaming endpoint
from fastapi.responses import StreamingResponse
import asyncio, json

@app.get("/api/stream/prices")
async def stream_prices():
    async def event_generator():
        while True:
            # Get current watchlist from DB
            tickers = DEFAULT_TICKERS  # replace with DB lookup
            prices = await provider.get_prices(tickers)
            
            for ticker, point in prices.items():
                data = {
                    "ticker": ticker,
                    "price": point.price,
                    "prev_price": point.prev_price,
                    "change": round(point.change, 2),
                    "change_pct": round(point.change_pct, 4),
                    "direction": point.direction,
                    "timestamp": point.timestamp,
                }
                yield f"data: {json.dumps(data)}\n\n"
            
            await asyncio.sleep(0.5)  # ~500ms cadence

    return StreamingResponse(event_generator(), media_type="text/event-stream")
```

---

## 8. Adding a New Ticker (Watchlist Change)

```python
# When a user adds a ticker via the API or AI chat:
await provider.update_tickers(new_watchlist)
```

Both implementations handle this: the simulator initialises a new GBM process; the Massive provider includes the new ticker in the next snapshot poll.

---

## 9. File Structure

```
backend/
└── market/
    ├── __init__.py
    ├── models.py            # PricePoint dataclass
    ├── base.py              # MarketDataProvider ABC
    ├── cache.py             # PriceCache
    ├── factory.py           # create_provider() factory
    ├── massive_provider.py  # MassiveProvider (live data)
    └── simulator_provider.py # SimulatorProvider (default)
```

---

## 10. Testing the Interface

Both providers implement the same ABC, so unit tests can mock either implementation:

```python
# tests/test_market.py

import pytest
from market.simulator_provider import SimulatorProvider

@pytest.mark.asyncio
async def test_simulator_returns_prices():
    provider = SimulatorProvider(initial_tickers=["AAPL", "MSFT"])
    await provider.start()
    prices = await provider.get_prices(["AAPL", "MSFT"])
    assert "AAPL" in prices
    assert prices["AAPL"].price > 0
    assert prices["AAPL"].direction in ("up", "down", "flat")
    await provider.stop()
```
