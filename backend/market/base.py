# backend/market/base.py

from abc import ABC, abstractmethod
from typing import Dict, List
from .models import PricePoint


class MarketDataProvider(ABC):
    """
    Abstract base class for market data providers.

    Lifecycle:
        provider = create_provider(tickers)
        await provider.start()          # once, on FastAPI startup
        # ... application runs ...
        await provider.stop()           # once, on FastAPI shutdown

    Hot path (called many times per second):
        prices = await provider.get_prices(tickers)

    Watchlist changes (called when user adds/removes a ticker):
        await provider.update_tickers(new_watchlist)
    """

    @abstractmethod
    async def start(self) -> None:
        """
        Initialise the provider and begin producing data.

        - Populate the cache with an initial snapshot before returning so
          the first SSE client is not served an empty response.
        - Spawn any background tasks needed (poll loop, tick loop).
        - Called once inside FastAPI's lifespan context manager.
        """
        ...

    @abstractmethod
    async def stop(self) -> None:
        """
        Gracefully shut down and release resources.

        - Cancel background tasks.
        - Close network connections.
        - Called once when FastAPI shuts down.
        """
        ...

    @abstractmethod
    async def get_prices(self, tickers: List[str]) -> Dict[str, PricePoint]:
        """
        Return the latest cached price for each requested ticker.

        NEVER makes a live network call. Reads from the in-memory cache only.
        Tickers not yet in the cache are silently omitted from the result.

        Args:
            tickers: Uppercase ticker symbols, e.g. ["AAPL", "MSFT"].

        Returns:
            Dict mapping ticker → PricePoint. Missing tickers are absent.
        """
        ...

    @abstractmethod
    async def update_tickers(self, tickers: List[str]) -> None:
        """
        Notify the provider that the watched ticker set has changed.

        Receives the *complete* new watchlist, not a delta.
        New tickers should appear in the cache as quickly as possible.
        Removed tickers may be kept or evicted at the provider's discretion.

        Args:
            tickers: Complete updated watchlist, e.g. ["AAPL", "MSFT", "NVDA"].
        """
        ...
