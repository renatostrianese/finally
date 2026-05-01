# backend/market/cache.py

import asyncio
from typing import Dict, List, Optional
from .models import PricePoint


class PriceCache:
    """
    Asyncio-safe in-memory store for the latest price observation per ticker.

    Uses an asyncio.Lock for all mutations, ensuring there are no data
    races between the writer (background task) and readers (SSE handlers).

    Uses an asyncio.Event to let SSE handlers sleep efficiently rather
    than busy-waiting for new data.
    """

    def __init__(self) -> None:
        self._data: Dict[str, PricePoint] = {}
        self._lock = asyncio.Lock()
        self._updated = asyncio.Event()

    # ------------------------------------------------------------------
    # Write path (background task only)
    # ------------------------------------------------------------------

    async def set(self, ticker: str, point: PricePoint) -> None:
        """Write a single price point and signal waiting readers."""
        async with self._lock:
            self._data[ticker] = point
        self._updated.set()
        self._updated.clear()

    async def set_many(self, points: Dict[str, PricePoint]) -> None:
        """Atomically write a batch of price points and signal readers once."""
        async with self._lock:
            self._data.update(points)
        self._updated.set()
        self._updated.clear()

    # ------------------------------------------------------------------
    # Read path (SSE handlers and portfolio endpoints)
    # ------------------------------------------------------------------

    async def get(self, ticker: str) -> Optional[PricePoint]:
        """Return the latest PricePoint for one ticker, or None if not seen yet."""
        async with self._lock:
            return self._data.get(ticker)

    async def get_all(self, tickers: Optional[List[str]] = None) -> Dict[str, PricePoint]:
        """
        Return latest prices for a list of tickers, or all known tickers.

        Args:
            tickers: Filter to these tickers. None returns everything.

        Returns:
            Shallow copy of the matching subset. Missing tickers are absent.
        """
        async with self._lock:
            if tickers is None:
                return dict(self._data)
            return {t: self._data[t] for t in tickers if t in self._data}

    async def wait_for_update(self, timeout: float = 1.0) -> bool:
        """
        Block until new price data is written or the timeout elapses.

        Used by SSE handlers to avoid polling in a tight loop.

        Returns:
            True if new data arrived before the timeout; False otherwise.
        """
        try:
            await asyncio.wait_for(self._updated.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    @property
    def ticker_count(self) -> int:
        """Number of tickers currently held in the cache (for monitoring)."""
        return len(self._data)
