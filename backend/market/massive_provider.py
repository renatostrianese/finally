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

# Free-tier safe: 4 req/min → well under the 5 req/min limit
_DEFAULT_POLL_INTERVAL = 15.0  # seconds


class MassiveProvider(MarketDataProvider):
    """
    Market data provider backed by the Massive REST API.

    Uses the /v2/snapshot endpoint to fetch all watched tickers in a
    single request per poll cycle. This is rate-efficient: one API call
    per interval regardless of watchlist size.

    Args:
        api_key: Massive API key (MASSIVE_API_KEY env var).
        initial_tickers: Tickers to watch from startup.
        poll_interval: Seconds between snapshot fetches. Default 15s is
                       safe for free-tier (5 req/min limit).
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
        # Tracks price-from-last-cycle for the direction arrow
        self._prev_prices: Dict[str, float] = {}

    # ── MarketDataProvider interface ───────────────────────────────────────────

    async def start(self) -> None:
        """Fetch initial snapshot, then start background poll loop."""
        await self._fetch_and_cache()
        self._task = asyncio.create_task(
            self._poll_loop(), name="massive-poller"
        )
        logger.info(
            "MassiveProvider started (poll_interval=%.1fs, tickers=%s)",
            self._poll_interval, self._tickers,
        )

    async def stop(self) -> None:
        """Cancel the poll loop."""
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
        """Update the watchlist and immediately fetch the new tickers."""
        self._tickers = list(tickers)
        await self._fetch_and_cache()   # eagerly populate cache for new tickers

    # ── Poll loop ──────────────────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """Sleep for poll_interval, then fetch. Repeat forever."""
        while True:
            await asyncio.sleep(self._poll_interval)
            await self._fetch_and_cache()

    # ── Fetch helpers ──────────────────────────────────────────────────────────

    async def _fetch_and_cache(self) -> None:
        """
        Fetch a snapshot for all tracked tickers and push to PriceCache.

        The blocking HTTP call is run in a thread-pool executor so it does
        not block the asyncio event loop.
        """
        if not self._tickers:
            return
        try:
            loop   = asyncio.get_event_loop()
            points = await loop.run_in_executor(None, self._fetch_sync)
            await self._cache.set_many(points)
        except Exception as e:
            # Log and continue — cache retains last-known prices (fail-safe)
            logger.warning("Massive API fetch failed: %s", e)

    def _fetch_sync(self) -> Dict[str, PricePoint]:
        """
        Synchronous snapshot fetch. Executed in a thread pool.

        Requests all watched tickers in a single API call.
        Maps Massive snapshot objects to PricePoint instances.
        """
        now    = time.time()
        points: Dict[str, PricePoint] = {}

        snapshots = self._client.get_snapshot_all_tickers(
            "stocks", tickers=self._tickers
        )

        for snap in snapshots:
            ticker = snap.ticker

            # ── Current price: prefer lastTrade; fall back to day close ──────
            if snap.last_trade and snap.last_trade.price:
                price = float(snap.last_trade.price)
            elif snap.day and snap.day.close:
                price = float(snap.day.close)
            else:
                logger.debug("No price data for %s, skipping", ticker)
                continue

            # ── Intraday OHLC ─────────────────────────────────────────────────
            prev_close  = float(snap.prev_day.close)  if (snap.prev_day  and snap.prev_day.close)  else price
            open_price  = float(snap.day.open)        if (snap.day       and snap.day.open)        else price
            high_price  = float(snap.day.high)        if (snap.day       and snap.day.high)        else price
            low_price   = float(snap.day.low)         if (snap.day       and snap.day.low)         else price
            volume      = int(snap.day.volume)        if (snap.day       and snap.day.volume)      else None

            # ── Direction arrow ───────────────────────────────────────────────
            prev_price              = self._prev_prices.get(ticker, price)
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
