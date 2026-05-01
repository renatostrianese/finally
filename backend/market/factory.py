# backend/market/factory.py

import os
import logging
from .base import MarketDataProvider
from .massive_provider import MassiveProvider
from .simulator_provider import SimulatorProvider

logger = logging.getLogger(__name__)

# Default watchlist used when no DB row exists yet (seed data)
DEFAULT_TICKERS = [
    "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA",
    "NVDA", "META",  "JPM",  "V",    "NFLX",
]


def create_provider(
    initial_tickers: list[str] | None = None,
) -> MarketDataProvider:
    """
    Instantiate the correct market data provider based on the environment.

    Selection logic:
        MASSIVE_API_KEY set and non-empty  →  MassiveProvider (live data)
        Otherwise                          →  SimulatorProvider (GBM fake data)

    Args:
        initial_tickers: Watchlist to track from startup. Defaults to
                         DEFAULT_TICKERS if None.

    Returns:
        A configured but not-yet-started MarketDataProvider.
    """
    tickers = initial_tickers if initial_tickers is not None else DEFAULT_TICKERS
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info(
            "MASSIVE_API_KEY detected — using Massive REST API for market data"
        )
        return MassiveProvider(api_key=api_key, initial_tickers=tickers)
    else:
        logger.info("No MASSIVE_API_KEY — using built-in market simulator")
        return SimulatorProvider(initial_tickers=tickers)
