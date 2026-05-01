# backend/market/__init__.py
"""
Market data package for FinAlly.

Public API:
    create_provider  — factory function that returns the appropriate provider
    PricePoint       — data model flowing through the entire pipeline
    MarketDataProvider — abstract base class for type hints in consumer code
    DEFAULT_TICKERS  — default watchlist
"""

from .factory import create_provider, DEFAULT_TICKERS
from .models import PricePoint
from .base import MarketDataProvider

__all__ = [
    "create_provider",
    "DEFAULT_TICKERS",
    "PricePoint",
    "MarketDataProvider",
]
