# backend/tests/test_provider_contract.py
"""
Contract tests: both SimulatorProvider and MassiveProvider must honour
the MarketDataProvider ABC. These parametrized tests run the same
assertions against both implementations.
"""

import asyncio
from unittest.mock import MagicMock, patch
import pytest
from market.simulator_provider import SimulatorProvider
from market.massive_provider import MassiveProvider
from market.base import MarketDataProvider


def make_snapshot(ticker: str, price: float):
    snap = MagicMock()
    snap.ticker           = ticker
    snap.last_trade.price = price
    snap.day.open         = price
    snap.day.high         = price
    snap.day.low          = price
    snap.day.close        = price
    snap.day.volume       = 100_000
    snap.prev_day.close   = price - 1.0
    return snap


@pytest.fixture(params=["simulator", "massive"])
async def provider(request) -> MarketDataProvider:
    """Parametrized fixture that yields both provider implementations."""
    if request.param == "simulator":
        prov = SimulatorProvider(initial_tickers=["AAPL", "MSFT"], seed=42, tick_interval=0.05)
        await prov.start()
        yield prov
        await prov.stop()

    else:  # massive
        mock_snaps = [make_snapshot("AAPL", 195.0), make_snapshot("MSFT", 415.0)]
        with patch("market.massive_provider.RESTClient") as MockClient:
            MockClient.return_value.get_snapshot_all_tickers.return_value = mock_snaps
            prov = MassiveProvider(
                api_key="test-key", initial_tickers=["AAPL", "MSFT"], poll_interval=60.0
            )
            await prov.start()
            yield prov
            await prov.stop()


@pytest.mark.asyncio
async def test_get_prices_returns_dict(provider):
    prices = await provider.get_prices(["AAPL"])
    assert isinstance(prices, dict)


@pytest.mark.asyncio
async def test_known_ticker_is_in_result(provider):
    await asyncio.sleep(0.1)   # let simulator tick at least once
    prices = await provider.get_prices(["AAPL", "MSFT"])
    assert "AAPL" in prices
    assert "MSFT" in prices


@pytest.mark.asyncio
async def test_unknown_ticker_absent(provider):
    prices = await provider.get_prices(["FAKE_TICKER_XYZ"])
    assert "FAKE_TICKER_XYZ" not in prices


@pytest.mark.asyncio
async def test_price_is_positive(provider):
    await asyncio.sleep(0.1)
    prices = await provider.get_prices(["AAPL"])
    assert prices["AAPL"].price > 0


@pytest.mark.asyncio
async def test_direction_is_valid(provider):
    await asyncio.sleep(0.15)
    prices = await provider.get_prices(["AAPL"])
    assert prices["AAPL"].direction in ("up", "down", "flat")


@pytest.mark.asyncio
async def test_price_point_has_all_fields(provider):
    """PricePoint must expose all required fields."""
    await asyncio.sleep(0.1)
    prices = await provider.get_prices(["AAPL"])
    p = prices["AAPL"]
    assert isinstance(p.ticker, str)
    assert isinstance(p.price, float)
    assert isinstance(p.prev_price, float)
    assert isinstance(p.open, float)
    assert isinstance(p.high, float)
    assert isinstance(p.low, float)
    assert isinstance(p.prev_close, float)
    assert isinstance(p.timestamp, float)


@pytest.mark.asyncio
async def test_to_sse_dict_shape(provider):
    """to_sse_dict() must include all expected keys."""
    await asyncio.sleep(0.1)
    prices = await provider.get_prices(["AAPL"])
    d = prices["AAPL"].to_sse_dict()
    required_keys = {
        "ticker", "price", "prev_price", "open", "high", "low",
        "prev_close", "change", "change_pct", "direction", "volume", "timestamp",
    }
    assert required_keys.issubset(d.keys())


@pytest.mark.asyncio
async def test_provider_is_subclass_of_abc(provider):
    """Both concrete providers must be instances of MarketDataProvider."""
    assert isinstance(provider, MarketDataProvider)
