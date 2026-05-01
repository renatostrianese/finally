# backend/tests/test_massive_provider.py

import asyncio
from unittest.mock import MagicMock, patch
import pytest
from market.massive_provider import MassiveProvider


def make_snapshot(ticker: str, price: float):
    """Build a mock snapshot object matching the Massive API response shape."""
    snap = MagicMock()
    snap.ticker              = ticker
    snap.last_trade.price    = price
    snap.day.open            = price - 2.0
    snap.day.high            = price + 1.0
    snap.day.low             = price - 3.0
    snap.day.close           = price
    snap.day.volume          = 1_000_000
    snap.prev_day.close      = price - 1.0
    return snap


@pytest.mark.asyncio
async def test_prices_populated_after_start():
    """After start(), get_prices() should return data for all watched tickers."""
    mock_snaps = [make_snapshot("AAPL", 195.0), make_snapshot("MSFT", 415.0)]

    with patch("market.massive_provider.RESTClient") as MockClient:
        instance = MockClient.return_value
        instance.get_snapshot_all_tickers.return_value = mock_snaps

        prov = MassiveProvider(
            api_key="test-key",
            initial_tickers=["AAPL", "MSFT"],
            poll_interval=60.0,  # long interval — we don't want it firing in tests
        )
        await prov.start()

        prices = await prov.get_prices(["AAPL", "MSFT"])
        assert prices["AAPL"].price == 195.0
        assert prices["MSFT"].price == 415.0

        await prov.stop()


@pytest.mark.asyncio
async def test_api_failure_keeps_last_known_price():
    """If the API call fails, cache should retain the previous value."""
    mock_snaps = [make_snapshot("AAPL", 195.0)]

    with patch("market.massive_provider.RESTClient") as MockClient:
        instance = MockClient.return_value
        instance.get_snapshot_all_tickers.return_value = mock_snaps

        prov = MassiveProvider(
            api_key="test-key",
            initial_tickers=["AAPL"],
            poll_interval=60.0,
        )
        await prov.start()

        # Now make the API raise
        instance.get_snapshot_all_tickers.side_effect = RuntimeError("network down")
        await prov._fetch_and_cache()   # should not raise

        prices = await prov.get_prices(["AAPL"])
        assert prices["AAPL"].price == 195.0  # still the last known value

        await prov.stop()


@pytest.mark.asyncio
async def test_update_tickers_fetches_immediately():
    """update_tickers() should trigger an immediate fetch for new tickers."""
    initial = [make_snapshot("AAPL", 195.0)]
    updated = [make_snapshot("AAPL", 195.0), make_snapshot("NVDA", 870.0)]

    with patch("market.massive_provider.RESTClient") as MockClient:
        instance = MockClient.return_value
        instance.get_snapshot_all_tickers.return_value = initial

        prov = MassiveProvider(
            api_key="test-key",
            initial_tickers=["AAPL"],
            poll_interval=60.0,
        )
        await prov.start()

        instance.get_snapshot_all_tickers.return_value = updated
        await prov.update_tickers(["AAPL", "NVDA"])

        prices = await prov.get_prices(["AAPL", "NVDA"])
        assert "NVDA" in prices
        assert prices["NVDA"].price == 870.0

        await prov.stop()


@pytest.mark.asyncio
async def test_prev_price_tracking():
    """prev_price should reflect the price from the previous poll cycle."""
    first_snaps  = [make_snapshot("AAPL", 195.0)]
    second_snaps = [make_snapshot("AAPL", 200.0)]

    with patch("market.massive_provider.RESTClient") as MockClient:
        instance = MockClient.return_value
        instance.get_snapshot_all_tickers.return_value = first_snaps

        prov = MassiveProvider(
            api_key="test-key",
            initial_tickers=["AAPL"],
            poll_interval=60.0,
        )
        await prov.start()

        # First poll: price = 195, prev_price = 195 (no prior)
        prices = await prov.get_prices(["AAPL"])
        assert prices["AAPL"].price == 195.0
        assert prices["AAPL"].prev_price == 195.0

        # Second poll: price = 200, prev_price = 195
        instance.get_snapshot_all_tickers.return_value = second_snaps
        await prov._fetch_and_cache()

        prices = await prov.get_prices(["AAPL"])
        assert prices["AAPL"].price == 200.0
        assert prices["AAPL"].prev_price == 195.0

        await prov.stop()


@pytest.mark.asyncio
async def test_ticker_with_no_price_skipped():
    """Tickers where both last_trade.price and day.close are falsy should be skipped."""
    snap = MagicMock()
    snap.ticker = "NOPRICE"
    snap.last_trade.price = None
    snap.day.close = None

    with patch("market.massive_provider.RESTClient") as MockClient:
        instance = MockClient.return_value
        instance.get_snapshot_all_tickers.return_value = [snap]

        prov = MassiveProvider(
            api_key="test-key",
            initial_tickers=["NOPRICE"],
            poll_interval=60.0,
        )
        await prov.start()

        prices = await prov.get_prices(["NOPRICE"])
        assert "NOPRICE" not in prices

        await prov.stop()


@pytest.mark.asyncio
async def test_empty_tickers_list_no_fetch():
    """start() with an empty ticker list should not make any API calls."""
    with patch("market.massive_provider.RESTClient") as MockClient:
        instance = MockClient.return_value

        prov = MassiveProvider(
            api_key="test-key",
            initial_tickers=[],
            poll_interval=60.0,
        )
        await prov.start()

        instance.get_snapshot_all_tickers.assert_not_called()

        await prov.stop()
