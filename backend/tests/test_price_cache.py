# backend/tests/test_price_cache.py

import asyncio
import pytest
from market.cache import PriceCache
from market.models import PricePoint


def make_point(ticker: str, price: float) -> PricePoint:
    return PricePoint(
        ticker=ticker, price=price, prev_price=price - 1,
        open=price, high=price, low=price, prev_close=price - 0.5,
    )


@pytest.mark.asyncio
async def test_set_and_get_single():
    cache = PriceCache()
    await cache.set("AAPL", make_point("AAPL", 195.0))
    point = await cache.get("AAPL")
    assert point is not None
    assert point.price == 195.0


@pytest.mark.asyncio
async def test_get_missing_ticker_returns_none():
    cache = PriceCache()
    assert await cache.get("FAKE") is None


@pytest.mark.asyncio
async def test_set_many_and_get_all():
    cache = PriceCache()
    points = {
        "AAPL": make_point("AAPL", 195.0),
        "MSFT": make_point("MSFT", 415.0),
    }
    await cache.set_many(points)
    result = await cache.get_all(["AAPL", "MSFT"])
    assert result["AAPL"].price == 195.0
    assert result["MSFT"].price == 415.0


@pytest.mark.asyncio
async def test_get_all_filters_missing():
    cache = PriceCache()
    await cache.set("AAPL", make_point("AAPL", 195.0))
    result = await cache.get_all(["AAPL", "MISSING"])
    assert "AAPL" in result
    assert "MISSING" not in result


@pytest.mark.asyncio
async def test_get_all_no_filter_returns_all():
    cache = PriceCache()
    await cache.set_many({
        "AAPL": make_point("AAPL", 195.0),
        "MSFT": make_point("MSFT", 415.0),
    })
    result = await cache.get_all()
    assert "AAPL" in result
    assert "MSFT" in result


@pytest.mark.asyncio
async def test_wait_for_update_signals_correctly():
    cache = PriceCache()

    async def writer():
        await asyncio.sleep(0.1)
        await cache.set("AAPL", make_point("AAPL", 200.0))

    asyncio.create_task(writer())
    arrived = await cache.wait_for_update(timeout=1.0)
    assert arrived is True


@pytest.mark.asyncio
async def test_wait_for_update_times_out():
    cache = PriceCache()
    arrived = await cache.wait_for_update(timeout=0.05)
    assert arrived is False


@pytest.mark.asyncio
async def test_ticker_count():
    cache = PriceCache()
    assert cache.ticker_count == 0
    await cache.set("AAPL", make_point("AAPL", 195.0))
    assert cache.ticker_count == 1
    await cache.set("MSFT", make_point("MSFT", 415.0))
    assert cache.ticker_count == 2


@pytest.mark.asyncio
async def test_set_overwrites_existing():
    cache = PriceCache()
    await cache.set("AAPL", make_point("AAPL", 195.0))
    await cache.set("AAPL", make_point("AAPL", 200.0))
    point = await cache.get("AAPL")
    assert point is not None
    assert point.price == 200.0
