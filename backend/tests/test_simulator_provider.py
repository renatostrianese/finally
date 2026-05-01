# backend/tests/test_simulator_provider.py

import asyncio
import pytest
from market.simulator_provider import SimulatorProvider


@pytest.mark.asyncio
async def test_initial_prices_populated():
    """Cache should be non-empty immediately after start()."""
    sim = SimulatorProvider(initial_tickers=["AAPL", "MSFT"], seed=42)
    await sim.start()

    prices = await sim.get_prices(["AAPL", "MSFT"])
    assert "AAPL" in prices
    assert "MSFT" in prices
    assert prices["AAPL"].price > 0
    assert prices["MSFT"].price > 0

    await sim.stop()


@pytest.mark.asyncio
async def test_prices_change_over_time():
    """Prices must update on each tick (GBM never produces identical values)."""
    sim = SimulatorProvider(initial_tickers=["AAPL"], seed=42, tick_interval=0.05)
    await sim.start()

    first = (await sim.get_prices(["AAPL"]))["AAPL"].price
    await asyncio.sleep(0.25)   # wait for ~5 ticks
    second = (await sim.get_prices(["AAPL"]))["AAPL"].price

    assert first != second, "Price should change after several ticks"

    await sim.stop()


@pytest.mark.asyncio
async def test_price_never_goes_below_floor():
    """Even with extreme vol, price must stay ≥ $0.01."""
    # TSLA has sigma=0.65 — highest in the default config
    sim = SimulatorProvider(initial_tickers=["TSLA"], seed=999, tick_interval=0.01)
    await sim.start()
    await asyncio.sleep(1.0)   # 100 ticks

    prices = await sim.get_prices(["TSLA"])
    assert prices["TSLA"].price >= 0.01

    await sim.stop()


@pytest.mark.asyncio
async def test_direction_property():
    """direction should be one of 'up', 'down', 'flat'."""
    sim = SimulatorProvider(initial_tickers=["AAPL"], seed=1, tick_interval=0.05)
    await sim.start()
    await asyncio.sleep(0.15)

    point = (await sim.get_prices(["AAPL"]))["AAPL"]
    assert point.direction in ("up", "down", "flat")

    await sim.stop()


@pytest.mark.asyncio
async def test_dynamic_ticker_add():
    """A ticker added via update_tickers() should appear in the cache within one tick."""
    sim = SimulatorProvider(initial_tickers=["AAPL"], seed=0, tick_interval=0.05)
    await sim.start()

    await sim.update_tickers(["AAPL", "NVDA"])
    await asyncio.sleep(0.15)

    prices = await sim.get_prices(["AAPL", "NVDA"])
    assert "NVDA" in prices
    assert prices["NVDA"].price > 0

    await sim.stop()


@pytest.mark.asyncio
async def test_deterministic_with_seed():
    """Two simulators with the same seed and tickers must produce identical prices."""
    kwargs = dict(initial_tickers=["AAPL", "MSFT"], seed=123, tick_interval=0.05)

    sim_a = SimulatorProvider(**kwargs)
    sim_b = SimulatorProvider(**kwargs)

    await sim_a.start()
    await sim_b.start()
    await asyncio.sleep(0.30)

    prices_a = await sim_a.get_prices(["AAPL", "MSFT"])
    prices_b = await sim_b.get_prices(["AAPL", "MSFT"])

    assert prices_a["AAPL"].price == prices_b["AAPL"].price
    assert prices_a["MSFT"].price == prices_b["MSFT"].price

    await sim_a.stop()
    await sim_b.stop()


@pytest.mark.asyncio
async def test_high_is_always_gte_low():
    """Intraday high must always be >= intraday low."""
    sim = SimulatorProvider(initial_tickers=["AAPL", "TSLA"], seed=7, tick_interval=0.01)
    await sim.start()
    await asyncio.sleep(0.5)

    for ticker in ["AAPL", "TSLA"]:
        p = (await sim.get_prices([ticker]))[ticker]
        assert p.high >= p.low, f"{ticker}: high={p.high} < low={p.low}"

    await sim.stop()


@pytest.mark.asyncio
async def test_unknown_ticker_not_in_prices():
    """Requesting an unknown ticker should return an empty result for that ticker."""
    sim = SimulatorProvider(initial_tickers=["AAPL"], seed=42)
    await sim.start()

    prices = await sim.get_prices(["UNKNOWN_XYZ"])
    assert "UNKNOWN_XYZ" not in prices

    await sim.stop()


@pytest.mark.asyncio
async def test_fallback_config_for_unknown_ticker():
    """Dynamically added tickers not in DEFAULT_TICKER_CONFIGS get fallback config."""
    sim = SimulatorProvider(initial_tickers=[], seed=42)
    await sim.start()

    await sim.update_tickers(["UNKNOWN_TICKER_XYZ"])
    await asyncio.sleep(0.1)

    prices = await sim.get_prices(["UNKNOWN_TICKER_XYZ"])
    assert "UNKNOWN_TICKER_XYZ" in prices
    assert prices["UNKNOWN_TICKER_XYZ"].price > 0

    await sim.stop()


@pytest.mark.asyncio
async def test_volume_increases_over_time():
    """Simulated volume should increase with each tick."""
    sim = SimulatorProvider(initial_tickers=["AAPL"], seed=5, tick_interval=0.05)
    await sim.start()

    first_vol = (await sim.get_prices(["AAPL"]))["AAPL"].volume
    await asyncio.sleep(0.25)
    second_vol = (await sim.get_prices(["AAPL"]))["AAPL"].volume

    assert second_vol > first_vol, "Volume should increase over time"

    await sim.stop()


@pytest.mark.asyncio
async def test_ohlc_fields_present():
    """PricePoint should have valid OHLC fields."""
    sim = SimulatorProvider(initial_tickers=["AAPL"], seed=42)
    await sim.start()

    point = (await sim.get_prices(["AAPL"]))["AAPL"]
    assert point.open > 0
    assert point.high > 0
    assert point.low > 0
    assert point.prev_close > 0

    await sim.stop()
