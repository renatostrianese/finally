# Market Simulator — Design and Code Structure

This document describes the approach and implementation structure for FinAlly's built-in market simulator. The simulator is the **default** market data provider when no `MASSIVE_API_KEY` is set. It generates realistic-looking price movements without any external dependencies.

---

## 1. Goals

| Goal | Detail |
|---|---|
| **Realistic prices** | Prices start near real-world values and move in plausible ways |
| **Smooth streaming** | Updates at ~500ms intervals; no discontinuous jumps except during events |
| **Drama** | Occasional sudden moves (earnings, news) to make the UI interesting |
| **Correlation** | Tech stocks move somewhat together; defensive stocks are uncorrelated |
| **Self-contained** | Runs as an asyncio background task; zero external dependencies |
| **Deterministic option** | Seeded RNG for reproducible test scenarios |

---

## 2. Price Model: Geometric Brownian Motion (GBM)

Each ticker follows **Geometric Brownian Motion**, the standard model for equity prices:

```
dS = S · (μ · dt + σ · dW)
```

Where:
- `S` — current price
- `μ` — drift (annualised expected return, small, typically ±2% annualised)
- `σ` — volatility (annualised, typically 20–60% for individual stocks)
- `dt` — time step (0.5s expressed as a fraction of a trading year)
- `dW` — Wiener increment: a draw from `N(0, dt)` (i.e. `√dt · Z` where `Z ~ N(0,1)`)

**Discrete update formula** (applied each 500ms tick):

```
S_new = S_old · exp((μ - σ²/2) · dt + σ · √dt · Z)
```

The `exp()` form ensures prices can never go negative (a key property of GBM).

**Trading year constant:** One trading year ≈ 252 days × 6.5 hours × 3600 seconds = **5,900,400 seconds**.  
With `dt = 0.5s`: `dt_annual = 0.5 / 5_900_400 ≈ 8.47e-8`.

---

## 3. Correlation Structure

Real stock markets exhibit correlation — tech stocks tend to move together, defensive stocks are more independent. The simulator models this with a simple **common-factor decomposition**:

```
Z_ticker = ρ_sector · Z_market + √(1 - ρ_sector²) · Z_idio
```

Where:
- `Z_market` — a single market-wide random shock drawn once per tick
- `Z_idio` — ticker-specific (idiosyncratic) shock
- `ρ_sector` — correlation with the market factor (0 = fully independent, 1 = identical to market)

Each tick:
1. Draw one `Z_market ~ N(0,1)`
2. For each ticker, draw `Z_idio ~ N(0,1)` independently
3. Combine: `Z_eff = ρ · Z_market + √(1-ρ²) · Z_idio`
4. Apply GBM update with `Z_eff`

---

## 4. Seed Prices and Ticker Configuration

Seed prices are set to approximate real-world values as of early 2025. Each ticker has its own volatility and sector correlation:

```python
# backend/market/simulator_config.py

from dataclasses import dataclass

@dataclass
class TickerConfig:
    seed_price: float       # Starting price ($)
    sigma: float            # Annualised volatility (e.g. 0.30 = 30%)
    mu: float               # Annualised drift (e.g. 0.05 = 5% expected return)
    rho: float              # Market correlation (0.0 to 1.0)

DEFAULT_TICKER_CONFIGS: dict[str, TickerConfig] = {
    # Large-cap tech — high correlation, moderate/high vol
    "AAPL":  TickerConfig(seed_price=195.00, sigma=0.28, mu=0.08, rho=0.70),
    "MSFT":  TickerConfig(seed_price=415.00, sigma=0.26, mu=0.10, rho=0.68),
    "GOOGL": TickerConfig(seed_price=175.00, sigma=0.30, mu=0.08, rho=0.65),
    "AMZN":  TickerConfig(seed_price=185.00, sigma=0.32, mu=0.10, rho=0.65),
    "META":  TickerConfig(seed_price=520.00, sigma=0.38, mu=0.12, rho=0.62),
    "NVDA":  TickerConfig(seed_price=870.00, sigma=0.55, mu=0.15, rho=0.60),
    "TSLA":  TickerConfig(seed_price=175.00, sigma=0.65, mu=0.05, rho=0.45),
    # Financials — moderate correlation
    "JPM":   TickerConfig(seed_price=200.00, sigma=0.22, mu=0.07, rho=0.55),
    "V":     TickerConfig(seed_price=280.00, sigma=0.20, mu=0.09, rho=0.52),
    # Defensive / value
    "BRK.B": TickerConfig(seed_price=410.00, sigma=0.18, mu=0.07, rho=0.45),
}

# Fallback config for dynamically added tickers not in the table
FALLBACK_CONFIG = TickerConfig(seed_price=100.00, sigma=0.30, mu=0.07, rho=0.55)
```

---

## 5. Event System (Drama Injection)

To make the UI interesting, the simulator periodically fires **sudden price events** on random tickers — mimicking earnings surprises, news, analyst upgrades, etc.

**Event parameters:**
- Probability of an event on any given tick: `~0.03%` per ticker per tick (roughly one event per ticker per ~10 minutes)
- Event magnitude: uniform random draw from `±2% to ±5%` (instantaneous jump applied once)
- Event direction: equally likely up or down

```python
import random

EVENT_PROBABILITY = 0.0003    # Per ticker per tick
EVENT_MIN_MAGNITUDE = 0.02    # 2%
EVENT_MAX_MAGNITUDE = 0.05    # 5%

def maybe_apply_event(price: float, rng: random.Random) -> float:
    """Randomly apply a sudden price event. Returns (possibly unchanged) price."""
    if rng.random() < EVENT_PROBABILITY:
        magnitude = rng.uniform(EVENT_MIN_MAGNITUDE, EVENT_MAX_MAGNITUDE)
        direction = rng.choice([-1, 1])
        price *= (1 + direction * magnitude)
    return price
```

---

## 6. Simulator State

Each ticker maintains a small mutable state object:

```python
# backend/market/simulator_state.py

from dataclasses import dataclass, field
import time


@dataclass
class TickerState:
    ticker: str
    price: float           # Current price
    open: float            # Today's open price
    high: float            # Today's high (rolling intraday)
    low: float             # Today's low  (rolling intraday)
    prev_close: float      # Previous close (set at "day open", static until next day)
    prev_price: float      # Price on the previous tick (for direction arrow)
    volume: int = 0        # Accumulated simulated volume
    day_start: float = field(default_factory=time.time)  # When today's session started
```

---

## 7. Full SimulatorProvider Implementation

```python
# backend/market/simulator_provider.py

import asyncio
import math
import random
import time
import logging
from typing import Dict, List, Optional

from .base import MarketDataProvider
from .cache import PriceCache
from .models import PricePoint
from .simulator_config import DEFAULT_TICKER_CONFIGS, FALLBACK_CONFIG, TickerConfig
from .simulator_state import TickerState

logger = logging.getLogger(__name__)

# Time constants
SECONDS_PER_TRADING_YEAR = 252 * 6.5 * 3600  # ≈ 5,900,400 seconds
TICK_INTERVAL = 0.5                            # seconds per update
DT_ANNUAL = TICK_INTERVAL / SECONDS_PER_TRADING_YEAR

# Event parameters
EVENT_PROBABILITY = 0.0003
EVENT_MIN_MAGNITUDE = 0.02
EVENT_MAX_MAGNITUDE = 0.05


class SimulatorProvider(MarketDataProvider):
    """
    Market data simulator using correlated Geometric Brownian Motion.
    
    Produces realistic-looking price streams without any external API.
    Each ticker follows its own GBM process; a common market factor
    induces cross-ticker correlation.
    """

    def __init__(
        self,
        initial_tickers: List[str],
        seed: Optional[int] = None,
        tick_interval: float = TICK_INTERVAL,
    ) -> None:
        self._tickers: List[str] = list(initial_tickers)
        self._rng = random.Random(seed)
        self._tick_interval = tick_interval
        self._cache = PriceCache()
        self._states: Dict[str, TickerState] = {}
        self._task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # MarketDataProvider interface
    # ------------------------------------------------------------------

    async def start(self) -> None:
        for ticker in self._tickers:
            self._init_ticker(ticker)
        # Populate cache with initial prices before first client connects
        await self._write_cache()
        self._task = asyncio.create_task(self._tick_loop(), name="simulator-ticker")
        logger.info("SimulatorProvider started (%d tickers)", len(self._tickers))

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("SimulatorProvider stopped")

    async def get_prices(self, tickers: List[str]) -> Dict[str, PricePoint]:
        return await self._cache.get_all(tickers)

    async def update_tickers(self, tickers: List[str]) -> None:
        new_tickers = [t for t in tickers if t not in self._states]
        for ticker in new_tickers:
            self._init_ticker(ticker)
        self._tickers = list(tickers)
        logger.debug("Simulator watchlist updated: %s", self._tickers)

    # ------------------------------------------------------------------
    # Simulation internals
    # ------------------------------------------------------------------

    def _init_ticker(self, ticker: str) -> None:
        """Initialise a new ticker's state from config or fallback defaults."""
        cfg: TickerConfig = DEFAULT_TICKER_CONFIGS.get(ticker, FALLBACK_CONFIG)
        # Add small random perturbation to seed price (±2%) for visual variety
        perturb = 1 + self._rng.uniform(-0.02, 0.02)
        start_price = round(cfg.seed_price * perturb, 2)
        self._states[ticker] = TickerState(
            ticker=ticker,
            price=start_price,
            open=start_price,
            high=start_price,
            low=start_price,
            prev_close=round(start_price * (1 + self._rng.uniform(-0.015, 0.015)), 2),
            prev_price=start_price,
        )

    async def _tick_loop(self) -> None:
        """Main simulation loop: fire once per tick_interval."""
        while True:
            await asyncio.sleep(self._tick_interval)
            self._advance()
            await self._write_cache()

    def _advance(self) -> None:
        """
        Advance all tickers by one time step using correlated GBM.
        
        Algorithm:
        1. Draw a single market-wide shock Z_market ~ N(0,1)
        2. For each ticker:
           a. Draw idiosyncratic shock Z_idio ~ N(0,1)
           b. Combine: Z_eff = rho * Z_market + sqrt(1-rho^2) * Z_idio
           c. Apply GBM: price *= exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z_eff)
           d. Possibly apply random event
           e. Update intraday high/low
        """
        z_market = self._rng.gauss(0, 1)
        sqrt_dt = math.sqrt(DT_ANNUAL)

        for ticker in self._tickers:
            if ticker not in self._states:
                continue

            state = self._states[ticker]
            cfg = DEFAULT_TICKER_CONFIGS.get(ticker, FALLBACK_CONFIG)

            # Correlated random shock
            z_idio = self._rng.gauss(0, 1)
            z_eff = cfg.rho * z_market + math.sqrt(1 - cfg.rho ** 2) * z_idio

            # GBM step (log-normal update)
            drift_term = (cfg.mu - 0.5 * cfg.sigma ** 2) * DT_ANNUAL
            diffusion_term = cfg.sigma * sqrt_dt * z_eff
            state.prev_price = state.price
            state.price *= math.exp(drift_term + diffusion_term)

            # Random event (sudden jump)
            state.price = self._maybe_apply_event(state.price)

            # Floor at $0.01 (price can never be negative or zero)
            state.price = max(state.price, 0.01)

            # Round to 2 decimal places
            state.price = round(state.price, 2)

            # Update intraday high/low
            state.high = max(state.high, state.price)
            state.low  = min(state.low,  state.price)

            # Simulate volume: add a random trade size each tick
            state.volume += self._rng.randint(100, 50000)

    def _maybe_apply_event(self, price: float) -> float:
        """Randomly inject a sudden price move (earnings/news simulation)."""
        if self._rng.random() < EVENT_PROBABILITY:
            magnitude = self._rng.uniform(EVENT_MIN_MAGNITUDE, EVENT_MAX_MAGNITUDE)
            direction = self._rng.choice([-1, 1])
            price *= (1 + direction * magnitude)
            logger.debug("Price event fired: %.2f%%", direction * magnitude * 100)
        return price

    async def _write_cache(self) -> None:
        """Flush current states to the shared price cache."""
        now = time.time()
        points: Dict[str, PricePoint] = {}
        for ticker in self._tickers:
            if ticker not in self._states:
                continue
            s = self._states[ticker]
            points[ticker] = PricePoint(
                ticker=ticker,
                price=s.price,
                prev_price=s.prev_price,
                open=s.open,
                high=s.high,
                low=s.low,
                prev_close=s.prev_close,
                volume=s.volume,
                timestamp=now,
            )
        await self._cache.set_many(points)
```

---

## 8. File Structure

```
backend/
└── market/
    ├── __init__.py
    ├── models.py               # PricePoint dataclass (shared with Massive provider)
    ├── base.py                 # MarketDataProvider ABC
    ├── cache.py                # PriceCache
    ├── factory.py              # create_provider()
    ├── massive_provider.py     # Live data via Massive REST API
    ├── simulator_provider.py   # SimulatorProvider (this file)
    ├── simulator_config.py     # TickerConfig, DEFAULT_TICKER_CONFIGS
    └── simulator_state.py      # TickerState dataclass
```

---

## 9. Behavioural Properties

| Property | Value / Notes |
|---|---|
| Update frequency | 500ms per tick |
| Price range | Bounded below by $0.01; no upper bound |
| GBM dt | `0.5 / 5,900,400 ≈ 8.47e-8` (fraction of a trading year) |
| Effective hourly vol | Annualised σ compressed to ~500ms ticks |
| Correlation | Parameterised per ticker via `rho`; market factor drawn once per tick |
| Events | ~0.03% chance per ticker per tick; ±2–5% instantaneous jump |
| Intraday OHLC | Open set at init; high/low updated each tick; prev_close static |
| Volume | Simulated as random increments; useful only for display |
| Determinism | Pass `seed=int` to `SimulatorProvider` for reproducible streams |

---

## 10. Testing the Simulator

```python
# tests/test_simulator.py

import asyncio
import pytest
from market.simulator_provider import SimulatorProvider


@pytest.mark.asyncio
async def test_prices_update_over_time():
    sim = SimulatorProvider(initial_tickers=["AAPL"], seed=42, tick_interval=0.05)
    await sim.start()
    
    prices_before = await sim.get_prices(["AAPL"])
    await asyncio.sleep(0.2)  # Wait for a few ticks
    prices_after = await sim.get_prices(["AAPL"])
    
    # Prices should change (extremely unlikely to be identical with GBM)
    assert prices_before["AAPL"].price != prices_after["AAPL"].price
    
    await sim.stop()


@pytest.mark.asyncio
async def test_new_ticker_added_dynamically():
    sim = SimulatorProvider(initial_tickers=["AAPL"], seed=0)
    await sim.start()
    
    await sim.update_tickers(["AAPL", "MSFT"])
    await asyncio.sleep(0.6)
    prices = await sim.get_prices(["AAPL", "MSFT"])
    
    assert "AAPL" in prices
    assert "MSFT" in prices
    assert prices["MSFT"].price > 0
    
    await sim.stop()


@pytest.mark.asyncio
async def test_price_never_goes_negative():
    sim = SimulatorProvider(initial_tickers=["TSLA"], seed=99, tick_interval=0.01)
    await sim.start()
    await asyncio.sleep(1.0)
    prices = await sim.get_prices(["TSLA"])
    assert prices["TSLA"].price >= 0.01
    await sim.stop()
```

---

## 11. Extension Points

- **Day boundary reset**: At market open (9:30 AM ET), reset `open`, `high`, `low`, set `prev_close = last price`. Currently the simulator runs continuously without day boundaries (simplification for a trading demo).
- **Mean reversion**: Replace pure GBM with Ornstein-Uhlenbeck to prevent prices drifting too far from seed over long sessions.
- **Sector ETFs**: Add sector-level intermediate factors (tech, finance, energy) between the market factor and individual tickers for richer cross-ticker correlation structure.
- **Custom event schedules**: Allow injecting scripted events (e.g. an "earnings" event for AAPL at a specific wall-clock time) for demos or testing.
