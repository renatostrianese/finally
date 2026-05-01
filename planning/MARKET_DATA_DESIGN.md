# Market Data Backend — Detailed Design

This document is the single authoritative reference for everything related to market data in FinAlly. It covers the unified provider interface, the built-in simulator, and the Massive (formerly Polygon.io) REST API integration, with full code snippets and worked examples for each component.

---

## Table of Contents

1. [Big Picture](#1-big-picture)
2. [File Layout](#2-file-layout)
3. [Data Model (`models.py`)](#3-data-model-modelspy)
4. [Abstract Interface (`base.py`)](#4-abstract-interface-basepy)
5. [Price Cache (`cache.py`)](#5-price-cache-cachepy)
6. [Provider Factory (`factory.py`)](#6-provider-factory-factorypy)
7. [Simulator Provider](#7-simulator-provider)
   - 7.1 [Price Model — Geometric Brownian Motion](#71-price-model--geometric-brownian-motion)
   - 7.2 [Correlation Structure](#72-correlation-structure)
   - 7.3 [Ticker Configuration (`simulator_config.py`)](#73-ticker-configuration-simulator_configpy)
   - 7.4 [Ticker State (`simulator_state.py`)](#74-ticker-state-simulator_statepy)
   - 7.5 [Event System](#75-event-system)
   - 7.6 [Full Implementation (`simulator_provider.py`)](#76-full-implementation-simulator_providerpy)
8. [Massive Provider](#8-massive-provider)
   - 8.1 [Authentication and Client Setup](#81-authentication-and-client-setup)
   - 8.2 [Snapshot Endpoint (Primary)](#82-snapshot-endpoint-primary)
   - 8.3 [Supporting Endpoints](#83-supporting-endpoints)
   - 8.4 [Rate Limits and Poll Intervals](#84-rate-limits-and-poll-intervals)
   - 8.5 [Full Implementation (`massive_provider.py`)](#85-full-implementation-massive_providerpy)
   - 8.6 [Error Handling](#86-error-handling)
9. [FastAPI Integration](#9-fastapi-integration)
   - 9.1 [Startup / Shutdown Lifecycle](#91-startup--shutdown-lifecycle)
   - 9.2 [SSE Streaming Endpoint](#92-sse-streaming-endpoint)
   - 9.3 [Watchlist Update Endpoint](#93-watchlist-update-endpoint)
10. [Testing](#10-testing)
    - 10.1 [Unit Tests — Simulator](#101-unit-tests--simulator)
    - 10.2 [Unit Tests — Cache](#102-unit-tests--cache)
    - 10.3 [Integration Tests — Massive Provider (Mock)](#103-integration-tests--massive-provider-mock)
    - 10.4 [Contract Tests — Both Providers Share the Same ABC](#104-contract-tests--both-providers-share-the-same-abc)
11. [Operational Reference](#11-operational-reference)
    - 11.1 [Environment Variables](#111-environment-variables)
    - 11.2 [Logging](#112-logging)
    - 11.3 [Extension Points](#113-extension-points)

---

## 1. Big Picture

```
┌──────────────────────────────────────────────────────┐
│  FastAPI process                                     │
│                                                      │
│  ┌──────────────────────────────────────────────┐   │
│  │  Background task (lifespan)                  │   │
│  │  MarketDataProvider.start()                  │   │
│  │  ┌──────────────┐  OR  ┌──────────────────┐  │   │
│  │  │ SimulatorPro │      │  MassiveProvider  │  │   │
│  │  │ GBM tick loop│      │  REST poll loop   │  │   │
│  │  └──────┬───────┘      └────────┬─────────┘  │   │
│  │         │  writes PricePoint    │             │   │
│  │         └──────────┬────────────┘             │   │
│  │                    ▼                          │   │
│  │              PriceCache                       │   │
│  │            (Dict[str, PricePoint])            │   │
│  └──────────────────────────────────────────────┘   │
│                       │ reads                        │
│  ┌────────────────────▼─────────────────────────┐   │
│  │  GET /api/stream/prices (SSE)                │   │
│  │  → one generator per connected client        │   │
│  └──────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────┘
```

**Key invariants:**
- Consumer code (SSE, portfolio valuation, AI tools) calls **only** the `MarketDataProvider` interface — never the Massive client or simulator directly.
- The background task owns all writes to `PriceCache`; SSE handlers own all reads.
- Swapping from simulator to live data requires only setting `MASSIVE_API_KEY` — zero code changes.

---

## 2. File Layout

```
backend/
└── market/
    ├── __init__.py           # re-exports create_provider and PricePoint for convenience
    ├── models.py             # PricePoint dataclass
    ├── base.py               # MarketDataProvider ABC
    ├── cache.py              # PriceCache (async in-memory store)
    ├── factory.py            # create_provider() — environment-driven selection
    ├── massive_provider.py   # MassiveProvider — live data via REST
    ├── simulator_provider.py # SimulatorProvider — GBM-based fake data
    ├── simulator_config.py   # TickerConfig, DEFAULT_TICKER_CONFIGS
    └── simulator_state.py    # TickerState — mutable per-ticker sim state
```

---

## 3. Data Model (`models.py`)

`PricePoint` is the single currency of information that flows through the entire market data pipeline — from provider through cache to SSE to frontend.

```python
# backend/market/models.py

from dataclasses import dataclass, field
from typing import Optional
import time


@dataclass
class PricePoint:
    """A single price observation for a ticker.

    Produced by both providers and stored in PriceCache.
    Consumed by SSE handlers, portfolio valuation, and AI tools.
    """
    ticker: str
    price: float         # Current price (latest trade or close)
    prev_price: float    # Price on the previous observation (for direction arrow)
    open: float          # Today's session open price
    high: float          # Today's intraday high
    low: float           # Today's intraday low
    prev_close: float    # Previous trading day's closing price
    volume: Optional[int] = None           # Today's accumulated volume (None if unknown)
    timestamp: float = field(default_factory=time.time)  # Unix seconds (UTC)

    # ------------------------------------------------------------------
    # Derived properties — computed on the fly; not stored
    # ------------------------------------------------------------------

    @property
    def change(self) -> float:
        """Absolute price change from previous close."""
        return round(self.price - self.prev_close, 2)

    @property
    def change_pct(self) -> float:
        """Percentage price change from previous close."""
        if self.prev_close == 0:
            return 0.0
        return round((self.price - self.prev_close) / self.prev_close * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat' relative to the immediately preceding observation."""
        if self.price > self.prev_price:
            return "up"
        elif self.price < self.prev_price:
            return "down"
        return "flat"

    def to_sse_dict(self) -> dict:
        """Serialise to the shape pushed over SSE to the frontend."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "prev_price": self.prev_price,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "prev_close": self.prev_close,
            "change": self.change,
            "change_pct": self.change_pct,
            "direction": self.direction,
            "volume": self.volume,
            "timestamp": self.timestamp,
        }
```

**Example usage:**

```python
from market.models import PricePoint

p = PricePoint(
    ticker="AAPL",
    price=196.45,
    prev_price=195.80,
    open=194.10,
    high=196.90,
    low=193.80,
    prev_close=194.83,
    volume=52_341_200,
)

print(p.change)       # 1.62
print(p.change_pct)   # 0.8315
print(p.direction)    # "up"
print(p.to_sse_dict())
# {
#   "ticker": "AAPL", "price": 196.45, "prev_price": 195.80,
#   "open": 194.10, "high": 196.90, "low": 193.80, "prev_close": 194.83,
#   "change": 1.62, "change_pct": 0.8315, "direction": "up",
#   "volume": 52341200, "timestamp": <unix_time>
# }
```

---

## 4. Abstract Interface (`base.py`)

All consumer code is written against this ABC. Both `SimulatorProvider` and `MassiveProvider` are concrete implementations.

```python
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
```

---

## 5. Price Cache (`cache.py`)

The cache is the single in-memory source of truth for current prices. It is written exclusively by the background provider task and read exclusively by SSE handlers and portfolio endpoints.

```python
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
```

**Example — SSE handler using `wait_for_update`:**

```python
async def event_generator(tickers: list[str]):
    while True:
        await cache.wait_for_update(timeout=1.0)   # sleep until data changes
        prices = await cache.get_all(tickers)
        for ticker, point in prices.items():
            yield f"data: {json.dumps(point.to_sse_dict())}\n\n"
```

---

## 6. Provider Factory (`factory.py`)

The factory is the only place where the environment variable is read. All other code receives a `MarketDataProvider` instance without knowing which concrete type it is.

```python
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
```

---

## 7. Simulator Provider

### 7.1 Price Model — Geometric Brownian Motion

Each ticker's price evolves as **Geometric Brownian Motion (GBM)**, the standard continuous-time model for equity prices:

```
dS = S · (μ · dt + σ · dW)
```

Where:
- `S` — current price
- `μ` — annualised drift (expected return), e.g. `0.08` = 8%/year
- `σ` — annualised volatility, e.g. `0.30` = 30%/year
- `dt` — time step expressed as fraction of a trading year
- `dW` — Wiener increment: `√dt · Z` where `Z ~ N(0,1)`

**Discrete update formula** (exact solution, applied each 500ms tick):

```
S_new = S_old · exp( (μ − σ²/2) · dt  +  σ · √dt · Z )
```

The `exp()` form guarantees `S_new > 0` even for extreme random draws.

**Time constant derivation:**

```
One trading year = 252 days × 6.5 hours/day × 3600 s/hour = 5,900,400 seconds
Tick interval    = 0.5 s
dt_annual        = 0.5 / 5,900,400 ≈ 8.47 × 10⁻⁸
√dt_annual       ≈ 2.91 × 10⁻⁴
```

For a stock with σ = 0.30 the standard deviation of each 500ms return is:

```
σ_tick = 0.30 × √(8.47e-8) ≈ 0.0000873  (≈ 0.009% per tick)
```

This is deliberately small — prices move smoothly on screen without jarring jumps during normal operation.

---

### 7.2 Correlation Structure

Real stocks are correlated — tech names tend to move together. The simulator approximates this with a **single common-factor model**:

```
Z_effective = ρ · Z_market  +  √(1 − ρ²) · Z_idiosyncratic
```

Where:
- `Z_market` — one market-wide shock drawn **once per tick** (same for all tickers)
- `Z_idiosyncratic` — ticker-specific shock drawn independently
- `ρ` (rho) — per-ticker correlation with the market factor (0 = fully independent, 1 = perfectly correlated)

**Per-tick algorithm:**

```python
import math, random

rng = random.Random()

def advance_tick(tickers_configs, states):
    z_market = rng.gauss(0, 1)       # one draw, shared by all tickers
    sqrt_dt  = math.sqrt(DT_ANNUAL)

    for ticker, cfg in tickers_configs.items():
        state = states[ticker]
        z_idio = rng.gauss(0, 1)     # independent per-ticker draw
        z_eff  = cfg.rho * z_market + math.sqrt(1 - cfg.rho ** 2) * z_idio

        drift     = (cfg.mu - 0.5 * cfg.sigma ** 2) * DT_ANNUAL
        diffusion = cfg.sigma * sqrt_dt * z_eff

        state.prev_price = state.price
        state.price *= math.exp(drift + diffusion)
        state.price  = max(round(state.price, 2), 0.01)  # floor at $0.01
```

The correlation between two tickers `i` and `j` (both using the factor model) is:

```
Corr(Z_i, Z_j) = ρ_i · ρ_j
```

For AAPL (ρ=0.70) and MSFT (ρ=0.68): `Corr ≈ 0.476` — a realistic intraday correlation for large-cap tech.

---

### 7.3 Ticker Configuration (`simulator_config.py`)

```python
# backend/market/simulator_config.py

from dataclasses import dataclass


@dataclass(frozen=True)
class TickerConfig:
    """Immutable configuration for one ticker's GBM process."""
    seed_price: float   # Starting price in USD (approximate real-world value, early 2025)
    sigma: float        # Annualised volatility, e.g. 0.30 = 30%
    mu: float           # Annualised drift (expected return), e.g. 0.08 = 8%
    rho: float          # Market factor correlation, 0.0–1.0


DEFAULT_TICKER_CONFIGS: dict[str, TickerConfig] = {
    # ── Large-cap tech ─────────────────────────────────────────────────
    # High rho (move with the market); moderate-to-high volatility
    "AAPL":  TickerConfig(seed_price=195.00, sigma=0.28, mu=0.08, rho=0.70),
    "MSFT":  TickerConfig(seed_price=415.00, sigma=0.26, mu=0.10, rho=0.68),
    "GOOGL": TickerConfig(seed_price=175.00, sigma=0.30, mu=0.08, rho=0.65),
    "AMZN":  TickerConfig(seed_price=185.00, sigma=0.32, mu=0.10, rho=0.65),
    "META":  TickerConfig(seed_price=520.00, sigma=0.38, mu=0.12, rho=0.62),
    "NVDA":  TickerConfig(seed_price=870.00, sigma=0.55, mu=0.15, rho=0.60),
    "TSLA":  TickerConfig(seed_price=175.00, sigma=0.65, mu=0.05, rho=0.45),
    "NFLX":  TickerConfig(seed_price=650.00, sigma=0.40, mu=0.10, rho=0.58),
    # ── Financials ─────────────────────────────────────────────────────
    # Moderate rho; lower vol than tech
    "JPM":   TickerConfig(seed_price=200.00, sigma=0.22, mu=0.07, rho=0.55),
    "V":     TickerConfig(seed_price=280.00, sigma=0.20, mu=0.09, rho=0.52),
    # ── Defensive / value ──────────────────────────────────────────────
    # Lower rho; lower vol; moves more independently of the market
    "BRK.B": TickerConfig(seed_price=410.00, sigma=0.18, mu=0.07, rho=0.45),
}

# Used for any ticker added dynamically that isn't in the table above
FALLBACK_CONFIG = TickerConfig(seed_price=100.00, sigma=0.30, mu=0.07, rho=0.55)
```

---

### 7.4 Ticker State (`simulator_state.py`)

One `TickerState` is created per tracked ticker when the simulator starts (or when a new ticker is added via `update_tickers`).

```python
# backend/market/simulator_state.py

from dataclasses import dataclass, field
import time


@dataclass
class TickerState:
    """
    Mutable per-ticker state maintained by the simulator.

    Updated every 500ms by the _advance() method.
    Flushed to PriceCache by _write_cache() after each tick.
    """
    ticker: str
    price: float        # Current price
    open: float         # Today's session open (set at initialisation; reset daily if implemented)
    high: float         # Intraday high (max of all prices since open)
    low: float          # Intraday low  (min of all prices since open)
    prev_close: float   # Previous day's close (static until day boundary reset)
    prev_price: float   # Price on the last tick (used for direction arrow)
    volume: int = 0     # Simulated accumulated volume (incremented each tick)
    day_start: float = field(default_factory=time.time)  # Epoch seconds when session opened
```

---

### 7.5 Event System

To make the UI feel alive, the simulator randomly injects sudden price jumps — mimicking earnings surprises, analyst upgrades, or news headlines.

```python
# Probability and magnitude constants (top of simulator_provider.py)

EVENT_PROBABILITY   = 0.0003   # Chance of an event per ticker per tick
EVENT_MIN_MAGNITUDE = 0.02     # Minimum jump size (2%)
EVENT_MAX_MAGNITUDE = 0.05     # Maximum jump size (5%)
```

**Expected frequency:**
- With 500ms ticks: ~2 events/second across all tickers.
- Per-ticker: `1 / (0.0003 × 2)` ≈ one event every ~28 minutes per ticker.

```python
def _maybe_apply_event(self, price: float) -> float:
    """Randomly inject a sudden price jump. Returns the (possibly unchanged) price."""
    if self._rng.random() < EVENT_PROBABILITY:
        magnitude = self._rng.uniform(EVENT_MIN_MAGNITUDE, EVENT_MAX_MAGNITUDE)
        direction = self._rng.choice([-1, 1])
        price *= (1 + direction * magnitude)
        logger.debug(
            "Simulated event: %+.1f%% applied",
            direction * magnitude * 100,
        )
    return price
```

---

### 7.6 Full Implementation (`simulator_provider.py`)

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

# ── Time constants ─────────────────────────────────────────────────────────────
SECONDS_PER_TRADING_YEAR = 252 * 6.5 * 3600   # ≈ 5,900,400 seconds
TICK_INTERVAL = 0.5                             # seconds between GBM steps
DT_ANNUAL = TICK_INTERVAL / SECONDS_PER_TRADING_YEAR  # ≈ 8.47e-8

# ── Event parameters ───────────────────────────────────────────────────────────
EVENT_PROBABILITY   = 0.0003
EVENT_MIN_MAGNITUDE = 0.02
EVENT_MAX_MAGNITUDE = 0.05


class SimulatorProvider(MarketDataProvider):
    """
    Market data provider using correlated Geometric Brownian Motion.

    Produces realistic-looking price streams with no external dependencies.
    Each ticker follows its own GBM process; a single market-wide factor
    induces cross-ticker correlation parameterised by rho.

    Suitable for development, testing, and demo environments where a
    MASSIVE_API_KEY is not available.

    Args:
        initial_tickers: Tickers to simulate from startup.
        seed: Optional integer seed for the RNG — makes the simulation
              deterministic and reproducible. Omit for true randomness.
        tick_interval: Seconds between price updates (default 0.5).
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

    # ── MarketDataProvider interface ───────────────────────────────────────────

    async def start(self) -> None:
        """Initialise all ticker states, pre-populate cache, start tick loop."""
        for ticker in self._tickers:
            self._init_ticker(ticker)
        await self._write_cache()   # so first SSE client is not served empty data
        self._task = asyncio.create_task(
            self._tick_loop(), name="simulator-ticker"
        )
        logger.info(
            "SimulatorProvider started (tickers=%d, tick=%.2fs, seed=%s)",
            len(self._tickers), self._tick_interval,
            self._rng.getstate()[1][0],
        )

    async def stop(self) -> None:
        """Cancel the tick loop and log shutdown."""
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
        """Add any new tickers and update the active watchlist."""
        new_tickers = [t for t in tickers if t not in self._states]
        for ticker in new_tickers:
            self._init_ticker(ticker)
        self._tickers = list(tickers)
        if new_tickers:
            await self._write_cache()  # immediately expose new tickers in cache
            logger.debug("Simulator added new tickers: %s", new_tickers)

    # ── Initialisation ─────────────────────────────────────────────────────────

    def _init_ticker(self, ticker: str) -> None:
        """
        Initialise a TickerState from config.

        Adds a small random perturbation (±2%) to the seed price so that
        different sessions start at slightly different levels, which looks
        more realistic on screen.
        """
        cfg: TickerConfig = DEFAULT_TICKER_CONFIGS.get(ticker, FALLBACK_CONFIG)
        perturb = 1 + self._rng.uniform(-0.02, 0.02)
        start_price = round(cfg.seed_price * perturb, 2)
        prev_close  = round(start_price * (1 + self._rng.uniform(-0.015, 0.015)), 2)
        self._states[ticker] = TickerState(
            ticker=ticker,
            price=start_price,
            open=start_price,
            high=start_price,
            low=start_price,
            prev_close=prev_close,
            prev_price=start_price,
        )

    # ── Simulation loop ────────────────────────────────────────────────────────

    async def _tick_loop(self) -> None:
        """Fire _advance() + _write_cache() every tick_interval seconds."""
        while True:
            await asyncio.sleep(self._tick_interval)
            self._advance()
            await self._write_cache()

    def _advance(self) -> None:
        """
        Advance all tickers by one time step.

        Draws one market shock (shared) and one idiosyncratic shock (per ticker),
        combines them via the factor model, applies the GBM step, and optionally
        fires a sudden event.
        """
        z_market = self._rng.gauss(0, 1)
        sqrt_dt  = math.sqrt(DT_ANNUAL)

        for ticker in self._tickers:
            if ticker not in self._states:
                continue

            state = self._states[ticker]
            cfg   = DEFAULT_TICKER_CONFIGS.get(ticker, FALLBACK_CONFIG)

            # Correlated random shock
            z_idio = self._rng.gauss(0, 1)
            z_eff  = cfg.rho * z_market + math.sqrt(1.0 - cfg.rho ** 2) * z_idio

            # GBM log-normal step
            drift     = (cfg.mu - 0.5 * cfg.sigma ** 2) * DT_ANNUAL
            diffusion = cfg.sigma * sqrt_dt * z_eff

            state.prev_price  = state.price
            state.price      *= math.exp(drift + diffusion)

            # Random event injection
            state.price = self._maybe_apply_event(state.price)

            # Safety floor — price can never go to zero or below
            state.price = max(round(state.price, 2), 0.01)

            # Maintain intraday OHLC
            state.high = max(state.high, state.price)
            state.low  = min(state.low,  state.price)

            # Simulated volume: ~10,000 shares traded per 500ms tick on average
            state.volume += self._rng.randint(100, 50_000)

    def _maybe_apply_event(self, price: float) -> float:
        """Probabilistically inject a sudden price jump (news/earnings simulation)."""
        if self._rng.random() < EVENT_PROBABILITY:
            magnitude = self._rng.uniform(EVENT_MIN_MAGNITUDE, EVENT_MAX_MAGNITUDE)
            direction = self._rng.choice([-1, 1])
            price    *= 1 + direction * magnitude
        return price

    # ── Cache flush ────────────────────────────────────────────────────────────

    async def _write_cache(self) -> None:
        """Convert TickerState objects to PricePoints and push to PriceCache."""
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

## 8. Massive Provider

### 8.1 Authentication and Client Setup

Polygon.io rebranded as **Massive** in October 2025. The Python package and API surface are identical; only the import and domain name changed.

```bash
pip install massive
```

```python
from massive import RESTClient

# Explicit key
client = RESTClient(api_key="YOUR_API_KEY")

# Or let the client read MASSIVE_API_KEY from the environment
client = RESTClient()

# Debug mode — prints raw HTTP requests and responses
client = RESTClient(api_key="YOUR_API_KEY", trace=True, verbose=True)
```

---

### 8.2 Snapshot Endpoint (Primary)

The snapshot endpoint is the **only** endpoint used in FinAlly's polling architecture. It fetches all watched tickers in a single API call, making it maximally rate-efficient.

**REST request:**

```bash
# All tickers in one call
curl "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers\
?tickers=AAPL,MSFT,GOOGL,NVDA&apiKey=YOUR_API_KEY"
```

**Python client:**

```python
from massive import RESTClient

client = RESTClient(api_key="YOUR_API_KEY")
watchlist = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

snapshots = client.get_snapshot_all_tickers("stocks", tickers=watchlist)

for snap in snapshots:
    # Best current price: prefer last trade; fall back to day close
    price = None
    if snap.last_trade and snap.last_trade.price:
        price = float(snap.last_trade.price)
    elif snap.day and snap.day.close:
        price = float(snap.day.close)

    if price is None:
        continue

    print(
        f"{snap.ticker:6s}  "
        f"price={price:8.2f}  "
        f"change={snap.todays_change_perc:+.2f}%  "
        f"O={snap.day.open:.2f} H={snap.day.high:.2f} "
        f"L={snap.day.low:.2f}  "
        f"prevClose={snap.prev_day.close:.2f}"
    )
```

**Abbreviated response shape:**

```json
{
  "status": "OK",
  "count": 1,
  "tickers": [
    {
      "ticker": "AAPL",
      "day": { "o": 194.10, "h": 196.90, "l": 193.80, "c": 196.45, "v": 52341200, "vw": 195.82 },
      "lastTrade": { "p": 196.45, "s": 100, "t": 1713975600000 },
      "lastQuote": { "P": 196.50, "S": 2, "p": 196.45, "s": 3 },
      "prevDay":   { "o": 193.50, "h": 195.20, "l": 193.10, "c": 194.83, "v": 48920000 },
      "todaysChange": 1.62,
      "todaysChangePerc": 0.83,
      "updated": 1713975600000
    }
  ]
}
```

**Field mapping to `PricePoint`:**

| Massive field       | `PricePoint` field | Notes                                              |
|---|---|---|
| `lastTrade.p`       | `price`            | Preferred; falls back to `day.c`                  |
| `day.o`             | `open`             |                                                    |
| `day.h`             | `high`             |                                                    |
| `day.l`             | `low`              |                                                    |
| `prevDay.c`         | `prev_close`       | Static EOD reference                               |
| `day.v`             | `volume`           |                                                    |
| Previous `price`    | `prev_price`       | Tracked in `_prev_prices` dict between poll cycles |
| `time.time()`       | `timestamp`        | Set at fetch time                                  |

---

### 8.3 Supporting Endpoints

These endpoints are not used in FinAlly's background poller but are useful for one-off lookups (e.g. historical charts).

**Previous close (single ticker):**

```python
# GET /v2/aggs/ticker/{ticker}/prev
prev_bars = client.get_previous_close("AAPL")
for bar in prev_bars:
    print(f"Prev close: {bar.close} on {bar.timestamp}")
```

**Historical daily bars:**

```python
# GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}
bars = list(client.list_aggs(
    ticker="AAPL",
    multiplier=1,
    timespan="day",
    from_="2025-01-01",
    to="2025-03-31",
    adjusted=True,
    limit=50_000,
))

for bar in bars:
    print(f"{bar.timestamp}  O={bar.open:.2f} H={bar.high:.2f} "
          f"L={bar.low:.2f} C={bar.close:.2f} V={bar.volume}")
```

**Last trade (single ticker, real-time):**

```python
trade = client.get_last_trade("AAPL")
print(f"Last trade: ${trade.price} at {trade.participant_timestamp}")
```

**Last quote / NBBO:**

```python
quote = client.get_last_quote("AAPL")
print(f"Bid: ${quote.bid_price}  Ask: ${quote.ask_price}")
```

---

### 8.4 Rate Limits and Poll Intervals

| Plan              | Rate Limit     | Data Freshness   | Recommended `poll_interval` |
|---|---|---|---|
| Free (Starter)    | 5 req/min      | 15-min delayed   | 15–60 s                     |
| Stocks Starter    | ~20 req/min    | Real-time        | 5–15 s                      |
| Stocks Developer+ | Unlimited (FU) | Real-time        | 2–5 s                       |

The snapshot endpoint counts as **one API call** regardless of how many tickers are included. With a 15-second interval the free tier is well within limits (4 calls/min).

---

### 8.5 Full Implementation (`massive_provider.py`)

```python
# backend/market/massive_provider.py

import asyncio
import logging
import time
from typing import Dict, List

from massive import RESTClient
from massive.rest.models import RequestError

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
```

---

### 8.6 Error Handling

The `_fetch_and_cache` method catches all exceptions and logs a warning. This means:

- On a transient network error the cache retains the last-known values.
- The next poll cycle will retry automatically.
- No exception propagates to the SSE handler or crashes the server.

For explicit error classification (useful in a retry strategy or alerting):

```python
from massive.rest.models import RequestError

try:
    snapshots = client.get_snapshot_all_tickers("stocks", tickers=watchlist)
except RequestError as e:
    match e.status_code:
        case 403:
            logger.error("Invalid or expired MASSIVE_API_KEY")
        case 429:
            logger.warning("Rate limit exceeded — backing off")
            await asyncio.sleep(60)   # extra back-off
        case 404:
            logger.warning("Snapshot endpoint returned 404 (market closed?)")
        case _:
            logger.error("Massive API error %d: %s", e.status_code, e)
except Exception as e:
    logger.warning("Network error fetching prices: %s", e)
```

---

## 9. FastAPI Integration

### 9.1 Startup / Shutdown Lifecycle

The provider is created, started, and stopped inside FastAPI's `lifespan` async context manager. This guarantees:

1. The cache is pre-populated before the first HTTP request is served.
2. The background task is cleanly cancelled when the server shuts down (no zombie tasks or unclosed connections).

```python
# backend/main.py

from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI

from market.factory import create_provider, DEFAULT_TICKERS
from market.base import MarketDataProvider

logger = logging.getLogger(__name__)

# Module-level reference — shared between lifespan and route handlers
provider: MarketDataProvider | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global provider

    # ── Startup ──────────────────────────────────────────────────────────────
    # In production, load the saved watchlist from SQLite here:
    #   tickers = await db.get_watchlist()
    # For now, use the default list:
    tickers = DEFAULT_TICKERS

    provider = create_provider(initial_tickers=tickers)
    await provider.start()
    logger.info("Market data provider is live")

    yield   # application runs

    # ── Shutdown ─────────────────────────────────────────────────────────────
    await provider.stop()
    logger.info("Market data provider stopped")


app = FastAPI(title="FinAlly API", lifespan=lifespan)
```

---

### 9.2 SSE Streaming Endpoint

The SSE endpoint delivers price updates to the frontend. It uses `wait_for_update` on the cache to sleep efficiently between updates, rather than polling on a fixed timer.

```python
# backend/routes/stream.py

import asyncio
import json
import logging

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from market.base import MarketDataProvider   # injected via dependency

router = APIRouter(prefix="/api/stream")
logger = logging.getLogger(__name__)


def get_provider() -> MarketDataProvider:
    """FastAPI dependency — returns the running provider."""
    from main import provider
    return provider


@router.get("/prices", response_class=StreamingResponse)
async def stream_prices():
    """
    Long-lived SSE connection that pushes price updates to the browser.

    The client uses the browser's native EventSource API:

        const es = new EventSource('/api/stream/prices');
        es.onmessage = (event) => {
            const data = JSON.parse(event.data);
            // { ticker, price, prev_price, change, change_pct, direction, ... }
        };

    The stream runs indefinitely. If the server disconnects, EventSource
    automatically reconnects after a brief delay.
    """
    prov = get_provider()

    async def event_generator():
        logger.debug("SSE client connected")
        try:
            while True:
                # Wait for new data or 1-second timeout (heartbeat)
                await prov._cache.wait_for_update(timeout=1.0)

                # In production, fetch the user's watchlist from SQLite here
                from market.factory import DEFAULT_TICKERS
                prices = await prov.get_prices(DEFAULT_TICKERS)

                for ticker, point in prices.items():
                    payload = json.dumps(point.to_sse_dict())
                    yield f"data: {payload}\n\n"

        except asyncio.CancelledError:
            logger.debug("SSE client disconnected")
            return

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # disable Nginx proxy buffering
        },
    )
```

**Frontend JavaScript (reference):**

```typescript
// frontend/lib/usePriceStream.ts (excerpt)

const es = new EventSource('/api/stream/prices');

es.onmessage = (event: MessageEvent) => {
  const data = JSON.parse(event.data) as PriceUpdate;
  dispatch({ type: 'PRICE_UPDATE', payload: data });
};

es.onerror = () => {
  setConnectionStatus('reconnecting');
  // EventSource handles reconnection automatically
};
```

---

### 9.3 Watchlist Update Endpoint

When a user adds or removes a ticker, the backend updates the watchlist in SQLite and notifies the provider:

```python
# backend/routes/watchlist.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/watchlist")


class WatchlistUpdateRequest(BaseModel):
    tickers: list[str]   # complete new watchlist, not a delta


@router.put("/")
async def update_watchlist(body: WatchlistUpdateRequest):
    """
    Replace the watchlist with a new set of tickers.

    Also notifies the market data provider so new tickers are fetched /
    simulated immediately.
    """
    from main import provider

    # 1. Validate all tickers are uppercase non-empty strings
    tickers = [t.strip().upper() for t in body.tickers if t.strip()]
    if not tickers:
        raise HTTPException(status_code=400, detail="Watchlist cannot be empty")

    # 2. Persist to SQLite
    # await db.set_watchlist(tickers)   # replace with real DB call

    # 3. Notify the provider — it will begin tracking any new tickers
    await provider.update_tickers(tickers)

    return {"tickers": tickers, "status": "updated"}
```

**Adding a single ticker (convenience endpoint):**

```python
@router.post("/{ticker}")
async def add_ticker(ticker: str):
    ticker = ticker.strip().upper()
    # current_watchlist = await db.get_watchlist()
    current_watchlist = ["AAPL", "MSFT"]  # replace with DB read
    if ticker not in current_watchlist:
        new_watchlist = current_watchlist + [ticker]
        await provider.update_tickers(new_watchlist)
    return {"ticker": ticker, "status": "added"}


@router.delete("/{ticker}")
async def remove_ticker(ticker: str):
    ticker = ticker.strip().upper()
    # current_watchlist = await db.get_watchlist()
    current_watchlist = ["AAPL", "MSFT", ticker]  # replace with DB read
    new_watchlist = [t for t in current_watchlist if t != ticker]
    await provider.update_tickers(new_watchlist)
    return {"ticker": ticker, "status": "removed"}
```

---

## 10. Testing

### 10.1 Unit Tests — Simulator

```python
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
```

---

### 10.2 Unit Tests — Cache

```python
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
```

---

### 10.3 Integration Tests — Massive Provider (Mock)

These tests patch the Massive `RESTClient` so no live network calls are made.

```python
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
```

---

### 10.4 Contract Tests — Both Providers Share the Same ABC

These parametrized tests run against both implementations and ensure they honour the interface contract:

```python
# backend/tests/test_provider_contract.py

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
```

---

## 11. Operational Reference

### 11.1 Environment Variables

| Variable           | Required | Default | Description |
|---|---|---|---|
| `MASSIVE_API_KEY`  | No       | `""`    | If set, enables the Massive live data provider. If empty, simulator is used. |

**Example `.env`:**

```bash
# .env (gitignored)

# Leave blank to use the built-in simulator (recommended for development)
MASSIVE_API_KEY=

# Uncomment and fill in to use real market data
# MASSIVE_API_KEY=your_key_here
```

---

### 11.2 Logging

All market data components log at standard Python logging levels. The recommended logger configuration:

```python
# backend/logging_config.py

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            "datefmt": "%H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
        },
    },
    "loggers": {
        "market": {"level": "INFO", "handlers": ["console"], "propagate": False},
    },
    "root": {"level": "WARNING", "handlers": ["console"]},
}
```

**Key log messages:**

| Logger                  | Level   | Message                                               |
|---|---|---|
| `market.factory`        | INFO    | `"No MASSIVE_API_KEY — using built-in market simulator"` |
| `market.factory`        | INFO    | `"MASSIVE_API_KEY detected — using Massive REST API"` |
| `market.simulator_provider` | INFO | `"SimulatorProvider started (tickers=10, tick=0.50s)"` |
| `market.massive_provider`   | INFO | `"MassiveProvider started (poll_interval=15.0s)"` |
| `market.massive_provider`   | WARNING | `"Massive API fetch failed: <reason>"` |
| `market.simulator_provider` | DEBUG | `"Simulated event: +3.2% applied"` |

---

### 11.3 Extension Points

The following improvements are explicitly out of scope for the initial implementation but are supported by the current architecture:

| Extension | Notes |
|---|---|
| **Day boundary reset** | At 9:30 AM ET, set `prev_close = last price`, reset `open/high/low`. The simulator currently runs continuously without day boundaries. |
| **Mean reversion** | Replace GBM with Ornstein-Uhlenbeck to keep prices near their seed values over long sessions. Change only `_advance()`. |
| **WebSocket streaming** | For real-time paid-tier Massive data, replace the REST poll loop with a `WebSocketClient` subscription. Cache write path is unchanged. |
| **Sector factors** | Add mid-level sector `Z_sector` draws between `Z_market` and `Z_idio` for richer correlation structure (tech, finance, energy sub-groups). |
| **Scripted events** | Allow a test fixture to inject a pre-scheduled event (e.g. AAPL earnings beat at t+60s) for deterministic demo or E2E test scenarios. |
| **Multi-user watchlists** | `update_tickers()` currently accepts a single flat list. Change to `update_tickers(user_id, tickers)` and track a per-user union watchlist internally. |
| **Historical chart data** | Add `get_history(ticker, bars)` to the provider interface. The simulator generates synthetic OHLCV history; the Massive provider fetches from the aggregates endpoint. |
