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
            "SimulatorProvider started (tickers=%d, tick=%.2fs)",
            len(self._tickers), self._tick_interval,
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
