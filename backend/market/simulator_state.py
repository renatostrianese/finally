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
