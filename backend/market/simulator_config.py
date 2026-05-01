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
