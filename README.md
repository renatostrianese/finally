# FinAlly — AI Trading Workstation

A Bloomberg-style trading terminal with an AI copilot, built as a capstone for an agentic AI coding course. Entirely constructed by orchestrated coding agents.

## Features

- **Live price stream** — 10 tickers updating every ~500ms with green/red flash animations
- **Sparkline charts** — per-ticker price history built from the SSE stream
- **$10,000 virtual portfolio** — instant market orders, no fees, no login required
- **Portfolio heatmap** — treemap sized by position weight, colored by P&L
- **AI chat assistant** — ask about your portfolio, get analysis, or have the AI execute trades and manage your watchlist via natural language

## Quick Start

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env and add your OPENROUTER_API_KEY

# 2. Launch
./scripts/start_mac.sh        # macOS / Linux
.\scripts\start_windows.ps1   # Windows (PowerShell)
```

Open **http://localhost:8000**. No real money, no signup.

> No `MASSIVE_API_KEY`? The built-in market simulator runs by default — no external API needed.

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `OPENROUTER_API_KEY` | ✅ | LLM chat via OpenRouter (Cerebras) |
| `MASSIVE_API_KEY` | No | Real market data; simulator used if absent |
| `LLM_MOCK` | No | `true` for deterministic mock responses (CI/testing) |

## Architecture

Single Docker container on port `8000`:

```
FastAPI (Python / uv)
├── /api/*         REST endpoints
├── /api/stream/*  SSE price streaming
└── /*             Serves static Next.js export

SQLite  (volume-mounted at db/finally.db)
Background task: GBM market simulator or Massive API polling
```

**Stack:** Next.js (TypeScript) · FastAPI · SQLite · SSE · LiteLLM / OpenRouter · Docker

## License

[MIT](LICENSE)
