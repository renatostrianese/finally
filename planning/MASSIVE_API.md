# Massive API (formerly Polygon.io) — Reference Guide

Polygon.io rebranded as **Massive** on October 30, 2025. Existing API keys and integrations continue to work unchanged. The API base URL moved from `api.polygon.io` to `api.massive.com` (old domain still supported during transition).

**Documentation:** https://massive.com/docs  
**Python client:** https://github.com/massive-com/client-python  
**Sign up / API keys:** https://massive.com/dashboard/api-keys

---

## 1. Authentication

All requests require an API key, passed either as a query parameter or a Bearer header:

```bash
# Query parameter (simplest for testing)
curl "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?apiKey=YOUR_API_KEY"

# Authorization header (recommended for production)
curl -H "Authorization: Bearer YOUR_API_KEY" \
     "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers"
```

---

## 2. Python Client Setup

```bash
pip install -U massive
```

```python
from massive import RESTClient

client = RESTClient(api_key="YOUR_API_KEY")
# Or rely on env var MASSIVE_API_KEY:
# client = RESTClient()
```

The client defaults to `api.massive.com`. Debug mode shows raw request/response details:

```python
client = RESTClient(api_key="YOUR_API_KEY", trace=True, verbose=True)
```

---

## 3. Key Endpoints for FinAlly

### 3.1 Snapshot — Multiple Tickers (Primary Endpoint)

**`GET /v2/snapshot/locale/us/markets/stocks/tickers`**

Returns a real-time (or 15-min delayed on lower plans) snapshot for multiple tickers in a single request. This is the most efficient approach for polling a watchlist.

**Plan access:**
- Free tier: 15-minute delayed
- Paid plans: Real-time

```python
from massive import RESTClient

client = RESTClient(api_key="YOUR_API_KEY")

# Fetch snapshots for specific tickers
snapshots = client.get_snapshot_all_tickers(
    "stocks",
    tickers=["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
)

for snap in snapshots:
    print(f"{snap.ticker}:")
    print(f"  Last trade price : {snap.last_trade.price if snap.last_trade else 'N/A'}")
    print(f"  Today's change % : {snap.todays_change_perc:.2f}%")
    print(f"  Today OHLC       : O={snap.day.open} H={snap.day.high} L={snap.day.low} C={snap.day.close}")
    print(f"  Prev day close   : {snap.prev_day.close}")
    print(f"  Updated          : {snap.updated}")
```

**Raw HTTP equivalent:**

```bash
curl "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT,GOOGL&apiKey=YOUR_API_KEY"
```

**Response shape:**

```json
{
  "status": "OK",
  "count": 3,
  "tickers": [
    {
      "ticker": "AAPL",
      "day": {
        "o": 195.10,
        "h": 196.80,
        "l": 194.60,
        "c": 196.45,
        "v": 52341200,
        "vw": 195.82
      },
      "lastTrade": {
        "p": 196.45,
        "s": 100,
        "t": 1713975600000,
        "c": [14, 41]
      },
      "lastQuote": {
        "P": 196.50,
        "S": 2,
        "p": 196.45,
        "s": 3,
        "t": 1713975600100
      },
      "min": {
        "o": 196.40,
        "h": 196.55,
        "l": 196.30,
        "c": 196.45,
        "v": 12300,
        "vw": 196.43,
        "t": 1713975540000
      },
      "prevDay": {
        "o": 193.50,
        "h": 195.20,
        "l": 193.10,
        "c": 194.83,
        "v": 48920000,
        "vw": 194.25
      },
      "todaysChange": 1.62,
      "todaysChangePerc": 0.83,
      "updated": 1713975600000
    }
  ]
}
```

**Key fields:**
| Field | Description |
|---|---|
| `day.c` | Current day's close (or latest price during trading hours) |
| `lastTrade.p` | Price of the most recent trade |
| `prevDay.c` | Previous day's closing price (useful as EOD reference) |
| `todaysChange` | Absolute change from previous close |
| `todaysChangePerc` | Percentage change from previous close |
| `updated` | Unix timestamp (ms) of last update |
| `min.c` | Close of the most recent 1-minute bar |

---

### 3.2 Single Ticker Snapshot

**`GET /v2/snapshot/locale/us/markets/stocks/tickers/{ticker}`**

Same data as above but for one ticker:

```python
snap = client.get_snapshot_ticker("stocks", "AAPL")
print(snap.ticker, snap.last_trade.price, snap.todays_change_perc)
```

---

### 3.3 Previous Close (End-of-Day)

**`GET /v2/aggs/ticker/{ticker}/prev`**

Returns the previous trading day's OHLCV bar — the canonical EOD price.

```python
# Single ticker previous close
prev = client.get_previous_close("AAPL")
for bar in prev:
    print(f"AAPL prev close: {bar.close} on {bar.timestamp}")
```

**Raw HTTP:**

```bash
curl "https://api.massive.com/v2/aggs/ticker/AAPL/prev?apiKey=YOUR_API_KEY"
```

**Response:**

```json
{
  "ticker": "AAPL",
  "queryCount": 1,
  "resultsCount": 1,
  "adjusted": true,
  "results": [
    {
      "T": "AAPL",
      "v": 48920000,
      "vw": 194.25,
      "o": 193.50,
      "c": 194.83,
      "h": 195.20,
      "l": 193.10,
      "t": 1713830400000,
      "n": 412000
    }
  ]
}
```

---

### 3.4 Aggregate Bars (Historical / Intraday)

**`GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}`**

Fetches OHLCV bars for any time range and resolution. Use `timespan=day` for daily EOD history.

```python
from massive import RESTClient

client = RESTClient(api_key="YOUR_API_KEY")

# Daily bars for the past 30 days
bars = []
for bar in client.list_aggs(
    ticker="AAPL",
    multiplier=1,
    timespan="day",
    from_="2024-01-01",
    to="2024-01-31",
    limit=50000,
    adjusted=True,
):
    bars.append(bar)

for b in bars:
    print(f"Date: {b.timestamp}  O:{b.open} H:{b.high} L:{b.low} C:{b.close} V:{b.volume}")
```

**Timespan options:** `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`

**Adjusted parameter:** `True` applies split/dividend adjustments (recommended for most use cases).

---

### 3.5 Last Trade

**`GET /v2/last/trade/{ticker}`**

The single most-recent trade, useful for real-time price during market hours:

```python
trade = client.get_last_trade(ticker="AAPL")
print(f"Last trade: ${trade.price} at {trade.participant_timestamp}")
```

---

### 3.6 Last Quote (NBBO)

**`GET /v2/last/nbbo/{ticker}`**

Most recent National Best Bid and Offer:

```python
quote = client.get_last_quote(ticker="AAPL")
print(f"Bid: ${quote.bid_price}  Ask: ${quote.ask_price}")
```

---

## 4. Rate Limits

| Plan | Rate Limit | Snapshot Data Freshness | Recommended Poll Interval |
|---|---|---|---|
| Free (Starter) | 5 req/min | 15-min delayed | 15–60 seconds |
| Stocks Starter | ~20 req/min | Real-time | 5–15 seconds |
| Stocks Developer+ | Unlimited (fair use) | Real-time | 2–5 seconds |

**Notes:**
- The full-market snapshot endpoint (`/v2/snapshot/...`) counts as **one** API call even when requesting many tickers — this is the most rate-efficient approach for polling a watchlist.
- Snapshot data is cleared daily at 3:30 AM ET and repopulated starting ~4:00 AM ET.
- All timestamps are Unix milliseconds in UTC; convert to ET for display.

---

## 5. Fetching Multiple Tickers Efficiently

For FinAlly's polling architecture (REST, not WebSocket), the snapshot endpoint is optimal:

```python
import os
import time
from massive import RESTClient
from typing import Dict

def poll_prices(tickers: list[str], interval_seconds: float = 15.0) -> None:
    """Poll the Massive snapshot endpoint for a watchlist of tickers."""
    client = RESTClient(api_key=os.environ["MASSIVE_API_KEY"])

    while True:
        try:
            snapshots = client.get_snapshot_all_tickers("stocks", tickers=tickers)
            prices: Dict[str, float] = {}
            for snap in snapshots:
                # Prefer lastTrade price during market hours; fall back to day close
                if snap.last_trade and snap.last_trade.price:
                    prices[snap.ticker] = snap.last_trade.price
                elif snap.day and snap.day.close:
                    prices[snap.ticker] = snap.day.close
            print(prices)
        except Exception as e:
            print(f"Error fetching prices: {e}")

        time.sleep(interval_seconds)

if __name__ == "__main__":
    watchlist = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "BRK.B", "JPM", "V"]
    poll_prices(watchlist, interval_seconds=15)
```

---

## 6. WebSocket Streaming (Real-Time Trades)

For paid plans with real-time access, WebSocket streaming eliminates polling latency:

**Endpoints:**
- Real-time: `wss://socket.massive.com/stocks`
- Delayed:   `wss://delayed.socket.massive.com/stocks`

**Subscription channels:**
| Prefix | Data |
|---|---|
| `T.{ticker}` | Trades (e.g. `T.AAPL`) |
| `Q.{ticker}` | NBBO Quotes |
| `AM.{ticker}` | Aggregate per minute |
| `A.{ticker}` | Aggregate per second |

```python
from massive import WebSocketClient
from massive.websocket.models import WebSocketMessage
from typing import List

ws = WebSocketClient(
    api_key="YOUR_API_KEY",
    subscriptions=["T.AAPL", "T.MSFT", "T.GOOGL", "AM.*"],  # or T.* for all trades
)

def handle_msg(msgs: List[WebSocketMessage]) -> None:
    for m in msgs:
        if hasattr(m, "symbol") and hasattr(m, "price"):
            print(f"{m.symbol}: ${m.price}")

ws.run(handle_msg=handle_msg)
```

> **FinAlly Note:** WebSocket streaming is not used in FinAlly's current architecture. We use REST polling via the snapshot endpoint, which is simpler, works on all plan tiers, and is sufficient for ~500ms refresh intervals with the simulator or ~15s intervals with the real API.

---

## 7. Error Handling

```python
from massive import RESTClient
from massive.rest.models import RequestError

client = RESTClient(api_key="YOUR_API_KEY")

try:
    snap = client.get_snapshot_ticker("stocks", "AAPL")
except RequestError as e:
    if e.status_code == 403:
        print("Invalid or expired API key")
    elif e.status_code == 429:
        print("Rate limit exceeded — back off and retry")
    elif e.status_code == 404:
        print("Ticker not found or market closed")
    else:
        print(f"API error {e.status_code}: {e}")
except Exception as e:
    print(f"Network/unexpected error: {e}")
```

---

## 8. Market Hours Reference

All sessions are in **Eastern Time (ET)**:

| Session | Hours (ET) |
|---|---|
| Pre-market | 4:00 AM – 9:30 AM |
| Regular market | 9:30 AM – 4:00 PM |
| After-hours | 4:00 PM – 8:00 PM |

During market hours, `lastTrade.price` reflects live prints. Outside hours, `prevDay.close` is the reference price.

---

## 9. Migration from polygon-api-client

If transitioning from the old `polygon-api-client` package:

```bash
# Old
pip install polygon-api-client
# New
pip install massive
```

| Old import | New import |
|---|---|
| `from polygon import RESTClient` | `from massive import RESTClient` |
| `from polygon import WebSocketClient` | `from massive import WebSocketClient` |
| `api.polygon.io` | `api.massive.com` |

Method names and response shapes are identical. Existing code migrates with an `import` change only.
