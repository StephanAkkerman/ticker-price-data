# ticker-price-data

Unified ticker price data from **Yahoo Finance** (stocks/indices/forex/futures),
**CoinGecko** (crypto), and **TradingView** (universal fallback).

One normalized quote shape for every asset, with sensible fallbacks and built-in
caching.

## Key Features 🔑

- `get_price(ticker, asset_type)` — single entry point; `asset_type="auto"` uses
  [ticker-classifier](https://github.com/StephanAkkerman/ticker-classifier) to decide
  stock vs crypto vs forex automatically.
- `get_ticker(ticker)` — everything known about a symbol in one call: classification
  metadata (sector, industry, market cap, company profile, ...) **plus** the live quote,
  classifying only once.
- `get_stock_info(ticker)` — Yahoo Finance, with a TradingView fallback.
- `get_crypto_info(ticker)` — CoinGecko via the website `search_v2` endpoint (avoids the
  public API's free-tier rate limits), with Yahoo → TradingView fallbacks.
- `get_tradingview_quote(symbol, asset_hint)` — realtime websocket pool + scraper fallback.
- In-memory caching, stale/negative caching, and concurrency limits built in.
- Pre-market and after-hours data where available.

All helpers return `Optional[dict]`:

```python
{
    "price": float,
    "change_percent": float,
    "volume": float,
    "website": str,
    "source": str,            # "yahoo" | "coingecko" | "tradingview"
    # yahoo also includes "last_close": float | None
}
```

## Installation ⚙️

```bash
pip install ticker-price-data
```

or, for local development:

```bash
pip install -e .
```

## Usage ⌨️

```python
import asyncio
from ticker_price_data import get_price, get_ticker, get_stock_info, get_crypto_info

async def main():
    print(await get_stock_info("AAPL"))     # Yahoo
    print(await get_crypto_info("BTC"))     # CoinGecko
    print(await get_price("BTC", "auto"))   # classified automatically
    print(await get_ticker("AAPL"))         # metadata (sector, industry, ...) + quote

asyncio.run(main())
```

## License 📜

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
